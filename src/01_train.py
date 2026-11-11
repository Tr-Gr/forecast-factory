# Databricks notebook source
# Forecast Factory: 01_train (one KPI per run, parameter kpi_id)
# 1. read the registry row
# 2. build the series panel (Spark aggregation, dense per series)
# 3. fit a model per series in parallel: groupBy(series_key).applyInPandas with statsforecast
#    - rolling-origin backtest inside the training period (no holdout peeking)
#    - holdout forecast for the last `horizon` periods (actuals exist, so accuracy is real)
# 4. log to MLflow, register a pyfunc model in UC (<catalog>.<schema>.<prefix><kpi_id>), set alias @challenger
# 5. write per-series rows to fc_backtest_detail, set task values wape + model_version

# COMMAND ----------
import datetime as dt
import json
import warnings

import numpy as np
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from pyspark.sql import types as T

warnings.filterwarnings("ignore")

dbutils.widgets.text("kpi_id", "")
KPI_ID = dbutils.widgets.get("kpi_id").strip()
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "forecast_factory")
dbutils.widgets.text("table_prefix", "fc_")
dbutils.widgets.text("experiment_id", "")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
EXP_ID = dbutils.widgets.get("experiment_id").strip()
assert CAT and SCH, "Set the catalog and schema parameters (the bundle passes them from databricks.yml)"
REG = f"{CAT}.{SCH}.{P}forecast_registry"
DETAIL = f"{CAT}.{SCH}.{P}backtest_detail"

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_id=EXP_ID)
client = MlflowClient()

# COMMAND ----------
# 1. registry row
rows = spark.table(REG).where(F.col("kpi_id") == KPI_ID).collect()
assert len(rows) == 1, f"kpi_id '{KPI_ID}' not found in {REG}"
cfg = rows[0].asDict()
SERIES_KEYS = list(cfg["series_keys"])
GRAIN = cfg["grain"]
FREQ = {"daily": "D", "weekly": "W-MON", "monthly": "MS"}[GRAIN]
TRUNC = {"daily": "day", "weekly": "week", "monthly": "month"}[GRAIN]
SEASON = {"daily": 7, "weekly": 52, "monthly": 12}[GRAIN]
ZERO_FILL = cfg["aggregation"] in ("sum", "count")  # missing period = no activity
H, K = int(cfg["horizon"]), int(cfg["backtest_windows"])
FAMILY = cfg["model_family"]
MODEL_NAME = f"{CAT}.{SCH}.{P}{KPI_ID}"
RUN_TS = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
print(f"KPI {KPI_ID}: {cfg['kpi_name']} | {FAMILY} | grain={GRAIN} | horizon={H} | windows={K} | keys={SERIES_KEYS}")

# COMMAND ----------
# 2. series panel
AGG = {"sum": F.sum, "avg": F.avg, "count": F.count}[cfg["aggregation"]]
key_expr = F.concat_ws(" | ", *[F.concat(F.lit(f"{k}="), F.col(k).cast("string")) for k in SERIES_KEYS])
_src = spark.table(cfg["source_table"]).where(F.col(cfg["target_col"]).isNotNull())
if cfg.get("series_filter"):  # documented universe restriction from the registry
    _src = _src.where(cfg["series_filter"])
panel_sp = (
    _src
    .select(
        key_expr.alias("series_key"),
        F.to_date(F.date_trunc(TRUNC, F.col(cfg["time_col"]))).alias("ds"),
        (F.lit(1.0) if cfg["aggregation"] == "count" else F.col(cfg["target_col"]).cast("double")).alias("v"),  # count works on any id type
    )
    .groupBy("series_key", "ds")
    .agg(AGG("v").alias("y"))
)
pdf = panel_sp.toPandas()
pdf["ds"] = pd.to_datetime(pdf["ds"])
M = pdf["ds"].max()                                                     # last observed period
CUTOFF = pd.date_range(end=M, periods=H + 1, freq=FREQ)[0]              # training ends here; holdout = last H periods
print("last period M:", M.date(), "| training cutoff:", CUTOFF.date(), "| series:", pdf["series_key"].nunique())


def densify(g):
    g = g.sort_values("ds")
    idx = pd.date_range(g["ds"].iloc[0], M, freq=FREQ)
    s = g.set_index("ds")["y"].reindex(idx)
    s = s.fillna(0.0) if ZERO_FILL else s.interpolate(limit_direction="both")
    return pd.DataFrame({"series_key": g["series_key"].iloc[0], "ds": idx, "y": s.values})


dense = pd.concat([densify(g) for _, g in pdf.groupby("series_key")], ignore_index=True)
N_SERIES = dense["series_key"].nunique()
print("dense panel rows:", len(dense), "series:", N_SERIES)

# COMMAND ----------
# 3. per-series fit + backtest (runs on executors, one series per group)
OUT_SCHEMA = T.StructType([
    T.StructField("series_key", T.StringType()),
    T.StructField("split", T.StringType()),
    T.StructField("cutoff", T.TimestampType()),
    T.StructField("ds", T.TimestampType()),
    T.StructField("y", T.DoubleType()),
    T.StructField("yhat", T.DoubleType()),
    T.StructField("yhat_lo", T.DoubleType()),
    T.StructField("yhat_hi", T.DoubleType()),
])
_CFG = dict(FAMILY=FAMILY, SEASON=SEASON, FREQ=FREQ, H=H, K=K, CUTOFF=CUTOFF, ZERO_FILL=ZERO_FILL)


def _models(family, season):
    from statsforecast.models import AutoETS, AutoARIMA, SeasonalNaive
    return {
        "AutoETS": lambda: [AutoETS(season_length=season)],
        "AutoARIMA": lambda: [AutoARIMA(season_length=season)],
        "SeasonalNaive": lambda: [SeasonalNaive(season_length=season)],
    }[family]()


def fit_group(g):
    import warnings as _w
    _w.filterwarnings("ignore")
    from statsforecast import StatsForecast
    c = _CFG
    sk = g["series_key"].iloc[0]
    g = g.sort_values("ds")
    train = g.loc[g["ds"] <= c["CUTOFF"], ["series_key", "ds", "y"]].rename(columns={"series_key": "unique_id"})
    fam = c["FAMILY"]

    # rolling-origin backtest inside the training window
    sf = StatsForecast(models=_models(fam, c["SEASON"]), freq=c["FREQ"], n_jobs=1)
    cv = sf.cross_validation(df=train, h=c["H"], n_windows=c["K"], step_size=c["H"])
    bt = pd.DataFrame({
        "series_key": sk, "split": "backtest",
        "cutoff": pd.to_datetime(cv["cutoff"]), "ds": pd.to_datetime(cv["ds"]),
        "y": cv["y"].astype(float), "yhat": cv[fam].astype(float),
        "yhat_lo": np.nan, "yhat_hi": np.nan,
    })

    # holdout forecast: fit on training period, forecast the last H periods
    sf2 = StatsForecast(models=_models(fam, c["SEASON"]), freq=c["FREQ"], n_jobs=1)
    fc = sf2.forecast(df=train, h=c["H"], level=[90])
    actual = g.loc[g["ds"] > c["CUTOFF"], ["ds", "y"]]
    ho = pd.DataFrame({
        "series_key": sk, "split": "holdout", "cutoff": c["CUTOFF"],
        "ds": pd.to_datetime(fc["ds"]), "yhat": fc[fam].astype(float),
        "yhat_lo": fc[f"{fam}-lo-90"].astype(float), "yhat_hi": fc[f"{fam}-hi-90"].astype(float),
    }).merge(actual, on="ds", how="left")

    out = pd.concat([bt, ho[["series_key", "split", "cutoff", "ds", "y", "yhat", "yhat_lo", "yhat_hi"]]], ignore_index=True)
    out["yhat"] = out["yhat"].fillna(0.0)
    out["yhat_lo"] = out["yhat_lo"].fillna(out["yhat"])
    out["yhat_hi"] = out["yhat_hi"].fillna(out["yhat"])
    if c["ZERO_FILL"]:  # counts cannot be negative
        out[["yhat", "yhat_lo", "yhat_hi"]] = out[["yhat", "yhat_lo", "yhat_hi"]].clip(lower=0.0)
    return out[["series_key", "split", "cutoff", "ds", "y", "yhat", "yhat_lo", "yhat_hi"]]


t0 = dt.datetime.utcnow()
# serverless does not support cache/persist, so materialize the (small) result once on the driver
res_pd = (
    spark.createDataFrame(dense)
    .groupBy("series_key")
    .applyInPandas(fit_group, schema=OUT_SCHEMA)
    .toPandas()
)
res = spark.createDataFrame(res_pd, schema=OUT_SCHEMA)
n_rows = len(res_pd)
FIT_SECONDS = (dt.datetime.utcnow() - t0).total_seconds()
print(f"fit + backtest done: {n_rows} rows in {FIT_SECONDS:.0f}s")

# COMMAND ----------
# metrics: WAPE = sum|y-yhat| / sum|y|; MAPE over rows with y > 0 (MAPE is undefined at zero)
def acc(df):
    r = df.agg(
        F.sum(F.abs(F.col("y") - F.col("yhat"))).alias("ae"),
        F.sum(F.abs(F.col("y"))).alias("ay"),
        F.avg(F.when(F.col("y") > 0, F.abs(F.col("y") - F.col("yhat")) / F.col("y"))).alias("mape"),
    ).first()
    return (float(r["ae"] / r["ay"]) if r["ay"] else float("nan"), float(r["mape"]) if r["mape"] is not None else float("nan"))


wape_bt, mape_bt = acc(res.where("split = 'backtest'"))
wape_ho, mape_ho = acc(res.where("split = 'holdout'"))
print(f"backtest WAPE={wape_bt:.4f} MAPE={mape_bt:.4f} | holdout WAPE={wape_ho:.4f} MAPE={mape_ho:.4f}")

# COMMAND ----------
# 4. pyfunc: refit-on-predict wrapper around the stored training panel
class ForecastFactoryModel(mlflow.pyfunc.PythonModel):
    """Holds the training panel and the model config.
    predict(empty or None): refit each series on the stored panel and forecast `horizon` periods.
    predict(df[series_key, ds, y]): same, on the supplied history."""

    def load_context(self, context):
        import json as _json
        self.panel = pd.read_pickle(context.artifacts["panel"])
        with open(context.artifacts["config"]) as fh:
            self.cfg = _json.load(fh)

    def predict(self, context, model_input, params=None):
        import warnings as _w
        _w.filterwarnings("ignore")
        from statsforecast import StatsForecast
        from statsforecast.models import AutoETS, AutoARIMA, SeasonalNaive
        cfg = self.cfg
        fam, season = cfg["model_family"], cfg["season"]
        hist = self.panel if (model_input is None or len(model_input) == 0) else model_input
        df = hist[["series_key", "ds", "y"]].rename(columns={"series_key": "unique_id"}).copy()
        df["ds"] = pd.to_datetime(df["ds"])
        df["y"] = df["y"].astype(float)
        models = {
            "AutoETS": lambda: [AutoETS(season_length=season)],
            "AutoARIMA": lambda: [AutoARIMA(season_length=season)],
            "SeasonalNaive": lambda: [SeasonalNaive(season_length=season)],
        }[fam]()
        sf = StatsForecast(models=models, freq=cfg["freq"], n_jobs=-1)
        fc = sf.forecast(df=df, h=int(cfg["horizon"]), level=[90])
        lo, hi = fc[f"{fam}-lo-90"].astype(float), fc[f"{fam}-hi-90"].astype(float)
        yhat = fc[fam].astype(float)
        if cfg.get("zero_fill", False):
            yhat, lo, hi = yhat.clip(lower=0), lo.clip(lower=0), hi.clip(lower=0)
        return pd.DataFrame({
            "series_key": fc["unique_id"].values,
            "ds": pd.to_datetime(fc["ds"]).values,
            "yhat": yhat.values, "yhat_lo": lo.values, "yhat_hi": hi.values,
        })


train_panel = dense[dense["ds"] <= CUTOFF].reset_index(drop=True)
panel_path = f"/tmp/fc_{KPI_ID}_panel_{RUN_TS}.pkl"
cfg_path = f"/tmp/fc_{KPI_ID}_config_{RUN_TS}.json"
train_panel.to_pickle(panel_path)
model_cfg = dict(kpi_id=KPI_ID, model_family=FAMILY, freq=FREQ, season=SEASON, horizon=H,
                 series_keys=SERIES_KEYS, cutoff=str(CUTOFF.date()), zero_fill=ZERO_FILL)
with open(cfg_path, "w") as fh:
    json.dump(model_cfg, fh)

pip_reqs = ["statsforecast", "pandas", "numpy", "mlflow"]
from mlflow.models import ModelSignature
from mlflow.types import Schema, ColSpec
SIGNATURE = ModelSignature(
    inputs=Schema([ColSpec("string", "series_key"), ColSpec("datetime", "ds"), ColSpec("double", "y")]),
    outputs=Schema([ColSpec("string", "series_key"), ColSpec("datetime", "ds"), ColSpec("double", "yhat"),
                    ColSpec("double", "yhat_lo"), ColSpec("double", "yhat_hi")]),
)
with mlflow.start_run(run_name=f"{KPI_ID}_{RUN_TS}") as run:
    run_id = run.info.run_id
    mlflow.log_params({
        "kpi_id": KPI_ID, "kpi_name": cfg["kpi_name"], "model_family": FAMILY, "grain": GRAIN,
        "horizon": H, "backtest_windows": K, "season_length": SEASON, "series_keys": ",".join(SERIES_KEYS),
        "aggregation": cfg["aggregation"], "cutoff": str(CUTOFF.date()), "last_period": str(M.date()),
        "n_series": N_SERIES,
    })
    mlflow.log_metrics({
        "wape_backtest": wape_bt, "mape_backtest": mape_bt,
        "wape_holdout": wape_ho, "mape_holdout": mape_ho,
        "fit_seconds": FIT_SECONDS, "n_series": float(N_SERIES),
    })
    mlflow.set_tags({"kpi_id": KPI_ID, "model_family": FAMILY, "grain": GRAIN, "run_kind": "train",
                     "job_stage": "01_train", "cutoff": str(CUTOFF.date())})
    info = mlflow.pyfunc.log_model(
        name="model",
        python_model=ForecastFactoryModel(),
        artifacts={"panel": panel_path, "config": cfg_path},
        pip_requirements=pip_reqs,
        signature=SIGNATURE,
        registered_model_name=MODEL_NAME,
    )
mv = str(info.registered_model_version)
client.set_registered_model_alias(MODEL_NAME, "challenger", mv)
client.update_model_version(MODEL_NAME, mv, description=f"{FAMILY} per-series, {N_SERIES} series, cutoff {CUTOFF.date()}, backtest WAPE {wape_bt:.4f}")
print(f"registered {MODEL_NAME} v{mv} run={run_id} alias=challenger")

# COMMAND ----------
# 5. stage detail rows for 04_accuracy
det = (
    res.withColumn("kpi_id", F.lit(KPI_ID))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("model_version", F.lit(mv))
    .withColumn("model_family", F.lit(FAMILY))
    .withColumn("run_ts", F.lit(RUN_TS))
    .select("kpi_id", "run_id", "model_version", "model_family", "run_ts", "series_key", "split",
            "cutoff", "ds", "y", "yhat", "yhat_lo", "yhat_hi")
)
det.write.mode("append").saveAsTable(DETAIL)
print("detail rows appended to", DETAIL)

# COMMAND ----------
dbutils.jobs.taskValues.set(key="wape", value=round(wape_bt, 6))
dbutils.jobs.taskValues.set(key="model_version", value=mv)
dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="kpi_id", value=KPI_ID)
print(json.dumps({"kpi_id": KPI_ID, "model_version": mv, "run_id": run_id, "wape_backtest": wape_bt,
                  "wape_holdout": wape_ho, "fit_seconds": FIT_SECONDS}))
