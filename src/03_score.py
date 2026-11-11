# Databricks notebook source
# Forecast Factory: 03_score (child job, runs only when promoted)
# Loads models:/<prefix><kpi_id>@champion and writes forecasts with lower/upper bounds to <prefix>forecasts.
# Replaces the KPI's current rows, so <prefix>forecasts always holds the latest champion forecast per KPI.
# score_mode = forward (default): refit on the full history from the source table and forecast the next `horizon` periods.
# score_mode = holdout: forecast the holdout window the model was evaluated on (handy for demos: actuals already exist).

# COMMAND ----------
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

dbutils.widgets.text("kpi_id", "")
dbutils.widgets.text("score_mode", "forward")
SCORE_MODE = dbutils.widgets.get("score_mode").strip().lower()
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
MODEL_NAME = f"{CAT}.{SCH}.{P}{KPI_ID}"
FCST = f"{CAT}.{SCH}.{P}forecasts"
REG = f"{CAT}.{SCH}.{P}forecast_registry"
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------
champ = client.get_model_version_by_alias(MODEL_NAME, "champion")
model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
def full_history():
    """Rebuild the dense series panel from the source table, the same way 01_train does, through the last period."""
    cfg = spark.table(REG).where(F.col("kpi_id") == KPI_ID).first().asDict()
    keys = list(cfg["series_keys"])
    freq = {"daily": "D", "weekly": "W-MON", "monthly": "MS"}[cfg["grain"]]
    trunc = {"daily": "day", "weekly": "week", "monthly": "month"}[cfg["grain"]]
    agg = {"sum": F.sum, "avg": F.avg, "count": F.count}[cfg["aggregation"]]
    zero_fill = cfg["aggregation"] in ("sum", "count")
    key_expr = F.concat_ws(" | ", *[F.concat(F.lit(f"{k}="), F.col(k).cast("string")) for k in keys])
    src = spark.table(cfg["source_table"]).where(F.col(cfg["target_col"]).isNotNull())
    if cfg.get("series_filter"):
        src = src.where(cfg["series_filter"])
    v = F.lit(1.0) if cfg["aggregation"] == "count" else F.col(cfg["target_col"]).cast("double")
    pdf = (src.select(key_expr.alias("series_key"), F.to_date(F.date_trunc(trunc, F.col(cfg["time_col"]))).alias("ds"), v.alias("v"))
              .groupBy("series_key", "ds").agg(agg("v").alias("y")).toPandas())
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    last = pdf["ds"].max()
    parts = []
    for sk, g in pdf.groupby("series_key"):
        s = g.set_index("ds")["y"].reindex(pd.date_range(g["ds"].min(), last, freq=freq))
        s = s.fillna(0.0) if zero_fill else s.interpolate(limit_direction="both")
        parts.append(pd.DataFrame({"series_key": sk, "ds": s.index, "y": s.values}))
    out = pd.concat(parts, ignore_index=True)
    return out.astype({"y": "float64"})


if SCORE_MODE == "forward":
    history = full_history()
else:  # holdout: empty input = the training panel stored with the model
    history = pd.DataFrame({"series_key": pd.Series(dtype="object"), "ds": pd.Series(dtype="datetime64[ns]"), "y": pd.Series(dtype="float64")})
fc = model.predict(history)
print(f"{KPI_ID}: champion v{champ.version}, mode={SCORE_MODE}, {fc['series_key'].nunique()} series, {len(fc)} forecast rows, "
      f"{pd.to_datetime(fc['ds']).min().date()} to {pd.to_datetime(fc['ds']).max().date()}")

# COMMAND ----------
out = (
    spark.createDataFrame(fc.astype({"yhat": "float64", "yhat_lo": "float64", "yhat_hi": "float64", "ds": "datetime64[ns]"}))
    .select(
        F.lit(KPI_ID).alias("kpi_id"),
        "series_key",
        F.col("ds").cast("date").alias("ds"),
        "yhat", "yhat_lo", "yhat_hi",
        F.lit(str(champ.version)).alias("model_version"),
        F.current_date().alias("run_date"),
    )
)
spark.sql(f"DELETE FROM {FCST} WHERE kpi_id = '{KPI_ID}'")
out.write.mode("append").saveAsTable(FCST)
n = spark.table(FCST).where(F.col("kpi_id") == KPI_ID).count()
print(f"wrote {n} rows to {FCST} for {KPI_ID} (model v{champ.version})")
dbutils.jobs.taskValues.set(key="rows_written", value=int(n))
