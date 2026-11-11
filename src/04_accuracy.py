# Databricks notebook source
# Forecast Factory: 04_accuracy (parent job, after the KPI fan-out)
# 1. fc_forecast_accuracy: row-level errors for backtest and holdout (from fc_backtest_detail, all runs)
#    plus scored-vs-actual rows (fc_forecasts joined to actuals in the source tables).
# 2. fc_model_runs: MLflow run log for every KPI run in the experiment.
# 3. fc_model_status: current champion and challenger per KPI.

# COMMAND ----------
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from pyspark.sql import Window

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
FCST = f"{CAT}.{SCH}.{P}forecasts"
ACC = f"{CAT}.{SCH}.{P}forecast_accuracy"
RUNS = f"{CAT}.{SCH}.{P}model_runs"
STATUS = f"{CAT}.{SCH}.{P}model_status"
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------
# 1a. backtest + holdout rows, every run
detail = spark.table(DETAIL).select(
    "kpi_id", "model_version", F.col("split"), "series_key", F.col("ds").cast("date").alias("ds"),
    F.col("y").alias("y_actual"), "yhat", "yhat_lo", "yhat_hi", "run_id",
)

# COMMAND ----------
# 1b. scored-vs-actual: champion forecasts (fc_forecasts) against actuals rebuilt from the source tables
def actuals_for(r):
    keys = list(r["series_keys"])
    agg = {"sum": F.sum, "avg": F.avg, "count": F.count}[r["aggregation"]]
    trunc = {"daily": "day", "weekly": "week", "monthly": "month"}[r["grain"]]
    key_expr = F.concat_ws(" | ", *[F.concat(F.lit(f"{k}="), F.col(k).cast("string")) for k in keys])
    a = (spark.table(r["source_table"]).where(F.col(r["target_col"]).isNotNull())
         .select(key_expr.alias("series_key"),
                 F.to_date(F.date_trunc(trunc, F.col(r["time_col"]))).alias("ds"),
                 (F.lit(1.0) if r["aggregation"] == "count" else F.col(r["target_col"]).cast("double")).alias("v"))
         .groupBy("series_key", "ds").agg(agg("v").alias("actual")))
    return a, r["aggregation"] in ("sum", "count")


regs = spark.table(REG).collect()  # every KPI, so retired KPIs keep their history
scored_parts = []
for r in regs:
    fc = spark.table(FCST).where(F.col("kpi_id") == r["kpi_id"])
    if fc.limit(1).count() == 0:
        continue
    a, zero_fill = actuals_for(r)
    j = fc.join(a, ["series_key", "ds"], "left" if zero_fill else "inner")
    if zero_fill:  # a missing period inside the observed range means zero activity; future periods are not actuals yet
        last_actual = a.agg(F.max("ds")).first()[0]
        j = j.where(F.col("ds") <= F.lit(last_actual)).withColumn("actual", F.coalesce(F.col("actual"), F.lit(0.0)))
    j = j.where(F.col("actual").isNotNull())
    scored_parts.append(j.select(
        "kpi_id", "model_version", F.lit("scored_vs_actual").alias("split"), "series_key", "ds",
        F.col("actual").alias("y_actual"), "yhat", "yhat_lo", "yhat_hi", F.lit(None).cast("string").alias("run_id"),
    ))

scored = None
for p in scored_parts:
    scored = p if scored is None else scored.unionByName(p)

# COMMAND ----------
acc = ((detail.unionByName(scored) if scored is not None else detail)
       .withColumn("abs_error", F.abs(F.col("y_actual") - F.col("yhat")))
       .withColumn("abs_pct_error", F.when(F.col("y_actual") > 0, F.abs(F.col("y_actual") - F.col("yhat")) / F.col("y_actual")))
       .withColumn("evaluated_at", F.current_timestamp()))
acc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ACC)
spark.sql(f"COMMENT ON TABLE {ACC} IS 'Row-level accuracy: backtest/holdout (per run) and scored_vs_actual (champion). WAPE = sum(abs_error)/sum(y_actual).'")
summary = (spark.table(ACC).groupBy("kpi_id", "split", "model_version")
           .agg((F.sum("abs_error") / F.sum("y_actual")).alias("wape"), F.count("*").alias("rows"))
           .orderBy("kpi_id", "split", "model_version"))
print(summary.toPandas().to_string())

# COMMAND ----------
# 2. MLflow run log (one row per KPI run)
runs = mlflow.search_runs(experiment_ids=[EXP_ID], filter_string="tags.kpi_id != ''", order_by=["start_time DESC"])
log_rows = []
for _, r in runs.iterrows():
    kpi = r["params.kpi_id"]
    versions = client.search_model_versions(f"name='{CAT}.{SCH}.{P}{kpi}'")
    mv = next((v.version for v in versions if v.run_id == r["run_id"]), None)
    log_rows.append({
        "run_id": r["run_id"], "kpi_id": kpi, "run_name": r.get("tags.mlflow.runName"),
        "model_family": r.get("params.model_family"), "model_version": mv,
        "cutoff": r.get("params.cutoff"), "n_series": r.get("params.n_series"),
        "wape_backtest": r.get("metrics.wape_backtest"), "wape_holdout": r.get("metrics.wape_holdout"),
        "mape_backtest": r.get("metrics.mape_backtest"), "fit_seconds": r.get("metrics.fit_seconds"),
        "promotion_decision": r.get("tags.promotion_decision"), "promotion_reason": r.get("tags.promotion_reason"),
        "started_at": r["start_time"].to_pydatetime() if pd.notna(r["start_time"]) else None,
        "status": r.get("status"),
    })
log_df = spark.createDataFrame(pd.DataFrame(log_rows).astype({"n_series": "float64"}, errors="ignore"))
log_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RUNS)
print(f"fc_model_runs: {len(log_rows)} runs")

# COMMAND ----------
# 3. registry status per KPI
status = []
for r in regs:
    kpi = r["kpi_id"]
    name = f"{CAT}.{SCH}.{P}{kpi}"
    entry = {"kpi_id": kpi, "model_name": name, "champion_version": None, "champion_wape": None,
             "challenger_version": None, "challenger_wape": None}
    for alias, pre in (("champion", "champion"), ("challenger", "challenger")):
        try:
            v = client.get_model_version_by_alias(name, alias)
            entry[f"{pre}_version"] = v.version
            entry[f"{pre}_wape"] = client.get_run(v.run_id).data.metrics.get("wape_backtest")
        except Exception:
            pass
    status.append(entry)
st = spark.createDataFrame(pd.DataFrame(status).astype({"champion_wape": "float64", "challenger_wape": "float64"}))
st = st.withColumn("updated_at", F.current_timestamp())
st.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STATUS)
print(spark.table(STATUS).toPandas().to_string())
