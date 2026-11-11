# Databricks notebook source
# Forecast Factory: 02_evaluate_promote (child job, after train)
# Challenger (alias @challenger, from train) vs champion (alias @champion), on backtest WAPE.
# Same cutoff: compare the WAPE logged in MLflow. Different cutoff (new data arrived): re-score the champion on the
# challenger's exact backtest windows, so the comparison is always like for like.
# Promote if there is no champion yet, or the challenger's WAPE is strictly lower. Ties keep the champion.
# Sets task values `promoted` and `should_score`. The score task runs through a condition task on should_score:
#   forward mode: always (the current champion publishes fresh forecasts every run)
#   holdout mode: only when a new champion was promoted

# COMMAND ----------
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.exceptions import MlflowException

dbutils.widgets.text("kpi_id", "")
dbutils.widgets.text("score_mode", "forward")
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
DETAIL = f"{CAT}.{SCH}.{P}backtest_detail"
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------
chall_version = dbutils.jobs.taskValues.get(taskKey="train", key="model_version")
chall_run_id = dbutils.jobs.taskValues.get(taskKey="train", key="run_id")
chall = client.get_model_version(MODEL_NAME, chall_version)
chall_run = client.get_run(chall_run_id)
chall_wape = chall_run.data.metrics["wape_backtest"]
chall_cutoff = chall_run.data.params["cutoff"]

try:
    champ = client.get_model_version_by_alias(MODEL_NAME, "champion")
    champ_run = client.get_run(champ.run_id)
    champ_wape = champ_run.data.metrics["wape_backtest"]
    champ_cutoff = champ_run.data.params["cutoff"]
except MlflowException:
    champ, champ_wape, champ_cutoff = None, None, None

# COMMAND ----------
if champ is None:
    promote, reason = True, "no champion yet"
elif champ_cutoff != chall_cutoff:
    # new data moved the cutoff: re-score the champion on the challenger's backtest windows
    import pandas as pd
    panel = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{chall_version}").unwrap_python_model().panel.astype({"y": "float64"})
    champ_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
    bt = (spark.table(DETAIL).where(f"kpi_id = '{KPI_ID}' AND run_id = '{chall_run_id}' AND split = 'backtest'")
          .select("series_key", "cutoff", "ds", "y").toPandas())
    bt["ds"], bt["cutoff"] = pd.to_datetime(bt["ds"]), pd.to_datetime(bt["cutoff"])
    ae = ay = 0.0
    for c in sorted(bt["cutoff"].unique()):
        fc = champ_model.predict(panel[panel["ds"] <= c])
        fc["ds"] = pd.to_datetime(fc["ds"])
        m = bt[bt["cutoff"] == c].merge(fc[["series_key", "ds", "yhat"]], on=["series_key", "ds"], how="inner")
        ae += float((m["y"] - m["yhat"]).abs().sum())
        ay += float(m["y"].abs().sum())
    champ_wape = ae / ay if ay else float("inf")
    promote = round(chall_wape, 6) < round(champ_wape, 6)  # ties (to 6 decimals) keep the champion
    reason = (f"cutoff moved ({champ_cutoff} to {chall_cutoff}); champion re-scored on the same backtest windows: "
              f"challenger WAPE {chall_wape:.4f} vs champion {champ_wape:.4f}; " + ("promoted" if promote else "champion retained"))
else:
    promote = round(chall_wape, 6) < round(champ_wape, 6)  # ties (to 6 decimals) keep the champion
    reason = (f"challenger WAPE {chall_wape:.4f} < champion {champ_wape:.4f}" if promote
              else f"challenger WAPE {chall_wape:.4f} is not better than champion {champ_wape:.4f}; champion retained")

decision = "promoted" if promote else "retained"
if promote:
    client.set_registered_model_alias(MODEL_NAME, "champion", chall_version)
client.set_model_version_tag(MODEL_NAME, chall_version, "promotion_decision", decision)
client.set_tag(chall_run_id, "promotion_decision", decision)
client.set_tag(chall_run_id, "promotion_reason", reason)
client.set_tag(chall_run_id, "champion_version", champ.version if champ else "none")

print(f"{KPI_ID}: challenger v{chall_version} -> {decision.upper()} ({reason})")
dbutils.jobs.taskValues.set(key="promoted", value="true" if promote else "false")
should_score = promote or dbutils.widgets.get("score_mode").strip().lower() == "forward"
dbutils.jobs.taskValues.set(key="should_score", value="true" if should_score else "false")
dbutils.jobs.taskValues.set(key="champion_version", value=str(chall_version if promote else (champ.version if champ else "none")))
