# Databricks notebook source
# Forecast Factory: load_registry (first task of the parent job).
# Reads the active KPIs and publishes them as a task value for the for_each fan-out.

# COMMAND ----------
import json
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

# COMMAND ----------
rows = spark.table(REG).where("active = true").orderBy("kpi_id").select("kpi_id", "kpi_name", "model_family", "grain", "horizon").collect()
kpi_ids = [r["kpi_id"] for r in rows]
print(f"{len(kpi_ids)} active KPIs:")
for r in rows:
    print(f"  {r['kpi_id']:<24} {r['model_family']:<14} {r['grain']:<8} h={r['horizon']}  {r['kpi_name']}")
dbutils.jobs.taskValues.set(key="kpi_ids", value=kpi_ids)
