# Databricks notebook source
# Forecast Factory: 00_setup (setup job; safe to re-run)
# Creates the config registry, the output tables and the two metric views.
# If seed_sample_kpis = true, registers four example KPIs that point at the synthetic sample tables from 00_sample_data.
# To add your own KPI, insert one row into <prefix>forecast_registry (see src/sql/add_kpi_template.sql).

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "forecast_factory")
dbutils.widgets.text("table_prefix", "fc_")
dbutils.widgets.text("seed_sample_kpis", "true")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
SEED = dbutils.widgets.get("seed_sample_kpis").strip().lower() == "true"
assert CAT and SCH, "Set the catalog and schema parameters"
T = lambda n: f"{CAT}.{SCH}.{P}{n}"
REG, DETAIL, FCST, ACC = T("forecast_registry"), T("backtest_detail"), T("forecasts"), T("forecast_accuracy")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REG} (
  kpi_id STRING COMMENT 'Stable id; also the suffix of the UC model <prefix><kpi_id>',
  kpi_name STRING,
  description STRING,
  source_table STRING COMMENT 'Fully qualified source table or view (catalog.schema.table)',
  time_col STRING COMMENT 'Date or timestamp column',
  series_keys ARRAY<STRING> COMMENT 'Columns that define one series',
  target_col STRING COMMENT 'Column to aggregate (for count, any non-null id column)',
  aggregation STRING COMMENT 'sum | avg | count',
  grain STRING COMMENT 'daily | weekly | monthly',
  horizon INT COMMENT 'Periods to forecast (also the holdout length)',
  model_family STRING COMMENT 'AutoETS | AutoARIMA | SeasonalNaive',
  backtest_windows INT COMMENT 'Rolling-origin windows inside the training period',
  promote_metric STRING COMMENT 'wape (backtest WAPE decides champion vs challenger)',
  active BOOLEAN,
  owner STRING,
  powerbi_dataset STRING,
  series_filter STRING COMMENT 'Optional SQL predicate on source_table that restricts the series universe'
) USING DELTA
COMMENT 'Forecast Factory config. One row per KPI. Add a row and rerun the job.'
""")

# COMMAND ----------
if SEED:
    seed = f"""
    SELECT * FROM VALUES
     ('weekly_visits', 'Weekly completed visits', 'Completed clinic visits per week, by facility (SYNTHETIC sample)',
      '{T("sample_clinic_throughput_daily")}', 'visit_date', array('facility_id'), 'completed_visits', 'sum', 'weekly', 8,
      'AutoETS', 3, 'wape', true, 'Sample', 'Forecast Factory - Clinic Throughput', CAST(NULL AS STRING)),
     ('monthly_appointments', 'Monthly appointments', 'Appointments per month, by facility x specialty x visit type (SYNTHETIC sample)',
      '{T("sample_specialty_access_monthly")}', 'service_month', array('facility_id', 'specialty', 'visit_type'), 'appointments', 'sum', 'monthly', 6,
      'AutoETS', 3, 'wape', true, 'Sample', 'Forecast Factory - Specialty Access', CAST(NULL AS STRING)),
     ('monthly_no_show_pct', 'Monthly no-show %', 'No-show rate per month, by facility x specialty x visit type (SYNTHETIC sample, rate KPI)',
      '{T("sample_specialty_access_monthly")}', 'service_month', array('facility_id', 'specialty', 'visit_type'), 'no_show_pct', 'avg', 'monthly', 6,
      'SeasonalNaive', 3, 'wape', true, 'Sample', 'Forecast Factory - Specialty Access', CAST(NULL AS STRING)),
     ('monthly_referrals', 'Monthly referral volume', 'Referrals per month, by specialty (SYNTHETIC sample)',
      '{T("sample_referrals")}', 'referral_date', array('specialty'), 'referral_id', 'count', 'monthly', 6,
      'AutoETS', 3, 'wape', true, 'Sample', 'Forecast Factory - Referrals', CAST(NULL AS STRING))
    AS t(kpi_id, kpi_name, description, source_table, time_col, series_keys, target_col, aggregation, grain,
         horizon, model_family, backtest_windows, promote_metric, active, owner, powerbi_dataset, series_filter)
    """
    spark.sql(f"MERGE INTO {REG} t USING ({seed}) s ON t.kpi_id = s.kpi_id WHEN NOT MATCHED THEN INSERT *")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DETAIL} (
  kpi_id STRING, run_id STRING, model_version STRING, model_family STRING, run_ts STRING,
  series_key STRING, split STRING COMMENT 'backtest | holdout', cutoff TIMESTAMP,
  ds TIMESTAMP, y DOUBLE, yhat DOUBLE, yhat_lo DOUBLE, yhat_hi DOUBLE
) USING DELTA COMMENT 'Per-series backtest and holdout rows written by 01_train'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FCST} (
  kpi_id STRING, series_key STRING, ds DATE, yhat DOUBLE, yhat_lo DOUBLE, yhat_hi DOUBLE,
  model_version STRING, run_date DATE
) USING DELTA COMMENT 'Champion forecasts written by 03_score (latest scoring per KPI)'
""")
# the accuracy table is rebuilt by 04_accuracy; create it empty so the metric view can be defined now
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ACC} (
  kpi_id STRING, model_version STRING, split STRING, series_key STRING, ds DATE, y_actual DOUBLE,
  yhat DOUBLE, yhat_lo DOUBLE, yhat_hi DOUBLE, run_id STRING, abs_error DOUBLE, abs_pct_error DOUBLE, evaluated_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------
# metric views for Power BI (DirectQuery with MEASURE()) and dashboards
spark.sql(f"""
CREATE OR REPLACE VIEW {T("mv_forecasts")}
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Forecast Factory: champion forecasts by KPI, series and period"
source: {FCST}
dimensions:
  - name: KPI
    expr: kpi_id
  - name: Series
    expr: series_key
  - name: Forecast Date
    expr: ds
  - name: Model Version
    expr: model_version
  - name: Run Date
    expr: run_date
measures:
  - name: Forecast Units
    expr: SUM(yhat)
  - name: Forecast Lower 90
    expr: SUM(yhat_lo)
  - name: Forecast Upper 90
    expr: SUM(yhat_hi)
  - name: Series Count
    expr: COUNT(DISTINCT series_key)
$$""")
spark.sql(f"""
CREATE OR REPLACE VIEW {T("mv_accuracy")}
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Forecast Factory accuracy: backtest, holdout and scored-vs-actual errors"
source: {ACC}
dimensions:
  - name: KPI
    expr: kpi_id
  - name: Split
    expr: split
  - name: Model Version
    expr: model_version
  - name: Series
    expr: series_key
  - name: Period
    expr: ds
measures:
  - name: Actual Units
    expr: SUM(y_actual)
  - name: Forecast Units
    expr: SUM(yhat)
  - name: Absolute Error
    expr: SUM(abs_error)
  - name: WAPE
    expr: SUM(abs_error) / NULLIF(SUM(y_actual), 0)
    comment: "Weighted absolute percentage error"
$$""")

# COMMAND ----------
print(spark.table(REG).select("kpi_id", "source_table", "grain", "horizon", "model_family", "active").toPandas().to_string())
