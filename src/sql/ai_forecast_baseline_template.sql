-- Tier 1 baseline: ai_forecast() in SQL, no pipeline. Useful to compare against the framework's WAPE.
-- Example over the synthetic sample table; replace the table, columns and dates for your own KPI.
WITH hist AS (
  SELECT facility_id, date_trunc('week', visit_date) AS ds, sum(completed_visits) AS y
  FROM <catalog>.<schema>.fc_sample_clinic_throughput_daily
  WHERE visit_date < date_sub(current_date(), 56)
  GROUP BY ALL
)
SELECT * FROM ai_forecast(
  TABLE(hist), horizon => current_date(), time_col => 'ds', value_col => 'y', group_col => 'facility_id');
