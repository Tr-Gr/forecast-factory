-- Query for a SQL alert on accuracy drift (Databricks SQL > Alerts). Trigger when max_scored_wape > your threshold.
-- scored_vs_actual rows appear once actuals land for forecast periods; until then, alert on split = 'holdout'.
-- Pair it with a job task or a job trigger that reruns the Forecast Factory job; the promotion gate still applies.
SELECT max(wape) AS max_scored_wape, max_by(kpi_id, wape) AS worst_kpi
FROM (
  SELECT kpi_id, sum(abs_error) / nullif(sum(y_actual), 0) AS wape
  FROM <catalog>.<schema>.fc_forecast_accuracy
  WHERE split = 'scored_vs_actual'
  GROUP BY kpi_id
);
