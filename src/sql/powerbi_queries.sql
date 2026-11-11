-- Native queries for Power BI DirectQuery over the metric views (definitions stay in Unity Catalog).
SELECT KPI, `Forecast Date`,
       MEASURE(`Forecast Units`) AS forecast_units,
       MEASURE(`Forecast Lower 90`) AS lower_90,
       MEASURE(`Forecast Upper 90`) AS upper_90
FROM <catalog>.<schema>.fc_mv_forecasts
GROUP BY ALL;

SELECT KPI, MEASURE(`WAPE`) AS wape
FROM <catalog>.<schema>.fc_mv_accuracy
WHERE Split = 'scored_vs_actual'
GROUP BY ALL;
