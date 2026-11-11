-- Forecast Factory: register a new KPI (one row), then run the "Forecast Factory" job.
-- Replace every <...> value. Keep one KPI per row; kpi_id becomes the Unity Catalog model name <prefix><kpi_id>.
-- Tip: start with active = false, review the row with the KPI owner, then set active = true.
INSERT INTO <catalog>.<schema>.fc_forecast_registry
  (kpi_id, kpi_name, description, source_table, time_col, series_keys, target_col, aggregation, grain,
   horizon, model_family, backtest_windows, promote_metric, active, owner, powerbi_dataset, series_filter)
SELECT
  '<kpi_id, for example weekly_ed_arrivals>',
  '<display name>',
  '<what one value means, and the business definition owner>',
  '<catalog.schema.source_table_or_view>',   -- one SQL view per KPI that holds the business definition is recommended
  '<date or timestamp column>',
  array('<series key column>'),             -- for example array('facility_id') or array('facility_id', 'specialty')
  '<target column>',                         -- for count, any non-null id column
  'sum',                                     -- sum | avg | count
  'weekly',                                  -- daily | weekly | monthly
  8,                                         -- horizon: periods to forecast (also the holdout length)
  'AutoETS',                                 -- AutoETS | AutoARIMA | SeasonalNaive
  3,                                         -- backtest windows
  'wape',
  true,
  '<KPI owner>',
  '<Power BI semantic model that consumes it>',
  NULL                                       -- optional predicate on the source table, for example "region = 'East'"
WHERE NOT EXISTS (
  SELECT 1 FROM <catalog>.<schema>.fc_forecast_registry WHERE kpi_id = '<kpi_id>'
);

-- Retire a KPI without losing its history:
-- UPDATE <catalog>.<schema>.fc_forecast_registry SET active = false WHERE kpi_id = '<kpi_id>';
