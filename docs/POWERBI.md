# Forecast Factory: Power BI

## What Power BI reads

| Object | Use it for | Notes |
|---|---|---|
| `fc_forecasts` | Champion forecasts: one row per KPI, series and period, with `yhat`, `yhat_lo`, `yhat_hi`, `model_version`, `run_date` | Replaced per KPI on each scoring run |
| `fc_mv_forecasts` | Metric view over `fc_forecasts` (Forecast Units, Forecast Lower 90, Forecast Upper 90, Series Count) | Query with `MEASURE()` |
| `fc_mv_accuracy` | Metric view over `fc_forecast_accuracy` (WAPE, Actual Units, Forecast Units, Absolute Error) | WAPE is a ratio measure, so it aggregates correctly at any level |
| `fc_model_status`, `fc_model_runs` | Champion and challenger per KPI, and the run log | Small tables for an accuracy or governance page |

## Connect

1. In Power BI Desktop, use the Azure Databricks connector and choose your SQL warehouse. Sign in with Microsoft Entra ID. Unity Catalog permissions apply to every query. See [Power BI Desktop with Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi-desktop).
2. Pick a storage mode per dashboard:
   - **DirectQuery** when the dashboard must show the latest champion forecast without a refresh. The semantic model queries the metric view, so there is no second copy to keep in sync.
   - **Import** when a refresh after each weekly run is enough.

## Native queries with MEASURE() (DirectQuery)

Metric views are queried with `MEASURE()`. Use a native query so the definitions stay in Unity Catalog. Examples are in `src/sql/powerbi_queries.sql`:

```sql
SELECT KPI, `Forecast Date`,
       MEASURE(`Forecast Units`) AS forecast_units,
       MEASURE(`Forecast Lower 90`) AS lower_90,
       MEASURE(`Forecast Upper 90`) AS upper_90
FROM <catalog>.<schema>.fc_mv_forecasts
GROUP BY ALL
```

The older metric view BI compatibility mode was removed in April 2026, so plan on native `MEASURE()` queries or Publish to Power BI.

## Publish to Power BI from Catalog Explorer

To give a team a managed semantic model, publish the tables or schema from Catalog Explorer to a Power BI workspace. See [Azure Databricks and Power BI](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi).

## Refresh after scoring

For Import mode, refresh the semantic model as soon as new forecasts land by adding a step after `accuracy_rollup` in the `Forecast Factory` job:

- A Power BI task in Lakeflow Jobs (Public Preview), or
- A notebook task that calls the Power BI REST API refresh endpoint with a service principal.

Confirm the task type is available in your workspace and that the identity running the job has permission on the Power BI workspace.
