# Forecast Factory

A config-driven framework to train, evaluate, promote, score and monitor many forecasting models on Databricks, with the results ready for Power BI. Adding a KPI is one row in a registry table, not a new pipeline.

```
registry (one row per KPI)
   │
   ▼
Forecast Factory job ── load_registry ──► for_each KPI (parallel) ──► accuracy_rollup
                                              │
                                              ▼
                         KPI pipeline job (sequential, one KPI)
                         train ─► evaluate_promote ─► if_should_score ─► score
                           │            │                                 │
                     MLflow run    @champion / @challenger          <prefix>forecasts
                     UC model      (promote only if backtest              │
                                    WAPE is lower)                metric views ─► Power BI
```

## What gets deployed

| Resource | What it does |
|---|---|
| `Forecast Factory - setup` job | Optional synthetic sample data, the registry, output tables and two metric views. Safe to re-run. |
| `Forecast Factory` job | Reads active KPIs, fans out one child run per KPI in parallel, then rebuilds the accuracy tables. Weekly schedule (paused unless you unpause it). |
| `Forecast Factory - KPI pipeline` job | One KPI end to end: train with a rolling-origin backtest, register a challenger, promote it if it beats the champion, score with the champion. |
| MLflow experiment | One run per KPI per training, with backtest and holdout WAPE and MAPE, and the promotion decision and reason as tags. |
| `Forecast Factory` dashboard | Overview, forecast vs actual, and run log. |

Tables, views and models are created in `<catalog>.<schema>` with the prefix `fc_` (configurable):

| Object | Contents |
|---|---|
| `fc_forecast_registry` | One row per KPI: source, grain, series keys, target, aggregation, horizon, model family, owner, Power BI dataset |
| `fc_<kpi_id>` (registered models) | One Unity Catalog model per KPI, with `@champion` and `@challenger` aliases |
| `fc_forecasts` | Latest champion forecast per KPI, series and period, with a 90% interval |
| `fc_backtest_detail`, `fc_forecast_accuracy` | Row-level backtest, holdout and scored-vs-actual errors |
| `fc_model_runs`, `fc_model_status` | Run log and current champion and challenger per KPI |
| `fc_mv_forecasts`, `fc_mv_accuracy` | Metric views for Power BI (DirectQuery with `MEASURE()`) and dashboards |

## Prerequisites

- A Unity Catalog-enabled Azure Databricks workspace with serverless jobs and a SQL warehouse.
- A catalog you can create a schema, tables and models in (`USE CATALOG`, `CREATE SCHEMA`, or an existing schema with `CREATE TABLE` and `CREATE MODEL`).
- The [Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install), version 0.250 or later, signed in to your workspace.
- Network access from serverless compute to PyPI, or a private mirror, for `statsforecast`.

## Get running in dev (about 20 minutes)

1. **Configure.** In `databricks.yml`, under `targets.dev`, set `workspace.host`, `catalog` and `warehouse_id`.
2. **Sign in.** `databricks auth login --host https://<your-dev-workspace>.azuredatabricks.net`
3. **Deploy.** `databricks bundle deploy -t dev`
4. **Set up.** `databricks bundle run -t dev forecast_factory_setup`
   This creates synthetic sample tables and registers four example KPIs, so you can see the whole loop before using real data.
5. **Run.** `databricks bundle run -t dev forecast_factory`
   Expect about 5 to 8 minutes for four KPIs. Open the job run to see the fan-out and each child run's task graph.
6. **Look.** Open the MLflow experiment, the `fc_*` models in Catalog Explorer, and the Forecast Factory dashboard. Forecasts run forward from the latest data, so the forecast-vs-actual view fills in as actuals land (use `score_mode: holdout` to see it immediately).
7. **Run it again.** Each KPI trains a new challenger; it is promoted only if its backtest WAPE is lower than the champion's, and the decision and reason are tagged on the run.

## Add your own KPI

1. Create one SQL view per KPI that holds the business definition, owned by the KPI owner. Exclude the current incomplete period.
2. Copy `src/sql/add_kpi_template.sql`, fill in the values, and run it. Start with `active = false`, review the row with the owner, then set `active = true`.
3. Run the `Forecast Factory` job. The new KPI gets its own model, runs and forecasts. Nothing else changes.
4. When you no longer need the sample KPIs, set them to `active = false`, and set `create_sample_data` and `seed_sample_kpis` to `"false"`.

Registry columns that matter most:

| Column | Guidance |
|---|---|
| `grain` and `horizon` | Match the decision. Two-week staffing needs weekly or daily; annual budget needs monthly. `horizon` is also the holdout length. |
| `series_keys` | What one series is, for example `array('facility_id', 'specialty')`. More keys means more, smaller series, which are harder to forecast. |
| `aggregation` | `sum` and `count` treat missing periods as zero; `avg` (rates) interpolates. |
| `model_family` | `AutoETS` is a good default; `SeasonalNaive` is a strong baseline for noisy series; `AutoARIMA` is also available. Adding a family is a one-time code change in `01_train.py`. |

## Settings (`databricks.yml` variables)

| Variable | Default | Meaning |
|---|---|---|
| `catalog`, `schema`, `table_prefix` | none, `forecast_factory`, `fc_` | Where everything is created. The dashboard expects the default prefix. |
| `warehouse_id` | none | SQL warehouse for the dashboard |
| `fanout_concurrency` | `5` | KPIs that train at the same time |
| `score_mode` | `forward` | `forward`: the champion refits on the full history and forecasts the next horizon every run. `holdout`: forecast the evaluated window instead, so scored-vs-actual is available immediately (useful for demos). |
| `create_sample_data`, `seed_sample_kpis` | `"true"` in dev, `"false"` in staging and prod | Synthetic sample tables and example KPIs |
| `schedule_pause_status` | `PAUSED` (`UNPAUSED` in prod) | Weekly schedule, Mondays 6:00 America/Chicago |
| `service_principal_application_id` | none | Identity that runs staging and prod |

## Staging and prod: deploy code, not models

Because models train on sensitive data, the code moves between environments and models train where the data lives. Nothing trained on production data leaves production.

1. Create a service principal for each of staging and prod, and grant it the catalog privileges above plus permission to use serverless jobs.
2. In `databricks.yml`, set `workspace.host`, `catalog` and `warehouse_id` for `staging` and `prod`.
3. In GitHub, create `staging` and `prod` environments. In each, add the variable `DATABRICKS_HOST` and the secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` (an OAuth secret for that service principal). Add required reviewers to `prod`.
4. The workflows skip themselves until `DATABRICKS_HOST` is set, so nothing runs before you configure them. Pull requests run `validate.yml`. Merging to `main` deploys to staging and runs setup. Run `deploy.yml` manually with `target = prod` to promote.

## Monitoring and retraining

- **Accuracy:** `fc_forecast_accuracy` and the `fc_mv_accuracy` metric view hold backtest, holdout and scored-vs-actual WAPE per KPI and model version. The job rebuilds them after every run.
- **Data profiling:** In Catalog Explorer, open `fc_forecast_accuracy` (and your KPI source views), go to the **Quality** tab and turn on data profiling to track drift, gaps and volume shifts.
- **Alert:** Use `src/sql/accuracy_alert.sql` as a Databricks SQL alert on WAPE drift.
- **Retrain triggers:** Keep the weekly schedule, or add a table-update trigger on your source tables so the job runs when data changes. Either way, the promotion gate decides whether a new model replaces the champion.
- **Human approval (optional):** MLflow deployment jobs can require an approval before promotion. They are in Public Preview.

## Power BI

See `docs/POWERBI.md`. In short: connect Power BI to the SQL warehouse, query the metric views with `MEASURE()` in DirectQuery (or publish to Power BI from Catalog Explorer), and add a refresh step after scoring.

## Things to know

- WAPE (sum of absolute errors over sum of actuals) is the promotion metric because it is stable when values are small. MAPE is logged for reference.
- The comparison is always like for like. When new data moves the training cutoff, the champion is re-scored on the challenger's exact backtest windows before the two are compared. The decision and reason are tagged on the run.
- Some KPIs are hard to forecast at a fine grain, for example a noisy no-show rate. Every KPI gets the same backtest, so this shows up early. Aggregate the series or choose a different target before changing the model.
- Run times are set by the slowest KPI because KPIs run in parallel. Adding KPIs mostly adds cost, not time.

## Learn more

- [Declarative Automation Bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- [Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/): [for each tasks](https://learn.microsoft.com/en-us/azure/databricks/jobs/for-each), [condition tasks](https://learn.microsoft.com/en-us/azure/databricks/jobs/if-else), [triggers](https://learn.microsoft.com/en-us/azure/databricks/jobs/triggers)
- [Models in Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/manage-model-lifecycle/)
- [MLOps workflows and deploy code](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/mlops/mlops-workflow)
- [Data profiling](https://learn.microsoft.com/en-us/azure/databricks/data-quality-monitoring/)
- [Metric views](https://learn.microsoft.com/en-us/azure/databricks/metric-views/)
- [ai_forecast](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_forecast) and [AutoML forecasting](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/automl/forecasting)
- [Many-models forecasting solution accelerator](https://github.com/databricks-industry-solutions/many-model-forecasting)
