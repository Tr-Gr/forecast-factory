# Databricks notebook source
# Forecast Factory: 00_sample_data (optional, part of the setup job)
# Generates small SYNTHETIC source tables so the framework runs end to end before you point it at real KPIs.
# Nothing here reads real data. Skip this task (or set create_sample_data = false) once your own KPIs are registered.
#
# Tables created in <catalog>.<schema>:
#   <prefix>sample_clinic_throughput_daily   facility_id, visit_date, completed_visits          (daily, 4 years)
#   <prefix>sample_specialty_access_monthly  facility_id, specialty, visit_type, service_month,
#                                            appointments, no_show_pct                         (monthly, 4 years)
#   <prefix>sample_referrals                 referral_id, specialty, referral_date              (one row per referral)

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "forecast_factory")
dbutils.widgets.text("table_prefix", "fc_")
dbutils.widgets.text("create_sample_data", "true")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
assert CAT and SCH, "Set the catalog and schema parameters"
if dbutils.widgets.get("create_sample_data").strip().lower() != "true":
    dbutils.notebook.exit("create_sample_data is false; skipped")

# COMMAND ----------
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)
today = pd.Timestamp.today().normalize()
last_day = today - pd.Timedelta(days=today.weekday() + 1)                 # last complete week ends on Sunday
last_month = (today.replace(day=1) - pd.Timedelta(days=1)).replace(day=1)  # last complete month
start = (last_month - pd.DateOffset(years=4)).replace(day=1)

facilities = [f"FAC{i:02d}" for i in range(1, 7)]
specialties = ["Cardiology", "Dermatology", "Family Medicine", "Orthopedics", "Pediatrics"]
visit_types = ["New patient", "Return"]

# daily completed visits: weekly pattern + yearly seasonality + trend + noise, by facility
days = pd.date_range(start, last_day, freq="D")
dow = np.array([1.0, 1.05, 1.0, 0.98, 0.9, 0.25, 0.1])
rows = []
for i, f in enumerate(facilities):
    base = 120 + 35 * i
    t = np.arange(len(days))
    season = 1 + 0.12 * np.cos(2 * np.pi * (days.dayofyear - 20) / 365.25)
    trend = 1 + 0.0002 * t
    lam = base * dow[days.dayofweek] * season * trend
    rows.append(pd.DataFrame({"facility_id": f, "visit_date": days.date, "completed_visits": rng.poisson(lam)}))
clinic = pd.concat(rows, ignore_index=True)

# monthly appointments and no-show % by facility x specialty x visit type
months = pd.date_range(start, last_month, freq="MS")
rows = []
for i, f in enumerate(facilities):
    for j, s in enumerate(specialties):
        for k, v in enumerate(visit_types):
            base = (300 + 60 * i) * (0.6 + 0.2 * j) * (0.45 if k == 0 else 1.0)
            season = 1 + 0.1 * np.cos(2 * np.pi * (months.month - 1) / 12)
            appts = rng.poisson(base * season * (1 + 0.004 * np.arange(len(months))))
            noshow = np.clip(rng.normal(0.07 + 0.01 * k + 0.004 * j, 0.015, len(months)), 0.01, 0.25)
            rows.append(pd.DataFrame({"facility_id": f, "specialty": s, "visit_type": v, "service_month": months.date,
                                      "appointments": appts, "no_show_pct": np.round(noshow * 100, 2)}))
access = pd.concat(rows, ignore_index=True)

# referrals: one row per referral, by specialty
rows = []
for j, s in enumerate(specialties):
    lam = (14 + 4 * j) * (1 + 0.15 * np.sin(2 * np.pi * days.dayofyear / 365.25)) * np.where(days.dayofweek < 5, 1.0, 0.15)
    n = rng.poisson(lam)
    d = np.repeat(days.date, n)
    d = d[d < (last_month + pd.DateOffset(months=1)).date()]  # whole months only: the monthly KPI must not see a partial month
    rows.append(pd.DataFrame({"specialty": s, "referral_date": d}))
refs = pd.concat(rows, ignore_index=True)
refs.insert(0, "referral_id", [f"R{i:08d}" for i in range(len(refs))])

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
for name, df, comment in [
    ("sample_clinic_throughput_daily", clinic, "SYNTHETIC sample data for Forecast Factory: daily completed visits by facility"),
    ("sample_specialty_access_monthly", access, "SYNTHETIC sample data for Forecast Factory: monthly appointments and no-show % by facility, specialty and visit type"),
    ("sample_referrals", refs, "SYNTHETIC sample data for Forecast Factory: one row per referral"),
]:
    full = f"{CAT}.{SCH}.{P}{name}"
    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    spark.sql(f"COMMENT ON TABLE {full} IS '{comment}'")
    print(f"{full}: {len(df):,} rows")
