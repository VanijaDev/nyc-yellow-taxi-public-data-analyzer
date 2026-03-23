# Roadmap

Progress tracking for the NYC Yellow Taxi Public Data Analyzer.

**Legend:** `[ ]` not started · `[~]` in progress · `[x]` complete

---

## Phase 1 — Foundation

Project scaffolding, documentation, and CI infrastructure.

- [x] Define project goals, business questions, and target users
- [x] Design ELT architecture (S3 → Pandas → Snowflake → Streamlit)
- [x] Design star schema data model (fact_trips + dim tables)
- [x] Document source schema and data quality caveats
- [x] Write README
- [x] Set up GitHub Actions CI (lint, format, type check, tests)
- [x] Set up repo directory structure (pipeline/extract/, tests/)
- [x] Create pyproject.toml with runtime dependencies as they are added
- [x] Create .env.example

---

## Phase 2 — Extract (Python script)

Plain Python script — no Airflow yet. Goal: reliably download a raw Parquet file from the TLC source.

- [x] Implement extract function: download monthly Parquet from TLC public endpoint
- [x] Handle missing month: return a clear signal when the source has no data for a target month
- [x] Run manually and confirm the file downloads correctly
- [ ] **Tests:** correct URL construction, HTTP error handling, missing-month signal

---

## Phase 3 — Land (Python script)

Goal: upload the raw downloaded file to S3 unchanged.

- [ ] Implement S3 land function: upload raw file to `s3://nyc-taxi-raw/{year}/{month}/`
- [ ] Verify file appears in S3 with correct path and size
- [ ] **Tests:** correct S3 key construction, file uploaded unchanged, error on invalid bucket/credentials

---

## Phase 4 — Transform (Python script)

Goal: take a raw Parquet from S3, produce clean fact/dim DataFrames.

- [ ] Handle schema evolution: detect pre-2016 (lat/lng) vs modern (zone ID) format
- [ ] Implement null handling for all 20 fields
- [ ] Implement business rule validation (pickup < dropoff, positive fares, valid location IDs)
- [ ] Implement outlier removal
- [ ] Handle nullable fee fields correctly (congestion_surcharge, Airport_fee, cbd_congestion_fee)
- [ ] Filter invalid payment types (Dispute=4, Unknown=5, Voided=6)
- [ ] Derive computed fields: trip_duration, revenue_per_mile, average_speed
- [ ] Model into fact_trips and dimension tables
- [ ] Run manually and inspect output DataFrames
- [ ] **Tests:** each validation rule, each derived field, null handling, invalid row filtering — use a small Parquet fixture

---

## Phase 5 — Load (Python script)

Goal: load transformed DataFrames into Snowflake.

- [ ] Write Snowflake DDL for all tables (fact_trips, dim_locations, dim_time, dim_vehicles)
- [ ] Implement Snowflake loader (snowflake-connector-python)
- [ ] Implement idempotent upsert logic (no duplicate loads on retry)
- [ ] Run manually and confirm rows appear in Snowflake
- [ ] **Tests:** idempotency (running twice produces no duplicates), schema matches DDL, row count matches input

---

## Phase 6 — End-to-End Pipeline Test

Run the full extract → land → transform → load chain manually for several months. Confirm data integrity before adding orchestration.

- [ ] Run pipeline for 2–3 months of data end-to-end
- [ ] Verify row counts, schema, and spot-check values in Snowflake
- [ ] Add CI coverage reporting across all test suites

---

## Phase 7 — Airflow DAG

Only added here once the pipeline logic is proven. The DAG wraps the existing Python functions — no logic lives in the DAG itself.

- [ ] Set up Airflow locally (Docker or standalone)
- [ ] Implement DAG skeleton: wire extract → transform → load tasks
- [ ] Implement backfill pointer logic (Airflow Variable or metadata table)
- [ ] Implement Phase 1 → Phase 2 transition (backfill complete → ongoing refresh)
- [ ] Handle 3-month lag: skip gracefully if new month not yet published
- [ ] Test DAG manually via Airflow UI for a single month
- [ ] Confirm full backfill run completes without errors

---

## Phase 8 — Dashboard v1

First Streamlit dashboard covering the core business questions.

- [ ] Route profitability heatmap (pickup → dropoff by revenue)
- [ ] Peak demand by hour and day of week
- [ ] Airport vs. standard trip comparison

---

## Phase 9 — Weather Integration

- [ ] Integrate Open-Meteo historical API for NYC weather data
- [ ] Join weather data to fact_trips by date
- [ ] Add weather impact analysis to dashboard

---

## Phase 10 — Dashboard v2

Full analytics suite.

- [ ] Fare and tip trend analysis (monthly/annual)
- [ ] Vehicle capacity utilization analytics
- [ ] Weather correlation charts
- [ ] Executive summary view

---

## Phase 11 — Production Hardening

- [ ] CI/CD: auto-deploy dashboard on merge to main
- [ ] Infrastructure as Code (Terraform for S3 buckets, IAM roles)
- [ ] Alerting on DAG failure (email or Slack)
- [ ] dbt integration: migrate Snowflake-side transformations to SQL models

---

## Deferred / Under Consideration

- Migrate Pandas transformation to dbt models (load raw staging → transform in Snowflake)
- Add green taxi and FHV data sources
- Add competitor analysis (Uber/Lyft public data)
