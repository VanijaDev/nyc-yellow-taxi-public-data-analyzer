# NYC Yellow Taxi Public Data Analyzer

> A production-style ELT data platform for analyzing 10+ years of NYC Yellow Taxi trip data to answer whether starting a taxi operation in New York City is financially viable.

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE?logo=apacheairflow)
![AWS S3](https://img.shields.io/badge/AWS_S3-Storage-FF9900?logo=amazons3)
![Snowflake](https://img.shields.io/badge/Snowflake-Warehouse-29B5E8?logo=snowflake)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)
![Status](https://img.shields.io/badge/Status-In_Development-yellow)
![CI](https://github.com/your-username/nyc-yellow-taxi-public-data-analyzer/actions/workflows/ci.yml/badge.svg)

---

## Table of Contents

- [Project Overview](#project-overview)
- [Target Users](#target-users)
- [Architecture Overview](#architecture-overview)
- [ELT Pipeline](#elt-pipeline)
- [Data Model](#data-model)
- [Business Analytics](#business-analytics)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [Roadmap](#roadmap)
- [Data Sources & Schema](#data-sources--schema)

---

## Project Overview

### Problem Statement

Prospective taxi fleet operators and ride-hailing entrepreneurs lack a consolidated, data-driven view of the NYC taxi market. Raw NYC TLC data exists publicly but is scattered across monthly Parquet files spanning years—requiring significant engineering effort to extract actionable insights.

### Solution

This platform automates the full journey from raw public data to interactive business intelligence:

1. **Ingests** monthly NYC TLC Yellow Taxi trip records via a scheduled Airflow ELT pipeline
2. **Transforms** raw data into clean, validated, enriched analytical datasets using Python and Pandas
3. **Loads** modeled fact and dimension tables into Snowflake for analytical queries
4. **Serves** business insights through an interactive Streamlit dashboard

The result is a self-updating analytics platform that answers concrete questions about route profitability, peak demand, fleet sizing, and fare trends—backed by years of historical trip data.

---

## Target Users

| User | Use Case |
|---|---|
| **Taxi fleet operators** | Evaluate NYC market entry and optimize existing operations |
| **Ride-hailing entrepreneurs** | Size the market opportunity before launching |
| **Operations managers** | Optimize scheduling, routing, and vehicle capacity decisions |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Orchestration Layer                       │
│                     Apache Airflow (daily DAG)                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
            ┌───────────────▼───────────────┐
            │         EXTRACT & LAND         │
            │  NYC TLC Public Parquet Files   │
            │  → AWS S3 Raw Bucket            │
            │  (s3://nyc-taxi-raw/YYYY/MM/)   │
            └───────────────┬───────────────┘
                            │
            ┌───────────────▼───────────────┐
            │           TRANSFORM            │
            │   Python + Pandas              │
            │   - Clean & validate           │
            │   - Enrich with derived fields │
            │   - Model into fact/dim tables  │
            └───────────────┬───────────────┘
                            │
            ┌───────────────▼───────────────┐
            │             LOAD               │
            │   Snowflake Analytical Warehouse│
            │   - fact_trips                  │
            │   - dim_locations               │
            │   - dim_time                    │
            │   - dim_vehicles                │
            └───────────────┬───────────────┘
                            │
            ┌───────────────▼───────────────┐
            │            SERVE               │
            │   Streamlit Dashboard           │
            │   - Route heatmaps              │
            │   - Revenue analytics           │
            │   - Trend charts                │
            └───────────────────────────────┘
```

### Backfill Strategy

The pipeline runs in two phases, managed by a pointer stored in Airflow metadata:

**Phase 1 — Historical backfill**
Starts from the most recent available month and works backwards, processing one month per daily DAG execution. When the source returns no data for the target month, the historical backfill is considered complete.

**Phase 2 — Ongoing refresh**
Once the backfill is complete, the pipeline switches to forward-looking mode. Each daily run checks whether a new month's data has become available. NYC TLC data is published monthly with approximately a **3-month lag** (e.g. in March 2026, the latest available data is from December 2025). The DAG checks for the next expected month and skips gracefully if it has not been published yet.

This approach:
- Keeps each run lightweight — one month processed per execution
- Builds a complete multi-year historical dataset progressively
- Transitions automatically from backfill to ongoing refresh without manual intervention
- Is idempotent — a failed run can be retried for only the affected month

---

## ELT Pipeline

### Stage 1 — Extract & Land (Raw Storage)

| Property | Detail |
|---|---|
| **Source** | NYC TLC Yellow Taxi monthly Parquet files (public HTTP endpoints) |
| **Action** | Download target month → store raw files unchanged |
| **Destination** | `s3://nyc-taxi-raw/{year}/{month}/` |
| **Key rule** | No transformations at ingest — preserve original schema |

### Stage 2 — Transform

| Property | Detail |
|---|---|
| **Input** | Raw Parquet files from S3 |
| **Tools** | Python 3 + Pandas (PyArrow as implicit Parquet engine) |
| **Output** | Cleaned, validated, enriched Pandas DataFrames |

**Transformation steps:**
- Remove outliers and handle missing values
- Validate business rules (pickup time < dropoff time, positive fares, valid location IDs)
- Derive computed fields: trip duration, revenue per mile, average speed
- Model data into relational fact and dimension tables

### Stage 3 — Load

| Property | Detail |
|---|---|
| **Input** | Transformed Pandas DataFrames |
| **Destination** | Snowflake analytical schema |
| **Method** | `snowflake-connector-python` with `pandas.DataFrame.to_sql()` |

### Stage 4 — Serve (Dashboard)

| Property | Detail |
|---|---|
| **Input** | Snowflake analytical tables |
| **Tool** | Streamlit |
| **Outputs** | Route heatmaps, revenue trends, capacity analytics, weather impact charts |

---

## Data Model

Star schema optimized for analytical queries:

```
                    ┌─────────────┐
                    │  dim_time   │
                    │─────────────│
                    │ time_id (PK)│
                    │ hour        │
                    │ day_of_week │
                    │ month       │
                    │ season      │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼───────┐    ┌───────────────┐
│ dim_locations│    │  fact_trips  │    │  dim_vehicles  │
│──────────────│    │──────────────│    │───────────────│
│ location_id  ├────│ trip_id (PK) ├────│ vehicle_id    │
│ zone_name    │    │ pickup_loc   │    │ rate_code     │
│ borough      │    │ dropoff_loc  │    │ capacity_type │
│ service_zone │    │ pickup_time  │    │ (sedan/SUV/van)│
└──────────────┘    │ dropoff_time │    └───────────────┘
                    │ passenger_ct │
                    │ trip_distance│
                    │ fare_amount  │
                    │ tip_amount   │
                    │ total_amount │
                    │ revenue_per_mile│
                    └──────────────┘
```

---

## Business Analytics

The platform is built to answer five core categories of business questions:

### Route & Zone Intelligence
- Most profitable pickup → dropoff routes by total revenue
- High-demand pickup zones with underserved dropoff areas
- Airport transfer profitability vs. standard trips
- Historical revenue trends per route (3-year view)

### Vehicle Capacity Optimization
- Revenue per vehicle type: sedan vs. SUV vs. van
- Load factor: average passengers per trip by time and zone
- Capacity utilization trends over time

### Peak Demand & Scheduling
- Most profitable hours by zone and day of week
- Rush hour vs. late night vs. weekend revenue patterns
- Multi-year peak hour shift analysis for driver scheduling

### External Factor Impact
- Weather correlation with trip volume and revenue (NOAA / Open-Meteo API)
- Day-of-week and seasonal patterns
- Holiday surge analysis

### Trip Economics
- Fare-per-mile and fare-per-minute benchmarks by zone
- Tip percentage analysis by fare amount, distance, and time of day
- Fare and tip trend analysis (monthly and annual)

---

## Tech Stack

| Layer | Technology | Justification |
|---|---|---|
| **Orchestration** | Apache Airflow | Industry-standard workflow orchestrator; native support for DAG-based scheduling and backfill logic |
| **Raw Storage** | AWS S3 | Scalable, cheap object storage; ideal for landing raw files before transformation |
| **Processing** | Python 3 + Pandas | Pandas is the de-facto standard for tabular data transformation. PyArrow is installed as a dependency and used by Pandas internally to read/write Parquet files — no PyArrow code is written directly |
| **Warehouse** | Snowflake | Columnar analytical warehouse optimized for OLAP queries; separates storage and compute |
| **Visualization** | Streamlit | Python-native dashboard framework; minimizes context-switching and integrates cleanly with Pandas/Snowflake |

---

## Project Structure

```
nyc-yellow-taxi-public-data-analyzer/
├── dags/                        # Airflow DAG definitions
│   └── taxi_etl_dag.py
├── pipeline/
│   ├── extract/                 # Download and land raw data
│   ├── transform/               # Pandas transformation logic
│   └── load/                    # Snowflake loader
├── models/                      # SQL DDL for Snowflake tables
├── dashboard/                   # Streamlit app
├── tests/                       # Unit and integration tests
├── docs/                        # Reference documents
│   └── data_dictionary_trip_records_yellow.pdf
├── .gitignore
└── README.md
```

> **Note:** Project is in early development. The directory structure above reflects the planned layout.

---

## Getting Started

> **Prerequisites:** Python 3.13, AWS account, Snowflake account, Airflow 3.x

```bash
# Clone the repository
git clone https://github.com/your-username/nyc-yellow-taxi-public-data-analyzer.git
cd nyc-yellow-taxi-public-data-analyzer

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your AWS credentials, Snowflake connection string, etc.

# Initialize Airflow
airflow db init
airflow dags trigger taxi_etl_dag
```

> **Note:** Full setup guide with environment configuration details coming soon.

---

## Design Decisions & Trade-offs

### ELT over ETL
Transform after landing raw data in S3. This preserves the original source data (making reruns safe and auditable) and decouples ingestion reliability from transformation complexity.

### Pandas over PySpark
For this dataset size (monthly Parquet files, ~1–5 GB each), Pandas on a single machine is sufficient and far simpler to develop and debug. PySpark would add operational overhead without meaningful performance gains at this scale. This decision will be revisited if processing requirements exceed single-machine capacity.

### Snowflake over self-hosted PostgreSQL
Snowflake's separation of storage and compute makes it cost-efficient for analytical workloads that run in bursts (dashboard queries) rather than continuously. A traditional RDBMS would require manual scaling and optimization for columnar analytical patterns.

### Monthly backfill cadence
Processing one month per daily DAG run keeps each execution lightweight and idempotent. It also provides natural checkpointing—if a run fails, only that month needs to be retried.

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full project roadmap and progress tracking.

---

## Data Sources & Schema

| Source | Description | Format | Access |
|---|---|---|---|
| [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Monthly Yellow Taxi trip records with pickup/dropoff locations, fares, tips, and timestamps | Parquet / CSV | Public, free |
| [Open-Meteo Historical API](https://open-meteo.com/) | Historical weather data for NYC (temperature, precipitation, wind) | JSON API | Public, free |

### Source Schema — NYC TLC Yellow Taxi Trip Records

> Full data dictionary: [`docs/data_dictionary_trip_records_yellow.pdf`](docs/data_dictionary_trip_records_yellow.pdf)

Types and nullability are taken from the actual Parquet file schema (verified via `DESCRIBE SELECT * FROM read_parquet(...)`). All columns are nullable.

| # | Field | Parquet Type | Description |
|---|---|---|---|
| 1 | `VendorID` | INTEGER | TPEP provider (1=Creative Mobile Technologies, 2=VeriFone) |
| 2 | `tpep_pickup_datetime` | TIMESTAMP | Meter engaged time |
| 3 | `tpep_dropoff_datetime` | TIMESTAMP | Meter disengaged time |
| 4 | `passenger_count` | BIGINT | Passengers in vehicle — **driver-entered, unreliable** |
| 5 | `trip_distance` | DOUBLE | Trip distance in miles (taximeter) |
| 6 | `RatecodeID` | BIGINT | Rate type: 1=Standard, **2=JFK**, **3=Newark**, 4=Nassau/Westchester, 5=Negotiated, 6=Group |
| 7 | `store_and_fwd_flag` | VARCHAR | Y = record stored in vehicle before upload (connectivity issue) |
| 8 | `PULocationID` | INTEGER | TLC Taxi Zone ID for pickup location |
| 9 | `DOLocationID` | INTEGER | TLC Taxi Zone ID for dropoff location |
| 10 | `payment_type` | BIGINT | 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided |
| 11 | `fare_amount` | DOUBLE | Time-and-distance fare from meter |
| 12 | `extra` | DOUBLE | Rush hour ($1) and overnight ($0.50) surcharges |
| 13 | `mta_tax` | DOUBLE | $0.50 MTA tax (automatically applied) |
| 14 | `tip_amount` | DOUBLE | Tip — **credit card only. Cash tips are always $0.00 in this field** |
| 15 | `tolls_amount` | DOUBLE | Total tolls paid |
| 16 | `improvement_surcharge` | DOUBLE | $0.30 surcharge applied since 2015 |
| 17 | `total_amount` | DOUBLE | Total charged to passenger (excludes cash tips) |
| 18 | `congestion_surcharge` | DOUBLE | NYC taxi surcharge added **Feb 2019** — `NULL` in older records |
| 19 | `Airport_fee` | DOUBLE | JFK/LaGuardia pickup/dropoff fee added **Jul 2022** — `NULL` in older records |
| 20 | `cbd_congestion_fee` | DOUBLE | Central Business District congestion pricing fee added **Jan 2025** — `NULL` in older records |

### Key Data Quality Caveats

These are known limitations in the source data that must be handled during transformation:

- **Tip underreporting:** `tip_amount` only captures credit card tips. Any tip analysis applies exclusively to credit card payments — cash-paying passengers' tips are never recorded. Filter by `payment_type = 1` when analyzing tips.
- **Unreliable passenger count:** `passenger_count` is manually entered by the driver and frequently incorrect (0s, nulls, or inflated values). Treat aggregate passenger metrics with caution.
- **Airport trip identification:** Use `RatecodeID` (2=JFK, 3=Newark) to identify airport trips — do not rely on zone proximity logic. Note the actual column name is `RatecodeID` (lowercase 'c'), not `RateCodeID` as written in the 2015 data dictionary.
- **Schema evolution — location fields:** Data files **before ~2016** use raw `Pickup_longitude`/`Pickup_latitude` coordinates instead of `PULocationID`/`DOLocationID` zone IDs. The pipeline must handle both formats when backfilling historical data.
- **Schema evolution — fee fields:** `congestion_surcharge` (added Feb 2019), `Airport_fee` (added Jul 2022), and `cbd_congestion_fee` (added Jan 2025) are `NULL` in all records predating those policy changes. Do not treat `NULL` as $0 — handle these as truly absent values.
- **All columns are nullable:** Every field in the Parquet schema has `null=YES`. Null-checks are required across the board during validation — do not assume any field is guaranteed to be populated.
- **Voided and disputed trips:** Filter out `payment_type` values 4 (Dispute), 5 (Unknown), and 6 (Voided) for revenue analysis.
