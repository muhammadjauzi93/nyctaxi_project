# NYC Taxi Lakehouse Pipeline

An end-to-end data engineering project on **Databricks** that ingests
[NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
processes them through a **Medallion architecture** (Landing → Bronze → Silver → Gold),
and exports enriched data to Azure Data Lake Storage as JSON.

The project is built with PySpark, Delta Lake and Unity Catalog, and is split into
a **one-off initial load** (historical backfill) and a **monthly incremental pipeline**
designed to run as a Databricks Job.

---

## Architecture

```
NYC TLC CloudFront (yellow_tripdata_<yyyy-MM>.parquet, taxi_zone_lookup.csv)
        │
        ▼
00_landing   Volume: /Volumes/nyctaxi/00_landing/data_sources
        │
        ▼
01_bronze    yellow_trips_raw          raw trips + processed_timestamp
        │
        ▼
02_silver    yellow_trips_cleansed     decoded and renamed columns
             taxi_zone_lookup          SCD Type 2 dimension
             yellow_trips_enriched     trips joined to pickup/drop-off zones
        │
        ├──► 03_gold     daily_trip_summary     daily KPIs
        │
        └──► 04_export   yellow_trips_export    JSON on ADLS Gen2, partitioned by vendor/year_month
```

| Layer | Table / Location | Description |
|---|---|---|
| **00_landing** | `/Volumes/nyctaxi/00_landing/data_sources/` | Raw files downloaded from the TLC CloudFront endpoint |
| **01_bronze** | `nyctaxi.01_bronze.yellow_trips_raw` | Raw Parquet data plus a `processed_timestamp` column |
| **02_silver** | `nyctaxi.02_silver.yellow_trips_cleansed` | Coded values decoded (vendor, rate type, payment type), columns renamed, trip duration calculated |
| **02_silver** | `nyctaxi.02_silver.taxi_zone_lookup` | Taxi zone dimension maintained as a **Slowly Changing Dimension (Type 2)** |
| **02_silver** | `nyctaxi.02_silver.yellow_trips_enriched` | Trips joined to the zone lookup for pickup/drop-off borough and zone |
| **03_gold** | `nyctaxi.03_gold.daily_trip_summary` | Daily aggregates: trip count, avg passengers, avg distance, avg/min/max fare, total revenue |
| **04_export** | `nyctaxi.04_export.yellow_trips_export` | External table in **JSON** on ADLS Gen2, partitioned by `vendor` and `year_month` |

---

## Project Structure

```
nyctaxi_project/
├── modules/                        # Reusable Python modules shared across notebooks
│   ├── data_loader/
│   │   └── file_downloader.py      # download_file(): download a file from a URL into a volume
│   ├── transformations/
│   │   └── metadata.py             # add_ingestion_date(): adds processed_timestamp column
│   └── utils/
│       └── date_utils.py           # get_target_yyyymm(), get_month_start_n_months_ago()
│
├── one_off/                        # Run once to set up the environment and backfill history
│   ├── creating_catalogs_schemas_volume.py
│   └── initial_load/notebooks/
│       ├── 00_landing/             # backfill_historical_yellow_trips, load_taxi_zone_lookup
│       ├── 01_bronze/              # yellow_trips_raw (overwrite, mergeSchema)
│       ├── 02_silver/              # taxi_zone_lookup, yellow_trips_cleansed, yellow_trips_enriched
│       ├── 03_gold/                # daily_trip_summary
│       └── 04_export/              # yellow_trips_export (full JSON export)
│
├── transformations/notebooks/      # Monthly incremental pipeline (Databricks Job tasks)
│   ├── 00_landing/                 # ingest_yellow_trips, ingest_lookup
│   ├── 01_bronze/                  # yellow_trips_raw
│   ├── 02_silver/                  # taxi_zone_lookup (SCD2 merge), yellow_trips_cleansed, yellow_trips_enriched
│   ├── 03_gold/                    # daily_trip_summary
│   └── 04_export/                  # yellow_trips_export (incremental JSON export)
│
└── ad_hoc/                         # Exploratory analysis and data validation
    ├── yellow_taxi_eda.py          # Business questions (top vendor, busiest borough, common journeys)
    └── yellow_taxi_eda2.py         # Record counts per month across every layer
```

---

## Pipelines

### 1. One-off setup and initial load

Run these once, in order:

1. **`one_off/creating_catalogs_schemas_volume.py`** – creates the `nyctaxi` catalog,
   the `00_landing`, `01_bronze`, `02_silver` and `03_gold` schemas, and the
   `00_landing.data_sources` volume.
2. **`00_landing/backfill_historical_yellow_trips.py`** – downloads the monthly Parquet files
   for the configured `years_to_process` / `months_to_process`.
3. **`00_landing/load_taxi_zone_lookup.py`** – downloads `taxi_zone_lookup.csv`.
4. **`01_bronze/yellow_trips_raw.py`** – reads every landed month with `mergeSchema`
   (the schema differs between years) and overwrites the bronze table.
5. **`02_silver/taxi_zone_lookup.py`** – creates the zone dimension with
   `effective_date` / `end_date` columns.
6. **`02_silver/yellow_trips_cleansed.py`** – filters out-of-range pickup dates and decodes/renames columns.
7. **`02_silver/yellow_trips_enriched.py`** – joins trips to pickup and drop-off zones.
8. **`03_gold/daily_trip_summary.py`** – builds the daily aggregate table.
9. **`04_export/yellow_trips_export.py`** – writes the full enriched dataset to the external JSON table.

### 2. Monthly incremental pipeline

The notebooks in `transformations/notebooks/` process **the month two months before the current month**
(TLC publishes trip data with a delay of about two months). The month is worked out with
`modules/utils/date_utils.py`, so no parameters are needed.

| Step | Notebook | Behaviour |
|---|---|---|
| Landing | `ingest_yellow_trips` | Downloads `yellow_tripdata_<yyyy-MM>.parquet`. If the file already exists or the download fails, it sets the job task value `continue_downstream = "no"` so that downstream tasks are skipped |
| Landing | `ingest_lookup` | Re-downloads the taxi zone lookup CSV and sets `continue_downstream` |
| Bronze | `yellow_trips_raw` | Appends the new month with a `processed_timestamp` |
| Silver | `taxi_zone_lookup` | **SCD Type 2 merge** in three passes: (1) close active rows whose borough/zone/service_zone changed, (2) insert new versions of changed keys, (3) insert brand-new keys |
| Silver | `yellow_trips_cleansed` | Filters bronze to the target month, decodes and appends |
| Silver | `yellow_trips_enriched` | Joins the new trips to zones and appends |
| Gold | `daily_trip_summary` | Aggregates the new month's daily metrics and appends |
| Export | `yellow_trips_export` | Appends the new month to the external JSON table on ADLS |

When you set this up as a Databricks Job, add an **If/else condition task** after each landing
task that checks `{{tasks.<landing_task>.values.continue_downstream}} == "yes"` before the
downstream tasks run.

---

## Shared Modules

| Function | Module | Purpose |
|---|---|---|
| `download_file(url, dir_path, local_path)` | `modules/data_loader/file_downloader.py` | Creates the target directory and streams a file from a URL into it |
| `add_ingestion_date(df)` | `modules/transformations/metadata.py` | Adds a `processed_timestamp` column set to the current timestamp |
| `get_target_yyyymm(months_ago)` | `modules/utils/date_utils.py` | Returns the `yyyy-MM` string for *n* months ago |
| `get_month_start_n_months_ago(months_ago)` | `modules/utils/date_utils.py` | Returns the first day of the month *n* months ago |

The notebooks import these modules by adding the project root (two directories up) to `sys.path`.
This works when the repo is cloned as a **Databricks Git folder**.

---

## Getting Started

### Prerequisites

- A Databricks workspace with **Unity Catalog** enabled
- An Azure Data Lake Storage Gen2 account, with an **external location** / storage credential
  set up for the export path
- A cluster or serverless compute running a Databricks Runtime that supports
  `timestamp_diff` (Spark 3.5+ / DBR 14+)

### Setup

1. Clone this repository into your workspace as a Git folder (**Workspace → Create → Git folder**).
2. Update the storage paths to match your environment:
   - Catalog managed location in `one_off/creating_catalogs_schemas_volume.py`
   - ADLS export path (`abfss://...`) in both `04_export/yellow_trips_export.py` notebooks
3. Create the export schema. The setup script does not create it yet:
   ```sql
   CREATE SCHEMA IF NOT EXISTS nyctaxi.04_export;
   ```
4. Set the months to backfill in `backfill_historical_yellow_trips.py` and the matching
   date filter in `one_off/.../02_silver/yellow_trips_cleansed.py`.
5. Run the one-off notebooks in the order listed above.
6. Create a monthly Databricks Job from the `transformations/notebooks/` tasks.

---

## Ad-hoc Analysis

- **`ad_hoc/yellow_taxi_eda.py`** answers business questions against the enriched table:
  which vendor earns the most revenue, which pickup borough is busiest, and the most
  common borough-to-borough journeys.
- **`ad_hoc/yellow_taxi_eda2.py`** is a data quality check that compares monthly record
  counts across the bronze, silver and gold layers.

---

## Tech Stack

- **Databricks** (notebooks, Jobs, Unity Catalog, Volumes)
- **Apache Spark / PySpark**
- **Delta Lake** (managed tables, `MERGE` for SCD Type 2)
- **Azure Data Lake Storage Gen2** (external JSON table)
- **Python** (`urllib`, `dateutil`)

## Data Source

NYC Taxi & Limousine Commission –
[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
