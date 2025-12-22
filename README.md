# MAANG Stock Data Pipeline

An Airflow-orchestrated ETL pipeline that ingests daily stock data from the Polygon (Massive) API, validates it through a comprehensive data quality framework, and produces both daily partitioned tables and 7-day rolling cumulative metrics in Databricks using Apache Iceberg.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│ Polygon API │ ──▶ │   Staging   │ ──▶ │  DQ Checks   │ ──▶ │   Production    │
│  (MAANG)    │     │   Table     │     │  (6 checks)  │     │     Table       │
└─────────────┘     └─────────────┘     └──────────────┘     └────────┬────────┘
                                                                      │
                                                                      ▼
                                                             ┌─────────────────┐
                                                             │   Cumulative    │
                                                             │  (7-day roll)   │
                                                             └─────────────────┘
```

## Key Engineering Decisions

### Staging/Production Pattern
Data lands in an ephemeral staging table before promotion to production. This isolates raw ingestion from validated data and enables atomic swaps if validation fails—production never sees bad data.

### Idempotency
Each run clears existing data for its execution date before inserting. Re-runs are safe and produce identical results, critical for backfills and recovery scenarios.

### Iceberg Tables
Using Apache Iceberg provides ACID transactions, time travel, and efficient partition pruning on the `date` column. The format also enables schema evolution without table rewrites.

### Data Quality Gates
Six validation checks run against staging before promotion:

| Check | Validates |
|-------|-----------|
| Missing stocks | All 5 MAANG tickers present |
| NULL prices | No missing OHLC values |
| Price ranges | `high >= low`, OHLC within bounds |
| Volume | No zero or negative volume |
| Date mismatch | All records match execution date |
| Ticker format | Only expected symbols present |

### Cumulative Metrics
A separate table stores 7-day rolling windows as arrays, plus pre-computed aggregates (average volume, price volatility). This supports time-series analysis without expensive window recalculations at query time.

## DAG Structure

```
create_schema
      │
      ├──▶ create_staging_table
      ├──▶ create_prod_table
      └──▶ create_cumulative_table
                  │
                  ▼
         load_to_staging
                  │
                  ▼
            run_dq_check
                  │
                  ▼
             clear_step
                  │
                  ▼
      exchange_data_from_staging
              │
              ├──▶ drop_staging_table
              │
              └──▶ clear_cumulative ──▶ cumulate_step
```

## Tech Stack

- **Orchestration**: Apache Airflow 3.x
- **Compute**: Databricks SQL Warehouse
- **Storage**: Apache Iceberg on Unity Catalog
- **Data Source**: Polygon.io (Massive) REST API
- **Language**: Python 3.11+

## Table Schemas

### `daily_stock_prices` (Production)
```sql
ticker              STRING
date                DATE          -- partition key
open                DECIMAL(10,2)
high                DECIMAL(10,2)
low                 DECIMAL(10,2)
close               DECIMAL(10,2)
volume              BIGINT
vwap                DECIMAL(10,2)
transactions        INTEGER
insertion_timestamp TIMESTAMP
```

### `daily_stock_prices_cumulative`
```sql
ticker              STRING
date                DATE          -- partition key
last_7_days_open    ARRAY<DECIMAL(10,2)>
last_7_days_high    ARRAY<DECIMAL(10,2)>
last_7_days_low     ARRAY<DECIMAL(10,2)>
last_7_days_close   ARRAY<DECIMAL(10,2)>
last_7_days_volume  ARRAY<BIGINT>
avg_7_day_volume    DECIMAL(15,2)
volatility_7_day    DECIMAL(10,4)
updated_at          TIMESTAMP
```

## Setup

### Environment Variables
```bash
export POLYGON_TOKEN="polygon_api_key"
export DATABRICKS_HOST="workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/warehouse_id"
export DATABRICKS_TOKEN="databricks_pat"
```

### Dependencies
```
apache-airflow>=3.0.0
databricks-sql-connector
polygon (massive)-api-client
```

## Usage

The DAG runs daily and processes the previous complete trading day. Airflow's execution model means a run triggered on January 2nd processes January 1st's data (`ds = '2025-01-01'`).

To backfill historical data:
```bash
airflow dags backfill stock_dag --start-date 2025-01-01 --end-date 2025-01-15
```

## Future Improvements

- Add alerting on DQ check failures (Slack/email integration)
- Parameterize stock list for easy expansion beyond MAANG
- Add retry logic with exponential backoff for API calls
- Implement SLA monitoring for pipeline latency
