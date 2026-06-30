# Implementation Approach

## Implementation Approach: Hand-Authored BigQuery DDL

### File Organization

3 SQL files organized by dataset, placed at:

```
bigquery/ddl/
├── staging.sql    -- 45 CREATE TABLE statements
├── ods.sql        -- 30 CREATE TABLE statements
└── dm.sql         -- 21 CREATE TABLE + 4 CREATE MATERIALIZED VIEW statements
```

Source of truth for column names, types, and order: `manifests/tables.yaml` from the source repo. Each DDL statement is hand-authored against this manifest applying the locked type-mapping, partition, clustering, and MV conversion rules.

### DDL Conventions

- **Statement form**: `CREATE OR REPLACE TABLE project_id.dataset.table_name (...)` — fully qualified with BigQuery project ID for idempotent re-runs.
- **Column ordering**: Matches source column ordinal position from `tables.yaml` / `hive/ddl/*.hql`. Partition columns appear as regular columns in the column list with a separate `PARTITION BY` clause after.
- **No Hive-isms**: Strip all `EXTERNAL`, `STORED AS`, `ROW FORMAT`, `SERDE`, `LOCATION`, `TBLPROPERTIES`, `CLUSTERED BY...INTO N BUCKETS` clauses.

### Staging Tables (45 in `staging.sql`)

- 27 Sqoop mirror tables + 8 delta feed tables + 10 file feed tables
- All columns keep their original staging-layer types (BIGINT epochs remain INT64 — no timestamp conversion at this layer)
- **Partition**: `load_date DATE` (27 sqoop mirrors), `extract_ts DATE` (8 delta feeds), `feed_date DATE` (10 file feeds) — all converted from STRING to DATE
- **Multi-column partition demotion**: `stg_wfm_schedule` has `(load_date, site_code)` → partition on `load_date DATE`, cluster on `site_code`. 10 file feed tables have `(client_code, feed_date)` → partition on `feed_date DATE`, cluster on `client_code`.
- **Partition expiration**: All 45 tables include `OPTIONS(partition_expiration_days=90)`.
- **Complex types** (4 columns on 3 tables):
  - `stg_file_qa_forms.sections` → `ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>`
  - `stg_file_chat_transcripts.messages` → `ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>`
  - `stg_file_chat_transcripts.metadata` → `ARRAY<STRUCT<key STRING, value STRING>>` (MAP conversion)
  - `stg_file_speech_analytics.keywords` → `ARRAY<STRING>`

### ODS Tables (30 in `ods.sql`)

- 15 cleanse tables + 8 delta-merge tables + 3 SCD-2 tables + 4 ACID tables
- All epoch columns already converted to TIMESTAMP at this layer (matches source)
- **Partition types**:
  - `snapshot_date DATE` (8 cleanse tables) — converted from STRING
  - `event_date DATE` / `call_date DATE` / `sched_date DATE` (7 cleanse tables) — converted from STRING
  - `work_month DATE` / `period_month DATE` / `swap_month DATE` / `event_month DATE` (5 delta-merge tables) — converted from STRING to DATE, MONTH granularity
  - `eff_from_ts TIMESTAMP` (3 SCD-2 tables) — YEAR granularity, replaces dropped `eff_from_year INT`
  - 4 ACID tables: **unpartitioned**, clustered on PK (`client_id`, `agent_id`, `ticket_id`, `invoice_id`)
- **3 intentional column drops**: `eff_from_year INT` removed from `ods_agent_scd2`, `ods_agent_skill_scd2`, `ods_agent_assignment_scd2` — partitioning moves to existing `eff_from_ts`.

### DM Tables and MVs (25 in `dm.sql`)

- 9 dimension tables (unpartitioned, no clustering)
- 9 fact tables (partitioned, selectively clustered)
- 3 aggregate tables kept as `CREATE TABLE` (use GROUPING SETS / ROLLUP / multi-table joins)
- 4 aggregate tables as `CREATE MATERIALIZED VIEW`

**Fact partition renames** (16 total):
- 11x `date_key INT` → `event_date DATE` (DAY granularity)
- 4x `period_month STRING` → `period_month_date DATE` (MONTH granularity)
- 1x `week_start_key INT` → `week_start_date DATE` (DAY granularity)

**Clustering** (14 explicitly-clustered objects per locked matrix):
`fact_interaction(channel, agent_sk, client_sk)`, `fact_agent_activity(agent_sk, state_code)`, `fact_queue_interval(queue_sk)`, `fact_csat_survey(program_sk, agent_sk)`, `fact_billing_line(client_sk, program_sk)`, `fact_adherence_daily(agent_sk)`, `fact_ticket(program_sk, status)`, `agg_agent_daily MV(agent_sk, site_code)`, `agg_queue_hourly MV(queue_sk, hour_of_day)`, `ods_agent_scd2(agent_id)`, `ods_client_acid(client_id)`, `ods_agent_acid(agent_id)`, `ods_ticket_acid(ticket_id)`, `ods_invoice_acid(invoice_id)`.

Plus 12 demoted multi-column partition columns become cluster columns (1 `stg_wfm_schedule.site_code`, 10 file-feed `client_code`, 1 `fact_interaction.channel`).

### Materialized View Approach (4 MVs)

Simplified core aggregation queries that comply with BigQuery MV constraints (no LEFT JOIN, no subqueries in FROM, no MV-on-MV):

| MV | Base Table(s) | Core Aggregation |
|---|---|---|
| `agg_agent_daily` | `fact_agent_activity` | `GROUP BY agent_sk, event_date` — SUM state_seconds by state_code via CASE |
| `agg_agent_weekly` | `fact_agent_activity` | `GROUP BY agent_sk, DATE_TRUNC(event_date, ISOWEEK)` — weekly rollup |
| `agg_queue_hourly` | `fact_queue_interval` | `GROUP BY queue_sk, event_date, EXTRACT(HOUR FROM interval_start_ts)` |
| `agg_site_daily` | `fact_agent_activity` INNER JOIN `dim_site` | Site-level aggregation (can't chain MV-on-MV) |

Supplementary scheduled SQL in the Transform story fills any gaps (e.g., forecast variance, occupancy calculation).

### Column Descriptions

- All 68 epoch/date columns carry BigQuery `OPTIONS(description='...')` documenting the original encoding.
- The 2 lying columns (`stg_fin_invoice.issued_ts_sec`, `stg_fin_invoice.due_ts_sec`) carry: `'column name says seconds, values are milliseconds — use TIMESTAMP_MILLIS, not TIMESTAMP_SECONDS'`.

