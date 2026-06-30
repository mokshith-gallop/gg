# Locked Decisions for Story 9db8d0e1-ca40-406c-8d8d-f2e01986a513

## Implementation Approach
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


## Data Mapping
## Data Mapping: Hive → BigQuery (916 source → 913 target columns)

### Type Mapping Rules

| Hive Type | BigQuery Type | Count | Notes |
|---|---|---|---|
| `BIGINT` | `INT64` | ~350 cols | Includes all epoch columns, surrogate keys, PKs |
| `INT` | `INT64` | ~80 cols | Including `date_key INT` partition columns before rename |
| `SMALLINT` | `INT64` | 0 | Not used in this estate |
| `STRING` | `STRING` | ~380 cols | Direct mapping |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | 52 cols | Precision ≤14, scale ≤4 across estate. Examples: `DECIMAL(12,4)`→`NUMERIC(12,4)`, `DECIMAL(14,2)`→`NUMERIC(14,2)`, `DECIMAL(5,2)`→`NUMERIC(5,2)`, `DECIMAL(8,2)`→`NUMERIC(8,2)`, `DECIMAL(7,2)`→`NUMERIC(7,2)`, `DECIMAL(10,4)`→`NUMERIC(10,4)` |
| `DOUBLE` | `FLOAT64` | 2 cols | `stg_file_speech_analytics.sentiment_score`, `.silence_pct` |
| `BOOLEAN` | `BOOL` | ~40 cols | Direct mapping |
| `TIMESTAMP` | `TIMESTAMP` | ~50 cols | ODS/DM layer only (staging uses BIGINT epochs) |
| `DATE` | `DATE` | 0 source → 87 target | New partition columns converted from INT/STRING |

### Complex / Nested Type Mapping (4 columns)

| Source Column | Hive Type | BigQuery Type |
|---|---|---|
| `stg_file_qa_forms.sections` | `ARRAY<STRUCT<section_code:STRING, max_points:INT, scored_points:INT>>` | `ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>` |
| `stg_file_chat_transcripts.messages` | `ARRAY<STRUCT<sender:STRING, ts_ms:BIGINT, text:STRING>>` | `ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>` |
| `stg_file_chat_transcripts.metadata` | `MAP<STRING,STRING>` | `ARRAY<STRUCT<key STRING, value STRING>>` |
| `stg_file_speech_analytics.keywords` | `ARRAY<STRING>` | `ARRAY<STRING>` |

### Intentional Column Drops (3)

| Table | Dropped Column | Reason |
|---|---|---|
| `ods_agent_scd2` | `eff_from_year INT` | Partition column replaced by partitioning on existing `eff_from_ts TIMESTAMP` (YEAR granularity) |
| `ods_agent_skill_scd2` | `eff_from_year INT` | Same |
| `ods_agent_assignment_scd2` | `eff_from_year INT` | Same |

### Intentional Column Renames (16 partition columns)

| Tables | Source Column | Target Column | Type Change |
|---|---|---|---|
| `fact_interaction`, `fact_agent_activity`, `fact_queue_interval`, `fact_csat_survey`, `fact_qa_evaluation`, `fact_adherence_daily`, `fact_ticket`, `fact_ivr_path`, `agg_agent_daily`, `agg_queue_hourly`, `agg_site_daily` (11 tables) | `date_key INT` | `event_date DATE` | INT → DATE |
| `fact_billing_line`, `agg_program_monthly`, `agg_csat_rollup_monthly`, `agg_billing_monthly` (4 tables) | `period_month STRING` | `period_month_date DATE` | STRING → DATE |
| `agg_agent_weekly` (1 table) | `week_start_key INT` | `week_start_date DATE` | INT → DATE |

### Partition Mapping (87 partitioned objects)

| Source Partition | Target Partition | Granularity | Tables |
|---|---|---|---|
| `load_date STRING` | `load_date DATE` | DAY | 27 sqoop mirrors + `stg_wfm_schedule` (demoted multi-col) |
| `extract_ts STRING` | `extract_ts DATE` | DAY | 8 delta feeds |
| `client_code STRING, feed_date STRING` | `feed_date DATE` (partition) + `client_code STRING` (cluster) | DAY | 10 file feeds |
| `snapshot_date STRING` | `snapshot_date DATE` | DAY | 8 ODS cleanse tables |
| `event_date STRING` / `call_date STRING` / `sched_date STRING` | Same name as `DATE` | DAY | 7 ODS cleanse tables |
| `work_month STRING` / `period_month STRING` / `swap_month STRING` / `event_month STRING` | Same name as `DATE` | MONTH | 5 ODS delta-merge tables |
| `eff_from_year INT` | `eff_from_ts TIMESTAMP` | YEAR | 3 SCD-2 tables |
| `date_key INT` | `event_date DATE` | DAY | 11 DM facts/aggs |
| `period_month STRING` | `period_month_date DATE` | MONTH | 4 DM facts/aggs |
| `week_start_key INT` | `week_start_date DATE` | DAY | 1 DM agg |
| `date_key INT, channel STRING` | `event_date DATE` (partition) + `channel STRING` (first cluster col) | DAY | `fact_interaction` only |

### Unpartitioned Objects (13)

- 9 dimension tables: `dim_date`, `dim_agent`, `dim_client`, `dim_program`, `dim_queue`, `dim_site`, `dim_shift`, `dim_org`, `dim_disposition`
- 4 ACID tables: `ods_client_acid`, `ods_agent_acid`, `ods_ticket_acid`, `ods_invoice_acid`

### Object Type Mapping (100 objects)

| Source Type | Target Type | Count | Objects |
|---|---|---|---|
| `CREATE EXTERNAL TABLE` | `CREATE TABLE` | 45 | All staging tables |
| `CREATE TABLE` (Parquet) | `CREATE TABLE` | 47 | ODS cleanse/delta/SCD-2 + DM dims/facts + 3 agg tables |
| `CREATE TABLE` (ORC ACID) | `CREATE TABLE` | 4 | ODS ACID tables (drop transactional properties) |
| `CREATE TABLE` (Parquet agg) | `CREATE MATERIALIZED VIEW` | 4 | `agg_agent_daily`, `agg_agent_weekly`, `agg_queue_hourly`, `agg_site_daily` |

### Dataset Mapping

| Hive Database | BigQuery Dataset | Object Count |
|---|---|---|
| `staging` | `staging` | 45 tables |
| `ods` | `ods` | 30 tables |
| `dm` | `dm` | 21 tables + 4 MVs |


## Validation
## Validation: MVS YAML Spec Files

### Approach

All 9 acceptance criteria are validated via **MVS yaml spec files**. The agent writes the `.yaml` specs and the existing MVS framework handles execution against the live BigQuery catalog. No custom Python test scripts or validation harnesses are needed.

### MVS Spec Coverage by Acceptance Criterion

| AC# | Criterion | MVS Spec Scope |
|---|---|---|
| 1 | DDL-apply completeness | Verify all 100 DDL statements execute with 0 errors (96 CREATE TABLE + 4 CREATE MATERIALIZED VIEW). Any failure is a HARD FAIL naming the object and quoting BigQuery's error. |
| 2 | Per-column fidelity | Check all 913 target columns against 916 source columns: presence, name (preserved or mapped per 16 renames), ordinal position, mapped type including DECIMAL precision/scale (52 NUMERIC columns), 4 complex/nested types checked recursively, nullability/mode (REPEATED on 4 complex-type columns), column descriptions on 68 epoch columns + 2 lying columns, no reserved-word collisions. 3 intentional drops documented. |
| 3 | Object-type fidelity | Assert 96 objects = `BASE TABLE`, 4 objects (`agg_agent_daily`, `agg_agent_weekly`, `agg_queue_hourly`, `agg_site_daily`) = `MATERIALIZED VIEW`. No silent VIEW flips. 3 remaining agg tables (`agg_program_monthly`, `agg_csat_rollup_monthly`, `agg_billing_monthly`) remain BASE TABLE. |
| 4 | Partition, clustering, key intent | Verify 87 partitioned + 13 unpartitioned. Check partition column, granularity, and clustering columns for all 100 objects per the locked Performance Optimization matrix. Verify `partition_expiration_days=90` on all 45 staging tables. Verify 12 multi-column partition demotions. |
| 5 | Cross-dataset FK↔PK type consistency | For every documented join path (dm surrogate keys, ods↔dm natural keys, staging↔ods PKs), verify FK column type matches PK column type (all INT64↔INT64 for surrogate keys, matching types for natural keys). |
| 6 | Queryability smoke | `SELECT * FROM <object> LIMIT 0` succeeds on all 100 objects. Plus 3 representative queries: staging partition-filtered, ODS cross-table join, DM fact-dim join — all return with 0 type-coercion errors. |
| 7 | Integrity guards | Every object confirmed PRESENT and READABLE in INFORMATION_SCHEMA. Column count read-back matches expected count. Two empty/missing sides never match. |
| 8 | No-silent-skip | All checks produce run timestamps from live catalog data. No offline parse, no hardcoded pass, no prior-run bypass. |
| 9 | Physical-access performance | For 14 explicitly-clustered hot-path objects, partition+cluster-filtered query scans fewer bytes than unfiltered. NOT-EXERCISED on empty tables is documented, never hardcoded PASS. |

### Key Validation Data Points

The MVS specs should encode these critical checks:

**Column counts per table**: Derived from `manifests/tables.yaml` — each table's expected column count (data columns + partition columns that become regular columns, minus any dropped columns).

**Type assertions for the 52 DECIMAL columns**: Each must verify exact `NUMERIC(p,s)` precision and scale. Example: `fact_billing_line.line_amount` → `NUMERIC(14,2)`, `fact_queue_interval.avg_speed_answer_sec` → `NUMERIC(8,2)`.

**Complex type recursive checks**: 4 columns with sub-field verification (field names, field types including INT→INT64 inside STRUCTs).

**Partition column and granularity**: Per-table assertion matching the locked partition matrix.

**Clustering column order**: Per-table assertion for all 14+12 clustered objects.

**FK type pairs**: Every documented join path across the 3 datasets.

**The 2 lying column descriptions**: Must contain the exact millis warning text.

