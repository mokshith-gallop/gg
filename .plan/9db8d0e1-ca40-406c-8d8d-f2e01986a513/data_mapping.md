# Data Mapping

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

