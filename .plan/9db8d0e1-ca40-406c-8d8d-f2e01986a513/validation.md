# Validation

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

