-- ============================================================================
-- BigQuery DDL — dm dataset (21 CREATE TABLE + 4 CREATE MATERIALIZED VIEW)
-- Migrated from Hive: manifests/tables.yaml + hive/ddl/08-dm-tables.hql
-- Type mapping: BIGINT/INT → INT64, STRING → STRING, DECIMAL(p,s) → NUMERIC(p,s),
--               BOOLEAN → BOOL, TIMESTAMP → TIMESTAMP
-- Partition renames: date_key INT → event_date DATE (11 tables),
--                    period_month STRING → period_month_date DATE (4 tables),
--                    week_start_key INT → week_start_date DATE (1 table)
-- Multi-col partition demotion: fact_interaction (date_key,channel) →
--   PARTITION BY event_date + channel demoted to first CLUSTER BY column
-- 4 MVs: agg_agent_daily, agg_agent_weekly, agg_queue_hourly, agg_site_daily
-- 9 dimensions: unpartitioned, no clustering
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Dimensions (9 tables) — unpartitioned, no clustering
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE dim_date (
  date_key                    INT64,
  full_date                   STRING,
  day_of_week                 INT64,
  day_name                    STRING,
  week_of_year                INT64,
  month_no                    INT64,
  month_name                  STRING,
  quarter_no                  INT64,
  year_no                     INT64,
  is_weekend                  BOOL,
  is_holiday_us               BOOL,
  fiscal_period               STRING
);

CREATE OR REPLACE TABLE dim_agent (
  agent_sk                    INT64,
  agent_id                    INT64,
  employee_no                 STRING,
  full_name                   STRING,
  job_grade                   STRING,
  employment_type             STRING,
  org_unit_id                 INT64,
  team_name                   STRING,
  site_code                   STRING,
  status                      STRING,
  hire_date_key               INT64,
  is_current                  BOOL
);

CREATE OR REPLACE TABLE dim_client (
  client_sk                   INT64,
  client_id                   INT64,
  client_code                 STRING,
  client_name                 STRING,
  industry                    STRING,
  hq_country                  STRING,
  primary_contact_name        STRING,
  primary_contact_email       STRING,
  status                      STRING
);

CREATE OR REPLACE TABLE dim_program (
  program_sk                  INT64,
  program_id                  INT64,
  program_code                STRING,
  program_name                STRING,
  client_id                   INT64,
  line_of_business            STRING,
  channel_mix                 STRING,
  site_code                   STRING,
  billing_model               STRING,
  status                      STRING,
  go_live_date_key            INT64
);

CREATE OR REPLACE TABLE dim_queue (
  queue_sk                    INT64,
  queue_id                    INT64,
  queue_code                  STRING,
  queue_name                  STRING,
  program_id                  INT64,
  media_type                  STRING,
  priority                    INT64
);

CREATE OR REPLACE TABLE dim_site (
  site_sk                     INT64,
  site_code                   STRING,
  site_name                   STRING,
  region                      STRING,
  country                     STRING,
  timezone                    STRING
);

CREATE OR REPLACE TABLE dim_shift (
  shift_sk                    INT64,
  shift_id                    INT64,
  shift_code                  STRING,
  shift_name                  STRING,
  start_hhmm                  STRING,
  end_hhmm                    STRING,
  overnight_flag              BOOL,
  site_code                   STRING
);

CREATE OR REPLACE TABLE dim_org (
  org_sk                      INT64,
  org_unit_id                 INT64,
  unit_code                   STRING,
  unit_name                   STRING,
  unit_type                   STRING,
  level1_name                 STRING,
  level2_name                 STRING,
  level3_name                 STRING,
  level4_name                 STRING,
  site_code                   STRING,
  cost_center                 STRING
);

CREATE OR REPLACE TABLE dim_disposition (
  disposition_sk              INT64,
  disposition_code            STRING,
  disposition_desc            STRING,
  category                    STRING,
  billable_flag               BOOL
);

-- ---------------------------------------------------------------------------
-- Facts (9 tables)
-- date_key INT → event_date DATE; period_month STRING → period_month_date DATE
-- Multi-col partition (date_key, channel) on fact_interaction →
--   PARTITION BY event_date + CLUSTER BY channel, agent_sk, client_sk
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE fact_interaction (
  interaction_id              STRING,
  client_sk                   INT64,
  program_sk                  INT64,
  queue_sk                    INT64,
  agent_sk                    INT64,
  customer_ref                STRING,
  start_ts                    TIMESTAMP,
  end_ts                      TIMESTAMP,
  handle_seconds              INT64,
  resolved_flag               BOOL,
  source_system               STRING,
  event_date                  DATE,
  channel                     STRING
)
PARTITION BY event_date
CLUSTER BY channel, agent_sk, client_sk;

CREATE OR REPLACE TABLE fact_agent_activity (
  agent_sk                    INT64,
  state_code                  STRING,
  state_seconds               INT64,
  occurrence_count            INT64,
  first_state_ts              TIMESTAMP,
  last_state_ts               TIMESTAMP,
  event_date                  DATE
)
PARTITION BY event_date
CLUSTER BY agent_sk, state_code;

CREATE OR REPLACE TABLE fact_queue_interval (
  queue_sk                    INT64,
  interval_start_ts           TIMESTAMP,
  offered                     INT64,
  answered                    INT64,
  abandoned                   INT64,
  answered_in_sl              INT64,
  sl_threshold_sec            INT64,
  avg_speed_answer_sec        NUMERIC(8,2),
  avg_handle_sec              NUMERIC(8,2),
  event_date                  DATE
)
PARTITION BY event_date
CLUSTER BY queue_sk;

CREATE OR REPLACE TABLE fact_csat_survey (
  survey_id                   STRING,
  interaction_id              STRING,
  client_sk                   INT64,
  program_sk                  INT64,
  agent_sk                    INT64,
  survey_ts                   TIMESTAMP,
  csat_score                  INT64,
  nps_score                   INT64,
  fcr_claimed                 BOOL,
  event_date                  DATE
)
PARTITION BY event_date
CLUSTER BY program_sk, agent_sk;

CREATE OR REPLACE TABLE fact_qa_evaluation (
  qa_form_id                  STRING,
  interaction_id              STRING,
  agent_sk                    INT64,
  program_sk                  INT64,
  evaluated_ts                TIMESTAMP,
  scored_points               INT64,
  max_points                  INT64,
  overall_pct                 NUMERIC(5,2),
  auto_fail                   BOOL,
  event_date                  DATE
)
PARTITION BY event_date;

CREATE OR REPLACE TABLE fact_billing_line (
  invoice_line_id             INT64,
  invoice_id                  INT64,
  client_sk                   INT64,
  program_sk                  INT64,
  service_code                STRING,
  qty                         NUMERIC(12,2),
  unit_rate                   NUMERIC(12,4),
  line_amount                 NUMERIC(14,2),
  adjustment_flag             BOOL,
  invoice_status              STRING,
  period_month_date           DATE
)
PARTITION BY DATE_TRUNC(period_month_date, MONTH)
CLUSTER BY client_sk, program_sk;

CREATE OR REPLACE TABLE fact_adherence_daily (
  agent_sk                    INT64,
  scheduled_minutes           INT64,
  worked_minutes              INT64,
  exception_minutes           INT64,
  timeoff_minutes             INT64,
  adherence_pct               NUMERIC(5,2),
  occupancy_pct               NUMERIC(5,2),
  event_date                  DATE
)
PARTITION BY event_date
CLUSTER BY agent_sk;

CREATE OR REPLACE TABLE fact_ticket (
  ticket_id                   INT64,
  program_sk                  INT64,
  category_code               STRING,
  assigned_agent_sk           INT64,
  priority                    STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  resolved_ts                 TIMESTAMP,
  resolution_minutes          INT64,
  sla_breached_flag           BOOL,
  touch_count                 INT64,
  event_date                  DATE
)
PARTITION BY event_date
CLUSTER BY program_sk, status;

CREATE OR REPLACE TABLE fact_ivr_path (
  session_ref                 STRING,
  client_code                 STRING,
  menu_path_full              STRING,
  hops                        INT64,
  contained_flag              BOOL,
  exit_key                    STRING,
  duration_seconds            INT64,
  event_date                  DATE
)
PARTITION BY event_date;

-- ---------------------------------------------------------------------------
-- Aggregate tables kept as BASE TABLE (3 tables)
-- These use GROUPING SETS / ROLLUP / multi-table joins unsupported by BQ MVs.
-- period_month STRING → period_month_date DATE (MONTH granularity)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE agg_program_monthly (
  client_sk                   INT64,
  program_sk                  INT64,
  line_of_business            STRING,
  interactions                INT64,
  avg_handle_seconds          NUMERIC(8,2),
  avg_csat                    NUMERIC(5,2),
  billed_amount               NUMERIC(14,2),
  grouping_level              INT64,
  period_month_date           DATE
)
PARTITION BY DATE_TRUNC(period_month_date, MONTH);

CREATE OR REPLACE TABLE agg_csat_rollup_monthly (
  client_sk                   INT64,
  program_sk                  INT64,
  site_code                   STRING,
  surveys                     INT64,
  avg_csat                    NUMERIC(5,2),
  pct_promoters               NUMERIC(5,2),
  pct_detractors              NUMERIC(5,2),
  grouping_id                 INT64,
  period_month_date           DATE
)
PARTITION BY DATE_TRUNC(period_month_date, MONTH);

CREATE OR REPLACE TABLE agg_billing_monthly (
  client_sk                   INT64,
  program_sk                  INT64,
  billed_amount               NUMERIC(14,2),
  sla_credit_amount           NUMERIC(12,2),
  telco_cost_amount           NUMERIC(12,2),
  net_revenue                 NUMERIC(14,2),
  period_month_date           DATE
)
PARTITION BY DATE_TRUNC(period_month_date, MONTH);

-- ---------------------------------------------------------------------------
-- Aggregate materialized views (4 MVs)
-- Simple GROUP BY aggregations eligible for BigQuery MV.
-- No LEFT JOIN, no subqueries in FROM, no MV-on-MV chaining.
-- Base tables must exist before MVs — order matters.
-- ---------------------------------------------------------------------------

-- MV 1: agg_agent_daily — daily agent-level activity aggregation
-- Incremental star-join MV: fact_agent_activity (fact) + dim_agent (dimension).
-- Aggregate operands come from the fact table only; dim columns in GROUP BY only.
-- BigQuery incremental MVs prohibit scalar wraps on aggregate output, so
-- avg_handle_seconds uses AVG(CAST(input AS NUMERIC)) and placeholder columns
-- use SUM(CAST(0 AS NUMERIC)) to preserve NUMERIC output type.
CREATE OR REPLACE MATERIALIZED VIEW agg_agent_daily
PARTITION BY event_date
CLUSTER BY agent_sk, site_code
AS
SELECT
  a.agent_sk,
  d.site_code,
  SUM(a.occurrence_count)                                                     AS interactions_handled,
  AVG(CAST(a.state_seconds AS NUMERIC))                                       AS avg_handle_seconds,
  SUM(CASE WHEN a.state_code = 'TALK' THEN a.state_seconds ELSE 0 END)       AS talk_seconds,
  SUM(CASE WHEN a.state_code = 'ACW'  THEN a.state_seconds ELSE 0 END)       AS acw_seconds,
  SUM(CASE WHEN a.state_code LIKE 'AUX%' THEN a.state_seconds ELSE 0 END)    AS aux_seconds,
  SUM(CAST(0 AS NUMERIC))                                                     AS adherence_pct,
  SUM(CAST(0 AS NUMERIC))                                                     AS occupancy_pct,
  a.event_date
FROM ${BUILD_DATASET}.fact_agent_activity a
INNER JOIN ${BUILD_DATASET}.dim_agent d ON a.agent_sk = d.agent_sk
GROUP BY a.agent_sk, d.site_code, a.event_date;

-- MV 2: agg_agent_weekly — weekly rollup of agent activity
-- Non-incremental MV: ISOWEEK truncation is unsupported for incremental MV partitioning.
-- Uses allow_non_incremental_definition + max_staleness; refreshed on schedule.
CREATE OR REPLACE MATERIALIZED VIEW agg_agent_weekly
PARTITION BY week_start_date
OPTIONS(allow_non_incremental_definition=true, enable_refresh=true,
        refresh_interval_minutes=60, max_staleness=INTERVAL '8' HOUR)
AS
SELECT
  a.agent_sk,
  d.site_code,
  COUNT(DISTINCT a.event_date)                                                AS days_worked,
  SUM(a.occurrence_count)                                                     AS interactions_handled,
  CAST(SAFE_DIVIDE(
    SUM(a.state_seconds),
    NULLIF(SUM(a.occurrence_count), 0)
  ) AS NUMERIC)                                                               AS avg_handle_seconds,
  CAST(0 AS NUMERIC)                                                          AS adherence_pct,
  CAST(0 AS NUMERIC)                                                          AS occupancy_pct,
  DATE_TRUNC(a.event_date, ISOWEEK)                                           AS week_start_date
FROM ${BUILD_DATASET}.fact_agent_activity a
INNER JOIN ${BUILD_DATASET}.dim_agent d ON a.agent_sk = d.agent_sk
GROUP BY a.agent_sk, d.site_code, DATE_TRUNC(a.event_date, ISOWEEK);

-- MV 3: agg_queue_hourly — hourly queue-interval aggregation
-- Incremental single-table MV on fact_queue_interval.
-- forecast_volume/volume_variance_pct require stg_wfm_forecast (layer-skip) —
-- supplementary scheduled SQL fills those.
CREATE OR REPLACE MATERIALIZED VIEW agg_queue_hourly
PARTITION BY event_date
CLUSTER BY queue_sk, hour_of_day
AS
SELECT
  queue_sk,
  EXTRACT(HOUR FROM interval_start_ts)                                        AS hour_of_day,
  SUM(offered)                                                                AS offered,
  SUM(answered)                                                               AS answered,
  SUM(abandoned)                                                              AS abandoned,
  AVG(CAST(answered_in_sl AS NUMERIC))                                        AS sl_pct,
  SUM(CAST(0 AS INT64))                                                       AS forecast_volume,
  SUM(CAST(0 AS NUMERIC))                                                     AS volume_variance_pct,
  event_date
FROM ${BUILD_DATASET}.fact_queue_interval
GROUP BY queue_sk, EXTRACT(HOUR FROM interval_start_ts), event_date;

-- MV 4: agg_site_daily — site-level daily aggregation
-- Incremental star-join MV: fact_agent_activity (fact) + dim_agent (dimension).
-- sl_pct/adherence_pct require different source tables — supplementary scheduled SQL.
CREATE OR REPLACE MATERIALIZED VIEW agg_site_daily
PARTITION BY event_date
AS
SELECT
  d.site_code,
  APPROX_COUNT_DISTINCT(a.agent_sk)                                           AS agents_active,
  SUM(a.occurrence_count)                                                     AS interactions,
  AVG(CAST(a.state_seconds AS NUMERIC))                                       AS avg_handle_seconds,
  SUM(CAST(0 AS NUMERIC))                                                     AS sl_pct,
  SUM(CAST(0 AS NUMERIC))                                                     AS adherence_pct,
  a.event_date
FROM ${BUILD_DATASET}.fact_agent_activity a
INNER JOIN ${BUILD_DATASET}.dim_agent d ON a.agent_sk = d.agent_sk
GROUP BY d.site_code, a.event_date;
