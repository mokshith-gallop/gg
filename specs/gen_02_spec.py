#!/usr/bin/env python3
"""Generate 02-type-mapping.mvs.yaml by cross-referencing Hive HQL and BQ DDL.

Parses source Hive DDL to extract (table, column, hive_type), then matches
against the BQ DDL columns to produce the source_type cross-check mapping.
"""
import re, sys, os

SRC_DDL_DIR = '/workspace/source/hive/ddl'
BQ_DDL_DIR = os.path.join(os.path.dirname(__file__), '..', 'bigquery', 'ddl')

# ── Hive DDL parsing ────────────────────────────────────────────────────────

def split_top_level(block, sep=','):
    """Split on sep at top-level (respecting (), <>, quotes)."""
    parts, depth_p, depth_a, in_q, cur = [], 0, 0, False, []
    for ch in block:
        if ch == "'" and not in_q: in_q = True; cur.append(ch); continue
        if ch == "'" and in_q: in_q = False; cur.append(ch); continue
        if in_q: cur.append(ch); continue
        if ch == '(': depth_p += 1
        elif ch == ')': depth_p -= 1
        elif ch == '<': depth_a += 1
        elif ch == '>': depth_a -= 1
        elif ch == sep and depth_p == 0 and depth_a == 0:
            parts.append(''.join(cur)); cur = []; continue
        cur.append(ch)
    if cur: parts.append(''.join(cur))
    return parts


def find_matching_paren(s, start):
    depth = 0
    for i in range(start, len(s)):
        if s[i] == '(': depth += 1
        elif s[i] == ')':
            depth -= 1
            if depth == 0: return i
    return len(s)


def parse_hive_type(raw):
    """Return the canonical Hive type string for cross-check.

    For simple types: BIGINT, INT, STRING, BOOLEAN, TIMESTAMP, DOUBLE
    For DECIMAL(p,s): DECIMAL  (the harness normalizes DECIMAL->NUMERIC)
    For ARRAY<...>: ARRAY
    For MAP<...>: MAP
    """
    raw = raw.strip()
    upper = raw.upper()
    if upper.startswith('ARRAY'):
        return 'ARRAY'
    if upper.startswith('MAP'):
        return 'MAP'
    if upper.startswith('DECIMAL'):
        return 'DECIMAL'
    # Return the base type name
    return upper.split('(')[0].strip()


def parse_hive_ddl_files():
    """Parse all Hive DDL files, return dict: {(db, table_name): [(col_name, hive_type), ...]}"""
    files = [
        '02-staging-sqoop-mirrors.hql',
        '03-staging-delta-feeds.hql',
        '04-staging-file-feeds.hql',
        '05-ods-cleanse.hql',
        '06-ods-delta-scd2.hql',
        '07-ods-acid.hql',
        '08-dm-tables.hql',
    ]

    result = {}
    for fname in files:
        path = os.path.join(SRC_DDL_DIR, fname)
        with open(path) as f:
            text = f.read()

        # Split on CREATE statements
        stmts = re.split(r'(?=CREATE\s+)', text, flags=re.IGNORECASE)
        for stmt in stmts:
            stmt = stmt.strip()
            if not stmt.upper().startswith('CREATE'):
                continue
            # Skip views
            if re.search(r'CREATE\s+VIEW', stmt, re.IGNORECASE):
                continue

            # Extract db.table_name
            m = re.search(r'(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\s*\(', stmt, re.IGNORECASE)
            if not m:
                continue
            db = m.group(1).lower()
            tbl = m.group(2).lower()

            # Extract column block
            paren_start = stmt.index('(', m.end() - 1)
            paren_end = find_matching_paren(stmt, paren_start)
            col_block = stmt[paren_start + 1:paren_end]

            columns = []
            for part in split_top_level(col_block):
                part = part.strip()
                if not part:
                    continue
                # Remove COMMENT '...'
                part = re.sub(r"COMMENT\s+'[^']*'", '', part).strip()
                part = part.rstrip(',').strip()
                if not part:
                    continue
                tokens = part.split(None, 1)
                if len(tokens) < 2:
                    continue
                col_name = tokens[0].lower()
                col_type_raw = tokens[1].strip()
                columns.append((col_name, col_type_raw))

            # Also extract PARTITIONED BY columns
            part_m = re.search(r'PARTITIONED\s+BY\s*\(([^)]+)\)', stmt, re.IGNORECASE)
            if part_m:
                part_block = part_m.group(1)
                for part in split_top_level(part_block):
                    part = part.strip()
                    if not part:
                        continue
                    tokens = part.split(None, 1)
                    if len(tokens) < 2:
                        continue
                    col_name = tokens[0].lower()
                    col_type_raw = tokens[1].strip()
                    columns.append((col_name, col_type_raw))

            result[(db, tbl)] = columns
    return result


# ── BQ DDL parsing (reuse from gen_01) ──────────────────────────────────────

def parse_bq_table_cols(stmt):
    paren_start = stmt.index('(')
    paren_end = find_matching_paren(stmt, paren_start)
    col_block = stmt[paren_start + 1:paren_end]

    columns = []
    for part in split_top_level(col_block):
        part = part.strip()
        if not part:
            continue
        dm = re.search(r"OPTIONS\s*\(\s*description\s*=\s*'([^']*)'\s*\)", part)
        desc = dm.group(1) if dm else None
        if dm:
            part = part[:dm.start()].strip() + ' ' + part[dm.end():].strip()
        part = part.strip().rstrip(',').strip()
        if not part:
            continue
        tokens = part.split(None, 1)
        if len(tokens) < 2:
            continue
        col = {'name': tokens[0], 'type': tokens[1].strip().rstrip(',').strip()}
        if desc:
            col['description'] = desc
        columns.append(col)
    return columns


# Hard-coded MV schemas (same as gen_01)
MV_SCHEMAS = {
    'agg_agent_daily': [
        ('agent_sk', 'INT64'), ('site_code', 'STRING'),
        ('interactions_handled', 'INT64'), ('avg_handle_seconds', 'NUMERIC'),
        ('talk_seconds', 'INT64'), ('acw_seconds', 'INT64'), ('aux_seconds', 'INT64'),
        ('adherence_pct', 'NUMERIC'), ('occupancy_pct', 'NUMERIC'), ('event_date', 'DATE'),
    ],
    'agg_agent_weekly': [
        ('agent_sk', 'INT64'), ('site_code', 'STRING'),
        ('days_worked', 'INT64'), ('interactions_handled', 'INT64'),
        ('avg_handle_seconds', 'NUMERIC'), ('adherence_pct', 'NUMERIC'),
        ('occupancy_pct', 'NUMERIC'), ('week_start_date', 'DATE'),
    ],
    'agg_queue_hourly': [
        ('queue_sk', 'INT64'), ('hour_of_day', 'INT64'),
        ('offered', 'INT64'), ('answered', 'INT64'), ('abandoned', 'INT64'),
        ('sl_pct', 'NUMERIC'), ('forecast_volume', 'INT64'),
        ('volume_variance_pct', 'NUMERIC'), ('event_date', 'DATE'),
    ],
    'agg_site_daily': [
        ('site_code', 'STRING'), ('agents_active', 'INT64'), ('interactions', 'INT64'),
        ('avg_handle_seconds', 'NUMERIC'), ('sl_pct', 'NUMERIC'),
        ('adherence_pct', 'NUMERIC'), ('event_date', 'DATE'),
    ],
}

# Map of renamed columns: (bq_table, bq_col) -> hive_source_name
RENAMES = {}
# 11 tables: date_key -> event_date
for t in ['fact_interaction', 'fact_agent_activity', 'fact_queue_interval',
          'fact_csat_survey', 'fact_qa_evaluation', 'fact_adherence_daily',
          'fact_ticket', 'fact_ivr_path',
          'agg_agent_daily', 'agg_queue_hourly', 'agg_site_daily']:
    RENAMES[(t, 'event_date')] = 'date_key'
# 4 tables: period_month -> period_month_date
for t in ['fact_billing_line', 'agg_program_monthly', 'agg_csat_rollup_monthly', 'agg_billing_monthly']:
    RENAMES[(t, 'period_month_date')] = 'period_month'
# 1 table: week_start_key -> week_start_date
RENAMES[('agg_agent_weekly', 'week_start_date')] = 'week_start_key'

# Columns that exist only in BQ target (dropped from Hive source partition cols -> no source cross-check)
# eff_from_year dropped from 3 SCD-2 tables — those are absent from BQ so no entry needed.
# Partition cols that changed type but keep same name: these DO need source_type.
# NEW columns that don't exist at all in source: hour_of_day in agg_queue_hourly MV (derived from EXTRACT)
DERIVED_COLS = {
    ('agg_queue_hourly', 'hour_of_day'),  # EXTRACT(HOUR FROM interval_start_ts), no Hive source column
}


def parse_bq_ddl_file(path):
    with open(path) as f:
        text = f.read()
    tables = []
    stmts = re.split(r'(?=CREATE\s+OR\s+REPLACE\s+)', text)
    for stmt in stmts:
        stmt = stmt.strip()
        if not stmt.startswith('CREATE'):
            continue
        header = stmt.split('(')[0] if '(' in stmt else stmt.split('\n')[0]
        if 'MATERIALIZED VIEW' in header:
            obj_type = 'MATERIALIZED_VIEW'
            m = re.search(r'VIEW\s+(\w+)', header)
        else:
            obj_type = 'TABLE'
            m = re.search(r'TABLE\s+(\w+)', header)
        if not m:
            continue
        name = m.group(1)
        if obj_type == 'TABLE':
            cols = parse_bq_table_cols(stmt)
        else:
            cols = [{'name': n, 'type': t} for n, t in MV_SCHEMAS.get(name, [])]
        tables.append({'name': name, 'obj_type': obj_type, 'columns': cols})
    return tables


def numeric_scale(type_str):
    m = re.match(r'NUMERIC\((\d+),(\d+)\)', type_str)
    if m: return int(m.group(2))
    if type_str == 'NUMERIC': return 9
    return None


def yaml_type(s):
    if any(c in s for c in '<>:,{}[]'):
        return "'" + s.replace("'", "''") + "'"
    return s


# ── Layer mapping ───────────────────────────────────────────────────────────

# Map BQ DDL file -> Hive database name
FILE_DB_MAP = {
    'staging.sql': 'staging',
    'ods.sql': 'ods',
    'dm.sql': 'dm',
}


def build_spec():
    # Parse Hive sources
    hive = parse_hive_ddl_files()
    print(f"Hive tables parsed: {len(hive)}", file=sys.stderr)

    # Parse BQ targets
    bq_tables = {}
    for fname, db in FILE_DB_MAP.items():
        tables = parse_bq_ddl_file(os.path.join(BQ_DDL_DIR, fname))
        for t in tables:
            t['db'] = db
            bq_tables[t['name']] = t
    print(f"BQ tables parsed: {len(bq_tables)}", file=sys.stderr)

    # Group BQ tables by database layer
    layers = {'staging': [], 'ods': [], 'dm': []}
    for t in bq_tables.values():
        layers[t['db']].append(t)

    lines = []
    lines.append('name: type_mapping_cross_check')
    lines.append('')
    lines.append('connections:')
    lines.append('  source: { engine: impala }')
    lines.append('  target: { engine: bigquery }')
    lines.append('')
    lines.append('source_setup:')
    lines.append('  location_base: ${SOURCE_WAREHOUSE:-/tmp/dmt_src}')
    lines.append('  ddl:')
    lines.append('    - /workspace/source/hive/ddl/01-create-databases.hql')
    lines.append('    - /workspace/source/hive/ddl/02-staging-sqoop-mirrors.hql')
    lines.append('    - /workspace/source/hive/ddl/03-staging-delta-feeds.hql')
    lines.append('    - /workspace/source/hive/ddl/04-staging-file-feeds.hql')
    lines.append('    - /workspace/source/hive/ddl/05-ods-cleanse.hql')
    lines.append('    - /workspace/source/hive/ddl/06-ods-delta-scd2.hql')
    lines.append('    - /workspace/source/hive/ddl/07-ods-acid.hql')
    lines.append('    - /workspace/source/hive/ddl/08-dm-tables.hql')
    lines.append('')
    lines.append('migration:')
    lines.append('  steps:')
    lines.append('    - { kind: ddl, sql: bigquery/ddl/staging.sql }')
    lines.append('    - { kind: ddl, sql: bigquery/ddl/ods.sql }')
    lines.append('    - { kind: ddl, sql: bigquery/ddl/dm.sql }')
    lines.append('')
    lines.append('suites:')

    total_cols = 0
    total_cross = 0

    for db_name in ('staging', 'ods', 'dm'):
        lines.append(f'  # --- {db_name} layer ---')
        lines.append(f'  - pattern: schema_conformance')
        lines.append(f'    id: type-mapping-{db_name}')
        lines.append(f'    target_dataset: "${{BUILD_DATASET}}"')
        lines.append(f'    source_database: {db_name}')
        lines.append(f'    tables:')

        for tbl in layers[db_name]:
            tbl_name = tbl['name']
            hive_key = (db_name, tbl_name)
            hive_cols = dict(hive.get(hive_key, []))  # {col_name: raw_hive_type}

            lines.append(f'      - table: {tbl_name}')
            lines.append(f'        source_table: {tbl_name}')
            lines.append(f'        columns:')

            for col in tbl['columns']:
                bq_name = col['name']
                bq_type = col['type']

                # Skip derived columns with no Hive source
                if (tbl_name, bq_name) in DERIVED_COLS:
                    # Still declare the column but WITHOUT source_type
                    parts = [f'name: {bq_name}', f'type: {yaml_type(bq_type)}']
                    scale = numeric_scale(bq_type)
                    if scale is not None:
                        parts.append(f'scale: {scale}')
                    lines.append('          - { ' + ', '.join(parts) + ' }')
                    total_cols += 1
                    continue

                # Determine source column name (might be renamed)
                rename_key = (tbl_name, bq_name)
                src_name = RENAMES.get(rename_key, None)

                # Look up Hive type
                hive_lookup = src_name if src_name else bq_name
                hive_raw_type = hive_cols.get(hive_lookup)

                parts = [f'name: {bq_name}', f'type: {yaml_type(bq_type)}']
                scale = numeric_scale(bq_type)
                if scale is not None:
                    parts.append(f'scale: {scale}')

                if src_name:
                    parts.append(f'source_name: {src_name}')

                if hive_raw_type:
                    src_type = parse_hive_type(hive_raw_type)
                    parts.append(f'source_type: {src_type}')
                    total_cross += 1
                else:
                    # No source column found — might be a partition col renamed
                    # that's not in the column block. Try to provide source_type anyway.
                    # For converted partition cols (STRING→DATE), we know the source type
                    if bq_type == 'DATE' and bq_name in ('load_date', 'extract_ts', 'feed_date',
                        'snapshot_date', 'event_date', 'call_date', 'sched_date',
                        'work_month', 'period_month', 'swap_month', 'event_month'):
                        parts.append('source_type: STRING')
                        total_cross += 1
                    elif bq_type == 'DATE' and src_name:
                        # Renamed partition col: date_key was INT, period_month was STRING, week_start_key was INT
                        if src_name in ('date_key', 'week_start_key'):
                            parts.append('source_type: INT')
                        elif src_name == 'period_month':
                            parts.append('source_type: STRING')
                        total_cross += 1

                lines.append('          - { ' + ', '.join(parts) + ' }')
                total_cols += 1

    print(f"Total columns emitted: {total_cols}", file=sys.stderr)
    print(f"Columns with source_type cross-check: {total_cross}", file=sys.stderr)

    return '\n'.join(lines) + '\n'


def main():
    spec = build_spec()
    out_path = os.path.join(os.path.dirname(__file__), '02-type-mapping.mvs.yaml')
    with open(out_path, 'w') as f:
        f.write(spec)
    print(f"Wrote {out_path} ({len(spec)} bytes)", file=sys.stderr)


if __name__ == '__main__':
    main()
