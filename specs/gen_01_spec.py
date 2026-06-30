#!/usr/bin/env python3
"""Generate 01-ddl-apply-structure.mvs.yaml from the BigQuery DDL files."""
import re, sys, os

DDL_DIR = os.path.join(os.path.dirname(__file__), '..', 'bigquery', 'ddl')

# Hard-code the 4 MV schemas since MV SELECT parsing is fragile.
# These match the ACTUAL BigQuery output types verified in step 1.
MV_SCHEMAS = {
    'agg_agent_daily': [
        ('agent_sk', 'INT64'),
        ('site_code', 'STRING'),
        ('interactions_handled', 'INT64'),
        ('avg_handle_seconds', 'NUMERIC'),
        ('talk_seconds', 'INT64'),
        ('acw_seconds', 'INT64'),
        ('aux_seconds', 'INT64'),
        ('adherence_pct', 'NUMERIC'),
        ('occupancy_pct', 'NUMERIC'),
        ('event_date', 'DATE'),
    ],
    'agg_agent_weekly': [
        ('agent_sk', 'INT64'),
        ('site_code', 'STRING'),
        ('days_worked', 'INT64'),
        ('interactions_handled', 'INT64'),
        ('avg_handle_seconds', 'NUMERIC'),
        ('adherence_pct', 'NUMERIC'),
        ('occupancy_pct', 'NUMERIC'),
        ('week_start_date', 'DATE'),
    ],
    'agg_queue_hourly': [
        ('queue_sk', 'INT64'),
        ('hour_of_day', 'INT64'),
        ('offered', 'INT64'),
        ('answered', 'INT64'),
        ('abandoned', 'INT64'),
        ('sl_pct', 'NUMERIC'),
        ('forecast_volume', 'INT64'),
        ('volume_variance_pct', 'NUMERIC'),
        ('event_date', 'DATE'),
    ],
    'agg_site_daily': [
        ('site_code', 'STRING'),
        ('agents_active', 'INT64'),
        ('interactions', 'INT64'),
        ('avg_handle_seconds', 'NUMERIC'),
        ('sl_pct', 'NUMERIC'),
        ('adherence_pct', 'NUMERIC'),
        ('event_date', 'DATE'),
    ],
}


def split_top_level(block, sep=','):
    """Split block by sep at top level only (respect (), <>, quotes)."""
    parts = []
    depth_p = 0
    depth_a = 0
    in_q = False
    cur = []
    for ch in block:
        if ch == "'" and not in_q:
            in_q = True; cur.append(ch); continue
        if ch == "'" and in_q:
            in_q = False; cur.append(ch); continue
        if in_q:
            cur.append(ch); continue
        if ch == '(':
            depth_p += 1
        elif ch == ')':
            depth_p -= 1
        elif ch == '<':
            depth_a += 1
        elif ch == '>':
            depth_a -= 1
        elif ch == sep and depth_p == 0 and depth_a == 0:
            parts.append(''.join(cur))
            cur = []
            continue
        cur.append(ch)
    if cur:
        parts.append(''.join(cur))
    return parts


def find_matching_paren(s, start):
    """Find the closing ) matching the ( at position start."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == '(':
            depth += 1
        elif s[i] == ')':
            depth -= 1
            if depth == 0:
                return i
    return len(s)


def parse_table_cols(stmt):
    """Parse columns from a CREATE TABLE statement."""
    paren_start = stmt.index('(')
    paren_end = find_matching_paren(stmt, paren_start)
    col_block = stmt[paren_start + 1:paren_end]

    columns = []
    for part in split_top_level(col_block):
        part = part.strip()
        if not part:
            continue
        # Extract OPTIONS(description='...')
        desc = None
        dm = re.search(r"OPTIONS\s*\(\s*description\s*=\s*'([^']*)'\s*\)", part)
        if dm:
            desc = dm.group(1)
            part = part[:dm.start()].strip() + ' ' + part[dm.end():].strip()
            part = part.strip()

        # Remove trailing comma
        part = part.rstrip(',').strip()
        if not part:
            continue

        # Split into name and type
        tokens = part.split(None, 1)
        if len(tokens) < 2:
            continue
        name = tokens[0]
        ctype = tokens[1].strip().rstrip(',').strip()

        col = {'name': name, 'type': ctype}
        if desc:
            col['description'] = desc
        columns.append(col)
    return columns


def parse_ddl_file(path):
    """Parse all CREATE statements from a DDL file."""
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
            cols = parse_table_cols(stmt)
        else:
            # Use hard-coded MV schemas
            if name in MV_SCHEMAS:
                cols = [{'name': n, 'type': t} for n, t in MV_SCHEMAS[name]]
            else:
                print(f"WARNING: unknown MV {name}", file=sys.stderr)
                cols = []

        tables.append({'name': name, 'obj_type': obj_type, 'columns': cols})
    return tables


def numeric_scale(type_str):
    """Extract scale from NUMERIC(p,s) or bare NUMERIC."""
    m = re.match(r'NUMERIC\((\d+),(\d+)\)', type_str)
    if m:
        return int(m.group(2))
    if type_str == 'NUMERIC':
        return 9  # NUMERIC(38,9)
    return None


def yaml_type(s):
    """Quote a type string for YAML if it contains special chars."""
    if any(c in s for c in '<>:,{}[]'):
        return "'" + s.replace("'", "''") + "'"
    return s


def emit_spec(all_tables):
    """Emit the full MVS YAML spec."""
    lines = []
    lines.append('name: ddl_apply_structure')
    lines.append('')
    lines.append('connections:')
    lines.append('  target: { engine: bigquery }')
    lines.append('')
    lines.append('migration:')
    lines.append('  steps:')
    lines.append('    - { kind: ddl, sql: bigquery/ddl/staging.sql }')
    lines.append('    - { kind: ddl, sql: bigquery/ddl/ods.sql }')
    lines.append('    - { kind: ddl, sql: bigquery/ddl/dm.sql }')
    lines.append('')
    lines.append('suites:')
    lines.append('  - pattern: schema_conformance')
    lines.append('    id: ddl-apply-structure')
    lines.append('    target_dataset: "${BUILD_DATASET}"')
    lines.append('    expect_table_count: 100')
    lines.append('    tables:')

    for tbl in all_tables:
        lines.append(f'      - table: {tbl["name"]}')
        lines.append(f'        expect_object_type: {tbl["obj_type"]}')
        lines.append(f'        columns:')
        for col in tbl['columns']:
            parts = [f'name: {col["name"]}']
            parts.append(f'type: {yaml_type(col["type"])}')

            scale = numeric_scale(col['type'])
            if scale is not None:
                parts.append(f'scale: {scale}')

            if 'description' in col:
                d = col['description']
                # Escape single quotes for YAML
                d_esc = d.replace("'", "''")
                parts.append(f"description: '{d_esc}'")

            lines.append('          - { ' + ', '.join(parts) + ' }')

    return '\n'.join(lines) + '\n'


def main():
    staging = parse_ddl_file(os.path.join(DDL_DIR, 'staging.sql'))
    ods = parse_ddl_file(os.path.join(DDL_DIR, 'ods.sql'))
    dm = parse_ddl_file(os.path.join(DDL_DIR, 'dm.sql'))

    all_tables = staging + ods + dm

    print(f"Parsed {len(staging)} staging + {len(ods)} ods + {len(dm)} dm = {len(all_tables)} objects", file=sys.stderr)
    total_cols = sum(len(t['columns']) for t in all_tables)
    print(f"Total columns: {total_cols}", file=sys.stderr)
    mv_count = sum(1 for t in all_tables if t['obj_type'] == 'MATERIALIZED_VIEW')
    print(f"MVs: {mv_count}, Tables: {len(all_tables) - mv_count}", file=sys.stderr)

    # Verify MV columns
    for t in all_tables:
        if t['obj_type'] == 'MATERIALIZED_VIEW':
            print(f"  MV {t['name']}: {len(t['columns'])} cols", file=sys.stderr)

    spec = emit_spec(all_tables)
    out_path = os.path.join(os.path.dirname(__file__), '01-ddl-apply-structure.mvs.yaml')
    with open(out_path, 'w') as f:
        f.write(spec)
    print(f"Wrote {out_path} ({len(spec)} bytes, {total_cols} columns)", file=sys.stderr)


if __name__ == '__main__':
    main()
