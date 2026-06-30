#!/usr/bin/env python3
"""Generate 03-partition-cluster.mvs.yaml from the BQ DDL files.

For each of the 100 objects, declares: partition_by, cluster_by, table_options,
and at least one column (required by schema_conformance).
"""
import re, sys, os

BQ_DDL_DIR = os.path.join(os.path.dirname(__file__), '..', 'bigquery', 'ddl')

# ── Reusable parsing ────────────────────────────────────────────────────────

def split_top_level(block, sep=','):
    parts, dp, da, iq, cur = [], 0, 0, False, []
    for ch in block:
        if ch == "'" and not iq: iq = True; cur.append(ch); continue
        if ch == "'" and iq: iq = False; cur.append(ch); continue
        if iq: cur.append(ch); continue
        if ch == '(': dp += 1
        elif ch == ')': dp -= 1
        elif ch == '<': da += 1
        elif ch == '>': da -= 1
        elif ch == sep and dp == 0 and da == 0:
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

def parse_first_col(stmt):
    """Get the first column name and type from a CREATE TABLE statement."""
    paren_start = stmt.index('(')
    paren_end = find_matching_paren(stmt, paren_start)
    col_block = stmt[paren_start + 1:paren_end]
    first = split_top_level(col_block)[0].strip()
    # Remove OPTIONS(...) and COMMENT
    first = re.sub(r"OPTIONS\s*\([^)]*\)", '', first)
    first = re.sub(r"COMMENT\s+'[^']*'", '', first)
    first = first.strip().rstrip(',').strip()
    tokens = first.split(None, 1)
    if len(tokens) < 2:
        return None, None
    return tokens[0], tokens[1].strip().rstrip(',').strip()

def yaml_type(s):
    if any(c in s for c in '<>:,{}[]'):
        return "'" + s.replace("'", "''") + "'"
    return s

# ── Parse DDL and extract metadata ─────────────────────────────────────────

def parse_ddl_file(path, layer):
    """Parse a DDL file and return table metadata."""
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

        # Extract first column for the required columns entry
        if obj_type == 'TABLE':
            first_name, first_type = parse_first_col(stmt)
        else:
            first_name, first_type = _mv_first_col(name)

        # For MVs, the DDL section after AS SELECT is the query, not table metadata.
        # Strip the AS SELECT ... portion for metadata extraction.
        if obj_type == 'MATERIALIZED_VIEW':
            as_pos = re.search(r'\bAS\s*\n', stmt)
            meta_block = stmt[:as_pos.start()] if as_pos else stmt
        else:
            meta_block = stmt
        # Strip SQL comments to avoid matching CLUSTER BY / PARTITION BY in comment text
        meta_block = re.sub(r'--[^\n]*', '', meta_block)

        # Extract PARTITION BY
        partition_col = None
        pm = re.search(r'PARTITION\s+BY\s+(?:DATE_TRUNC|TIMESTAMP_TRUNC)\s*\(\s*(\w+)', meta_block)
        if pm:
            partition_col = pm.group(1)
        else:
            pm = re.search(r'PARTITION\s+BY\s+(\w+)', meta_block)
            if pm:
                partition_col = pm.group(1)

        # Extract CLUSTER BY (only from the DDL metadata, not query body)
        cluster_cols = []
        cm = re.search(r'CLUSTER\s+BY\s+([^\n;]+)', meta_block)
        if cm:
            raw = cm.group(1).strip().rstrip(';').strip()
            cluster_cols = [c.strip() for c in raw.split(',') if c.strip()]

        # Extract partition_expiration_days
        expiration = None
        em = re.search(r'partition_expiration_days\s*=\s*(\d+)', meta_block)
        if em:
            expiration = int(em.group(1))

        tables.append({
            'name': name,
            'obj_type': obj_type,
            'layer': layer,
            'first_col': (first_name, first_type),
            'partition_col': partition_col,
            'cluster_cols': cluster_cols,
            'expiration': expiration,
        })
    return tables


MV_FIRST_COLS = {
    'agg_agent_daily': ('agent_sk', 'INT64'),
    'agg_agent_weekly': ('agent_sk', 'INT64'),
    'agg_queue_hourly': ('queue_sk', 'INT64'),
    'agg_site_daily': ('site_code', 'STRING'),
}

def _mv_first_col(name):
    return MV_FIRST_COLS.get(name, ('id', 'INT64'))


def numeric_scale(t):
    m = re.match(r'NUMERIC\((\d+),(\d+)\)', t)
    if m: return int(m.group(2))
    if t == 'NUMERIC': return 9
    return None


def emit_spec(all_tables):
    lines = []
    lines.append('name: partition_cluster_expiration')
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
    lines.append('    id: partition-cluster-expiration')
    lines.append('    target_dataset: "${BUILD_DATASET}"')
    lines.append('    tables:')

    partitioned = 0
    clustered = 0
    with_expiration = 0

    for tbl in all_tables:
        lines.append(f'      - table: {tbl["name"]}')

        # Partition
        if tbl['partition_col']:
            lines.append(f'        partition_by: {tbl["partition_col"]}')
            partitioned += 1

        # Cluster
        if tbl['cluster_cols']:
            cols_yaml = ', '.join(tbl['cluster_cols'])
            lines.append(f'        cluster_by: [{cols_yaml}]')
            clustered += 1

        # Table options (expiration)
        if tbl['expiration'] is not None:
            lines.append(f'        table_options:')
            lines.append(f'          partition_expiration_days: {tbl["expiration"]}')
            with_expiration += 1

        # At least one column required
        fn, ft = tbl['first_col']
        if fn and ft:
            scale = numeric_scale(ft)
            parts = [f'name: {fn}', f'type: {yaml_type(ft)}']
            if scale is not None:
                parts.append(f'scale: {scale}')
            lines.append(f'        columns:')
            lines.append(f'          - {{ {", ".join(parts)} }}')
        else:
            # fallback — shouldn't happen
            lines.append(f'        columns:')
            lines.append(f'          - {{ name: _placeholder, type: INT64 }}')

    print(f"Partitioned: {partitioned}/100", file=sys.stderr)
    print(f"Clustered: {clustered}/100", file=sys.stderr)
    print(f"With expiration: {with_expiration}/100", file=sys.stderr)

    return '\n'.join(lines) + '\n'


def main():
    staging = parse_ddl_file(os.path.join(BQ_DDL_DIR, 'staging.sql'), 'staging')
    ods = parse_ddl_file(os.path.join(BQ_DDL_DIR, 'ods.sql'), 'ods')
    dm = parse_ddl_file(os.path.join(BQ_DDL_DIR, 'dm.sql'), 'dm')
    all_tables = staging + ods + dm

    print(f"Total tables: {len(all_tables)}", file=sys.stderr)
    spec = emit_spec(all_tables)

    out_path = os.path.join(os.path.dirname(__file__), '03-partition-cluster.mvs.yaml')
    with open(out_path, 'w') as f:
        f.write(spec)
    print(f"Wrote {out_path} ({len(spec)} bytes)", file=sys.stderr)


if __name__ == '__main__':
    main()
