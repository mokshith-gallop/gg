#!/usr/bin/env python3
"""Generate 05-queryability.mvs.yaml — 100 smoke queries + 3 representative queries."""
import re, sys, os

BQ_DDL_DIR = os.path.join(os.path.dirname(__file__), '..', 'bigquery', 'ddl')

def extract_table_names(path):
    """Extract table names from a BQ DDL file."""
    with open(path) as f:
        text = f.read()
    names = []
    for m in re.finditer(r'CREATE\s+OR\s+REPLACE\s+(?:MATERIALIZED\s+VIEW|TABLE)\s+(\w+)', text):
        names.append(m.group(1))
    return names


def main():
    staging = extract_table_names(os.path.join(BQ_DDL_DIR, 'staging.sql'))
    ods = extract_table_names(os.path.join(BQ_DDL_DIR, 'ods.sql'))
    dm = extract_table_names(os.path.join(BQ_DDL_DIR, 'dm.sql'))
    all_names = staging + ods + dm
    print(f"Total objects: {len(all_names)}", file=sys.stderr)

    lines = []
    lines.append('name: queryability_smoke')
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
    lines.append('  # =========================================================================')
    lines.append('  # AC6 + AC8: Queryability smoke — SELECT * LIMIT 0 on every object plus')
    lines.append('  # 3 representative cross-table queries. All produce live run timestamps.')
    lines.append('  # Any failure to execute surfaces as ERROR (mode: measure never gates,')
    lines.append('  # but a query that cannot run raises an engine error → suite ERROR).')
    lines.append('  # =========================================================================')
    lines.append('  - pattern: query_performance')
    lines.append('    id: queryability-smoke')
    lines.append('    target_dataset: "${BUILD_DATASET}"')
    lines.append('    queries:')

    # 100 smoke queries
    for name in all_names:
        lines.append(f'      - id: smoke-{name}')
        lines.append(f'        mode: measure')
        lines.append(f'        sql: "SELECT * FROM ${{BUILD_DATASET}}.{name} LIMIT 0"')

    # 3 representative queries
    lines.append('')
    lines.append('      # --- Representative cross-table queries (1 per dataset tier) ---')
    lines.append('      - id: smoke-staging-filter')
    lines.append('        mode: measure')
    lines.append("        sql: \"SELECT * FROM ${BUILD_DATASET}.stg_tel_call WHERE load_date = DATE '2026-01-01' LIMIT 10\"")

    lines.append('      - id: smoke-ods-join')
    lines.append('        mode: measure')
    lines.append('        sql: "SELECT c.call_id, q.queue_name FROM ${BUILD_DATASET}.ods_call c JOIN ${BUILD_DATASET}.ods_queue q ON c.queue_id = q.queue_id LIMIT 10"')

    lines.append('      - id: smoke-dm-join')
    lines.append('        mode: measure')
    lines.append("        sql: \"SELECT f.interaction_id, a.full_name, p.program_name FROM ${BUILD_DATASET}.fact_interaction f JOIN ${BUILD_DATASET}.dim_agent a ON f.agent_sk = a.agent_sk JOIN ${BUILD_DATASET}.dim_program p ON f.program_sk = p.program_sk WHERE f.event_date = DATE '2026-01-01' LIMIT 10\"")

    spec = '\n'.join(lines) + '\n'
    out_path = os.path.join(os.path.dirname(__file__), '05-queryability.mvs.yaml')
    with open(out_path, 'w') as f:
        f.write(spec)
    print(f"Wrote {out_path} ({len(all_names) + 3} queries, {len(spec)} bytes)", file=sys.stderr)


if __name__ == '__main__':
    main()
