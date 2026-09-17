"""Exercise the actual reopen UPDATE in an isolated, disposable PostgreSQL database.

Example: python3 benchmark.py /path/to/repo --runtime podman --container bigname-test-postgres
The connected role must be able to create/drop the temporary database. No existing
application tables are changed. Query mutations are rolled back after each probe.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('repo', type=Path)
parser.add_argument('--runtime', default='podman')
parser.add_argument('--container', default='bigname-test-postgres')
args = parser.parse_args()
root = args.repo
name = 'reopen_index_' + uuid.uuid4().hex[:12]
base = [args.runtime, 'exec', '-i', args.container, 'psql', '-X', '-qAt', '-U', 'bigname', '-v', 'ON_ERROR_STOP=1']

def run(sql, database=name):
    result = subprocess.run(base + ['-d', database], input=sql, text=True, capture_output=True, timeout=240)
    if result.returncode:
        raise RuntimeError(result.stderr[-4000:])
    return result.stdout.strip()

def literal(value):
    return 'NULL' if value is None else "'" + str(value).replace("'", "''") + "'"

def nodes(node):
    yield node
    for child in node.get('Plans', []):
        yield from nodes(child)

source = (root / 'crates/interpret/src/write/discovery.rs').read_text()
statement = re.search(r'let reopen_statement = format!\(\s*"(.*?)"\s*[,)]', source, re.S)[1]
statement = statement.replace('{OBSERVATION_KEY}', 'observation_key')
from_id = '11111111-1111-1111-1111-111111111111'
target_id = '22222222-2222-2222-2222-222222222222'
other_id = '33333333-3333-3333-3333-333333333333'
key = 'resolver:fixture:0x' + 'a' * 64
cases = {
    'canonical_null_manifest': (100, None, None, False, None),
    'closed': (110, 7, None, False, None),
    'orphaned': (120, 7, None, False, None),
    'preserve_earlier_close': (130, 7, 200, True, 150),
    'replace_old_close': (140, 7, None, False, None),
}
queries = {}
for label, (block, manifest, successor, preserve, expected_end) in cases.items():
    values = ['fixture', 'resolver', from_id, target_id, 'updated', 'updated', manifest,
              block, 'block' + str(block), 'canonical', json.dumps({'observation_key': key, 'marker': label}),
              key, successor, None if successor is None else 'block' + str(successor), preserve]
    sql = statement
    for index, value in reversed(list(enumerate(values, 1))):
        sql = sql.replace('$' + str(index), literal(value))
    queries[label] = sql

run('CREATE DATABASE ' + name, database='bigname')
try:
    run('CREATE SCHEMA bigname_phase; SET search_path=bigname_phase,public;' +
        (root / 'schema-v2/baseline/01_chain.sql').read_text() +
        (root / 'schema-v2/baseline/03_identity.sql').read_text())
    run(f"""SET search_path=bigname_phase,public;
    INSERT INTO contract_instances(contract_instance_id,chain_id,contract_kind) VALUES
      ('{from_id}','fixture','root'),('{target_id}','fixture','contract'),('{other_id}','fixture','contract');
    INSERT INTO discovery_edges(chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,
      discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,
      active_to_block_number,active_to_block_hash,canonicality_state,deactivated_at,provenance)
    SELECT 'fixture','resolver','{from_id}'::uuid,target::uuid,'fixture','fixture',manifest,b,h,
      finish,CASE WHEN finish IS NOT NULL THEN 'block'||finish END,state::canonicality_state,
      CASE WHEN finish IS NOT NULL THEN now() END,jsonb_build_object('observation_key','{key}')
    FROM (VALUES
      (100,'block100',NULL::bigint,NULL::bigint,'canonical','{target_id}'),
      (100,'block100',NULL,1,'canonical','{target_id}'),
      (100,'block100fork',NULL,NULL,'orphaned','{target_id}'),
      (100,'block100',NULL,NULL,'canonical','{other_id}'),
      (110,'block110',120,7,'canonical','{target_id}'),
      (120,'block120',130,7,'orphaned','{target_id}'),
      (130,'block130',150,7,'orphaned','{target_id}'),
      (140,'block140',150,7,'canonical','{target_id}')
    ) v(b,h,finish,manifest,state,target);
    """)
    report = {'database': name, 'scaling': []}
    previous = 0
    for unrelated in [1000, 100000, 1000000]:
        run(f"""SET search_path=bigname_phase,public;
        INSERT INTO discovery_edges(chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,
          discovery_source,admission_basis,active_from_block_number,active_from_block_hash,
          active_to_block_number,active_to_block_hash,canonicality_state,deactivated_at,provenance)
        SELECT 'fixture','resolver','{from_id}'::uuid,'{target_id}'::uuid,'fixture','fixture',100,'block100',
          CASE WHEN n%3=0 THEN 200 END,CASE WHEN n%3=0 THEN 'block200' END,
          CASE WHEN n%10=0 THEN 'orphaned'::canonicality_state ELSE 'canonical'::canonicality_state END,
          CASE WHEN n%3=0 THEN now() END,
          jsonb_build_object('observation_key','resolver:fixture:0x'||lpad(to_hex(n),64,'0'))
        FROM generate_series({previous + 1},{unrelated}) n;
        ANALYZE discovery_edges;
        DROP INDEX discovery_edges_reopen_idx;
        """)
        previous = unrelated
        point = {'unrelated_rows': unrelated, 'queries': {}}
        selected = queries if unrelated == 1000000 else {'canonical_null_manifest': queries['canonical_null_manifest']}
        for stage in ['before', 'after']:
            if stage == 'after':
                started = time.monotonic()
                point['install_output'] = run((root / 'ops/discovery-reopen-index/install.sql').read_text())
                point['install_seconds'] = time.monotonic() - started
                run('ANALYZE bigname_phase.discovery_edges')
            for label, sql in selected.items():
                prefix = "BEGIN; SET LOCAL search_path=bigname_phase,public; SET LOCAL statement_timeout='30s';"
                plan = json.loads(run(prefix + 'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ' + sql + ';ROLLBACK;'))
                result = json.loads(run(prefix + sql + f""";
                SELECT json_agg(t ORDER BY discovery_edge_id) FROM (
                  SELECT discovery_edge_id,to_contract_instance_id,source_manifest_id,discovery_source,
                    admission_basis,active_from_block_number,active_from_block_hash,active_to_block_number,
                    active_to_block_hash,canonicality_state,deactivated_at IS NULL AS active,provenance
                  FROM discovery_edges WHERE provenance->>'observation_key'='{key}'
                ) t; ROLLBACK;"""))
                changed = [row for row in result if row['discovery_source'] == 'updated']
                assert len(changed) == 1, (label, changed)
                expected = cases[label]
                assert changed[0]['active_from_block_number'] == expected[0]
                assert changed[0]['source_manifest_id'] == expected[1]
                assert changed[0]['canonicality_state'] == 'canonical'
                assert changed[0]['active_to_block_number'] == expected[4]
                assert changed[0]['active'] == (expected[4] is None)
                point['queries'].setdefault(label, {})[stage] = {'plan': plan, 'result': result}
        for label, pair in point['queries'].items():
            assert pair['before']['result'] == pair['after']['result'], label
            selected_index = [n for n in nodes(pair['after']['plan'][0]['Plan']) if n.get('Index Name') == 'discovery_edges_reopen_idx']
            assert selected_index, (label, pair['after']['plan'])
            assert any('observation_key' in n.get('Index Cond', '') and 'active_from_block_number' in n.get('Index Cond', '') for n in selected_index), label
            pair['identical'] = True
        report['scaling'].append(point)
    definition_sql = "SELECT pg_get_indexdef('bigname_phase.discovery_edges_reopen_idx'::regclass)"
    definition = run(definition_sql)
    run('DROP INDEX bigname_phase.discovery_edges_reopen_idx;' + (root / 'migrations/20260917130000_discovery_edges_reopen_idx.sql').read_text())
    assert run(definition_sql) == definition
    run('DROP INDEX bigname_phase.discovery_edges_reopen_idx;SET search_path=bigname_phase,public;' + (root / 'schema-v2/baseline/03_identity.sql').read_text())
    assert run(definition_sql) == definition
    report['baseline_migration_online_identical'] = True
    report['index_definition'] = definition
    print(json.dumps(report, indent=2))
finally:
    run('DROP DATABASE ' + name, database='bigname')
