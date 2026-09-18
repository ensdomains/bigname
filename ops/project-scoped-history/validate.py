#!/usr/bin/env python3
"""Emit a rollback-only scratch-database check of the production history SQL."""

import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "crates/project/tests/scoped_history"
QUERIES = ROOT / "crates/project/src/stage/history"
CHAIN = "project-history-test"


def bound(sql):
    return sql.replace("$1", "'" + CHAIN + "'").replace("$2", "10")


def equality(previous, current, expected=None):
    comparison = f"({previous} EXCEPT ALL {current}) UNION ALL ({current} EXCEPT ALL {previous})"
    print(f"CREATE TEMP TABLE history_difference ON COMMIT DROP AS {comparison};")
    print("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM history_difference) THEN
            RAISE EXCEPTION 'History event selection changed';
        END IF;
    END $$;
    DROP TABLE history_difference;""")
    if expected:
        actual = f"SELECT DISTINCT event_identity FROM ({current}) selected JOIN normalized_events USING (normalized_event_id)"
        reference = f"SELECT event_identity FROM history_fixture WHERE {expected}"
        equality(reference, actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=int, nargs="+", default=[25_000, 100_000],
                        help="Unrelated rows per event family at each cumulative scale")
    args = parser.parse_args()
    if not args.scales or args.scales != sorted(set(args.scales)) or args.scales[0] < 1:
        parser.error("scales must be positive, distinct, and increasing")

    print("""\\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bigname_phase') THEN
        RAISE EXCEPTION 'Use an empty scratch database; bigname_phase already exists';
    END IF;
END $$;
CREATE SCHEMA bigname_phase;
SET LOCAL search_path TO bigname_phase, public;
SET LOCAL statement_timeout = '2min';""")
    for name in ["01_chain.sql", "02_raw_facts.sql", "03_identity.sql",
                 "04_manifests.sql", "05_normalized_events.sql"]:
        print((ROOT / "schema-v2/baseline" / name).read_text())
    print((FIXTURES / "fixture.sql").read_text())
    pairs = []
    for label in ["names", "primary"]:
        current = (QUERIES / f"{label}.sql").read_text()
        previous = (FIXTURES / f"previous_{label}.sql").read_text()
        print(f"PREPARE project_history_{label}(text, bigint) AS {current};")
        pairs.append((label, bound(previous), bound(current)))
        equality(bound(previous), bound(current), "expected_name" if label == "names" else "expected_primary")

    print("""CREATE FUNCTION pg_temp.history_plan(statement text) RETURNS jsonb LANGUAGE plpgsql AS $$
    DECLARE result jsonb; examined numeric;
    BEGIN
        EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || statement INTO result;
        WITH RECURSIVE nodes(node) AS (
            SELECT result -> 0 -> 'Plan'
            UNION ALL
            SELECT child FROM nodes CROSS JOIN LATERAL jsonb_array_elements(node -> 'Plans') child
        )
        SELECT sum((node ->> 'Actual Loops')::numeric * (
            (node ->> 'Actual Rows')::numeric +
            COALESCE((node ->> 'Rows Removed by Filter')::numeric, 0) +
            COALESCE((node ->> 'Rows Removed by Index Recheck')::numeric, 0)))
        INTO examined FROM nodes WHERE node ->> 'Relation Name' = 'normalized_events';
        IF examined > 1024 THEN
            RAISE EXCEPTION 'History query examined unrelated rows: %, plan: %', examined, result;
        END IF;
        RETURN jsonb_build_object('event_rows_examined', examined, 'plan', result);
    END $$;""")
    start = 1
    for scale in args.scales:
        print((FIXTURES / "unrelated.sql").read_text().replace("$1", str(start)).replace("$2", str(scale)) + ";")
        print("ANALYZE normalized_events;")
        print((QUERIES / "analyze_scopes.sql").read_text())
        for label, previous, current in pairs:
            equality(previous, current)
            statement = f"EXECUTE project_history_{label}('{CHAIN}', 10)".replace("'", "''")
            print(f"SELECT jsonb_build_object('query', '{label}', 'unrelated_rows', {scale * 2}, 'evidence', pg_temp.history_plan('{statement}'));")
        start = scale + 1
    print("ROLLBACK;")


if __name__ == "__main__":
    main()
