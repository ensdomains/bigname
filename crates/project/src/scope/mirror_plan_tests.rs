//! Access-path check for the mirror's historical ENSv1 pointer probes, keyed by the name each
//! pointer event addresses (`child_node`, then `namehash`, then `node`). The table holds a large
//! amount of unrelated pointer history, including many state-derived child pointers whose `node`
//! is the same parent, and `enable_seqscan` is off to stand in for a production-sized
//! `normalized_events`: the assertions are about which access path the planner can use at all.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::raw_sql;

use crate::stage::mirror_evidence::EVIDENCE_EVENTS_SQL;

const CHAIN: &str = "mirror-pointer-plan";
const INDEX: &str = "normalized_events_project_v1_pointer_addressed_node_idx";
const PARENT: &str = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
const CHILD: &str = "0x00000000000000000000000000000000000000000000000000000000000c0001";

const BASELINE: &[&str] = &[
    include_str!("../../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
];

/// The keyed history probe of `mirror_bulk.sql`, as `execute` runs it for a small frontier.
fn cached_nodes_probe() -> &'static str {
    include_str!("mirror_bulk.sql")
        .split(';')
        .find(|statement| statement.contains("INSERT INTO project_mirror_cached_nodes"))
        .expect("mirror_bulk.sql stages cached nodes")
}

#[tokio::test]
async fn addressed_node_pointer_probes_use_the_pointer_index() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("mirror_pointer_probe_plan")).await?;
    let mut tx = database.pool().begin().await?;
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *tx)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *tx).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT $1, '0x' || lpad(to_hex(i), 64, '0'), i, to_timestamp(i), 'canonical'
         FROM generate_series(1, 100) i",
    )
    .bind(CHAIN)
    .execute(&mut *tx)
    .await?;
    // 20,000 genuine pointers on unrelated nodes, 20,000 derived child pointers whose `node` is
    // the shared parent, and the queried child's own derived pointer.
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version, chain_id,
             block_number, block_hash, derivation_kind, canonicality_state, after_state
         )
         SELECT 'plan:' || kind || ':' || i, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1, $1,
                1 + i % 100, '0x' || lpad(to_hex(1 + i % 100), 64, '0'),
                'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
                CASE kind
                    WHEN 'genuine' THEN jsonb_build_object(
                        'node', '0x' || lpad(to_hex(i), 64, 'a'),
                        'resolver', '0x1111111111111111111111111111111111111111')
                    ELSE jsonb_build_object(
                        'node', $2::text,
                        'child_node', '0x' || lpad(to_hex(i), 64, 'b'),
                        'resolver', '0x1111111111111111111111111111111111111111')
                END
         FROM generate_series(1, 20000) i CROSS JOIN (VALUES ('genuine'), ('derived')) kinds(kind)
         UNION ALL
         SELECT 'plan:child', 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1, $1,
                50, '0x' || lpad(to_hex(50), 64, '0'), 'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                jsonb_build_object('node', $2::text, 'child_node', $3::text,
                                   'resolver', '0x2222222222222222222222222222222222222222')",
    )
    .bind(CHAIN)
    .bind(PARENT)
    .bind(CHILD)
    .execute(&mut *tx)
    .await?;
    raw_sql(&format!(
        "CREATE TEMP TABLE project_mirror_wanted(namespace text, namehash text) ON COMMIT DROP;
         CREATE TEMP TABLE project_mirror_cached_nodes(namespace text, namehash text, resource_id uuid,
             UNIQUE NULLS NOT DISTINCT(namespace, namehash, resource_id)) ON COMMIT DROP;
         CREATE TEMP TABLE project_mirror_evidence_nodes(namespace text, namehash text,
             PRIMARY KEY(namespace, namehash)) ON COMMIT DROP;
         CREATE TEMP TABLE project_mirror_evidence_events(normalized_event_id bigint PRIMARY KEY)
             ON COMMIT DROP;
         INSERT INTO project_mirror_wanted VALUES ('ens', '{CHILD}'), ('ens', '{PARENT}');
         INSERT INTO project_mirror_evidence_nodes SELECT * FROM project_mirror_wanted;
         ANALYZE normalized_events; ANALYZE chain_lineage;
         ANALYZE project_mirror_wanted; ANALYZE project_mirror_evidence_nodes;
         SET LOCAL enable_seqscan = off"
    ))
    .execute(&mut *tx)
    .await?;

    for (label, statement) in [
        ("cached nodes", cached_nodes_probe()),
        ("evidence events", EVIDENCE_EVENTS_SQL),
    ] {
        let plan = sqlx::query_scalar::<_, String>(&format!("EXPLAIN (COSTS OFF) {statement}"))
            .bind(CHAIN)
            .bind(100_i64)
            .fetch_all(&mut *tx)
            .await?
            .join("\n");
        eprintln!("{label}:\n{plan}");
        ensure!(
            plan.contains(INDEX),
            "{label} probe must use {INDEX}:\n{plan}"
        );
        ensure!(
            !plan.contains("Seq Scan on normalized_events"),
            "{label} probe must not scan all normalized events:\n{plan}"
        );
        sqlx::query(statement)
            .bind(CHAIN)
            .bind(100_i64)
            .execute(&mut *tx)
            .await?;
    }

    // Only the child's own pointer is filed under the child, and nothing is filed under the
    // parent, although 20,001 events carry the parent in `node`.
    let cached: Vec<String> =
        sqlx::query_scalar("SELECT namehash FROM project_mirror_cached_nodes ORDER BY namehash")
            .fetch_all(&mut *tx)
            .await?;
    ensure!(cached == [CHILD], "cached nodes {cached:?}");
    let evidence: Vec<String> = sqlx::query_scalar(
        "SELECT event.event_identity FROM project_mirror_evidence_events evidence
         JOIN normalized_events event USING (normalized_event_id)",
    )
    .fetch_all(&mut *tx)
    .await?;
    ensure!(evidence == ["plan:child"], "evidence events {evidence:?}");

    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
