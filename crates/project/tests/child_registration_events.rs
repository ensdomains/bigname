//! `child_registration_events`: the historical membership name history reads for
//! `include=child_registrations` (docs/projections.md, "Child registration events").
use alloy_primitives::{B256, keccak256};
use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const V2: &str = "ens_v2_registry_l1";

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

fn fork_hash(block: i64) -> String {
    format!("0x{:064x}", 0xf000 + block)
}

fn labelhashes(name: &str) -> Vec<B256> {
    name.split('.')
        .map(|label| keccak256(label.as_bytes()))
        .collect()
}

fn namehash(name: &str) -> B256 {
    labelhashes(name)
        .iter()
        .rev()
        .fold(B256::ZERO, |node, label| {
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(node.as_slice());
            input[32..].copy_from_slice(label.as_slice());
            keccak256(input)
        })
}

fn id(name: &str) -> String {
    format!("ens:{:#x}", namehash(name))
}

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *tx)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
        name.replace('"', r#""""#)
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *tx)
        .await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connection = pool.acquire().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    drop(connection);
    for block in 10..=14 {
        sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, $3, to_timestamp(1800000000 + $3), 'canonical')")
            .bind(CHAIN).bind(hash(block)).bind(block).execute(&pool).await?;
    }
    Ok((database, pool))
}

async fn surface(pool: &PgPool, name: &str, visibility: &str) -> Result<()> {
    let labels = name.split('.').map(str::to_owned).collect::<Vec<_>>();
    let hashes = labelhashes(name)
        .iter()
        .map(|labelhash| format!("{labelhash:#x}"))
        .collect::<Vec<_>>();
    let (reason, deactivated) = if visibility == "shadow" {
        (Some("fixture"), Some(1_800_000_010_i64))
    } else {
        (None, None)
    };
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, deactivation_reason, deactivated_at, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'ens', $2, $3, '\\x00', $4, $5, 'ensip15', $6, $7, to_timestamp($8), $9, $10, 10, 'canonical')")
        .bind(id(name)).bind(name).bind(labels).bind(format!("{:#x}", namehash(name))).bind(hashes)
        .bind(visibility).bind(reason).bind(deactivated).bind(CHAIN).bind(hash(10))
        .execute(pool).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn event(
    pool: &PgPool,
    identity: &str,
    name: &str,
    kind: &str,
    block_hash: &str,
    block: i64,
    log: i64,
    after: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref) VALUES ($1, 'ens', $2, $3, $4, 1, $5, $6, $7, $8, 0, $9, 'ens_v2_registry_resource_surface', 'canonical', $10, '{}')")
        .bind(identity).bind(id(name)).bind(kind).bind(V2).bind(CHAIN).bind(block)
        .bind(block_hash).bind(format!("0xtx{block}")).bind(log).bind(after)
        .execute(pool).await?;
    Ok(())
}

async fn grant(pool: &PgPool, identity: &str, name: &str, block: i64, log: i64) -> Result<()> {
    event(
        pool,
        identity,
        name,
        "RegistrationGranted",
        &hash(block),
        block,
        log,
        json!({"status": "registered"}),
    )
    .await
}

async fn project(pool: &PgPool, target: i64, resume: Option<i64>, mode: RunMode) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: resume.map_or(0, |number| number + 1),
            affected_to_block: target,
            resume_current: resume.map(|number| Marker {
                number,
                hash: hash(number),
            }),
            mode,
        })
        .await?;
    Ok(())
}

/// Published rows as `(parent, event identity, child, block, block hash)`, ignoring the target
/// and maintenance columns that legitimately differ between publications.
async fn rows(pool: &PgPool) -> Result<Vec<(String, String, String, i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT parent_logical_name_id, event_identity, child_logical_name_id, block_number, block_hash
         FROM child_registration_events ORDER BY parent_logical_name_id, event_identity",
    )
    .fetch_all(pool)
    .await?)
}

fn row(
    parent: &str,
    identity: &str,
    child: &str,
    block: i64,
) -> (String, String, String, i64, String) {
    (
        id(parent),
        identity.to_owned(),
        id(child),
        block,
        hash(block),
    )
}

async fn seed(pool: &PgPool) -> Result<()> {
    for name in [
        "eth",
        "p.eth",
        "q.eth",
        "a.p.eth",
        "a.q.eth",
        "b.p.eth",
        "x.a.p.eth",
    ] {
        surface(pool, name, "active").await?;
    }
    surface(pool, "bad.p.eth", "shadow").await?;
    // The parents' own registrations are children of `eth`, which the table never lists.
    grant(pool, "p-grant", "p.eth", 10, 0).await?;
    grant(pool, "q-grant", "q.eth", 10, 1).await?;
    // A registry reachable under p.eth grants `a`; the child is later released.
    grant(pool, "a-p-grant", "a.p.eth", 10, 2).await?;
    event(
        pool,
        "a-p-release",
        "a.p.eth",
        "RegistrationReleased",
        &hash(11),
        11,
        0,
        json!({}),
    )
    .await?;
    // The same registry later hangs under q.eth: its later grants carry q.eth names.
    grant(pool, "a-q-grant", "a.q.eth", 12, 0).await?;
    // A grandchild belongs to a.p.eth, not to p.eth.
    grant(pool, "x-a-p-grant", "x.a.p.eth", 11, 1).await?;
    // A label that fails normalization is not served as a name.
    grant(pool, "bad-p-grant", "bad.p.eth", 11, 2).await?;
    // A registrar surface snapshot is already a product duplicate.
    event(
        pool,
        "b-p-snapshot",
        "b.p.eth",
        "RegistrationGranted",
        &hash(11),
        11,
        3,
        json!({"state_derived": true, "registrar_surface_snapshot": true}),
    )
    .await?;
    // Candidate rows never reach Project.
    event(
        pool,
        "b-p-candidate",
        "b.p.eth",
        "RegistrationGranted",
        &hash(11),
        11,
        4,
        json!({}),
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['fixture'] WHERE event_identity = 'b-p-candidate'")
        .execute(pool).await?;
    // A reachability grant carries the name the label reached; membership needs no registry
    // instance, so a grant restored without one still counts.
    event(
        pool,
        "b-p-topology",
        "b.p.eth",
        "RegistrationGranted",
        &hash(12),
        12,
        1,
        json!({"source_event": "ParentUpdated", "status": "registered"}),
    )
    .await?;
    // A registration after release is another row.
    grant(pool, "a-p-regrant", "a.p.eth", 13, 0).await?;
    Ok(())
}

fn seeded_rows() -> Vec<(String, String, String, i64, String)> {
    let mut expected = vec![
        row("p.eth", "a-p-grant", "a.p.eth", 10),
        row("p.eth", "a-p-regrant", "a.p.eth", 13),
        row("p.eth", "b-p-topology", "b.p.eth", 12),
        row("a.p.eth", "x-a-p-grant", "x.a.p.eth", 11),
        row("q.eth", "a-q-grant", "a.q.eth", 12),
    ];
    expected.sort();
    expected
}

#[tokio::test]
async fn membership_follows_event_time_names_and_keeps_history() -> Result<()> {
    let (database, pool) = database("child_registration_membership").await?;
    seed(&pool).await?;
    project(&pool, 13, None, RunMode::Normal).await?;
    assert_eq!(rows(&pool).await?, seeded_rows());
    let (order_key, log_key, provenance): (String, i64, Value) = sqlx::query_as(
        "SELECT transaction_order_key, log_order_key, provenance FROM child_registration_events WHERE event_identity = 'a-p-grant'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!((order_key.as_str(), log_key), ("0xtx10", 2));
    assert_eq!(provenance["source_family"], json!(V2));
    database.cleanup().await
}

#[tokio::test]
async fn incremental_and_redo_publications_match_a_full_rebuild() -> Result<()> {
    let (database, pool) = database("child_registration_incremental").await?;
    seed(&pool).await?;
    project(&pool, 10, None, RunMode::Normal).await?;
    assert_eq!(
        rows(&pool).await?,
        vec![row("p.eth", "a-p-grant", "a.p.eth", 10)]
    );
    project(&pool, 12, Some(10), RunMode::Normal).await?;
    project(&pool, 13, Some(12), RunMode::Normal).await?;
    let incremental = rows(&pool).await?;

    // A reorg replaces block 13: the old grant goes, the winning fork's grant arrives.
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_number = 13")
        .bind(CHAIN).execute(&pool).await?;
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE block_number = 13",
    )
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 13, to_timestamp(1800000013), 'canonical')")
        .bind(CHAIN).bind(fork_hash(13)).execute(&pool).await?;
    event(
        &pool,
        "b-p-fork-grant",
        "b.p.eth",
        "RegistrationGranted",
        &fork_hash(13),
        13,
        0,
        json!({}),
    )
    .await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 13,
            affected_from_block: 13,
            affected_to_block: 13,
            resume_current: Some(Marker {
                number: 12,
                hash: hash(12),
            }),
            mode: RunMode::Redo,
        })
        .await?;
    let redone = rows(&pool).await?;
    assert!(
        redone.iter().all(|row| row.1 != "a-p-regrant"),
        "{redone:?}"
    );
    assert!(redone.contains(&(
        id("p.eth"),
        "b-p-fork-grant".to_owned(),
        id("b.p.eth"),
        13,
        fork_hash(13)
    )));

    // A fresh full rebuild over the same inputs publishes exactly the incremental result.
    project(&pool, 13, None, RunMode::Normal).await?;
    assert_eq!(rows(&pool).await?, redone);
    // And the pre-reorg incremental chain matched the pre-reorg full rebuild.
    assert_eq!(incremental, seeded_rows());
    database.cleanup().await
}

#[tokio::test]
async fn an_operator_redo_below_the_head_keeps_later_rows() -> Result<()> {
    let (database, pool) = database("child_registration_partial_redo").await?;
    seed(&pool).await?;
    project(&pool, 13, None, RunMode::Normal).await?;
    let before = rows(&pool).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 11,
            affected_from_block: 11,
            affected_to_block: 11,
            resume_current: Some(Marker {
                number: 10,
                hash: hash(10),
            }),
            mode: RunMode::Redo,
        })
        .await?;
    assert_eq!(rows(&pool).await?, before);
    database.cleanup().await
}

/// A redo whose range ends below a reorged block still removes that block's rows: the orphaned
/// row lies above `affected_to_block`, so only the readable-lineage check can reach it.
#[tokio::test]
async fn a_redo_below_a_reorged_block_drops_its_orphaned_rows() -> Result<()> {
    let (database, pool) = database("child_registration_orphan_above_range").await?;
    seed(&pool).await?;
    project(&pool, 13, None, RunMode::Normal).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_number = 13")
        .bind(CHAIN).execute(&pool).await?;
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE block_number = 13",
    )
    .execute(&pool)
    .await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 11,
            affected_from_block: 11,
            affected_to_block: 11,
            resume_current: Some(Marker {
                number: 10,
                hash: hash(10),
            }),
            mode: RunMode::Redo,
        })
        .await?;
    let expected = seeded_rows()
        .into_iter()
        .filter(|row| row.1 != "a-p-regrant")
        .collect::<Vec<_>>();
    assert_eq!(rows(&pool).await?, expected);
    database.cleanup().await
}

const SERVED_TABLES: [&str; 11] = [
    "name_current",
    "children_current",
    "permissions_current",
    "account_permission_state_current",
    "permissions_current_resource_summary",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "address_records_current",
    "primary_names_current",
    "child_registration_events",
];

async fn table_rows(pool: &PgPool) -> Result<Vec<u64>> {
    let mut counts = Vec::new();
    for table in SERVED_TABLES {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await?;
        counts.push(u64::try_from(count)?);
    }
    Ok(counts)
}

/// Every served table ends each batch with the rows it had, less the rows the summary says the
/// batch deleted, plus the rows it says the batch inserted.
async fn assert_summary_matches_row_deltas(
    pool: &PgPool,
    request: BatchRequest,
) -> Result<bigname_project::WriteSummary> {
    let before = table_rows(pool).await?;
    let summary = Engine::new(pool.clone())
        .run_batch(request)
        .await?
        .write_summary;
    let after = table_rows(pool).await?;
    for ((table, before), after) in SERVED_TABLES.into_iter().zip(before).zip(after) {
        let deleted = summary.deleted.get(table).copied();
        let inserted = summary.inserted.get(table).copied();
        assert_eq!(
            Some(after),
            deleted
                .zip(inserted)
                .map(|(deleted, inserted)| before - deleted + inserted),
            "{table}: {before} rows before, {after} after, summary {summary:?}"
        );
    }
    assert_eq!(
        summary.stage_elapsed_ms.keys().copied().collect::<Vec<_>>(),
        [
            "builders",
            "inputs",
            "integrity",
            "prepare",
            "publish",
            "scope"
        ]
    );
    Ok(summary)
}

#[tokio::test]
async fn the_write_summary_counts_the_rows_each_batch_replaced() -> Result<()> {
    let (database, pool) = database("child_registration_write_summary").await?;
    seed(&pool).await?;
    let request = |target: i64, resume: Option<i64>, from: i64, mode: RunMode| BatchRequest {
        chain_id: CHAIN.into(),
        target_block: target,
        affected_from_block: from,
        affected_to_block: target,
        resume_current: resume.map(|number| Marker {
            number,
            hash: hash(number),
        }),
        mode,
    };

    let full =
        assert_summary_matches_row_deltas(&pool, request(10, None, 0, RunMode::Normal)).await?;
    assert_eq!((full.blocks, full.changed_events), (11, 0));
    assert_eq!(full.inserted["child_registration_events"], 1);
    assert!(full.staged_events > 0);

    let incremental =
        assert_summary_matches_row_deltas(&pool, request(12, Some(10), 11, RunMode::Normal))
            .await?;
    assert_eq!(incremental.blocks, 2);
    assert!(incremental.changed_events > 0);
    assert!(incremental.scope_keys["names"] > 0);
    assert_eq!(
        incremental.scope_keys.keys().copied().collect::<Vec<_>>(),
        [
            "account_permissions",
            "children",
            "names",
            "primary",
            "resolvers",
            "resources"
        ]
    );

    // A redo of block 12 deletes and republishes its rows.
    let redo =
        assert_summary_matches_row_deltas(&pool, request(12, Some(11), 12, RunMode::Redo)).await?;
    assert_eq!(redo.blocks, 1);
    assert!(redo.deleted["child_registration_events"] > 0);
    assert_eq!(
        redo.deleted["child_registration_events"],
        redo.inserted["child_registration_events"]
    );
    database.cleanup().await
}
