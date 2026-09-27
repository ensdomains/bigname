//! The serving fence under the publication switch (TYR-36 step 7b): with the switch on, snapshot
//! selection, the served generation and `/v1/status`'s generation check read the family marker
//! (`state = 'live'`, this build's interpreter hash, readable lineage, at most one block behind
//! the head) and report its `sequence` as the generation; with the switch off they read the
//! Project row of `chain_phase_state` exactly as before.

use anyhow::Result;
use bigname_storage::publication_source::with_serve_from_families;
use bigname_storage::{
    ChainPositions, ChildrenCurrentPageFilter, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotSelectionErrorKind, SnapshotSelectionScope, SnapshotSelectorInput,
    load_children_current_page_filtered, load_phase_indexing_status,
    load_served_project_generation, resolve_exact_name_snapshot_selection,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};

const CHAIN_ID: &str = "ethereum-mainnet";
const HASH_9: &str = "0x0900000000000000000000000000000000000000000000000000000000000000";
const HASH_10: &str = "0x1000000000000000000000000000000000000000000000000000000000000000";
const HASH_11: &str = "0x1100000000000000000000000000000000000000000000000000000000000000";
const HASH_11_ORPHANED: &str = "0x11dead0000000000000000000000000000000000000000000000000000000000";
const SEQUENCE: i64 = 7;

const BASELINE: &[&str] = &[
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
    include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    include_str!("../../../schema-v2/baseline/12_project_generation_failures.sql"),
    include_str!("../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
];

/// A head at block 11 with both publications on it: the Project row (`completed`) and a live
/// family marker at `SEQUENCE`. Each test then moves one of them.
async fn fixture(name: &str) -> Result<TestDatabase> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let pool = database.pool();
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(pool)
        .await?;
    for (number, hash, state) in [
        (9, HASH_9, "canonical"),
        (10, HASH_10, "canonical"),
        (11, HASH_11, "canonical"),
        (11, HASH_11_ORPHANED, "orphaned"),
    ] {
        sqlx::query(
            "INSERT INTO chain_lineage
                 (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, TIMESTAMPTZ '2026-04-17 00:00:00+00' + $3 * INTERVAL '12 seconds', $4::canonicality_state)",
        )
        .bind(CHAIN_ID)
        .bind(hash)
        .bind(number)
        .bind(state)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, 11)",
    )
    .bind(CHAIN_ID)
    .bind(HASH_11)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_phase_state
             (chain_id, phase_name, phase_status, current_block_number, current_block_hash,
              target_block_number, target_block_hash, input_content_hash, started_at, finished_at)
         VALUES ($1, 'project', 'completed', 11, $2, 11, $2, $3, now(), now()),
                ($1, 'interpret', 'completed', 11, $2, 11, $2, $3, now(), now())",
    )
    .bind(CHAIN_ID)
    .bind(HASH_11)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO project_family_marker
             (chain_id, current_block_number, current_block_hash, block_timestamp,
              input_content_hash, sequence, state)
         SELECT chain_id, block_number, block_hash, block_timestamp, $3, $4, 'live'
         FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(CHAIN_ID)
    .bind(HASH_11)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .bind(SEQUENCE)
    .execute(pool)
    .await?;
    Ok(database)
}

async fn update(pool: &PgPool, statement: &str) -> Result<()> {
    sqlx::query(statement).execute(pool).await?;
    Ok(())
}

/// Moves the marker to `hash` (and that block's number and timestamp).
async fn move_marker(pool: &PgPool, hash: &str) -> Result<()> {
    sqlx::query(
        "UPDATE project_family_marker marker
         SET current_block_number = lineage.block_number,
             current_block_hash = lineage.block_hash,
             block_timestamp = lineage.block_timestamp
         FROM chain_lineage lineage
         WHERE lineage.chain_id = marker.chain_id AND lineage.block_hash = $1",
    )
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn generation(pool: &PgPool, on: bool, hash: &str, number: i64) -> Result<Option<String>> {
    Ok(with_serve_from_families(
        on,
        load_served_project_generation(pool, CHAIN_ID, number, hash, true, true),
    )
    .await?)
}

async fn project_xmin(pool: &PgPool) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT xmin::text FROM chain_phase_state WHERE phase_name = 'project'")
            .fetch_one(pool)
            .await?,
    )
}

fn scope() -> SnapshotSelectionScope {
    SnapshotSelectionScope::new(
        vec![SnapshotPositionRequirement::new("ethereum", CHAIN_ID)],
        Some("ethereum".to_owned()),
    )
    .expect("single-chain scope is valid")
}

async fn select(
    pool: &PgPool,
    on: bool,
) -> bigname_storage::SnapshotSelectionResult<ChainPositions> {
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)?;
    let scope = scope();
    with_serve_from_families(on, async {
        resolve_exact_name_snapshot_selection(pool, &scope, &input)
            .await
            .map(|selected| selected.chain_positions)
    })
    .await
}

fn selected_number(positions: &ChainPositions) -> i64 {
    positions
        .as_map()
        .values()
        .next()
        .expect("one selected position")
        .block_number
}

async fn generation_current(pool: &PgPool, on: bool) -> Result<bool> {
    let status = with_serve_from_families(on, load_phase_indexing_status(pool)).await?;
    Ok(status
        .chains
        .iter()
        .find(|chain| chain.chain_id == CHAIN_ID)
        .expect("the chain has a status row")
        .project_generation_current)
}

#[tokio::test]
async fn a_live_marker_at_the_head_is_served_with_its_sequence_as_the_generation() -> Result<()> {
    let database = fixture("family_marker_fence_live").await?;
    let pool = database.pool().clone();

    assert_eq!(
        generation(&pool, true, HASH_11, 11).await?,
        Some(SEQUENCE.to_string()),
        "switch on: the generation is the marker's sequence"
    );
    assert_eq!(
        generation(&pool, false, HASH_11, 11).await?,
        Some(project_xmin(&pool).await?),
        "switch off: the generation is the Project row's xmin"
    );
    assert_eq!(selected_number(&select(&pool, true).await?), 11);
    assert!(generation_current(&pool, true).await?);
    assert!(generation_current(&pool, false).await?);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_bootstrap_pending_marker_is_stale_only_with_the_switch_on() -> Result<()> {
    let database = fixture("family_marker_fence_bootstrap").await?;
    let pool = database.pool().clone();
    update(
        &pool,
        "UPDATE project_family_marker SET state = 'bootstrap_pending'",
    )
    .await?;

    assert_eq!(generation(&pool, true, HASH_11, 11).await?, None);
    let error = select(&pool, true)
        .await
        .expect_err("a marker still populating the families is not servable");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);
    assert!(!generation_current(&pool, true).await?);

    assert_eq!(
        generation(&pool, false, HASH_11, 11).await?,
        Some(project_xmin(&pool).await?)
    );
    assert_eq!(selected_number(&select(&pool, false).await?), 11);
    assert!(generation_current(&pool, false).await?);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_from_another_interpreter_generation_is_stale_only_with_the_switch_on()
-> Result<()> {
    let database = fixture("family_marker_fence_hash").await?;
    let pool = database.pool().clone();
    update(
        &pool,
        "UPDATE project_family_marker SET input_content_hash = 'another-build'",
    )
    .await?;

    assert_eq!(generation(&pool, true, HASH_11, 11).await?, None);
    assert_eq!(
        select(&pool, true).await.expect_err("stale").kind(),
        SnapshotSelectionErrorKind::Stale
    );
    assert!(!generation_current(&pool, true).await?);

    assert!(generation(&pool, false, HASH_11, 11).await?.is_some());
    assert_eq!(selected_number(&select(&pool, false).await?), 11);
    assert!(generation_current(&pool, false).await?);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_beyond_the_lag_tolerance_is_stale_only_with_the_switch_on() -> Result<()> {
    let database = fixture("family_marker_fence_lag").await?;
    let pool = database.pool().clone();
    move_marker(&pool, HASH_9).await?;

    assert_eq!(generation(&pool, true, HASH_9, 9).await?, None);
    assert_eq!(
        select(&pool, true).await.expect_err("stale").kind(),
        SnapshotSelectionErrorKind::Stale
    );

    assert_eq!(selected_number(&select(&pool, false).await?), 11);
    assert!(generation(&pool, false, HASH_11, 11).await?.is_some());

    // One block behind is within tolerance: the marker's block is the served position.
    move_marker(&pool, HASH_10).await?;
    assert_eq!(selected_number(&select(&pool, true).await?), 10);
    assert_eq!(
        generation(&pool, true, HASH_10, 10).await?,
        Some(SEQUENCE.to_string())
    );
    assert_eq!(selected_number(&select(&pool, false).await?), 11);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_on_an_orphaned_block_is_stale_only_with_the_switch_on() -> Result<()> {
    let database = fixture("family_marker_fence_orphan").await?;
    let pool = database.pool().clone();
    move_marker(&pool, HASH_11_ORPHANED).await?;

    assert_eq!(generation(&pool, true, HASH_11_ORPHANED, 11).await?, None);
    assert_eq!(generation(&pool, true, HASH_11, 11).await?, None);
    assert_eq!(
        select(&pool, true).await.expect_err("stale").kind(),
        SnapshotSelectionErrorKind::Stale
    );

    assert!(generation(&pool, false, HASH_11, 11).await?.is_some());
    assert_eq!(selected_number(&select(&pool, false).await?), 11);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn an_expiry_filtered_children_page_needs_an_evaluation_time() -> Result<()> {
    let database = fixture("family_marker_fence_children_clock").await?;
    let pool = database.pool().clone();
    let filter = ChildrenCurrentPageFilter {
        include_expired: false,
        evaluated_at: None,
        ..ChildrenCurrentPageFilter::default()
    };

    let error = load_children_current_page_filtered(&pool, "ens:parent.eth", &filter, None, 10)
        .await
        .expect_err("the expiry fence never falls back to the database clock");
    assert!(
        format!("{error:#}").contains("evaluation time"),
        "unexpected error: {error:#}"
    );

    drop(pool);
    database.cleanup().await
}
