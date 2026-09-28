//! Permanent family marker admission: generation, canonical lineage, lag, build and status.

#[path = "../../project/tests/families_support/mod.rs"]
mod family_support;

use anyhow::Result;
use bigname_storage::{
    ChainPositions, ChildrenCurrentPageFilter, SnapshotAt, SnapshotConsistency,
    SnapshotPositionRequirement, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, load_children_current_page_filtered,
    load_phase_indexing_status, load_served_project_generation, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection,
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

async fn generation(pool: &PgPool, hash: &str, number: i64) -> Result<Option<String>> {
    Ok(load_served_project_generation(pool, CHAIN_ID, number, hash, true, true).await?)
}

fn scope() -> SnapshotSelectionScope {
    SnapshotSelectionScope::new(
        vec![SnapshotPositionRequirement::new("ethereum", CHAIN_ID)],
        Some("ethereum".to_owned()),
    )
    .expect("single-chain scope is valid")
}

async fn select(pool: &PgPool) -> bigname_storage::SnapshotSelectionResult<ChainPositions> {
    select_input(pool, None).await
}

/// A historical `at` read at block 11's timestamp, which the selection checks against the current
/// publication after resolving the position.
async fn select_at_block_11(
    pool: &PgPool,
) -> bigname_storage::SnapshotSelectionResult<ChainPositions> {
    let at = parse_rfc3339_utc_timestamp("2026-04-17T00:02:12Z")?;
    select_input(pool, Some(SnapshotAt::Timestamp(at))).await
}

async fn select_input(
    pool: &PgPool,
    at: Option<SnapshotAt>,
) -> bigname_storage::SnapshotSelectionResult<ChainPositions> {
    let input = SnapshotSelectorInput::new(at, None, SnapshotConsistency::Head)?;
    let scope = scope();
    resolve_exact_name_snapshot_selection(pool, &scope, &input)
        .await
        .map(|selected| selected.chain_positions)
}

fn selected_number(positions: &ChainPositions) -> i64 {
    positions
        .as_map()
        .values()
        .next()
        .expect("one selected position")
        .block_number
}

async fn generation_current(pool: &PgPool) -> Result<bool> {
    let status = load_phase_indexing_status(pool).await?;
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
        generation(&pool, HASH_11, 11).await?,
        Some(SEQUENCE.to_string()),
        "the generation is the marker's sequence"
    );

    assert_eq!(selected_number(&select(&pool).await?), 11);
    assert!(generation_current(&pool).await?);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_bootstrap_pending_marker_is_stale() -> Result<()> {
    let database = fixture("family_marker_fence_bootstrap").await?;
    let pool = database.pool().clone();
    update(
        &pool,
        "UPDATE project_family_marker SET state = 'bootstrap_pending'",
    )
    .await?;

    assert_eq!(generation(&pool, HASH_11, 11).await?, None);
    let error = select(&pool)
        .await
        .expect_err("a marker still populating the families is not servable");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);
    assert!(!generation_current(&pool).await?);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_from_another_interpreter_generation_is_stale() -> Result<()> {
    let database = fixture("family_marker_fence_hash").await?;
    let pool = database.pool().clone();
    update(
        &pool,
        "UPDATE project_family_marker SET input_content_hash = 'another-build'",
    )
    .await?;

    assert_eq!(generation(&pool, HASH_11, 11).await?, None);
    assert_eq!(
        select(&pool).await.expect_err("stale").kind(),
        SnapshotSelectionErrorKind::Stale
    );
    assert!(!generation_current(&pool).await?);

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_beyond_the_lag_tolerance_is_stale() -> Result<()> {
    let database = fixture("family_marker_fence_lag").await?;
    let pool = database.pool().clone();
    move_marker(&pool, HASH_9).await?;

    assert_eq!(generation(&pool, HASH_9, 9).await?, None);
    assert_eq!(
        select(&pool).await.expect_err("stale").kind(),
        SnapshotSelectionErrorKind::Stale
    );

    // One block behind is within tolerance: the marker's block is the served position.
    move_marker(&pool, HASH_10).await?;
    assert_eq!(selected_number(&select(&pool).await?), 10);
    assert_eq!(
        generation(&pool, HASH_10, 10).await?,
        Some(SEQUENCE.to_string())
    );

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_on_an_orphaned_block_is_stale() -> Result<()> {
    let database = fixture("family_marker_fence_orphan").await?;
    let pool = database.pool().clone();
    move_marker(&pool, HASH_11_ORPHANED).await?;

    assert_eq!(generation(&pool, HASH_11_ORPHANED, 11).await?, None);
    assert_eq!(generation(&pool, HASH_11, 11).await?, None);
    assert_eq!(
        select(&pool).await.expect_err("stale").kind(),
        SnapshotSelectionErrorKind::Stale
    );

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn expiry_filtered_children_use_the_publication_clock_or_explicit_evaluation() -> Result<()> {
    use bigname_project::families::FamilyMode;
    use family_support::{Event, Fixture, uuid};
    use serde_json::json;
    let fixture = Fixture::new("family_marker_children_clock", 2).await?;
    // Install a historical fixture clock before any identity or event references the lineage.
    sqlx::raw_sql("DELETE FROM chain_lineage; INSERT INTO chain_lineage (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state) SELECT 'ethereum-sepolia','0x'||lpad(to_hex(n),64,'0'),CASE WHEN n>0 THEN '0x'||lpad(to_hex(n-1),64,'0') END,n,to_timestamp(946684800+n*12),'canonical' FROM generate_series(0,2) n")
        .execute(&fixture.pool).await?;
    let parent_node = format!("0x{:064x}", 1);
    let child_node = format!("0x{:064x}", 2);
    let parent = format!("ens:{parent_node}");
    let child = format!("ens:{child_node}");
    fixture.surface(&parent, &parent_node).await?;
    fixture
        .binding(&uuid(1), &child, &uuid(2), "ens_v1", 0, 0, None)
        .await?;
    sqlx::raw_sql(
        "UPDATE surface_bindings SET active_from=to_timestamp(946684800+block_number*12)",
    )
    .execute(&fixture.pool)
    .await?;
    fixture.event(Event::new("clock-edge",1,0,"SubregistryChanged","ens_v1_registry_l1")
        .name(&child).resource(&uuid(2)).after(json!({"source_event":"NewOwner","node":parent_node,"child_node":child_node,"labelhash":format!("0x{:064x}",3),"owner":"0x00000000000000000000000000000000000000a1"}))).await?;
    fixture.event(Event::new("clock-registration",1,1,"RegistrationGranted","ens_v1_registrar_l1")
        .name(&child).resource(&uuid(2)).after(json!({"authority_kind":"registrar","registrant":"0x00000000000000000000000000000000000000a1","expiry":946684840}))).await?;
    fixture.apply(2, FamilyMode::Rebuild).await?;
    let mut filter = ChildrenCurrentPageFilter {
        include_expired: false,
        ..ChildrenCurrentPageFilter::default()
    };
    let page =
        load_children_current_page_filtered(&fixture.pool, &parent, &filter, None, 10).await?;
    assert_eq!(
        page.total_count, 1,
        "the child is live at the publication's historical clock, even though wall time is past its expiry"
    );
    filter.evaluated_at = Some(parse_rfc3339_utc_timestamp("2000-01-01T00:01:00Z")?);
    let page =
        load_children_current_page_filtered(&fixture.pool, &parent, &filter, None, 10).await?;
    assert_eq!(
        page.total_count, 0,
        "the explicit snapshot clock applies the same expiry predicate"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_chain_without_a_marker_row_is_stale_not_an_error() -> Result<()> {
    let database = fixture("family_marker_fence_no_row").await?;
    let pool = database.pool().clone();
    update(&pool, "DELETE FROM project_family_marker").await?;

    assert_eq!(generation(&pool, HASH_11, 11).await?, None);
    for (read, error) in [
        ("head", select(&pool).await.expect_err("no marker row")),
        (
            "at",
            select_at_block_11(&pool).await.expect_err("no marker row"),
        ),
    ] {
        assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale, "{read}");
    }
    assert!(
        !generation_current(&pool).await?,
        "/v1/status: no marker row is not a current generation"
    );

    drop(pool);
    database.cleanup().await
}

fn stale_message(
    result: bigname_storage::SnapshotSelectionResult<ChainPositions>,
) -> SnapshotSelectionError {
    let error = result.expect_err("the publication is not servable");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale, "{error}");
    error
}

#[tokio::test]
async fn the_stale_message_names_the_family_marker() -> Result<()> {
    let database = fixture("family_marker_fence_wording").await?;
    let pool = database.pool().clone();
    let families = format!(
        "chain {CHAIN_ID} owned key families are not published at its current schema-v2 head"
    );
    update(
        &pool,
        "UPDATE project_family_marker SET state='bootstrap_pending'",
    )
    .await?;
    assert_eq!(stale_message(select(&pool).await).message(), families);
    assert_eq!(
        stale_message(select_at_block_11(&pool).await).message(),
        families
    );
    update(&pool, "UPDATE project_family_marker SET state='live'").await?;
    move_marker(&pool, HASH_9).await?;
    assert_eq!(
        stale_message(select(&pool).await).message(),
        format!("{families} (publication at 9 lags head 11 beyond tolerance)")
    );
    assert_eq!(
        stale_message(select_at_block_11(&pool).await).message(),
        format!("{families} (publication at 9 lags head 11)")
    );
    drop(pool);
    database.cleanup().await
}

/// `/v1/status`'s `project_generation_current` applies the serving fence's rule to the marker
/// live state, this build's interpreter hash, readable lineage at the marker's block and hash,
/// and at most the lag tolerance behind the head.
#[tokio::test]
async fn status_generation_current_applies_the_serving_fence_rule() -> Result<()> {
    let database = fixture("family_marker_fence_status_rule").await?;
    let pool = database.pool().clone();

    for (hash, current, why) in [
        (
            HASH_9,
            false,
            "two blocks behind head 11 is beyond the lag tolerance",
        ),
        (
            HASH_10,
            true,
            "one block behind the head is within the lag tolerance",
        ),
        (HASH_11, true, "at the head"),
        (
            HASH_11_ORPHANED,
            false,
            "the marker's block is not on the readable lineage",
        ),
    ] {
        move_marker(&pool, hash).await?;
        assert_eq!(generation_current(&pool).await?, current, "{why}");
    }

    // Readable lineage is checked at the marker's own number and hash: block 10's lineage row
    // orphaned under a marker at block 10.
    move_marker(&pool, HASH_10).await?;
    update(
        &pool,
        "UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE block_number = 10",
    )
    .await?;
    assert!(
        !generation_current(&pool).await?,
        "an orphaned block under the marker"
    );

    drop(pool);
    database.cleanup().await
}
