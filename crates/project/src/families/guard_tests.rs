//! The block compare-and-swap: a family block applies only on top of the marker the loop planned
//! for, and only when that marker is the block's parent on the readable lineage.
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};

use super::{FamilyOptions, block, marker};
use crate::Marker;

pub(super) const CHAIN: &str = "ethereum-sepolia";

pub(super) fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

pub(super) async fn database() -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("families_guard").pool_max_connections(1))
            .await?;
    let pool = database.pool().clone();
    let mut tx = pool.begin().await?;
    for script in [
        "CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public;",
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    raw_sql("SET search_path TO bigname_phase, public")
        .execute(&pool)
        .await?;
    // Blocks 0 to 12, with block 12 recorded under a parent that is not block 11.
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         SELECT $1, '0x' || lpad(to_hex(block), 64, '0'),
                CASE WHEN block = 12 THEN '0xforeign'
                     WHEN block > 0 THEN '0x' || lpad(to_hex(block - 1), 64, '0') END,
                block, to_timestamp(1800000000 + block * 12), 'canonical'
         FROM generate_series(0, 12) block",
    )
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    Ok((database, pool))
}

/// The revision a chain with no Interpret row reads.
pub(super) const NO_INTERPRET: crate::families::Revision = (None, None);

async fn apply(pool: &PgPool, number: i64, predecessor: Option<&Marker>) -> crate::Result<()> {
    let sequence = marker::read(pool, CHAIN).await?.sequence;
    let plan = block::Plan {
        predecessor,
        sequence,
        contiguous: true,
        bootstrap: false,
        revision: &NO_INTERPRET,
        role: block::Role::Follow,
        manifests: &crate::families::manifests::History::default(),
    };
    block::apply(pool, CHAIN, number, &plan, &FamilyOptions::new("guard"))
        .await
        .map(|_| ())
}

#[tokio::test]
async fn a_block_applies_only_on_the_marker_the_loop_planned_for() -> Result<()> {
    let (database, pool) = database().await?;
    let ten = Marker {
        number: 10,
        hash: hash(10),
    };
    let refused = apply(&pool, 11, Some(&ten)).await;
    assert!(refused.is_err(), "the marker is empty, not block 10");
    assert_eq!(marker::read(&pool, CHAIN).await?.current, None);

    apply(&pool, 10, None).await.map_err(anyhow::Error::msg)?;
    let refused = apply(&pool, 10, Some(&ten)).await;
    assert!(refused.is_err(), "block 10 is already applied");
    apply(&pool, 11, Some(&ten))
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(
        marker::read(&pool, CHAIN)
            .await?
            .current
            .map(|marker| marker.number),
        Some(11)
    );
    database.cleanup().await
}

#[tokio::test]
async fn a_block_whose_parent_is_not_the_marker_is_refused() -> Result<()> {
    let (database, pool) = database().await?;
    apply(&pool, 11, None).await.map_err(anyhow::Error::msg)?;
    let eleven = Marker {
        number: 11,
        hash: hash(11),
    };
    let refused = apply(&pool, 12, Some(&eleven)).await;
    assert!(
        refused.is_err(),
        "block 12's recorded parent is not block 11"
    );
    assert_eq!(
        marker::read(&pool, CHAIN).await?.current,
        Some(eleven),
        "the refused block wrote nothing"
    );
    database.cleanup().await
}

/// A repair record in `state` at `attempt`, trusted base 10, target 12.
async fn record(pool: &PgPool, state: &str, attempt: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_repair_record (chain_id, attempt, reason, trusted_base_number,
             trusted_base_hash, replay_target_number, replay_target_hash, state,
             prefix_recorded)
         VALUES ($1, $2, 'operator_redo', 10, $3, 12, $4, $5, $5 <> 'undoing')",
    )
    .bind(CHAIN)
    .bind(attempt)
    .bind(hash(10))
    .bind(hash(12))
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

async fn apply_as(
    pool: &PgPool,
    number: i64,
    predecessor: Option<&Marker>,
    sequence: i64,
    role: block::Role,
) -> crate::Result<()> {
    let plan = block::Plan {
        predecessor,
        sequence,
        contiguous: true,
        bootstrap: false,
        revision: &NO_INTERPRET,
        role,
        manifests: &crate::families::manifests::History::default(),
    };
    block::apply(pool, CHAIN, number, &plan, &FamilyOptions::new("guard"))
        .await
        .map(|_| ())
}

// Every block is fenced on the marker generation it planned from and on the repair record its
// role names: a stale generation, a replay under another attempt, or a replay while the record
// is still undoing all write nothing.
#[tokio::test]
async fn a_block_is_fenced_by_the_generation_and_the_repair_attempt_it_planned_from() -> Result<()>
{
    let (database, pool) = database().await?;
    apply(&pool, 10, None).await.map_err(anyhow::Error::msg)?;
    let ten = Marker {
        number: 10,
        hash: hash(10),
    };
    let sequence = marker::read(&pool, CHAIN).await?.sequence;
    let stale = apply_as(&pool, 11, Some(&ten), sequence - 1, block::Role::Follow).await;
    assert!(stale.is_err(), "a stale marker generation is refused");

    record(&pool, "undoing", 1).await?;
    let replay = |attempt| block::Role::Replay {
        attempt,
        completes: false,
    };
    let early = apply_as(&pool, 11, Some(&ten), sequence, replay(1)).await;
    assert!(early.is_err(), "no replay while the record is undoing");
    sqlx::query("UPDATE project_repair_record SET state = 'replaying', prefix_recorded = true")
        .execute(&pool)
        .await?;
    let other = apply_as(&pool, 11, Some(&ten), sequence, replay(2)).await;
    assert!(other.is_err(), "a replay of another attempt is refused");
    let follow = apply_as(&pool, 11, Some(&ten), sequence, block::Role::Follow).await;
    assert!(follow.is_err(), "no plain follow while a repair is active");
    assert_eq!(marker::read(&pool, CHAIN).await?.current, Some(ten.clone()));

    apply_as(&pool, 11, Some(&ten), sequence, replay(1))
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(
        marker::read(&pool, CHAIN)
            .await?
            .current
            .map(|marker| marker.number),
        Some(11)
    );
    database.cleanup().await
}

#[test]
fn an_active_repair_keeps_the_undo_rows_above_its_trusted_base() {
    use super::repair::{Record, State};
    let mut record = Record {
        attempt: 1,
        reason: "operator_redo".to_owned(),
        trusted_base: Some(Marker {
            number: 10,
            hash: hash(10),
        }),
        replay_target: Marker {
            number: 20,
            hash: hash(20),
        },
        state: State::Undoing,
        prefix_revision: None,
        invalidation_from: None,
        pending_undo_target: Some(8),
        completed_sequence: None,
        completed_marker: None,
        completed_input_hash: None,
        reset_sequence: None,
    };
    assert_eq!(
        record.retention_floor(),
        Some(9),
        "the pending undo target bounds it"
    );
    record.pending_undo_target = None;
    record.state = State::Replaying;
    assert_eq!(record.retention_floor(), Some(11));
    record.state = State::Complete;
    assert_eq!(
        record.retention_floor(),
        None,
        "a finished repair keeps nothing"
    );
}
