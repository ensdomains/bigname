use std::collections::BTreeMap;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::{ChainRedoState, PhaseRedoState, PhaseRedoStateMissing, capture_history_bound_state};

const CHAIN: &str = "ethereum-mainnet";

/// A phase schema holding the lineage and phase-state tables. The pool has one connection, so
/// the session `search_path` set here is the one every later read uses.
async fn phase_database(name: &str) -> Result<TestDatabase> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let mut connection = database.pool().acquire().await?;
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    sqlx::raw_sql(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                                    canonicality_state)
         VALUES ('ethereum-mainnet', '0x64', 100, now(), 'canonical');
         INSERT INTO chain_phase_state (chain_id, phase_name, redo_attempt_generation)
         VALUES ('ethereum-mainnet', 'interpret', 3), ('ethereum-mainnet', 'project', 4);",
    )
    .execute(&mut *connection)
    .await?;
    Ok(database)
}

fn bound(hash: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(CHAIN.to_owned(), hash.to_owned())])
}

#[tokio::test]
async fn captures_both_phases_and_the_readable_bound_block() -> Result<()> {
    let database = phase_database("history_bound_state_capture").await?;
    let state = capture_history_bound_state(database.pool(), &bound("0x64")).await?;
    assert_eq!(
        state.redo,
        BTreeMap::from([(
            CHAIN.to_owned(),
            ChainRedoState {
                interpret: PhaseRedoState {
                    generation: 3,
                    in_progress: false,
                },
                project: PhaseRedoState {
                    generation: 4,
                    in_progress: false,
                },
            },
        )])
    );
    assert!(!state.interpret_redo_active);
    assert!(state.readable_bound_block(CHAIN, 100).is_some());
    // The same hash at another height, or a hash lineage does not hold, is not readable.
    assert!(state.readable_bound_block(CHAIN, 101).is_none());
    let unknown = capture_history_bound_state(database.pool(), &bound("0x65")).await?;
    assert!(unknown.readable_bound_block(CHAIN, 100).is_none());

    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE block_hash = '0x64'",
    )
    .execute(database.pool())
    .await?;
    let orphaned = capture_history_bound_state(database.pool(), &bound("0x64")).await?;
    assert!(orphaned.readable_bound_block(CHAIN, 100).is_none());
    database.cleanup().await
}

/// A chain without its Project row has unknown redo state: the capture fails instead of
/// returning a shorter map that another shorter map would equal.
#[tokio::test]
async fn a_missing_phase_row_fails_the_capture() -> Result<()> {
    let database = phase_database("history_bound_state_missing_phase").await?;
    sqlx::query("DELETE FROM chain_phase_state WHERE phase_name = 'project'")
        .execute(database.pool())
        .await?;
    let error = capture_history_bound_state(database.pool(), &bound("0x64"))
        .await
        .expect_err("a missing Project row must fail");
    assert!(
        error.downcast_ref::<PhaseRedoStateMissing>().is_some(),
        "{error:?}"
    );
    database.cleanup().await
}
