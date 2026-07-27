use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

#[tokio::test]
async fn partial_startup_checkpoint_resets_after_below_head_lineage_insert() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("subregistry_partial_lineage_reset"),
        &bigname_storage::MIGRATOR,
        "failed to migrate subregistry partial-lineage test database",
    )
    .await?;
    let chain = "subregistry-partial-lineage";
    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id, revision, retention_generation, retained_history_complete, incomplete_since
         ) VALUES ($1, 0, 0, FALSE, now())",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query("INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 0)")
        .bind(chain)
        .execute(database.pool())
        .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, '0xhead', NULL, 10, TO_TIMESTAMP(10), 'canonical')",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let startup = crate::StartupAdapterCheckpointContext::new("test-lineage-reset", 10)?;
    let initial_context = startup.adapter_context(database.pool(), chain, 1).await?;
    SubregistryReplayCheckpoint::load_or_start(database.pool(), chain, &initial_context).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET status = 'stream_complete',
             scanned_log_count = 9,
             matched_log_count = 8,
             staged_item_count = 7,
             last_block_number = 8,
             last_transaction_index = 0,
             last_log_index = 0,
             last_emitting_address = '0x0000000000000000000000000000000000000001'
         WHERE deployment_profile = 'test-lineage-reset'
           AND chain_id = $1
           AND adapter = $2",
    )
    .bind(chain)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;

    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, '0xbelow-head', NULL, 5, TO_TIMESTAMP(5), 'canonical')",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let refreshed_context = startup.adapter_context(database.pool(), chain, 1).await?;
    let reset =
        SubregistryReplayCheckpoint::load_or_start(database.pool(), chain, &refreshed_context)
            .await?;

    assert_eq!(reset.status, "running");
    assert_eq!(reset.last_position, None);
    assert_eq!(reset.scanned_log_count, 0);
    assert_eq!(reset.matched_log_count, 0);
    assert_eq!(reset.staged_item_count, 0);
    assert_eq!(
        reset.context.startup_canonical_lineage_head,
        initial_context.startup_canonical_lineage_head,
        "the fixture must change lineage below an unchanged canonical head"
    );
    assert_ne!(
        reset.context.startup_lineage_mutation_revision,
        initial_context.startup_lineage_mutation_revision
    );

    database.cleanup().await
}
