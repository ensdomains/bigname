use std::borrow::Cow;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{Acquire, migrate::Migrate};

use super::*;

const PROFILE: &str = "startup-test";
const CHAIN: &str = "startup-chain";
const ADAPTER: &str = "test-startup-adapter";

async fn database(name: &str) -> Result<TestDatabase> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new(name),
        &crate::MIGRATOR,
        "failed to migrate startup adapter checkpoint test database",
    )
    .await?;
    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id,
             revision,
             retention_generation,
             retained_history_complete,
             incomplete_since
         ) VALUES ($1, 7, 3, FALSE, now())",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    sqlx::query("INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 11)")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    Ok(database)
}

async fn complete(database: &TestDatabase, version: i64) -> Result<StartupAdapterSyncKey> {
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, version).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };
    let started_key = started_key.expect("fixture key must be fully known");
    assert_eq!(
        complete_startup_adapter_sync(
            database.pool(),
            PROFILE,
            CHAIN,
            ADAPTER,
            version,
            Some(started_key.clone()),
        )
        .await?,
        StartupAdapterSyncCompletion::Completed
    );
    Ok(started_key)
}

async fn insert_canonical_head(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, NULL, $3, TO_TIMESTAMP($3), 'canonical')
        "#,
    )
    .bind(CHAIN)
    .bind(block_hash)
    .bind(block_number)
    .execute(database.pool())
    .await?;
    Ok(())
}

#[tokio::test]
async fn lineage_mutation_revision_migration_seeds_existing_chains() -> Result<()> {
    const LINEAGE_MUTATION_MIGRATION: i64 = 20260727120100;

    let database = TestDatabase::create(TestDatabaseConfig::new(
        "startup_adapter_lineage_revision_migration",
    ))
    .await?;
    let before_lineage_revision = sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|migration| migration.version < LINEAGE_MUTATION_MIGRATION)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    database
        .apply_migrations(
            &before_lineage_revision,
            "failed to apply migrations before lineage revision storage",
        )
        .await?;
    insert_canonical_head(&database, 8, "0xexisting-head").await?;

    let through_lineage_revision = sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|migration| migration.version <= LINEAGE_MUTATION_MIGRATION)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    database
        .apply_migrations(
            &through_lineage_revision,
            "failed to apply lineage revision migration",
        )
        .await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision
             FROM chain_lineage_mutation_revisions
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(database.pool())
        .await?,
        0,
        "the migration must seed a stable baseline for every existing lineage chain"
    );

    insert_canonical_head(&database, 7, "0xexisting-below-head").await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision
             FROM chain_lineage_mutation_revisions
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(database.pool())
        .await?,
        1,
        "the first post-migration statement must advance the seeded revision"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT revision, min_affected_block_number
             FROM chain_lineage_mutation_revision_evidence
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(database.pool())
        .await?,
        (1, 7),
        "the first bump must retain its minimum affected block as completion evidence"
    );

    database.cleanup().await
}

#[tokio::test]
async fn completed_startup_adapter_checkpoint_reuses_only_an_exact_key() -> Result<()> {
    let database = database("startup_adapter_exact_key").await?;
    let original = complete(&database, 1).await?;

    assert_eq!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::ReuseCompleted,
        "an unchanged second boot must take the cheap completed-row verification"
    );

    sqlx::query(
        "UPDATE raw_log_staging_input_revisions SET revision = revision + 1 WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(key)
        } if key.raw_log_input_version.revision == original.raw_log_input_version.revision + 1
    ));

    sqlx::query(
        "UPDATE raw_log_staging_input_revisions
         SET retention_generation = retention_generation + 1
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(key)
        } if key.raw_log_input_version.retention_generation
            == original.raw_log_input_version.retention_generation + 1
    ));

    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 2).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(key)
        } if key.adapter_semantic_version == 2
    ));

    database.cleanup().await
}

#[tokio::test]
async fn prepare_invalidates_a_nonmatching_completion_before_rollback() -> Result<()> {
    let database = database("startup_adapter_prepare_rollback").await?;
    let original = complete(&database, 1).await?;

    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 2).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(StartupAdapterSyncKey {
                adapter_semantic_version: 2,
                ..
            })
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1
               AND chain_id = $2
               AND cursor_kind = $3
               AND adapter = $4
               AND checkpoint_scope = $5",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(STARTUP_ADAPTER_CURSOR_KIND)
        .bind(ADAPTER)
        .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
        .fetch_one(database.pool())
        .await?,
        0,
        "preparing the new semantic version must invalidate the old completion"
    );

    assert_eq!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(original),
        },
        "rolling back after the new pass crashes must not revive the old completion"
    );

    database.cleanup().await
}

#[tokio::test]
async fn prepare_preserves_a_nonmatching_partial_checkpoint() -> Result<()> {
    let database = database("startup_adapter_prepare_partial").await?;
    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET status = 'running', completed_at = NULL
         WHERE deployment_profile = $1
           AND chain_id = $2
           AND cursor_kind = $3
           AND adapter = $4
           AND checkpoint_scope = $5",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(ADAPTER)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .execute(database.pool())
    .await?;

    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 2).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(StartupAdapterSyncKey {
                adapter_semantic_version: 2,
                ..
            })
        }
    ));
    assert_eq!(
        sqlx::query_as::<_, (String, Option<i64>)>(
            "SELECT status::TEXT, adapter_semantic_version
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1
               AND chain_id = $2
               AND cursor_kind = $3
               AND adapter = $4
               AND checkpoint_scope = $5",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(STARTUP_ADAPTER_CURSOR_KIND)
        .bind(ADAPTER)
        .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
        .fetch_one(database.pool())
        .await?,
        ("running".to_owned(), Some(1)),
        "prepare must leave partial state for the adapter's own drift predicates"
    );

    database.cleanup().await
}

#[tokio::test]
async fn completed_lineage_extent_reuses_tail_growth_and_rejects_prefix_changes() -> Result<()> {
    let database = database("startup_adapter_lineage_key").await?;
    insert_canonical_head(&database, 8, "0xempty-a").await?;
    let original = complete(&database, 1).await?;
    assert_eq!(original.lineage_mutation_revision, 1);
    assert_eq!(
        original.canonical_lineage_head,
        Some(StartupCanonicalLineageHead {
            block_number: 8,
            block_hash: "0xempty-a".to_owned(),
        })
    );

    insert_canonical_head(&database, 9, "0xempty-tail").await?;
    assert_eq!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::ReuseCompleted,
        "lineage growth strictly above the scanned extent must reuse the completed prefix"
    );

    insert_canonical_head(&database, 7, "0xbelow-head").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("a below-head lineage insert must invalidate startup reuse");
    };
    let below_head = started_key.expect("below-head lineage insert must retain a known key");
    assert_eq!(below_head.lineage_mutation_revision, 3);
    assert_eq!(
        below_head.canonical_lineage_head,
        Some(StartupCanonicalLineageHead {
            block_number: 9,
            block_hash: "0xempty-tail".to_owned(),
        }),
        "the mutation revision must detect a corpus change even when the head is unchanged"
    );
    assert_eq!(
        complete_startup_adapter_sync(
            database.pool(),
            PROFILE,
            CHAIN,
            ADAPTER,
            1,
            Some(below_head),
        )
        .await?,
        StartupAdapterSyncCompletion::Completed
    );

    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = '0xempty-tail'",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    insert_canonical_head(&database, 9, "0xempty-b").await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(StartupAdapterSyncKey {
                canonical_lineage_head: Some(StartupCanonicalLineageHead {
                    block_number: 9,
                    ref block_hash,
                }),
                ..
            })
        } if block_hash == "0xempty-b"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn lineage_statement_triggers_track_insert_update_and_delete() -> Result<()> {
    let database = database("startup_adapter_lineage_triggers").await?;
    assert_eq!(
        load_startup_adapter_lineage_state(database.pool(), CHAIN).await?,
        Some(StartupAdapterLineageState {
            mutation_revision: 0,
            canonical_lineage_head: None,
        })
    );

    insert_canonical_head(&database, 8, "0xtriggered").await?;
    assert_eq!(
        load_startup_adapter_lineage_state(database.pool(), CHAIN)
            .await?
            .expect("inserted lineage must have known revision")
            .mutation_revision,
        1
    );

    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'safe'
         WHERE chain_id = $1 AND block_hash = '0xtriggered'",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert_eq!(
        load_startup_adapter_lineage_state(database.pool(), CHAIN)
            .await?
            .expect("updated lineage must have known revision")
            .mutation_revision,
        2
    );

    sqlx::query(
        "DELETE FROM chain_lineage
         WHERE chain_id = $1 AND block_hash = '0xtriggered'",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert_eq!(
        load_startup_adapter_lineage_state(database.pool(), CHAIN).await?,
        Some(StartupAdapterLineageState {
            mutation_revision: 3,
            canonical_lineage_head: None,
        })
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT revision, min_affected_block_number
             FROM chain_lineage_mutation_revision_evidence
             WHERE chain_id = $1
             ORDER BY revision",
        )
        .bind(CHAIN)
        .fetch_all(database.pool())
        .await?,
        vec![(1, 8), (2, 8), (3, 8)],
        "every statement bump must retain one minimum-block evidence row"
    );

    database.cleanup().await
}

#[tokio::test]
async fn completion_accepts_only_evidenced_lineage_growth_above_its_scanned_extent() -> Result<()> {
    let database = database("startup_adapter_lineage_completion_extent").await?;
    insert_canonical_head(&database, 8, "0xextent").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };
    let started_key = started_key.expect("fixture key must be known");

    insert_canonical_head(&database, 9, "0xtail").await?;
    assert_eq!(
        complete_startup_adapter_sync(
            database.pool(),
            PROFILE,
            CHAIN,
            ADAPTER,
            1,
            Some(started_key),
        )
        .await?,
        StartupAdapterSyncCompletion::Completed,
        "a concurrent tail-only lineage writer must not exhaust the bounded startup passes"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT
                 replay_target_block_number,
                 (state_payload ->> $4)::BIGINT,
                 (state_payload -> $5 ->> 'block_number')::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1
               AND chain_id = $2
               AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .bind(STARTUP_LINEAGE_MUTATION_REVISION_FIELD)
        .bind(STARTUP_LINEAGE_SCAN_EXTENT_FIELD)
        .fetch_one(database.pool())
        .await?,
        (8, 2, 8),
        "completion must accept the current revision while retaining the actually scanned extent"
    );
    assert_eq!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::ReuseCompleted,
    );

    database.cleanup().await
}

#[tokio::test]
async fn completed_lineage_extent_fails_closed_when_revision_evidence_is_missing() -> Result<()> {
    let database = database("startup_adapter_missing_lineage_evidence").await?;
    insert_canonical_head(&database, 8, "0xextent").await?;
    complete(&database, 1).await?;
    insert_canonical_head(&database, 9, "0xtail").await?;
    sqlx::query(
        "DELETE FROM chain_lineage_mutation_revision_evidence
         WHERE chain_id = $1 AND revision = 2",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;

    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(_)
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1
               AND chain_id = $2
               AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .fetch_one(database.pool())
        .await?,
        0,
        "missing per-revision lineage evidence must invalidate the completion"
    );

    database.cleanup().await
}

#[tokio::test]
async fn same_height_lineage_multiplicity_makes_the_key_unknown_until_repaired() -> Result<()> {
    let database = database("startup_adapter_lineage_multiplicity").await?;
    insert_canonical_head(&database, 8, "0xhead-a").await?;
    insert_canonical_head(&database, 8, "0xhead-b").await?;

    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("ambiguous highest lineage must never reuse a completion");
    };
    assert_eq!(started_key, None);
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key)
            .await?,
        StartupAdapterSyncCompletion::KeyUnknown
    );

    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = '0xhead-b'",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(StartupAdapterSyncKey {
                lineage_mutation_revision: 3,
                canonical_lineage_head: Some(StartupCanonicalLineageHead {
                    block_number: 8,
                    ref block_hash,
                }),
                ..
            })
        } if block_hash == "0xhead-a"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn startup_adapter_checkpoint_fails_closed_on_missing_partial_and_skewed_state() -> Result<()>
{
    let database = database("startup_adapter_fail_closed").await?;
    complete(&database, 1).await?;

    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET status = 'running', completed_at = NULL
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET state_payload = state_payload - $4
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .bind(STARTUP_LINEAGE_SCAN_EXTENT_FIELD)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET state_payload = state_payload - $4
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .bind(STARTUP_LINEAGE_MUTATION_REVISION_FIELD)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET schema_migration_count = schema_migration_count - 1
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET adapter_semantic_version = NULL
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET state_payload = state_payload - $4
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .bind(STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    complete(&database, 1).await?;
    sqlx::query(
        "UPDATE normalized_replay_adapter_checkpoints
         SET state_payload = state_payload - $4
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .bind(STARTUP_CANONICAL_LINEAGE_HEAD_FIELD)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    sqlx::query(
        "DELETE FROM normalized_replay_adapter_checkpoints
         WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
    )
    .bind(PROFILE)
    .bind(CHAIN)
    .bind(ADAPTER)
    .execute(database.pool())
    .await?;
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    sqlx::query("DELETE FROM raw_log_staging_input_revisions WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("an unknown raw-log input must never reuse completion");
    };
    assert_eq!(started_key, None);
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key,)
            .await?,
        StartupAdapterSyncCompletion::KeyUnknown
    );

    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id,
             revision,
             retention_generation,
             retained_history_complete,
             incomplete_since
         ) VALUES ($1, 7, 3, FALSE, now())",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    sqlx::query("DROP TABLE _sqlx_migrations")
        .execute(database.pool())
        .await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("unknown migration state must never reuse completion");
    };
    assert_eq!(started_key, None);

    database.cleanup().await
}

#[tokio::test]
async fn startup_adapter_checkpoint_rechecks_the_key_before_completion() -> Result<()> {
    let database = database("startup_adapter_completion_fence").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };
    let mut old_completion = database.pool().begin().await?;
    publish_completed_checkpoint(
        old_completion.as_mut(),
        PROFILE,
        CHAIN,
        ADAPTER,
        started_key.as_ref().expect("fixture key must be known"),
        started_key
            .as_ref()
            .and_then(|key| key.canonical_lineage_head.as_ref()),
    )
    .await?;
    old_completion.commit().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .fetch_one(database.pool())
        .await?,
        1,
        "the fixture must retain the pre-pass completion before input drift"
    );

    sqlx::query("UPDATE discovery_admission_epochs SET epoch = epoch + 1 WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key,)
            .await?,
        StartupAdapterSyncCompletion::InputChanged
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .fetch_one(database.pool())
        .await?,
        0,
        "InputChanged must invalidate an old completion before the bounded retry"
    );

    database.cleanup().await
}

#[tokio::test]
async fn startup_adapter_completion_invalidates_checkpoint_when_key_becomes_unknown() -> Result<()>
{
    let database = database("startup_adapter_unknown_completion_fence").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };
    let started_key = started_key.expect("fixture key must be known");

    let mut competing_completion = database.pool().begin().await?;
    publish_completed_checkpoint(
        competing_completion.as_mut(),
        PROFILE,
        CHAIN,
        ADAPTER,
        &started_key,
        started_key.canonical_lineage_head.as_ref(),
    )
    .await?;
    competing_completion.commit().await?;

    sqlx::query("DELETE FROM raw_log_staging_input_revisions WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    assert_eq!(
        complete_startup_adapter_sync(
            database.pool(),
            PROFILE,
            CHAIN,
            ADAPTER,
            1,
            Some(started_key.clone()),
        )
        .await?,
        StartupAdapterSyncCompletion::KeyUnknown
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .fetch_one(database.pool())
        .await?,
        0,
        "an unknown post-pass key must invalidate any retained completion"
    );

    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id,
             revision,
             retention_generation,
             retained_history_complete,
             incomplete_since
         ) VALUES ($1, 7, 3, FALSE, now())",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert_eq!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(started_key),
        },
        "restoring the same key must not revive the invalidated completion"
    );

    database.cleanup().await
}

#[tokio::test]
async fn key_unknown_attempt_deletes_a_private_completion_minted_mid_pass() -> Result<()> {
    let database = database("startup_adapter_unknown_prepare_private_completion").await?;
    sqlx::query("DELETE FROM raw_log_staging_input_revisions WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("a key-unknown attempt must run the family");
    };
    assert_eq!(started_key, None);

    sqlx::query(
        "INSERT INTO raw_log_staging_input_revisions (
             chain_id,
             revision,
             retention_generation,
             retained_history_complete,
             incomplete_since
         ) VALUES ($1, 7, 3, FALSE, now())",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    let mut private_completion = database.pool().begin().await?;
    lock_canonical_lineage(private_completion.as_mut(), CHAIN).await?;
    let minted_key = load_startup_adapter_sync_key(private_completion.as_mut(), CHAIN, 1)
        .await?
        .expect("the missing key component must be known after it is minted");
    publish_completed_checkpoint(
        private_completion.as_mut(),
        PROFILE,
        CHAIN,
        ADAPTER,
        &minted_key,
        minted_key.canonical_lineage_head.as_ref(),
    )
    .await?;
    private_completion.commit().await?;

    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key,)
            .await?,
        StartupAdapterSyncCompletion::KeyUnknown
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1
               AND chain_id = $2
               AND cursor_kind = $3
               AND adapter = $4
               AND checkpoint_scope = $5",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(STARTUP_ADAPTER_CURSOR_KIND)
        .bind(ADAPTER)
        .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
        .fetch_one(database.pool())
        .await?,
        0,
        "the outer completion fence must delete a row produced under uncaptured inputs"
    );
    assert!(matches!(
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?,
        StartupAdapterSyncDecision::RunFullSync {
            started_key: Some(_)
        }
    ));

    database.cleanup().await
}

#[tokio::test]
async fn startup_adapter_completion_rechecks_below_head_lineage_mutation() -> Result<()> {
    let database = database("startup_adapter_lineage_completion_fence").await?;
    insert_canonical_head(&database, 8, "0xempty-a").await?;
    let StartupAdapterSyncDecision::RunFullSync { started_key } =
        prepare_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1).await?
    else {
        panic!("fresh startup adapter checkpoint must run");
    };

    insert_canonical_head(&database, 7, "0xbelow-head").await?;
    assert_eq!(
        complete_startup_adapter_sync(database.pool(), PROFILE, CHAIN, ADAPTER, 1, started_key,)
            .await?,
        StartupAdapterSyncCompletion::InputChanged
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = $1 AND chain_id = $2 AND adapter = $3",
        )
        .bind(PROFILE)
        .bind(CHAIN)
        .bind(ADAPTER)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn startup_waits_on_the_migrator_lock_before_the_ledger_or_checkpoint_table() -> Result<()> {
    let database = database("startup_adapter_migration_lock_order").await?;
    let mut migration_connection = database.pool().acquire().await?;
    Migrate::lock(&mut *migration_connection).await?;
    let mut migration = migration_connection.begin().await?;
    sqlx::query("LOCK TABLE normalized_replay_adapter_checkpoints IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *migration)
        .await?;

    let startup_pool = database.pool().clone();
    let startup = tokio::spawn(async move {
        prepare_startup_adapter_sync(&startup_pool, PROFILE, CHAIN, ADAPTER, 1).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        sqlx::query(
            "UPDATE _sqlx_migrations
             SET execution_time = execution_time
             WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
        )
        .execute(&mut *migration),
    )
    .await
    .expect("migration ledger write must not wait behind startup")?;
    migration.commit().await?;
    Migrate::unlock(&mut *migration_connection).await?;
    drop(migration_connection);

    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), startup)
            .await
            .expect("startup must proceed after the migration fence releases")??,
        StartupAdapterSyncDecision::RunFullSync { .. }
    ));

    database.cleanup().await
}

#[tokio::test]
async fn cancelled_startup_does_not_return_a_migrator_locked_connection_to_the_pool() -> Result<()>
{
    let database = database("startup_adapter_cancelled_migration_lock").await?;
    let mut blocker_connection = database.pool().acquire().await?;
    let mut blocker = blocker_connection.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{CHAIN}"))
        .execute(&mut *blocker)
        .await?;

    let startup_pool = database.pool().clone();
    let startup = tokio::spawn(async move {
        prepare_startup_adapter_sync(&startup_pool, PROFILE, CHAIN, ADAPTER, 1).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let held_advisory_locks = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND database = (
                       SELECT oid
                       FROM pg_database
                       WHERE datname = current_database()
                   )
                   AND granted",
            )
            .fetch_one(database.pool())
            .await
            .expect("advisory-lock inspection must succeed");
            if held_advisory_locks >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup must acquire the migrator lock before waiting on the raw-log fence");

    startup.abort();
    assert!(
        startup
            .await
            .expect_err("aborted startup task must be cancelled")
            .is_cancelled()
    );
    blocker.rollback().await?;

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let held_advisory_locks = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND database = (
                       SELECT oid
                       FROM pg_database
                       WHERE datname = current_database()
                   )
                   AND granted",
            )
            .fetch_one(database.pool())
            .await
            .expect("advisory-lock inspection must succeed");
            if held_advisory_locks == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling startup must close the migrator-locked session");

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_range_check_fails_closed_when_revision_evidence_is_missing() -> Result<()> {
    let database = database("startup_adapter_missing_block_revision").await?;
    sqlx::query(
        "UPDATE raw_log_staging_input_revisions
         SET revision = revision + 1
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;

    assert!(
        crate::raw_log_staging_block_range_changed_since(database.pool(), CHAIN, 7, 0, 100).await?,
        "an advanced revision without per-block proof must reset a partial checkpoint"
    );

    sqlx::query(
        "INSERT INTO raw_log_staging_block_revisions (
             chain_id,
             block_hash,
             block_number,
             revision
         ) VALUES ($1, '0xoutside-consumed-boundary', 101, 9)",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "UPDATE raw_log_staging_input_revisions
         SET revision = 10
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;
    assert!(
        crate::raw_log_staging_block_range_changed_since(database.pool(), CHAIN, 7, 0, 100).await?,
        "evidence for an earlier revision must not prove that the latest revision missed the range"
    );

    database.cleanup().await
}
