use super::*;
use crate::projection_apply::apply_locks::{
    acquire_invalidation_apply_locks_with_timeout, ensure_invalidation_apply_locks_alive,
    ensure_invalidation_apply_locks_probe_alive_for_test, invalidation_apply_lock_key,
    invalidation_apply_locks_backend_pid, open_invalidation_apply_locks_connection_for_test,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::Connection;

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_worker_projection_apply_claim_test")
            // This module explicitly emulates processes that predate the
            // [projection replay-version fence](../../../../../docs/glossary.md#projection-replay-version-fence)
            // and selected replay versions. All ordinary test databases retain the default
            // connection stamp.
            .without_projection_replay_version_stamp()
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for projection apply claim tests")
            .admin_connect_context("failed to connect admin pool for projection apply claim tests")
            .pool_connect_context("failed to connect projection apply claim test pool"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for projection apply claim tests",
    )
    .await
}

#[tokio::test]
async fn newer_replay_marker_fatally_refuses_projection_invalidation_claim() -> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:outdated.eth").await?;
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count
        )
        VALUES ('name_current', $1, 100, 0, 0, 0)
        "#,
    )
    .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION + 1)
    .execute(database.pool())
    .await?;

    let error = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4())
        .await
        .expect_err("an outdated worker must not claim projection invalidations");
    assert!(
        bigname_storage::projection_staging::is_outdated_projection_replay_version_error(&error),
        "the version fence must return the process-fatal error, got: {error:#}"
    );
    let claim_token = sqlx::query_scalar::<_, Option<Uuid>>(
        r#"
        SELECT claim_token
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:outdated.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert!(
        claim_token.is_none(),
        "the refused claim transaction must leave the invalidation unclaimed"
    );

    database.cleanup().await
}

#[tokio::test]
async fn missing_replay_version_singleton_fatally_refuses_projection_invalidation_claim()
-> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:missing-fence.eth").await?;
    sqlx::query(
        r#"
        DELETE FROM current_projection_full_replay_input_revision
        WHERE singleton
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4())
        .await
        .expect_err("a worker must stop when the replay-version singleton is missing");
    assert!(
        bigname_storage::projection_staging::is_fatal_projection_replay_version_fence_error(&error),
        "missing singleton state must return a process-fatal fence error, got: {error:#}"
    );
    assert!(
        !bigname_storage::projection_staging::is_outdated_projection_replay_version_error(&error),
        "missing singleton state is fatal corruption, not an outdated process"
    );
    let claim_token = sqlx::query_scalar::<_, Option<Uuid>>(
        r#"
        SELECT claim_token
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:missing-fence.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert!(
        claim_token.is_none(),
        "the failed claim transaction must leave the invalidation unclaimed"
    );

    database.cleanup().await
}

#[tokio::test]
async fn current_replay_marker_allows_projection_invalidation_claim() -> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:current.eth").await?;
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count
        )
        VALUES ('name_current', $1, 100, 0, 0, 0)
        "#,
    )
    .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION)
    .execute(database.pool())
    .await?;
    admit_replay_version(
        &database,
        bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
    )
    .await?;

    let claim_token = Uuid::new_v4();
    let claimed = claim_pending_invalidations(database.pool(), 10, claim_token).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].claim_token, claim_token);

    database.cleanup().await
}

#[tokio::test]
async fn pre_fence_claim_sql_is_rejected_after_newer_replay_admission() -> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:legacy-claim.eth").await?;
    admit_replay_version(
        &database,
        bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION + 1,
    )
    .await?;

    let error = sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET
            claim_token = $1,
            claimed_at = now()
        WHERE projection = 'name_current'
          AND projection_key = 'ens:legacy-claim.eth'
        "#,
    )
    .bind(Uuid::new_v4())
    .execute(database.pool())
    .await
    .expect_err("pre-fence claim SQL must be rejected after newer replay admission");
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "legacy claim refusal must be loud and fatal, got: {error}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn pre_fence_publish_sql_is_rejected_after_newer_replay_admission() -> Result<()> {
    const LEGACY_PUBLISH_SQL: &str = r#"
        UPDATE primary_names_current
        SET claim_provenance = '{"legacy_publish": true}'::jsonb
        WHERE address = '0x1111111111111111111111111111111111111111'
          AND coin_type = '60'
          AND namespace = 'ens'
        "#;

    let database = test_database().await?;
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            claim_provenance,
            normalized_claim_name
        )
        VALUES (
            '0x1111111111111111111111111111111111111111',
            '60',
            'ens',
            'success',
            NULL,
            '{}'::jsonb,
            'legacy.eth'
        )
        "#,
    )
    .execute(database.pool())
    .await?;
    admit_replay_version(
        &database,
        bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION + 1,
    )
    .await?;

    let error = sqlx::query(LEGACY_PUBLISH_SQL)
        .execute(database.pool())
        .await
        .expect_err(
            "pre-fence projection publish SQL must be rejected after newer replay admission",
        );
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "legacy publish refusal must be loud and fatal, got: {error}"
    );

    let outdated_options = bigname_storage::stamp_projection_replay_version(
        database.pool().connect_options().as_ref().clone(),
    );
    let mut outdated_connection = sqlx::PgConnection::connect_with(&outdated_options).await?;
    let error = sqlx::query(LEGACY_PUBLISH_SQL)
        .execute(&mut outdated_connection)
        .await
        .expect_err("a stamped lower-version projection publish must be rejected");
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "lower-version publish refusal must be loud and fatal, got: {error}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn outdated_claim_heartbeat_is_rejected_after_newer_replay_admission() -> Result<()> {
    let database = test_database().await?;
    let claim_token = Uuid::new_v4();
    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:legacy-heartbeat.eth",
        claim_token,
        "1 minute",
    )
    .await?;
    admit_replay_version(
        &database,
        bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION + 1,
    )
    .await?;

    let invalidation = ClaimedInvalidation {
        projection: "name_current".to_owned(),
        projection_key: "ens:legacy-heartbeat.eth".to_owned(),
        key_payload: serde_json::json!({}),
        generation: 0,
        claim_token,
        attempt_count: 0,
    };
    let error = refresh_claimed_invalidation_claim(database.pool(), &invalidation)
        .await
        .expect_err("an outdated claim heartbeat must not extend its lease");
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "heartbeat refusal must be loud and fatal, got: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_version_fence_covers_every_static_projection_writer_table() -> Result<()> {
    let database = test_database().await?;
    let protected_tables = sqlx::query_scalar::<_, String>(
        r#"
        SELECT relation.relname
        FROM pg_catalog.pg_trigger AS trigger
        JOIN pg_catalog.pg_class AS relation
          ON relation.oid = trigger.tgrelid
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'public'
          AND trigger.tgname =
              'current_projection_replay_version_fence_before_write'
          AND NOT trigger.tgisinternal
        ORDER BY relation.relname
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        protected_tables,
        [
            "address_names_current",
            "address_names_current_identity_counts",
            "address_names_current_identity_feed",
            "children_current",
            "current_projection_full_replay_input_revision",
            "current_projection_replay_attempt",
            "current_projection_replay_status",
            "current_projection_staging_checkpoints",
            "name_current",
            "permissions_current",
            "permissions_current_publication",
            "permissions_current_resource_summary",
            "primary_names_current",
            "projection_apply_cursors",
            "projection_invalidation_dead_letters",
            "projection_invalidations",
            "record_inventory_current",
            "resolver_current",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn pre_activation_legacy_write_finishes_before_fence_activation() -> Result<()> {
    let database = test_database().await?;
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            claim_provenance,
            normalized_claim_name
        )
        VALUES (
            '0x2222222222222222222222222222222222222222',
            '60',
            'ens',
            'success',
            NULL,
            '{}'::jsonb,
            'serialized.eth'
        )
        "#,
    )
    .execute(database.pool())
    .await?;

    let mut legacy_write = database.pool().begin().await?;
    sqlx::query(
        r#"
        UPDATE primary_names_current
        SET claim_provenance = '{"before_activation": true}'::jsonb
        WHERE address = '0x2222222222222222222222222222222222222222'
          AND coin_type = '60'
          AND namespace = 'ens'
        "#,
    )
    .execute(&mut *legacy_write)
    .await?;

    let pool = database.pool().clone();
    let (activation_started, wait_for_activation) = tokio::sync::oneshot::channel();
    let mut activation = tokio::spawn(async move {
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT set_config('bigname.projection_replay_version', $1, true)")
            .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION.to_string())
            .execute(&mut *transaction)
            .await?;
        let _ = activation_started.send(());
        sqlx::query(
            r#"
            UPDATE current_projection_full_replay_input_revision
            SET
                projection_replay_version_floor = $1,
                projection_replay_version_fence_active = true
            WHERE singleton
            "#,
        )
        .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok::<(), anyhow::Error>(())
    });
    wait_for_activation.await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut activation)
            .await
            .is_err(),
        "activation must wait for the pre-existing legacy writer's shared fence"
    );

    legacy_write.commit().await?;
    tokio::time::timeout(Duration::from_secs(5), activation)
        .await
        .context("replay-version fence activation stayed blocked after the legacy commit")?
        .context("replay-version fence activation task failed")??;

    let provenance: serde_json::Value = sqlx::query_scalar(
        r#"
        SELECT claim_provenance
        FROM primary_names_current
        WHERE address = '0x2222222222222222222222222222222222222222'
          AND coin_type = '60'
          AND namespace = 'ens'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(provenance, serde_json::json!({"before_activation": true}));

    let error = sqlx::query(
        r#"
        UPDATE primary_names_current
        SET claim_provenance = '{"after_activation": true}'::jsonb
        WHERE address = '0x2222222222222222222222222222222222222222'
          AND coin_type = '60'
          AND namespace = 'ens'
        "#,
    )
    .execute(database.pool())
    .await
    .expect_err("the same unstamped legacy writer must fail after activation");
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "post-activation legacy write must fail loudly, got: {error}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn legacy_table_lock_cannot_deadlock_replay_admission() -> Result<()> {
    let database = test_database().await?;
    let mut replay_admission = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.projection_replay_version', $1, true)")
        .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION.to_string())
        .execute(&mut *replay_admission)
        .await?;
    sqlx::query(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET
            projection_replay_version_floor = $1,
            projection_replay_version_fence_active = true
        WHERE singleton
        "#,
    )
    .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION)
    .execute(&mut *replay_admission)
    .await?;

    let pool = database.pool().clone();
    let mut legacy_write = tokio::spawn(async move {
        sqlx::query("TRUNCATE TABLE primary_names_current")
            .execute(&pool)
            .await
    });
    let legacy_result = tokio::time::timeout(Duration::from_secs(1), &mut legacy_write).await;
    if legacy_result.is_err() {
        replay_admission.rollback().await?;
        legacy_write.abort();
        let _ = legacy_write.await;
        database.cleanup().await?;
        bail!("legacy table-lock writer blocked behind replay admission");
    }
    let error = legacy_result??
        .expect_err("a legacy table-lock writer must fail instead of crossing replay admission");
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "legacy table-lock refusal must be loud and fatal, got: {error}"
    );
    let error = anyhow::Error::from(error);
    assert!(
        bigname_storage::projection_staging::is_outdated_projection_replay_version_error(&error),
        "an unstamped admission failure must retain the typed outdated-process classification"
    );

    let current_options = bigname_storage::stamp_projection_replay_version(
        database.pool().connect_options().as_ref().clone(),
    );
    let mut current_connection = sqlx::PgConnection::connect_with(&current_options).await?;
    let current_error = tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query("TRUNCATE TABLE primary_names_current").execute(&mut current_connection),
    )
    .await
    .context("current stamped writer waited behind replay admission")?
    .expect_err("a current stamped writer must retry after losing the NOWAIT admission race");
    assert!(
        current_error
            .to_string()
            .contains("projection replay admission is in progress; retry protected write"),
        "current stamped admission refusal must explain that it is retryable, got: {current_error}"
    );
    let current_error = anyhow::Error::from(current_error);
    assert!(
        !bigname_storage::projection_staging::is_outdated_projection_replay_version_error(
            &current_error
        ),
        "a current stamped admission race must not be classified as an outdated process"
    );

    replay_admission.commit().await?;
    database.cleanup().await
}

#[tokio::test]
async fn current_indexer_invalidation_upsert_checks_floor_without_waiting_for_replay_admission()
-> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:indexer-overlap.eth").await?;
    let mut replay_admission = database.pool().begin().await?;
    bigname_storage::projection_staging::lock_current_projection_replay_version_for_replay_write_in_transaction(
        &mut replay_admission,
    )
    .await?;

    let indexer_options = bigname_storage::stamp_projection_replay_version(
        database.pool().connect_options().as_ref().clone(),
    );
    let mut indexer_write = tokio::spawn(async move {
        let mut connection = sqlx::PgConnection::connect_with(&indexer_options).await?;
        sqlx::query(
            r#"
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload
            )
            VALUES ('name_current', 'ens:indexer-overlap.eth', '{}'::jsonb)
            ON CONFLICT (projection, projection_key)
            DO UPDATE SET
                generation = projection_invalidations.generation + 1,
                invalidated_at = now(),
                last_changed_at = now(),
                claim_token = NULL,
                claimed_at = NULL
            "#,
        )
        .execute(&mut connection)
        .await?;
        Ok::<(), sqlx::Error>(())
    });
    tokio::time::timeout(Duration::from_secs(1), &mut indexer_write)
        .await
        .context("current-version indexer invalidation waited for replay admission")?
        .context("current-version indexer invalidation task failed")?
        .context("current-version indexer invalidation was rejected")?;
    replay_admission.commit().await?;
    let generation: i64 = sqlx::query_scalar(
        r#"
        SELECT generation
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:indexer-overlap.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(generation, 1);

    database.cleanup().await
}

#[tokio::test]
async fn committed_floor_enqueue_survives_concurrent_newer_replay_admission() -> Result<()> {
    let database = test_database().await?;
    let current_version = bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION;
    let newer_version = current_version + 1;
    admit_replay_version(&database, current_version).await?;

    let indexer_options = bigname_storage::stamp_projection_replay_version(
        database.pool().connect_options().as_ref().clone(),
    );
    let mut indexer_connection = sqlx::PgConnection::connect_with(&indexer_options).await?;

    let mut newer_admission = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.projection_replay_version', $1, true)")
        .bind(newer_version.to_string())
        .execute(&mut *newer_admission)
        .await?;
    sqlx::query(
        r#"
        SELECT projection_replay_version_floor
        FROM current_projection_full_replay_input_revision
        WHERE singleton
        FOR UPDATE
        "#,
    )
    .execute(&mut *newer_admission)
    .await?;
    sqlx::query(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET projection_replay_version_floor = $1
        WHERE singleton
        "#,
    )
    .bind(newer_version)
    .execute(&mut *newer_admission)
    .await?;

    tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query(
            r#"
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload
            )
            VALUES ('name_current', 'ens:crossing-floor-raise.eth', '{}'::jsonb)
            "#,
        )
        .execute(&mut indexer_connection),
    )
    .await
    .context("current-at-committed-floor enqueue waited for newer replay admission")?
    .context("current-at-committed-floor enqueue was rejected")?;

    newer_admission.commit().await?;

    let persisted: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM projection_invalidations
            WHERE projection = 'name_current'
              AND projection_key = 'ens:crossing-floor-raise.eth'
        )
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert!(
        persisted,
        "the enqueue admitted against the committed floor must remain durable after the raise"
    );

    let error = sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ('name_current', 'ens:after-floor-raise.eth', '{}'::jsonb)
        "#,
    )
    .execute(&mut indexer_connection)
    .await
    .expect_err("the same process stamp must be rejected after the newer floor commits");
    let error = anyhow::Error::from(error);
    assert!(
        bigname_storage::projection_staging::is_outdated_projection_replay_version_error(&error),
        "post-raise enqueue must be classified as an outdated process, got: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn committed_floor_queue_rejects_repeatable_read_producer() -> Result<()> {
    let database = test_database().await?;
    let indexer_options = bigname_storage::stamp_projection_replay_version(
        database.pool().connect_options().as_ref().clone(),
    );
    let mut connection = sqlx::PgConnection::connect_with(&indexer_options).await?;
    let mut transaction = connection.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await?;

    let error = sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ('name_current', 'ens:repeatable-read.eth', '{}'::jsonb)
        "#,
    )
    .execute(&mut *transaction)
    .await
    .expect_err("the committed-floor queue branch must reject a stale transaction snapshot");
    let error = anyhow::Error::from(error);
    assert!(
        error
            .to_string()
            .contains("requires READ COMMITTED transaction isolation"),
        "the isolation failure must explain the queue requirement, got: {error:#}"
    );
    assert!(
        bigname_storage::projection_staging::is_fatal_projection_replay_version_fence_error(&error),
        "unsupported queue isolation must remain a fatal fence error"
    );
    assert!(
        !bigname_storage::projection_staging::is_retryable_projection_replay_admission_error(
            &error
        ),
        "unsupported queue isolation must not enter the admission retry loop"
    );
    transaction.rollback().await?;

    database.cleanup().await
}

#[derive(Clone, Copy)]
enum ReplayJournalLockOrdering {
    JournalFirst,
    ReplayFirst,
}

#[tokio::test]
async fn replay_admission_and_ingestion_journal_locks_do_not_deadlock() -> Result<()> {
    let database = test_database().await?;
    admit_replay_version(
        &database,
        bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
    )
    .await?;

    // Production capture bounds journal waits at 100 ms. Extend only that timeout in this
    // isolated database so the test can inspect the blocked lock edge before releasing it.
    sqlx::query(
        r#"
        ALTER FUNCTION public.capture_projection_permissions_resource_input_watermark()
        SET lock_timeout = '5s'
        "#,
    )
    .execute(database.pool())
    .await?;

    for (ordering, suffix) in [
        (ReplayJournalLockOrdering::JournalFirst, "journal-first"),
        (ReplayJournalLockOrdering::ReplayFirst, "replay-first"),
    ] {
        assert_replay_admission_and_ingestion_journal_complete(&database, ordering, suffix).await?;
    }

    database.cleanup().await
}

async fn assert_replay_admission_and_ingestion_journal_complete(
    database: &TestDatabase,
    ordering: ReplayJournalLockOrdering,
    suffix: &str,
) -> Result<()> {
    let mut producer = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.projection_replay_version', $1, true)")
        .bind(bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION.to_string())
        .execute(&mut *producer)
        .await?;
    let producer_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *producer)
        .await?;

    let mut replay = database.pool().begin().await?;
    let replay_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *replay)
        .await?;

    if matches!(ordering, ReplayJournalLockOrdering::JournalFirst) {
        insert_resource_to_lock_permissions_journal(&mut producer).await?;
    }
    bigname_storage::projection_staging::lock_current_projection_replay_version_for_replay_write_in_transaction(
        &mut replay,
    )
    .await?;
    if matches!(ordering, ReplayJournalLockOrdering::ReplayFirst) {
        insert_resource_to_lock_permissions_journal(&mut producer).await?;
    }

    let mut capture = tokio::spawn(async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_normalized_event_change_watermark()",
        )
        .fetch_one(&mut *replay)
        .await?;
        sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_permissions_resource_input_watermark()",
        )
        .fetch_one(&mut *replay)
        .await?;
        sqlx::query_scalar::<_, i64>(
            "SELECT public.capture_projection_direct_invalidation_watermark()",
        )
        .fetch_one(&mut *replay)
        .await?;
        replay.commit().await?;
        Ok::<(), sqlx::Error>(())
    });
    wait_for_backend_blocked_by(
        database.pool(),
        replay_pid,
        producer_pid,
        "replay capture did not wait for the ingestion journal writer",
    )
    .await?;

    // Under the former queue-side FOR SHARE behavior this statement introduced the reverse edge:
    // producer -> singleton-owning replay, while replay already waited producer -> journal. The
    // database then had to abort one transaction as a deadlock victim.
    tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query(
            r#"
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload
            )
            VALUES ('name_current', $1, '{}'::jsonb)
            "#,
        )
        .bind(format!("ens:{suffix}.eth"))
        .execute(&mut *producer),
    )
    .await
    .with_context(|| {
        format!("{suffix} invalidation enqueue waited behind replay admission and deadlocked")
    })??;
    producer.commit().await?;

    tokio::time::timeout(Duration::from_secs(5), &mut capture)
        .await
        .with_context(|| format!("{suffix} replay capture stayed blocked after producer commit"))?
        .with_context(|| format!("{suffix} replay capture task failed"))?
        .with_context(|| format!("{suffix} replay capture transaction failed"))?;

    Ok(())
}

async fn insert_resource_to_lock_permissions_journal(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let resource_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ethereum-mainnet',
            $2,
            1,
            'finalized'
        )
        "#,
    )
    .bind(resource_id)
    .bind(format!("0x{}", resource_id.simple()))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn wait_for_backend_blocked_by(
    pool: &PgPool,
    blocked_pid: i32,
    blocker_pid: i32,
    failure: &str,
) -> Result<()> {
    for _ in 0..500 {
        let blocked = sqlx::query_scalar::<_, bool>("SELECT $2 = ANY(pg_blocking_pids($1))")
            .bind(blocked_pid)
            .bind(blocker_pid)
            .fetch_one(pool)
            .await?;
        if blocked {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("{failure}")
}

#[tokio::test]
async fn outdated_stamped_invalidation_producer_is_rejected() -> Result<()> {
    let database = test_database().await?;
    admit_replay_version(
        &database,
        bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION + 1,
    )
    .await?;
    let outdated_options = bigname_storage::stamp_projection_replay_version(
        database.pool().connect_options().as_ref().clone(),
    );
    let mut outdated_connection = sqlx::PgConnection::connect_with(&outdated_options).await?;

    let error = sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ('name_current', 'ens:outdated-producer.eth', '{}'::jsonb)
        "#,
    )
    .execute(&mut outdated_connection)
    .await
    .expect_err("an outdated stamped invalidation producer must be rejected");
    assert!(
        error
            .to_string()
            .contains("fatal projection replay version fence"),
        "outdated producer refusal must be loud and fatal, got: {error}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn stale_projection_invalidation_claims_are_reclaimed() -> Result<()> {
    let database = test_database().await?;
    let stale_token = Uuid::new_v4();
    let new_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:stale.eth",
        stale_token,
        "10 minutes",
    )
    .await?;

    let claimed = claim_pending_invalidations(database.pool(), 10, new_token).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].projection, "name_current");
    assert_eq!(claimed[0].projection_key, "ens:stale.eth");
    assert_eq!(claimed[0].claim_token, new_token);

    let (claim_token, attempt_count): (Uuid, i64) = sqlx::query_as(
        r#"
        SELECT claim_token, attempt_count
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:stale.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load reclaimed projection invalidation")?;
    assert_eq!(claim_token, new_token);
    assert_eq!(
        attempt_count, 0,
        "claim recovery is not a failed projection apply attempt"
    );

    database.cleanup().await
}

#[tokio::test]
async fn stale_projection_invalidation_reclaim_preserves_dependency_priority_across_limit()
-> Result<()> {
    let database = test_database().await?;
    let stale_token = Uuid::new_v4();
    let new_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "address_names_current",
        "0x1111111111111111111111111111111111111111",
        stale_token,
        "10 minutes",
    )
    .await?;
    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:priority.eth",
        stale_token,
        "10 minutes",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET claimed_at = now() - '10 minutes'::INTERVAL
        WHERE projection IN ('address_names_current', 'name_current')
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to align stale claim timestamps")?;

    let claimed = claim_pending_invalidations(database.pool(), 1, new_token).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].projection, "name_current");
    assert_eq!(claimed[0].projection_key, "ens:priority.eth");
    assert_eq!(claimed[0].claim_token, new_token);

    let address_claim_token: Uuid = sqlx::query_scalar(
        r#"
        SELECT claim_token
        FROM projection_invalidations
        WHERE projection = 'address_names_current'
          AND projection_key = '0x1111111111111111111111111111111111111111'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load lower-priority stale invalidation")?;
    assert_eq!(
        address_claim_token, stale_token,
        "lower-priority stale invalidation must remain unreclaimed until higher-priority work fits"
    );

    database.cleanup().await
}

#[tokio::test]
async fn reinvalidation_after_dead_letter_preserves_dead_letter_history() -> Result<()> {
    let database = test_database().await?;

    insert_poison_invalidation(&database, "poisoned-key", 4).await?;
    apply_pending_invalidations(database.pool(), 10, None).await?;

    insert_poison_invalidation(&database, "poisoned-key", 4).await?;
    apply_pending_invalidations(database.pool(), 10, None).await?;

    let dead_letters = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT generation, attempt_count
        FROM projection_invalidation_dead_letters
        WHERE projection = 'unsupported_projection'
          AND projection_key = 'poisoned-key'
        ORDER BY generation
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to load projection invalidation dead-letter history")?;

    assert_eq!(
        dead_letters,
        vec![(0, 5), (1, 5)],
        "each dead-lettered generation must remain operator-visible"
    );

    database.cleanup().await
}

#[tokio::test]
async fn fresh_projection_invalidation_claims_are_not_reclaimed() -> Result<()> {
    let database = test_database().await?;
    let fresh_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:fresh.eth",
        fresh_token,
        "1 minute",
    )
    .await?;

    let claimed = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4()).await?;
    assert!(claimed.is_empty());

    let claim_token: Uuid = sqlx::query_scalar(
        r#"
        SELECT claim_token
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:fresh.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load fresh projection invalidation")?;
    assert_eq!(claim_token, fresh_token);

    database.cleanup().await
}

#[tokio::test]
async fn repeated_failures_move_projection_invalidation_to_dead_letter() -> Result<()> {
    let database = test_database().await?;

    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            attempt_count
        )
        VALUES (
            'unsupported_projection',
            'poisoned-key',
            '{}'::jsonb,
            4
        )
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to seed poison projection invalidation")?;

    let summary = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(
        summary,
        ProjectionInvalidationApplySummary {
            claimed_invalidation_count: 1,
            applied_invalidation_count: 0,
            failed_invalidation_count: 1,
        }
    );

    let queue_row_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE projection = 'unsupported_projection'
          AND projection_key = 'poisoned-key'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to count live projection invalidation rows after dead-letter")?;
    assert_eq!(
        queue_row_count, 0,
        "dead-lettered invalidations must leave the live queue"
    );

    let (state, attempt_count, dead_lettered_at, failure_reason): (
        String,
        i64,
        Option<sqlx::types::time::OffsetDateTime>,
        Option<String>,
    ) = sqlx::query_as(
        r#"
        SELECT
            state::TEXT AS state,
            attempt_count,
            dead_lettered_at,
            last_failure_reason
        FROM projection_invalidation_dead_letters
        WHERE projection = 'unsupported_projection'
          AND projection_key = 'poisoned-key'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load dead-lettered projection invalidation")?;
    assert_eq!(state, "dead_letter");
    assert_eq!(attempt_count, 5);
    assert!(dead_lettered_at.is_some());
    assert!(
        failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("unsupported projection invalidation family"))
    );

    let indexing_status = bigname_storage::load_indexing_status(database.pool()).await?;
    assert!(
        !indexing_status.has_unscoped_pending_invalidations,
        "dead-lettered invalidations must not poison indexing status"
    );

    let claimed = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4()).await?;
    assert!(
        claimed.is_empty(),
        "dead-lettered invalidations must not be claimed again"
    );

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_claim_heartbeat_refreshes_claimed_at() -> Result<()> {
    let database = test_database().await?;
    let claim_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "address_names_current",
        "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
        claim_token,
        "4 minutes",
    )
    .await?;

    let before: sqlx::types::time::OffsetDateTime = sqlx::query_scalar(
        r#"
        SELECT claimed_at
        FROM projection_invalidations
        WHERE projection = 'address_names_current'
          AND projection_key = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load initial claim timestamp")?;

    let invalidation = ClaimedInvalidation {
        projection: "address_names_current".to_owned(),
        projection_key: "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401".to_owned(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token,
        attempt_count: 0,
    };
    refresh_claimed_invalidation_claim(database.pool(), &invalidation).await?;

    let (after, refreshed_token): (sqlx::types::time::OffsetDateTime, Uuid) = sqlx::query_as(
        r#"
            SELECT claimed_at, claim_token
            FROM projection_invalidations
            WHERE projection = 'address_names_current'
              AND projection_key = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
            "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load refreshed claim timestamp")?;

    assert!(after > before);
    assert_eq!(refreshed_token, claim_token);

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_apply_locks_serialize_same_key() -> Result<()> {
    let database = test_database().await?;
    let invalidation = ClaimedInvalidation {
        projection: "permissions_current".to_owned(),
        projection_key: Uuid::new_v4().to_string(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token: Uuid::new_v4(),
        attempt_count: 0,
    };

    let mut first_lock =
        acquire_invalidation_apply_locks(database.pool(), std::slice::from_ref(&invalidation))
            .await?;
    let lock_key = invalidation_apply_lock_key(&invalidation);
    let mut second_conn = database
        .pool()
        .acquire()
        .await
        .context("failed to acquire second lock probe connection")?;

    let acquired_while_locked: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(&lock_key)
            .fetch_one(&mut *second_conn)
            .await
            .context("failed to probe held projection invalidation apply lock")?;
    assert!(!acquired_while_locked);

    release_invalidation_apply_locks(&mut first_lock).await?;
    drop(first_lock);
    let acquired_after_release: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(&lock_key)
            .fetch_one(&mut *second_conn)
            .await
            .context("failed to acquire released projection invalidation apply lock")?;
    assert!(acquired_after_release);
    let released_probe_lock: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(&lock_key)
            .fetch_one(&mut *second_conn)
            .await
            .context("failed to release second projection invalidation apply lock")?;
    assert!(released_probe_lock);
    drop(second_conn);

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_apply_lock_acquisition_is_bounded_when_blocked() -> Result<()> {
    let database = test_database().await?;
    let invalidation = ClaimedInvalidation {
        projection: "permissions_current".to_owned(),
        projection_key: Uuid::new_v4().to_string(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token: Uuid::new_v4(),
        attempt_count: 0,
    };

    let mut first_lock =
        acquire_invalidation_apply_locks(database.pool(), std::slice::from_ref(&invalidation))
            .await?;

    let blocked = timeout(
        Duration::from_secs(1),
        acquire_invalidation_apply_locks_with_timeout(
            database.pool(),
            std::slice::from_ref(&invalidation),
            Duration::from_millis(100),
        ),
    )
    .await;

    release_invalidation_apply_locks(&mut first_lock).await?;
    let result = blocked.context("lock acquisition blocked past outer test timeout")?;
    let error = match result {
        Ok(mut locks) => {
            release_invalidation_apply_locks(&mut locks).await?;
            bail!("blocked lock acquisition unexpectedly succeeded");
        }
        Err(error) => error,
    };
    assert!(
        format!("{error:#}").contains("timed out acquiring projection invalidation apply lock"),
        "unexpected blocked lock acquisition error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_apply_lock_liveness_detects_dead_connection() -> Result<()> {
    let database = test_database().await?;
    let invalidation = ClaimedInvalidation {
        projection: "permissions_current".to_owned(),
        projection_key: Uuid::new_v4().to_string(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token: Uuid::new_v4(),
        attempt_count: 0,
    };

    let mut locks =
        acquire_invalidation_apply_locks(database.pool(), std::slice::from_ref(&invalidation))
            .await?;
    let lock_backend_pid = invalidation_apply_locks_backend_pid(&mut locks).await?;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(lock_backend_pid)
        .fetch_one(database.pool())
        .await
        .context("failed to terminate projection invalidation apply lock backend")?;
    assert!(terminated);

    let liveness = ensure_invalidation_apply_locks_alive(&mut locks).await;
    assert!(
        liveness.is_err(),
        "dead projection invalidation apply lock connection must fail liveness check"
    );

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_apply_lock_liveness_probe_is_bounded_when_blocked() -> Result<()> {
    let database = test_database().await?;
    let mut conn = open_invalidation_apply_locks_connection_for_test(database.pool()).await?;

    let blocked = timeout(
        Duration::from_secs(1),
        ensure_invalidation_apply_locks_probe_alive_for_test(
            &mut conn,
            Duration::from_millis(100),
            "SELECT 1 FROM pg_sleep(5)",
        ),
    )
    .await;
    let result = blocked.context("liveness probe ignored its timeout and blocked the worker")?;
    let error = result.expect_err("blocked liveness probe unexpectedly succeeded");
    assert!(
        format!("{error:#}")
            .contains("timed out running projection invalidation apply lock liveness probe"),
        "unexpected blocked liveness probe error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn basenames_name_current_invalidations_are_claimed_before_older_ens_names() -> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:older.eth").await?;
    insert_unclaimed_invalidation(&database, "name_current", "basenames:newer.base.eth").await?;
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET last_changed_at = now() - '10 minutes'::INTERVAL
        WHERE projection = 'name_current'
          AND projection_key = 'ens:older.eth'
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to age ENS projection invalidation")?;

    let claimed = claim_pending_invalidations(database.pool(), 1, Uuid::new_v4()).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].projection, "name_current");
    assert_eq!(claimed[0].projection_key, "basenames:newer.base.eth");

    database.cleanup().await
}

#[test]
fn claimed_invalidation_apply_order_prioritizes_basenames_name_current() {
    let mut invalidations = vec![
        claimed_invalidation("name_current", "ens:later.eth"),
        claimed_invalidation("permissions_current", "permissions:example"),
        claimed_invalidation("name_current", "basenames:base-name.base.eth"),
        claimed_invalidation("name_current", "ens:earlier.eth"),
    ];

    sort_claimed_invalidations_for_apply(&mut invalidations);

    let ordered_keys = invalidations
        .iter()
        .map(|invalidation| invalidation.projection_key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_keys,
        vec![
            "basenames:base-name.base.eth",
            "ens:earlier.eth",
            "ens:later.eth",
            "permissions:example"
        ]
    );
}

#[test]
fn address_name_invalidations_are_grouped_by_address() {
    let mut invalidations = vec![
        address_names_claimed_invalidation(
            "0x1111111111111111111111111111111111111111",
            Some("ens:first.eth"),
        ),
        address_names_claimed_invalidation(
            "0x1111111111111111111111111111111111111111",
            Some("ens:second.eth"),
        ),
        address_names_claimed_invalidation(
            "0x2222222222222222222222222222222222222222",
            Some("ens:third.eth"),
        ),
        claimed_invalidation("primary_names_current", "primary:example"),
    ];

    let group = drain_address_names_group(&mut invalidations);

    assert_eq!(group.len(), 2);
    assert_eq!(
        group
            .iter()
            .map(|invalidation| {
                payload_str(&invalidation.key_payload, "logical_name_id").unwrap()
            })
            .collect::<Vec<_>>(),
        vec!["ens:first.eth", "ens:second.eth"]
    );
    assert_eq!(invalidations.len(), 2);
    assert_eq!(
        payload_str(&invalidations[0].key_payload, "address").unwrap(),
        "0x2222222222222222222222222222222222222222"
    );
}

#[tokio::test]
async fn every_targeted_apply_family_records_internal_progress() -> Result<()> {
    let database = test_database().await?;
    let instance_id = "projection-apply-family-progress-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    let mut heartbeat = LoopHeartbeat::new(instance_id.to_owned(), Duration::ZERO);

    let cases = [
        claimed_invalidation("name_current", "ens:missing-name.eth"),
        claimed_invalidation("children_current", "ens:missing-parent.eth"),
        claimed_invalidation("permissions_current", &Uuid::new_v4().to_string()),
        claimed_invalidation("record_inventory_current", &Uuid::new_v4().to_string()),
        ClaimedInvalidation {
            projection: "resolver_current".to_owned(),
            projection_key: "ethereum-mainnet:0x1111111111111111111111111111111111111111"
                .to_owned(),
            key_payload: serde_json::json!({
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x1111111111111111111111111111111111111111"
            }),
            generation: 0,
            claim_token: Uuid::nil(),
            attempt_count: 0,
        },
        ClaimedInvalidation {
            projection: "primary_names_current".to_owned(),
            projection_key: "0x2222222222222222222222222222222222222222:ens:60".to_owned(),
            key_payload: serde_json::json!({
                "address": "0x2222222222222222222222222222222222222222",
                "namespace": "ens",
                "coin_type": "60"
            }),
            generation: 0,
            claim_token: Uuid::nil(),
            attempt_count: 0,
        },
    ];

    for invalidation in &cases {
        let before = heartbeat.progress_record_count();
        {
            let mut progress = Some(&mut heartbeat);
            apply_one(database.pool(), invalidation, None, &mut progress).await?;
        }
        assert!(
            heartbeat.progress_record_count() > before,
            "{} targeted apply must report progress inside its projection work",
            invalidation.projection
        );
    }

    let address = "0x3333333333333333333333333333333333333333";
    let group = (0..40)
        .map(|index| {
            address_names_claimed_invalidation(address, Some(&format!("ens:missing-{index}.eth")))
        })
        .collect::<Vec<_>>();
    let before = heartbeat.progress_record_count();
    {
        let mut progress = Some(&mut heartbeat);
        apply_address_names_group(database.pool(), &group, &mut progress).await?;
    }
    assert!(
        heartbeat.progress_record_count() >= before + group.len(),
        "address_names_current targeted apply must report each completed logical-name unit"
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_apply_beats_before_a_blocked_publish_finishes() -> Result<()> {
    let database = test_database().await?;
    let instance_id = "projection-apply-in-flight-progress-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'worker'
          AND instance_id = $1
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;

    let address = "0x4444444444444444444444444444444444444444";
    let mut publish_blocker = database.pool().begin().await?;
    sqlx::query("SELECT address_names_current_identity_counts_lock_address($1)")
        .bind(address)
        .execute(&mut *publish_blocker)
        .await?;

    let group = (0..40)
        .map(|index| {
            address_names_claimed_invalidation(address, Some(&format!("ens:blocked-{index}.eth")))
        })
        .collect::<Vec<_>>();
    let apply_pool = database.pool().clone();
    let apply = tokio::spawn(async move {
        let mut heartbeat = LoopHeartbeat::new(instance_id.to_owned(), Duration::ZERO);
        let mut progress = Some(&mut heartbeat);
        apply_address_names_group(&apply_pool, &group, &mut progress).await
    });

    let heartbeat_observed = timeout(Duration::from_secs(10), async {
        loop {
            let heartbeat = bigname_storage::load_service_loop_heartbeat(
                database.pool(),
                bigname_storage::WORKER_SERVICE_NAME,
                instance_id,
            )
            .await?
            .context("targeted apply must retain its registered heartbeat")?;
            if heartbeat.age_seconds <= 1 {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if let Err(error) = heartbeat_observed {
        apply.abort();
        publish_blocker.rollback().await?;
        let _ = apply.await;
        database.cleanup().await?;
        return Err(error).context("targeted apply did not beat before blocked publication");
    }
    heartbeat_observed??;
    assert!(
        !apply.is_finished(),
        "publication must still be blocked when internal progress is observed"
    );

    publish_blocker.rollback().await?;
    timeout(Duration::from_secs(10), apply)
        .await
        .context("targeted apply did not finish after publication was unblocked")?
        .context("targeted apply task failed")??;

    database.cleanup().await
}

#[tokio::test]
async fn address_name_default_payload_invalidation_rebuilds_by_projection_key() -> Result<()> {
    let database = test_database().await?;
    let address = "0x3333333333333333333333333333333333333333";

    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            last_changed_at,
            invalidated_at
        )
        VALUES (
            'address_names_current',
            $1,
            '{}'::jsonb,
            now(),
            now()
        )
        "#,
    )
    .bind(address)
    .execute(database.pool())
    .await
    .context("failed to seed broad address_names_current invalidation")?;

    let summary = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(
        summary,
        ProjectionInvalidationApplySummary {
            claimed_invalidation_count: 1,
            applied_invalidation_count: 1,
            failed_invalidation_count: 0,
        }
    );

    let remaining_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE projection = 'address_names_current'
          AND projection_key = $1
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to count remaining broad address_names_current invalidation")?;
    assert_eq!(remaining_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn primary_name_repair_invalidations_delete_old_tuple_and_rebuild_new_tuple() -> Result<()> {
    let database = test_database().await?;
    let address = "0x7e50c29692e8d701a375bf53de93b62f9aa47af8";

    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            claim_provenance,
            normalized_claim_name
        )
        VALUES ($1, '60', 'basenames', 'success', NULL, '{}'::jsonb, 'old.base.eth')
        "#,
    )
    .bind(address)
    .execute(database.pool())
    .await
    .context("failed to seed stale old primary-name tuple")?;

    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state,
            observed_at
        )
        VALUES (
            'projection-apply:base-primary-repair-reverse',
            'basenames',
            NULL,
            NULL,
            'ReverseChanged',
            'basenames_base_primary',
            1,
            'base-mainnet',
            100,
            '0xbase100',
            '{}'::jsonb,
            'ens_v1_reverse_claim',
            'canonical'::canonicality_state,
            '{}'::jsonb,
            $1,
            now()
        ),
        (
            'projection-apply:base-primary-repair-name',
            'basenames',
            NULL,
            NULL,
            'RecordChanged',
            'basenames_base_primary',
            1,
            'base-mainnet',
            100,
            '0xbase100',
            '{}'::jsonb,
            'ens_v1_reverse_claim',
            'canonical'::canonicality_state,
            '{}'::jsonb,
            $2,
            now()
        )
        "#,
    )
    .bind(serde_json::json!({
        "address": address,
        "namespace": "basenames",
        "coin_type": "2147492101",
        "claim_provenance": {
            "contract_role": "reverse_registrar",
            "source_family": "basenames_base_primary",
            "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664"
        }
    }))
    .bind(serde_json::json!({
        "record_key": "name",
        "raw_name": "fixed.base.eth",
        "primary_claim_source": {
            "address": address,
            "namespace": "basenames",
            "coin_type": "2147492101",
            "reverse_name": "7e50c29692e8d701a375bf53de93b62f9aa47af8.80002105.reverse",
            "reverse_node": "0x76097049b6146b77e9cd73ee786c29ae4eefb49e4772d0a3cefd99f7087760c5",
            "claim_provenance": {
                "contract_role": "reverse_registrar",
                "source_family": "basenames_base_primary",
                "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664"
            }
        }
    }))
    .execute(database.pool())
    .await
    .context("failed to seed corrected primary-name normalized events")?;

    insert_primary_invalidation(&database, address, "basenames", "60").await?;
    insert_primary_invalidation(&database, address, "basenames", "2147492101").await?;

    let summary = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(summary.claimed_invalidation_count, 2);
    assert_eq!(summary.applied_invalidation_count, 2);
    assert_eq!(summary.failed_invalidation_count, 0);

    let old_tuple_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM primary_names_current
        WHERE address = $1
          AND namespace = 'basenames'
          AND coin_type = '60'
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to count stale old primary-name tuple")?;
    assert_eq!(old_tuple_count, 0);

    let new_claim = sqlx::query_scalar::<_, String>(
        r#"
        SELECT normalized_claim_name
        FROM primary_names_current
        WHERE address = $1
          AND namespace = 'basenames'
          AND coin_type = '2147492101'
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to load rebuilt corrected primary-name tuple")?;
    assert_eq!(new_claim, "fixed.base.eth");

    database.cleanup().await
}

fn claimed_invalidation(projection: &str, projection_key: &str) -> ClaimedInvalidation {
    ClaimedInvalidation {
        projection: projection.to_string(),
        projection_key: projection_key.to_string(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token: Uuid::nil(),
        attempt_count: 0,
    }
}

fn address_names_claimed_invalidation(
    address: &str,
    logical_name_id: Option<&str>,
) -> ClaimedInvalidation {
    let projection_key = match logical_name_id {
        Some(logical_name_id) => format!("{address}:{logical_name_id}"),
        None => address.to_owned(),
    };
    let key_payload = match logical_name_id {
        Some(logical_name_id) => {
            serde_json::json!({ "address": address, "logical_name_id": logical_name_id })
        }
        None => serde_json::json!({ "address": address }),
    };
    ClaimedInvalidation {
        projection: "address_names_current".to_owned(),
        projection_key,
        key_payload,
        generation: 0,
        claim_token: Uuid::nil(),
        attempt_count: 0,
    }
}

async fn insert_unclaimed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ($1, $2, '{}'::jsonb)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .execute(database.pool())
    .await
    .context("failed to insert projection invalidation")?;

    Ok(())
}

async fn insert_primary_invalidation(
    database: &TestDatabase,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES (
            'primary_names_current',
            $1 || ':' || $2 || ':' || $3,
            jsonb_build_object(
                'address', $1,
                'namespace', $2,
                'coin_type', $3
            )
        )
        "#,
    )
    .bind(address)
    .bind(namespace)
    .bind(coin_type)
    .execute(database.pool())
    .await
    .context("failed to insert primary-name projection invalidation")?;

    Ok(())
}

async fn insert_poison_invalidation(
    database: &TestDatabase,
    projection_key: &str,
    attempt_count: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            attempt_count
        )
        VALUES (
            'unsupported_projection',
            $1,
            '{}'::jsonb,
            $2
        )
        "#,
    )
    .bind(projection_key)
    .bind(attempt_count)
    .execute(database.pool())
    .await
    .context("failed to insert poison projection invalidation")?;

    Ok(())
}

async fn insert_claimed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
    claim_token: Uuid,
    claim_age: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            claim_token,
            claimed_at
        )
        VALUES ($1, $2, '{}'::jsonb, $3, now() - $4::INTERVAL)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .bind(claim_token)
    .bind(claim_age)
    .execute(database.pool())
    .await
    .context("failed to insert claimed projection invalidation")?;

    Ok(())
}

async fn admit_replay_version(database: &TestDatabase, replay_version: i32) -> Result<()> {
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.projection_replay_version', $1, true)")
        .bind(replay_version.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET
            projection_replay_version_floor = $1,
            projection_replay_version_fence_active = true
        WHERE singleton
        "#,
    )
    .bind(replay_version)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_attempt (
            singleton,
            replay_version,
            normalized_target_block,
            full_replay_input_revision,
            apply_baseline_change_id
        )
        VALUES (true, $1, NULL, 0, 0)
        "#,
    )
    .bind(replay_version)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}
