use anyhow::Result;
use bigname_storage::load_resolver_profile_authority_journal;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

fn admission_semantics(status: &str, admission_basis: &str) -> ResolverProfileAdmissionSemantics {
    ResolverProfileAdmissionSemantics {
        profile: "ens_v1_public_resolver_compatible".to_owned(),
        fact_family: "ens_v1_resolver_records".to_owned(),
        status: status.to_owned(),
        admission_basis: admission_basis.to_owned(),
        matched_code_hash: Some("0x01".to_owned()),
        matched_contract_instance_id: Some(Uuid::from_u128(1)),
    }
}

fn entry(address: &str, is_seed: bool) -> ResolverProfileAuthorityEntry {
    ResolverProfileAuthorityEntry {
        chain: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        address: address.to_owned(),
        contract_instance_id: Uuid::new_v4(),
        source: "discovery_edge".to_owned(),
        source_manifest_id: Some(1),
        active_from_block_number: Some(1),
        active_to_block_number: None,
        is_seed,
        admission_semantics: BTreeSet::from([admission_semantics(
            "admitted",
            if is_seed {
                "manifest_public_resolver_seed"
            } else {
                "matching_seed_code_hash"
            },
        )]),
    }
}

#[test]
fn authority_entry_key_changes_only_with_natural_identity() -> Result<()> {
    let before = entry("0x0000000000000000000000000000000000000001", true);
    let before_key =
        bigname_storage::resolver_profile_authority_entry_key(&serde_json::to_value(&before)?)?;
    let mut semantics_changed = before.clone();
    semantics_changed.admission_semantics = BTreeSet::from([admission_semantics(
        "admitted",
        "first_party_known_resolver_admission",
    )]);
    semantics_changed.is_seed = false;
    assert_eq!(
        bigname_storage::resolver_profile_authority_entry_key(&serde_json::to_value(
            &semantics_changed
        )?)?,
        before_key
    );
    let mut identity_changed = before;
    identity_changed.address = "0x0000000000000000000000000000000000000002".to_owned();
    assert_ne!(
        bigname_storage::resolver_profile_authority_entry_key(&serde_json::to_value(
            identity_changed
        )?)?,
        before_key
    );
    Ok(())
}

#[tokio::test]
async fn authority_journal_rejects_pool_that_runtime_guard_would_starve() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_authority_pool_capacity")
            .pool_max_connections(2),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile pool-capacity test",
    )
    .await?;
    let runtime_guard = bigname_storage::hold_base_normalized_rederive_runtime_shared_lock(
        database.pool(),
        "bigname-indexer",
    )
    .await?;

    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        journal_resolver_profile_authority(database.pool()),
    )
    .await
    .expect("authority journal must reject an undersized pool instead of waiting forever")
    .expect_err("authority journal must reject a pool with only one usable connection");
    assert!(
        format!("{error:?}").contains("requires at least 3 database connections"),
        "{error:?}"
    );

    drop(runtime_guard);
    database.cleanup().await
}

#[tokio::test]
async fn authority_journal_pages_targets_with_runtime_guard_on_three_connections() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_authority_three_connections")
            .pool_max_connections(3),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile connection-topology test",
    )
    .await?;
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (
            1,
            'ens',
            'ens_v1_resolver_l1',
            'ethereum-mainnet',
            'authority-journal-test',
            'active',
            'test',
            'authority-journal-test.toml',
            '{"roots": [], "contracts": []}'::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        SELECT
            md5('authority-target-' || target::TEXT)::UUID,
            'ethereum-mainnet',
            'resolver'
        FROM generate_series(1, 251) AS target
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role
        )
        SELECT
            $1,
            'contract',
            'target-' || target::TEXT,
            md5('authority-target-' || target::TEXT)::UUID,
            '0x' || LPAD(TO_HEX(target), 40, '0'),
            'test_resolver'
        FROM generate_series(1, 251) AS target
        "#,
    )
    .bind(manifest_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            source_manifest_id
        )
        SELECT
            md5('authority-target-' || target::TEXT)::UUID,
            'ethereum-mainnet',
            '0x' || LPAD(TO_HEX(target), 40, '0'),
            1,
            $1
        FROM generate_series(1, 251) AS target
        "#,
    )
    .bind(manifest_id)
    .execute(database.pool())
    .await?;

    let runtime_guard = bigname_storage::hold_base_normalized_rederive_runtime_shared_lock(
        database.pool(),
        "bigname-indexer",
    )
    .await?;
    let summary = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        journal_resolver_profile_authority(database.pool()),
    )
    .await
    .expect("three-connection authority journal must not starve admission reads")?;
    drop(runtime_guard);

    assert!(summary.journal_advanced);
    assert_eq!(summary.authority_scan_count, 1);
    assert_eq!(summary.enqueued_target_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM resolver_profile_authority_journal_entries"
        )
        .fetch_one(database.pool())
        .await?,
        251,
        "the cursor must consume the page after the first 250 targets"
    );

    database.cleanup().await
}

#[tokio::test]
async fn unchanged_epoch_guard_does_not_load_authority_entries() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_epoch_only_guard"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile epoch guard test",
    )
    .await?;

    // Removing the entry table in this isolated database proves that the cheap
    // epoch guard reads only the journal header.
    sqlx::query("DROP TABLE resolver_profile_authority_journal_entries")
        .execute(database.pool())
        .await?;

    let summary =
        journal_resolver_profile_authority_if_epoch_changed(database.pool(), "ethereum-mainnet")
            .await?;
    assert_eq!(summary.epoch_guard_count, 1);
    assert_eq!(summary.authority_scan_count, 0);
    assert!(!summary.journal_advanced);

    database.cleanup().await
}

#[tokio::test]
async fn empty_initial_capture_establishes_baseline_before_later_addition() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_empty_authority_baseline"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for empty resolver-profile authority baseline test",
    )
    .await?;

    let first = journal_resolver_profile_authority(database.pool()).await?;
    assert!(first.journal_advanced);
    assert_eq!(first.enqueued_target_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM resolver_profile_input_changes")
            .fetch_one(database.pool())
            .await?,
        0,
        "an empty first capture is a baseline, not repair work"
    );

    let baseline = load_resolver_profile_authority_journal(database.pool()).await?;
    assert_eq!(baseline.revision, 1);
    let before = ResolverProfileAuthoritySnapshot::default();
    assert_eq!(before, ResolverProfileAuthoritySnapshot::default());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM resolver_profile_authority_journal_entries"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    let address = "0x0000000000000000000000000000000000000002";
    let added = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([entry(address, false)]),
    };
    let second = journal_resolver_profile_authority_attempt(
        database.pool(),
        &baseline,
        &before,
        &added,
        &BTreeMap::new(),
    )
    .await?;
    assert!(second.journal_advanced);
    assert_eq!(second.enqueued_target_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (bool, bool)>(
            r#"
            SELECT
                processed_generation < generation AS pending,
                force_reconciliation
            FROM resolver_profile_input_changes
            WHERE chain_id = 'ethereum-mainnet'
              AND contract_address = $1
            "#,
        )
        .bind(address)
        .fetch_one(database.pool())
        .await?,
        (true, true),
        "authority added after the baseline must become forced repair work"
    );

    database.cleanup().await
}

#[tokio::test]
async fn journal_baselines_initial_authority_then_queues_later_removals() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_authority_journal"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile authority journal test",
    )
    .await?;
    let address = "0x0000000000000000000000000000000000000002";
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since
        ) VALUES ('ethereum-mainnet', 0, 1, false, now())
        "#,
    )
    .execute(database.pool())
    .await?;
    let added = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([entry(address, false)]),
    };
    let initial = load_resolver_profile_authority_journal(database.pool()).await?;
    let first = journal_resolver_profile_authority_attempt(
        database.pool(),
        &initial,
        &ResolverProfileAuthoritySnapshot::default(),
        &added,
        &BTreeMap::new(),
    )
    .await?;
    assert_eq!(first.enqueued_target_count, 0);
    assert!(first.journal_advanced);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM resolver_profile_input_changes")
            .fetch_one(database.pool())
            .await?,
        0,
        "the first journal snapshot is a baseline, not an unproven historical repair request"
    );

    sqlx::query(
        r#"
        CREATE FUNCTION require_profile_authority_enqueue_before_journal()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM resolver_profile_input_changes
                WHERE chain_id = 'ethereum-mainnet'
                  AND contract_address = '0x0000000000000000000000000000000000000002'
                  AND processed_generation < generation
                  AND force_reconciliation
            ) THEN
                RAISE EXCEPTION 'resolver-profile target was not queued before journal CAS';
            END IF;
            RETURN NEW;
        END;
        $$;
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER require_profile_authority_enqueue_before_journal
        BEFORE UPDATE ON resolver_profile_authority_journal
        FOR EACH ROW
        EXECUTE FUNCTION require_profile_authority_enqueue_before_journal();
        "#,
    )
    .execute(database.pool())
    .await?;

    let persisted = load_resolver_profile_authority_journal(database.pool()).await?;
    let before = added.clone();
    assert_eq!(before, added);
    let removed = ResolverProfileAuthoritySnapshot::default();
    let second = journal_resolver_profile_authority_attempt(
        database.pool(),
        &persisted,
        &before,
        &removed,
        &BTreeMap::new(),
    )
    .await?;
    assert_eq!(second.enqueued_target_count, 1);
    assert!(second.journal_advanced);
    assert_eq!(
        sqlx::query_as::<_, (bool, bool)>(
            r#"
            SELECT
                processed_generation < generation AS pending,
                force_reconciliation
            FROM resolver_profile_input_changes
            WHERE chain_id = 'ethereum-mainnet'
              AND contract_address = $1
            "#,
        )
        .bind(address)
        .fetch_one(database.pool())
        .await?,
        (true, true),
        "the persisted before-snapshot must retain a removed target for absence cleanup"
    );
    let final_journal = load_resolver_profile_authority_journal(database.pool()).await?;
    assert_eq!(final_journal.revision, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM resolver_profile_authority_journal_entries"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    let summary = super::super::drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.loaded_input_count, 1);
    assert_eq!(summary.reconciled_target_count, 0);
    assert_eq!(summary.invalidated_projection_key_count, 0);
    assert_eq!(summary.acknowledged_input_count, 0);
    assert_eq!(summary.deferred_input_count, 1);
    assert_eq!(
        summary.deferred_chains,
        BTreeSet::from(["ethereum-mainnet".to_owned()])
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT generation, processed_generation
            FROM resolver_profile_input_changes
            WHERE chain_id = 'ethereum-mainnet'
              AND contract_address = $1
            "#,
        )
        .bind(address)
        .fetch_one(database.pool())
        .await?,
        (1, 0),
        "failed-closed work must remain pending for operator recovery"
    );

    database.cleanup().await
}
