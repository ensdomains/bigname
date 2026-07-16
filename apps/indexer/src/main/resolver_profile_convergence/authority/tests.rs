use anyhow::Result;
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
fn candidate_authority_change_targets_only_that_address() {
    let before = ResolverProfileAuthoritySnapshot::default();
    let candidate = entry("0x0000000000000000000000000000000000000002", false);
    let after = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([candidate.clone()]),
    };

    assert_eq!(
        authority_change_targets(&before, &after),
        vec![ResolverProfileReconciliationTarget {
            chain_id: candidate.chain,
            contract_address: candidate.address,
        }]
    );
}

#[test]
fn seed_authority_change_targets_every_family_candidate() {
    let seed = entry("0x0000000000000000000000000000000000000001", true);
    let candidate = entry("0x0000000000000000000000000000000000000002", false);
    let after = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([seed, candidate]),
    };

    let targets = authority_change_targets(&ResolverProfileAuthoritySnapshot::default(), &after);
    assert_eq!(targets.len(), 2);
}

#[test]
fn admission_semantics_change_targets_the_unchanged_candidate_identity() {
    let before_entry = entry("0x0000000000000000000000000000000000000002", false);
    let mut after_entry = before_entry.clone();
    after_entry.admission_semantics = BTreeSet::from([admission_semantics(
        "pending_code_hash",
        "matching_seed_code_hash",
    )]);
    let before = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([before_entry]),
    };
    let after = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([after_entry.clone()]),
    };

    assert_eq!(
        authority_change_targets(&before, &after),
        vec![ResolverProfileReconciliationTarget {
            chain_id: after_entry.chain,
            contract_address: after_entry.address,
        }]
    );
}

#[test]
fn seed_admission_semantics_change_ripples_to_unchanged_candidates() {
    let before_seed = entry("0x0000000000000000000000000000000000000001", true);
    let candidate = entry("0x0000000000000000000000000000000000000002", false);
    let mut after_seed = before_seed.clone();
    after_seed.admission_semantics = BTreeSet::from([admission_semantics(
        "admitted",
        "first_party_known_resolver_admission",
    )]);
    let before = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([before_seed, candidate.clone()]),
    };
    let after = ResolverProfileAuthoritySnapshot {
        entries: BTreeSet::from([after_seed, candidate]),
    };

    let targets = authority_change_targets(&before, &after);
    assert_eq!(targets.len(), 2);
    assert_eq!(
        targets[0].contract_address,
        "0x0000000000000000000000000000000000000001"
    );
    assert_eq!(
        targets[1].contract_address,
        "0x0000000000000000000000000000000000000002"
    );
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
    let before = serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
        baseline.authority_snapshot.clone(),
    )?;
    assert_eq!(before, ResolverProfileAuthoritySnapshot::default());

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
    let before = serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
        persisted.authority_snapshot.clone(),
    )?;
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
        serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
            final_journal.authority_snapshot
        )?,
        removed
    );
    let summary = super::super::drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.loaded_input_count, 1);
    assert_eq!(summary.reconciled_target_count, 0);
    assert_eq!(summary.invalidated_projection_key_count, 0);
    assert_eq!(summary.acknowledged_input_count, 0);
    assert_eq!(summary.deferred_input_count, 1);
    assert_eq!(summary.deferred_chain_count, 1);
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
