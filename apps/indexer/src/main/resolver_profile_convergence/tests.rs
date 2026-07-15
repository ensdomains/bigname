use bigname_storage::ResolverProfileInputChange;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use super::{
    ResolverProfileAuthoritySnapshot,
    authority::{ResolverProfileAdmissionSemantics, ResolverProfileAuthorityEntry},
    drain_resolver_profile_input_changes, expanded_reconciliation_targets,
    invalidations::{
        enqueue_resolver_profile_projection_invalidations,
        load_resolver_profile_projection_invalidation_plan,
    },
};

fn input(chain: &str, address: &str) -> ResolverProfileInputChange {
    ResolverProfileInputChange {
        chain_id: chain.to_owned(),
        contract_address: address.to_owned(),
        generation: 1,
        processed_generation: 0,
        previous_code_hash: None,
        current_code_hash: Some("0x01".to_owned()),
        force_reconciliation: false,
    }
}

fn forced_input(chain: &str, address: &str) -> ResolverProfileInputChange {
    ResolverProfileInputChange {
        force_reconciliation: true,
        ..input(chain, address)
    }
}

fn entry(
    chain: &str,
    source_family: &str,
    address: &str,
    is_seed: bool,
) -> ResolverProfileAuthorityEntry {
    ResolverProfileAuthorityEntry {
        chain: chain.to_owned(),
        source_family: source_family.to_owned(),
        address: address.to_owned(),
        contract_instance_id: Uuid::new_v4(),
        source: "discovery_edge".to_owned(),
        source_manifest_id: Some(1),
        active_from_block_number: Some(1),
        active_to_block_number: None,
        is_seed,
        admission_semantics: std::collections::BTreeSet::from([
            ResolverProfileAdmissionSemantics {
                profile: if source_family == "basenames_base_resolver" {
                    "basenames_l2_resolver"
                } else {
                    "ens_v1_public_resolver_compatible"
                }
                .to_owned(),
                fact_family: "resolver_records".to_owned(),
                status: "admitted".to_owned(),
                admission_basis: if is_seed {
                    "manifest_public_resolver_seed"
                } else {
                    "matching_seed_code_hash"
                }
                .to_owned(),
                matched_code_hash: Some("0x01".to_owned()),
                matched_contract_instance_id: Some(Uuid::from_u128(1)),
            },
        ]),
    }
}

#[test]
fn candidate_change_reconciles_only_the_dirty_address() {
    let dirty = "0x0000000000000000000000000000000000000002";
    let authority = ResolverProfileAuthoritySnapshot {
        entries: [entry(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            dirty,
            false,
        )]
        .into_iter()
        .collect(),
    };

    let targets = expanded_reconciliation_targets(&[input("ethereum-mainnet", dirty)], &authority);
    assert_eq!(targets["ethereum-mainnet"].len(), 1);
    assert!(targets["ethereum-mainnet"].contains(dirty));
}

#[test]
fn any_ens_v1_known_resolver_seed_change_ripples_all_active_candidates() {
    let seed = "0x0000000000000000000000000000000000000001";
    let candidate = "0x0000000000000000000000000000000000000002";
    let authority = ResolverProfileAuthoritySnapshot {
        entries: [
            entry("ethereum-mainnet", "ens_v1_resolver_l1", seed, true),
            entry("ethereum-mainnet", "ens_v1_resolver_l1", candidate, false),
        ]
        .into_iter()
        .collect(),
    };

    let targets = expanded_reconciliation_targets(&[input("ethereum-mainnet", seed)], &authority);
    assert_eq!(targets["ethereum-mainnet"].len(), 2);
    assert!(targets["ethereum-mainnet"].contains(candidate));
}

#[test]
fn basenames_seed_change_ripples_only_the_basenames_family() {
    let seed = "0x0000000000000000000000000000000000000011";
    let candidate = "0x0000000000000000000000000000000000000012";
    let unrelated = "0x0000000000000000000000000000000000000013";
    let authority = ResolverProfileAuthoritySnapshot {
        entries: [
            entry("base-mainnet", "basenames_base_resolver", seed, true),
            entry("base-mainnet", "basenames_base_resolver", candidate, false),
            entry("base-mainnet", "ens_v1_resolver_l1", unrelated, false),
        ]
        .into_iter()
        .collect(),
    };

    let targets = expanded_reconciliation_targets(&[input("base-mainnet", seed)], &authority);
    assert!(targets["base-mainnet"].contains(seed));
    assert!(targets["base-mainnet"].contains(candidate));
    assert!(!targets["base-mainnet"].contains(unrelated));
}

#[test]
fn removed_profile_address_with_an_authority_kick_gets_an_absence_aware_pass() {
    let dirty = "0x0000000000000000000000000000000000000099";
    let targets = expanded_reconciliation_targets(
        &[forced_input("ethereum-mainnet", dirty)],
        &ResolverProfileAuthoritySnapshot::default(),
    );
    assert_eq!(
        targets["ethereum-mainnet"]
            .iter()
            .next()
            .map(String::as_str),
        Some(dirty)
    );
}

#[test]
fn ordinary_non_resolver_raw_code_change_has_no_reconciliation_target() {
    let dirty = "0x0000000000000000000000000000000000000099";
    let targets = expanded_reconciliation_targets(
        &[input("ethereum-mainnet", dirty)],
        &ResolverProfileAuthoritySnapshot::default(),
    );
    assert!(targets.is_empty());
}

#[tokio::test]
async fn ordinary_non_resolver_raw_code_change_is_acknowledged_without_invalidations()
-> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_non_resolver_profile_input"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for non-resolver profile input test",
    )
    .await?;
    let dirty = "0x0000000000000000000000000000000000000099";
    sqlx::query(
        r#"
        INSERT INTO resolver_profile_input_changes (
            chain_id,
            contract_address,
            previous_code_hash,
            current_code_hash
        ) VALUES ('ethereum-mainnet', $1, NULL, '0x01')
        "#,
    )
    .bind(dirty)
    .execute(database.pool())
    .await?;

    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.reconciled_target_count, 0);
    assert_eq!(summary.invalidated_projection_key_count, 0);
    assert_eq!(summary.acknowledged_input_count, 1);
    let processed = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT generation, processed_generation
        FROM resolver_profile_input_changes
        WHERE chain_id = 'ethereum-mainnet'
          AND contract_address = $1
        "#,
    )
    .bind(dirty)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(processed, (1, 1));
    let invalidation_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM projection_invalidations")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(invalidation_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn forced_removed_last_target_converges_without_another_raw_code_write() -> anyhow::Result<()>
{
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_removed_last_resolver_profile_target"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for removed resolver-profile target test",
    )
    .await?;
    let removed = "0x0000000000000000000000000000000000000088";
    bigname_storage::enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[bigname_storage::ResolverProfileReconciliationTarget {
            chain_id: "ethereum-mainnet".to_owned(),
            contract_address: removed.to_owned(),
        }],
    )
    .await?;

    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.reconciled_target_count, 1);
    assert_eq!(summary.acknowledged_input_count, 1);
    let resolver_key = sqlx::query_scalar::<_, String>(
        r#"
        SELECT projection_key
        FROM projection_invalidations
        WHERE projection = 'resolver_current'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(resolver_key, format!("ethereum-mainnet:{removed}"));

    database.cleanup().await
}

async fn seed_resolver_raw_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    resolver: &str,
    blocks: &[(i64, &str)],
) -> anyhow::Result<()> {
    let mut parent_hash = None::<&str>;
    for (index, (block_number, block_hash)) in blocks.iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                parent_hash,
                block_number,
                block_timestamp,
                canonicality_state
            ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            "#,
        )
        .bind(chain)
        .bind(block_hash)
        .bind(parent_hash)
        .bind(block_number)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            ) VALUES ($1, $2, $3, $4, 0, 0, $5, ARRAY[]::TEXT[], '\x', 'canonical')
            "#,
        )
        .bind(chain)
        .bind(block_hash)
        .bind(block_number)
        .bind(format!("0xresolver-profile-retention-{index}"))
        .bind(resolver)
        .execute(pool)
        .await?;
        parent_hash = Some(block_hash);
    }
    Ok(())
}

async fn assert_resolver_profile_generation_pending(
    pool: &sqlx::PgPool,
    chain: &str,
    resolver: &str,
) -> anyhow::Result<()> {
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT generation, processed_generation
            FROM resolver_profile_input_changes
            WHERE chain_id = $1 AND contract_address = $2
            "#,
        )
        .bind(chain)
        .bind(resolver)
        .fetch_one(pool)
        .await?,
        (1, 0),
        "an unproven resolver-profile replay must remain pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM projection_invalidations")
            .fetch_one(pool)
            .await?,
        0,
        "projection invalidations must not publish before resolver events converge"
    );
    Ok(())
}

#[tokio::test]
async fn fully_compacted_history_keeps_profile_generation_pending() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_full_compaction"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile full-compaction test",
    )
    .await?;
    let chain = "ethereum-mainnet";
    let resolver = "0x0000000000000000000000000000000000000088";
    seed_resolver_raw_logs(
        database.pool(),
        chain,
        resolver,
        &[(1, "0xresolver-profile-full-compaction-block")],
    )
    .await?;
    bigname_storage::enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[bigname_storage::ResolverProfileReconciliationTarget {
            chain_id: chain.to_owned(),
            contract_address: resolver.to_owned(),
        }],
    )
    .await?;
    sqlx::query("TRUNCATE raw_logs")
        .execute(database.pool())
        .await?;

    drain_resolver_profile_input_changes(database.pool())
        .await
        .expect_err("fully compacted resolver history must fail closed");
    assert_resolver_profile_generation_pending(database.pool(), chain, resolver).await?;

    database.cleanup().await
}

#[tokio::test]
async fn partially_compacted_history_keeps_profile_generation_pending() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_partial_compaction"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile partial-compaction test",
    )
    .await?;
    let chain = "ethereum-mainnet";
    let resolver = "0x0000000000000000000000000000000000000088";
    seed_resolver_raw_logs(
        database.pool(),
        chain,
        resolver,
        &[
            (1, "0xresolver-profile-partial-compaction-block-1"),
            (2, "0xresolver-profile-partial-compaction-block-2"),
        ],
    )
    .await?;
    bigname_storage::enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[bigname_storage::ResolverProfileReconciliationTarget {
            chain_id: chain.to_owned(),
            contract_address: resolver.to_owned(),
        }],
    )
    .await?;
    sqlx::query("DELETE FROM raw_logs WHERE chain_id = $1 AND block_number = 1")
        .bind(chain)
        .execute(database.pool())
        .await?;

    drain_resolver_profile_input_changes(database.pool())
        .await
        .expect_err("partially compacted resolver history must fail closed");
    assert_resolver_profile_generation_pending(database.pool(), chain, resolver).await?;

    database.cleanup().await
}

#[tokio::test]
async fn invalidation_plan_includes_readable_history_but_excludes_orphaned_fork_rows()
-> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_invalidation_scope"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile invalidation scope test",
    )
    .await?;
    let resolver = "0x00000000000000000000000000000000000000aa";
    let readable_resource = Uuid::new_v4();
    let orphaned_resource = Uuid::new_v4();
    let orphaned_binding_resource = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id, chain_id, block_hash, block_number, canonicality_state
        ) VALUES
            ($1, 'ethereum-mainnet', '0x01', 1, 'canonical'),
            ($2, 'ethereum-mainnet', '0x02', 2, 'orphaned'),
            ($3, 'ethereum-mainnet', '0x03', 3, 'canonical')
        "#,
    )
    .bind(readable_resource)
    .bind(orphaned_resource)
    .bind(orphaned_binding_resource)
    .execute(database.pool())
    .await?;
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
            derivation_kind,
            canonicality_state,
            after_state
        ) VALUES
            ('readable-binding', 'ens', 'ens:readable.eth', $1,
             'ResolverChanged', 'ens_v1_registry_l1', 1,
             'ethereum-mainnet', 'ens_v1_unwrapped_authority', 'canonical',
             jsonb_build_object('resolver', $3::TEXT)),
            ('readable-record', 'ens', 'ens:readable.eth', $1,
             'RecordChanged', 'ens_v1_resolver_l1', 1,
             'ethereum-mainnet', 'ens_v1_unwrapped_authority', 'canonical', '{}'),
            ('orphaned-binding', 'ens', 'ens:orphaned.eth', $2,
             'ResolverChanged', 'ens_v1_registry_l1', 1,
             'ethereum-mainnet', 'ens_v1_unwrapped_authority', 'orphaned',
             jsonb_build_object('resolver', $3::TEXT)),
            ('orphaned-record', 'ens', 'ens:orphaned.eth', $2,
             'RecordChanged', 'ens_v1_resolver_l1', 1,
             'ethereum-mainnet', 'ens_v1_unwrapped_authority', 'orphaned', '{}')
        "#,
    )
    .bind(readable_resource)
    .bind(orphaned_resource)
    .bind(resolver)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        WITH inserted_surface AS (
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        ) VALUES (
            'ens:readable.eth', 'ens', 'readable.eth', 'readable.eth',
            'readable.eth', '\x00', '0xnamehash', ARRAY[]::TEXT[], 'test-v1',
            'ethereum-mainnet', '0x01', 1, 'canonical'
        )
        RETURNING logical_name_id
        )
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        ) SELECT
            $1, inserted_surface.logical_name_id, $2, 'declared_registry_path',
            '2026-01-01T00:00:00Z', 'ethereum-mainnet', '0x03', 3, 'orphaned'
        FROM inserted_surface
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(orphaned_binding_resource)
    .execute(database.pool())
    .await?;

    let targets = std::collections::BTreeMap::from([(
        "ethereum-mainnet".to_owned(),
        std::collections::BTreeSet::from([resolver.to_owned()]),
    )]);
    let plan =
        load_resolver_profile_projection_invalidation_plan(database.pool(), &targets).await?;
    enqueue_resolver_profile_projection_invalidations(database.pool(), &plan).await?;
    let inventory_keys = sqlx::query_scalar::<_, String>(
        r#"
        SELECT projection_key
        FROM projection_invalidations
        WHERE projection = 'record_inventory_current'
        ORDER BY projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(inventory_keys, vec![readable_resource.to_string()]);

    database.cleanup().await
}
