use bigname_storage::ResolverProfileInputChange;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use crate::tests::{BlockingHeartbeatProgress, install_stale_indexer_heartbeat};

use super::{
    ResolverProfileAuthorityIndex, ResolverProfileAuthoritySnapshot,
    ResolverProfileConvergenceSummary,
    authority::{ResolverProfileAdmissionSemantics, ResolverProfileAuthorityEntry},
    drain_resolver_profile_input_changes, drain_resolver_profile_input_changes_with_progress,
    expanded_reconciliation_targets, expanded_reconciliation_targets_with_family_count,
    input_requires_reconciliation,
    invalidations::stage_resolver_profile_projection_invalidations_with_progress,
};

#[derive(Default)]
struct CountingStartupProgress {
    count: usize,
}

impl bigname_adapters::StartupAdapterProgress for CountingStartupProgress {
    fn record<'a>(
        &'a mut self,
        _pool: &'a sqlx::PgPool,
    ) -> bigname_adapters::StartupAdapterProgressFuture<'a> {
        self.count += 1;
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn completion_guard_refuses_only_the_deferred_chain() {
    let summary = ResolverProfileConvergenceSummary {
        deferred_chains: std::collections::BTreeSet::from(["ethereum-mainnet".to_owned()]),
        ..ResolverProfileConvergenceSummary::default()
    };
    let error = summary
        .ensure_chain_completion_allowed("ethereum-mainnet", "chain checkpoint advancement")
        .expect_err("deferred chain must not publish its checkpoint");
    assert!(
        error
            .to_string()
            .contains("refusing chain checkpoint advancement")
    );
    summary
        .ensure_chain_completion_allowed("base-mainnet", "chain checkpoint advancement")
        .expect("an eligible chain must not inherit another chain's deferral");
}

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

fn authority_index(authority: ResolverProfileAuthoritySnapshot) -> ResolverProfileAuthorityIndex {
    ResolverProfileAuthorityIndex::from_snapshot(authority)
}

#[tokio::test]
async fn invalidation_capture_pages_only_bound_names_and_their_resources() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("resolver_invalidation_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate resolver invalidation paging test database",
    )
    .await?;
    let pool = database.pool();
    let chain = "resolver-invalidation-chain";
    let resolver = "0x0000000000000000000000000000000000000d01";
    let run_id = Uuid::from_u128(0xd01);
    let target_resource = Uuid::from_u128(0xd02);
    let other_resource = Uuid::from_u128(0xd03);

    sqlx::query(
        r#"
        INSERT INTO resolver_profile_reconciliation_runs (
            run_id,
            chain_id,
            first_block_number,
            last_block_number,
            resolver_address_count,
            resolver_address_set_digest
        )
        VALUES ($1, $2, 0, 0, 1, 'test-digest')
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
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
        )
        SELECT
            'ens:other-' || value,
            'ens',
            'other-' || value,
            'other-' || value,
            'other-' || value,
            '\x00',
            'other-namehash-' || value,
            ARRAY[]::TEXT[],
            'test',
            'other-chain',
            'other-block',
            1,
            'canonical'::canonicality_state
        FROM generate_series(1, 2001) value
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resolver_profile_reconciliation_targets (run_id, resolver_address) VALUES ($1, $2)",
    )
    .bind(run_id)
    .bind(resolver)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            raw_fact_ref,
            derivation_kind,
            canonicality_state
        )
        SELECT
            'out-of-scope-invalidation-event-' || value,
            'ens',
            'Other',
            'other_family',
            1,
            'other-chain',
            '{}'::JSONB,
            'other_derivation',
            'canonical'::canonicality_state
        FROM generate_series(1, 2001) value
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
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
        )
        VALUES
            (
                'ens:target',
                'ens',
                'target',
                'target',
                'target',
                '\x00',
                'target-namehash',
                ARRAY[]::TEXT[],
                'test',
                $1,
                'target-block',
                10,
                'canonical'::canonicality_state
            ),
            (
                'ens:other',
                'ens',
                'other',
                'other',
                'other',
                '\x00',
                'other-namehash',
                ARRAY[]::TEXT[],
                'test',
                'other-chain',
                'other-block',
                1,
                'canonical'::canonicality_state
            )
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES
            ($1, $3, 'target-block', 10, 'canonical'::canonicality_state),
            ($2, 'other-chain', 'other-block', 1, 'canonical'::canonicality_state)
        "#,
    )
    .bind(target_resource)
    .bind(other_resource)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state
        )
        VALUES (
            'target-resolver-change',
            'ens',
            'ens:target',
            'ResolverChanged',
            'ens_v1_resolver_l1',
            1,
            $1,
            '{}'::JSONB,
            'ens_v1_unwrapped_authority',
            'canonical'::canonicality_state,
            '{}'::JSONB,
            jsonb_build_object('resolver', $2)
        )
        "#,
    )
    .bind(chain)
    .bind(resolver)
    .execute(pool)
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
            raw_fact_ref,
            derivation_kind,
            canonicality_state
        )
        VALUES (
            'target-resource-event',
            'ens',
            'ens:target',
            $1,
            'RecordChanged',
            'ens_v1_resolver_l1',
            1,
            $2,
            '{}'::JSONB,
            'ens_v1_unwrapped_authority',
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(target_resource)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
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
        )
        SELECT
            md5('out-of-scope-binding-' || value)::UUID,
            'ens:other-' || value,
            $1,
            'observed_only',
            now(),
            'other-chain',
            'other-block',
            1,
            'canonical'::canonicality_state
        FROM generate_series(1, 2001) value
        "#,
    )
    .bind(other_resource)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
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
        )
        VALUES (
            $1,
            'ens:target',
            $2,
            'observed_only',
            now(),
            $3,
            'target-block',
            10,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(Uuid::from_u128(0xd04))
    .bind(target_resource)
    .bind(chain)
    .execute(pool)
    .await?;

    let mut progress = CountingStartupProgress::default();
    stage_resolver_profile_projection_invalidations_with_progress(
        pool,
        run_id,
        chain,
        &mut progress,
    )
    .await?;

    assert_eq!(
        progress.count, 4,
        "one target, bound-name, event-resource, and binding-resource page must beat"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM resolver_profile_reconciliation_invalidation_keys WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(pool)
        .await?,
        2,
        "the resolver and record-inventory keys must be staged once"
    );
    database.cleanup().await
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

    let targets = expanded_reconciliation_targets(
        &[input("ethereum-mainnet", dirty)],
        &authority_index(authority),
    );
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

    let targets = expanded_reconciliation_targets(
        &[input("ethereum-mainnet", seed)],
        &authority_index(authority),
    );
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

    let targets = expanded_reconciliation_targets(
        &[input("base-mainnet", seed)],
        &authority_index(authority),
    );
    assert!(targets["base-mainnet"].contains(seed));
    assert!(targets["base-mainnet"].contains(candidate));
    assert!(!targets["base-mainnet"].contains(unrelated));
}

#[test]
fn removed_profile_address_with_an_authority_kick_gets_an_absence_aware_pass() {
    let dirty = "0x0000000000000000000000000000000000000099";
    let targets = expanded_reconciliation_targets(
        &[forced_input("ethereum-mainnet", dirty)],
        &authority_index(ResolverProfileAuthoritySnapshot::default()),
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
        &authority_index(ResolverProfileAuthoritySnapshot::default()),
    );
    assert!(targets.is_empty());
}

fn reference_expanded_reconciliation_targets(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthoritySnapshot,
) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
    let mut targets =
        std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();

    for input in pending {
        let current_entries = authority
            .entries
            .iter()
            .filter(|entry| {
                entry.chain == input.chain_id && entry.address == input.contract_address
            })
            .collect::<Vec<_>>();
        if current_entries.is_empty() && !input.force_reconciliation {
            continue;
        }
        targets
            .entry(input.chain_id.clone())
            .or_default()
            .insert(input.contract_address.clone());
        for seed in current_entries.into_iter().filter(|entry| entry.is_seed) {
            for candidate in authority.entries.iter().filter(|candidate| {
                candidate.chain == seed.chain && candidate.source_family == seed.source_family
            }) {
                targets
                    .entry(candidate.chain.clone())
                    .or_default()
                    .insert(candidate.address.clone());
            }
        }
    }

    targets
}

#[test]
fn indexed_authority_matches_full_scans_for_load_shaped_inputs() {
    let mut entries = std::collections::BTreeSet::new();
    for address_index in 1..=96 {
        entries.insert(entry(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            &format!("0x{address_index:040x}"),
            address_index <= 80,
        ));
        entries.insert(entry(
            "base-mainnet",
            "basenames_base_resolver",
            &format!("0x{:040x}", address_index + 0x100),
            address_index <= 80,
        ));
    }
    // One target may have multiple current authority entries; exact lookup
    // retains them while the family-address index still stores the address once.
    entries.insert(entry(
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        "0x0000000000000000000000000000000000000001",
        true,
    ));
    entries.insert(entry(
        "base-mainnet",
        "ens_v1_resolver_l1",
        "0x0000000000000000000000000000000000000201",
        false,
    ));
    let authority = ResolverProfileAuthoritySnapshot { entries };
    let authority_entry_count = authority.entries.len();
    let mut pending = Vec::new();
    for address_index in 1..=80 {
        pending.push(input(
            "ethereum-mainnet",
            &format!("0x{address_index:040x}"),
        ));
        pending.push(input(
            "base-mainnet",
            &format!("0x{:040x}", address_index + 0x100),
        ));
    }
    pending.extend((0..40).map(|index| {
        input(
            if index % 2 == 0 {
                "ethereum-mainnet"
            } else {
                "base-mainnet"
            },
            &format!("0x{:040x}", 0x1_000 + index),
        )
    }));
    pending.push(forced_input(
        "ethereum-mainnet",
        "0x000000000000000000000000000000000000ffff",
    ));
    assert!(pending.len() > 128);

    let expected = reference_expanded_reconciliation_targets(&pending, &authority);
    let index = authority_index(authority.clone());
    let (actual, expanded_seed_family_count) =
        expanded_reconciliation_targets_with_family_count(&pending, &index);

    assert_eq!(actual, expected);
    assert_eq!(index.indexed_entry_count, authority_entry_count);
    assert_eq!(
        index
            .entries_for(
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000001"
            )
            .map(<[_]>::len),
        Some(2)
    );
    assert_eq!(expanded_seed_family_count, 2);
    for candidate in &pending {
        let expected = candidate.force_reconciliation
            || authority.entries.iter().any(|entry| {
                entry.chain == candidate.chain_id && entry.address == candidate.contract_address
            });
        assert_eq!(input_requires_reconciliation(candidate, &index), expected);
    }
}

#[tokio::test]
async fn pending_input_drain_never_loads_the_full_authority_snapshot() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_scoped_authority_drain")
            .pool_max_connections(3),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for scoped resolver-profile drain test",
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resolver_profile_input_changes (
            chain_id,
            contract_address,
            previous_code_hash,
            current_code_hash
        ) VALUES (
            'ethereum-mainnet',
            '0x0000000000000000000000000000000000000099',
            NULL,
            '0x01'
        )
        "#,
    )
    .execute(database.pool())
    .await?;
    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.loaded_input_count, 1);
    assert_eq!(summary.authority_target_read_statement_count, 1);
    assert_eq!(summary.max_authority_target_read_batch_size, 1);
    assert_eq!(summary.family_target_read_statement_count, 0);
    assert_eq!(summary.reconciled_target_count, 0);
    assert_eq!(summary.acknowledged_input_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn convergence_rejects_a_pool_without_a_progress_heartbeat_connection() -> anyhow::Result<()>
{
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_three_connection_rejection")
            .pool_max_connections(3),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile pool rejection test",
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resolver_profile_input_changes (
            chain_id,
            contract_address,
            previous_code_hash,
            current_code_hash
        ) VALUES (
            'ethereum-mainnet',
            '0x0000000000000000000000000000000000000001',
            '0x01',
            '0x02'
        )
        "#,
    )
    .execute(database.pool())
    .await?;
    let instance_id = "resolver-profile-three-connection-rejection";
    install_stale_indexer_heartbeat(database.pool(), instance_id).await?;
    let (mut progress, _) = BlockingHeartbeatProgress::new(
        instance_id,
        vec!["ethereum-mainnet".to_owned()],
        usize::MAX,
    );

    let error = drain_resolver_profile_input_changes_with_progress(database.pool(), &mut progress)
        .await
        .expect_err("three connections cannot reserve a real convergence heartbeat writer");
    assert!(
        error
            .to_string()
            .contains("requires at least 4 database connections")
            && error.to_string().contains("progress heartbeat writes"),
        "unexpected convergence pool error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn seed_change_reconciles_journal_family_with_real_heartbeats() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_seed_family_pages")
            .pool_max_connections(4),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile seed-family paging test",
    )
    .await?;
    let mut entries = Vec::new();
    for address_number in 1..=2_501 {
        entries.push(
            bigname_storage::ResolverProfileAuthorityJournalEntry::from_payload(
                serde_json::to_value(entry(
                    "ethereum-mainnet",
                    "ens_v1_resolver_l1",
                    &format!("0x{address_number:040x}"),
                    address_number == 1,
                ))?,
            )?,
        );
    }
    let mut baseline =
        bigname_storage::begin_resolver_profile_authority_journal_advance(database.pool(), 0)
            .await?;
    baseline.stage_entries(&entries).await?;
    baseline.publish(&serde_json::json!({})).await?.unwrap();
    sqlx::query(
        r#"
        INSERT INTO resolver_profile_input_changes (
            chain_id,
            contract_address,
            previous_code_hash,
            current_code_hash
        ) VALUES (
            'ethereum-mainnet',
            '0x0000000000000000000000000000000000000001',
            '0x01',
            '0x02'
        )
        "#,
    )
    .execute(database.pool())
    .await?;

    let runtime_guard = bigname_storage::hold_base_normalized_rederive_runtime_shared_lock(
        database.pool(),
        "bigname-indexer",
    )
    .await?;
    let heartbeat_instance_id = "resolver-profile-seed-family-pages";
    install_stale_indexer_heartbeat(database.pool(), heartbeat_instance_id).await?;
    let (mut progress, progress_handle) = BlockingHeartbeatProgress::new(
        heartbeat_instance_id,
        vec!["ethereum-mainnet".to_owned()],
        2,
    );
    let mut operation = Box::pin(drain_resolver_profile_input_changes_with_progress(
        database.pool(),
        &mut progress,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            () = progress_handle.wait_until_blocked() => Ok(()),
            result = operation.as_mut() => Err(anyhow::anyhow!(
                "resolver-profile convergence completed before its later progress boundary blocked: {result:?}"
            )),
        }
    })
    .await
    .expect("resolver-profile convergence did not reach its later progress boundary")?;
    let persisted_heartbeat = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        heartbeat_instance_id,
    )
    .await?
    .expect("resolver-profile progress heartbeat must remain registered");
    assert!(
        persisted_heartbeat.age_seconds <= 1,
        "an earlier convergence page must beat before later target work finishes"
    );
    progress_handle.resume();
    let summary = tokio::time::timeout(std::time::Duration::from_secs(30), operation.as_mut())
        .await
        .expect("four-connection seed-family drain must not starve progress heartbeat writes")?;
    drop(operation);
    drop(runtime_guard);
    assert_eq!(summary.loaded_input_count, 1);
    assert_eq!(summary.authority_target_read_statement_count, 1);
    assert_eq!(summary.max_authority_target_read_batch_size, 1);
    assert_eq!(summary.family_target_read_statement_count, 12);
    assert_eq!(summary.max_family_target_page_size, 250);
    assert_eq!(
        summary.adapter_reconciliation_call_count, 1,
        "one chain-context reconciliation must consume every bounded target page"
    );
    assert_eq!(
        summary.invalidation_capture_pass_count, 1,
        "one streamed invalidation capture must consume every staged chain target"
    );
    assert_eq!(summary.reconciled_target_count, 2_501);
    assert_eq!(summary.invalidated_projection_key_count, 2_501);
    assert_eq!(summary.acknowledged_input_count, 1);
    assert!(
        progress_handle.record_count() >= 20,
        "the multi-page authority scan and reconciliation must report progress before completion"
    );

    database.cleanup().await
}

#[tokio::test]
async fn completed_reconciliation_crash_preserves_precaptured_invalidations() -> anyhow::Result<()>
{
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_crash_invalidation_staging"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile invalidation crash test",
    )
    .await?;
    let chain = "ethereum-mainnet";
    let resolver = "0x00000000000000000000000000000000000000aa".to_owned();

    let mut first =
        bigname_adapters::begin_resolver_profile_event_reconciliation(database.pool(), chain)
            .await?;
    first
        .stage_addresses(std::slice::from_ref(&resolver))
        .await?;
    super::invalidations::stage_resolver_profile_projection_invalidations(
        database.pool(),
        first.run_id(),
        chain,
    )
    .await?;
    let abandoned_publication = first.reconcile().await?;
    drop(abandoned_publication);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT \
             FROM resolver_profile_reconciliation_invalidation_keys"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let mut retry =
        bigname_adapters::begin_resolver_profile_event_reconciliation(database.pool(), chain)
            .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT \
             FROM resolver_profile_reconciliation_invalidation_keys"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "starting a retry must preserve the prior pre-repair invalidation keys"
    );
    retry
        .stage_addresses(std::slice::from_ref(&resolver))
        .await?;
    retry.reconcile().await?.finish().await?;
    sqlx::query("DELETE FROM resolver_profile_reconciliation_invalidation_keys")
        .execute(database.pool())
        .await?;

    database.cleanup().await
}

#[tokio::test]
async fn same_chain_reconciliation_lock_covers_invalidation_publication() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_invalidation_serialization"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile invalidation serialization test",
    )
    .await?;
    let chain = "ethereum-mainnet";
    let resolver = "0x00000000000000000000000000000000000000aa".to_owned();

    let mut reconciliation =
        bigname_adapters::begin_resolver_profile_event_reconciliation(database.pool(), chain)
            .await?;
    reconciliation
        .stage_addresses(std::slice::from_ref(&resolver))
        .await?;
    super::invalidations::stage_resolver_profile_projection_invalidations(
        database.pool(),
        reconciliation.run_id(),
        chain,
    )
    .await?;
    let mut publication = reconciliation.reconcile().await?;

    let same_chain_lock_was_available =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("resolver_profile_reconciliation:{chain}"))
            .fetch_one(database.pool())
            .await?;

    super::invalidations::publish_resolver_profile_projection_invalidations(
        publication.connection_mut(),
        chain,
    )
    .await?;
    let visible_invalidation_count_before_finish = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM projection_invalidations \
         WHERE projection = 'resolver_current' AND claim_token IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    publication.finish().await?;
    let visible_invalidation_count_after_finish = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM projection_invalidations \
         WHERE projection = 'resolver_current' AND claim_token IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    database.cleanup().await?;

    assert!(
        !same_chain_lock_was_available,
        "same-chain reconciliation must remain serialized until its staged invalidations publish"
    );
    assert_eq!(
        visible_invalidation_count_before_finish, 0,
        "a projection worker must not see invalidations before event repair commits"
    );
    assert_eq!(visible_invalidation_count_after_finish, 1);
    Ok(())
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

    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.deferred_input_count, 1);
    assert_eq!(summary.acknowledged_input_count, 0);
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

    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.deferred_input_count, 1);
    assert_eq!(summary.acknowledged_input_count, 0);
    assert_resolver_profile_generation_pending(database.pool(), chain, resolver).await?;

    database.cleanup().await
}

#[tokio::test]
async fn unavailable_chain_does_not_block_eligible_chain_convergence() -> anyhow::Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_resolver_profile_chain_deferral"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for resolver-profile chain-deferral test",
    )
    .await?;
    let deferred_chain = "ethereum-mainnet";
    let eligible_chain = "base-mainnet";
    let deferred_resolver = "0x0000000000000000000000000000000000000088";
    let eligible_resolver = "0x0000000000000000000000000000000000000099";
    seed_resolver_raw_logs(
        database.pool(),
        deferred_chain,
        deferred_resolver,
        &[(1, "0xresolver-profile-deferred-chain-block")],
    )
    .await?;
    sqlx::query("DELETE FROM raw_logs WHERE chain_id = $1")
        .bind(deferred_chain)
        .execute(database.pool())
        .await?;
    bigname_storage::enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[
            bigname_storage::ResolverProfileReconciliationTarget {
                chain_id: deferred_chain.to_owned(),
                contract_address: deferred_resolver.to_owned(),
            },
            bigname_storage::ResolverProfileReconciliationTarget {
                chain_id: eligible_chain.to_owned(),
                contract_address: eligible_resolver.to_owned(),
            },
        ],
    )
    .await?;

    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.deferred_input_count, 1);
    assert_eq!(summary.acknowledged_input_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT generation, processed_generation
            FROM resolver_profile_input_changes
            WHERE chain_id = $1 AND contract_address = $2
            "#,
        )
        .bind(deferred_chain)
        .bind(deferred_resolver)
        .fetch_one(database.pool())
        .await?,
        (1, 0)
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT generation, processed_generation
            FROM resolver_profile_input_changes
            WHERE chain_id = $1 AND contract_address = $2
            "#,
        )
        .bind(eligible_chain)
        .bind(eligible_resolver)
        .fetch_one(database.pool())
        .await?,
        (1, 1)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM projection_invalidations
            WHERE projection = 'resolver_current'
              AND projection_key = $1
            "#,
        )
        .bind(format!("{eligible_chain}:{eligible_resolver}"))
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn staged_invalidations_include_readable_history_but_exclude_orphaned_fork_rows()
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

    bigname_storage::enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[bigname_storage::ResolverProfileReconciliationTarget {
            chain_id: "ethereum-mainnet".to_owned(),
            contract_address: resolver.to_owned(),
        }],
    )
    .await?;
    let summary = drain_resolver_profile_input_changes(database.pool()).await?;
    assert_eq!(summary.reconciled_target_count, 1);
    assert_eq!(summary.acknowledged_input_count, 1);
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT \
             FROM resolver_profile_reconciliation_invalidation_keys"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "successful reconciliation must cascade-delete staged invalidation keys"
    );

    database.cleanup().await
}
