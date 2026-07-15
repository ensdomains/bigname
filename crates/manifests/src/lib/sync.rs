#[path = "sync/address_seeding.rs"]
mod address_seeding;
#[path = "sync/contract_resolution.rs"]
mod contract_resolution;
#[path = "sync/persistence.rs"]
mod persistence;
#[path = "sync/planning.rs"]
mod planning;

use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) use self::{
    address_seeding::ensure_contract_instance_address_seed,
    contract_resolution::resolve_contract_instance_by_address,
};
use self::{
    address_seeding::seed_planned_manifest_entry_addresses,
    persistence::{
        delete_stale_manifest_version, load_existing_manifest_entries,
        load_existing_manifest_versions, upsert_manifest_version,
    },
    planning::{declared_start_block_for_entry, plan_manifest_entries},
};
use crate::discovery::{bump_discovery_admission_epochs, fence_discovery_admission_epoch_writes};
use crate::{
    ManifestLoadStatus, ManifestRepository, ManifestSyncStatus, ManifestSyncSummary,
    managed_edges::{reconcile_manifest_source_graph, replace_manifest_children},
    support::{ManifestStorageKey, ManifestTransition},
};

pub async fn sync_repository(
    pool: &PgPool,
    repository: &ManifestRepository,
) -> Result<ManifestSyncSummary> {
    match repository.summary().status {
        ManifestLoadStatus::MissingRoot => {
            return Ok(ManifestSyncSummary::skipped(
                ManifestSyncStatus::SkippedMissingRoot,
            ));
        }
        ManifestLoadStatus::InvalidRoot => {
            return Ok(ManifestSyncSummary::skipped(
                ManifestSyncStatus::SkippedInvalidRoot,
            ));
        }
        ManifestLoadStatus::Loaded | ManifestLoadStatus::Empty => {}
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to start manifest sync transaction")?;
    let existing_manifests = load_existing_manifest_versions(transaction.as_mut()).await?;
    let admission_fence_chains = repository
        .manifests()
        .iter()
        .map(|loaded_manifest| loaded_manifest.manifest.chain.clone())
        .chain(
            existing_manifests
                .iter()
                .map(|manifest| manifest.storage_key.chain.clone()),
        )
        .collect::<BTreeSet<_>>();
    fence_discovery_admission_epoch_writes(transaction.as_mut(), &admission_fence_chains).await?;

    let mut retained_keys = HashSet::new();
    let mut in_place_transitions = Vec::new();
    let mut mutated_chains = BTreeSet::new();
    let mut active_declared_start_blocks = HashMap::<(String, Uuid), (i64, String, String)>::new();
    let mut sync_summary = ManifestSyncSummary {
        status: ManifestSyncStatus::Synced,
        synced_manifest_count: repository.manifests().len(),
        active_manifest_count: repository
            .manifests()
            .iter()
            .filter(|loaded_manifest| loaded_manifest.manifest.rollout_status.is_active())
            .count(),
        root_count: 0,
        contract_count: 0,
        capability_count: 0,
        discovery_rule_count: 0,
        removed_manifest_count: 0,
        cleared_discovery_edge_count: 0,
    };

    for loaded_manifest in repository.manifests() {
        let storage_key = ManifestStorageKey::from_loaded_manifest(loaded_manifest)?;
        let manifest_authority_changed = match existing_manifests
            .iter()
            .find(|existing_manifest| existing_manifest.storage_key == storage_key)
        {
            Some(existing_manifest) => !existing_manifest.authority_matches(loaded_manifest)?,
            None => true,
        };
        retained_keys.insert(storage_key);

        let manifest_id = upsert_manifest_version(transaction.as_mut(), loaded_manifest).await?;
        let existing_entries =
            load_existing_manifest_entries(transaction.as_mut(), manifest_id).await?;
        let planned_entries = plan_manifest_entries(
            transaction.as_mut(),
            manifest_id,
            loaded_manifest,
            &existing_entries,
        )
        .await?;

        if loaded_manifest.manifest.rollout_status.is_active() {
            for planned_entry in &planned_entries {
                if let Some(start_block) =
                    declared_start_block_for_entry(loaded_manifest, &planned_entry.key)?
                {
                    let active_key = (
                        loaded_manifest.manifest.source_family.clone(),
                        planned_entry.contract_instance_id,
                    );
                    if let Some((
                        existing_start_block,
                        existing_declaration_kind,
                        existing_declaration_name,
                    )) = active_declared_start_blocks.get(&active_key)
                    {
                        if *existing_start_block != start_block {
                            bail!(
                                "conflicting start_block declarations for active source_family {} contract_instance_id {}: {} {} starts at {}, {} {} starts at {}",
                                loaded_manifest.manifest.source_family,
                                planned_entry.contract_instance_id,
                                existing_declaration_kind,
                                existing_declaration_name,
                                existing_start_block,
                                planned_entry.key.declaration_kind,
                                planned_entry.key.declaration_name,
                                start_block
                            );
                        }
                    } else {
                        active_declared_start_blocks.insert(
                            active_key,
                            (
                                start_block,
                                planned_entry.key.declaration_kind.clone(),
                                planned_entry.key.declaration_name.clone(),
                            ),
                        );
                    }
                }

                if let Some(existing_entry) = existing_entries.get(&planned_entry.key)
                    && existing_entry.contract_instance_id != planned_entry.contract_instance_id
                {
                    in_place_transitions.push(ManifestTransition {
                        source_manifest_id: manifest_id,
                        chain: loaded_manifest.manifest.chain.clone(),
                        declaration_kind: planned_entry.key.declaration_kind.clone(),
                        declaration_name: planned_entry.key.declaration_name.clone(),
                        from_contract_instance_id: existing_entry.contract_instance_id,
                        from_address: existing_entry.declared_address.clone(),
                        to_contract_instance_id: planned_entry.contract_instance_id,
                        to_address: planned_entry.declared_address.clone(),
                    });
                }
            }
        }

        let children_changed = replace_manifest_children(
            transaction.as_mut(),
            manifest_id,
            &loaded_manifest.manifest,
            &existing_entries,
            &planned_entries,
        )
        .await?;
        let address_seeded = seed_planned_manifest_entry_addresses(
            transaction.as_mut(),
            manifest_id,
            loaded_manifest,
            &planned_entries,
        )
        .await?;
        if manifest_authority_changed || children_changed || address_seeded {
            mutated_chains.insert(loaded_manifest.manifest.chain.clone());
        }

        sync_summary.root_count += loaded_manifest.manifest.roots.len();
        sync_summary.contract_count += loaded_manifest.manifest.contracts.len();
        sync_summary.capability_count += loaded_manifest.manifest.capability_flags.len();
        sync_summary.discovery_rule_count += loaded_manifest.manifest.discovery_rules.len();
    }

    for existing_manifest in existing_manifests {
        if retained_keys.contains(&existing_manifest.storage_key) {
            continue;
        }

        delete_stale_manifest_version(transaction.as_mut(), existing_manifest.manifest_id).await?;
        mutated_chains.insert(existing_manifest.storage_key.chain.clone());
        sync_summary.removed_manifest_count += 1;
    }

    let (cleared_discovery_edge_count, mutated_address_chains) =
        reconcile_manifest_source_graph(transaction.as_mut(), &in_place_transitions).await?;
    sync_summary.cleared_discovery_edge_count = cleared_discovery_edge_count;
    mutated_chains.extend(mutated_address_chains);

    // The manifest-declared arm of the watched surface can change without a
    // discovery-edge mutation. Invalidate retained coverage only for chains
    // whose stored manifest authority, child rows, address seeds, active
    // ranges, or removals actually changed; a byte-identical startup refresh
    // must preserve the retained-history proof tuple.
    bump_discovery_admission_epochs(transaction.as_mut(), &mutated_chains).await?;

    transaction
        .commit()
        .await
        .context("failed to commit manifest sync transaction")?;

    Ok(sync_summary)
}
