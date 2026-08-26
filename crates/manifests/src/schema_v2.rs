use std::collections::{BTreeSet, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::{LoadedManifest, ManifestLoadStatus, ManifestRepository};

#[path = "schema_v2_event_history.rs"]
mod event_history;
#[path = "schema_v2_persistence.rs"]
mod persistence;
#[path = "schema_v2_retirement.rs"]
mod retirement;
#[path = "schema_v2_sync_state.rs"]
mod sync_state;
#[path = "schema_v2_watch.rs"]
mod watch;
#[path = "schema_v2_watch_floors.rs"]
mod watch_floors;
#[path = "schema_v2_watch_widening.rs"]
mod watch_widening;

use event_history::{load_manifest_states, manifest_state, write_manifest_event};
use persistence::{
    deactivate_retired_manifest_addresses, insert_declaration, normalize_address,
    reopen_proxy_edge, repair_retired_omitted_admission_floors, resolve_contract,
    validate_proxy_shape,
};
use retirement::active_manifest_address_declarations;

const SCHEMA_V2_MANIFEST_SYNC_LOCK: i64 = 0x4249_474e_414d_4532;

#[derive(Clone)]
struct ManifestAddressDeclaration {
    chain_id: String,
    instance_id: Uuid,
    address: String,
    manifest_id: i64,
    provenance: serde_json::Value,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaV2ManifestSyncSummary {
    pub manifest_count: usize,
    pub declaration_count: usize,
    pub discovery_rule_count: usize,
    pub proxy_edge_count: usize,
    pub notices: Vec<String>,
}

pub async fn sync_schema_v2_repository(
    pool: &PgPool,
    repository: &ManifestRepository,
) -> Result<SchemaV2ManifestSyncSummary> {
    match repository.summary().status {
        ManifestLoadStatus::Loaded => {}
        status => bail!(
            "schema-v2 manifest sync requires a loaded non-empty repository, got {} at {}",
            status.as_str(),
            repository.root().display()
        ),
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to start schema-v2 manifest sync transaction")?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SCHEMA_V2_MANIFEST_SYNC_LOCK)
        .execute(&mut *transaction)
        .await
        .context("failed to take schema-v2 manifest sync advisory lock")?;
    let mut omitted_admission_addresses = BTreeSet::new();
    for loaded in repository
        .manifests()
        .iter()
        .filter(|loaded| loaded.manifest.rollout_status.is_active())
    {
        let manifest = &loaded.manifest;
        omitted_admission_addresses.extend(
            manifest
                .roots
                .iter()
                .filter(|root| root.start_block.is_none())
                .map(|root| (manifest.chain.clone(), normalize_address(&root.address))),
        );
        for contract in manifest
            .contracts
            .iter()
            .filter(|contract| contract.start_block.is_none())
        {
            omitted_admission_addresses
                .insert((manifest.chain.clone(), normalize_address(&contract.address)));
            if let Some(implementation) = contract.implementation.as_deref() {
                omitted_admission_addresses
                    .insert((manifest.chain.clone(), normalize_address(implementation)));
            }
        }
    }
    sync_state::lock_phase_writers(&mut transaction, repository).await?;
    let previous_authority = sync_state::active_authority(&mut transaction).await?;
    let previous_admission_floors = sync_state::active_admission_floors(&mut transaction).await?;
    let desired_authority = sync_state::repository_authority(repository)?;
    let existing = load_manifest_states(&mut transaction).await?;
    let previous_declarations = active_manifest_address_declarations(&mut transaction).await?;

    sqlx::query(
        "UPDATE manifest_versions SET rollout_status = 'deprecated' WHERE rollout_status = 'active'",
    )
    .execute(&mut *transaction)
    .await
    .context("failed to stage active schema-v2 manifests for replacement")?;

    let mut declaration_count = 0usize;
    let mut discovery_rule_count = 0usize;
    let mut proxy_edge_count = 0usize;
    let mut desired_keys = HashSet::new();
    let mut repaired_floor_chains = HashSet::new();
    let mut notices = Vec::new();
    for loaded in repository.manifests() {
        let file_path = loaded.relative_path.to_string_lossy().into_owned();
        let manifest_id = upsert_manifest(&mut transaction, loaded, &file_path).await?;
        let state = manifest_state(manifest_id, loaded)?;
        desired_keys.insert(state.key.clone());
        match existing.get(&state.key) {
            None => write_manifest_event(&mut transaction, json!({}), &state).await?,
            Some(before) if !before.authority_matches(&state) => {
                write_manifest_event(&mut transaction, before.event_state(), &state).await?
            }
            Some(before) if !before.history_matches() => {
                write_manifest_event(
                    &mut transaction,
                    before.latest_event_state_or_empty(),
                    &state,
                )
                .await?
            }
            Some(_) => {}
        }
        let counts = replace_manifest_children(
            &mut transaction,
            manifest_id,
            loaded,
            &mut repaired_floor_chains,
            &mut notices,
        )
        .await?;
        declaration_count += counts.0;
        discovery_rule_count += counts.1;
        proxy_edge_count += counts.2;
    }
    for before in existing.values().filter(|state| {
        !desired_keys.contains(&state.key)
            && (state.rollout_status == "active" || !state.history_matches())
    }) {
        let mut after = before.clone();
        let before_state = if before.rollout_status == "active" {
            after.rollout_status = "deprecated".to_owned();
            before.event_state()
        } else {
            before.latest_event_state_or_empty()
        };
        write_manifest_event(&mut transaction, before_state, &after).await?;
    }
    deactivate_retired_manifest_addresses(&mut transaction, &previous_declarations).await?;
    repair_retired_omitted_admission_floors(
        &mut transaction,
        &omitted_admission_addresses,
        &mut repaired_floor_chains,
    )
    .await?;
    sync_state::invalidate_changed_derived_epochs(
        &mut transaction,
        &previous_authority,
        &desired_authority,
        &previous_admission_floors,
        &repaired_floor_chains,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit schema-v2 manifest sync")?;
    Ok(SchemaV2ManifestSyncSummary {
        manifest_count: repository.manifests().len(),
        declaration_count,
        discovery_rule_count,
        proxy_edge_count,
        notices,
    })
}

async fn upsert_manifest(
    transaction: &mut Transaction<'_, Postgres>,
    loaded: &LoadedManifest,
    file_path: &str,
) -> Result<i64> {
    let manifest = &loaded.manifest;
    let manifest_version = i64::try_from(manifest.manifest_version).with_context(|| {
        format!(
            "manifest version {} in {} exceeds BIGINT",
            manifest.manifest_version,
            loaded.path.display()
        )
    })?;
    let payload = watch::manifest_payload(manifest)
        .with_context(|| format!("failed to compile {}", loaded.path.display()))?;
    sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version,
            file_path, manifest_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (
            namespace, source_family, chain_id, deployment_label, manifest_version
        ) DO UPDATE
        SET rollout_status = EXCLUDED.rollout_status,
            normalizer_version = EXCLUDED.normalizer_version,
            file_path = EXCLUDED.file_path,
            manifest_payload = EXCLUDED.manifest_payload,
            loaded_at = now()
        RETURNING manifest_id
        ",
    )
    .bind(manifest_version)
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(&manifest.chain)
    .bind(&manifest.deployment_epoch)
    .bind(manifest.rollout_status.as_db_value())
    .bind(&manifest.normalizer_version)
    .bind(file_path)
    .bind(payload)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| format!("failed to upsert schema-v2 manifest {file_path}"))
}

async fn replace_manifest_children(
    transaction: &mut Transaction<'_, Postgres>,
    manifest_id: i64,
    loaded: &LoadedManifest,
    repaired_floor_chains: &mut HashSet<String>,
    notices: &mut Vec<String>,
) -> Result<(usize, usize, usize)> {
    sqlx::query("DELETE FROM manifest_contract_instances WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to replace declarations for manifest {manifest_id}"))?;
    sqlx::query("DELETE FROM manifest_discovery_rules WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to replace discovery rules for manifest {manifest_id}"))?;
    sqlx::query(
        "
        UPDATE discovery_edges
        SET deactivated_at = COALESCE(deactivated_at, now())
        WHERE source_manifest_id = $1
          AND discovery_source = 'manifest_declared_proxy'
        ",
    )
    .bind(manifest_id)
    .execute(&mut **transaction)
    .await
    .with_context(|| format!("failed to stage proxy edges for manifest {manifest_id}"))?;

    let manifest = &loaded.manifest;
    let admit_addresses = manifest.rollout_status.is_active();
    let mut declaration_count = 0usize;
    for root in &manifest.roots {
        let address = normalize_address(&root.address);
        let instance = resolve_contract(
            transaction,
            &manifest.chain,
            &address,
            "root",
            manifest_id,
            root.start_block,
            admit_addresses,
            json!({
                "source": "manifest_declaration",
                "manifest_id": manifest_id,
                "declaration_kind": "root",
                "declaration_name": root.name,
                "declared_address": address,
            }),
            repaired_floor_chains,
            notices,
        )
        .await?;
        insert_declaration(
            transaction,
            manifest_id,
            &manifest.chain,
            "root",
            &root.name,
            instance,
            &address,
            root.abi_ref.as_deref(),
            None,
            "none",
            None,
            None,
            root.start_block,
        )
        .await?;
        declaration_count += 1;
    }

    let mut proxy_edge_count = 0usize;
    for contract in &manifest.contracts {
        validate_proxy_shape(loaded, contract)?;
        let address = normalize_address(&contract.address);
        let instance = resolve_contract(
            transaction,
            &manifest.chain,
            &address,
            "contract",
            manifest_id,
            contract.start_block,
            admit_addresses,
            json!({
                "source": "manifest_declaration",
                "manifest_id": manifest_id,
                "declaration_kind": "contract",
                "declaration_name": contract.role,
                "declared_address": address,
            }),
            repaired_floor_chains,
            notices,
        )
        .await?;
        let implementation = if let Some(implementation) = contract.implementation.as_deref() {
            let implementation = normalize_address(implementation);
            let implementation_id = resolve_contract(
                transaction,
                &manifest.chain,
                &implementation,
                "contract",
                manifest_id,
                contract.start_block,
                admit_addresses,
                json!({
                    "source": "manifest_proxy_implementation",
                    "manifest_id": manifest_id,
                    "proxy_role": contract.role,
                    "proxy_address": address,
                    "declared_address": implementation,
                }),
                repaired_floor_chains,
                notices,
            )
            .await?;
            Some((implementation_id, implementation))
        } else {
            None
        };
        insert_declaration(
            transaction,
            manifest_id,
            &manifest.chain,
            "contract",
            &contract.role,
            instance,
            &address,
            None,
            Some(&contract.role),
            &contract.proxy_kind,
            implementation.as_ref().map(|value| value.0),
            implementation.as_ref().map(|value| value.1.as_str()),
            contract.start_block,
        )
        .await?;
        if admit_addresses && let Some((implementation_id, implementation_address)) = implementation
        {
            reopen_proxy_edge(
                transaction,
                manifest_id,
                &manifest.chain,
                &contract.role,
                instance,
                implementation_id,
                &address,
                &implementation_address,
            )
            .await?;
            proxy_edge_count += 1;
        }
        declaration_count += 1;
    }

    for rule in &manifest.discovery_rules {
        sqlx::query(
            "
            INSERT INTO manifest_discovery_rules (
                manifest_id, edge_kind, from_role, admission, rule_payload
            )
            VALUES ($1, $2, $3, $4, $5)
            ",
        )
        .bind(manifest_id)
        .bind(&rule.edge_kind)
        .bind(&rule.from_role)
        .bind(&rule.admission)
        .bind(serde_json::to_value(rule).context("failed to serialize discovery rule")?)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to insert discovery rule for manifest {manifest_id}"))?;
    }

    Ok((
        declaration_count,
        manifest.discovery_rules.len(),
        proxy_edge_count,
    ))
}
