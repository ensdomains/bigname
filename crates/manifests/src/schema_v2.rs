use std::collections::{HashMap, HashSet};

use alloy_primitives::keccak256;
use anyhow::{Context, Result, bail};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::{LoadedManifest, ManifestLoadStatus, ManifestRepository};

#[path = "schema_v2_persistence.rs"]
mod persistence;
#[path = "schema_v2_sync_state.rs"]
mod sync_state;

use persistence::{
    deactivate_retired_manifest_addresses, insert_declaration, normalize_address,
    reopen_proxy_edge, resolve_contract, validate_proxy_shape,
};

const SCHEMA_V2_MANIFEST_SYNC_LOCK: i64 = 0x4249_474e_414d_4532;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ManifestKey {
    namespace: String,
    source_family: String,
    chain_id: String,
    deployment_label: String,
    manifest_version: i64,
}

#[derive(Clone, Debug)]
struct StoredManifestState {
    manifest_id: i64,
    key: ManifestKey,
    rollout_status: String,
    normalizer_version: String,
    manifest_payload: serde_json::Value,
}

impl StoredManifestState {
    fn event_state(&self) -> serde_json::Value {
        json!({
            "manifest_version": self.key.manifest_version,
            "normalizer_version": self.normalizer_version,
            "rollout_status": self.rollout_status,
            "manifest_payload": self.manifest_payload,
        })
    }

    fn authority_matches(&self, other: &Self) -> bool {
        self.rollout_status == other.rollout_status
            && self.normalizer_version == other.normalizer_version
            && self.manifest_payload == other.manifest_payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaV2ManifestSyncSummary {
    pub manifest_count: usize,
    pub declaration_count: usize,
    pub discovery_rule_count: usize,
    pub proxy_edge_count: usize,
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
    sync_state::lock_phase_writers(&mut transaction, repository).await?;
    let previous_authority = sync_state::active_authority(&mut transaction).await?;
    let desired_authority = sync_state::repository_authority(repository)?;
    let existing = load_manifest_states(&mut transaction).await?;

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
    for loaded in repository.manifests() {
        let file_path = loaded.relative_path.to_string_lossy().into_owned();
        let manifest_id = upsert_manifest(&mut transaction, loaded, &file_path).await?;
        let state = manifest_state(manifest_id, loaded)?;
        desired_keys.insert(state.key.clone());
        if existing
            .get(&state.key)
            .is_none_or(|before| !before.authority_matches(&state))
        {
            write_manifest_event(&mut transaction, existing.get(&state.key), &state).await?;
        }
        let counts = replace_manifest_children(&mut transaction, manifest_id, loaded).await?;
        declaration_count += counts.0;
        discovery_rule_count += counts.1;
        proxy_edge_count += counts.2;
    }
    for before in existing
        .values()
        .filter(|state| state.rollout_status == "active" && !desired_keys.contains(&state.key))
    {
        let mut after = before.clone();
        after.rollout_status = "deprecated".to_owned();
        write_manifest_event(&mut transaction, Some(before), &after).await?;
    }
    deactivate_retired_manifest_addresses(&mut transaction).await?;
    sync_state::invalidate_changed_derived_epochs(
        &mut transaction,
        &previous_authority,
        &desired_authority,
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
    })
}

async fn load_manifest_states(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<HashMap<ManifestKey, StoredManifestState>> {
    let rows = sqlx::query(
        "
        SELECT manifest_id, namespace, source_family, chain_id, deployment_label,
               manifest_version, rollout_status, normalizer_version, manifest_payload
        FROM manifest_versions
        ",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load existing schema-v2 manifest states")?;
    rows.into_iter()
        .map(|row| {
            let key = ManifestKey {
                namespace: row.try_get("namespace")?,
                source_family: row.try_get("source_family")?,
                chain_id: row.try_get("chain_id")?,
                deployment_label: row.try_get("deployment_label")?,
                manifest_version: row.try_get("manifest_version")?,
            };
            let state = StoredManifestState {
                manifest_id: row.try_get("manifest_id")?,
                key: key.clone(),
                rollout_status: row.try_get("rollout_status")?,
                normalizer_version: row.try_get("normalizer_version")?,
                manifest_payload: row.try_get("manifest_payload")?,
            };
            Ok((key, state))
        })
        .collect::<std::result::Result<_, sqlx::Error>>()
        .context("failed to decode existing schema-v2 manifest states")
}

fn manifest_state(manifest_id: i64, loaded: &LoadedManifest) -> Result<StoredManifestState> {
    let manifest = &loaded.manifest;
    let manifest_version = i64::try_from(manifest.manifest_version).with_context(|| {
        format!(
            "manifest version {} in {} exceeds BIGINT",
            manifest.manifest_version,
            loaded.path.display()
        )
    })?;
    Ok(StoredManifestState {
        manifest_id,
        key: ManifestKey {
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            chain_id: manifest.chain.clone(),
            deployment_label: manifest.deployment_epoch.clone(),
            manifest_version,
        },
        rollout_status: manifest.rollout_status.as_db_value().to_owned(),
        normalizer_version: manifest.normalizer_version.clone(),
        manifest_payload: serde_json::to_value(manifest)
            .with_context(|| format!("failed to serialize {}", loaded.path.display()))?,
    })
}

async fn write_manifest_event(
    transaction: &mut Transaction<'_, Postgres>,
    before: Option<&StoredManifestState>,
    after: &StoredManifestState,
) -> Result<()> {
    let before_state = before.map_or_else(|| json!({}), StoredManifestState::event_state);
    let after_state = after.event_state();
    let raw_fact_ref = json!({
        "manifest_id": after.manifest_id,
        "namespace": after.key.namespace,
        "source_family": after.key.source_family,
        "chain": after.key.chain_id,
        "deployment_epoch": after.key.deployment_label,
    });
    let identity_material = json!({
        "manifest_id": after.manifest_id,
        "before_state": &before_state,
        "after_state": &after_state,
    });
    let identity_bytes = serde_json::to_vec(&identity_material)
        .context("failed to serialize SourceManifestUpdated identity")?;
    let event_identity = format!(
        "manifest_sync:source_manifest_updated:{:#x}",
        keccak256(identity_bytes)
    );
    let inserted = sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, event_kind, source_family, manifest_version,
            source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        VALUES (
            $1, $2, 'SourceManifestUpdated', $3, $4, $5, $6, $7,
            'manifest_sync', 'finalized', $8, $9
        )
        ON CONFLICT (event_identity) DO NOTHING
        ",
    )
    .bind(&event_identity)
    .bind(&after.key.namespace)
    .bind(&after.key.source_family)
    .bind(after.key.manifest_version)
    .bind(after.manifest_id)
    .bind(&after.key.chain_id)
    .bind(&raw_fact_ref)
    .bind(&before_state)
    .bind(&after_state)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to write SourceManifestUpdated for schema-v2 manifest {}",
            after.manifest_id
        )
    })?
    .rows_affected();
    if inserted == 0 {
        let compatible: Option<bool> = sqlx::query_scalar(
            "
            SELECT namespace = $2
               AND event_kind = 'SourceManifestUpdated'
               AND source_family = $3
               AND manifest_version = $4
               AND source_manifest_id = $5
               AND chain_id = $6
               AND raw_fact_ref = $7
               AND derivation_kind = 'manifest_sync'
               AND canonicality_state = 'finalized'
               AND before_state = $8
               AND after_state = $9
            FROM normalized_events
            WHERE event_identity = $1
            ",
        )
        .bind(&event_identity)
        .bind(&after.key.namespace)
        .bind(&after.key.source_family)
        .bind(after.key.manifest_version)
        .bind(after.manifest_id)
        .bind(&after.key.chain_id)
        .bind(&raw_fact_ref)
        .bind(&before_state)
        .bind(&after_state)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to verify idempotent SourceManifestUpdated event")?;
        if compatible != Some(true) {
            bail!("SourceManifestUpdated event identity collision for {event_identity}");
        }
    }
    Ok(())
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
    let payload = serde_json::to_value(manifest)
        .with_context(|| format!("failed to serialize {}", loaded.path.display()))?;
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
