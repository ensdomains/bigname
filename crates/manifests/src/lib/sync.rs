use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    CONTRACT_KIND_CONTRACT, CONTRACT_KIND_ROOT, DECLARATION_KIND_CONTRACT, DECLARATION_KIND_ROOT,
    LoadedManifest, ManifestLoadStatus, ManifestRepository, ManifestSyncStatus,
    ManifestSyncSummary,
    managed_edges::{reconcile_manifest_source_graph, replace_manifest_children},
    normalize_address,
    support::{DeclarationKey, ManifestStorageKey, ManifestTransition, PersistedManifestEntry},
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

    let existing_manifests = sqlx::query(
        r#"
        SELECT manifest_id, namespace, source_family, chain, deployment_epoch, manifest_version
        FROM manifest_versions
        "#,
    )
    .fetch_all(transaction.as_mut())
    .await
    .context("failed to load existing manifest versions")?;

    let mut retained_keys = HashSet::new();
    let mut in_place_transitions = Vec::new();
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

        replace_manifest_children(
            transaction.as_mut(),
            manifest_id,
            &loaded_manifest.manifest,
            &planned_entries,
        )
        .await?;
        seed_planned_manifest_entry_addresses(
            transaction.as_mut(),
            manifest_id,
            loaded_manifest,
            &planned_entries,
        )
        .await?;

        sync_summary.root_count += loaded_manifest.manifest.roots.len();
        sync_summary.contract_count += loaded_manifest.manifest.contracts.len();
        sync_summary.capability_count += loaded_manifest.manifest.capability_flags.len();
        sync_summary.discovery_rule_count += loaded_manifest.manifest.discovery_rules.len();
    }

    for existing_manifest in existing_manifests {
        let manifest_id = existing_manifest
            .try_get::<i64, _>("manifest_id")
            .context("failed to read existing manifest_id")?;
        let manifest_version = existing_manifest
            .try_get::<i64, _>("manifest_version")
            .context("failed to read existing manifest_version")?;
        let storage_key = ManifestStorageKey {
            namespace: existing_manifest
                .try_get("namespace")
                .context("failed to read existing namespace")?,
            source_family: existing_manifest
                .try_get("source_family")
                .context("failed to read existing source_family")?,
            chain: existing_manifest
                .try_get("chain")
                .context("failed to read existing chain")?,
            deployment_epoch: existing_manifest
                .try_get("deployment_epoch")
                .context("failed to read existing deployment_epoch")?,
            manifest_version,
        };

        if retained_keys.contains(&storage_key) {
            continue;
        }

        sqlx::query("DELETE FROM manifest_versions WHERE manifest_id = $1")
            .bind(manifest_id)
            .execute(transaction.as_mut())
            .await
            .with_context(|| format!("failed to delete stale manifest_id {manifest_id}"))?;
        sync_summary.removed_manifest_count += 1;
    }

    sync_summary.cleared_discovery_edge_count =
        reconcile_manifest_source_graph(transaction.as_mut(), &in_place_transitions).await?;

    transaction
        .commit()
        .await
        .context("failed to commit manifest sync transaction")?;

    Ok(sync_summary)
}

fn declared_start_block_for_entry(
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
) -> Result<Option<i64>> {
    let start_block = match key.declaration_kind.as_str() {
        DECLARATION_KIND_ROOT => loaded_manifest
            .manifest
            .roots
            .iter()
            .find(|root| root.name == key.declaration_name)
            .and_then(|root| root.start_block),
        DECLARATION_KIND_CONTRACT => loaded_manifest
            .manifest
            .contracts
            .iter()
            .find(|contract| contract.role == key.declaration_name)
            .and_then(|contract| contract.start_block),
        _ => None,
    };

    start_block
        .map(|start_block| {
            i64::try_from(start_block).with_context(|| {
                format!(
                    "start_block {start_block} for {} {} in {} does not fit into BIGINT",
                    key.declaration_kind,
                    key.declaration_name,
                    loaded_manifest.path.display()
                )
            })
        })
        .transpose()
}

async fn seed_planned_manifest_entry_addresses(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    planned_entries: &[PersistedManifestEntry],
) -> Result<()> {
    for entry in planned_entries {
        ensure_contract_instance_address_seed(
            executor,
            entry.contract_instance_id,
            &loaded_manifest.manifest.chain,
            &entry.declared_address,
            Some(manifest_id),
            &serde_json::json!({
                "source": "manifest_declaration_seed",
                "manifest_id": manifest_id,
                "declaration_kind": entry.key.declaration_kind,
                "declaration_name": entry.key.declaration_name,
                "source_family": loaded_manifest.manifest.source_family,
            }),
        )
        .await?;

        if let (Some(implementation_contract_instance_id), Some(implementation_address)) = (
            entry.implementation_contract_instance_id,
            entry.declared_implementation_address.as_deref(),
        ) {
            ensure_contract_instance_address_seed(
                executor,
                implementation_contract_instance_id,
                &loaded_manifest.manifest.chain,
                implementation_address,
                Some(manifest_id),
                &serde_json::json!({
                    "source": "manifest_proxy_implementation_seed",
                    "manifest_id": manifest_id,
                    "declaration_name": entry.key.declaration_name,
                    "source_family": loaded_manifest.manifest.source_family,
                    "proxy_contract_instance_id": entry.contract_instance_id,
                    "proxy_address": entry.declared_address,
                }),
            )
            .await?;
        }
    }

    Ok(())
}

async fn upsert_manifest_version(
    executor: &mut sqlx::postgres::PgConnection,
    loaded_manifest: &LoadedManifest,
) -> Result<i64> {
    let manifest_payload = serde_json::to_string(&loaded_manifest.manifest)
        .context("failed to serialize manifest payload")?;
    let manifest_key = ManifestStorageKey::from_loaded_manifest(loaded_manifest)?;

    let row = sqlx::query(
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
        VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
        ON CONFLICT (namespace, source_family, chain, deployment_epoch, manifest_version)
        DO UPDATE SET
            rollout_status = EXCLUDED.rollout_status,
            normalizer_version = EXCLUDED.normalizer_version,
            file_path = EXCLUDED.file_path,
            manifest_payload = EXCLUDED.manifest_payload,
            loaded_at = now()
        RETURNING manifest_id
        "#,
    )
    .bind(manifest_key.manifest_version)
    .bind(&manifest_key.namespace)
    .bind(&manifest_key.source_family)
    .bind(&manifest_key.chain)
    .bind(&manifest_key.deployment_epoch)
    .bind(loaded_manifest.manifest.rollout_status.as_db_value())
    .bind(&loaded_manifest.manifest.normalizer_version)
    .bind(loaded_manifest.relative_path.to_string_lossy().into_owned())
    .bind(manifest_payload)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert manifest version from {}",
            loaded_manifest.path.display()
        )
    })?;

    row.try_get("manifest_id")
        .context("failed to read manifest_id from manifest upsert")
}

async fn load_existing_manifest_entries(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
) -> Result<HashMap<DeclarationKey, PersistedManifestEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            code_hash,
            abi_ref,
            role,
            proxy_kind,
            implementation_contract_instance_id,
            declared_implementation_address
        FROM manifest_contract_instances
        WHERE manifest_id = $1
        "#,
    )
    .bind(manifest_id)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load existing manifest children for manifest_id {manifest_id}")
    })?;

    rows.into_iter()
        .map(|row| {
            let declaration_kind = row
                .try_get::<String, _>("declaration_kind")
                .context("failed to read declaration_kind")?;
            let declaration_name = row
                .try_get::<String, _>("declaration_name")
                .context("failed to read declaration_name")?;
            let entry = PersistedManifestEntry {
                key: DeclarationKey {
                    declaration_kind: declaration_kind.clone(),
                    declaration_name: declaration_name.clone(),
                },
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read contract_instance_id")?,
                declared_address: row
                    .try_get("declared_address")
                    .context("failed to read declared_address")?,
                code_hash: row
                    .try_get("code_hash")
                    .context("failed to read code_hash")?,
                abi_ref: row.try_get("abi_ref").context("failed to read abi_ref")?,
                role: row.try_get("role").context("failed to read role")?,
                proxy_kind: row
                    .try_get("proxy_kind")
                    .context("failed to read proxy_kind")?,
                implementation_contract_instance_id: row
                    .try_get("implementation_contract_instance_id")
                    .context("failed to read implementation_contract_instance_id")?,
                declared_implementation_address: row
                    .try_get("declared_implementation_address")
                    .context("failed to read declared_implementation_address")?,
            };
            Ok((entry.key.clone(), entry))
        })
        .collect()
}

async fn plan_manifest_entries(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    existing_entries: &HashMap<DeclarationKey, PersistedManifestEntry>,
) -> Result<Vec<PersistedManifestEntry>> {
    let mut planned_entries = Vec::new();
    let mut planned_contract_instance_ids_by_address = HashMap::<String, Uuid>::new();

    for root in &loaded_manifest.manifest.roots {
        let key = DeclarationKey {
            declaration_kind: DECLARATION_KIND_ROOT.to_owned(),
            declaration_name: root.name.clone(),
        };
        let declared_address = normalize_address(&root.address);
        let contract_instance_id =
            match planned_contract_instance_ids_by_address.get(&declared_address) {
                Some(contract_instance_id) => *contract_instance_id,
                None => {
                    let contract_instance_id = resolve_manifest_entry_contract_instance_id(
                        executor,
                        manifest_id,
                        loaded_manifest,
                        &key,
                        &declared_address,
                        existing_entries.get(&key),
                        CONTRACT_KIND_ROOT,
                    )
                    .await?;
                    planned_contract_instance_ids_by_address
                        .insert(declared_address.clone(), contract_instance_id);
                    contract_instance_id
                }
            };

        planned_entries.push(PersistedManifestEntry {
            key,
            contract_instance_id,
            declared_address,
            code_hash: root.code_hash.clone(),
            abi_ref: root.abi_ref.clone(),
            role: None,
            proxy_kind: None,
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        });
    }

    for contract in &loaded_manifest.manifest.contracts {
        let key = DeclarationKey {
            declaration_kind: DECLARATION_KIND_CONTRACT.to_owned(),
            declaration_name: contract.role.clone(),
        };
        let declared_address = normalize_address(&contract.address);
        let contract_instance_id =
            match planned_contract_instance_ids_by_address.get(&declared_address) {
                Some(contract_instance_id) => *contract_instance_id,
                None => {
                    let contract_instance_id = resolve_manifest_entry_contract_instance_id(
                        executor,
                        manifest_id,
                        loaded_manifest,
                        &key,
                        &declared_address,
                        existing_entries.get(&key),
                        CONTRACT_KIND_CONTRACT,
                    )
                    .await?;
                    planned_contract_instance_ids_by_address
                        .insert(declared_address.clone(), contract_instance_id);
                    contract_instance_id
                }
            };

        let declared_implementation_address = contract
            .implementation
            .as_ref()
            .map(|value| normalize_address(value));
        if declared_implementation_address.as_deref() == Some(declared_address.as_str()) {
            bail!(
                "manifest contract role {} in {} cannot declare the proxy address as its own implementation",
                contract.role,
                loaded_manifest.path.display()
            );
        }
        let implementation_contract_instance_id =
            if let Some(implementation_address) = &declared_implementation_address {
                Some(
                    resolve_contract_instance_by_address(
                        executor,
                        &loaded_manifest.manifest.chain,
                        implementation_address,
                        CONTRACT_KIND_CONTRACT,
                        &serde_json::json!({
                            "source": "manifest_contract_implementation",
                            "manifest_id": manifest_id,
                            "role": contract.role,
                        }),
                    )
                    .await?,
                )
            } else {
                None
            };

        planned_entries.push(PersistedManifestEntry {
            key,
            contract_instance_id,
            declared_address,
            code_hash: None,
            abi_ref: None,
            role: Some(contract.role.clone()),
            proxy_kind: Some(contract.proxy_kind.clone()),
            implementation_contract_instance_id,
            declared_implementation_address,
        });
    }

    Ok(planned_entries)
}

async fn resolve_manifest_entry_contract_instance_id(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
    declared_address: &str,
    existing_entry: Option<&PersistedManifestEntry>,
    contract_kind: &str,
) -> Result<Uuid> {
    if let Some(existing_entry) = existing_entry
        && existing_entry.declared_address == declared_address
    {
        return Ok(existing_entry.contract_instance_id);
    }

    if let Some(previous_entry) =
        load_latest_related_manifest_entry(executor, manifest_id, loaded_manifest, key).await?
        && previous_entry.declared_address == declared_address
    {
        return Ok(previous_entry.contract_instance_id);
    }

    resolve_contract_instance_by_address(
        executor,
        &loaded_manifest.manifest.chain,
        declared_address,
        contract_kind,
        &serde_json::json!({
            "source": "manifest_declaration",
            "manifest_id": manifest_id,
            "declaration_kind": key.declaration_kind,
            "declaration_name": key.declaration_name,
        }),
    )
    .await
}

async fn load_latest_related_manifest_entry(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
) -> Result<Option<PersistedManifestEntry>> {
    let row = sqlx::query(
        r#"
        SELECT
            mci.contract_instance_id,
            mci.declared_address,
            mci.code_hash,
            mci.abi_ref,
            mci.role,
            mci.proxy_kind,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_contract_instances mci
        JOIN manifest_versions mv ON mv.manifest_id = mci.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
          AND mv.chain = $3
          AND mv.deployment_epoch = $4
          AND mci.declaration_kind = $5
          AND mci.declaration_name = $6
          AND mci.manifest_id <> $7
        ORDER BY mv.manifest_version DESC, mci.manifest_contract_instance_id DESC
        LIMIT 1
        "#,
    )
    .bind(&loaded_manifest.manifest.namespace)
    .bind(&loaded_manifest.manifest.source_family)
    .bind(&loaded_manifest.manifest.chain)
    .bind(&loaded_manifest.manifest.deployment_epoch)
    .bind(&key.declaration_kind)
    .bind(&key.declaration_name)
    .bind(manifest_id)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load prior declaration state for {} {}",
            key.declaration_kind, key.declaration_name
        )
    })?;

    row.map(|row| {
        Ok(PersistedManifestEntry {
            key: key.clone(),
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read prior contract_instance_id")?,
            declared_address: row
                .try_get("declared_address")
                .context("failed to read prior declared_address")?,
            code_hash: row
                .try_get("code_hash")
                .context("failed to read prior code_hash")?,
            abi_ref: row
                .try_get("abi_ref")
                .context("failed to read prior abi_ref")?,
            role: row.try_get("role").context("failed to read prior role")?,
            proxy_kind: row
                .try_get("proxy_kind")
                .context("failed to read prior proxy_kind")?,
            implementation_contract_instance_id: row
                .try_get("implementation_contract_instance_id")
                .context("failed to read prior implementation_contract_instance_id")?,
            declared_implementation_address: row
                .try_get("declared_implementation_address")
                .context("failed to read prior declared_implementation_address")?,
        })
    })
    .transpose()
}

pub(crate) async fn resolve_contract_instance_by_address(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
    contract_kind: &str,
    provenance: &serde_json::Value,
) -> Result<Uuid> {
    let normalized_address = normalize_address(address);

    if let Some(contract_instance_id) =
        find_contract_instance_by_address(executor, chain, &normalized_address).await?
    {
        return Ok(contract_instance_id);
    }

    let contract_instance_id = Uuid::new_v4();
    let provenance = serde_json::to_string(provenance)
        .context("failed to serialize contract-instance provenance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, $3, $4::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(contract_kind)
    .bind(provenance)
    .execute(executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert contract_instance_id {contract_instance_id} for chain {chain} address {normalized_address}"
        )
    })?;

    Ok(contract_instance_id)
}

async fn find_contract_instance_by_address(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT contract_instance_id
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND address = $2
        ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(address)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to resolve contract instance for chain {chain} address {address}")
    })
}

pub(crate) async fn ensure_contract_instance_address_seed(
    executor: &mut sqlx::postgres::PgConnection,
    contract_instance_id: Uuid,
    chain: &str,
    address: &str,
    source_manifest_id: Option<i64>,
    provenance: &serde_json::Value,
) -> Result<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM contract_instance_addresses
            WHERE contract_instance_id = $1
        )
        "#,
    )
    .bind(contract_instance_id)
    .fetch_one(&mut *executor)
    .await
    .with_context(|| {
        format!(
            "failed to check seeded address rows for contract_instance_id {contract_instance_id}"
        )
    })?;

    if exists {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, $4, $5::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(address)
    .bind(source_manifest_id)
    .bind(
        serde_json::to_string(provenance)
            .context("failed to serialize contract-instance address seed provenance")?,
    )
    .execute(&mut *executor)
    .await
    .with_context(|| {
        format!(
            "failed to seed contract-instance address row for contract_instance_id {contract_instance_id}"
        )
    })?;

    Ok(())
}
