use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    MANIFEST_PROXY_IMPLEMENTATION_ADMISSION, MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE,
    MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND, MANIFEST_SUCCESSOR_ADMISSION,
    MANIFEST_SUCCESSOR_DISCOVERY_SOURCE, MANIFEST_SUCCESSOR_EDGE_KIND, SourceManifest,
    support::{
        ActiveAddressSpec, CurrentActiveAddressRow, ExistingManagedEdge, ManagedEdgeSpec,
        ManifestLineageKey, ManifestTransition, OrderedManifestEntry, PersistedManifestEntry,
    },
};
pub(crate) async fn replace_manifest_children(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    manifest: &SourceManifest,
    planned_entries: &[PersistedManifestEntry],
) -> Result<()> {
    sqlx::query("DELETE FROM manifest_contract_instances WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_contract_instances for manifest_id {manifest_id}")
        })?;
    sqlx::query("DELETE FROM manifest_capability_flags WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_capability_flags for manifest_id {manifest_id}")
        })?;
    sqlx::query("DELETE FROM manifest_discovery_rules WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_discovery_rules for manifest_id {manifest_id}")
        })?;

    for entry in planned_entries {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
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
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(manifest_id)
        .bind(&entry.key.declaration_kind)
        .bind(&entry.key.declaration_name)
        .bind(entry.contract_instance_id)
        .bind(&entry.declared_address)
        .bind(entry.code_hash.as_deref())
        .bind(entry.abi_ref.as_deref())
        .bind(entry.role.as_deref())
        .bind(entry.proxy_kind.as_deref())
        .bind(entry.implementation_contract_instance_id)
        .bind(entry.declared_implementation_address.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest entry {} {} for manifest_id {manifest_id}",
                entry.key.declaration_kind, entry.key.declaration_name
            )
        })?;
    }

    for (capability_name, capability_flag) in &manifest.capability_flags {
        sqlx::query(
            r#"
            INSERT INTO manifest_capability_flags (
                manifest_id,
                capability_name,
                status,
                notes
            )
            VALUES ($1, $2, $3::capability_support_status, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(capability_flag.status.as_db_value())
        .bind(capability_flag.notes.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert capability {} for manifest_id {manifest_id}",
                capability_name
            )
        })?;
    }

    for discovery_rule in &manifest.discovery_rules {
        sqlx::query(
            r#"
            INSERT INTO manifest_discovery_rules (
                manifest_id,
                edge_kind,
                from_role,
                admission
            )
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(&discovery_rule.edge_kind)
        .bind(&discovery_rule.from_role)
        .bind(&discovery_rule.admission)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert discovery rule {} for manifest_id {manifest_id}",
                discovery_rule.edge_kind
            )
        })?;
    }

    Ok(())
}

pub(crate) async fn reconcile_manifest_source_graph(
    executor: &mut sqlx::postgres::PgConnection,
    in_place_transitions: &[ManifestTransition],
) -> Result<usize> {
    let desired_proxy_edges = load_desired_proxy_edges(executor).await?;
    let desired_successor_edges =
        load_desired_manifest_successor_edges(executor, in_place_transitions).await?;

    let mut cleared_edge_count = 0;
    cleared_edge_count += reconcile_managed_edges(
        executor,
        &desired_proxy_edges,
        MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE,
    )
    .await?;
    cleared_edge_count += reconcile_managed_edges(
        executor,
        &desired_successor_edges,
        MANIFEST_SUCCESSOR_DISCOVERY_SOURCE,
    )
    .await?;

    reconcile_active_contract_instance_addresses(executor).await?;

    Ok(cleared_edge_count)
}

async fn load_desired_proxy_edges(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<Vec<ManagedEdgeSpec>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            mci.contract_instance_id,
            mci.implementation_contract_instance_id,
            mci.declaration_name,
            mci.proxy_kind,
            mci.declared_address,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
          AND mci.implementation_contract_instance_id IS NOT NULL
        ORDER BY mv.manifest_id, mci.declaration_name
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load desired proxy edges")?;

    rows.into_iter()
        .map(|row| {
            let implementation_contract_instance_id = row
                .try_get::<Uuid, _>("implementation_contract_instance_id")
                .context("failed to read implementation_contract_instance_id")?;
            let provenance_json = serde_json::json!({
                "source": "manifest_contract",
                "declaration_name": row
                    .try_get::<String, _>("declaration_name")
                    .context("failed to read declaration_name")?,
                "proxy_kind": row
                    .try_get::<String, _>("proxy_kind")
                    .context("failed to read proxy_kind")?,
                "from_address": row
                    .try_get::<String, _>("declared_address")
                    .context("failed to read declared_address")?,
                "to_address": row
                    .try_get::<Option<String>, _>("declared_implementation_address")
                    .context("failed to read declared_implementation_address")?,
            })
            .to_string();
            Ok(ManagedEdgeSpec {
                chain: row.try_get("chain").context("failed to read chain")?,
                edge_kind: MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND.to_owned(),
                from_contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read contract_instance_id")?,
                to_contract_instance_id: implementation_contract_instance_id,
                discovery_source: MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE.to_owned(),
                source_manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read manifest_id")?,
                admission: MANIFEST_PROXY_IMPLEMENTATION_ADMISSION.to_owned(),
                provenance_json,
            })
        })
        .collect()
}

async fn load_desired_manifest_successor_edges(
    executor: &mut sqlx::postgres::PgConnection,
    in_place_transitions: &[ManifestTransition],
) -> Result<Vec<ManagedEdgeSpec>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.rollout_status::TEXT AS rollout_status,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        ORDER BY
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mv.manifest_version,
            mv.manifest_id
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load ordered manifest entries for successor continuity")?;

    let mut desired = HashSet::new();
    let mut last_by_lineage = HashMap::<ManifestLineageKey, OrderedManifestEntry>::new();

    for row in rows {
        let entry = OrderedManifestEntry {
            manifest_id: row
                .try_get("manifest_id")
                .context("failed to read manifest_id")?,
            manifest_version: row
                .try_get("manifest_version")
                .context("failed to read manifest_version")?,
            rollout_status: row
                .try_get("rollout_status")
                .context("failed to read rollout_status")?,
            chain: row.try_get("chain").context("failed to read chain")?,
            lineage_key: ManifestLineageKey {
                namespace: row
                    .try_get("namespace")
                    .context("failed to read namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read lineage chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read deployment_epoch")?,
                declaration_kind: row
                    .try_get("declaration_kind")
                    .context("failed to read declaration_kind")?,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read declaration_name")?,
            },
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read contract_instance_id")?,
            declared_address: row
                .try_get("declared_address")
                .context("failed to read declared_address")?,
        };

        if let Some(previous_entry) =
            last_by_lineage.insert(entry.lineage_key.clone(), entry.clone())
            && entry.rollout_status == "active"
            && previous_entry.contract_instance_id != entry.contract_instance_id
            && previous_entry.declared_address != entry.declared_address
        {
            desired.insert(ManagedEdgeSpec {
                chain: entry.chain.clone(),
                edge_kind: MANIFEST_SUCCESSOR_EDGE_KIND.to_owned(),
                from_contract_instance_id: previous_entry.contract_instance_id,
                to_contract_instance_id: entry.contract_instance_id,
                discovery_source: MANIFEST_SUCCESSOR_DISCOVERY_SOURCE.to_owned(),
                source_manifest_id: entry.manifest_id,
                admission: MANIFEST_SUCCESSOR_ADMISSION.to_owned(),
                provenance_json: serde_json::json!({
                    "source": "manifest_successor",
                    "declaration_kind": entry.lineage_key.declaration_kind,
                    "declaration_name": entry.lineage_key.declaration_name,
                    "from_address": previous_entry.declared_address,
                    "to_address": entry.declared_address,
                    "manifest_version": entry.manifest_version,
                })
                .to_string(),
            });
        }
    }

    for transition in in_place_transitions {
        desired.insert(ManagedEdgeSpec {
            chain: transition.chain.clone(),
            edge_kind: MANIFEST_SUCCESSOR_EDGE_KIND.to_owned(),
            from_contract_instance_id: transition.from_contract_instance_id,
            to_contract_instance_id: transition.to_contract_instance_id,
            discovery_source: MANIFEST_SUCCESSOR_DISCOVERY_SOURCE.to_owned(),
            source_manifest_id: transition.source_manifest_id,
            admission: MANIFEST_SUCCESSOR_ADMISSION.to_owned(),
            provenance_json: serde_json::json!({
                "source": "manifest_successor",
                "declaration_kind": transition.declaration_kind,
                "declaration_name": transition.declaration_name,
                "from_address": transition.from_address,
                "to_address": transition.to_address,
                "manifest_update": "in_place",
            })
            .to_string(),
        });
    }

    Ok(desired.into_iter().collect())
}

async fn reconcile_managed_edges(
    executor: &mut sqlx::postgres::PgConnection,
    desired_edges: &[ManagedEdgeSpec],
    discovery_source: &str,
) -> Result<usize> {
    let existing_rows = sqlx::query(
        r#"
        SELECT
            discovery_edge_id,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            provenance
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .fetch_all(&mut *executor)
    .await
    .with_context(|| {
        format!("failed to load active managed edges for discovery_source {discovery_source}")
    })?;

    let existing_edges = existing_rows
        .into_iter()
        .map(|row| {
            Ok(ExistingManagedEdge {
                discovery_edge_id: row
                    .try_get("discovery_edge_id")
                    .context("failed to read discovery_edge_id")?,
                spec: ManagedEdgeSpec {
                    chain: row.try_get("chain_id").context("failed to read chain_id")?,
                    edge_kind: row
                        .try_get("edge_kind")
                        .context("failed to read edge_kind")?,
                    from_contract_instance_id: row
                        .try_get("from_contract_instance_id")
                        .context("failed to read from_contract_instance_id")?,
                    to_contract_instance_id: row
                        .try_get("to_contract_instance_id")
                        .context("failed to read to_contract_instance_id")?,
                    discovery_source: row
                        .try_get("discovery_source")
                        .context("failed to read discovery_source")?,
                    source_manifest_id: row
                        .try_get::<Option<i64>, _>("source_manifest_id")
                        .context("failed to read source_manifest_id")?
                        .unwrap_or(-1),
                    admission: row
                        .try_get("admission")
                        .context("failed to read admission")?,
                    provenance_json: row
                        .try_get::<serde_json::Value, _>("provenance")
                        .context("failed to read provenance")?
                        .to_string(),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let existing_set = existing_edges
        .iter()
        .map(|edge| edge.spec.clone())
        .collect::<HashSet<_>>();

    let mut cleared_edge_count = 0;
    for existing_edge in existing_edges {
        if desired_set.contains(&existing_edge.spec) {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET deactivated_at = now()
            WHERE discovery_edge_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_edge.discovery_edge_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to deactivate managed discovery_edge_id {}",
                existing_edge.discovery_edge_id
            )
        })?;
        cleared_edge_count += 1;
    }

    for desired_edge in desired_edges {
        if existing_set.contains(desired_edge) {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            "#,
        )
        .bind(&desired_edge.chain)
        .bind(&desired_edge.edge_kind)
        .bind(desired_edge.from_contract_instance_id)
        .bind(desired_edge.to_contract_instance_id)
        .bind(&desired_edge.discovery_source)
        .bind(desired_edge.source_manifest_id)
        .bind(&desired_edge.admission)
        .bind(&desired_edge.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert managed edge {} {} -> {}",
                desired_edge.edge_kind,
                desired_edge.from_contract_instance_id,
                desired_edge.to_contract_instance_id
            )
        })?;
    }

    Ok(cleared_edge_count)
}

pub(crate) async fn reconcile_active_contract_instance_addresses(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<()> {
    let desired_specs = load_desired_active_address_specs(executor).await?;
    let desired_ids = desired_specs
        .iter()
        .map(|spec| spec.contract_instance_id)
        .collect::<HashSet<_>>();

    let existing_active_rows = sqlx::query(
        r#"
        SELECT contract_instance_id, chain_id, address
        FROM contract_instance_addresses
        WHERE deactivated_at IS NULL
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active contract-instance addresses")?;

    let existing_active = existing_active_rows
        .into_iter()
        .map(|row| {
            Ok(CurrentActiveAddressRow {
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active contract_instance_id")?,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read active chain_id")?,
                address: row
                    .try_get("address")
                    .context("failed to read active address")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    for existing_row in &existing_active {
        if desired_ids.contains(&existing_row.contract_instance_id) {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET deactivated_at = now()
            WHERE contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_row.contract_instance_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to deactivate active address row for contract_instance_id {}",
                existing_row.contract_instance_id
            )
        })?;
    }

    let existing_active_map = existing_active
        .into_iter()
        .map(|row| (row.contract_instance_id, row))
        .collect::<HashMap<_, _>>();

    for desired_spec in desired_specs {
        if let Some(existing_row) = existing_active_map.get(&desired_spec.contract_instance_id) {
            if existing_row.chain != desired_spec.chain
                || existing_row.address != desired_spec.address
            {
                bail!(
                    "contract_instance_id {} changed address from {}:{} to {}:{}; successor addresses must rotate IDs",
                    desired_spec.contract_instance_id,
                    existing_row.chain,
                    existing_row.address,
                    desired_spec.chain,
                    desired_spec.address
                );
            }
            continue;
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
        .bind(desired_spec.contract_instance_id)
        .bind(&desired_spec.chain)
        .bind(&desired_spec.address)
        .bind(desired_spec.source_manifest_id)
        .bind(&desired_spec.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to activate address {} for contract_instance_id {}",
                desired_spec.address, desired_spec.contract_instance_id
            )
        })?;
    }

    Ok(())
}

async fn load_desired_active_address_specs(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<Vec<ActiveAddressSpec>> {
    let manifest_rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.manifest_id, mci.declaration_kind, mci.declaration_name
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active manifest address specs")?;

    let discovery_endpoint_rows = sqlx::query(
        r#"
        WITH active_discovery_endpoints AS (
            SELECT de.source_manifest_id, de.from_contract_instance_id AS contract_instance_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'

            UNION

            SELECT de.source_manifest_id, de.to_contract_instance_id AS contract_instance_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'
        )
        SELECT
            endpoints.source_manifest_id,
            cia.contract_instance_id,
            cia.chain_id,
            cia.address
        FROM active_discovery_endpoints endpoints
        JOIN LATERAL (
            SELECT contract_instance_id, chain_id, address
            FROM contract_instance_addresses
            WHERE contract_instance_id = endpoints.contract_instance_id
            ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
            LIMIT 1
        ) cia ON TRUE
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load discovery-edge endpoint address specs")?;

    let mut specs = HashMap::<Uuid, ActiveAddressSpec>::new();

    for row in manifest_rows {
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read manifest contract_instance_id")?;
        let declaration_kind = row
            .try_get::<String, _>("declaration_kind")
            .context("failed to read declaration_kind")?;
        let declaration_name = row
            .try_get::<String, _>("declaration_name")
            .context("failed to read declaration_name")?;
        let chain: String = row.try_get("chain").context("failed to read chain")?;
        let manifest_id = row
            .try_get::<i64, _>("manifest_id")
            .context("failed to read manifest_id")?;
        let declared_address = row
            .try_get::<String, _>("declared_address")
            .context("failed to read declared_address")?;

        specs
            .entry(contract_instance_id)
            .or_insert(ActiveAddressSpec {
                contract_instance_id,
                chain: chain.clone(),
                address: declared_address.clone(),
                source_manifest_id: Some(manifest_id),
                provenance_json: serde_json::json!({
                    "source": "manifest_declared",
                    "declaration_kind": declaration_kind,
                    "declaration_name": declaration_name,
                })
                .to_string(),
            });

        let implementation_contract_instance_id = row
            .try_get::<Option<Uuid>, _>("implementation_contract_instance_id")
            .context("failed to read implementation_contract_instance_id")?;
        let declared_implementation_address = row
            .try_get::<Option<String>, _>("declared_implementation_address")
            .context("failed to read declared_implementation_address")?;
        if let (Some(implementation_contract_instance_id), Some(implementation_address)) = (
            implementation_contract_instance_id,
            declared_implementation_address,
        ) {
            specs
                .entry(implementation_contract_instance_id)
                .or_insert(ActiveAddressSpec {
                    contract_instance_id: implementation_contract_instance_id,
                    chain: chain.clone(),
                    address: implementation_address.clone(),
                    source_manifest_id: Some(manifest_id),
                    provenance_json: serde_json::json!({
                        "source": "manifest_proxy_implementation",
                        "proxy_contract_instance_id": contract_instance_id,
                        "proxy_address": declared_address,
                    })
                    .to_string(),
                });
        }
    }

    for row in discovery_endpoint_rows {
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read discovery endpoint contract_instance_id")?;
        specs
            .entry(contract_instance_id)
            .or_insert(ActiveAddressSpec {
                contract_instance_id,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read discovery endpoint chain_id")?,
                address: row
                    .try_get("address")
                    .context("failed to read discovery endpoint address")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read discovery endpoint source_manifest_id")?,
                provenance_json: serde_json::json!({
                    "source": "discovery_edge_endpoint",
                })
                .to_string(),
            });
    }

    Ok(specs.into_values().collect())
}
