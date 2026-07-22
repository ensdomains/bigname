use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    ManifestRuntimeProgress,
    support::{ActiveAddressSpec, CurrentActiveAddressRow},
};

const MANIFEST_GRAPH_PROGRESS_ROWS: usize = 1_000;

pub(crate) async fn reconcile_active_contract_instance_addresses(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<()> {
    let mut progress = None;
    reconcile_active_contract_instance_addresses_inner(executor, None, &mut progress)
        .await
        .map(|_| ())
}

pub(crate) async fn reconcile_active_contract_instance_addresses_with_mutations_and_progress(
    executor: &mut sqlx::postgres::PgConnection,
    pool: &PgPool,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<BTreeSet<String>> {
    reconcile_active_contract_instance_addresses_inner(executor, Some(pool), progress).await
}

async fn reconcile_active_contract_instance_addresses_inner(
    executor: &mut sqlx::postgres::PgConnection,
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<BTreeSet<String>> {
    let desired_specs =
        load_desired_active_address_specs(executor, false, &[], pool, progress).await?;
    let existing_active =
        load_existing_active_address_rows(executor, false, &[], pool, progress).await?;
    apply_active_address_reconciliation(executor, desired_specs, existing_active, pool, progress)
        .await
}

pub(crate) async fn reconcile_active_contract_instance_addresses_for_ids(
    executor: &mut sqlx::postgres::PgConnection,
    contract_instance_ids: &HashSet<Uuid>,
) -> Result<()> {
    if contract_instance_ids.is_empty() {
        return Ok(());
    }

    let mut scoped_ids = contract_instance_ids.iter().copied().collect::<Vec<_>>();
    scoped_ids.sort();
    let mut progress = None;
    let desired_specs =
        load_desired_active_address_specs(executor, true, &scoped_ids, None, &mut progress).await?;
    let existing_active =
        load_existing_active_address_rows(executor, true, &scoped_ids, None, &mut progress).await?;
    apply_active_address_reconciliation(
        executor,
        desired_specs,
        existing_active,
        None,
        &mut progress,
    )
    .await
    .map(|_| ())
}

async fn apply_active_address_reconciliation(
    executor: &mut sqlx::postgres::PgConnection,
    desired_specs: Vec<ActiveAddressSpec>,
    existing_active: Vec<CurrentActiveAddressRow>,
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<BTreeSet<String>> {
    let mut mutated_chains = BTreeSet::new();
    let mut desired_ids = HashSet::with_capacity(desired_specs.len());
    for (index, spec) in desired_specs.iter().enumerate() {
        desired_ids.insert(spec.contract_instance_id);
        record_progress_after_row(pool, progress, index).await?;
    }

    for (index, existing_row) in existing_active.iter().enumerate() {
        if desired_ids.contains(&existing_row.contract_instance_id) {
            record_progress_after_row(pool, progress, index).await?;
            continue;
        }

        let result = sqlx::query(
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
        if result.rows_affected() > 0 {
            mutated_chains.insert(existing_row.chain.clone());
        }
        record_progress_after_row(pool, progress, index).await?;
    }

    let mut existing_active_map = HashMap::with_capacity(existing_active.len());
    for (index, row) in existing_active.into_iter().enumerate() {
        existing_active_map.insert(row.contract_instance_id, row);
        record_progress_after_row(pool, progress, index).await?;
    }

    for (index, desired_spec) in desired_specs.into_iter().enumerate() {
        let manifest_declared_active_from_block =
            manifest_declared_active_from_block(&desired_spec.provenance_json)?;
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
            if let Some(active_from_block_number) = manifest_declared_active_from_block {
                let result = sqlx::query(
                    r#"
                    UPDATE contract_instance_addresses
                    SET active_from_block_number = $2,
                        active_from_block_hash = NULL
                    WHERE contract_instance_id = $1
                      AND deactivated_at IS NULL
                      AND (
                          active_from_block_number IS DISTINCT FROM $2
                          OR active_from_block_hash IS NOT NULL
                      )
                    "#,
                )
                .bind(desired_spec.contract_instance_id)
                .bind(active_from_block_number)
                .execute(&mut *executor)
                .await
                .with_context(|| {
                    format!(
                        "failed to update manifest-declared active range for contract_instance_id {}",
                        desired_spec.contract_instance_id
                    )
                })?;
                if result.rows_affected() > 0 {
                    mutated_chains.insert(desired_spec.chain.clone());
                }
            }
            record_progress_after_row(pool, progress, index).await?;
            continue;
        }

        let active_from_block_number = manifest_declared_active_from_block.flatten();
        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                active_from_block_number,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            "#,
        )
        .bind(desired_spec.contract_instance_id)
        .bind(&desired_spec.chain)
        .bind(&desired_spec.address)
        .bind(desired_spec.source_manifest_id)
        .bind(active_from_block_number)
        .bind(&desired_spec.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to activate address {} for contract_instance_id {}",
                desired_spec.address, desired_spec.contract_instance_id
            )
        })?;
        mutated_chains.insert(desired_spec.chain);
        record_progress_after_row(pool, progress, index).await?;
    }

    Ok(mutated_chains)
}

async fn load_existing_active_address_rows(
    executor: &mut sqlx::postgres::PgConnection,
    scoped: bool,
    scope_ids: &[Uuid],
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<Vec<CurrentActiveAddressRow>> {
    let query = sqlx::query(
        r#"
        SELECT contract_instance_id, chain_id, address
        FROM contract_instance_addresses
        WHERE deactivated_at IS NULL
          AND ($1::BOOLEAN = FALSE OR contract_instance_id = ANY($2::UUID[]))
        "#,
    )
    .bind(scoped)
    .bind(scope_ids);
    let mut rows = query.fetch(&mut *executor);
    let mut existing = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .context("failed to stream active contract-instance addresses")?
    {
        existing.push(current_active_address_from_row(row)?);
        record_progress_after_row(pool, progress, existing.len() - 1).await?;
    }
    record_final_progress(pool, progress, existing.len()).await?;
    Ok(existing)
}

fn current_active_address_from_row(row: PgRow) -> Result<CurrentActiveAddressRow> {
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
}

async fn load_desired_active_address_specs(
    executor: &mut sqlx::postgres::PgConnection,
    scoped: bool,
    scope_ids: &[Uuid],
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
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
            manifest_range.start_block AS manifest_start_block,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        LEFT JOIN LATERAL (
            SELECT (entry ->> 'start_block')::BIGINT AS start_block
            FROM jsonb_array_elements(
                CASE
                    WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                    ELSE mv.manifest_payload -> 'contracts'
                END
            ) entry
            WHERE (
                    mci.declaration_kind = 'root'
                    AND entry ->> 'name' = mci.declaration_name
                )
               OR (
                    mci.declaration_kind = 'contract'
                    AND entry ->> 'role' = mci.declaration_name
                )
            ORDER BY start_block NULLS LAST
            LIMIT 1
        ) manifest_range ON TRUE
        WHERE mv.rollout_status = 'active'
          AND (
              $1::BOOLEAN = FALSE
              OR mci.contract_instance_id = ANY($2::UUID[])
              OR mci.implementation_contract_instance_id = ANY($2::UUID[])
          )
        ORDER BY mv.manifest_id, mci.declaration_kind, mci.declaration_name
        "#,
    )
    .bind(scoped)
    .bind(scope_ids)
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active manifest address specs")?;
    record_progress(pool, progress).await?;

    let mut specs = HashMap::<Uuid, ActiveAddressSpec>::new();
    for row in manifest_rows {
        merge_manifest_address_spec(&mut specs, row, scoped, scope_ids)?;
    }

    if scoped {
        let discovery_endpoint_rows = sqlx::query(
            r#"
            WITH scoped_ids AS (
                SELECT DISTINCT contract_instance_id
                FROM UNNEST($1::UUID[]) AS scope(contract_instance_id)
            )
            SELECT
                endpoint.source_manifest_id,
                cia.contract_instance_id,
                cia.chain_id,
                cia.address
            FROM scoped_ids scope
            JOIN LATERAL (
                SELECT de.source_manifest_id
                FROM discovery_edges de
                JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
                WHERE mv.rollout_status = 'active'
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                  AND de.to_contract_instance_id = scope.contract_instance_id
                ORDER BY de.source_manifest_id
                LIMIT 1
            ) endpoint ON TRUE
            JOIN LATERAL (
                SELECT contract_instance_id, chain_id, address
                FROM contract_instance_addresses
                WHERE contract_instance_id = scope.contract_instance_id
                ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
                LIMIT 1
            ) cia ON TRUE
            "#,
        )
        .bind(scope_ids)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load scoped discovery-edge endpoint address specs")?;
        for row in discovery_endpoint_rows {
            let spec = discovery_endpoint_spec_from_row(row)?;
            specs.entry(spec.contract_instance_id).or_insert(spec);
        }
        record_progress(pool, progress).await?;
    } else {
        let mut rows = sqlx::query(
            r#"
            SELECT
                de.source_manifest_id,
                cia.contract_instance_id,
                cia.chain_id,
                cia.address
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            JOIN LATERAL (
                SELECT contract_instance_id, chain_id, address
                FROM contract_instance_addresses
                WHERE contract_instance_id = de.to_contract_instance_id
                ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
                LIMIT 1
            ) cia ON TRUE
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'
            "#,
        )
        .fetch(&mut *executor);
        let mut discovery_specs = HashMap::<Uuid, ActiveAddressSpec>::new();
        let mut row_count = 0usize;
        while let Some(row) = rows
            .try_next()
            .await
            .context("failed to stream discovery-edge endpoint address specs")?
        {
            let spec = discovery_endpoint_spec_from_row(row)?;
            discovery_specs
                .entry(spec.contract_instance_id)
                .and_modify(|existing| {
                    if spec.source_manifest_id < existing.source_manifest_id {
                        *existing = spec.clone();
                    }
                })
                .or_insert(spec);
            row_count += 1;
            record_progress_after_row(pool, progress, row_count - 1).await?;
        }
        record_final_progress(pool, progress, row_count).await?;
        for (index, spec) in discovery_specs.into_values().enumerate() {
            specs.entry(spec.contract_instance_id).or_insert(spec);
            record_progress_after_row(pool, progress, index).await?;
        }
    }

    Ok(specs.into_values().collect())
}

fn merge_manifest_address_spec(
    specs: &mut HashMap<Uuid, ActiveAddressSpec>,
    row: PgRow,
    scoped: bool,
    scope_ids: &[Uuid],
) -> Result<()> {
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
    let manifest_start_block = row
        .try_get::<Option<i64>, _>("manifest_start_block")
        .context("failed to read manifest_start_block")?;

    if !scoped || scope_ids.contains(&contract_instance_id) {
        let provenance_json = manifest_declared_address_provenance(
            &declaration_kind,
            &declaration_name,
            manifest_start_block,
        )?;
        if let Some(existing_spec) = specs.get_mut(&contract_instance_id) {
            if manifest_start_block.is_some()
                && manifest_declared_active_from_block(&existing_spec.provenance_json)?
                    .flatten()
                    .is_none()
            {
                existing_spec.provenance_json = provenance_json;
            }
        } else {
            specs.insert(
                contract_instance_id,
                ActiveAddressSpec {
                    contract_instance_id,
                    chain: chain.clone(),
                    address: declared_address.clone(),
                    source_manifest_id: Some(manifest_id),
                    provenance_json,
                },
            );
        }
    }

    let implementation_contract_instance_id = row
        .try_get::<Option<Uuid>, _>("implementation_contract_instance_id")
        .context("failed to read implementation_contract_instance_id")?;
    let declared_implementation_address = row
        .try_get::<Option<String>, _>("declared_implementation_address")
        .context("failed to read declared_implementation_address")?;
    if let (Some(implementation_contract_instance_id), Some(implementation_address)) = (
        implementation_contract_instance_id,
        declared_implementation_address,
    ) && (!scoped || scope_ids.contains(&implementation_contract_instance_id))
    {
        specs
            .entry(implementation_contract_instance_id)
            .or_insert(ActiveAddressSpec {
                contract_instance_id: implementation_contract_instance_id,
                chain,
                address: implementation_address,
                source_manifest_id: Some(manifest_id),
                provenance_json: serde_json::json!({
                    "source": "manifest_proxy_implementation",
                    "proxy_contract_instance_id": contract_instance_id,
                    "proxy_address": declared_address,
                })
                .to_string(),
            });
    }
    Ok(())
}

fn discovery_endpoint_spec_from_row(row: PgRow) -> Result<ActiveAddressSpec> {
    let contract_instance_id = row
        .try_get::<Uuid, _>("contract_instance_id")
        .context("failed to read discovery endpoint contract_instance_id")?;
    Ok(ActiveAddressSpec {
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
    })
}

async fn record_progress_after_row(
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
    index: usize,
) -> Result<()> {
    if (index + 1).is_multiple_of(MANIFEST_GRAPH_PROGRESS_ROWS) {
        record_progress(pool, progress).await?;
    }
    Ok(())
}

async fn record_final_progress(
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
    row_count: usize,
) -> Result<()> {
    if row_count > 0 && !row_count.is_multiple_of(MANIFEST_GRAPH_PROGRESS_ROWS) {
        record_progress(pool, progress).await?;
    }
    Ok(())
}

async fn record_progress(
    pool: Option<&PgPool>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<()> {
    if let (Some(pool), Some(progress)) = (pool, progress.as_deref_mut()) {
        progress.record(pool).await?;
    }
    Ok(())
}

fn manifest_declared_address_provenance(
    declaration_kind: &str,
    declaration_name: &str,
    start_block: Option<i64>,
) -> Result<String> {
    let mut provenance = serde_json::json!({
        "source": "manifest_declared",
        "declaration_kind": declaration_kind,
        "declaration_name": declaration_name,
    });
    if let (Some(fields), Some(start_block)) = (provenance.as_object_mut(), start_block) {
        fields.insert("start_block".to_owned(), serde_json::json!(start_block));
    }
    serde_json::to_string(&provenance).context("failed to serialize manifest address provenance")
}

fn manifest_declared_active_from_block(provenance_json: &str) -> Result<Option<Option<i64>>> {
    let provenance = serde_json::from_str::<Value>(provenance_json)
        .context("failed to parse active address provenance")?;
    if provenance.get("source").and_then(Value::as_str) != Some("manifest_declared") {
        return Ok(None);
    }

    Ok(Some(
        provenance
            .get("start_block")
            .map(|value| {
                value
                    .as_i64()
                    .context("manifest_declared start_block provenance must be an integer")
            })
            .transpose()?,
    ))
}
