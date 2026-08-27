use alloy_primitives::keccak256;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeSet, HashSet};
use uuid::Uuid;

use crate::{LoadedManifest, ManifestContract, SourceManifest};

use super::{ManifestAddressDeclaration, retirement::RetiredManifestAddress};

#[derive(sqlx::FromRow)]
struct ExistingContractAddress {
    row_id: i64,
    instance_id: Uuid,
    is_active: bool,
    active_from: Option<i64>,
    active_to: Option<i64>,
    prior_epoch_end: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resolve_contract(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    address: &str,
    contract_kind: &str,
    manifest_id: i64,
    start_block: Option<u64>,
    admit_address: bool,
    provenance: serde_json::Value,
    repaired_floor_chains: &mut std::collections::HashSet<String>,
) -> Result<Uuid> {
    let existing = sqlx::query_as::<_, ExistingContractAddress>(
        "
        SELECT current.contract_instance_address_id AS row_id,
               current.contract_instance_id AS instance_id,
               current.deactivated_at IS NULL AS is_active,
               current.active_from_block_number AS active_from,
               current.active_to_block_number AS active_to,
               (
                   SELECT max(history.active_to_block_number)
                   FROM contract_instance_addresses history
                   WHERE history.contract_instance_id = current.contract_instance_id
                     AND history.chain_id = current.chain_id
                     AND lower(history.address) = lower(current.address)
                     AND history.contract_instance_address_id <>
                         current.contract_instance_address_id
               ) AS prior_epoch_end
        FROM contract_instance_addresses current
        WHERE current.chain_id = $1
          AND lower(current.address) = $2
        ORDER BY (current.deactivated_at IS NULL) DESC, current.admitted_at DESC
        LIMIT 1
        ",
    )
    .bind(chain_id)
    .bind(address)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| format!("failed to resolve declared contract {chain_id}:{address}"))?;
    let instance = if let Some(existing) = existing.as_ref() {
        existing.instance_id
    } else {
        sqlx::query_scalar::<_, Uuid>(
            "
            SELECT contract_instance_id
            FROM contract_instances
            WHERE chain_id = $1
              AND lower(provenance ->> 'declared_address') = $2
            ORDER BY inserted_at, contract_instance_id
            LIMIT 1
            ",
        )
        .bind(chain_id)
        .bind(address)
        .fetch_optional(&mut **transaction)
        .await
        .with_context(|| format!("failed to resolve retained contract {chain_id}:{address}"))?
        .unwrap_or_else(|| declared_contract_id(chain_id, address))
    };
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (contract_instance_id) DO UPDATE
        SET contract_kind = CASE
                WHEN contract_instances.contract_kind = 'root' OR EXCLUDED.contract_kind = 'root'
                    THEN 'root'
                ELSE 'contract'
            END,
            provenance = EXCLUDED.provenance
        ",
    )
    .bind(instance)
    .bind(chain_id)
    .bind(contract_kind)
    .bind(&provenance)
    .execute(&mut **transaction)
    .await
    .with_context(|| format!("failed to persist declared contract {chain_id}:{address}"))?;
    if !admit_address {
        return Ok(instance);
    }
    let omitted_start = start_block.is_none();
    let start_block = start_block
        .map(i64::try_from)
        .transpose()
        .with_context(|| format!("start block for {chain_id}:{address} exceeds BIGINT"))?;
    if let Some(existing) = existing.as_ref().filter(|existing| existing.is_active) {
        let repairs_legacy_floor = omitted_start
            && existing.active_from.is_some_and(|start| start > 0)
            && existing.prior_epoch_end.is_none();
        let start_block =
            bounded_epoch_start(start_block, existing.prior_epoch_end).with_context(|| {
                format!("failed to bound declared contract epoch {chain_id}:{address}")
            })?;
        sqlx::query(
            "
            UPDATE contract_instance_addresses
            SET active_from_block_number = CASE
                    -- Runtime interval readers treat either omitted bound as block zero. Persist
                    -- that floor so refresh order cannot leave a narrower finite address range.
                    WHEN active_from_block_number IS NULL OR $2::bigint IS NULL THEN 0
                    ELSE LEAST(active_from_block_number, $2)
                END,
                active_from_block_hash = NULL,
                active_to_block_number = NULL,
                active_to_block_hash = NULL,
                source_manifest_id = $3,
                deactivated_at = NULL,
                provenance = $4
            WHERE contract_instance_address_id = $1
            ",
        )
        .bind(existing.row_id)
        .bind(start_block)
        .bind(manifest_id)
        .bind(&provenance)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to refresh declared contract {chain_id}:{address}"))?;
        if repairs_legacy_floor {
            super::sync_state::stamp_required_ingest(transaction, chain_id, 0).await?;
            repaired_floor_chains.insert(chain_id.to_owned());
        }
    } else if let Some(previous_to) = existing.as_ref().and_then(|existing| existing.active_to) {
        let next_from = previous_to.checked_add(1).ok_or_else(|| {
            anyhow::anyhow!(
                "cannot append a declared address epoch for {chain_id}:{address}: prior end block \
                 overflowed"
            )
        })?;
        let next_from = start_block.map_or(next_from, |start| start.max(next_from));
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, source_manifest_id, provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ",
        )
        .bind(instance)
        .bind(chain_id)
        .bind(address)
        .bind(next_from)
        .bind(manifest_id)
        .bind(&provenance)
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to append declared contract address epoch {chain_id}:{address}")
        })?;
    } else if existing.is_some() {
        bail!(
            "cannot append a declared address epoch for {chain_id}:{address}: the inactive prior \
             row has no ending block"
        );
    } else {
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, source_manifest_id, provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ",
        )
        .bind(instance)
        .bind(chain_id)
        .bind(address)
        .bind(start_block)
        .bind(manifest_id)
        .bind(&provenance)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to admit declared contract {chain_id}:{address}"))?;
    }
    Ok(instance)
}

fn declared_contract_id(chain_id: &str, address: &str) -> Uuid {
    let digest =
        keccak256(format!("contract:{chain_id}:{}", address.to_ascii_lowercase()).as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn bounded_epoch_start(
    start_block: Option<i64>,
    prior_epoch_end: Option<i64>,
) -> Result<Option<i64>> {
    let Some(prior_epoch_end) = prior_epoch_end else {
        return Ok(start_block);
    };
    let floor = prior_epoch_end
        .checked_add(1)
        .context("prior declared address epoch end overflowed")?;
    Ok(Some(start_block.map_or(floor, |start| start.max(floor))))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_declaration(
    transaction: &mut Transaction<'_, Postgres>,
    manifest_id: i64,
    chain_id: &str,
    declaration_kind: &str,
    declaration_name: &str,
    instance: Uuid,
    address: &str,
    abi_ref: Option<&str>,
    role: Option<&str>,
    proxy_kind: &str,
    implementation_id: Option<Uuid>,
    implementation_address: Option<&str>,
    start_block: Option<u64>,
) -> Result<()> {
    let start_block = start_block
        .map(i64::try_from)
        .transpose()
        .context("manifest start block exceeds BIGINT")?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, abi_ref, role, proxy_kind,
            implementation_contract_instance_id, declared_implementation_address,
            start_block_number
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(declaration_kind)
    .bind(declaration_name)
    .bind(instance)
    .bind(address)
    .bind(abi_ref)
    .bind(role)
    .bind(proxy_kind)
    .bind(implementation_id)
    .bind(implementation_address)
    .bind(start_block)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to persist manifest declaration {declaration_kind}:{declaration_name}")
    })?;
    Ok(())
}

pub(super) async fn deactivate_retired_manifest_addresses(
    transaction: &mut Transaction<'_, Postgres>,
    previous_declarations: &[ManifestAddressDeclaration],
) -> Result<()> {
    let chains = previous_declarations
        .iter()
        .map(|row| row.chain_id.clone())
        .collect::<Vec<_>>();
    let instances = previous_declarations
        .iter()
        .map(|row| row.instance_id)
        .collect::<Vec<_>>();
    let addresses = previous_declarations
        .iter()
        .map(|row| row.address.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, RetiredManifestAddress>(
        "
        WITH previous AS (
            SELECT *
            FROM unnest($1::text[], $2::uuid[], $3::text[])
                 AS prior(chain_id, instance_id, address)
        )
        SELECT address.contract_instance_address_id AS row_id,
               address.contract_instance_id AS instance_id,
               address.chain_id,
               address.address,
               address.active_from_block_number AS active_from,
               address.active_to_block_number AS active_to,
               address.active_to_block_hash AS active_to_hash,
               head.latest_block_number AS head_number,
               head.latest_block_hash AS head_hash
        FROM contract_instance_addresses address
        LEFT JOIN chain_heads head
          ON head.chain_id = address.chain_id
        LEFT JOIN previous prior
          ON prior.chain_id = address.chain_id
         AND prior.instance_id = address.contract_instance_id
         AND prior.address = lower(address.address)
        WHERE address.deactivated_at IS NULL
          AND (
              address.provenance ->> 'source' IN (
                  'manifest_declaration',
                  'manifest_proxy_implementation'
              )
              OR prior.chain_id IS NOT NULL
          )
          AND NOT EXISTS (
              SELECT 1
              FROM manifest_contract_instances declaration
              JOIN manifest_versions manifest
                ON manifest.manifest_id = declaration.manifest_id
               AND manifest.chain_id = declaration.chain_id
              WHERE manifest.rollout_status = 'active'
                AND declaration.chain_id = address.chain_id
                AND (
                    (
                        declaration.contract_instance_id = address.contract_instance_id
                        AND lower(declaration.declared_address) = lower(address.address)
                    )
                    OR (
                        declaration.implementation_contract_instance_id =
                            address.contract_instance_id
                        AND lower(declaration.declared_implementation_address) =
                            lower(address.address)
                    )
                )
          )
        ORDER BY address.chain_id, address.contract_instance_address_id
        ",
    )
    .bind(chains)
    .bind(instances)
    .bind(addresses)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load retired manifest address rows")?;

    for row in rows {
        let previous = previous_declarations.iter().find(|declaration| {
            declaration.chain_id == row.chain_id
                && declaration.instance_id == row.instance_id
                && declaration.address.eq_ignore_ascii_case(&row.address)
        });
        let (Some(head_number), Some(head_hash)) = (row.head_number, row.head_hash.as_deref())
        else {
            bail!(
                "cannot retire manifest-declared address {}:{} without a recorded chain head",
                row.chain_id,
                row.address
            );
        };
        let close_number = row
            .active_to
            .map_or(head_number, |end| end.min(head_number));
        if row.active_from.is_some_and(|start| close_number < start) {
            bail!(
                "cannot retire manifest-declared address {}:{} at head {head_number} before its \
                 active start {}",
                row.chain_id,
                row.address,
                row.active_from.expect("checked as present")
            );
        }
        let close_hash = if row.active_to.is_some() {
            row.active_to_hash.as_deref()
        } else {
            Some(head_hash)
        };
        sqlx::query(
            "
            UPDATE contract_instance_addresses
            SET active_to_block_number = $2,
                active_to_block_hash = $3,
                source_manifest_id = COALESCE($4, source_manifest_id),
                provenance = COALESCE($5, provenance),
                deactivated_at = now()
            WHERE contract_instance_address_id = $1
              AND deactivated_at IS NULL
            ",
        )
        .bind(row.row_id)
        .bind(close_number)
        .bind(close_hash)
        .bind(previous.map(|declaration| declaration.manifest_id))
        .bind(previous.map(|declaration| &declaration.provenance))
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to retire manifest-declared address {}:{}",
                row.chain_id, row.address
            )
        })?;
    }
    Ok(())
}

pub(super) async fn repair_retired_omitted_admission_floors(
    transaction: &mut Transaction<'_, Postgres>,
    omitted_addresses: &BTreeSet<(String, String)>,
    repaired_floor_chains: &mut HashSet<String>,
) -> Result<()> {
    if omitted_addresses.is_empty() {
        return Ok(());
    }
    let (chains, addresses): (Vec<_>, Vec<_>) = omitted_addresses.iter().cloned().unzip();
    let repaired: Vec<String> = sqlx::query_scalar(
        "WITH target AS ( \
             SELECT chain_id, address FROM unnest($1::text[], $2::text[]) target(chain_id, address) \
         ), earliest AS ( \
             SELECT DISTINCT ON (admission.chain_id, lower(admission.address)) \
                    admission.contract_instance_address_id \
             FROM contract_instance_addresses admission \
             JOIN target ON target.chain_id = admission.chain_id \
                        AND target.address = lower(admission.address) \
             WHERE admission.deactivated_at IS NOT NULL \
               AND admission.active_to_block_number IS NOT NULL \
             ORDER BY admission.chain_id, lower(admission.address), \
                      admission.contract_instance_address_id \
         ) \
         UPDATE contract_instance_addresses admission \
         SET active_from_block_number = 0, active_from_block_hash = NULL \
         FROM earliest \
         WHERE admission.contract_instance_address_id = earliest.contract_instance_address_id \
           AND admission.active_from_block_number > 0 \
         RETURNING admission.chain_id",
    )
    .bind(chains)
    .bind(addresses)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to repair retired omitted-start admission floors")?;
    for chain_id in repaired.into_iter().collect::<BTreeSet<_>>() {
        super::sync_state::stamp_required_ingest(transaction, &chain_id, 0).await?;
        repaired_floor_chains.insert(chain_id);
    }
    Ok(())
}

pub(super) async fn retained_admission_manifests(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<SourceManifest>> {
    let payloads: Vec<Value> = sqlx::query_scalar(
        "SELECT manifest_payload FROM manifest_versions WHERE rollout_status = 'active' UNION ALL SELECT before_state -> 'manifest_payload' FROM normalized_events WHERE event_kind = 'SourceManifestUpdated' AND derivation_kind = 'manifest_sync' AND before_state ->> 'rollout_status' = 'active' AND before_state ? 'manifest_payload' UNION ALL SELECT after_state -> 'manifest_payload' FROM normalized_events WHERE event_kind = 'SourceManifestUpdated' AND derivation_kind = 'manifest_sync' AND after_state ->> 'rollout_status' = 'active' AND after_state ? 'manifest_payload'",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load retained manifest admission history")?;
    payloads
        .into_iter()
        .map(|payload| {
            serde_json::from_value::<SourceManifest>(payload)
                .context("failed to decode retained manifest admission history")
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn reopen_proxy_edge(
    transaction: &mut Transaction<'_, Postgres>,
    manifest_id: i64,
    chain_id: &str,
    role: &str,
    proxy_id: Uuid,
    implementation_id: Uuid,
    proxy_address: &str,
    implementation_address: &str,
) -> Result<()> {
    let observation_key = format!(
        "proxy-implementation:{}",
        proxy_address.to_ascii_lowercase()
    );
    let upgraded_edge_is_current: bool = sqlx::query_scalar(
        "
        SELECT EXISTS (
            SELECT 1
            FROM discovery_edges
            WHERE chain_id = $1
              AND edge_kind = 'proxy_implementation'
              AND from_contract_instance_id = $2
              AND provenance ->> 'observation_key' = $3
              AND discovery_source = 'Upgraded'
              AND deactivated_at IS NULL
        )
        ",
    )
    .bind(chain_id)
    .bind(proxy_id)
    .bind(&observation_key)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to check current proxy-upgrade edge")?;
    if upgraded_edge_is_current {
        return Ok(());
    }
    let provenance = json!({
        "source": "manifest",
        "source_event": "ManifestLoaded",
        "observation_key": observation_key,
        "proxy_role": role,
        "proxy_address": proxy_address,
        "implementation_address": implementation_address,
    });
    let reopened = sqlx::query(
        "
        UPDATE discovery_edges
        SET to_contract_instance_id = $4,
            canonicality_state = 'finalized',
            deactivated_at = NULL,
            provenance = $6
        WHERE chain_id = $1
          AND edge_kind = 'proxy_implementation'
          AND from_contract_instance_id = $3
          AND source_manifest_id = $2
          AND discovery_source = 'manifest_declared_proxy'
          AND provenance ->> 'observation_key' = $5
        ",
    )
    .bind(chain_id)
    .bind(manifest_id)
    .bind(proxy_id)
    .bind(implementation_id)
    .bind(&observation_key)
    .bind(&provenance)
    .execute(&mut **transaction)
    .await
    .context("failed to reopen manifest proxy edge")?;
    if reopened.rows_affected() == 0 {
        sqlx::query(
            "
            INSERT INTO discovery_edges (
                chain_id, edge_kind, from_contract_instance_id,
                to_contract_instance_id, discovery_source, admission_basis,
                source_manifest_id, canonicality_state, provenance
            )
            VALUES (
                $1, 'proxy_implementation', $2, $3,
                'manifest_declared_proxy', 'manifest_declared', $4,
                'finalized', $5
            )
            ",
        )
        .bind(chain_id)
        .bind(proxy_id)
        .bind(implementation_id)
        .bind(manifest_id)
        .bind(provenance)
        .execute(&mut **transaction)
        .await
        .context("failed to insert manifest proxy edge")?;
    }
    Ok(())
}

pub(super) fn validate_proxy_shape(
    loaded: &LoadedManifest,
    contract: &ManifestContract,
) -> Result<()> {
    match (
        contract.proxy_kind.as_str(),
        contract.implementation.as_ref(),
    ) {
        ("none", Some(_)) => bail!(
            "manifest contract {} in {} declares an implementation with proxy_kind none",
            contract.role,
            loaded.path.display()
        ),
        ("none", None) | (_, Some(_)) => Ok(()),
        (proxy_kind, None) => bail!(
            "manifest contract {} in {} omits its {proxy_kind} implementation",
            contract.role,
            loaded.path.display()
        ),
    }
}

pub(super) fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}
