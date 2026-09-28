use std::{str::FromStr, sync::atomic::{AtomicU64, Ordering}};

use anyhow::Context;
use axum::{
    body::{Body, to_bytes},
    http::Request,
    response::Response,
};
use bigname_storage::{
    CanonicalityState, NameSurface, NormalizedEvent, PermissionScope, PermissionsCurrentRow,
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    ResolverCurrentRow, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage,
    default_database_url, load_primary_name_current, parse_rfc3339_utc_timestamp,
};
use bigname_test_support::TestDatabaseConfig;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    raw_sql,
    types::{Uuid, time::OffsetDateTime},
};
use tower::ServiceExt;

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawBlock {
    chain_id: String,
    block_hash: String,
    parent_hash: Option<String>,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    logs_bloom: Option<Vec<u8>>,
    transactions_root: Option<String>,
    receipts_root: Option<String>,
    state_root: Option<String>,
    canonicality_state: CanonicalityState,
}

fn phase_support_from_coverage(coverage: &Value) -> (&'static str, Option<String>) {
    if coverage.get("status").and_then(Value::as_str) == Some("unsupported") {
        let reason = coverage
            .get("unsupported_reason")
            .and_then(Value::as_str)
            .unwrap_or("unsupported")
            .to_owned();
        ("unsupported", Some(reason))
    } else {
        ("supported", None)
    }
}

fn phase_logical_identity(namespace: &str, name: &str) -> Result<(String, String)> {
    let namehash = bigname_lookup::ens_namehash_hex(name)?;
    Ok((format!("{namespace}:{namehash}"), namehash))
}

async fn upsert_phase_raw_blocks(pool: &PgPool, rows: &[RawBlock]) -> Result<Vec<RawBlock>> {
    for row in rows {
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6::bigname_phase.canonicality_state)
            ON CONFLICT (chain_id, block_hash) DO NOTHING
            "#,
        )
        .bind(&row.chain_id)
        .bind(&row.block_hash)
        .bind(&row.parent_hash)
        .bind(row.block_number)
        .bind(row.block_timestamp)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(rows.to_vec())
}

async fn upsert_phase_name_current_rows(
    pool: &PgPool,
    rows: &[bigname_storage::NameCurrentRow],
) -> Result<Vec<bigname_storage::NameCurrentRow>> {
    for row in rows {
        let (support_status, mut unsupported_reason) = phase_support_from_coverage(&row.coverage);
        if unsupported_reason.as_deref() == Some("unsupported") {
            unsupported_reason = Some("name_coverage_unsupported_reason_missing".to_owned());
        }
        let phase_identity: Option<(String, String)> = sqlx::query_as(
            "SELECT logical_name_id, namehash FROM bigname_phase.name_surfaces
             WHERE namespace = $1 AND raw_name = $2
             ORDER BY logical_name_id
             LIMIT 1",
        )
        .bind(&row.namespace)
        .bind(&row.normalized_name)
        .fetch_optional(pool)
        .await?;
        let (logical_name_id, namehash) = match phase_identity {
            Some(identity) => identity,
            None => phase_logical_identity(&row.namespace, &row.normalized_name)?,
        };
        let chain_id = phase_projection_source_position(&row.chain_positions)?
            .get("chain_id")
            .and_then(Value::as_str)
            .context("name_current fixture position must include chain_id")?
            .to_owned();
        let (target_block_number, target_block_hash) =
            phase_projection_target_for_chain(pool, &chain_id, &row.chain_positions).await?;
        let mut provenance = row.provenance.clone();
        provenance
            .as_object_mut()
            .context("name_current fixture provenance must be an object")?
            .insert("chain_id".to_owned(), json!(chain_id));
        let chain_positions = align_phase_chain_positions(pool, &row.chain_positions).await?;
        let canonicality_summary = json!({
            "state": "canonical_lineage",
            "target_block_number": target_block_number,
            "target_block_hash": target_block_hash,
        });
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.name_current (
                logical_name_id, namespace, raw_name, namehash, surface_binding_id,
                resource_id, serving_resource_id, token_lineage_id, binding_kind, declared_summary,
                support_status, unsupported_reason, provenance, chain_positions,
                canonicality_summary, manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (logical_name_id) DO UPDATE SET
                raw_name = EXCLUDED.raw_name,
                surface_binding_id = EXCLUDED.surface_binding_id,
                resource_id = EXCLUDED.resource_id,
                serving_resource_id = EXCLUDED.serving_resource_id,
                token_lineage_id = EXCLUDED.token_lineage_id,
                binding_kind = EXCLUDED.binding_kind,
                declared_summary = EXCLUDED.declared_summary,
                support_status = EXCLUDED.support_status,
                unsupported_reason = EXCLUDED.unsupported_reason,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = EXCLUDED.last_recomputed_at
            "#,
        )
        .bind(logical_name_id)
        .bind(&row.namespace)
        .bind(&row.normalized_name)
        .bind(namehash)
        .bind(row.surface_binding_id)
        .bind(row.resource_id)
        .bind(row.serving_resource_id)
        .bind(row.token_lineage_id)
        .bind(row.binding_kind.map(|value| value.as_str()))
        .bind(&row.declared_summary)
        .bind(support_status)
        .bind(unsupported_reason)
        .bind(provenance)
        .bind(chain_positions)
        .bind(canonicality_summary)
        .bind(row.manifest_version)
        .bind(row.last_recomputed_at)
        .execute(pool)
        .await?;
    }
    Ok(rows.to_vec())
}

async fn upsert_phase_record_inventory_current_rows(
    pool: &PgPool,
    rows: &[bigname_storage::RecordInventoryCurrentRow],
) -> Result<Vec<bigname_storage::RecordInventoryCurrentRow>> {
    for row in rows {
        let mut record_version_boundary = row.record_version_boundary.clone();
        if let Some(logical_name_id) = record_version_boundary
            .get("logical_name_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            && let Some((namespace, name_or_hash)) = logical_name_id.split_once(':')
            && !(name_or_hash.starts_with("0x") && name_or_hash.len() == 66)
        {
            record_version_boundary["logical_name_id"] =
                json!(bigname_storage::logical_name_id_for_name(namespace, name_or_hash));
        }
        let projected_chain_positions: Option<Value> = sqlx::query_scalar(
            "SELECT chain_positions FROM bigname_phase.name_current
             WHERE COALESCE(serving_resource_id, resource_id) = $1
             ORDER BY last_recomputed_at DESC
             LIMIT 1",
        )
        .bind(row.resource_id)
        .fetch_optional(pool)
        .await?;
        if let Some(projected) = projected_chain_positions.as_ref()
            && let Some(positions) = projected.as_object()
        {
            let boundary_chain_id = record_version_boundary
                .pointer("/chain_position/chain_id")
                .and_then(Value::as_str);
            let position = positions
                .values()
                .find(|position| {
                    position.get("chain_id").and_then(Value::as_str) == boundary_chain_id
                })
                .or_else(|| (positions.len() == 1).then(|| positions.values().next()).flatten());
            if let Some(position) = position {
                record_version_boundary["chain_position"] = position.clone();
            }
        }
        let requested_chain_positions =
            align_phase_chain_positions(pool, &row.chain_positions).await?;
        let snapshot_positions = if requested_chain_positions
            .as_object()
            .is_some_and(|positions| !positions.is_empty())
        {
            requested_chain_positions
        } else {
            projected_chain_positions.unwrap_or_else(|| json!({}))
        };
        let chain_id = record_version_boundary
            .pointer("/chain_position/chain_id")
            .and_then(Value::as_str)
            .context("record inventory boundary is missing chain_position.chain_id")?;
        let target = snapshot_positions
            .as_object()
            .into_iter()
            .flat_map(|positions| positions.values())
            .find(|position| position.get("chain_id").and_then(Value::as_str) == Some(chain_id))
            .context("record inventory snapshot is missing its boundary chain position")?;
        let (target_block_number, target_block_hash) =
            phase_projection_target_for_chain(pool, chain_id, target).await?;
        let chain_positions = json!({
            "block_number": target_block_number,
            "block_hash": target_block_hash,
            "target_block_number": target_block_number,
            "target_block_hash": target_block_hash,
        });
        let mut provenance = row.provenance.clone();
        provenance
            .as_object_mut()
            .context("record inventory fixture provenance must be an object")?
            .insert("chain_id".to_owned(), json!(chain_id));
        let canonicality_summary = json!({
            "state": "canonical_lineage",
            "target_block_number": target_block_number,
            "target_block_hash": target_block_hash,
        });
        let boundary_key = bigname_storage::record_version_boundary_storage_key(
            &record_version_boundary,
            row.resource_id,
        )?;
        let (support_status, unsupported_reason) = phase_support_from_coverage(&row.coverage);
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.record_inventory_current (
                resource_id, record_version_boundary_key, record_version_boundary,
                selectors, unsupported_families, last_change, entries, support_status,
                unsupported_reason, provenance, chain_positions, canonicality_summary,
                manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (resource_id, record_version_boundary_key) DO UPDATE SET
                record_version_boundary = EXCLUDED.record_version_boundary,
                selectors = EXCLUDED.selectors,
                unsupported_families = EXCLUDED.unsupported_familIES,
                last_change = EXCLUDED.last_change,
                entries = EXCLUDED.entries,
                support_status = EXCLUDED.support_status,
                unsupported_reason = EXCLUDED.unsupported_reason,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = EXCLUDED.last_recomputed_at
            "#,
        )
        .bind(row.resource_id)
        .bind(boundary_key)
        .bind(record_version_boundary)
        .bind(&row.selectors)
        .bind(&row.unsupported_families)
        .bind(&row.last_change)
        .bind(&row.entries)
        .bind(support_status)
        .bind(unsupported_reason)
        .bind(provenance)
        .bind(chain_positions)
        .bind(canonicality_summary)
        .bind(row.manifest_version)
        .bind(row.last_recomputed_at)
        .execute(pool)
        .await?;
    }
    Ok(rows.to_vec())
}

fn phase_chain_positions(value: &Value) -> Value {
    let Some(positions) = value.as_object() else {
        return value.clone();
    };
    Value::Object(
        positions
            .values()
            .filter_map(|position| {
                let chain_id = position.get("chain_id")?.as_str()?;
                let slot = match chain_id {
                    "ethereum-mainnet" => "ethereum",
                    "ethereum-sepolia" => "ethereum-sepolia",
                    "base-mainnet" => "base",
                    _ => chain_id,
                };
                Some((slot.to_owned(), position.clone()))
            })
            .collect(),
    )
}

async fn align_phase_chain_positions(pool: &PgPool, value: &Value) -> Result<Value> {
    let mut aligned = phase_chain_positions(value);
    let Some(positions) = aligned.as_object_mut() else {
        return Ok(aligned);
    };
    for position in positions.values_mut() {
        let Some(chain_id) = position.get("chain_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(block_number) = position.get("block_number").and_then(Value::as_i64) else {
            continue;
        };
        let readable: Option<(String, String)> = sqlx::query_as(
            r#"
            SELECT block_hash,
                   to_char(block_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            FROM bigname_phase.chain_lineage
            WHERE chain_id = $1 AND block_number = $2
              AND canonicality_state IN ('canonical', 'safe', 'finalized')
            LIMIT 1
            "#,
        )
        .bind(chain_id)
        .bind(block_number)
        .fetch_optional(pool)
        .await?;
        if let Some((block_hash, timestamp)) = readable {
            position["block_hash"] = json!(block_hash);
            position["timestamp"] = json!(timestamp);
        }
    }
    Ok(aligned)
}

fn phase_projection_source_position(value: &Value) -> Result<&Value> {
    if value.get("block_number").is_some() {
        Ok(value)
    } else {
        value
            .as_object()
            .and_then(|positions| positions.values().next())
            .context("projection fixture requires one source chain position")
    }
}

fn phase_flat_projection_position(block_number: i64, block_hash: &str) -> Value {
    json!({
        "block_number": block_number,
        "block_hash": block_hash,
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    })
}

async fn upsert_phase_address_names_current_rows(
    pool: &PgPool,
    rows: &[bigname_storage::AddressNameCurrentRow],
) -> Result<Vec<bigname_storage::AddressNameCurrentRow>> {
    for row in rows {
        let (support_status, unsupported_reason) = phase_support_from_coverage(&row.coverage);
        let normalized_name = bigname_domain::normalization::normalize_name(
            &row.canonical_display_name,
        )
        .map_err(|error| anyhow::anyhow!(error.message().to_owned()))?
        .normalized_name;
        let (logical_name_id, namehash) =
            phase_logical_identity(&row.namespace, &normalized_name)?;
        let chain_positions: Option<Value> = sqlx::query_scalar(
            "SELECT chain_positions FROM bigname_phase.name_current
             WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_optional(pool)
        .await?;
        let chain_positions = match chain_positions {
            Some(chain_positions) => chain_positions,
            None => align_phase_chain_positions(pool, &row.chain_positions).await?,
        };
        let chain_id = phase_projection_source_position(&chain_positions)?
            .get("chain_id")
            .and_then(Value::as_str)
            .context("address_names_current fixture position must include chain_id")?
            .to_owned();
        let (target_block_number, target_block_hash) =
            phase_projection_target_for_chain(pool, &chain_id, &chain_positions).await?;
        let mut provenance = row.provenance.clone();
        provenance
            .as_object_mut()
            .context("address_names_current fixture provenance must be an object")?
            .insert("chain_id".to_owned(), json!(chain_id));
        let chain_positions =
            phase_flat_projection_position(target_block_number, &target_block_hash);
        let canonicality_summary = json!({
            "state": "canonical_lineage",
            "target_block_number": target_block_number,
            "target_block_hash": target_block_hash,
        });
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.address_names_current (
                address, logical_name_id, relation, namespace, raw_name, namehash,
                surface_binding_id, resource_id, token_lineage_id, binding_kind,
                support_status, unsupported_reason, provenance, chain_positions,
                canonicality_summary, manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (address, logical_name_id, relation) DO UPDATE SET
                raw_name = EXCLUDED.raw_name,
                support_status = EXCLUDED.support_status,
                unsupported_reason = EXCLUDED.unsupported_reason,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = EXCLUDED.last_recomputed_at
            "#,
        )
        .bind(row.address.to_ascii_lowercase())
        .bind(logical_name_id)
        .bind(row.relation.as_str())
        .bind(&row.namespace)
        .bind(&normalized_name)
        .bind(namehash)
        .bind(row.surface_binding_id)
        .bind(row.resource_id)
        .bind(row.token_lineage_id)
        .bind(row.binding_kind.as_str())
        .bind(support_status)
        .bind(unsupported_reason)
        .bind(provenance)
        .bind(chain_positions)
        .bind(canonicality_summary)
        .bind(row.manifest_version)
        .bind(row.last_recomputed_at)
        .execute(pool)
        .await?;
    }
    Ok(rows.to_vec())
}

async fn upsert_phase_permissions_current_rows(
    pool: &PgPool,
    rows: &[PermissionsCurrentRow],
) -> Result<Vec<PermissionsCurrentRow>> {
    for row in rows {
        let (chain_id, block_number, block_hash) =
            phase_permission_projection_target(pool, row.resource_id, &row.chain_positions).await?;
        let transfer_behavior = row
            .transfer_behavior
            .as_object()
            .map(|value| Value::Object(value.clone()))
            .unwrap_or_else(|| json!({}));
        let mut provenance = row.provenance.clone();
        provenance
            .as_object_mut()
            .context("permission provenance must be an object")?
            .insert("chain_id".to_owned(), json!(chain_id));
        let chain_positions = json!({
            "block_number": block_number,
            "block_hash": block_hash,
            "target_block_number": block_number,
            "target_block_hash": block_hash,
        });
        let canonicality_summary = json!({
            "state": "canonical",
            "target_block_number": block_number,
            "target_block_hash": block_hash,
        });
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.permissions_current (
                resource_id, subject, scope, scope_kind, scope_detail,
                effective_powers, grant_source, revocation_source, inheritance_path,
                transfer_behavior, provenance, chain_positions, canonicality_summary,
                manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (resource_id, subject, scope) DO UPDATE SET
                effective_powers = EXCLUDED.effective_powers,
                grant_source = EXCLUDED.grant_source,
                revocation_source = EXCLUDED.revocation_source,
                inheritance_path = EXCLUDED.inheritance_path,
                transfer_behavior = EXCLUDED.transfer_behavior,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = EXCLUDED.last_recomputed_at
            "#,
        )
        .bind(row.resource_id)
        .bind(row.subject.to_ascii_lowercase())
        .bind(row.scope.storage_key())
        .bind(row.scope.kind())
        .bind(match &row.record_resource_selector {
            Some(selector) => {
                let mut detail = row.scope.detail();
                detail["resource_selector"] = selector.clone();
                detail
            }
            None => row.scope.detail(),
        })
        .bind(&row.effective_powers)
        .bind(&row.grant_source)
        .bind(&row.revocation_source)
        .bind(&row.inheritance_path)
        .bind(transfer_behavior)
        .bind(provenance)
        .bind(chain_positions)
        .bind(canonicality_summary)
        .bind(row.manifest_version)
        .bind(row.last_recomputed_at)
        .execute(pool)
        .await?;
    }
    Ok(rows.to_vec())
}

/// Authority kinds whose known owner-derived permission rows omit standard approval paths.
/// ENSv1 registry operators, registrar approvals, and resolver operators/delegates can authorize
/// mutation. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114-L129 @ ens_v1@91c966f)
const PHASE_PROJECTED_PERMISSION_AUTHORITY_KINDS: &[&str] = &[
    "registrar",
    "registry",
    "registry_only",
    "registry_owner",
    "registrant",
    "resolver",
    "ens_v2_registry",
];

/// Mirror `crates/project/src/builders/permissions.rs`: the projected support columns come from
/// the resource's authority kind, not from the coverage the reader synthesizes back out of them.
/// Deriving them from the fixture's coverage instead would keep the unknown-authority state that
/// production writes out of the typed read path.
fn phase_permission_summary_support(
    authority_kind: Option<&str>,
) -> (&'static str, Option<&'static str>) {
    match authority_kind {
        Some(kind) if PHASE_PROJECTED_PERMISSION_AUTHORITY_KINDS.contains(&kind) => {
            (
                "unsupported",
                Some("operator_approval_surfaces_not_ingested"),
            )
        }
        Some("wrapper") => (
            "unsupported",
            Some("wrapper_parent_and_resolver_delegation_not_projected"),
        ),
        _ => (
            "unsupported",
            Some("resource_permission_authority_not_projected"),
        ),
    }
}

async fn upsert_phase_permissions_current_resource_summary(
    pool: &PgPool,
    row: &bigname_storage::PermissionsCurrentResourceSummary,
) -> Result<()> {
    let (support_status, unsupported_reason) =
        phase_permission_summary_support(row.authority_kind.as_deref());
    let (chain_id, block_number, block_hash) =
        phase_permission_projection_target(pool, row.resource_id, &row.chain_positions).await?;
    let mut provenance = row.provenance.clone();
    provenance
        .as_object_mut()
        .context("permission summary provenance must be an object")?
        .insert("chain_id".to_owned(), json!(chain_id));
    let chain_positions = json!({
        "block_number": block_number,
        "block_hash": block_hash,
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    });
    let canonicality_summary = json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    });
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.permissions_current_resource_summary (
            resource_id, authority_kind, root_resource_id, support_status,
            unsupported_reason, provenance, chain_positions, canonicality_summary,
            manifest_version, last_recomputed_at, resource_restrictions
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (resource_id) DO UPDATE SET
            authority_kind = EXCLUDED.authority_kind,
            root_resource_id = EXCLUDED.root_resource_id,
            resource_restrictions = EXCLUDED.resource_restrictions,
            support_status = EXCLUDED.support_status,
            unsupported_reason = EXCLUDED.unsupported_reason,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        "#,
    )
    .bind(row.resource_id)
    .bind(&row.authority_kind)
    .bind(row.root_resource_id)
    .bind(support_status)
    .bind(unsupported_reason)
    .bind(provenance)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .bind(&row.resource_restrictions)
    .execute(pool)
    .await?;
    Ok(())
}

async fn phase_permission_projection_target(
    pool: &PgPool,
    resource_id: Uuid,
    source_positions: &Value,
) -> Result<(String, i64, String)> {
    let chain_id: String = sqlx::query_scalar(
        "SELECT chain_id FROM bigname_phase.resources WHERE resource_id = $1",
    )
    .bind(resource_id)
    .fetch_one(pool)
    .await?;
    let (block_number, block_hash) =
        phase_projection_target_for_chain(pool, &chain_id, source_positions).await?;
    Ok((chain_id, block_number, block_hash))
}

async fn phase_projection_target_for_chain(
    pool: &PgPool,
    chain_id: &str,
    source_positions: &Value,
) -> Result<(i64, String)> {
    let position = phase_projection_source_position(source_positions)?;
    let block_number = position
        .get("block_number")
        .and_then(Value::as_i64)
        .context("permission fixture source position requires block_number")?;
    let requested_block_hash = position
        .get("block_hash")
        .and_then(Value::as_str)
        .context("permission fixture source position requires block_hash")?;
    let timestamp = position
        .get("timestamp")
        .and_then(Value::as_str)
        .unwrap_or("2026-04-17T00:00:00Z");
    let existing_block_hash: Option<String> = sqlx::query_scalar(
        "SELECT block_hash FROM bigname_phase.chain_lineage \
         WHERE chain_id = $1 AND block_number = $2 \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY block_hash LIMIT 1",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_optional(pool)
    .await?;
    let block_hash = existing_block_hash.unwrap_or_else(|| requested_block_hash.to_owned());
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage ( \
             chain_id, block_hash, block_number, block_timestamp, canonicality_state \
         ) VALUES ($1, $2, $3, $4::timestamptz, 'canonical') \
         ON CONFLICT (chain_id, block_hash) DO NOTHING",
    )
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .bind(timestamp)
    .execute(pool)
    .await?;
    Ok((block_number, block_hash))
}

async fn upsert_phase_resolver_current_rows(
    pool: &PgPool,
    rows: &[ResolverCurrentRow],
) -> Result<Vec<ResolverCurrentRow>> {
    for row in rows {
        let (support_status, unsupported_reason) = phase_support_from_coverage(&row.coverage);
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.resolver_current (
                chain_id, resolver_address, declared_summary, support_status,
                unsupported_reason, provenance, chain_positions, canonicality_summary,
                manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (chain_id, resolver_address) DO UPDATE SET
                declared_summary = EXCLUDED.declared_summary,
                support_status = EXCLUDED.support_status,
                unsupported_reason = EXCLUDED.unsupported_reason,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = EXCLUDED.last_recomputed_at
            "#,
        )
        .bind(&row.chain_id)
        .bind(row.resolver_address.to_ascii_lowercase())
        .bind(&row.declared_summary)
        .bind(support_status)
        .bind(unsupported_reason)
        .bind(&row.provenance)
        .bind(phase_chain_positions(&row.chain_positions))
        .bind(&row.canonicality_summary)
        .bind(row.manifest_version)
        .bind(row.last_recomputed_at)
        .execute(pool)
        .await?;
    }
    Ok(rows.to_vec())
}

struct TestDatabase {
    database: bigname_test_support::TestDatabase,
    pool: PgPool,
    lookup_pool: PgPool,
    database_name: String,
}

/// Add reverse inputs and let the real claim reducer classify their bytes.
async fn upsert_primary_name_current_rows(
    pool: &PgPool,
    rows: &[PrimaryNameCurrentRow],
) -> Result<()> {
    for row in rows {
        let provenance =
            phase_primary_claim_provenance(pool, &row.namespace, &row.claim_provenance).await?;
        let chain = provenance["chain_id"].as_str().context("claim chain")?;
        let block = provenance["target_block_number"]
            .as_i64()
            .context("claim block")?;
        let hash = provenance["target_block_hash"]
            .as_str()
            .context("claim hash")?;
        let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 * 2;
        let node = format!(
            "{:#x}",
            alloy_primitives::keccak256(
                format!(
                    "{}:{}:{}",
                    row.namespace,
                    row.coin_type,
                    row.address.to_ascii_lowercase()
                )
                .as_bytes()
            )
        );
        let source = json!({"address":row.address, "coin_type":row.coin_type,
            "namespace":row.namespace, "reverse_node":node, "claim_provenance":provenance});
        let mut reverse = history_event(
            &format!("claim-reverse-{ordinal}"),
            None,
            None,
            Some(chain),
            Some(block),
            Some(hash),
            Some("0xclaimfixture"),
            Some(ordinal),
            CanonicalityState::Canonical,
        );
        reverse.namespace = row.namespace.clone();
        reverse.event_kind = "ReverseChanged".into();
        reverse.source_family = if row.namespace == "basenames" {
            "basenames_base_reverse_registrar"
        } else {
            "ens_v1_reverse_registrar_l1"
        }
        .into();
        reverse.before_state = json!({});
        reverse.after_state = source.clone();
        reverse.after_state["source_event"] = json!("NameForAddrChanged");
        let mut claim = reverse.clone();
        claim.event_identity = format!("claim-name-{}", ordinal + 1);
        claim.log_index = Some(ordinal + 1);
        claim.event_kind = "RecordChanged".into();
        claim.source_family = if row.namespace == "basenames" {
            "basenames_base_resolver"
        } else {
            "ens_v1_resolver_l1"
        }
        .into();
        claim.after_state = json!({"source_event":"NameForAddrChanged", "node":node,
            "record_key":"name", "primary_claim_source":source});
        match row.claim_status {
            PrimaryNameClaimStatus::Success | PrimaryNameClaimStatus::InvalidName => {
                claim.after_state["raw_name"] = json!(row.raw_claim_name);
            }
            PrimaryNameClaimStatus::NotFound => {}
            PrimaryNameClaimStatus::Unsupported => {
                // An undecodable claim is the actual input of the unsupported outcome.
                claim.after_state["raw_name_bytes"] = json!("0xff");
            }
        }
        bigname_storage::insert_normalized_event_fixtures(pool, &[reverse, claim]).await?;
        rebuild_fixture_families(pool, chain, block, hash).await?;
        let produced =
            load_primary_name_current(pool, &row.address, &row.namespace, &row.coin_type)
                .await?
                .context("the reverse inputs produced a claim")?;
        anyhow::ensure!(
            produced.claim_status == row.claim_status,
            "claim fixture requests {:?}, but its raw input produces {:?}",
            row.claim_status,
            produced.claim_status
        );
    }
    Ok(())
}

/// Rebuild after a test adds retained inputs at its existing publication height. This uses
/// the ordinary family reducers and never writes a precomputed serving result or marker.
async fn rebuild_fixture_families(
    pool: &PgPool,
    chain: &str,
    block: i64,
    hash: &str,
) -> Result<()> {
    let token = bigname_project::families::input_token(pool, chain).await?;
    let outcome = bigname_project::families::apply(
        pool,
        chain,
        &bigname_project::Marker {
            number: block,
            hash: hash.to_owned(),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(block),
        "fixture family rebuild did not publish {chain} at {block}: {outcome:?}"
    );
    Ok(())
}

async fn upsert_primary_name_current_snapshots(
    pool: &PgPool,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<()> {
    for snapshot in snapshots {
        let mut row = snapshot.row.clone();
        if snapshot.claim_name_is_normalized {
            row.raw_claim_name = snapshot
                .normalized_claim_name
                .clone()
                .or(row.raw_claim_name);
        }
        upsert_primary_name_current_rows(pool, std::slice::from_ref(&row)).await?;
        let produced = bigname_storage::families::records::load_family_primary_name_snapshot(
            pool,
            &row.address,
            &row.namespace,
            &row.coin_type,
        )
        .await?
        .context("the reverse input produced its normalized claim")?;
        anyhow::ensure!(
            produced.claim_name_is_normalized == snapshot.claim_name_is_normalized,
            "claim fixture normalization flag must agree with its actual raw claim {:?}",
            row.raw_claim_name
        );
    }
    Ok(())
}

async fn phase_primary_claim_provenance(
    pool: &PgPool,
    namespace: &str,
    source: &Value,
) -> Result<Value> {
    let mut provenance = source.clone();
    let object = provenance
        .as_object_mut()
        .context("primary-name fixture provenance must be an object")?;
    let chain_id = object
        .get("chain_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if namespace == "basenames" {
                "base-mainnet".to_owned()
            } else {
                "ethereum-mainnet".to_owned()
            }
        });
    let requested_target = object
        .get("target_block_number")
        .and_then(Value::as_i64)
        .zip(object.get("target_block_hash").and_then(Value::as_str))
        .map(|(block_number, block_hash)| {
            json!({
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": "2026-04-17T00:00:00Z",
            })
        });
    let (block_number, block_hash) = match requested_target {
        Some(position) => phase_projection_target_for_chain(pool, &chain_id, &position).await?,
        None => sqlx::query_as(
            "SELECT block_number, block_hash FROM bigname_phase.chain_lineage \
             WHERE chain_id = $1 \
               AND canonicality_state IN ('canonical', 'safe', 'finalized') \
             ORDER BY block_number DESC, block_hash LIMIT 1",
        )
        .bind(&chain_id)
        .fetch_one(pool)
        .await?,
    };
    object.insert("chain_id".to_owned(), json!(chain_id));
    object.insert("target_block_number".to_owned(), json!(block_number));
    object.insert("target_block_hash".to_owned(), json!(block_hash));
    Ok(provenance)
}


impl TestDatabase {
    async fn new(initialize_manifest_schema: bool) -> Result<Self> {
        Self::new_with_schemas(initialize_manifest_schema, false).await
    }

    async fn new_with_schemas(
        _initialize_manifest_schema: bool,
        _initialize_name_current_schema: bool,
    ) -> Result<Self> {
        let database = bigname_test_support::TestDatabase::create(
            TestDatabaseConfig::new("bigname_api_test")
                .admin_database_from_url()
                .pool_max_connections(1)
                .parse_context("failed to parse database URL for API tests")
                .admin_connect_context("failed to connect admin pool for API tests")
                .pool_connect_context("failed to connect API test pool"),
        )
        .await?;
        let pool = database.pool().clone();
        let database_name = database.database_name().to_owned();

        let mut database = Self {
            database,
            lookup_pool: pool.clone(),
            pool,
            database_name,
        };
        database.initialize_lookup_schema().await?;
        database.lookup_pool = database.open_lookup_pool().await?;
        database.pool = database.lookup_pool.clone();
        Ok(database)
    }

    async fn new_migrated() -> Result<Self> {
        // API fixtures start from the current phase baseline. Historical public-schema
        // migrations belong to migration tests and cannot be replayed after that baseline.
        Self::new(false).await
    }

    async fn initialize_lookup_schema(&self) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS bigname_phase")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SET LOCAL search_path TO bigname_phase, public")
            .execute(&mut *transaction)
            .await?;
        for script in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
            include_str!("../../../../schema-v2/baseline/06_projections.sql"),
            include_str!("../../../../schema-v2/baseline/07_labels.sql"),
            include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
            include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
            include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
            include_str!("../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
            include_str!("../../../../schema-v2/baseline/12_project_generation_failures.sql"),
            include_str!("../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
            include_str!("../../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
        ] {
            raw_sql(script).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn lookup_pool(&self) -> Result<PgPool> {
        Ok(self.lookup_pool.clone())
    }

    async fn open_lookup_pool(&self) -> Result<PgPool> {
        let config = self.database_config(6)?;
        let options = PgConnectOptions::from_str(
            config
                .database_url
                .as_deref()
                .context("lookup test database URL is missing")?,
        )?
        .options([("search_path", "bigname_phase".to_owned())]);
        PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .context("failed to connect API lookup test pool")
    }

    async fn app_state_with_lookup_chain_rpc_urls(
        &self,
        chain_rpc_urls: bigname_lookup::ChainRpcUrls,
    ) -> Result<AppState> {
        Ok(AppState::new_with_rpc_urls(
            self.lookup_pool.clone(),
            chain_rpc_urls,
        )
        .with_public_namespaces_for_test(["ens", "basenames"]))
    }

    fn app_state(&self) -> AppState {
        AppState::new_with_rpc_urls(
            self.lookup_pool.clone(),
            bigname_lookup::ChainRpcUrls::default(),
        )
        .with_public_namespaces_for_test(["ens", "basenames"])
    }

    fn app_state_with_public_namespaces(&self, namespaces: &[&str]) -> AppState {
        AppState::new_with_rpc_urls(
            self.lookup_pool.clone(),
            bigname_lookup::ChainRpcUrls::default(),
        )
        .with_public_namespaces_for_test(namespaces.iter().copied())
    }

    fn database_config(&self, max_connections: u32) -> Result<bigname_storage::DatabaseConfig> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API pool configuration test")?
            .database(&self.database_name);
        Ok(bigname_storage::DatabaseConfig {
            database_url: Some(options.to_url_lossy().to_string()),
            max_connections,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        manifest_version: u64,
        rollout_status: &str,
        normalizer_version: &str,
    ) -> Result<i64> {
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let file_path =
            format!("tests/{namespace}/{source_family}/{manifest_version}-{sequence}.toml");

        sqlx::query(
            r#"
                INSERT INTO manifest_versions (
                    manifest_version,
                    namespace,
                    source_family,
                    chain_id,
                    deployment_label,
                    rollout_status,
                    normalizer_version,
                    file_path,
                    manifest_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING manifest_id
                "#,
        )
        .bind(i64::try_from(manifest_version).context("manifest_version exceeds BIGINT")?)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(normalizer_version)
        .bind(file_path)
        .bind(json!({
            "manifest_version": manifest_version,
            "namespace": namespace,
            "source_family": source_family,
            "chain": chain,
            "deployment_epoch": deployment_epoch,
            "rollout_status": rollout_status,
            "normalizer_version": normalizer_version,
            "capability_flags": {},
            "roots": [],
            "contracts": [],
            "discovery_rules": []
        }))
        .fetch_one(&self.pool)
        .await
        .context("failed to insert manifest_version for API test")?
        .try_get("manifest_id")
        .context("failed to read manifest_id for API test")
    }

    async fn insert_capability_flag(
        &self,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
        notes: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
                UPDATE bigname_phase.manifest_versions
                SET manifest_payload = jsonb_set(
                    manifest_payload,
                    ARRAY['capability_flags', $2],
                    jsonb_build_object('status', $3, 'notes', $4::text),
                    true
                )
                WHERE manifest_id = $1
                "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(status)
        .bind(notes)
        .execute(&self.pool)
        .await
        .context("failed to update phase manifest capability flag for API test")?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_name_current_binding(
        &self,
        logical_name_id: &str,
        namespace: &str,
        normalized_name: &str,
        canonical_display_name: &str,
        namehash: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        let chain_id = chain_id_for_namespace(namespace);
        upsert_test_name_surfaces(
            &self.pool,
            &[NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: namespace.to_owned(),
                input_name: normalized_name.to_owned(),
                canonical_display_name: canonical_display_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                dns_encoded_name: normalized_name.as_bytes().to_vec(),
                namehash: namehash.to_owned(),
                labelhashes: Vec::new(),
                normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: chain_id.to_owned(),
                block_hash: "0xsurface".to_owned(),
                block_number: 20_999_998,
                provenance: json!({"seed": "api_test"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_test_token_lineages(
            &self.pool,
            &[TokenLineage {
                token_lineage_id,
                chain_id: chain_id.to_owned(),
                block_hash: "0xlineage".to_owned(),
                block_number: 21_000_000,
                provenance: json!({"seed": "api_test"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_test_resources(
            &self.pool,
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: chain_id.to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 21_000_001,
                provenance: json!({"seed": "api_test"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_test_surface_bindings(
            &self.pool,
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                authority_arm: "ens_v1".to_owned(),
                active_from: timestamp(1_717_171_700),
                active_to: None,
                chain_id: chain_id.to_owned(),
                block_hash: "0xbinding".to_owned(),
                block_number: 21_000_003,
                provenance: json!({"seed": "api_test"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        Ok(())
    }

    async fn insert_name_current_row(
        &self,
        mut row: bigname_storage::NameCurrentRow,
    ) -> Result<()> {
        row.chain_positions = align_phase_chain_positions(&self.pool, &row.chain_positions).await?;
        self.seed_snapshot_selector_chain_positions(&row.chain_positions)
            .await?;
        upsert_phase_name_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert name_current row for API test")?;
        Ok(())
    }

    async fn insert_record_inventory_current_row(
        &self,
        row: bigname_storage::RecordInventoryCurrentRow,
    ) -> Result<()> {
        upsert_phase_record_inventory_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert record_inventory_current row for API test")?;
        Ok(())
    }

    async fn seed_snapshot_selector_chain_positions(&self, chain_positions: &Value) -> Result<()> {
        let Some(positions) = chain_positions.as_object() else {
            return Ok(());
        };

        for position in positions.values() {
            let chain_id = position
                .get("chain_id")
                .and_then(Value::as_str)
                .context("chain_position.chain_id must be present for API selector test seed")?;
            let block_hash = position
                .get("block_hash")
                .and_then(Value::as_str)
                .context("chain_position.block_hash must be present for API selector test seed")?;
            let block_number = position
                .get("block_number")
                .and_then(Value::as_i64)
                .context(
                    "chain_position.block_number must be present for API selector test seed",
                )?;
            let timestamp_value = position
                .get("timestamp")
                .and_then(Value::as_str)
                .context("chain_position.timestamp must be present for API selector test seed")?;
            let timestamp = parse_rfc3339_utc_timestamp(timestamp_value)
                .map_err(|error| anyhow::anyhow!("{error}"))?;

            sqlx::query(
                r#"
                INSERT INTO bigname_phase.chain_lineage (
                    chain_id,
                    block_hash,
                    block_number,
                    block_timestamp,
                    canonicality_state
                )
                VALUES ($1, $2, $3, $4, 'finalized'::bigname_phase.canonicality_state)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .bind(timestamp)
            .execute(&self.pool)
            .await
            .with_context(|| {
                format!("failed to seed chain_lineage for {chain_id} block {block_hash}")
            })?;

            sqlx::query(
                "UPDATE bigname_phase.chain_lineage
                 SET canonicality_state = 'canonical'
                 WHERE chain_id = $1 AND block_hash = $2
                   AND canonicality_state = 'observed'",
            )
            .bind(chain_id)
            .bind(block_hash)
            .execute(&self.lookup_pool)
            .await?;
            sqlx::query(
                "UPDATE bigname_phase.chain_lineage
                 SET canonicality_state = 'safe'
                 WHERE chain_id = $1 AND block_hash = $2
                   AND canonicality_state = 'canonical'",
            )
            .bind(chain_id)
            .bind(block_hash)
            .execute(&self.lookup_pool)
            .await?;
            sqlx::query(
                "UPDATE bigname_phase.chain_lineage
                 SET canonicality_state = 'finalized'
                 WHERE chain_id = $1 AND block_hash = $2
                   AND canonicality_state = 'safe'",
            )
            .bind(chain_id)
            .bind(block_hash)
            .execute(&self.lookup_pool)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO chain_heads (
                    chain_id,
                    latest_block_hash,
                    latest_block_number,
                    safe_block_hash,
                    safe_block_number,
                    finalized_block_hash,
                    finalized_block_number
                )
                VALUES ($1, $2, $3, $2, $3, $2, $3)
                ON CONFLICT (chain_id) DO UPDATE SET
                    latest_block_hash = EXCLUDED.latest_block_hash,
                    latest_block_number = EXCLUDED.latest_block_number,
                    safe_block_hash = EXCLUDED.safe_block_hash,
                    safe_block_number = EXCLUDED.safe_block_number,
                    finalized_block_hash = EXCLUDED.finalized_block_hash,
                    finalized_block_number = EXCLUDED.finalized_block_number,
                    updated_at = now()
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .execute(&self.lookup_pool)
            .await
            .with_context(|| format!("failed to seed phase head for {chain_id}"))?;

            sqlx::query(
                r#"
                INSERT INTO chain_phase_state (
                    chain_id,
                    phase_name,
                    phase_status,
                    current_block_number,
                    current_block_hash,
                    target_block_number,
                    target_block_hash,
                    input_content_hash,
                    started_at,
                    finished_at
                )
                VALUES
                    ($1, 'interpret', 'completed', $2, $3, $2, $3, $4, now(), now()),
                    ($1, 'project', 'completed', $2, $3, $2, $3, $4, now(), now())
                ON CONFLICT (chain_id, phase_name) DO UPDATE SET
                    phase_status = EXCLUDED.phase_status,
                    current_block_number = EXCLUDED.current_block_number,
                    current_block_hash = EXCLUDED.current_block_hash,
                    target_block_number = EXCLUDED.target_block_number,
                    target_block_hash = EXCLUDED.target_block_hash,
                    input_content_hash = EXCLUDED.input_content_hash,
                    started_at = EXCLUDED.started_at,
                    finished_at = EXCLUDED.finished_at,
                    updated_at = now()
                "#,
            )
            .bind(chain_id)
            .bind(block_number)
            .bind(block_hash)
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .execute(&self.lookup_pool)
            .await
            .with_context(|| {
                format!("failed to seed interpretation and project phase state for {chain_id}")
            })?;

        }

        Ok(())
    }

    async fn phase_state_fingerprint(
        &self,
        chain_id: &str,
        phase_name: &str,
    ) -> Result<(String, String, Option<i64>, Option<String>, String)> {
        sqlx::query_as(
            "SELECT xmin::TEXT, phase_status, current_block_number, current_block_hash,
                    updated_at::TEXT
             FROM bigname_phase.chain_phase_state
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(chain_id)
        .bind(phase_name)
        .fetch_one(&self.lookup_pool)
        .await
        .with_context(|| format!("failed to fingerprint {chain_id} {phase_name} phase state"))
    }

    async fn simulate_interpret_redo_begin(&self, chain_id: &str, redo_mode: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE bigname_phase.chain_phase_state
             SET phase_status = 'running',
                 redo_in_progress = true,
                 redo_attempt_generation = redo_attempt_generation + 1,
                 redo_mode = $2,
                 redo_previous_phase_status = phase_status,
                 redo_previous_last_error = last_error,
                 redo_previous_started_at = started_at,
                 redo_previous_finished_at = finished_at,
                 redo_from_block_number = 0,
                 redo_to_block_number = current_block_number,
                 started_at = now(),
                 finished_at = NULL,
                 updated_at = now()
             WHERE chain_id = $1 AND phase_name = 'interpret'",
        )
        .bind(chain_id)
        .bind(redo_mode)
        .execute(&self.lookup_pool)
        .await
        .with_context(|| format!("failed to simulate Interpret redo begin for {chain_id}"))?;
        anyhow::ensure!(
            result.rows_affected() == 1,
            "missing Interpret phase state for {chain_id}"
        );
        Ok(())
    }

    async fn simulate_interpret_redo_finish(&self, chain_id: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE bigname_phase.chain_phase_state
             SET phase_status = redo_previous_phase_status,
                 last_error = redo_previous_last_error,
                 started_at = redo_previous_started_at,
                 finished_at = redo_previous_finished_at,
                 redo_in_progress = false,
                 redo_mode = NULL,
                 redo_previous_phase_status = NULL,
                 redo_previous_last_error = NULL,
                 redo_previous_started_at = NULL,
                 redo_previous_finished_at = NULL,
                 redo_from_block_number = NULL,
                 redo_to_block_number = NULL,
                 redo_current_block_number = NULL,
                 redo_current_block_hash = NULL,
                 redo_target_block_number = NULL,
                 redo_target_block_hash = NULL,
                 redo_source_boundary_markers = NULL,
                 redo_manifest_authority_fingerprint = NULL,
                 updated_at = now()
             WHERE chain_id = $1 AND phase_name = 'interpret' AND redo_in_progress",
        )
        .bind(chain_id)
        .execute(&self.lookup_pool)
        .await
        .with_context(|| format!("failed to simulate Interpret redo finish for {chain_id}"))?;
        anyhow::ensure!(
            result.rows_affected() == 1,
            "missing active Interpret redo for {chain_id}"
        );
        Ok(())
    }

    async fn touch_interpret_phase_state(&self, chain_id: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE bigname_phase.chain_phase_state
             SET updated_at = updated_at + INTERVAL '1 second'
             WHERE chain_id = $1
               AND phase_name = 'interpret'
               AND redo_in_progress = false",
        )
        .bind(chain_id)
        .execute(&self.lookup_pool)
        .await
        .with_context(|| format!("failed to advance Interpret row version for {chain_id}"))?;
        anyhow::ensure!(
            result.rows_affected() == 1,
            "missing idle Interpret phase state for {chain_id}"
        );
        Ok(())
    }

    async fn seed_default_ens_snapshot_selector_position(&self) -> Result<()> {
        self.seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await
    }

    async fn seed_default_ens_primary_name_fallback_context(&self) -> Result<()> {
        self.seed_default_ens_snapshot_selector_position().await?;
        self.insert_manifest(
            "ens",
            bigname_lookup::ENS_EXECUTION_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
            1,
            "shadow",
            bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        )
        .await?;
        Ok(())
    }

    /// The Sepolia deployment profile's counterpart of
    /// `seed_default_ens_primary_name_fallback_context`: one readable `ethereum-sepolia`
    /// position under the `ethereum-sepolia` slot and a shadow `ens_execution` manifest on that
    /// chain, with no Mainnet head at all.
    async fn seed_default_sepolia_ens_primary_name_fallback_context(&self) -> Result<()> {
        self.seed_snapshot_selector_chain_positions(&json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await?;
        self.insert_manifest(
            "ens",
            bigname_lookup::ENS_EXECUTION_SOURCE_FAMILY,
            "ethereum-sepolia",
            "ens_v1",
            1,
            "shadow",
            bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        )
        .await?;
        Ok(())
    }

    async fn insert_primary_name_current_claim_row(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
        raw_claim_name: Option<&str>,
    ) -> Result<()> {
        self.insert_primary_name_current_claim_row_with_provenance(
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            json!({}),
        )
        .await
    }

    async fn insert_primary_name_current_claim_row_with_provenance(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
        raw_claim_name: Option<&str>,
        claim_provenance: Value,
    ) -> Result<()> {
        upsert_primary_name_current_rows(
            &self.pool,
            &[PrimaryNameCurrentRow {
                address: address.to_ascii_lowercase(),
                namespace: namespace.to_owned(),
                coin_type: coin_type.to_owned(),
                claim_status,
                raw_claim_name: raw_claim_name.map(str::to_owned),
                claim_provenance,
            }],
        )
        .await
        .context("failed to upsert primary_names_current row for API tests")?;
        Ok(())
    }

    async fn insert_primary_name_current_normalized_claim_name(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        normalized_claim_name: Option<&str>,
        claim_name_is_normalized: bool,
    ) -> Result<()> {
        let row = load_primary_name_current(&self.pool, address, namespace, coin_type)
            .await
            .context("failed to load primary_names_current row for API test")?
            .with_context(|| {
                format!(
                    "missing primary_names_current row for API test address {} namespace {} coin_type {}",
                    address, namespace, coin_type
                )
            })?;

        upsert_primary_name_current_snapshots(
            &self.pool,
            &[PrimaryNameCurrentSnapshot {
                row,
                normalized_claim_name: normalized_claim_name.map(str::to_owned),
                claim_name_is_normalized,
            }],
        )
        .await
        .context("failed to upsert primary_names_current snapshot for API test")?;
        Ok(())
    }

    async fn cleanup(self) -> Result<()> {
        let Self {
            database,
            pool,
            lookup_pool,
            database_name: _,
        } = self;
        drop(pool);
        drop(lookup_pool);
        database.cleanup().await
    }
}

async fn seed_schema_v2_ens_lookup_head(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    seed_schema_v2_lookup_head(
        pool,
        "ethereum-mainnet",
        block_number,
        block_hash,
        timestamp,
    )
    .await
}

async fn seed_schema_v2_lookup_head(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, $4::timestamptz, 'canonical')
         ON CONFLICT (chain_id, block_hash) DO NOTHING",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .bind(timestamp)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)
         ON CONFLICT (chain_id) DO UPDATE SET
             latest_block_hash = EXCLUDED.latest_block_hash,
             latest_block_number = EXCLUDED.latest_block_number,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_phase_state
            (chain_id, phase_name, phase_status, current_block_number, current_block_hash,
             target_block_number, target_block_hash, input_content_hash, started_at, finished_at)
         VALUES ($1, 'project', 'completed', $2, $3, $2, $3, $4, now(), now())
         ON CONFLICT (chain_id, phase_name) DO UPDATE SET
             phase_status = EXCLUDED.phase_status,
             current_block_number = EXCLUDED.current_block_number,
             current_block_hash = EXCLUDED.current_block_hash,
             target_block_number = EXCLUDED.target_block_number,
             target_block_hash = EXCLUDED.target_block_hash,
             input_content_hash = EXCLUDED.input_content_hash,
             started_at = EXCLUDED.started_at,
             finished_at = EXCLUDED.finished_at,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_schema_v2_ens_manifest(
    pool: &PgPool,
    source_family: &str,
    role: &str,
    address: &str,
    contract_instance_id: Uuid,
    resolution_capability: bool,
) -> Result<()> {
    seed_schema_v2_ens_manifest_on_chain(
        pool,
        "ethereum-mainnet",
        source_family,
        role,
        address,
        contract_instance_id,
        resolution_capability,
    )
    .await
}

async fn seed_schema_v2_ens_manifest_on_chain(
    pool: &PgPool,
    chain_id: &str,
    source_family: &str,
    role: &str,
    address: &str,
    contract_instance_id: Uuid,
    resolution_capability: bool,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let mut manifest_payload = if resolution_capability {
        json!({
            "capability_flags": {
                "verified_resolution": { "status": "supported" }
            }
        })
    } else {
        json!({})
    };
    manifest_payload["contracts"] = json!([{"role":role, "address":address,
        "proxy_kind":"none", "start_block":0, "read_features":[]}]);
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', $1, $4, 'api-test', 'active', 'test', $2, $3)
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(format!("test/ens/{source_family}.toml"))
    .bind(&manifest_payload)
    .bind(chain_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $5, 'contract', $2, $3, $4, $2, 'none')",
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .bind(chain_id)
    .execute(pool)
    .await?;
    seed_fixture_manifest_update(
        pool,
        manifest_id,
        chain_id,
        "ens",
        source_family,
        &manifest_payload,
    )
    .await
}

/// Manifest sync supplies these position-free normalized inputs to Project.
async fn seed_fixture_manifest_update(
    pool: &PgPool,
    manifest: i64,
    chain: &str,
    namespace: &str,
    family: &str,
    payload: &Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind,
         source_family, manifest_version, source_manifest_id, chain_id, derivation_kind,
         canonicality_state, after_state)
         VALUES ($1, $2, 'SourceManifestUpdated', $3,
             (SELECT manifest_version FROM manifest_versions WHERE manifest_id = $4),
             $4, $5, 'manifest_sync', 'canonical', $6)",
    )
    .bind(format!("fixture-manifest-{manifest}-{}", NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)))
    .bind(namespace)
    .bind(family)
    .bind(manifest)
    .bind(chain)
    .bind(json!({"rollout_status":"active",
             "normalizer_version":bigname_domain::normalization::ENS_NORMALIZER_VERSION,
             "manifest_payload":payload}))
    .execute(pool)
    .await?;
    Ok(())
}

/// The record-lookup fixtures publish the same normalized inputs the API reads in production.
/// Their resolver topology, version boundary and inventory are produced by the family reducers.
#[allow(clippy::too_many_arguments)]
async fn seed_record_lookup_inputs(
    pool: &PgPool,
    chain_id: &str,
    namespace: &str,
    name: &str,
    resource_id: Uuid,
    binding_id: Uuid,
    block_number: i64,
    block_hash: &str,
    timestamp_text: &str,
    indexed_address: &str,
) -> Result<String> {
    let resolver = "0x1000000000000000000000000000000000000001";
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let namehash = bigname_lookup::ens_namehash_hex(&normalized.normalized_name)?;
    let logical = format!("{namespace}:{namehash}");
    let at =
        parse_rfc3339_utc_timestamp(timestamp_text).map_err(|error| anyhow::anyhow!("{error}"))?;
    let (arm, registrar_family, registry_family, resolver_family) = if namespace == "basenames" {
        (
            "basenames",
            "basenames_base_registrar",
            "basenames_base_registry",
            "basenames_base_resolver",
        )
    } else {
        (
            "ens_v1",
            "ens_v1_registrar_l1",
            "ens_v1_registry_l1",
            "ens_v1_resolver_l1",
        )
    };
    let token_id = Uuid::from_u128(resource_id.as_u128());
    upsert_test_token_lineages(
        pool,
        &[TokenLineage {
            token_lineage_id: token_id,
            chain_id: chain_id.into(),
            block_hash: block_hash.into(),
            block_number,
            provenance: json!({"seed":"record_lookup"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_resources(
        pool,
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_id),
            chain_id: chain_id.into(),
            block_hash: block_hash.into(),
            block_number,
            provenance: json!({"seed":"record_lookup"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_name_surfaces(
        pool,
        &[NameSurface {
            logical_name_id: format!("{namespace}:{name}"),
            namespace: namespace.into(),
            input_name: name.into(),
            canonical_display_name: normalized.canonical_display_name,
            normalized_name: normalized.normalized_name,
            dns_encoded_name: normalized.dns_encoded_name,
            namehash: namehash.clone(),
            labelhashes: normalized
                .normalized_labels
                .iter()
                .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
                .collect(),
            normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.into(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.into(),
            block_hash: block_hash.into(),
            block_number,
            provenance: json!({"seed":"record_lookup"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_surface_bindings(
        pool,
        &[SurfaceBinding {
            surface_binding_id: binding_id,
            logical_name_id: format!("{namespace}:{name}"),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: arm.into(),
            active_from: at,
            active_to: None,
            chain_id: chain_id.into(),
            block_hash: block_hash.into(),
            block_number,
            provenance: json!({"seed":"record_lookup"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let manifest_payload = json!({"contracts":[{"role":"resolver", "address":resolver,
        "proxy_kind":"none", "start_block":0, "read_features":[]}]});
    let resolver_instance = Uuid::from_u128(resource_id.as_u128() + 3);
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
                 VALUES ($1, $2, 'contract')",
    )
    .bind(resolver_instance)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, $1, $2, $3, 'record-fixture', 'active', $4, $5, $6) RETURNING manifest_id",
    )
    .bind(namespace)
    .bind(resolver_family)
    .bind(chain_id)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(format!("test/{namespace}/record-resolver.toml"))
    .bind(&manifest_payload)
    .fetch_one(pool)
    .await?;
    sqlx::query("INSERT INTO manifest_contract_instances (manifest_id, chain_id,
         declaration_kind, declaration_name, contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $2, 'contract', 'resolver', $3, $4, 'resolver', 'none')")
        .bind(manifest_id).bind(chain_id).bind(resolver_instance).bind(resolver).execute(pool).await?;

    seed_fixture_manifest_update(
        pool,
        manifest_id,
        chain_id,
        namespace,
        resolver_family,
        &manifest_payload,
    )
    .await?;
    let facts = [
        (
            "RegistrationGranted",
            registrar_family,
            Some(logical.as_str()),
            Some(resource_id),
            json!({"authority_kind":"registrar", "registrant":indexed_address,
                "expiry":at.unix_timestamp() + 31_536_000}),
        ),
        (
            "ResolverChanged",
            registry_family,
            Some(logical.as_str()),
            Some(resource_id),
            json!({"node":namehash, "resolver":resolver}),
        ),
        (
            "RecordChanged",
            resolver_family,
            None,
            None,
            json!({"source_event":"AddressChanged", "node":namehash, "resolver":resolver,
                "record_key":"addr:60", "record_family":"addr", "selector_key":"60",
                "value":indexed_address}),
        ),
    ];
    let events = facts
        .into_iter()
        .enumerate()
        .map(|(log, (kind, family, logical, resource, after))| {
            let mut event = history_event(
                &format!("lookup-{resource_id}-{log}"),
                logical,
                resource,
                Some(chain_id),
                Some(block_number),
                Some(block_hash),
                Some("0xrecordlookup"),
                Some(log as i64),
                CanonicalityState::Canonical,
            );
            event.namespace = namespace.into();
            event.event_kind = kind.into();
            event.source_family = family.into();
            event.manifest_version = 1;
            event.source_manifest_id = (kind == "RecordChanged").then_some(manifest_id);
            event.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":resolver,
                                   "transaction_index":0, "block_timestamp":timestamp_text});
            event.before_state = json!({});
            event.after_state = after;
            event.derivation_kind = "record_lookup_fixture".into();
            event
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(pool, &events).await?;
    publish_test_families_on(pool, chain_id, block_number).await?;
    Ok(namehash)
}

#[tokio::test]
async fn family_record_fixture_inputs_reach_indexed_api_on_both_namespaces() -> Result<()> {
    for namespace in ["ens", "basenames"] {
        let database = TestDatabase::new_migrated().await?;
        let address = "0x0000000000000000000000000000000000000def";
        let name = if namespace == "ens" {
            "alice.eth"
        } else {
            "alice.base.eth"
        };
        if namespace == "ens" {
            seed_schema_v2_ens_record_lookup(
                &database.pool,
                21_000_003,
                "0xrecord-fixture",
                "2026-04-17T00:00:03Z",
                address,
            )
            .await?;
        } else {
            seed_schema_v2_basenames_record_lookup(
                &database.pool,
                21_000_003,
                "0xbase-record-fixture",
                "0xrecord-fixture",
                "2026-04-17T00:00:03Z",
                address,
            )
            .await?;
        }
        let (status, body) =
            read_family_response(&database, &format!("/v1/names/{name}?source=indexed")).await?;
        assert_eq!(status, StatusCode::OK, "{namespace}: {body}");
        assert_eq!(body["data"]["name"], json!(name));
        assert_eq!(body["data"]["registrant"], json!(address));
        assert_eq!(
            body["data"]["resolver"]["address"],
            json!("0x1000000000000000000000000000000000000001")
        );
        assert_eq!(body["data"]["addresses"]["60"], json!(address));
        database.cleanup().await?;
    }
    Ok(())
}

async fn seed_schema_v2_ens_record_lookup(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
    indexed_address: &str,
) -> Result<String> {
    seed_schema_v2_ens_lookup_head(pool, block_number, block_hash, timestamp).await?;
    seed_schema_v2_ens_manifest(
        pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0103),
        true,
    )
    .await?;
    seed_record_lookup_inputs(
        pool,
        "ethereum-mainnet",
        "ens",
        "alice.eth",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0101),
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0102),
        block_number,
        block_hash,
        timestamp,
        indexed_address,
    )
    .await
}

async fn seed_schema_v2_basenames_record_lookup(
    pool: &PgPool,
    block_number: i64,
    base_block_hash: &str,
    ethereum_block_hash: &str,
    timestamp: &str,
    indexed_address: &str,
) -> Result<String> {
    seed_schema_v2_lookup_head(
        pool,
        "base-mainnet",
        block_number,
        base_block_hash,
        timestamp,
    )
    .await?;
    seed_schema_v2_lookup_head(
        pool,
        "ethereum-mainnet",
        block_number,
        ethereum_block_hash,
        timestamp,
    )
    .await?;
    let l1_resolver = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
    let contract_instance_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0203);
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
                VALUES ($1, 'ethereum-mainnet', 'contract')",
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    let manifest_payload = json!({"deployment_epoch":"basenames_v1","contracts":[{"role":"l1_resolver", "address":l1_resolver,
        "proxy_kind":"none", "start_block":0, "read_features":[]}],
        "capability_flags":{"verified_resolution":{"status":"supported"}}});
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (2, 'basenames', 'basenames_execution', 'ethereum-mainnet', 'api-test', 'active',
                 'test', 'test/basenames/execution.toml', $1) RETURNING manifest_id",
    )
    .bind(&manifest_payload)
    .fetch_one(pool)
    .await?;
    sqlx::query("INSERT INTO manifest_contract_instances (manifest_id, chain_id,
         declaration_kind, declaration_name, contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract', 'l1_resolver', $2, $3, 'l1_resolver', 'none')")
        .bind(manifest_id).bind(contract_instance_id).bind(l1_resolver).execute(pool).await?;
    seed_fixture_manifest_update(
        pool,
        manifest_id,
        "ethereum-mainnet",
        "basenames",
        "basenames_execution",
        &manifest_payload,
    )
    .await?;
    let namehash = seed_record_lookup_inputs(
        pool,
        "base-mainnet",
        "basenames",
        "alice.base.eth",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0201),
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0202),
        block_number,
        base_block_hash,
        timestamp,
        indexed_address,
    )
    .await?;
    publish_test_families_on(pool, "ethereum-mainnet", block_number).await?;
    Ok(namehash)
}

async fn seed_schema_v2_ens_primary_name_authority(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    seed_schema_v2_ens_primary_name_authority_on_chain(
        pool,
        "ethereum-mainnet",
        block_number,
        block_hash,
        timestamp,
    )
    .await
}

/// Readable head plus the registry and Universal Resolver manifests primary-name lookup selects,
/// on the given ENS L1 chain. The Universal Resolver proxy address is the same on Mainnet and
/// Sepolia.
async fn seed_schema_v2_ens_primary_name_authority_on_chain(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    seed_schema_v2_lookup_head(pool, chain_id, block_number, block_hash, timestamp).await?;
    seed_schema_v2_ens_manifest_on_chain(
        pool,
        chain_id,
        "ens_v1_registry_l1",
        "registry",
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0104),
        false,
    )
    .await?;
    seed_schema_v2_ens_manifest_on_chain(
        pool,
        chain_id,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0105),
        true,
    )
    .await?;
    rebuild_fixture_families(pool, chain_id, block_number, block_hash).await
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read API response body")?;
    serde_json::from_slice(&bytes).context("failed to decode API response JSON")
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

async fn seed_readable_lineage_anchors<'a>(
    pool: &PgPool,
    anchors: impl IntoIterator<Item = (&'a str, &'a str, i64, CanonicalityState)>,
) -> Result<()> {
    for (chain_id, block_hash, block_number, canonicality_state) in anchors {
        if !matches!(
            canonicality_state,
            CanonicalityState::Canonical
                | CanonicalityState::Safe
                | CanonicalityState::Finalized
        ) {
            continue;
        }

        let block_timestamp = parse_rfc3339_utc_timestamp(&format!(
            "2026-04-17T00:00:{:02}Z",
            block_number.rem_euclid(60)
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.chain_lineage (
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5::bigname_phase.canonicality_state)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(block_timestamp)
        .bind(canonicality_state.as_str())
        .execute(pool)
        .await
        .with_context(|| {
            format!("failed to seed readable lineage for {chain_id} block {block_hash}")
        })?;
    }

    Ok(())
}

async fn readable_lineage_anchor(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> Result<(String, i64)> {
    seed_readable_lineage_anchors(
        pool,
        [(chain_id, block_hash, block_number, canonicality_state)],
    )
    .await?;
    sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT block_hash, block_number
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_one(pool)
    .await
    .context("readable test lineage anchor must exist")
}

async fn identity_lineage_anchor(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<(String, i64)> {
    let block_timestamp = parse_rfc3339_utc_timestamp(&format!(
        "2026-04-17T00:00:{:02}Z",
        block_number.rem_euclid(60)
    ))
    .map_err(|error| anyhow::anyhow!(error))?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, $4, 'observed'::bigname_phase.canonicality_state)
        ON CONFLICT (chain_id, block_hash) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .bind(block_timestamp)
    .execute(pool)
    .await?;
    Ok((block_hash.to_owned(), block_number))
}

async fn identity_lineage_anchor_for_state(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> Result<(String, i64)> {
    if matches!(
        canonicality_state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    ) {
        readable_lineage_anchor(
            pool,
            chain_id,
            block_hash,
            block_number,
            canonicality_state,
        )
        .await
    } else {
        identity_lineage_anchor(pool, chain_id, block_hash, block_number).await
    }
}

async fn upsert_test_token_lineages(
    pool: &PgPool,
    token_lineages: &[TokenLineage],
) -> Result<Vec<TokenLineage>> {
    seed_readable_lineage_anchors(
        pool,
        token_lineages.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in token_lineages {
        let (block_hash, block_number) = identity_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.token_lineages (
                token_lineage_id, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6::bigname_phase.canonicality_state)
            ON CONFLICT (token_lineage_id) DO UPDATE SET
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(row.token_lineage_id)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(token_lineages.to_vec())
}

async fn upsert_test_resources(
    pool: &PgPool,
    resources: &[Resource],
) -> Result<Vec<Resource>> {
    seed_readable_lineage_anchors(
        pool,
        resources.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in resources {
        let (block_hash, block_number) = identity_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.resources (
                resource_id, token_lineage_id, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::bigname_phase.canonicality_state)
            ON CONFLICT (resource_id) DO UPDATE SET
                token_lineage_id = EXCLUDED.token_lineage_id,
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(row.resource_id)
        .bind(row.token_lineage_id)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(resources.to_vec())
}

async fn upsert_test_name_surfaces(
    pool: &PgPool,
    name_surfaces: &[NameSurface],
) -> Result<Vec<NameSurface>> {
    seed_readable_lineage_anchors(
        pool,
        name_surfaces.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in name_surfaces {
        let (block_hash, block_number) = identity_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        let (logical_name_id, namehash) =
            phase_logical_identity(&row.namespace, &row.normalized_name)?;
        let raw_labels = row
            .normalized_name
            .split('.')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let labelhashes = raw_labels
            .iter()
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
            .collect::<Vec<_>>();
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.name_surfaces (
                logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                namehash, labelhashes, normalizer_version, visibility_state,
                normalization_errors, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9, $10, $11, $12, $13,
                    $14::bigname_phase.canonicality_state)
            ON CONFLICT (logical_name_id) DO UPDATE SET
                raw_name = EXCLUDED.raw_name,
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(logical_name_id)
        .bind(&row.namespace)
        .bind(&row.normalized_name)
        .bind(raw_labels)
        .bind(&row.dns_encoded_name)
        .bind(namehash)
        .bind(labelhashes)
        .bind(&row.normalizer_version)
        .bind(&row.normalization_errors)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(name_surfaces.to_vec())
}

async fn upsert_test_surface_bindings(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<Vec<SurfaceBinding>> {
    seed_readable_lineage_anchors(
        pool,
        bindings.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in bindings {
        let (block_hash, block_number) = identity_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        let (namespace, name) = row
            .logical_name_id
            .split_once(':')
            .context("test surface binding logical_name_id must include namespace")?;
        let (logical_name_id, _) = phase_logical_identity(namespace, name)?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.surface_bindings (
                surface_binding_id, logical_name_id, resource_id, binding_kind,
                authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::bigname_phase.canonicality_state)
            ON CONFLICT (surface_binding_id) DO UPDATE SET
                active_to = EXCLUDED.active_to,
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(row.surface_binding_id)
        .bind(logical_name_id)
        .bind(row.resource_id)
        .bind(row.binding_kind.as_str())
        .bind(&row.authority_arm)
        .bind(row.active_from)
        .bind(row.active_to)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(bindings.to_vec())
}

fn raw_block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: i64,
) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn resource(resource_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xresource".to_owned(),
        block_number: 99,
        provenance: json!({"seed": "resource"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn name_surface(logical_name_id: &str) -> NameSurface {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        input_name: normalized_name.to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec!["labelhash:alice".to_owned()],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: chain_id.to_owned(),
        block_hash: "0xsurface".to_owned(),
        block_number: 98,
        provenance: json!({"seed": "surface"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    active_from: OffsetDateTime,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        authority_arm: "ens_v1".to_owned(),
        active_from,
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xbinding".to_owned(),
        block_number: 100,
        provenance: json!({"seed": "binding"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

#[allow(clippy::too_many_arguments)]
fn history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    chain_id: Option<&str>,
    block_number: Option<i64>,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    log_index: Option<i64>,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: "HistoryEvent".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: None,
        chain_id: chain_id.map(str::to_owned),
        block_number,
        block_hash: block_hash.map(str::to_owned),
        transaction_hash: transaction_hash.map(str::to_owned),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "event_identity": event_identity,
        }),
        derivation_kind: "history_test".to_owned(),
        canonicality_state,
        before_state: json!({
            "provenance": {
                "before": event_identity,
            }
        }),
        after_state: json!({
            "provenance": {
                "after": event_identity,
            },
            "coverage": {
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["normalized_events"],
                "enumeration_basis": event_identity,
                "unsupported_reason": null,
            }
        }),
    }
}

fn permission_current_row(
    resource_id: Uuid,
    subject: &str,
    scope: PermissionScope,
    manifest_version: i64,
    block_number: i64,
) -> PermissionsCurrentRow {
    PermissionsCurrentRow {
        resource_id,
        subject: subject.to_owned(),
        scope,
        record_resource_selector: None,
        effective_powers: json!([
            "set_resolver",
            if manifest_version % 2 == 0 {
                "create_subnames"
            } else {
                "set_records"
            }
        ]),
        grant_source: json!({
            "kind": "raw_log",
            "source_event": "EACRolesChanged",
            "upstream_resource": resource_id.to_string(),
            "root_resource": false,
            "changed_powers": [
                "set_resolver",
                if manifest_version % 2 == 0 {
                    "create_subnames"
                } else {
                    "set_records"
                }
            ],
            "registry_contract_instance_id": "00000000-0000-0000-0000-00000000c001",
        }),
        revocation_source: None,
        inheritance_path: json!([]),
        transfer_behavior: json!({}),
        provenance: json!({
            "normalized_event_ids": [block_number, block_number + 1],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": manifest_version,
                "source_family": "ens_v2_registry_l1",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v2",
            }],
            "derivation_kind": "permissions_current_rebuild",
            "chain_id": "ethereum-mainnet",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["permissions_current"],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            "block_number": block_number,
            "block_hash": format!("0xperm{block_number:02x}"),
            "target_block_number": block_number,
            "target_block_hash": format!("0xperm{block_number:02x}"),
            "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
        }),
        canonicality_summary: json!({
            "state": "canonical",
            "target_block_number": block_number,
            "target_block_hash": format!("0xperm{block_number:02x}"),
        }),
        manifest_version,
        last_recomputed_at: timestamp(1_717_174_000 + block_number),
    }
}

fn permission_current_resource_summary(
    resource_id: Uuid,
    authority_kind: Option<&str>,
) -> bigname_storage::PermissionsCurrentResourceSummary {
    let authority_kind = authority_kind.map(str::to_owned);
    let coverage = match authority_kind.as_deref() {
        Some(kind) if PHASE_PROJECTED_PERMISSION_AUTHORITY_KINDS.contains(&kind) => {
            bigname_storage::ResourcePermissionCoverage::operator_approval_surfaces_not_ingested()
        }
        Some("wrapper") => bigname_storage::ResourcePermissionCoverage::wrapper_parent_and_resolver_delegation_not_projected(),
        _ => bigname_storage::ResourcePermissionCoverage::resource_authority_not_projected(),
    };
    bigname_storage::PermissionsCurrentResourceSummary {
        resource_id,
        authority_kind,
        root_resource_id: None,
        coverage,
        resource_restrictions: None,
        provenance: json!({
            "derivation_kind": "permissions_current_resource_summary_rebuild",
            "chain_id": "ethereum-mainnet",
        }),
        chain_positions: json!({
            "block_number": 1,
            "block_hash": "0xpermission-summary",
            "target_block_number": 1,
            "target_block_hash": "0xpermission-summary",
            "timestamp": "2024-05-31T01:13:20Z",
        }),
        canonicality_summary: json!({
            "state": "canonical_lineage",
            "target_block_number": 1,
            "target_block_hash": "0xpermission-summary",
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_174_000),
    }
}

fn resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
    ResolverCurrentRow {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_owned(),
        declared_summary: json!({
            "bindings": {
                "status": "supported",
                "count": 2,
                "items": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "normalized_name": "alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": "00000000-0000-0000-0000-00000000b100",
                        "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                        "binding_kind": "declared_registry_path",
                    },
                    {
                        "logical_name_id": "ens:beta.eth",
                        "canonical_display_name": "Beta.eth",
                        "normalized_name": "beta.eth",
                        "namehash": "namehash:beta.eth",
                        "resource_id": "00000000-0000-0000-0000-00000000b102",
                        "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                        "binding_kind": "resolver_alias_path",
                    }
                ],
            },
            "aliases": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "logical_name_id": "ens:beta.eth",
                    "canonical_display_name": "Beta.eth",
                    "normalized_name": "beta.eth",
                    "namehash": "namehash:beta.eth",
                    "resource_id": "00000000-0000-0000-0000-00000000b102",
                    "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                    "binding_kind": "resolver_alias_path",
                }],
            },
            "permissions": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "resource_id": "00000000-0000-0000-0000-00000000b100",
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "effective_powers": ["set_resolver", "set_records"],
                    "grant_source": {
                        "kind": "raw_log",
                        "source_event": "EACRolesChanged",
                        "upstream_resource": "root",
                        "root_resource": true,
                        "changed_powers": ["set_resolver", "set_records"],
                        "resolver_contract_instance_id": "00000000-0000-0000-0000-00000000c202",
                    },
                    "revocation_source": null,
                }],
            },
            "role_holders": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "resource_count": 1,
                    "permission_row_count": 1,
                    "effective_powers": ["set_records", "set_resolver"],
                    "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                }],
            },
            "links": {
                "status": "supported",
                "count": 2,
                "total_count": 2,
                "record_count": 2,
                "sample_limit": 100,
                "sample_count": 2,
                "truncated": false,
                "items": [
                    {
                        "record_id": "1",
                        "namehash": "namehash:alice.eth",
                        "default": false,
                        "logical_name_id": "ens:alice.eth",
                        "name": "Alice.eth",
                        "namespace": "ens",
                        "normalized_event_id": 303,
                        "chain_position": {
                            "chain_id": chain_id,
                            "block_number": 180,
                            "block_hash": "0xlink180",
                            "transaction_hash": "0xlink180tx",
                            "log_index": 2,
                            "timestamp": "2026-04-16T00:00:00Z",
                        },
                    },
                    {
                        "record_id": "2",
                        "namehash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "default": true,
                        "normalized_event_id": 304,
                        "chain_position": {
                            "chain_id": chain_id,
                            "block_number": 181,
                            "block_hash": "0xlink181",
                            "transaction_hash": "0xlink181tx",
                            "log_index": 0,
                            "timestamp": "2026-04-16T00:00:12Z",
                        },
                    }
                ],
            },
            "event_summary": {
                "status": "supported",
                "count": 3,
                "by_kind": {
                    "PermissionChanged": 1,
                    "ResolverChanged": 2,
                },
            },
        }),
        provenance: json!({
            "normalized_event_ids": [101, 202],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": 202,
            }],
            "manifest_versions": [{
                "manifest_version": 7,
                "source_family": "ens_v2_registry_l1",
                "chain": chain_id,
                "deployment_epoch": "ens_v2",
            }],
            "derivation_kind": "resolver_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
            "unsupported_reason": null,
            "enumeration_basis": "resolver_target",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": chain_id,
                "block_number": 202,
                "block_hash": "0xresolverc8",
                "timestamp": "2026-04-17T00:00:22Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized",
            }
        }),
        manifest_version: 7,
        last_recomputed_at: timestamp(1_748_800_202),
    }
}

fn resolver_current_row_with_writer_alias(
    chain_id: &str,
    resolver_address: &str,
) -> ResolverCurrentRow {
    let mut row = resolver_current_row(chain_id, resolver_address);
    row.declared_summary["aliases"]["count"] = json!(2);
    row.declared_summary["aliases"]["items"]
        .as_array_mut()
        .expect("resolver aliases fixture must be an array")
        .push(json!({
            "logical_name_id": "ens:alias.eth",
            "resource_id": "00000000-0000-0000-0000-00000000b104",
            "binding_kind": "resolver_alias_path",
            "alias_state": "active",
            "active": true,
            "chain_id": chain_id,
            "resolver_address": resolver_address,
            "from_dns_encoded_name": "0x05616c6961730365746800",
            "to_dns_encoded_name": "0x04626574610365746800",
            "from_name": "alias.eth",
            "to_name": "beta.eth",
            "to_logical_name_id": "ens:beta.eth",
            "to_resource_id": "00000000-0000-0000-0000-00000000b102",
            "latest_event_kind": "AliasChanged",
        }));
    row.declared_summary["event_summary"]["count"] = json!(4);
    row.declared_summary["event_summary"]["by_kind"]["AliasChanged"] = json!(1);
    row
}

fn exact_name_row(
    logical_name_id: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        serving_resource_id: None,
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": {
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        }),
        provenance: json!({
            "normalized_event_ids": [101, 102],
            "raw_fact_refs": [
                {
                    "kind": "log",
                    "chain_id": "ethereum-mainnet",
                    "block_hash": "0xabc"
                }
            ],
            "manifest_versions": [
                {
                    "manifest_version": 3,
                    "source_family": "ens_v1_registry",
                    "chain": "ethereum-mainnet",
                    "deployment_epoch": "ens_v1"
                }
            ],
            "derivation_kind": "name_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

fn record_inventory_boundary_with_pointer(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    })
}

fn record_inventory_boundary(logical_name_id: &str, resource_id: Uuid) -> Value {
    record_inventory_boundary_with_pointer(logical_name_id, resource_id, None, None)
}

fn record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary(logical_name_id, resource_id),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false
        }),
        selectors: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "cacheable": true
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "cacheable": false
            }
        ]),
        explicit_gaps: json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver"
            }
        ]),
        unsupported_families: json!([
            {
                "record_family": "abi",
                "unsupported_reason": "resolver_family_pending"
            },
            {
                "record_family": "pubkey",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        last_change: Some(json!({
            "normalized_event_id": 1200,
            "event_kind": "RecordsChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000abc"
                }
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1200],
            "derivation_kind": "record_inventory_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "declared_record_inventory"
        }),
        chain_positions: json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_718),
    }
}


#[allow(clippy::too_many_arguments)]
fn address_name_name_current_row(
    logical_name_id: &str,
    canonical_display_name: &str,
    normalized_name: &str,
    namehash: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
    declared_summary: Value,
) -> bigname_storage::NameCurrentRow {
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: canonical_display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        serving_resource_id: None,
        token_lineage_id,
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [block_number, block_number + 1],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v1",
            }],
            "derivation_kind": "name_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name",
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xname{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_175_000 + block_number),
    }
}

fn collection_name_surface(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
) -> NameSurface {
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace")
        .to_owned();
    let chain_id = chain_id_for_namespace(&namespace).to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace,
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: labelhash_for_display_name(display_name)
            .into_iter()
            .collect(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id,
        block_hash: format!("0xsurface{block_number:02x}"),
        block_number,
        provenance: json!({"seed": "children_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn labelhash_for_display_name(display_name: &str) -> Option<String> {
    display_name
        .split('.')
        .next()
        .filter(|label| !label.is_empty())
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
}

fn chain_id_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base-mainnet",
        _ => "ethereum-mainnet",
    }
}

fn chain_slot_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base",
        _ => "ethereum",
    }
}


fn address_name_token_lineage(
    token_lineage_id: Uuid,
    block_hash: &str,
    block_number: i64,
) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn address_name_resource(
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_hash: &str,
    block_number: i64,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn address_name_surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    block_hash: &str,
    block_number: i64,
    active_from: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        authority_arm: "ens_v1".to_owned(),
        active_from: timestamp(active_from),
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn address_name_current_row(
    address: &str,
    logical_name_id: &str,
    relation: bigname_storage::AddressNameRelation,
    display_name: &str,
    normalized_name: &str,
    namehash: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
) -> bigname_storage::AddressNameCurrentRow {
    debug_assert_eq!(
        bigname_domain::normalization::normalize_name(display_name)
            .map(|name| name.normalized_name)
            .ok()
            .as_deref(),
        Some(normalized_name)
    );
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);
    bigname_storage::AddressNameCurrentRow {
        address: address.to_ascii_lowercase(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id,
        resource_id,
        token_lineage_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": "ens_v1_registrar_l1",
                "source_manifest_id": null,
            }],
            "derivation_kind": "address_names_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "surface_current_relations",
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xaddr{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_173_000 + block_number),
    }
}

fn compact_records_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    let mut row = record_inventory_current_row(logical_name_id, resource_id);
    row.selectors = json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "cacheable": true,
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true,
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "cacheable": true,
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "cacheable": true,
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "cacheable": true,
        },
    ]);
    row.explicit_gaps = json!([]);
    row.entries = json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "status": "not_found",
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc",
            },
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": { "value": "ipfs://avatar" },
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": { "value": "ipfs://content" },
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "status": "success",
            "value": {
                "key": "com.twitter",
                "value": "@alice",
            },
        },
    ]);
    row
}

/// A retained resolver write. Missing values exercise event generations which announce a key
/// without retaining its value; output status and coverage are always derived by the reader.
fn family_fixture_record_write(key: &str, value: Option<Value>) -> Value {
    let (key, family, selector, source) = if key == "avatar" {
        (
            "text:avatar".to_owned(),
            "text",
            json!("avatar"),
            "TextChanged",
        )
    } else if let Some(selector) = key.strip_prefix("text:") {
        (key.to_owned(), "text", json!(selector), "TextChanged")
    } else if let Some(selector) = key.strip_prefix("addr:") {
        (key.to_owned(), "addr", json!(selector), "AddressChanged")
    } else {
        assert_eq!(key, "contenthash", "explicit fixture record family");
        (
            key.to_owned(),
            "contenthash",
            Value::Null,
            "ContenthashChanged",
        )
    };
    let mut after = json!({"record_key":key,"record_family":family,"selector_key":selector,"source_event":source});
    if let Some(value) = value {
        after["value"] = value;
    }
    after
}

#[allow(clippy::too_many_arguments)]
async fn insert_family_fixture_record_writes(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    name: &str,
    resolver: &str,
    block: i64,
    hash: &str,
    writes: &[Value],
) -> Result<()> {
    let family = if namespace == "basenames" {
        "basenames_base_resolver"
    } else {
        "ens_v1_resolver_l1"
    };
    let manifest =
        declare_family_fixture_resolver(pool, namespace, chain, family, resolver).await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let mut events = Vec::new();
    for write in writes {
        let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
        let mut after = write.clone();
        after["node"] = json!(node);
        after["resolver"] = json!(resolver);
        let mut event = history_event(
            &format!("fixture-record-{ordinal}"),
            None,
            None,
            Some(chain),
            Some(block),
            Some(hash),
            Some("0xrecords"),
            Some(ordinal),
            CanonicalityState::Canonical,
        );
        event.namespace = namespace.into();
        event.event_kind = "RecordChanged".into();
        event.source_family = family.into();
        event.manifest_version = 1;
        event.source_manifest_id = Some(manifest);
        event.raw_fact_ref =
            json!({"kind":"raw_log", "emitting_address":resolver,"transaction_index":0});
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(pool, &events).await?;
    Ok(())
}

/// The default Alice route fixture: first registry observation, registrar grant and current
/// resolver writes at three actual block times. The response's dates and values are produced.
/// Append a concrete registry pointer event to the current retained name binding.
async fn append_name_resolver_input(
    database: &TestDatabase,
    namespace: &str,
    name: &str,
    resolver: &str,
) -> Result<()> {
    let chain = chain_id_for_namespace(namespace);
    let family = if namespace == "basenames" {
        "basenames_base_registry"
    } else {
        "ens_v1_registry_l1"
    };
    let logical = bigname_storage::logical_name_id_for_name(namespace, name);
    let (block, hash): (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(&database.pool)
    .await?;
    let resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings WHERE logical_name_id = $1 AND active_to IS NULL",
    )
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?;
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let mut event = history_event(
        &format!("name-pointer-{ordinal}"),
        Some(&logical),
        Some(resource),
        Some(chain),
        Some(block),
        Some(&hash),
        Some("0xpointer"),
        Some(ordinal),
        CanonicalityState::Canonical,
    );
    event.namespace = namespace.into();
    event.event_kind = "ResolverChanged".into();
    event.source_family = family.into();
    event.before_state = json!({});
    event.after_state = json!({"source_event":"NewResolver", "node":bigname_lookup::ens_namehash_hex(name)?, "resolver":resolver});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_fixture_families(&database.pool, chain, block, &hash).await
}

/// Add observations to the direct ENS resolver used by seed_schema_v2_ens_record_lookup.
async fn insert_record_lookup_fixture_writes(
    database: &TestDatabase,
    writes: &[Value],
) -> Result<()> {
    let (block, hash): (i64, String) = sqlx::query_as("SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = 'ethereum-mainnet'")
        .fetch_one(&database.pool).await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "alice.eth",
        "0x1000000000000000000000000000000000000001",
        block,
        &hash,
        writes,
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, &hash).await
}

async fn seed_alice_name_inputs(database: &TestDatabase) -> Result<()> {
    seed_alice_name_inputs_with_writes(
        database,
        &[
            family_fixture_record_write(
                "addr:60",
                Some(json!("0x0000000000000000000000000000000000000def")),
            ),
            family_fixture_record_write("avatar", Some(json!("https://example.test/avatar.png"))),
            family_fixture_record_write("contenthash", Some(json!("ipfs://alice"))),
            family_fixture_record_write("text:description", Some(json!("Alice profile"))),
        ],
    )
    .await
}

async fn seed_alice_name_inputs_with_writes(
    database: &TestDatabase,
    writes: &[Value],
) -> Result<()> {
    let chain = "ethereum-mainnet";
    let resource = Uuid::from_u128(0x2200);
    let resolver = "0x0000000000000000000000000000000000000abc";
    for (block, hash, time) in [
        (21_000_001, "0xalice-created", "2023-01-02T03:04:05Z"),
        (21_000_002, "0xalice-granted", "2024-01-02T03:04:05Z"),
        (21_000_003, "0xbinding", "2026-04-17T00:00:03Z"),
    ] {
        seed_schema_v2_lookup_head(&database.pool, chain, block, hash, time).await?;
    }
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "alice.eth",
        chain,
        21_000_001,
        "0xalice-created",
        resource,
        Uuid::from_u128(0x1100),
        Uuid::from_u128(0x3300),
        "ens_v1",
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex("alice.eth")?;
    let facts = [
        (
            21_000_001,
            "0xalice-created",
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            json!({"source_event":"Transfer","node":node,"owner":"0x00000000000000000000000000000000000000bb"}),
        ),
        (
            21_000_002,
            "0xalice-granted",
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            json!({"authority_kind":"registrar","registrant":"0x00000000000000000000000000000000000000aa",
                "expiry":parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z").map_err(|e| anyhow::anyhow!("{e}"))?.unix_timestamp()}),
        ),
        (
            21_000_003,
            "0xbinding",
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"node":node,"resolver":resolver}),
        ),
    ];
    let mut events = Vec::new();
    for (block, hash, kind, family, after) in facts {
        let mut event = history_event(
            &format!("alice-{kind}"),
            Some(&logical),
            Some(resource),
            Some(chain),
            Some(block),
            Some(hash),
            Some("0xalice"),
            Some(0),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = family.into();
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        chain,
        "alice.eth",
        resolver,
        21_000_003,
        "0xbinding",
        writes,
    )
    .await?;
    rebuild_fixture_families(&database.pool, chain, 21_000_003, "0xbinding").await
}

/// Start a new resolver record version and retain only the supplied writes after that boundary.
async fn replace_alice_record_inputs(database: &TestDatabase, writes: &[Value]) -> Result<()> {
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let resolver = "0x0000000000000000000000000000000000000abc";
    let mut version = history_event(
        &format!("alice-record-version-{ordinal}"),
        None,
        None,
        Some("ethereum-mainnet"),
        Some(21_000_003),
        Some("0xbinding"),
        Some("0xrecords"),
        Some(ordinal),
        CanonicalityState::Canonical,
    );
    version.event_kind = "RecordVersionChanged".into();
    version.source_family = "ens_v1_resolver_l1".into();
    version.raw_fact_ref =
        json!({"kind":"raw_log", "emitting_address":resolver,"transaction_index":0});
    version.before_state = json!({});
    version.after_state = json!({"node":bigname_lookup::ens_namehash_hex("alice.eth")?,
        "record_version":ordinal});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[version]).await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "alice.eth",
        resolver,
        21_000_003,
        "0xbinding",
        writes,
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}

async fn v2_name_records_payload_with_writes(uri: &str, writes: &[Value]) -> Result<Value> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    replace_alice_record_inputs(&database, writes).await?;
    let body = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(body)
}

/// Identity inputs shared by named API fixtures. All names go through the production normalizer;
/// the caller supplies the actual chain position and stable resource/binding identities.
#[allow(clippy::too_many_arguments)]
async fn seed_family_identity_inputs(
    pool: &PgPool,
    namespace: &str,
    name: &str,
    chain: &str,
    block: i64,
    hash: &str,
    resource: Uuid,
    token: Uuid,
    binding: Uuid,
    arm: &str,
) -> Result<String> {
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let (logical, namehash) = phase_logical_identity(namespace, &normalized.normalized_name)?;
    let at: OffsetDateTime = sqlx::query_scalar(
        "SELECT block_timestamp FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2 AND block_number = $3"
    ).bind(chain).bind(hash).bind(block).fetch_one(pool).await?;
    upsert_test_token_lineages(
        pool,
        &[TokenLineage {
            token_lineage_id: token,
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_resources(
        pool,
        &[Resource {
            resource_id: resource,
            token_lineage_id: Some(token),
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_name_surfaces(
        pool,
        &[NameSurface {
            logical_name_id: logical.clone(),
            namespace: namespace.into(),
            input_name: name.into(),
            canonical_display_name: normalized.canonical_display_name,
            normalized_name: normalized.normalized_name,
            dns_encoded_name: normalized.dns_encoded_name,
            namehash,
            labelhashes: vec![],
            normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.into(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_surface_bindings(
        pool,
        &[SurfaceBinding {
            surface_binding_id: binding,
            logical_name_id: format!("{namespace}:{name}"),
            resource_id: resource,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: arm.into(),
            active_from: at,
            active_to: None,
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    Ok(logical)
}

/// Extend a test's real resolver declaration and provide its manifest-sync input to Project.
async fn declare_family_fixture_resolver(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    family: &str,
    address: &str,
) -> Result<i64> {
    let existing: Option<(i64, Value)> = sqlx::query_as(
        "SELECT manifest_id, manifest_payload FROM manifest_versions WHERE namespace = $1
         AND chain_id = $2 AND source_family = $3 AND rollout_status = 'active'",
    )
    .bind(namespace)
    .bind(chain)
    .bind(family)
    .fetch_optional(pool)
    .await?;
    let (manifest, mut payload) = if let Some(existing) = existing {
        existing
    } else {
        let payload = json!({"contracts":[]});
        let id = sqlx::query_scalar(
            "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
             VALUES (1, $1, $2, $3, 'family-fixture', 'active', $4, $5, $6) RETURNING manifest_id",
        )
        .bind(namespace)
        .bind(family)
        .bind(chain)
        .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
        .bind(format!("test/{namespace}/{chain}/{family}.toml"))
        .bind(&payload)
        .fetch_one(pool)
        .await?;
        (id, payload)
    };
    if payload["contracts"]
        .as_array()
        .is_some_and(|contracts| contracts.iter().any(|c| c["address"] == address))
    {
        return Ok(manifest);
    }
    payload["contracts"].as_array_mut().context("fixture manifest contracts")?.push(json!({
        "role":"resolver", "address":address, "proxy_kind":"none", "start_block":0, "read_features":[]
    }));
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(manifest)
        .bind(&payload)
        .execute(pool)
        .await?;
    let instance = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, $2, 'contract')")
        .bind(instance).bind(chain).execute(pool).await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (manifest_id, chain_id, declaration_kind,
        declaration_name, contract_instance_id, declared_address, role, proxy_kind)
        VALUES ($1, $2, 'contract', $3, $4, $3, 'resolver', 'none')",
    )
    .bind(manifest)
    .bind(chain)
    .bind(address)
    .bind(instance)
    .execute(pool)
    .await?;
    seed_fixture_manifest_update(pool, manifest, chain, namespace, family, &payload).await?;
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
async fn seed_identity_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    normalized_name: &str,
    _namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    block_number: i64,
) -> Result<()> {
    let namespace = logical_name_id
        .split_once(':')
        .context("fixture namespace")?
        .0;
    let chain = chain_id_for_namespace(namespace);
    let hash = format!("0xname{block_number:02x}");
    let at = format!("2026-04-17T00:00:{:02}Z", block_number % 60);
    let positions = align_phase_chain_positions(
        &database.pool,
        &json!({chain_slot_for_namespace(namespace): {
            "chain_id":chain, "block_number":block_number, "block_hash":hash, "timestamp":at
        }}),
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&positions)
        .await?;
    let hash = positions[chain_slot_for_namespace(namespace)]["block_hash"]
        .as_str()
        .context("identity hash")?;
    let normalized = bigname_domain::normalization::normalize_name(display_name)?;
    anyhow::ensure!(
        normalized.normalized_name == normalized_name,
        "identity fixture normalized bytes"
    );
    let arm = if namespace == "basenames" {
        "basenames"
    } else {
        "ens_v1"
    };
    let registrar = if namespace == "basenames" {
        "basenames_base_registrar"
    } else {
        "ens_v1_registrar_l1"
    };
    let registry = if namespace == "basenames" {
        "basenames_base_registry"
    } else {
        "ens_v1_registry_l1"
    };
    let resolver_family = if namespace == "basenames" {
        "basenames_base_resolver"
    } else {
        "ens_v1_resolver_l1"
    };
    let logical = seed_family_identity_inputs(
        &database.pool,
        namespace,
        normalized_name,
        chain,
        block_number,
        hash,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        arm,
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex(normalized_name)?;
    let manifest =
        declare_family_fixture_resolver(&database.pool, namespace, chain, resolver_family, address)
            .await?;
    let mut inputs = vec![
        (
            "RegistrationGranted",
            registrar,
            Some(logical.as_str()),
            Some(resource_id),
            json!({"authority_kind":"registrar", "registrant":address, "expiry":1900000000}),
        ),
        (
            "AuthorityTransferred",
            registry,
            Some(logical.as_str()),
            Some(resource_id),
            json!({"source_event":"Transfer", "node":node, "owner":address}),
        ),
        (
            "ResolverChanged",
            registry,
            Some(logical.as_str()),
            Some(resource_id),
            json!({"node":node, "resolver":address}),
        ),
    ];
    for (key, family, selector, value, source) in [
        ("addr:0", "addr", json!("0"), json!("0x"), "AddressChanged"),
        (
            "addr:60",
            "addr",
            json!("60"),
            json!("0x0000000000000000000000000000000000000abc"),
            "AddressChanged",
        ),
        (
            "text:avatar",
            "text",
            json!("avatar"),
            json!("ipfs://avatar"),
            "TextChanged",
        ),
        (
            "contenthash",
            "contenthash",
            Value::Null,
            json!("ipfs://content"),
            "ContenthashChanged",
        ),
        (
            "text:com.twitter",
            "text",
            json!("com.twitter"),
            json!("@alice"),
            "TextChanged",
        ),
    ] {
        inputs.push((
            "RecordChanged",
            resolver_family,
            None,
            None,
            json!({"source_event":source, "node":node, "resolver":address, "record_key":key,
                "record_family":family, "selector_key":selector, "value":value}),
        ));
    }
    let events = inputs
        .into_iter()
        .enumerate()
        .map(|(log, (kind, family, logical, resource, after))| {
            let mut event = history_event(
                &format!("identity-{resource_id}-{log}"),
                logical,
                resource,
                Some(chain),
                Some(block_number),
                Some(hash),
                Some("0xidentity"),
                Some(log as i64),
                CanonicalityState::Canonical,
            );
            event.namespace = namespace.into();
            event.event_kind = kind.into();
            event.source_family = family.into();
            event.manifest_version = 1;
            event.source_manifest_id = (kind == "RecordChanged").then_some(manifest);
            event.raw_fact_ref =
                json!({"kind":"raw_log", "emitting_address":address, "transaction_index":0});
            event.before_state = json!({});
            event.after_state = after;
            event
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, chain, block_number, hash).await?;
    let indexed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM project_address_name_index WHERE address = lower($1)
         AND logical_name_id = $2 AND relation = $3)",
    )
    .bind(address)
    .bind(&logical)
    .bind(relation.as_str())
    .fetch_one(&database.pool)
    .await?;
    anyhow::ensure!(
        indexed,
        "the actual identity inputs must produce the requested {relation:?} membership"
    );
    Ok(())
}

async fn seed_phase_primary_name_snapshot(
    database: &TestDatabase,
    address: &str,
    namespace: &str,
    coin_type: &str,
    claim_status: bigname_storage::PrimaryNameClaimStatus,
    raw_claim_name: Option<&str>,
    claim_name_is_normalized: bool,
) -> Result<()> {
    upsert_primary_name_current_snapshots(
        &database.lookup_pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: address.into(),
                namespace: namespace.into(),
                coin_type: coin_type.into(),
                claim_status,
                raw_claim_name: raw_claim_name.map(str::to_owned),
                claim_provenance: json!({}),
            },
            normalized_claim_name: claim_name_is_normalized
                .then(|| raw_claim_name.map(str::to_owned))
                .flatten(),
            claim_name_is_normalized,
        }],
    )
    .await
}

fn primary_name_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        primary_name_left_pad_hex("40", 64),
        primary_name_padded_address_hex("0xa2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_left_pad_hex("20", 64),
        primary_name_padded_address_hex(address),
    ))
}

fn primary_name_reverse_name_response(name: &str) -> Value {
    let name_hex = name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let padded_name_hex_len = name_hex.len().next_multiple_of(64);
    json!(format!(
        "0x{}{}{}",
        primary_name_left_pad_hex("20", 64),
        primary_name_left_pad_hex(&format!("{:x}", name.len()), 64),
        format!("{name_hex:0<padded_name_hex_len$}"),
    ))
}

fn primary_name_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    primary_name_left_pad_hex(stripped, 64)
}

fn primary_name_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
}

async fn spawn_primary_name_mock_rpc(
    responses: Vec<Value>,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            write_primary_name_mock_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn spawn_hanging_primary_name_rpc()
-> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind hanging mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging mock primary-name RPC request")?;
        read_primary_name_mock_rpc_request(&mut socket).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    });
    Ok((url, handle))
}

async fn spawn_primary_name_mock_rpc_with_last_response_gate(
    responses: Vec<Value>,
) -> Result<(
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<Vec<Value>>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind gated mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let (request_reached_tx, request_reached_rx) = tokio::sync::oneshot::channel();
    let (release_response_tx, release_response_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let response_count = responses.len();
        let mut requests = Vec::new();
        let mut request_reached_tx = Some(request_reached_tx);
        let mut release_response_rx = Some(release_response_rx);
        for (index, response) in responses.into_iter().enumerate() {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept gated mock primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            if index + 1 == response_count {
                request_reached_tx
                    .take()
                    .context("gated RPC reached its last request twice")?
                    .send(())
                    .map_err(|_| anyhow::anyhow!("gated RPC request receiver dropped"))?;
                release_response_rx
                    .take()
                    .context("gated RPC release receiver missing")?
                    .await
                    .context("gated RPC release sender dropped")?;
            }
            write_primary_name_mock_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, request_reached_rx, release_response_tx, handle))
}

async fn read_primary_name_mock_rpc_request(
    socket: &mut tokio::net::TcpStream,
) -> Result<Value> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(body_start) = primary_name_mock_header_end(&buffer) {
            let headers = std::str::from_utf8(&buffer[..body_start])
                .context("mock primary-name RPC request headers were not utf8")?;
            break (body_start, primary_name_mock_content_length(headers)?);
        }
    };
    while buffer.len() < body_start + content_length {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request body")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }
    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("failed to parse mock primary-name RPC request body")
}

fn primary_name_mock_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn primary_name_mock_content_length(headers: &str) -> Result<usize> {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .context("mock primary-name RPC request content-length was invalid")?
        .with_context(|| "mock primary-name RPC request did not include content-length")
}

async fn write_primary_name_mock_rpc_response(
    socket: &mut tokio::net::TcpStream,
    result: Value,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = if let Some(error) = result.get("__rpc_error") {
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": error,
        })
    } else {
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock primary-name RPC response")
}

async fn join_primary_name_mock_rpc_requests(
    handle: tokio::task::JoinHandle<Result<Vec<Value>>>,
) -> Result<Vec<Value>> {
    handle
        .await
        .context("mock primary-name RPC task panicked or was cancelled")?
}

// API family fixtures publish actual normalized events through the production family loop.
// Endpoint assertions remain after removal of the old served-table comparison path.

/// Blocks 200..=241 of ethereum-mainnet (hash `0xhistory{n}`, time 1_700_000_000 + n), the
/// shape the bounded-membership tests use.
const SWITCH_CHAIN: &str = "ethereum-mainnet";
const SWITCH_FIRST_BLOCK: i64 = 200;

/// A name for an event-built family fixture: its surface, its own resource with a token lineage, and an open
/// binding under `arm`, all at the first block. Returns the name id and the resource.
async fn seed_switch_name(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    arm: &str,
) -> Result<(String, Uuid)> {
    seed_switch_name_on(database, name, seed, arm, "ens", SWITCH_CHAIN).await
}

async fn seed_switch_name_on(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    arm: &str,
    namespace: &str,
    chain_id: &str,
) -> Result<(String, Uuid)> {
    let (logical_name_id, namehash) = phase_logical_identity(namespace, name)?;
    let (resource_id, token_lineage_id, surface_binding_id) = (
        Uuid::from_u128(seed),
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
    );
    let hash = format!("0xhistory{SWITCH_FIRST_BLOCK}");
    upsert_test_name_surfaces(
        &database.pool,
        &[NameSurface {
            logical_name_id: logical_name_id.clone(),
            namespace: namespace.to_owned(),
            input_name: name.to_owned(),
            canonical_display_name: name.to_owned(),
            normalized_name: name.to_owned(),
            dns_encoded_name: name.as_bytes().to_vec(),
            namehash: namehash.clone(),
            labelhashes: Vec::new(),
            normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: hash.clone(),
            block_number: SWITCH_FIRST_BLOCK,
            provenance: json!({"seed": "switch_differential"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_token_lineages(
        &database.pool,
        &[TokenLineage {
            token_lineage_id,
            chain_id: chain_id.to_owned(),
            block_hash: hash.clone(),
            block_number: SWITCH_FIRST_BLOCK,
            provenance: json!({"seed": "switch_differential"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: chain_id.to_owned(),
            block_hash: hash.clone(),
            block_number: SWITCH_FIRST_BLOCK,
            provenance: json!({"seed": "switch_differential"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            // The helper derives the name id from `namespace:name`.
            logical_name_id: format!("{namespace}:{name}"),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: arm.to_owned(),
            active_from: OffsetDateTime::from_unix_timestamp(1_700_000_000 + SWITCH_FIRST_BLOCK)?,
            active_to: None,
            chain_id: chain_id.to_owned(),
            block_hash: hash,
            block_number: SWITCH_FIRST_BLOCK,
            provenance: json!({"seed": "switch_differential", "transaction_index": 0,
                               "log_index": 0}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    Ok((logical_name_id, resource_id))
}

/// One event of the differential at `block` and `log` in transaction 0.
#[allow(clippy::too_many_arguments)]
fn switch_event(
    identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    kind: &str,
    family: &str,
    block: i64,
    log: i64,
    after_state: Value,
) -> NormalizedEvent {
    let mut event = v2_history_event(identity, logical_name_id, resource_id, kind, block);
    event.source_family = family.to_owned();
    event.log_index = Some(log);
    event.after_state = after_state;
    event
}

/// Publish the fixture's real family reducers at `target`; a later call follows from the
/// current marker. Tests replacing retained input explicitly reset/rebuild before using it.
async fn publish_test_families(database: &TestDatabase, target: i64) -> Result<()> {
    publish_bounded_membership_at(database, target).await?;
    // Collections also require an Interpret phase that is not redoing history.
    sqlx::query(
        "INSERT INTO chain_phase_state (chain_id, phase_name, phase_status, current_block_number,
             current_block_hash, target_block_number, target_block_hash, input_content_hash,
             started_at, finished_at)
         VALUES ($1, 'interpret', 'completed', $2, $3, $2, $3, $4, now(), now())
         ON CONFLICT (chain_id, phase_name) DO UPDATE SET
             current_block_number = EXCLUDED.current_block_number,
             current_block_hash = EXCLUDED.current_block_hash,
             target_block_number = EXCLUDED.target_block_number,
             target_block_hash = EXCLUDED.target_block_hash",
    )
    .bind(SWITCH_CHAIN)
    .bind(target)
    .bind(format!("0xhistory{target}"))
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.pool)
    .await?;
    publish_test_families_on(&database.pool, SWITCH_CHAIN, target).await
}

/// Follow/rebuild the fixture's real canonical inputs; the family marker owns resume state.
async fn publish_test_families_on(pool: &PgPool, chain: &str, target: i64) -> Result<()> {
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2
         AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    ).bind(chain).bind(target).fetch_one(pool).await?;
    let token = bigname_project::families::input_token(pool, chain).await?;
    let outcome = bigname_project::families::apply(
        pool, chain, &bigname_project::Marker { number: target, hash },
        bigname_project::families::FamilyMode::Normal, &token,
        &bigname_project::families::FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH),
    ).await?;
    anyhow::ensure!(outcome.marker.as_ref().map(|marker| marker.number) == Some(target),
        "the families published {chain} at {target}: {outcome:?}");
    Ok(())
}

/// Read the permanent family endpoint, preserving the full response for contract assertions.
async fn read_family_response(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = v2_get_response(database, uri).await?;
    Ok((response.status(), read_json(response).await?))
}

/// Walk all pages, following the endpoint's own cursors and checking the continuation contract.
async fn read_family_pages(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    read_family_pages_in(database, uri, "").await
}

/// `holder` is the JSON pointer of the object carrying `data` and `page`.
async fn read_family_pages_in(database: &TestDatabase, uri: &str, holder: &str) -> Result<Vec<Value>> {
    let mut pages = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let page_uri = match &next {
            None => uri.to_owned(),
            Some(cursor) => format!("{uri}&cursor={cursor}"),
        };
        let (status, body) = read_family_response(database, &page_uri).await?;
        anyhow::ensure!(status == StatusCode::OK, "{page_uri}: {body:#}");
        let held = body.pointer(holder)
            .with_context(|| format!("{page_uri}: no {holder} in {body:#}"))?;
        next = held["page"]["next_cursor"].as_str().map(str::to_owned);
        assert_eq!(held["page"]["has_more"], json!(next.is_some()), "{page_uri}: {body:#}");
        pages.push(json!({"data": held["data"], "has_more": held["page"]["has_more"],
                          "total_count": held["page"]["total_count"]}));
        if next.is_none() { break; }
        anyhow::ensure!(pages.len() < 100, "{uri}: too many pages");
    }
    Ok(pages)
}

/// Retain the label bytes which Interpret actually observed. An edge can be seeded without
/// this helper when its label preimage has not been observed.
async fn insert_family_label_preimage(pool: &PgPool, raw_label: &[u8]) -> Result<String> {
    let hash = format!("{:#x}", alloy_primitives::keccak256(raw_label));
    let decoded = std::str::from_utf8(raw_label).ok().filter(|label| !label.contains('\0'));
    let normalization_error = match decoded {
        Some(label) => match bigname_domain::normalization::normalize_label_under_suffix(label, &[]) {
            Ok(name) if name.normalized_name == label => None,
            Ok(_) => Some("raw label is not byte-identical to its normalized form".to_owned()),
            Err(error) => Some(error.to_string()),
        },
        None => Some("raw label has no PostgreSQL-safe UTF-8 decoding".to_owned()),
    };
    sqlx::query("INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
        normalized_under_version, normalization_error, source_kind, source_priority)
        VALUES ($1, $2, $3, $4, $5, $6, 'fixture', 0) ON CONFLICT DO NOTHING")
        .bind(&hash).bind(raw_label).bind(decoded)
        .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION).bind(normalization_error.is_none())
        .bind(normalization_error)
        .execute(pool).await?;
    Ok(hash)
}

/// A normalized registry NewOwner observation. It creates the retained edge only;
/// name bindings, lifecycle and family publication are separate fixture inputs.
#[allow(clippy::too_many_arguments)]
async fn insert_family_registry_child_edge(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    parent_name: &str,
    labelhash: &str,
    owner: &str,
    block: i64,
    hash: &str,
) -> Result<String> {
    let family = match namespace {
        "ens" => "ens_v1_registry_l1",
        "basenames" => "basenames_base_registry",
        _ => anyhow::bail!("registry child fixture supports ENSv1 and Basenames"),
    };
    let node = bigname_lookup::ens_namehash_hex(parent_name)?;
    let child = format!("{:#x}", alloy_primitives::keccak256([
        alloy_primitives::hex::decode(&node)?, alloy_primitives::hex::decode(labelhash)?
    ].concat()));
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let mut event = history_event(&format!("fixture-child-{ordinal}"), None, None, Some(chain), Some(block), Some(hash),
        Some("0xregistry-child"), Some(ordinal), CanonicalityState::Canonical);
    event.namespace = namespace.into(); event.event_kind = "SubregistryChanged".into();
    event.source_family = family.into(); event.before_state = json!({});
    event.after_state = json!({"source_event":"NewOwner", "node":node, "child_node":child,
        "labelhash":labelhash, "owner":owner});
    bigname_storage::insert_normalized_event_fixtures(pool, &[event]).await?;
    Ok(child)
}
