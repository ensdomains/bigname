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
             WHERE namespace = $1 AND lower(raw_name) = lower($2)
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
                resource_id, token_lineage_id, binding_kind, declared_summary,
                support_status, unsupported_reason, provenance, chain_positions,
                canonicality_summary, manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (logical_name_id) DO UPDATE SET
                raw_name = EXCLUDED.raw_name,
                surface_binding_id = EXCLUDED.surface_binding_id,
                resource_id = EXCLUDED.resource_id,
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
        .bind(&row.canonical_display_name)
        .bind(namehash)
        .bind(row.surface_binding_id)
        .bind(row.resource_id)
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
             WHERE resource_id = $1
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
        let (logical_name_id, namehash) =
            phase_logical_identity(&row.namespace, &row.normalized_name)?;
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
        .bind(&row.canonical_display_name)
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

async fn upsert_phase_children_current_rows(
    pool: &PgPool,
    rows: &[bigname_storage::ChildrenCurrentRow],
) -> Result<Vec<bigname_storage::ChildrenCurrentRow>> {
    for row in rows {
        let parent_name = row
            .parent_logical_name_id
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(&row.parent_logical_name_id);
        let (parent_logical_name_id, _) =
            phase_logical_identity(&row.namespace, parent_name)?;
        let (child_logical_name_id, namehash) =
            phase_logical_identity(&row.namespace, &row.normalized_name)?;
        let chain_id = phase_projection_source_position(&row.chain_positions)?
            .get("chain_id")
            .and_then(Value::as_str)
            .context("children_current fixture position must include chain_id")?
            .to_owned();
        let (target_block_number, target_block_hash) =
            phase_projection_target_for_chain(pool, &chain_id, &row.chain_positions).await?;
        let mut provenance = row.provenance.clone();
        provenance
            .as_object_mut()
            .context("children_current fixture provenance must be an object")?
            .insert("chain_id".to_owned(), json!(chain_id));
        let chain_positions =
            phase_flat_projection_position(target_block_number, &target_block_hash);
        let canonicality_summary = json!({
            "state": "canonical",
            "target_block_number": target_block_number,
            "target_block_hash": target_block_hash,
        });
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.children_current (
                parent_logical_name_id, child_logical_name_id, surface_class,
                namespace, raw_name, decoded_name, namehash, labelhash, owner,
                registrant, provenance, chain_positions, canonicality_summary,
                manifest_version, last_recomputed_at
            )
            VALUES ($1, $2, $3, $4, convert_to($5, 'UTF8'), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (parent_logical_name_id, child_logical_name_id, surface_class)
            DO UPDATE SET
                raw_name = EXCLUDED.raw_name,
                decoded_name = EXCLUDED.decoded_name,
                owner = EXCLUDED.owner,
                registrant = EXCLUDED.registrant,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = EXCLUDED.last_recomputed_at
            "#,
        )
        .bind(parent_logical_name_id)
        .bind(child_logical_name_id)
        .bind(&row.surface_class)
        .bind(&row.namespace)
        .bind(&row.canonical_display_name)
        .bind(namehash)
        .bind(row.labelhash.as_deref().unwrap_or(&row.namehash))
        .bind(&row.owner)
        .bind(&row.registrant)
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
        .bind(row.scope.detail())
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

/// Authority kinds the permission projection builder treats as projected authority.
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
            ("supported", None)
        }
        Some("wrapper") => (
            "unsupported",
            Some("ensv1_wrapper_holder_permissions_not_projected"),
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
            manifest_version, last_recomputed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (resource_id) DO UPDATE SET
            authority_kind = EXCLUDED.authority_kind,
            root_resource_id = EXCLUDED.root_resource_id,
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

async fn upsert_primary_name_current_rows(
    pool: &PgPool,
    rows: &[PrimaryNameCurrentRow],
) -> Result<()> {
    for row in rows {
        let raw_claim_name = matches!(
            row.claim_status,
            PrimaryNameClaimStatus::Success | PrimaryNameClaimStatus::InvalidName
        )
        .then_some(row.raw_claim_name.as_ref())
        .flatten();
        let claim_provenance =
            phase_primary_claim_provenance(pool, &row.namespace, &row.claim_provenance).await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.primary_names_current (
                address, coin_type, namespace, claim_status, raw_claim_name,
                claim_name_is_normalized, unsupported_reason, claim_provenance
            )
            VALUES ($1, $2, $3, $4, $5, false, $6, $7)
            ON CONFLICT (address, coin_type, namespace) DO UPDATE SET
                claim_status = EXCLUDED.claim_status,
                raw_claim_name = EXCLUDED.raw_claim_name,
                claim_name_is_normalized = EXCLUDED.claim_name_is_normalized,
                unsupported_reason = EXCLUDED.unsupported_reason,
                claim_provenance = EXCLUDED.claim_provenance
            "#,
        )
        .bind(row.address.to_ascii_lowercase())
        .bind(&row.coin_type)
        .bind(&row.namespace)
        .bind(row.claim_status.as_str())
        .bind(raw_claim_name)
        .bind((row.claim_status == PrimaryNameClaimStatus::Unsupported).then_some("unsupported"))
        .bind(claim_provenance)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn upsert_primary_name_current_snapshots(
    pool: &PgPool,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<()> {
    for snapshot in snapshots {
        let raw_claim_name = matches!(
            snapshot.row.claim_status,
            PrimaryNameClaimStatus::Success | PrimaryNameClaimStatus::InvalidName
        )
        .then(|| {
            snapshot
                .normalized_claim_name
                .as_ref()
                .or(snapshot.row.raw_claim_name.as_ref())
        })
        .flatten();
        let claim_provenance = phase_primary_claim_provenance(
            pool,
            &snapshot.row.namespace,
            &snapshot.row.claim_provenance,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.primary_names_current (
                address, coin_type, namespace, claim_status, raw_claim_name,
                claim_name_is_normalized, unsupported_reason, claim_provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (address, coin_type, namespace) DO UPDATE SET
                claim_status = EXCLUDED.claim_status,
                raw_claim_name = EXCLUDED.raw_claim_name,
                claim_name_is_normalized = EXCLUDED.claim_name_is_normalized,
                unsupported_reason = EXCLUDED.unsupported_reason,
                claim_provenance = EXCLUDED.claim_provenance
            "#,
        )
        .bind(snapshot.row.address.to_ascii_lowercase())
        .bind(&snapshot.row.coin_type)
        .bind(&snapshot.row.namespace)
        .bind(snapshot.row.claim_status.as_str())
        .bind(raw_claim_name)
        .bind(raw_claim_name.is_some() && snapshot.claim_name_is_normalized)
        .bind(
            (snapshot.row.claim_status == PrimaryNameClaimStatus::Unsupported)
                .then_some("unsupported"),
        )
        .bind(claim_provenance)
        .execute(pool)
        .await?;
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
        let mut database = Self::new(false).await?;
        database
            .database
            .apply_migrations(
                &bigname_storage::MIGRATOR,
                "failed to apply checked-in migrations for API tests",
            )
            .await?;
        database.initialize_lookup_schema().await?;
        database.lookup_pool = database.open_lookup_pool().await?;
        database.pool = database.lookup_pool.clone();
        Ok(database)
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

    async fn seed_name_current_binding_migrated(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        upsert_phase_raw_blocks(
            &self.pool,
            &[
                raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
                raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
                raw_block("ethereum-mainnet", "0xbinding", None, 100, 1_717_171_700),
            ],
        )
        .await?;
        upsert_test_name_surfaces(&self.pool, &[name_surface(logical_name_id)]).await?;
        upsert_test_token_lineages(
            &self.pool,
            &[address_name_token_lineage(
                token_lineage_id,
                "0xresource",
                99,
            )],
        )
        .await?;
        upsert_test_resources(
            &self.pool,
            &[address_name_resource(
                resource_id,
                Some(token_lineage_id),
                "0xresource",
                99,
            )],
        )
        .await?;
        upsert_test_surface_bindings(
            &self.pool,
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                timestamp(1_717_171_700),
            )],
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
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract')",
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    let manifest_payload = if resolution_capability {
        json!({
            "capability_flags": {
                "verified_resolution": { "status": "supported" }
            }
        })
    } else {
        json!({})
    };
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', $1, 'ethereum-mainnet', 'api-test', 'active', 'test', $2, $3)
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(format!("test/ens/{source_family}.toml"))
    .bind(manifest_payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract', $2, $3, $4, $2, 'none')",
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await?;
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
    let namehash = bigname_lookup::ens_namehash_hex("alice.eth")?;
    let logical_name_id = format!("ens:{namehash}");
    let resource_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0101);
    let binding_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0102);
    let positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp
        }
    });
    let boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": 1,
        "event_kind": "ResolverChanged",
        "chain_position": positions["ethereum"]
    });
    let topology = json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "resource_id": resource_id,
            "chain_id": "ethereum-mainnet",
            "address": "0x1000000000000000000000000000000000000001"
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": { "record_version_boundary": boundary },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null
        }
    });
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ethereum-mainnet', $2, $3, 'canonical')",
    )
    .bind(resource_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', 'alice.eth', ARRAY['alice.eth'], $2, $3, ARRAY[$3], 'test',
                 'active', 'ethereum-mainnet', $4, $5, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(b"\x05alice\x03eth\0".as_slice())
    .bind(&namehash)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, active_from,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, 'declared_registry_path', $4::timestamptz,
                 'ethereum-mainnet', $5, $6, 'canonical')",
    )
    .bind(binding_id)
    .bind(&logical_name_id)
    .bind(resource_id)
    .bind(timestamp)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_current
            (logical_name_id, namespace, raw_name, namehash, surface_binding_id,
             resource_id, binding_kind, declared_summary, support_status,
             provenance, chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, 'ens', 'alice.eth', $2, $3, $4, 'declared_registry_path',
                 jsonb_build_object('topology', $5::jsonb), 'supported', $6, $7, $8, 1)",
    )
    .bind(&logical_name_id)
    .bind(&namehash)
    .bind(binding_id)
    .bind(resource_id)
    .bind(&topology)
    .bind(json!({ "chain_id": "ethereum-mainnet" }))
    .bind(&positions)
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, record_version_boundary,
             selectors, unsupported_families, entries, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, $2, $3, $4, '[]', $5, 'supported', $6, $7,
                 $8, 1)",
    )
    .bind(resource_id)
    .bind(bigname_storage::record_version_boundary_storage_key(
        &boundary,
        resource_id,
    )?)
    .bind(&boundary)
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "cacheable": true
    }]))
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "success",
        "value": { "coin_type": "60", "value": indexed_address }
    }]))
    .bind(json!({ "chain_id": "ethereum-mainnet" }))
    .bind(json!({
        "target_block_number": block_number,
        "target_block_hash": block_hash
    }))
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(pool)
    .await?;
    Ok(namehash)
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
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract')",
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (2, 'basenames', 'basenames_execution', 'ethereum-mainnet',
                 'api-test', 'active', 'test', 'test/basenames/execution.toml', $1)
         RETURNING manifest_id",
    )
    .bind(json!({
        "capability_flags": {
            "verified_resolution": { "status": "supported" }
        }
    }))
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract', 'l1_resolver', $2, $3,
                 'l1_resolver', 'none')",
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(l1_resolver)
    .execute(pool)
    .await?;

    let namehash = bigname_lookup::ens_namehash_hex("alice.base.eth")?;
    let logical_name_id = format!("basenames:{namehash}");
    let resource_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0201);
    let binding_id = Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0202);
    let positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "block_hash": base_block_hash,
            "timestamp": timestamp
        },
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": ethereum_block_hash,
            "timestamp": timestamp
        }
    });
    let boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": 1,
        "event_kind": "ResolverChanged",
        "chain_position": positions["base"]
    });
    let topology = json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "resource_id": resource_id,
            "chain_id": "base-mainnet",
            "address": "0x1000000000000000000000000000000000000001"
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": { "record_version_boundary": boundary },
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": l1_resolver,
            "latest_event_kind": "ResolverChanged"
        }
    });
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'base-mainnet', $2, $3, 'canonical')",
    )
    .bind(resource_id)
    .bind(base_block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'basenames', 'alice.base.eth', ARRAY['alice.base.eth'], $2, $3,
                 ARRAY[$3], 'test', 'active', 'base-mainnet', $4, $5, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(b"\x05alice\x04base\x03eth\0".as_slice())
    .bind(&namehash)
    .bind(base_block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, active_from,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, 'declared_registry_path', $4::timestamptz,
                 'base-mainnet', $5, $6, 'canonical')",
    )
    .bind(binding_id)
    .bind(&logical_name_id)
    .bind(resource_id)
    .bind(timestamp)
    .bind(base_block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_current
            (logical_name_id, namespace, raw_name, namehash, surface_binding_id,
             resource_id, binding_kind, declared_summary, support_status,
             provenance, chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, 'basenames', 'alice.base.eth', $2, $3, $4,
                 'declared_registry_path', jsonb_build_object('topology', $5::jsonb),
                 'supported', $6, $7, $8, 2)",
    )
    .bind(&logical_name_id)
    .bind(&namehash)
    .bind(binding_id)
    .bind(resource_id)
    .bind(&topology)
    .bind(json!({ "chain_id": "base-mainnet" }))
    .bind(&positions)
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": base_block_hash,
    }))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, record_version_boundary,
             selectors, unsupported_families, entries, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, $2, $3, $4, '[]', $5, 'supported', $6, $7,
                 $8, 2)",
    )
    .bind(resource_id)
    .bind(bigname_storage::record_version_boundary_storage_key(
        &boundary,
        resource_id,
    )?)
    .bind(&boundary)
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "cacheable": true
    }]))
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "success",
        "value": { "coin_type": "60", "value": indexed_address }
    }]))
    .bind(json!({ "chain_id": "base-mainnet" }))
    .bind(json!({
        "target_block_number": block_number,
        "target_block_hash": base_block_hash
    }))
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": base_block_hash,
    }))
    .execute(pool)
    .await?;
    Ok(namehash)
}

async fn seed_schema_v2_ens_primary_name_authority(
    pool: &PgPool,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    seed_schema_v2_ens_lookup_head(pool, block_number, block_hash, timestamp).await?;
    seed_schema_v2_ens_manifest(
        pool,
        "ens_v1_registry_l1",
        "registry",
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0104),
        false,
    )
    .await?;
    seed_schema_v2_ens_manifest(
        pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0105),
        true,
    )
    .await
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
        .bind(&row.canonical_display_name)
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
                active_from, active_to, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11::bigname_phase.canonicality_state)
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
            bigname_storage::ResourcePermissionCoverage::authoritative(["permissions_current"])
        }
        Some("wrapper") => bigname_storage::ResourcePermissionCoverage::ensv1_wrapper_holder_permissions_not_projected(),
        _ => bigname_storage::ResourcePermissionCoverage::resource_authority_not_projected(),
    };
    bigname_storage::PermissionsCurrentResourceSummary {
        resource_id,
        authority_kind,
        root_resource_id: None,
        coverage,
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

fn worker_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary_with_pointer(
            logical_name_id,
            resource_id,
            Some(1201),
            Some("RecordVersionChanged"),
        ),
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
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true
            }
        ]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1202,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events"
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events"
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1201, 1202],
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
        last_recomputed_at: timestamp(1_717_171_719),
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

fn declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let namespace = parent_logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("parent_logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::ChildrenCurrentRow {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_logical_name_id: child_logical_name_id.to_owned(),
        surface_class: "declared".to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        labelhash: labelhash_for_display_name(display_name),
        owner: None,
        registrant: None,
        provenance: json!({
            "normalized_event_ids": [normalized_event_id],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "derivation_kind": "children_current_rebuild",
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xblock{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_172_000 + block_number),
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

fn source_family_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "basenames_base_registry",
        _ => "ens_v1_registry_l1",
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
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);
    bigname_storage::AddressNameCurrentRow {
        address: address.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
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

fn compact_name_declared_summary(
    owner: &str,
    registrant: &str,
    resolver: &str,
    expiry: i64,
    registered_at: &str,
    created_at: &str,
) -> Value {
    json!({
        "registration": {
            "status": "active",
            "registrant": registrant,
            "expiry": expiry,
            "registered_at": registered_at,
            "created_at": created_at,
        },
        "control": {
            "registry_owner": owner,
            "registrant": registrant,
            "expiry": expiry,
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": resolver,
            "latest_event_kind": "ResolverChanged",
        }
    })
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

#[allow(clippy::too_many_arguments)]
async fn seed_identity_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    normalized_name: &str,
    namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    block_number: i64,
) -> Result<()> {
    let name_row = address_name_name_current_row(
        logical_name_id,
        display_name,
        normalized_name,
        namehash,
        surface_binding_id,
        resource_id,
        Some(token_lineage_id),
        block_number,
        compact_name_declared_summary(
            address,
            address,
            address,
            1_900_000_000,
            "2026-04-17T00:00:21Z",
            "2026-04-17T00:00:11Z",
        ),
    );
    let publication_positions = name_row.chain_positions.clone();
    let mut inventory = compact_records_inventory_current_row(logical_name_id, resource_id);
    inventory.chain_positions = publication_positions.clone();
    let address_row = address_name_current_row(
        address,
        logical_name_id,
        relation,
        display_name,
        normalized_name,
        namehash,
        surface_binding_id,
        resource_id,
        Some(token_lineage_id),
        block_number,
    );

    if name_row.namespace == "basenames" {
        database
            .seed_name_current_binding(
                logical_name_id,
                &name_row.namespace,
                normalized_name,
                display_name,
                namehash,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
    } else {
        database
            .seed_name_current_binding_migrated(
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
    }
    database.insert_name_current_row(name_row.clone()).await?;
    database
        .insert_record_inventory_current_row(inventory.clone())
        .await?;
    upsert_phase_address_names_current_rows(
        &database.pool,
        std::slice::from_ref(&address_row),
    )
    .await?;
    seed_phase_identity_name(
        database,
        display_name,
        normalized_name,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        address,
        relation,
        &name_row.declared_summary,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_phase_identity_name(
    database: &TestDatabase,
    display_name: &str,
    normalized_name: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    declared_summary: &Value,
) -> Result<()> {
    let projected_namespace: Option<String> = sqlx::query_scalar(
        "SELECT namespace FROM bigname_phase.name_current WHERE resource_id = $1 LIMIT 1",
    )
    .bind(resource_id)
    .fetch_optional(&database.lookup_pool)
    .await?;
    let namespace = projected_namespace.as_deref().unwrap_or_else(|| {
        if normalized_name.ends_with(".base.eth") && normalized_name != "base.eth" {
            "basenames"
        } else {
            "ens"
        }
    });
    let namehash = bigname_lookup::ens_namehash_hex(normalized_name)?;
    let logical_name_id = format!("{namespace}:{namehash}");
    let normalized = bigname_domain::normalization::normalize_name(display_name)
        .map_err(|error| anyhow::anyhow!(error.message().to_owned()))?;
    let labelhashes = normalized
        .normalized_labels
        .iter()
        .map(|label| Ok(format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))))
        .collect::<Result<Vec<_>>>()?;
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    let (block_hash, block_number, timestamp, timestamp_text): (
        String,
        i64,
        OffsetDateTime,
        String,
    ) = sqlx::query_as(
        r#"
        SELECT head.latest_block_hash, head.latest_block_number, lineage.block_timestamp,
               to_char(
                   lineage.block_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS"Z"'
               )
        FROM chain_heads head
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE head.chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(&database.lookup_pool)
    .await?;
    let slot = if chain_id == "base-mainnet" { "base" } else { "ethereum" };
    let publication_positions = json!({
        slot: {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp_text,
        }
    });
    let mut transaction = database.lookup_pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO token_lineages (
            token_lineage_id, chain_id, block_hash, block_number,
            provenance, canonicality_state
        ) VALUES ($1, $2, $3, $4, '{}'::jsonb, 'finalized')
        ON CONFLICT (token_lineage_id) DO NOTHING
        "#,
    )
    .bind(token_lineage_id)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id, token_lineage_id, chain_id, block_hash, block_number,
            provenance, canonicality_state
        ) VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, 'finalized')
        ON CONFLICT (resource_id) DO NOTHING
        "#,
    )
    .bind(resource_id)
    .bind(token_lineage_id)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
            namehash, labelhashes, normalizer_version, visibility_state,
            normalization_errors, chain_id, block_hash, block_number,
            provenance, canonicality_state
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 'active', '[]'::jsonb,
            $9, $10, $11, '{}'::jsonb, 'finalized'
        ) ON CONFLICT (logical_name_id) DO NOTHING
        "#,
    )
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(&normalized.normalized_name)
    .bind(&normalized.normalized_labels)
    .bind(&normalized.dns_encoded_name)
    .bind(&namehash)
    .bind(labelhashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO surface_bindings (
            surface_binding_id, logical_name_id, resource_id, binding_kind,
            active_from, chain_id, block_hash, block_number, provenance,
            canonicality_state
        ) VALUES (
            $1, $2, $3, 'declared_registry_path', $4, $5, $6, $7,
            '{}'::jsonb, 'finalized'
        ) ON CONFLICT (surface_binding_id) DO NOTHING
        "#,
    )
    .bind(surface_binding_id)
    .bind(&logical_name_id)
    .bind(resource_id)
    .bind(timestamp)
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id, namespace, raw_name, namehash, surface_binding_id,
            resource_id, token_lineage_id, binding_kind, declared_summary,
            support_status, provenance, chain_positions, canonicality_summary,
            manifest_version
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'declared_registry_path', $8,
            'supported', $9, $10, $11, 1
        ) ON CONFLICT (logical_name_id) DO UPDATE SET
            declared_summary = EXCLUDED.declared_summary,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary
        "#,
    )
    .bind(&logical_name_id)
    .bind(namespace)
    .bind(&normalized.normalized_name)
    .bind(&namehash)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(token_lineage_id)
    .bind(declared_summary)
    .bind(json!({ "chain_id": chain_id }))
    .bind(&publication_positions)
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address, logical_name_id, relation, namespace, raw_name, namehash,
            surface_binding_id, resource_id, token_lineage_id, binding_kind,
            support_status, provenance, chain_positions, canonicality_summary,
            manifest_version
        ) VALUES (
            lower($1), $2, $3, $4, $5, $6, $7, $8, $9,
            'declared_registry_path', 'supported', $10, $11, $12, 1
        ) ON CONFLICT (address, logical_name_id, relation) DO UPDATE SET
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary
        "#,
    )
    .bind(address)
    .bind(&logical_name_id)
    .bind(relation.as_str())
    .bind(namespace)
    .bind(&normalized.normalized_name)
    .bind(&namehash)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(token_lineage_id)
    .bind(json!({ "chain_id": chain_id }))
    .bind(phase_flat_projection_position(block_number, &block_hash))
    .bind(json!({
        "state": "canonical_lineage",
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    }))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
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
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    let (block_number, block_hash): (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&database.lookup_pool)
    .await?;
    let claim_provenance = json!({
        "chain_id": chain_id,
        "target_block_number": block_number,
        "target_block_hash": block_hash,
    });
    let status = match claim_status {
        bigname_storage::PrimaryNameClaimStatus::Success => "success",
        bigname_storage::PrimaryNameClaimStatus::NotFound => "not_found",
        bigname_storage::PrimaryNameClaimStatus::Unsupported => "unsupported",
        bigname_storage::PrimaryNameClaimStatus::InvalidName => "invalid_name",
    };
    let unsupported_reason = (status == "unsupported").then_some("unsupported_test_claim");
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address, coin_type, namespace, claim_status, raw_claim_name,
            claim_name_is_normalized, unsupported_reason, claim_provenance
        ) VALUES (lower($1), $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (address, coin_type, namespace) DO UPDATE SET
            claim_status = EXCLUDED.claim_status,
            raw_claim_name = EXCLUDED.raw_claim_name,
            claim_name_is_normalized = EXCLUDED.claim_name_is_normalized,
            unsupported_reason = EXCLUDED.unsupported_reason,
            claim_provenance = EXCLUDED.claim_provenance
        "#,
    )
    .bind(address)
    .bind(coin_type)
    .bind(namespace)
    .bind(status)
    .bind(raw_claim_name)
    .bind(claim_name_is_normalized)
    .bind(unsupported_reason)
    .bind(claim_provenance)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

fn basenames_execution_manifest_version() -> Value {
    json!({
        "source_family": "basenames_execution",
        "manifest_version": 2,
        "chain": "ethereum-mainnet",
        "deployment_epoch": "basenames_v1",
    })
}

fn basenames_dynamic_resolver_record_inventory_boundary(
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
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z",
        }
    })
}

fn basenames_l2resolver_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: basenames_dynamic_resolver_record_inventory_boundary(
            logical_name_id,
            resource_id,
            Some(1201),
            Some("RecordChanged"),
        ),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "cacheable": true,
        }]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1201,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        })),
        entries: json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": "value_not_retained_in_normalized_events",
        }]),
        provenance: json!({
            "normalized_event_ids": [1201],
            "derivation_kind": "record_inventory_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [
                "basenames_base_registry",
                "basenames_base_resolver",
            ],
            "unsupported_reason": null,
            "enumeration_basis": "declared_record_inventory",
        }),
        chain_positions: json!({
            "base-mainnet": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": { "base-mainnet": "finalized" }
        }),
        manifest_version: 6,
        last_recomputed_at: timestamp(1_717_171_719),
    }
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

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result,
    })
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
