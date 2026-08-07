use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    AddressNameRelation, IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityRecordInventoryRow, NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow,
    SurfaceBindingKind,
    address_names::{
        DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS, DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    },
    name_current::{DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER},
    record_inventory::{DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER, RESOURCE_CANONICALITY_JOINS},
};

pub async fn load_phase_identity_records_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    load_phase_identity_records(pool, logical_name_ids, true).await
}

pub async fn load_phase_identity_name_feed_records_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    load_phase_identity_records(pool, logical_name_ids, false).await
}

async fn load_phase_identity_records(
    pool: &PgPool,
    logical_name_ids: &[String],
    include_inventory: bool,
) -> Result<Vec<IdentityNameRecordRow>> {
    let requested = dedupe(logical_name_ids);
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let name_rows = load_phase_identity_name_rows(pool, &requested).await?;
    let relations = load_phase_relations(pool, &requested).await?;
    let inventories = if include_inventory {
        load_phase_inventories(pool, name_rows.values()).await?
    } else {
        BTreeMap::new()
    };

    Ok(requested
        .into_iter()
        .filter_map(|logical_name_id| {
            let row = name_rows.get(&logical_name_id)?.clone();
            let record_inventory_current = row.resource_id.and_then(|resource_id| {
                select_phase_inventory(inventories.get(&resource_id)?, &row.declared_summary)
            });
            Some(IdentityNameRecordRow {
                row,
                record_inventory_current,
                relations: relations.get(&logical_name_id).cloned().unwrap_or_default(),
            })
        })
        .collect())
}

pub async fn load_phase_name_current_rows_by_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    let rows = load_phase_name_rows(pool, logical_name_ids).await?;
    rows.into_iter()
        .map(|row| {
            let phase_id: String = row.try_get("logical_name_id")?;
            Ok((phase_id, decode_name_current(row)?))
        })
        .collect()
}

pub async fn load_phase_resolver_bound_name_rows(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namespace: Option<&str>,
    cursor: Option<&NameCurrentListCursor>,
    limit: i64,
) -> Result<Vec<NameCurrentRow>> {
    let cursor_values = cursor
        .map(|cursor| match &cursor.sort_value {
            NameCurrentListCursorValue::Name(_) => Ok((
                cursor.normalized_name.as_str(),
                cursor.namespace.as_str(),
                cursor.namehash.as_str(),
            )),
            _ => anyhow::bail!("phase resolver bound-name cursor must use name ordering"),
        })
        .transpose()?;
    let query = format!(
        r#"
        SELECT nc.logical_name_id, nc.namespace, nc.raw_name, nc.namehash,
               nc.surface_binding_id, nc.resource_id, nc.token_lineage_id,
               nc.binding_kind, nc.declared_summary, nc.support_status,
               nc.unsupported_reason, nc.provenance, nc.chain_positions,
               nc.canonicality_summary, nc.manifest_version,
               nc.last_recomputed_at
        FROM bigname_phase.name_current nc
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN bigname_phase.resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN bigname_phase.surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN bigname_phase.token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
        WHERE nc.support_status IN ('supported', 'unsupported')
          {DEFAULT_NAME_CURRENT_READ_FILTER}
          AND nc.declared_summary #>> '{{resolver,chain_id}}' = $1
          AND lower(nc.declared_summary #>> '{{resolver,address}}') = lower($2)
          AND ($3::TEXT IS NULL OR nc.namespace = $3)
          AND (
              $4::TEXT IS NULL
              OR (nc.raw_name, nc.namespace, nc.namehash) > ($4, $5, $6)
          )
        ORDER BY nc.raw_name, nc.namespace, nc.namehash
        LIMIT $7
        "#
    );
    let rows = sqlx::query(&query)
        .bind(chain_id)
        .bind(resolver_address)
        .bind(namespace)
        .bind(cursor_values.map(|values| values.0))
        .bind(cursor_values.map(|values| values.1))
        .bind(cursor_values.map(|values| values.2))
        .bind(limit)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load phase bound-name ids for resolver {chain_id}:{resolver_address}"
            )
        })?;
    rows.into_iter().map(decode_name_current).collect()
}

async fn load_phase_identity_name_rows(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, IdentityNameCurrentRow>> {
    let rows = load_phase_name_rows(pool, logical_name_ids).await?;
    rows.into_iter()
        .map(|row| {
            let phase_id: String = row.try_get("logical_name_id")?;
            Ok((phase_id, decode_identity_name(row)?))
        })
        .collect()
}

async fn load_phase_name_rows(pool: &PgPool, logical_name_ids: &[String]) -> Result<Vec<PgRow>> {
    if logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }
    let query = format!(
        r#"
        SELECT nc.logical_name_id, nc.namespace, nc.raw_name, nc.namehash,
               nc.surface_binding_id, nc.resource_id, nc.token_lineage_id,
               nc.binding_kind, nc.declared_summary, nc.support_status,
               nc.unsupported_reason, nc.provenance, nc.chain_positions,
               nc.canonicality_summary, nc.manifest_version,
               nc.last_recomputed_at
        FROM bigname_phase.name_current nc
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN bigname_phase.resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN bigname_phase.surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN bigname_phase.token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
        WHERE nc.logical_name_id = ANY($1::TEXT[])
          AND nc.support_status IN ('supported', 'unsupported')
          {DEFAULT_NAME_CURRENT_READ_FILTER}
        ORDER BY nc.logical_name_id
        "#
    );
    sqlx::query(&query)
        .bind(logical_name_ids)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load {} phase name rows", logical_name_ids.len()))
}

fn decode_identity_name(row: PgRow) -> Result<IdentityNameCurrentRow> {
    let phase_id: String = row.try_get("logical_name_id")?;
    let resource_id: Option<Uuid> = row.try_get("resource_id")?;
    let declared_summary: Value = row.try_get("declared_summary")?;
    let raw_name: String = row.try_get("raw_name")?;
    let normalized = normalize_phase_name(&phase_id, &raw_name)?;
    let labelhash = phase_labelhash(&normalized);
    let labelhash_count = i32::try_from(normalized.normalized_labels.len()).ok();
    Ok(IdentityNameCurrentRow {
        logical_name_id: phase_id,
        namespace: row.try_get("namespace")?,
        canonical_display_name: normalized.canonical_display_name,
        normalized_name: normalized.normalized_name,
        namehash: row.try_get("namehash")?,
        labelhash,
        labelhash_count,
        resource_id,
        record_inventory_boundary_key: None,
        coverage: phase_coverage(&row)?,
        declared_summary,
        chain_positions: row.try_get("chain_positions")?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}

fn decode_name_current(row: PgRow) -> Result<NameCurrentRow> {
    let phase_id: String = row.try_get("logical_name_id")?;
    let raw_name: String = row.try_get("raw_name")?;
    let normalized = normalize_phase_name(&phase_id, &raw_name)?;
    let binding_kind = row
        .try_get::<Option<String>, _>("binding_kind")?
        .map(|value| SurfaceBindingKind::parse(&value))
        .transpose()?;
    Ok(NameCurrentRow {
        logical_name_id: phase_id,
        namespace: row.try_get("namespace")?,
        canonical_display_name: normalized.canonical_display_name,
        normalized_name: normalized.normalized_name,
        namehash: row.try_get("namehash")?,
        surface_binding_id: row.try_get("surface_binding_id")?,
        resource_id: row.try_get("resource_id")?,
        token_lineage_id: row.try_get("token_lineage_id")?,
        binding_kind,
        declared_summary: row.try_get("declared_summary")?,
        provenance: row.try_get("provenance")?,
        coverage: phase_coverage(&row)?,
        chain_positions: row.try_get("chain_positions")?,
        canonicality_summary: row.try_get("canonicality_summary")?,
        manifest_version: row.try_get("manifest_version")?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}

fn phase_coverage(row: &PgRow) -> Result<Value> {
    let status: String = row.try_get("support_status")?;
    let reason: Option<String> = row.try_get("unsupported_reason")?;
    Ok(if status == "supported" {
        json!({"status": "projected", "exhaustiveness": "not_asserted"})
    } else {
        json!({
            "status": "unsupported",
            "exhaustiveness": "not_asserted",
            "unsupported_reason": reason,
        })
    })
}

fn normalize_phase_name(
    logical_name_id: &str,
    raw_name: &str,
) -> Result<bigname_domain::normalization::NormalizedEnsName> {
    bigname_domain::normalization::normalize_name(raw_name).with_context(|| {
        format!("phase name row {logical_name_id} has an unreadable active raw_name")
    })
}

fn phase_labelhash(
    normalized: &bigname_domain::normalization::NormalizedEnsName,
) -> Option<String> {
    normalized.normalized_labels.first().map(|label| {
        format!(
            "0x{}",
            alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
        )
    })
}

async fn load_phase_inventories<'a>(
    pool: &PgPool,
    names: impl Iterator<Item = &'a IdentityNameCurrentRow>,
) -> Result<BTreeMap<Uuid, Vec<(Value, IdentityRecordInventoryRow)>>> {
    let resource_ids = names
        .filter_map(|row| row.resource_id)
        .collect::<BTreeSet<_>>();
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let query = format!(
        r#"
        SELECT ric.resource_id, ric.record_version_boundary,
               ric.entries, ric.unsupported_families,
               ric.support_status, ric.unsupported_reason,
               ric.chain_positions, ric.last_recomputed_at
        FROM bigname_phase.record_inventory_current ric
        {RESOURCE_CANONICALITY_JOINS}
        WHERE ric.resource_id = ANY($1::UUID[])
          {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        ORDER BY ric.resource_id, ric.record_version_boundary_key
        "#
    );
    let rows = sqlx::query(&query)
        .bind(resource_ids.into_iter().collect::<Vec<_>>())
        .fetch_all(pool)
        .await
        .context("failed to load phase record inventories")?;

    let mut by_resource = BTreeMap::<Uuid, Vec<(Value, IdentityRecordInventoryRow)>>::new();
    for row in rows {
        let resource_id: Uuid = row.try_get("resource_id")?;
        let boundary: Value = row.try_get("record_version_boundary")?;
        by_resource.entry(resource_id).or_default().push((
            boundary,
            IdentityRecordInventoryRow {
                resource_id,
                support_status: row.try_get("support_status")?,
                unsupported_reason: row.try_get("unsupported_reason")?,
                entries: row.try_get("entries")?,
                unsupported_families: row.try_get("unsupported_families")?,
                chain_positions: row.try_get("chain_positions")?,
                last_recomputed_at: row.try_get("last_recomputed_at")?,
            },
        ));
    }
    Ok(by_resource)
}

fn select_phase_inventory(
    rows: &[(Value, IdentityRecordInventoryRow)],
    declared_summary: &Value,
) -> Option<IdentityRecordInventoryRow> {
    if rows.len() == 1 {
        return Some(rows[0].1.clone());
    }
    let boundary = declared_summary.pointer("/topology/version_boundaries/record_version_boundary");
    match boundary {
        Some(boundary) => rows
            .iter()
            .find(|(candidate, _)| candidate == boundary)
            .map(|(_, row)| row.clone()),
        None => None,
    }
}

async fn load_phase_relations(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, Vec<IdentityAddressRelationRow>>> {
    let query = format!(
        r#"
        SELECT anc.address, anc.logical_name_id, anc.relation,
               anc.chain_positions
        FROM bigname_phase.address_names_current anc
        {DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS}
        WHERE anc.logical_name_id = ANY($1::TEXT[])
          AND anc.support_status = 'supported'
          {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
        ORDER BY anc.logical_name_id, anc.address, anc.relation
        "#
    );
    let rows = sqlx::query(&query)
        .bind(logical_name_ids)
        .fetch_all(pool)
        .await
        .context("failed to load phase address-name relations")?;
    let mut grouped = BTreeMap::<String, Vec<IdentityAddressRelationRow>>::new();
    for row in rows {
        let logical_name_id: String = row.try_get("logical_name_id")?;
        let relation = match row.try_get::<String, _>("relation")?.as_str() {
            "registrant" => AddressNameRelation::Registrant,
            "token_holder" => AddressNameRelation::TokenHolder,
            "effective_controller" => AddressNameRelation::EffectiveController,
            value => bail!("unknown phase address-name relation {value}"),
        };
        grouped
            .entry(logical_name_id.clone())
            .or_default()
            .push(IdentityAddressRelationRow {
                address: row.try_get::<String, _>("address")?.to_ascii_lowercase(),
                logical_name_id,
                relation,
                chain_positions: row.try_get("chain_positions")?,
            });
    }
    Ok(grouped)
}

fn dedupe(values: &[String]) -> Vec<String> {
    values
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
