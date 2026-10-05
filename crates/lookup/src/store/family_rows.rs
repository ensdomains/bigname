//! Lookup inputs composed on the caller's snapshot, fenced by the family publication.
use super::{
    HeadRow, InventoryRow, NameRow, positions::CapturedPublication,
    textless_name::dns_name_from_preimages,
};
use crate::{LookupError, LookupPosition, Result, error::database};
use bigname_storage::families::{
    name::load_family_name_on,
    records::{FamilyAttribution, load_family_record_inventory_detail_on},
};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

/// A surface's stored wire name (absent when it stores no bytes) and label-hash path, with the
/// chain and publication sequence of the resource it is read with.
type SurfaceIdentity = (Option<Vec<u8>>, Vec<String>, String, String);

fn composition(error: anyhow::Error) -> LookupError {
    if bigname_storage::families::name::is_publication_unavailable(&error) {
        LookupError::stale(error.to_string())
    } else {
        LookupError::database(format!("failed to compose lookup input: {error:#}"))
    }
}

pub(super) async fn load_name(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<NameRow> {
    let row = load_family_name_on(transaction, logical_name_id)
        .await
        .map_err(composition)?
        .filter(|row| row.coverage["status"] == "projected")
        .ok_or_else(|| {
            LookupError::unsupported("verified lookup name is not readable or supported")
        })?;
    let identity: Option<SurfaceIdentity> = sqlx::query_as(
        "SELECT surface.dns_encoded_name, surface.labelhashes, resource.chain_id,
                marker.sequence::text
         FROM name_surfaces surface JOIN resources resource ON resource.resource_id = $2
         JOIN project_family_marker marker ON marker.chain_id = resource.chain_id
         WHERE surface.logical_name_id = $1",
    )
    .bind(logical_name_id)
    .bind(row.serving_resource_id.or(row.resource_id))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load family lookup identity"))?;
    let (stored_dns_name, labelhashes, resource_chain_id, row_xmin) = identity
        .ok_or_else(|| LookupError::unsupported("verified lookup requires a readable resource"))?;
    let dns_encoded_name = match stored_dns_name {
        Some(dns_name) => dns_name,
        // A resolver call needs the bytes of the name, and this surface stores none.
        None => dns_name_from_preimages(transaction, &labelhashes, &row.namehash)
            .await?
            .ok_or_else(|| {
                LookupError::unsupported(
                    "verified lookup requires the verified bytes of every label of the name",
                )
            })?,
    };
    Ok(NameRow {
        logical_name_id: row.logical_name_id,
        namespace: row.namespace,
        raw_name: row.normalized_name,
        namehash: row.namehash,
        dns_encoded_name,
        resource_chain_id,
        declared_summary: row.declared_summary,
        provenance: row.provenance,
        chain_positions: row.chain_positions,
        row_xmin,
    })
}

pub(super) async fn load_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: &str,
    boundary: &Value,
) -> Result<InventoryRow> {
    let resource_id = resource_id
        .parse()
        .map_err(|_| LookupError::unsupported("invalid record boundary resource"))?;
    let identity: Option<(String,String,i64,String)> = sqlx::query_as(
        "SELECT resource.chain_id, marker.sequence::text, marker.current_block_number, marker.current_block_hash FROM resources resource
         JOIN project_family_marker marker ON marker.chain_id = resource.chain_id
         WHERE resource.resource_id = $1 AND marker.state = 'live' AND marker.current_block_number IS NOT NULL")
        .bind(resource_id).fetch_optional(&mut **transaction).await.map_err(database("load family inventory publication"))?;
    let (chain_id, row_xmin, block_number, block_hash) =
        identity.ok_or_else(|| LookupError::stale("record family publication unavailable"))?;
    let inventory = load_family_record_inventory_detail_on(
        transaction,
        &chain_id,
        resource_id,
        FamilyAttribution::Omit,
    )
    .await
    .map_err(composition)?
    .filter(|inventory| inventory.row.record_version_boundary == *boundary)
    .ok_or_else(|| {
        LookupError::unsupported("verified lookup requires an indexed record boundary")
    })?;
    let row = inventory.row;
    Ok(InventoryRow {
        resource_id: row.resource_id.to_string(),
        record_version_boundary_key: inventory.record_version_boundary_key,
        entries: row.entries,
        provenance: row.provenance,
        coverage: row.coverage,
        chain_positions: json!({"target_block_number":block_number,"target_block_hash":block_hash}),
        row_xmin,
    })
}

pub(super) async fn publication(
    transaction: &mut Transaction<'_, Postgres>,
    head: &HeadRow,
    lag_tolerance_blocks: i64,
) -> Result<CapturedPublication> {
    let family: Option<(Value, i64, String, String)> = sqlx::query_as(
        "SELECT jsonb_build_object('sequence',marker.sequence::text,'block_number',marker.current_block_number,
            'block_hash',marker.current_block_hash,'input_content_hash',marker.input_content_hash),
            marker.current_block_number, marker.current_block_hash, to_char(lineage.block_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
         FROM project_family_marker marker JOIN chain_lineage lineage ON lineage.chain_id = marker.chain_id
            AND lineage.block_number = marker.current_block_number AND lineage.block_hash = marker.current_block_hash
            AND lineage.canonicality_state IN ('canonical','safe','finalized')
         WHERE marker.chain_id = $1 AND marker.state = 'live' AND marker.input_content_hash = $4
            AND $2 - marker.current_block_number BETWEEN 0 AND $5
            AND (marker.current_block_number <> $2 OR marker.current_block_hash = $3)
            AND NOT EXISTS (
                SELECT 1 FROM chain_phase_state input_phase
                WHERE input_phase.chain_id = marker.chain_id
                  AND input_phase.phase_name IN ('interpret', 'project')
                  AND input_phase.redo_in_progress
                  AND COALESCE(input_phase.redo_requested_from_block_number, input_phase.redo_from_block_number) <= marker.current_block_number
            )")
        .bind(&head.chain_id).bind(head.block_number).bind(&head.block_hash)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .bind(lag_tolerance_blocks)
        .fetch_optional(&mut **transaction).await.map_err(database("load lookup family publication"))?
        ;
    let (family, block_number, block_hash, timestamp) = family.ok_or_else(|| {
        LookupError::stale(format!(
            "owned key families have not reached the newest processed {} block",
            head.chain_id
        ))
    })?;
    let position = LookupPosition {
        chain_id: head.chain_id.clone(),
        block_number,
        block_hash,
        timestamp,
    };
    Ok(CapturedPublication { family, position })
}
