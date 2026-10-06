//! Declared resolution topology on the same snapshot as the composed name and inventory.
//! The five arms are read in a fixed order. No provider result is used or retained.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::{
    NameCurrentRow,
    families::{
        records::{FamilyAttribution, FamilyRecordInventory, load_family_record_inventories_on},
        topology::{load_family_wildcard_source_on, load_name_topology_on},
    },
    name_current::wrapper_expiry::{self, WrapperExpiryKey},
};

/// Enrich every composed row with its declared topology. The record inventories the topology
/// reads are read for all rows at once, one read per chain.
pub(super) async fn enrich_all(
    conn: &mut PgConnection,
    rows: &mut BTreeMap<String, NameCurrentRow>,
) -> Result<()> {
    attach_wrapper_expiries(conn, rows).await?;
    let mut wanted: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();
    for row in rows.values() {
        if let Some(resource) = row.serving_resource_id.or(row.resource_id) {
            wanted.entry(chain_of(row)?).or_default().push(resource);
        }
    }
    let mut inventories = BTreeMap::new();
    for (chain_id, resources) in wanted {
        for (resource, inventory) in load_family_record_inventories_on(
            conn,
            &chain_id,
            &resources,
            FamilyAttribution::Given(Default::default()),
        )
        .await?
        {
            inventories.insert((chain_id.clone(), resource), inventory);
        }
    }
    for row in rows.values_mut() {
        enrich(conn, row, &inventories).await?;
    }
    Ok(())
}

/// Write the stored expiry of the name's NameWrapper entry beside the composed wrapper fields:
/// on a row that serves a wrapper state, and on a row whose emancipated or locked wrapper has
/// passed its expiry, which composition masks (`wrapper_masked`) instead because NameWrapper then
/// reports no owner and no fuses for it
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f).
/// Inside
/// `defer_wrapper_expiries` each such row gets a pending marker instead, which the response
/// resolves once for all the rows it serves.
pub(super) async fn attach_wrapper_expiries(
    conn: &mut PgConnection,
    rows: &mut BTreeMap<String, NameCurrentRow>,
) -> Result<()> {
    let mut wanted = BTreeMap::new();
    for row in rows.values().filter(|row| wrapper_flagged(row)) {
        wanted.insert(wrapper_key(row)?, backed(row));
    }
    if wanted.is_empty() {
        return Ok(());
    }
    if wrapper_expiry::deferred() {
        for row in rows.values_mut().filter(|row| wrapper_flagged(row)) {
            let marker = wrapper_expiry::pending_marker(&wrapper_key(row)?, backed(row));
            row.declared_summary[wrapper_expiry::WRAPPER_EXPIRY_PENDING_KEY] = marker;
        }
        return Ok(());
    }
    let served = wrapper_expiry::load_wrapper_expiries(&mut *conn, &wanted).await?;
    for row in rows.values_mut().filter(|row| wrapper_flagged(row)) {
        if let Some(expiry) = served.get(&wrapper_key(row)?) {
            row.declared_summary[wrapper_expiry::WRAPPER_EXPIRY_KEY] = expiry.clone();
        }
    }
    Ok(())
}

fn wrapper_flagged(row: &NameCurrentRow) -> bool {
    backed(row) || row.declared_summary.get("wrapper_masked") == Some(&Value::Bool(true))
}

fn backed(row: &NameCurrentRow) -> bool {
    row.declared_summary.get("wrapper_state").is_some()
}

/// Composition reads the wrapper of the name's binding resource, which is the row's
/// `resource_id` whenever the row is bound.
fn wrapper_key(row: &NameCurrentRow) -> Result<WrapperExpiryKey> {
    Ok(WrapperExpiryKey {
        chain_id: chain_of(row)?,
        resource_id: row.resource_id.with_context(|| {
            format!(
                "composed wrapper of {} has no resource",
                row.logical_name_id
            )
        })?,
    })
}

fn chain_of(row: &NameCurrentRow) -> Result<String> {
    Ok(row.provenance["chain_id"]
        .as_str()
        .context("composed name has no chain")?
        .to_owned())
}

async fn enrich(
    conn: &mut PgConnection,
    row: &mut NameCurrentRow,
    inventories: &BTreeMap<(String, Uuid), FamilyRecordInventory>,
) -> Result<()> {
    let mut topology = match row.binding_kind.map(|kind| kind.as_str()) {
        Some("observed_wildcard_path") => load_name_topology_on(conn, &row.logical_name_id).await?,
        _ => None,
    };
    let chain_id = chain_of(row)?;
    let resource = row.serving_resource_id.or(row.resource_id);
    if let Some(resource) = resource {
        let inventory = inventories.get(&(chain_id.clone(), resource));
        if row.namespace == "ens"
            && let Some(inventory) = inventory
        {
            let resolver = &row.declared_summary["resolver"];
            let ownerless = row.serving_resource_id.is_some();
            let direct = topology.is_none()
                && row
                    .binding_kind
                    .is_some_and(|kind| kind.as_str() == "declared_registry_path")
                && matches!(
                    row.provenance
                        .pointer("/authority_selection/authority_arm")
                        .and_then(Value::as_str),
                    Some("ens_v1" | "ens_v2")
                );
            if (ownerless || direct)
                && resolver["address"].is_string()
                && resolver["chain_id"].is_string()
            {
                topology = Some(shape(
                    row,
                    resource,
                    !ownerless,
                    resolver_hop(
                        row,
                        resource,
                        resolver["chain_id"].clone(),
                        resolver["address"].clone(),
                        resolver["latest_event_kind"].clone(),
                    ),
                    inventory.row.record_version_boundary.clone(),
                ));
            }
        }
        if row.namespace == "basenames"
            && chain_id == "base-mainnet"
            && (row
                .binding_kind
                .is_some_and(|kind| kind.as_str() == "declared_registry_path")
                || row.surface_binding_id.is_none())
        {
            topology = basenames(
                conn,
                row,
                resource,
                inventory.map(|inventory| &inventory.row.record_version_boundary),
            )
            .await?;
        }
    }
    if let Some(topology) = topology {
        let typed: ResolutionTopology = serde_json::from_value(topology)
            .with_context(|| format!("invalid composed topology of {}", row.logical_name_id))?;
        row.declared_summary["topology"] = serde_json::to_value(typed)?;
    }
    Ok(())
}

fn resolver_hop(
    row: &NameCurrentRow,
    resource: Uuid,
    chain: Value,
    address: Value,
    event: Value,
) -> Value {
    json!({"logical_name_id":row.logical_name_id, "namespace":row.namespace,
        "normalized_name":row.normalized_name, "canonical_display_name":row.canonical_display_name,
        "resource_id":resource, "chain_id":chain, "address":address, "latest_event_kind":event})
}

fn shape(
    row: &NameCurrentRow,
    resource: Uuid,
    control: bool,
    resolver: Value,
    boundary: Value,
) -> Value {
    let registry = if control {
        json!([{"logical_name_id":row.logical_name_id, "namespace":row.namespace,
        "normalized_name":row.normalized_name, "canonical_display_name":row.canonical_display_name,
        "namehash":row.namehash, "resource_id":resource, "binding_kind":row.binding_kind.map(|kind|kind.as_str())}])
    } else {
        json!([])
    };
    json!({"registry_path":registry, "subregistry_path":[], "resolver_path":[resolver],
        "wildcard":{"source":null,"matched_labels":[]},
        "version_boundaries":{"topology_version_boundary":boundary,"record_version_boundary":boundary},
        "transport":{"source_chain_id":null,"target_chain_id":null,"contract_address":null,"latest_event_kind":null}})
}

async fn basenames(
    conn: &mut PgConnection,
    row: &mut NameCurrentRow,
    resource: Uuid,
    inventory_boundary: Option<&Value>,
) -> Result<Option<Value>> {
    // The admitted transport resolves Base names through the L1 CCIP entrypoint
    // (upstream: .refs/basenames/src/L1/L1Resolver.sol:L164-L173 @ basenames@1809bbc).
    // Select the Ethereum execution block at source time.
    let publication = super::servable_publication(conn, "base-mainnet").await?;
    let execution = sqlx::query(
        "WITH manifests AS (
            SELECT DISTINCT ON (event.source_manifest_id) event.manifest_version,
                event.after_state, event.raw_fact_ref
            FROM bigname_phase.normalized_events event
            LEFT JOIN bigname_phase.chain_lineage lineage ON lineage.chain_id = event.chain_id
                AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
            WHERE event.namespace = 'basenames' AND event.source_family = 'basenames_execution'
                AND event.chain_id = 'ethereum-mainnet' AND event.event_kind = 'SourceManifestUpdated'
                AND event.source_manifest_id IS NOT NULL
                AND event.canonicality_state IN ('canonical','safe','finalized')
                AND (event.block_hash IS NULL OR lineage.canonicality_state IN ('canonical','safe','finalized'))
                AND (event.block_number IS NULL OR event.block_number <= $1)
            ORDER BY event.source_manifest_id, event.normalized_event_id DESC
        )
        SELECT manifest.manifest_version, lineage.block_number, lineage.block_hash,
            to_jsonb(lineage.block_timestamp) AS timestamp
        FROM manifests manifest
        CROSS JOIN LATERAL (
            SELECT * FROM bigname_phase.chain_lineage
            WHERE chain_id = 'ethereum-mainnet' AND block_timestamp <= $2
                AND canonicality_state IN ('canonical','safe','finalized')
            ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC LIMIT 1
        ) lineage
        WHERE manifest.manifest_version = 2 AND manifest.after_state ->> 'rollout_status' = 'active'
            AND COALESCE(manifest.after_state #>> '{manifest_payload,deployment_epoch}', manifest.raw_fact_ref ->> 'deployment_epoch') = 'basenames_v1'
            AND manifest.after_state #>> '{manifest_payload,capability_flags,verified_resolution,status}' = 'supported'
            AND EXISTS (SELECT 1 FROM jsonb_array_elements(manifest.after_state #> '{manifest_payload,contracts}') declaration
                WHERE declaration ->> 'role' = 'l1_resolver' AND lower(declaration ->> 'address') = '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31')
        LIMIT 1")
        .bind(publication.block_number).bind(publication.block_timestamp)
        .fetch_optional(&mut *conn).await?;
    let Some(execution) = execution else {
        return Ok(None);
    };
    let Some(pointer) = load_family_wildcard_source_on(conn, "base-mainnet", resource).await?
    else {
        return Ok(None);
    };
    let event: Option<(i64,String)> = sqlx::query_as("SELECT normalized_event_id, block_hash FROM bigname_phase.normalized_events WHERE event_identity = $1")
        .bind(pointer.boundary_position["event_identity"].as_str()).fetch_optional(&mut *conn).await?;
    let Some((event_id, block_hash)) = event else {
        return Ok(None);
    };
    let version = pointer.boundary_kind == "RecordVersionChanged";
    let boundary = json!({"logical_name_id":row.logical_name_id,"resource_id":resource,
        "normalized_event_id":if version {json!(event_id)} else {Value::Null},
        "event_kind":if version {json!(pointer.boundary_kind)} else {Value::Null},
        "chain_position":{"chain_id":"base-mainnet","block_number":pointer.boundary_position["block_number"],"block_hash":block_hash,"timestamp":pointer.boundary_block_timestamp}});
    let resolver = resolver_hop(
        row,
        resource,
        json!("base-mainnet"),
        json!(pointer.nonzero_resolver_address),
        json!("ResolverChanged"),
    );
    let mut topology = shape(
        row,
        resource,
        row.surface_binding_id.is_some(),
        resolver,
        boundary,
    );
    if let Some(boundary) = inventory_boundary {
        topology["version_boundaries"]["record_version_boundary"] = boundary.clone();
    }
    topology["transport"] = json!({"source_chain_id":"base-mainnet","target_chain_id":"ethereum-mainnet","contract_address":"0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31","latest_event_kind":null});
    row.chain_positions["ethereum"] = json!({"chain_id":"ethereum-mainnet","block_number":execution.try_get::<i64,_>("block_number")?,"block_hash":execution.try_get::<String,_>("block_hash")?,"timestamp":execution.try_get::<Value,_>("timestamp")?});
    row.manifest_version = row
        .manifest_version
        .max(execution.try_get("manifest_version")?);
    row.provenance["manifest_versions"] = json!([{"source_family":"basenames_execution","manifest_version":2,"chain":"ethereum-mainnet","deployment_epoch":"basenames_v1"}]);
    Ok(Some(topology))
}
