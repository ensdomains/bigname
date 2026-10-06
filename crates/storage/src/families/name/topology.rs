//! Declared resolution topology on the same snapshot as the composed name and inventory.
//! The five arms are read in a fixed order. No provider result is used or retained.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{
    NameCurrentRow,
    families::{
        records::{FamilyAttribution, FamilyRecordInventory, load_family_record_inventories_on},
        topology::load_name_topology_on,
    },
};

/// Enrich every composed row with its declared topology. The record inventories the topology
/// reads are read for all rows at once, one read per chain.
pub(super) async fn enrich_all(
    conn: &mut PgConnection,
    rows: &mut BTreeMap<String, NameCurrentRow>,
) -> Result<()> {
    super::wrapper_fields::attach_wrapper_expiries(conn, rows).await?;
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
    let Some(execution) = crate::families::basenames_context::execution(conn, &publication).await?
    else {
        return Ok(None);
    };
    let Some(qualified) = crate::families::basenames_context::qualified_pointers(conn, &[resource])
        .await?
        .remove(&resource)
    else {
        return Ok(None);
    };
    let pointer = qualified.pointer;
    let event_id = qualified.event_id;
    let block_hash = qualified.block_hash;
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
    row.chain_positions["ethereum"] = json!({"chain_id":"ethereum-mainnet","block_number":execution.block_number,"block_hash":execution.block_hash,"timestamp":execution.timestamp});
    row.manifest_version = row.manifest_version.max(execution.manifest_version);
    row.provenance["manifest_versions"] = json!([{"source_family":"basenames_execution","manifest_version":2,"chain":"ethereum-mainnet","deployment_epoch":"basenames_v1"}]);
    Ok(Some(topology))
}
