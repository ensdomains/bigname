//! Declared resolution topology on the same snapshot as the composed name and inventory.
//! The five arms mirror the served builder's order. No provider result is used or retained.
use anyhow::{Context, Result};
use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::{
    NameCurrentRow,
    families::{
        records::{FamilyAttribution, load_family_record_inventory_detail_on},
        topology::{load_family_wildcard_source_on, load_name_topology_on},
    },
};

pub(super) async fn enrich(conn: &mut PgConnection, row: &mut NameCurrentRow) -> Result<()> {
    let mut topology = match row.binding_kind.map(|kind| kind.as_str()) {
        Some("resolver_alias_path" | "observed_wildcard_path") => {
            load_name_topology_on(conn, &row.logical_name_id).await?
        }
        _ => None,
    };
    let chain_id = row.provenance["chain_id"]
        .as_str()
        .context("composed name has no chain")?
        .to_owned();
    let resource = row.serving_resource_id.or(row.resource_id);
    if let Some(resource) = resource {
        let inventory = load_family_record_inventory_detail_on(
            conn,
            &chain_id,
            resource,
            FamilyAttribution::Given(Default::default()),
        )
        .await?;
        if row.namespace == "ens" && inventory.is_some() {
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
                    inventory
                        .as_ref()
                        .unwrap()
                        .row
                        .record_version_boundary
                        .clone(),
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
                inventory
                    .as_ref()
                    .map(|inventory| &inventory.row.record_version_boundary),
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
        "wildcard":{"source":null,"matched_labels":[]}, "alias":{"final_target":null,"hops":[]},
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
    // Retain the served builder's source-time selection of the Ethereum execution block.
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
