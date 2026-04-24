use anyhow::{Context, Result};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    identity::SurfaceBindingKind, name_current::NameCurrentRow,
    record_inventory::RecordInventoryCurrentRow,
};

use super::{
    support_classes::{
        BASE_MAINNET_CHAIN_ID, BASENAMES_NAMESPACE, ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID,
        ResolutionProjectionChainPosition, VerifiedResolutionPathClass,
        VerifiedResolutionSupportBoundary, json_field, json_string_field,
        resolution_projection_chain_position_from_value,
    },
    topology::{
        classify_supported_resolution_topology, projected_resolution_topology,
        row_has_basenames_supported_chain_positions,
        row_has_basenames_supported_chain_positions_for_revalidation,
        try_classify_supported_resolution_topology,
    },
};

pub fn resolution_supports_avatar_readback(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> bool {
    resolution_verified_support_boundary(row, record_inventory_row).is_some()
}

pub fn resolution_record_inventory_lookup_key(row: &NameCurrentRow) -> Option<(Uuid, Value)> {
    Some((
        row.resource_id?,
        build_supported_resolution_declared_boundary(row)?,
    ))
}

pub fn resolution_record_inventory_lookup_key_for_revalidation(
    row: &NameCurrentRow,
) -> Result<Option<(Uuid, Value)>> {
    if let Some(lookup) = projected_record_inventory_lookup_key_for_revalidation(row)? {
        return Ok(Some(lookup));
    }

    let Some(record_version_boundary) =
        build_supported_resolution_declared_boundary_for_revalidation(row)
    else {
        return Ok(None);
    };
    let resource_id = row
        .resource_id
        .with_context(|| "supported resolution revalidation requires resource_id".to_owned())?;
    Ok(Some((resource_id, record_version_boundary)))
}

pub fn resolution_record_version_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<Value> {
    record_inventory_row
        .map(|record_inventory_row| record_inventory_row.record_version_boundary.clone())
        .or_else(|| build_supported_resolution_declared_boundary(row))
}

pub fn resolution_record_version_boundary_for_revalidation(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<Value> {
    record_inventory_row
        .map(|row| row.record_version_boundary.clone())
        .or_else(|| build_supported_resolution_declared_boundary_for_revalidation(row))
}

pub fn record_version_boundary_has_pointer(record_version_boundary: &Value) -> bool {
    json_field(record_version_boundary, "normalized_event_id").is_some_and(|value| !value.is_null())
        && json_field(record_version_boundary, "event_kind").is_some_and(|value| !value.is_null())
}

pub fn projected_resolution_boundaries_from_topology(topology: &Value) -> Result<(Value, Value)> {
    let version_boundaries = json_field(topology, "version_boundaries")
        .with_context(|| "projected topology must include version_boundaries".to_owned())?;
    Ok((
        json_field(version_boundaries, "topology_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.topology_version_boundary"
                    .to_owned()
            })?,
        json_field(version_boundaries, "record_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.record_version_boundary"
                    .to_owned()
            })?,
    ))
}

pub fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<VerifiedResolutionSupportBoundary> {
    if !matches!(row.namespace.as_str(), ENS_NAMESPACE | BASENAMES_NAMESPACE) {
        return None;
    }

    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        let version_boundaries = json_field(&projected_topology, "version_boundaries")?;
        let topology_version_boundary =
            json_field(version_boundaries, "topology_version_boundary")?.clone();
        let record_version_boundary =
            json_field(version_boundaries, "record_version_boundary")?.clone();
        match row.namespace.as_str() {
            ENS_NAMESPACE
                if !boundary_chain_id_matches(
                    &topology_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) || !boundary_chain_id_matches(
                    &record_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) =>
            {
                return None;
            }
            BASENAMES_NAMESPACE if !row_has_basenames_supported_chain_positions(row) => {
                return None;
            }
            ENS_NAMESPACE | BASENAMES_NAMESPACE => {}
            _ => return None,
        }
        let path_class = classify_supported_resolution_topology(
            &row.namespace,
            &row.logical_name_id,
            &projected_topology,
        )?;
        return Some(VerifiedResolutionSupportBoundary {
            path_class,
            topology_version_boundary,
            record_version_boundary,
        });
    }

    let topology_version_boundary = match row.namespace.as_str() {
        ENS_NAMESPACE => build_supported_resolution_verified_boundary(row)?,
        BASENAMES_NAMESPACE => return None,
        _ => return None,
    };
    let record_version_boundary = resolution_record_version_boundary(row, record_inventory_row)
        .or_else(|| Some(topology_version_boundary.clone()))?;
    let path_class = match row.binding_kind {
        Some(SurfaceBindingKind::ResolverAliasPath) => VerifiedResolutionPathClass::AliasOnly,
        _ => VerifiedResolutionPathClass::Direct,
    };

    Some(VerifiedResolutionSupportBoundary {
        path_class,
        topology_version_boundary,
        record_version_boundary,
    })
}

pub fn try_resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Option<VerifiedResolutionSupportBoundary>> {
    if !matches!(row.namespace.as_str(), ENS_NAMESPACE | BASENAMES_NAMESPACE) {
        return Ok(None);
    }

    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        let version_boundaries = json_field(&projected_topology, "version_boundaries")
            .with_context(|| "projected topology must include version_boundaries".to_owned())?;
        let topology_version_boundary = json_field(version_boundaries, "topology_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.topology_version_boundary"
                    .to_owned()
            })?;
        let record_version_boundary = json_field(version_boundaries, "record_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.record_version_boundary"
                    .to_owned()
            })?;
        match row.namespace.as_str() {
            ENS_NAMESPACE
                if !boundary_chain_id_matches(
                    &topology_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) || !boundary_chain_id_matches(
                    &record_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) =>
            {
                return Ok(None);
            }
            BASENAMES_NAMESPACE
                if !row_has_basenames_supported_chain_positions_for_revalidation(row) =>
            {
                return Ok(None);
            }
            ENS_NAMESPACE | BASENAMES_NAMESPACE => {}
            _ => return Ok(None),
        }
        let path_class = try_classify_supported_resolution_topology(
            &row.namespace,
            &row.logical_name_id,
            &projected_topology,
        )?;
        return Ok(Some(VerifiedResolutionSupportBoundary {
            path_class,
            topology_version_boundary,
            record_version_boundary,
        }));
    }

    let Some(topology_version_boundary) = (match row.namespace.as_str() {
        ENS_NAMESPACE => build_supported_resolution_declared_boundary_for_revalidation(row),
        BASENAMES_NAMESPACE => None,
        _ => None,
    }) else {
        return Ok(None);
    };
    let record_version_boundary =
        resolution_record_version_boundary_for_revalidation(row, record_inventory_row)
            .unwrap_or_else(|| topology_version_boundary.clone());
    let path_class = match row.binding_kind {
        Some(SurfaceBindingKind::ResolverAliasPath) => VerifiedResolutionPathClass::AliasOnly,
        _ => VerifiedResolutionPathClass::Direct,
    };

    Ok(Some(VerifiedResolutionSupportBoundary {
        path_class,
        topology_version_boundary,
        record_version_boundary,
    }))
}

fn build_supported_resolution_verified_boundary(row: &NameCurrentRow) -> Option<Value> {
    if row.namespace != ENS_NAMESPACE
        || !matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        )
        || row.resource_id.is_none()
    {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    if chain_position.chain_id != ETHEREUM_MAINNET_CHAIN_ID {
        return None;
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_supported_resolution_declared_boundary(row: &NameCurrentRow) -> Option<Value> {
    let binding_supported = match row.namespace.as_str() {
        ENS_NAMESPACE => matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        ),
        BASENAMES_NAMESPACE => row.binding_kind == Some(SurfaceBindingKind::DeclaredRegistryPath),
        _ => false,
    };
    if !binding_supported || row.resource_id.is_none() {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    match row.namespace.as_str() {
        ENS_NAMESPACE if chain_position.chain_id == ETHEREUM_MAINNET_CHAIN_ID => {}
        BASENAMES_NAMESPACE if chain_position.chain_id == BASE_MAINNET_CHAIN_ID => {}
        _ => return None,
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_supported_resolution_declared_boundary_for_revalidation(
    row: &NameCurrentRow,
) -> Option<Value> {
    let binding_supported = match row.namespace.as_str() {
        ENS_NAMESPACE => matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        ),
        BASENAMES_NAMESPACE => row.binding_kind == Some(SurfaceBindingKind::DeclaredRegistryPath),
        _ => false,
    };
    if !binding_supported || row.resource_id.is_none() {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    match row.namespace.as_str() {
        ENS_NAMESPACE if chain_position.chain_id == ETHEREUM_MAINNET_CHAIN_ID => {}
        BASENAMES_NAMESPACE if chain_position.chain_id == BASE_MAINNET_CHAIN_ID => {}
        _ => return None,
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_resolution_boundary_chain_position(
    row: &NameCurrentRow,
) -> Option<ResolutionProjectionChainPosition> {
    let chain_positions = row.chain_positions.as_object()?;
    if row.namespace == BASENAMES_NAMESPACE
        && let Some(position) = chain_positions
            .values()
            .filter_map(resolution_projection_chain_position_from_value)
            .find(|position| position.chain_id == BASE_MAINNET_CHAIN_ID)
    {
        return Some(position);
    }

    chain_positions
        .get("ethereum")
        .and_then(resolution_projection_chain_position_from_value)
        .or_else(|| {
            let mut parsed = chain_positions
                .values()
                .filter_map(resolution_projection_chain_position_from_value);
            let first = parsed.next()?;
            parsed.next().is_none().then_some(first)
        })
}

fn build_resolution_version_boundary(
    row: &NameCurrentRow,
    chain_position: &ResolutionProjectionChainPosition,
) -> Value {
    let mut boundary = Map::new();
    boundary.insert(
        "logical_name_id".to_owned(),
        Value::String(row.logical_name_id.clone()),
    );
    boundary.insert(
        "resource_id".to_owned(),
        row.resource_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    boundary.insert("normalized_event_id".to_owned(), Value::Null);
    boundary.insert("event_kind".to_owned(), Value::Null);
    boundary.insert(
        "chain_position".to_owned(),
        Value::Object(chain_position_value(chain_position)),
    );
    Value::Object(boundary)
}

fn boundary_chain_id_matches(boundary: &Value, expected_chain_id: &str) -> bool {
    json_field(boundary, "chain_position")
        .and_then(|chain_position| json_string_field(json_field(chain_position, "chain_id")))
        .is_some_and(|chain_id| chain_id == expected_chain_id)
}

fn chain_position_value(position: &ResolutionProjectionChainPosition) -> Map<String, Value> {
    let mut value = Map::new();
    value.insert(
        "chain_id".to_owned(),
        Value::String(position.chain_id.clone()),
    );
    value.insert(
        "block_number".to_owned(),
        Value::Number(position.block_number.into()),
    );
    value.insert(
        "block_hash".to_owned(),
        Value::String(position.block_hash.clone()),
    );
    value.insert(
        "timestamp".to_owned(),
        Value::String(position.timestamp.clone()),
    );
    value
}

fn projected_record_inventory_lookup_key_for_revalidation(
    row: &NameCurrentRow,
) -> Result<Option<(Uuid, Value)>> {
    let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) else {
        return Ok(None);
    };

    let version_boundaries =
        json_field(&projected_topology, "version_boundaries").with_context(|| {
            format!(
                "projected topology for logical_name_id {} must include version_boundaries",
                row.logical_name_id
            )
        })?;
    let record_version_boundary = json_field(version_boundaries, "record_version_boundary")
        .cloned()
        .with_context(|| {
            format!(
                "projected topology for logical_name_id {} must include version_boundaries.record_version_boundary",
                row.logical_name_id
            )
        })?;
    let resource_id = json_field(&record_version_boundary, "resource_id")
        .and_then(Value::as_str)
        .with_context(|| {
            format!(
                "projected topology record_version_boundary for logical_name_id {} must include resource_id",
                row.logical_name_id
            )
        })?;
    let resource_id = Uuid::parse_str(resource_id).with_context(|| {
        format!(
            "projected topology record_version_boundary for logical_name_id {} must include a valid UUID resource_id",
            row.logical_name_id
        )
    })?;

    Ok(Some((resource_id, record_version_boundary)))
}
