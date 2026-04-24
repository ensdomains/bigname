use anyhow::{Context, Result, bail};
use bigname_storage::{
    NameCurrentRow, RecordInventoryCurrentRow, SurfaceBindingKind, VerifiedResolutionPathClass,
    VerifiedResolutionSupportBoundary,
};
use serde_json::{Map, Value};

use super::details::{default_alias_detail, default_transport_detail, default_wildcard_detail};
use super::{normalize_alias_detail, normalize_transport_detail, normalize_wildcard_detail};
use crate::json_helpers::{json_field, json_string_field};
use crate::persistence::PersistEnsExactNameVerifiedResolutionRequest;
use crate::validation::{
    RequestedSelectorSet, SupportedResolutionPathClass, classify_supported_resolution_path,
    persisted_trace_detail_object,
};
use crate::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE, ENS_NAMESPACE,
    ETHEREUM_MAINNET_CHAIN_ID,
};

pub(super) fn build_resolution_topology_for_revalidation(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Value> {
    if let Some(projected_topology) =
        bigname_storage::projected_resolution_topology(&row.declared_summary)
    {
        return Ok(projected_topology);
    }

    build_legacy_resolution_topology_for_revalidation(row, record_inventory_row)
}

fn build_legacy_resolution_topology_for_revalidation(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Value> {
    if !matches!(row.namespace.as_str(), ENS_NAMESPACE | BASENAMES_NAMESPACE)
        || row.binding_kind != Some(SurfaceBindingKind::DeclaredRegistryPath)
        || row.resource_id.is_none()
    {
        bail!("declared resolution topology is not yet projected");
    }

    let resolver_summary = json_field(&row.declared_summary, "resolver")
        .filter(|value| value.is_object())
        .filter(|value| !summary_is_unsupported(Some(value)))
        .with_context(|| "declared resolution topology is not yet projected".to_owned())?;

    let resolver_chain_id = json_string_field(json_field(resolver_summary, "chain_id"));
    let resolver_address = json_string_field(json_field(resolver_summary, "address"));
    if resolver_chain_id.is_some() != resolver_address.is_some() {
        bail!("declared resolution topology is not yet projected");
    }

    let record_version_boundary =
        bigname_storage::resolution_record_version_boundary_for_revalidation(
            row,
            record_inventory_row,
        )
        .with_context(|| "declared resolution topology is not yet projected".to_owned())?;

    let registry_ref = build_resolution_name_ref_for_revalidation(row);
    let resolver_hop = build_resolution_resolver_hop_for_revalidation(
        row,
        resolver_chain_id,
        resolver_address,
        json_string_field(json_field(resolver_summary, "latest_event_kind")),
    );

    let mut version_boundaries = Map::new();
    version_boundaries.insert(
        "topology_version_boundary".to_owned(),
        record_version_boundary.clone(),
    );
    version_boundaries.insert(
        "record_version_boundary".to_owned(),
        record_version_boundary,
    );

    let mut topology = Map::new();
    topology.insert("registry_path".to_owned(), Value::Array(vec![registry_ref]));
    topology.insert("subregistry_path".to_owned(), Value::Array(Vec::new()));
    topology.insert("resolver_path".to_owned(), Value::Array(vec![resolver_hop]));
    topology.insert(
        "wildcard".to_owned(),
        Value::Object(default_wildcard_detail()),
    );
    topology.insert("alias".to_owned(), Value::Object(default_alias_detail()));
    topology.insert(
        "version_boundaries".to_owned(),
        Value::Object(version_boundaries),
    );
    topology.insert(
        "transport".to_owned(),
        Value::Object(build_resolution_transport_for_revalidation(row)),
    );
    Ok(Value::Object(topology))
}

fn build_resolution_name_ref_for_revalidation(row: &NameCurrentRow) -> Value {
    let mut name_ref = Map::new();
    name_ref.insert(
        "logical_name_id".to_owned(),
        Value::String(row.logical_name_id.clone()),
    );
    name_ref.insert("namespace".to_owned(), Value::String(row.namespace.clone()));
    name_ref.insert(
        "normalized_name".to_owned(),
        Value::String(row.normalized_name.clone()),
    );
    name_ref.insert(
        "canonical_display_name".to_owned(),
        Value::String(row.canonical_display_name.clone()),
    );
    name_ref.insert("namehash".to_owned(), Value::String(row.namehash.clone()));
    name_ref.insert(
        "resource_id".to_owned(),
        row.resource_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    name_ref.insert(
        "binding_kind".to_owned(),
        row.binding_kind
            .map(|value| Value::String(value.as_str().to_owned()))
            .unwrap_or(Value::Null),
    );
    Value::Object(name_ref)
}

fn build_resolution_resolver_hop_for_revalidation(
    row: &NameCurrentRow,
    chain_id: Option<String>,
    address: Option<String>,
    latest_event_kind: Option<String>,
) -> Value {
    let mut hop = Map::new();
    hop.insert(
        "logical_name_id".to_owned(),
        Value::String(row.logical_name_id.clone()),
    );
    hop.insert("namespace".to_owned(), Value::String(row.namespace.clone()));
    hop.insert(
        "normalized_name".to_owned(),
        Value::String(row.normalized_name.clone()),
    );
    hop.insert(
        "canonical_display_name".to_owned(),
        Value::String(row.canonical_display_name.clone()),
    );
    hop.insert(
        "resource_id".to_owned(),
        row.resource_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    hop.insert(
        "chain_id".to_owned(),
        chain_id.map(Value::String).unwrap_or(Value::Null),
    );
    hop.insert(
        "address".to_owned(),
        address.map(Value::String).unwrap_or(Value::Null),
    );
    hop.insert(
        "latest_event_kind".to_owned(),
        latest_event_kind.map(Value::String).unwrap_or(Value::Null),
    );
    Value::Object(hop)
}

fn build_resolution_transport_for_revalidation(row: &NameCurrentRow) -> Map<String, Value> {
    let mut transport = default_transport_detail();
    if row.namespace == BASENAMES_NAMESPACE {
        transport.insert(
            "source_chain_id".to_owned(),
            Value::String(BASE_MAINNET_CHAIN_ID.to_owned()),
        );
        transport.insert(
            "target_chain_id".to_owned(),
            Value::String(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        );
        transport.insert(
            "contract_address".to_owned(),
            Value::String(BASENAMES_L1_RESOLVER_ADDRESS.to_owned()),
        );
    }
    transport
}

pub(super) fn ensure_storage_supported_boundary_matches_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    requested_selectors: &RequestedSelectorSet,
    topology: &Value,
    support_boundary: &VerifiedResolutionSupportBoundary,
    context: &str,
) -> Result<()> {
    let expected_path_class = match request.trace.namespace.as_str() {
        ENS_NAMESPACE => match classify_supported_resolution_path(
            requested_selectors.binding_kind.as_deref(),
            request.trace.execution_trace_id,
        )? {
            SupportedResolutionPathClass::Direct => VerifiedResolutionPathClass::Direct,
            SupportedResolutionPathClass::AliasOnly => VerifiedResolutionPathClass::AliasOnly,
            SupportedResolutionPathClass::WildcardDerived => {
                VerifiedResolutionPathClass::WildcardDerived
            }
        },
        BASENAMES_NAMESPACE => VerifiedResolutionPathClass::BasenamesTransportDirect,
        other => bail!("{context} does not support namespace {other}"),
    };
    if support_boundary.path_class != expected_path_class {
        bail!("{context} stored supported path class does not match the request trace");
    }
    if support_boundary.topology_version_boundary
        != request.outcome.cache_key.topology_version_boundary
    {
        bail!(
            "{context} cache_key.topology_version_boundary must match the stored mixed-route topology boundary"
        );
    }
    if support_boundary.record_version_boundary != request.outcome.cache_key.record_version_boundary
    {
        bail!(
            "{context} cache_key.record_version_boundary must match the stored mixed-route record boundary"
        );
    }

    let stored_alias =
        normalize_alias_detail(json_field(topology, "alias"), &request.trace.namespace)?;
    let request_alias = normalize_alias_detail(
        persisted_trace_detail_object(&request.trace, "alias").as_ref(),
        &request.trace.namespace,
    )?;
    if stored_alias != request_alias {
        bail!("{context} stored alias topology does not match the request trace");
    }

    let stored_wildcard =
        normalize_wildcard_detail(json_field(topology, "wildcard"), &request.trace.namespace)?;
    let request_wildcard = normalize_wildcard_detail(
        persisted_trace_detail_object(&request.trace, "wildcard").as_ref(),
        &request.trace.namespace,
    )?;
    if stored_wildcard != request_wildcard {
        bail!("{context} stored wildcard topology does not match the request trace");
    }

    let stored_transport = normalize_transport_detail(json_field(topology, "transport"))?;
    let request_transport = normalize_transport_detail(
        persisted_trace_detail_object(&request.trace, "transport").as_ref(),
    )?;
    if stored_transport != request_transport {
        bail!("{context} stored transport topology does not match the request trace");
    }

    Ok(())
}

fn summary_is_unsupported(section: Option<&Value>) -> bool {
    matches!(
        json_string_field(section.and_then(|value| json_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && json_string_field(section.and_then(|value| json_field(value, "unsupported_reason")))
        .is_some()
}
