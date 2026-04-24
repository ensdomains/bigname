use anyhow::{Context, Result, bail};
use bigname_storage::{
    NameCurrentRow, RecordInventoryCurrentRow,
    SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey, SurfaceBindingKind,
    VerifiedResolutionPathClass, VerifiedResolutionSupportBoundary,
};
use serde_json::{Map, Value};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::json_helpers::{json_field, json_string_field};
use crate::persistence::PersistEnsExactNameVerifiedResolutionRequest;
use crate::primary_name::validate_verified_primary_name_ref;
use crate::validation::{
    RequestedChainPosition, RequestedSelectorSet, SupportedResolutionPathClass,
    VerifiedQuerySummary, classify_supported_resolution_path, extract_requested_selectors,
    extract_supported_verified_queries, persisted_trace_detail_object, required_chain_positions,
};
use crate::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE, ENS_NAMESPACE,
    ETHEREUM_MAINNET_CHAIN_ID,
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ManifestVersionIdentity {
    source_manifest_id: Option<i64>,
    source_family: Option<String>,
    manifest_version: i64,
}

pub(crate) async fn revalidate_supported_resolution_persistence_from_storage(
    transaction: &mut Transaction<'_, Postgres>,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<()> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    let logical_name_id = format!(
        "{}:{}",
        request.trace.namespace, requested_selectors.surface
    );
    let context = match request.trace.namespace.as_str() {
        ENS_NAMESPACE => "ENS verified-resolution storage revalidation",
        BASENAMES_NAMESPACE => "Basenames verified-resolution storage revalidation",
        other => bail!("{other} verified-resolution storage revalidation is unsupported"),
    };

    let row = load_name_current_for_revalidation(transaction, &logical_name_id)
        .await?
        .with_context(|| {
            format!("{context} requires name_current row for logical_name_id {logical_name_id}")
        })?;
    let record_inventory_row =
        load_supported_record_inventory_current_for_revalidation(transaction, &row)
            .await
            .with_context(|| {
                format!(
                    "{context} failed to load supported record_inventory_current for logical_name_id {logical_name_id}"
                )
            })?;

    let stored_manifest_versions = normalize_manifest_versions_for_revalidation(
        row.provenance
            .as_object()
            .and_then(|object| object.get("manifest_versions"))
            .with_context(|| {
                format!("{context} name_current provenance must include manifest_versions")
            })?,
        &format!("{context} name_current provenance.manifest_versions"),
    )?;
    let outcome_manifest_versions = normalize_manifest_versions_for_revalidation(
        &request.outcome.cache_key.manifest_versions,
        &format!("{context} cache_key.manifest_versions"),
    )?;
    if stored_manifest_versions != outcome_manifest_versions {
        bail!(
            "{context} cache_key.manifest_versions must match name_current provenance.manifest_versions"
        );
    }

    let stored_requested_positions =
        build_requested_chain_positions_from_projection(&row.chain_positions)?;
    let outcome_requested_positions = normalize_requested_chain_positions(
        Some(&request.outcome.cache_key.requested_chain_positions),
        &format!("{context} cache_key.requested_chain_positions"),
    )?;
    if stored_requested_positions != outcome_requested_positions {
        bail!(
            "{context} cache_key.requested_chain_positions must match projected chain_positions for logical_name_id {logical_name_id}"
        );
    }

    let topology = build_resolution_topology_for_revalidation(&row, record_inventory_row.as_ref())?;
    let support_boundary = bigname_storage::try_resolution_verified_support_boundary(
        &row,
        record_inventory_row.as_ref(),
    )?
    .with_context(|| {
        format!(
            "{context} could not re-establish a supported mixed-route topology boundary for logical_name_id {logical_name_id}"
        )
    })?;

    ensure_storage_supported_boundary_matches_request(
        request,
        &requested_selectors,
        &topology,
        &support_boundary,
        context,
    )?;
    ensure_storage_selector_families_supported(
        record_inventory_row.as_ref(),
        &queries,
        &request.outcome.cache_key.request_key,
        context,
    )?;

    Ok(())
}

async fn load_supported_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NameCurrentRow,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let Some((resource_id, record_version_boundary)) =
        bigname_storage::resolution_record_inventory_lookup_key_for_revalidation(row)?
    else {
        return Ok(None);
    };

    if let Some(record_inventory_row) = load_record_inventory_current_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    {
        return Ok(Some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) = find_supported_record_inventory_boundary_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    else {
        return Ok(None);
    };

    load_record_inventory_current_for_revalidation(transaction, resource_id, &persisted_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

fn normalize_requested_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let mut positions = required_chain_positions(value, context)?;
    positions.sort_by(|left, right| {
        left.chain_id
            .cmp(&right.chain_id)
            .then(left.block_number.cmp(&right.block_number))
            .then(left.block_hash.cmp(&right.block_hash))
    });
    Ok(positions)
}

pub(crate) fn build_requested_chain_positions_from_projection(
    chain_positions: &Value,
) -> Result<Vec<RequestedChainPosition>> {
    Ok(
        bigname_storage::resolution_requested_chain_positions_from_projection(chain_positions)?
            .into_iter()
            .map(|position| RequestedChainPosition {
                chain_id: position.chain_id,
                block_number: position.block_number,
                block_hash: position.block_hash,
            })
            .collect(),
    )
}

fn normalize_manifest_versions_for_revalidation(value: &Value, context: &str) -> Result<Value> {
    let items = value
        .as_array()
        .with_context(|| format!("{context} must be a JSON array"))?;
    if items.is_empty() {
        bail!("{context} must not be empty");
    }

    let mut versions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = item
            .as_object()
            .with_context(|| format!("{context}[{index}] must be a JSON object"))?;
        let source_manifest_id = match object.get("source_manifest_id") {
            None | Some(Value::Null) => None,
            Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
                format!("{context}[{index}].source_manifest_id must be null or a positive integer")
            })?),
        };
        let source_family = match object.get("source_family") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
            Some(_) => bail!("{context}[{index}].source_family must be null or a non-empty string"),
        };
        if source_manifest_id.is_none() && source_family.is_none() {
            bail!("{context}[{index}] must include source_manifest_id or source_family");
        }
        let manifest_version = object
            .get("manifest_version")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .with_context(|| {
                format!("{context}[{index}].manifest_version must be a positive integer")
            })?;
        versions.push(ManifestVersionIdentity {
            source_manifest_id,
            source_family,
            manifest_version,
        });
    }

    versions.sort();
    versions.dedup();

    Ok(Value::Array(
        versions
            .into_iter()
            .map(|version| {
                let mut object = Map::new();
                if let Some(source_manifest_id) = version.source_manifest_id {
                    object.insert(
                        "source_manifest_id".to_owned(),
                        Value::Number(source_manifest_id.into()),
                    );
                }
                if let Some(source_family) = version.source_family {
                    object.insert("source_family".to_owned(), Value::String(source_family));
                }
                object.insert(
                    "manifest_version".to_owned(),
                    Value::Number(version.manifest_version.into()),
                );
                Value::Object(object)
            })
            .collect(),
    ))
}

fn build_resolution_topology_for_revalidation(
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
    let mut transport = Map::new();
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
        transport.insert("latest_event_kind".to_owned(), Value::Null);
        return transport;
    }

    transport.insert("source_chain_id".to_owned(), Value::Null);
    transport.insert("target_chain_id".to_owned(), Value::Null);
    transport.insert("contract_address".to_owned(), Value::Null);
    transport.insert("latest_event_kind".to_owned(), Value::Null);
    transport
}

fn ensure_storage_supported_boundary_matches_request(
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

pub(crate) fn normalize_alias_detail(value: Option<&Value>, namespace: &str) -> Result<Value> {
    let Some(alias) = value else {
        return Ok(Value::Object(default_alias_detail()));
    };
    let alias = alias
        .as_object()
        .with_context(|| "alias detail must be a JSON object".to_owned())?;
    let mut normalized = default_alias_detail();

    let final_target = match alias.get("final_target") {
        None | Some(Value::Null) => Value::Null,
        Some(value) => {
            validate_verified_primary_name_ref(Some(value), "alias.final_target", namespace)?;
            value.clone()
        }
    };
    let hops = alias
        .get("hops")
        .and_then(Value::as_array)
        .with_context(|| "alias.hops must be a JSON array".to_owned())?;
    for (index, hop) in hops.iter().enumerate() {
        validate_verified_primary_name_ref(Some(hop), &format!("alias.hops[{index}]"), namespace)?;
    }
    if final_target.is_null() != hops.is_empty() {
        bail!("alias detail must set final_target and non-empty hops together");
    }
    normalized.insert("final_target".to_owned(), final_target);
    normalized.insert("hops".to_owned(), Value::Array(hops.clone()));
    Ok(Value::Object(normalized))
}

pub(crate) fn normalize_wildcard_detail(value: Option<&Value>, namespace: &str) -> Result<Value> {
    let Some(wildcard) = value else {
        return Ok(Value::Object(default_wildcard_detail()));
    };
    let wildcard = wildcard
        .as_object()
        .with_context(|| "wildcard detail must be a JSON object".to_owned())?;
    let mut normalized = default_wildcard_detail();

    let source = match wildcard.get("source") {
        None | Some(Value::Null) => Value::Null,
        Some(value) => {
            validate_verified_primary_name_ref(Some(value), "wildcard.source", namespace)?;
            value.clone()
        }
    };
    let matched_labels = wildcard
        .get("matched_labels")
        .and_then(Value::as_array)
        .with_context(|| "wildcard.matched_labels must be a JSON array".to_owned())?;
    if source.is_null() && !matched_labels.is_empty() {
        bail!("wildcard detail must keep matched_labels empty when source is null");
    }
    if !source.is_null() && matched_labels.is_empty() {
        bail!("wildcard detail must keep matched_labels non-empty when source is present");
    }
    normalized.insert("source".to_owned(), source);
    normalized.insert(
        "matched_labels".to_owned(),
        Value::Array(matched_labels.clone()),
    );
    Ok(Value::Object(normalized))
}

pub(crate) fn normalize_transport_detail(value: Option<&Value>) -> Result<Value> {
    let Some(transport) = value else {
        return Ok(Value::Object(default_transport_detail()));
    };
    let transport = transport
        .as_object()
        .with_context(|| "transport detail must be a JSON object".to_owned())?;
    let mut normalized = default_transport_detail();
    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        let value = match transport.get(field_name) {
            None | Some(Value::Null) => Value::Null,
            Some(Value::String(value)) if !value.trim().is_empty() => Value::String(value.clone()),
            Some(_) => {
                bail!("transport detail field {field_name} must be null or a non-empty string")
            }
        };
        normalized.insert(field_name.to_owned(), value);
    }
    Ok(Value::Object(normalized))
}

fn default_alias_detail() -> Map<String, Value> {
    let mut alias = Map::new();
    alias.insert("final_target".to_owned(), Value::Null);
    alias.insert("hops".to_owned(), Value::Array(Vec::new()));
    alias
}

fn default_wildcard_detail() -> Map<String, Value> {
    let mut wildcard = Map::new();
    wildcard.insert("source".to_owned(), Value::Null);
    wildcard.insert("matched_labels".to_owned(), Value::Array(Vec::new()));
    wildcard
}

fn default_transport_detail() -> Map<String, Value> {
    let mut transport = Map::new();
    transport.insert("source_chain_id".to_owned(), Value::Null);
    transport.insert("target_chain_id".to_owned(), Value::Null);
    transport.insert("contract_address".to_owned(), Value::Null);
    transport.insert("latest_event_kind".to_owned(), Value::Null);
    transport
}

fn ensure_storage_selector_families_supported(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    queries: &[VerifiedQuerySummary],
    request_key: &str,
    context: &str,
) -> Result<()> {
    let record_inventory_row = record_inventory_row.with_context(|| {
        format!(
            "{context} requires record_inventory_current to revalidate supported selectors for request_key {request_key}"
        )
    })?;
    let unsupported_families = record_inventory_row
        .unsupported_families
        .as_array()
        .with_context(|| {
            format!("{context} record_inventory_current.unsupported_families must be a JSON array")
        })?;
    let entries = record_inventory_row.entries.as_array().with_context(|| {
        format!("{context} record_inventory_current.entries must be a JSON array")
    })?;

    for query in queries {
        let (record_family, selector_key) =
            selector_family_and_key(&query.record_key, &query.selector);

        if unsupported_families.iter().any(|entry| {
            json_string_field(json_field(entry, "record_family"))
                .is_some_and(|value| value == record_family)
        }) {
            bail!(
                "{context} record family {record_family} is still unsupported in record_inventory_current for request_key {request_key}"
            );
        }

        if entries.iter().any(|entry| {
            json_string_field(json_field(entry, "record_key"))
                .is_some_and(|value| value == query.record_key)
                && json_string_field(json_field(entry, "status"))
                    .is_some_and(|value| value == "unsupported")
                && selector_key_matches_inventory(entry, selector_key.as_deref())
        }) {
            bail!(
                "{context} selector {} is still unsupported in record_inventory_current for request_key {request_key}",
                query.record_key
            );
        }
    }

    Ok(())
}

pub(crate) fn selector_family_and_key(
    record_key: &str,
    selector: &SupportedVerifiedRecordKey,
) -> (String, Option<String>) {
    match selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            ("addr".to_owned(), Some(coin_type.clone()))
        }
        SupportedVerifiedRecordKey::Avatar => ("avatar".to_owned(), None),
        SupportedVerifiedRecordKey::Contenthash => ("contenthash".to_owned(), None),
        SupportedVerifiedRecordKey::Text => (
            "text".to_owned(),
            record_key.strip_prefix("text:").map(str::to_owned),
        ),
    }
}

fn selector_key_matches_inventory(entry: &Value, selector_key: Option<&str>) -> bool {
    match (json_field(entry, "selector_key"), selector_key) {
        (None | Some(Value::Null), None) => true,
        (Some(Value::String(left)), Some(right)) => left == right,
        _ => false,
    }
}

async fn find_supported_record_inventory_boundary_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<Value>> {
    let logical_name_id =
        json_string_field(json_field(record_version_boundary, "logical_name_id")).with_context(
            || {
                format!(
                    "supported record version boundary for resource_id {resource_id} must include logical_name_id"
                )
            },
        )?;
    let chain_position = json_field(record_version_boundary, "chain_position").with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position"
        )
    })?;
    let chain_id = json_string_field(json_field(chain_position, "chain_id")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.chain_id"
        )
    })?;
    let block_number = json_field(chain_position, "block_number")
        .and_then(Value::as_i64)
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position.block_number"
            )
        })?;
    let block_hash = json_string_field(json_field(chain_position, "block_hash")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.block_hash"
        )
    })?;
    let timestamp = json_string_field(json_field(chain_position, "timestamp")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.timestamp"
        )
    })?;

    let boundaries = sqlx::query(
        r#"
        SELECT record_version_boundary
        FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary ->> 'logical_name_id' = $2
          AND record_version_boundary -> 'chain_position' ->> 'chain_id' = $3
          AND (record_version_boundary -> 'chain_position' ->> 'block_number')::bigint = $4
          AND record_version_boundary -> 'chain_position' ->> 'block_hash' = $5
          AND record_version_boundary -> 'chain_position' ->> 'timestamp' = $6
        ORDER BY
          (record_version_boundary ->> 'normalized_event_id') IS NULL ASC,
          (record_version_boundary ->> 'normalized_event_id')::bigint DESC NULLS LAST
        LIMIT 2
        "#,
    )
    .bind(resource_id)
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(timestamp)
    .fetch_all(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to locate supported record_inventory_current boundary for resource_id {resource_id}"
        )
    })?
    .into_iter()
    .map(|row| {
        row.try_get("record_version_boundary").with_context(|| {
            format!(
                "supported record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary"
            )
        })
    })
    .collect::<Result<Vec<Value>, _>>()?;

    let Some(first_boundary) = boundaries.first().cloned() else {
        return Ok(None);
    };
    if let Some(second_boundary) = boundaries.get(1)
        && (!record_version_boundary_has_pointer(&first_boundary)
            || record_version_boundary_has_pointer(second_boundary))
    {
        bail!(
            "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
            resource_id
        );
    }

    Ok(Some(first_boundary))
}

fn record_version_boundary_has_pointer(record_version_boundary: &Value) -> bool {
    bigname_storage::record_version_boundary_has_pointer(record_version_boundary)
}

async fn load_name_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let row = sqlx::query(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = $1
          AND surface.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND (
              nc.surface_binding_id IS NULL
              OR (
                  resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND binding.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND (
                      nc.token_lineage_id IS NULL
                      OR token_lineage.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                  )
              )
          )
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load name_current row for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_name_current_row_for_revalidation)
        .transpose()
}

fn decode_name_current_row_for_revalidation(row: PgRow) -> Result<NameCurrentRow> {
    Ok(NameCurrentRow {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind: row
            .try_get::<Option<String>, _>("binding_kind")
            .context("missing binding_kind")?
            .map(|value| parse_surface_binding_kind_for_revalidation(&value))
            .transpose()?,
        declared_summary: row
            .try_get("declared_summary")
            .context("missing declared_summary")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn parse_surface_binding_kind_for_revalidation(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}

async fn load_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = serde_json::to_string(record_version_boundary)
        .context("failed to serialize revalidation record_version_boundary")?;

    let row = sqlx::query(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary = $2::JSONB
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary_key)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row_for_revalidation)
        .transpose()
}

fn decode_record_inventory_current_row_for_revalidation(
    row: PgRow,
) -> Result<RecordInventoryCurrentRow> {
    Ok(RecordInventoryCurrentRow {
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        record_version_boundary: row
            .try_get("record_version_boundary")
            .context("missing record_version_boundary")?,
        enumeration_basis: row
            .try_get("enumeration_basis")
            .context("missing enumeration_basis")?,
        selectors: row.try_get("selectors").context("missing selectors")?,
        explicit_gaps: row
            .try_get("explicit_gaps")
            .context("missing explicit_gaps")?,
        unsupported_families: row
            .try_get("unsupported_families")
            .context("missing unsupported_families")?,
        last_change: row.try_get("last_change").context("missing last_change")?,
        entries: row.try_get("entries").context("missing entries")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn summary_is_unsupported(section: Option<&Value>) -> bool {
    matches!(
        json_string_field(section.and_then(|value| json_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && json_string_field(section.and_then(|value| json_field(value, "unsupported_reason")))
        .is_some()
}
