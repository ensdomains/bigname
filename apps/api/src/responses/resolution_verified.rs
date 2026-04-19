const BASENAMES_NAMESPACE: &str = "basenames";
const BASENAMES_COMPAT_SOURCE_CHAIN_ID: &str = "base-mainnet";
const BASENAMES_COMPAT_TARGET_CHAIN_ID: &str = "ethereum-mainnet";
const BASENAMES_COMPAT_CONTRACT_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";

fn build_resolution_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "topology",
        build_resolution_topology(row, record_inventory_row),
    );
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        build_record_inventory_section(
            record_inventory_row,
            "declared resolution record inventory is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "record_cache",
        build_record_cache_section(
            record_inventory_row,
            records,
            "declared resolution record cache is not yet projected",
        ),
    );
    declared_state
}

fn build_resolution_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    persisted_outcome: Option<&ExecutionOutcome>,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    let persisted_queries_by_record_key = persisted_outcome
        .map(|outcome| -> Result<BTreeMap<String, JsonValue>> {
            let supported_records = supported_resolution_verified_readback_records(row, records);
            let persisted_queries = persisted_verified_queries_by_record_key(outcome)?;
            Ok(supported_records
                .into_iter()
                .filter_map(|record| {
                    persisted_queries
                        .get(&record.record_key)
                        .cloned()
                        .map(|query| (record.record_key, query))
                })
                .collect::<BTreeMap<_, _>>())
        })
        .transpose()?
        .unwrap_or_default();
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        JsonValue::Array(
            records
                .iter()
                .map(|record| {
                    persisted_queries_by_record_key
                        .get(&record.record_key)
                        .cloned()
                        .unwrap_or_else(|| build_resolution_verified_query(record))
                })
                .collect(),
        ),
    );
    Ok(verified_state)
}

fn build_resolution_execution_explain_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    insert_value_field(
        &mut verified_state,
        "execution",
        build_resolution_execution_summary(row, trace, outcome)?,
    );
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        reordered_persisted_verified_queries(outcome, records)?,
    );
    Ok(verified_state)
}

fn build_resolution_verified_query(record: &ResolutionRecordKey) -> JsonValue {
    let mut query = empty_object();
    insert_string_field(&mut query, "record_key", record.record_key.clone());
    insert_string_field(&mut query, "status", "unsupported".to_owned());
    insert_string_field(
        &mut query,
        "unsupported_reason",
        "verified resolution entrypoint is not yet supported".to_owned(),
    );
    query
}

fn supported_resolution_verified_lookup_records(
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    records
        .iter()
        .filter(|record| match record.record_family.as_str() {
            "addr" => record
                .selector_key
                .as_deref()
                .is_some_and(|selector| selector.as_bytes().iter().all(u8::is_ascii_digit)),
            "contenthash" => record.record_key == "contenthash" && record.selector_key.is_none(),
            "text" => record.selector_key.is_some(),
            _ => false,
        })
        .cloned()
        .collect()
}

fn supported_resolution_verified_readback_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    records
        .iter()
        .filter(|record| {
            supports_resolution_verified_lookup_record(record)
                || (resolution_supports_avatar_readback(row, None)
                    && is_resolution_avatar_record(record))
        })
        .cloned()
        .collect()
}

async fn load_resolution_verified_outcome(
    pool: &PgPool,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Option<ExecutionOutcome>> {
    if resolution_verified_support_boundary(row, record_inventory_row).is_none() {
        return Ok(None);
    }

    let supported_records = supported_resolution_verified_lookup_records(records);
    if supported_records.is_empty() {
        return Ok(None);
    }

    let Ok(cache_key) =
        build_resolution_execution_cache_key(row, &supported_records, record_inventory_row)
    else {
        return Ok(None);
    };
    load_execution_outcome(pool, &cache_key).await
}

fn build_resolution_execution_summary(
    row: &NameCurrentRow,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE
        || outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE
    {
        bail!(
            "persisted execution explain requires request_type {VERIFIED_RESOLUTION_REQUEST_TYPE}"
        );
    }

    let mut execution = empty_object();
    insert_string_field(
        &mut execution,
        "execution_trace_id",
        trace.execution_trace_id.to_string(),
    );
    insert_value_field(
        &mut execution,
        "selected_entrypoint",
        build_resolution_selected_entrypoint(trace),
    );
    insert_value_field(
        &mut execution,
        "resolver_discovery_path",
        build_resolution_execution_resolver_discovery_path(row, trace),
    );
    insert_value_field(
        &mut execution,
        "wildcard",
        build_resolution_execution_wildcard(trace),
    );
    insert_value_field(
        &mut execution,
        "alias",
        build_resolution_execution_alias(trace),
    );
    insert_value_field(
        &mut execution,
        "steps",
        JsonValue::Array(
            trace
                .steps
                .iter()
                .map(build_execution_step_summary)
                .collect(),
        ),
    );
    insert_string_field(
        &mut execution,
        "finished_at",
        format_timestamp(trace.finished_at.unwrap_or(outcome.finished_at)),
    );

    Ok(execution)
}

fn build_resolution_selected_entrypoint(trace: &ExecutionTrace) -> JsonValue {
    let source_family = provenance_field(&trace.manifest_context, "manifest_versions")
        .and_then(JsonValue::as_array)
        .and_then(|items| {
            items
                .iter()
                .find_map(|item| string_field(provenance_field(item, "source_family")))
        });
    let role =
        string_field(provenance_field(&trace.request_metadata, "entrypoint")).or_else(|| {
            trace
                .steps
                .iter()
                .find_map(|step| string_field(provenance_field(&step.step_payload, "entrypoint")))
        });
    let contract_call = trace
        .contracts_called
        .as_array()
        .and_then(|items| items.iter().find(|item| item.is_object()));

    let chain_id = string_field(contract_call.and_then(|item| provenance_field(item, "chain_id")));
    let contract_address = string_field(provenance_field(
        &trace.request_metadata,
        "contract_address",
    ))
    .or_else(|| {
        trace
            .steps
            .iter()
            .find_map(|step| string_field(provenance_field(&step.step_payload, "resolver")))
    })
    .or_else(|| {
        string_field(contract_call.and_then(|item| provenance_field(item, "contract_address")))
    });

    let mut selected_entrypoint = empty_object();
    insert_nullable_string_field(&mut selected_entrypoint, "source_family", source_family);
    insert_nullable_string_field(&mut selected_entrypoint, "role", role);
    insert_nullable_string_field(&mut selected_entrypoint, "chain_id", chain_id);
    insert_nullable_string_field(
        &mut selected_entrypoint,
        "contract_address",
        contract_address,
    );
    selected_entrypoint
}

fn build_resolution_execution_resolver_discovery_path(
    row: &NameCurrentRow,
    trace: &ExecutionTrace,
) -> JsonValue {
    if let Some(resolver_path) = projected_resolution_resolver_path(&row.declared_summary) {
        return resolver_path;
    }

    let declared_resolver = provenance_field(&row.declared_summary, "resolver");
    let chain_id = trace
        .contracts_called
        .as_array()
        .and_then(|items| items.iter().find(|item| item.is_object()))
        .and_then(|item| string_field(provenance_field(item, "chain_id")))
        .or_else(|| {
            string_field(declared_resolver.and_then(|value| provenance_field(value, "chain_id")))
        });
    let address = trace
        .steps
        .iter()
        .find_map(|step| string_field(provenance_field(&step.step_payload, "resolver")))
        .or_else(|| {
            string_field(declared_resolver.and_then(|value| provenance_field(value, "address")))
        });
    let latest_event_kind = string_field(
        declared_resolver.and_then(|value| provenance_field(value, "latest_event_kind")),
    );

    JsonValue::Array(vec![build_resolution_resolver_hop(
        row,
        chain_id,
        address,
        latest_event_kind,
    )])
}

fn build_resolution_execution_wildcard(trace: &ExecutionTrace) -> JsonValue {
    persisted_trace_detail_object(trace, "wildcard").unwrap_or_else(|| {
        json!({
            "source": null,
            "matched_labels": [],
        })
    })
}

fn build_resolution_execution_alias(trace: &ExecutionTrace) -> JsonValue {
    persisted_trace_detail_object(trace, "alias").unwrap_or_else(|| {
        json!({
            "final_target": null,
            "hops": [],
        })
    })
}

fn build_execution_step_summary(step: &bigname_storage::ExecutionTraceStep) -> JsonValue {
    let mut summary = empty_object();
    insert_value_field(
        &mut summary,
        "step_index",
        JsonValue::Number(step.step_index.into()),
    );
    insert_string_field(&mut summary, "step_kind", step.step_kind.clone());
    insert_nullable_string_field(&mut summary, "input_digest", step.input_digest.clone());
    insert_nullable_string_field(&mut summary, "output_digest", step.output_digest.clone());
    insert_value_field(
        &mut summary,
        "latency",
        step.latency_ms
            .map(|value| JsonValue::Number(value.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut summary,
        "canonicality_dependency",
        ensure_object(&step.canonicality_dependency),
    );
    summary
}

fn reordered_persisted_verified_queries(
    outcome: &ExecutionOutcome,
    records: &[ResolutionRecordKey],
) -> Result<JsonValue> {
    let queries_by_record_key = persisted_verified_queries_by_record_key(outcome)?;

    let requested_record_keys = records
        .iter()
        .map(|record| record.record_key.clone())
        .collect::<BTreeSet<_>>();
    if queries_by_record_key.len() != requested_record_keys.len()
        || queries_by_record_key
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != requested_record_keys
    {
        bail!("persisted execution outcome selector set did not match requested records");
    }

    Ok(JsonValue::Array(
        records
            .iter()
            .map(|record| {
                queries_by_record_key
                    .get(&record.record_key)
                    .cloned()
                    .with_context(|| {
                        format!(
                            "persisted execution outcome did not include selector {}",
                            record.record_key
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn persisted_verified_queries_by_record_key(
    outcome: &ExecutionOutcome,
) -> Result<BTreeMap<String, JsonValue>> {
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .context("persisted execution outcome must set outcome_payload")?;
    let verified_queries = provenance_field(outcome_payload, "verified_queries")
        .and_then(JsonValue::as_array)
        .context("persisted execution outcome must set verified_queries")?;

    let mut queries_by_record_key = BTreeMap::new();
    for query in verified_queries {
        let record_key = string_field(provenance_field(query, "record_key"))
            .context("persisted verified query must include record_key")?;
        if queries_by_record_key
            .insert(record_key.clone(), query.clone())
            .is_some()
        {
            bail!("persisted execution outcome contained duplicate verified query {record_key}");
        }
    }

    Ok(queries_by_record_key)
}

fn build_resolution_topology(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        return projected_topology;
    }

    build_legacy_resolution_topology(row, record_inventory_row)
}

fn build_legacy_resolution_topology(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    if !matches!(
        row.namespace.as_str(),
        "ens" | BASENAMES_NAMESPACE
    ) || row.binding_kind != Some(SurfaceBindingKind::DeclaredRegistryPath)
        || row.resource_id.is_none()
    {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let Some(resolver_summary) = provenance_field(&row.declared_summary, "resolver")
        .filter(|value| value.is_object())
        .filter(|value| !summary_is_unsupported(Some(value)))
    else {
        return unsupported_section("declared resolution topology is not yet projected");
    };

    let resolver_chain_id = string_field(provenance_field(resolver_summary, "chain_id"));
    let resolver_address = string_field(provenance_field(resolver_summary, "address"));
    if resolver_chain_id.is_some() != resolver_address.is_some() {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let Some(boundary) = resolution_record_version_boundary(row, record_inventory_row) else {
        return unsupported_section("declared resolution topology is not yet projected");
    };

    let registry_ref = build_resolution_name_ref(row);
    let resolver_hop = build_resolution_resolver_hop(
        row,
        resolver_chain_id,
        resolver_address,
        string_field(provenance_field(resolver_summary, "latest_event_kind")),
    );

    let mut wildcard = empty_object();
    insert_value_field(&mut wildcard, "source", JsonValue::Null);
    insert_value_field(
        &mut wildcard,
        "matched_labels",
        JsonValue::Array(Vec::new()),
    );

    let mut alias = empty_object();
    insert_value_field(&mut alias, "final_target", JsonValue::Null);
    insert_value_field(&mut alias, "hops", JsonValue::Array(Vec::new()));

    let mut version_boundaries = empty_object();
    insert_value_field(
        &mut version_boundaries,
        "topology_version_boundary",
        boundary.clone(),
    );
    insert_value_field(&mut version_boundaries, "record_version_boundary", boundary);

    let mut topology = empty_object();
    insert_value_field(
        &mut topology,
        "registry_path",
        JsonValue::Array(vec![registry_ref]),
    );
    insert_value_field(
        &mut topology,
        "subregistry_path",
        JsonValue::Array(Vec::new()),
    );
    insert_value_field(
        &mut topology,
        "resolver_path",
        JsonValue::Array(vec![resolver_hop]),
    );
    insert_value_field(&mut topology, "wildcard", wildcard);
    insert_value_field(&mut topology, "alias", alias);
    insert_value_field(&mut topology, "version_boundaries", version_boundaries);
    insert_value_field(&mut topology, "transport", build_resolution_transport(row));
    topology
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SupportedResolutionPathClass {
    Direct,
    AliasOnly,
    WildcardDerived,
}

struct ResolutionVerifiedSupportBoundary {
    #[allow(dead_code)]
    path_class: SupportedResolutionPathClass,
    topology_version_boundary: JsonValue,
    record_version_boundary: JsonValue,
}

fn build_resolution_name_ref(row: &NameCurrentRow) -> JsonValue {
    let mut name_ref = empty_object();
    insert_string_field(
        &mut name_ref,
        "logical_name_id",
        row.logical_name_id.clone(),
    );
    insert_string_field(&mut name_ref, "namespace", row.namespace.clone());
    insert_string_field(
        &mut name_ref,
        "normalized_name",
        row.normalized_name.clone(),
    );
    insert_string_field(
        &mut name_ref,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut name_ref, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut name_ref,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut name_ref,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    name_ref
}

fn build_resolution_resolver_hop(
    row: &NameCurrentRow,
    chain_id: Option<String>,
    address: Option<String>,
    latest_event_kind: Option<String>,
) -> JsonValue {
    let mut hop = empty_object();
    insert_string_field(&mut hop, "logical_name_id", row.logical_name_id.clone());
    insert_string_field(&mut hop, "namespace", row.namespace.clone());
    insert_string_field(&mut hop, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut hop,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_optional_string_field(
        &mut hop,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_nullable_string_field(&mut hop, "chain_id", chain_id);
    insert_nullable_string_field(&mut hop, "address", address);
    insert_nullable_string_field(&mut hop, "latest_event_kind", latest_event_kind);
    hop
}

fn build_resolution_version_boundary(
    row: &NameCurrentRow,
    chain_position: &ChainPositionResponse,
) -> JsonValue {
    let mut boundary = empty_object();
    insert_string_field(
        &mut boundary,
        "logical_name_id",
        row.logical_name_id.clone(),
    );
    insert_optional_string_field(
        &mut boundary,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_value_field(&mut boundary, "normalized_event_id", JsonValue::Null);
    insert_value_field(&mut boundary, "event_kind", JsonValue::Null);
    insert_value_field(
        &mut boundary,
        "chain_position",
        serde_json::to_value(chain_position).expect("chain position must serialize"),
    );
    boundary
}

fn resolution_record_version_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<JsonValue> {
    record_inventory_row
        .map(|record_inventory_row| record_inventory_row.record_version_boundary.clone())
        .or_else(|| build_supported_resolution_declared_boundary(row))
}

fn build_resolution_execution_cache_key(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<ExecutionCacheKey> {
    let manifest_versions = array_or_empty(provenance_field(&row.provenance, "manifest_versions"));
    if manifest_versions
        .as_array()
        .is_none_or(|items| items.is_empty())
    {
        bail!(
            "resolution execution explain requires non-empty manifest_versions provenance for {}",
            row.logical_name_id
        );
    }

    let support_boundary = resolution_verified_support_boundary(row, record_inventory_row)
        .with_context(|| {
            format!(
                "resolution execution explain requires a supported topology boundary for {}",
                row.logical_name_id
            )
        })?;
    let topology_version_boundary = support_boundary.topology_version_boundary;
    let record_version_boundary = support_boundary.record_version_boundary;

    Ok(ExecutionCacheKey {
        request_key: normalized_resolution_request_key(
            &row.namespace,
            &row.normalized_name,
            records,
        ),
        requested_chain_positions: build_requested_chain_positions(&row.chain_positions)?,
        manifest_versions,
        topology_version_boundary,
        record_version_boundary,
    })
}

fn build_resolution_boundary_chain_position(row: &NameCurrentRow) -> Option<ChainPositionResponse> {
    let chain_positions = row.chain_positions.as_object()?;
    chain_positions
        .get("ethereum")
        .and_then(chain_position_from_value)
        .or_else(|| {
            let mut parsed = chain_positions
                .values()
                .filter_map(chain_position_from_value);
            let first = parsed.next()?;
            parsed.next().is_none().then_some(first)
        })
}

fn normalized_resolution_request_key(
    namespace: &str,
    normalized_name: &str,
    records: &[ResolutionRecordKey],
) -> String {
    let mut record_keys = records
        .iter()
        .map(|record| record.record_key.clone())
        .collect::<Vec<_>>();
    record_keys.sort_unstable();
    format!("{namespace}:{normalized_name}:{}", record_keys.join(","))
}

fn build_requested_chain_positions(chain_positions: &JsonValue) -> Result<JsonValue> {
    let positions = chain_positions
        .as_object()
        .context("resolution execution explain requires chain_positions")?
        .values()
        .filter_map(chain_position_from_value)
        .map(|position| {
            json!({
                "chain_id": position.chain_id,
                "block_number": position.block_number,
                "block_hash": position.block_hash,
            })
        })
        .collect::<Vec<_>>();

    if positions.is_empty() {
        bail!("resolution execution explain requires at least one chain position");
    }

    let mut positions = positions;
    positions.sort_by(|left, right| {
        string_field(provenance_field(left, "chain_id"))
            .cmp(&string_field(provenance_field(right, "chain_id")))
            .then(
                provenance_field(left, "block_number")
                    .and_then(JsonValue::as_i64)
                    .cmp(&provenance_field(right, "block_number").and_then(JsonValue::as_i64)),
            )
            .then(
                string_field(provenance_field(left, "block_hash"))
                    .cmp(&string_field(provenance_field(right, "block_hash"))),
            )
    });

    Ok(JsonValue::Array(positions))
}

fn resolution_execution_cache_lookup_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    if !resolution_supports_avatar_readback(row, None) {
        return records.to_vec();
    }

    let lookup_records = records
        .iter()
        .filter(|record| !is_resolution_avatar_record(record))
        .cloned()
        .collect::<Vec<_>>();

    if lookup_records.is_empty() || lookup_records.len() == records.len() {
        records.to_vec()
    } else {
        lookup_records
    }
}

fn persisted_trace_detail_object(trace: &ExecutionTrace, key: &str) -> Option<JsonValue> {
    provenance_field(&trace.request_metadata, key)
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| {
            trace
                .steps
                .iter()
                .find_map(|step| {
                    provenance_field(&step.step_payload, key).filter(|value| value.is_object())
                })
                .cloned()
        })
}

async fn load_supported_record_inventory_current(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let Some((resource_id, record_version_boundary)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    if let Some(record_inventory_row) =
        load_record_inventory_current(pool, resource_id, &record_version_boundary).await?
    {
        return Ok(Some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) =
        find_supported_record_inventory_boundary(pool, resource_id, &record_version_boundary)
            .await?
    else {
        return Ok(None);
    };

    load_record_inventory_current(pool, resource_id, &persisted_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

fn record_inventory_lookup_key(row: &NameCurrentRow) -> Option<(Uuid, JsonValue)> {
    Some((row.resource_id?, build_supported_resolution_declared_boundary(row)?))
}

fn supports_resolution_verified_lookup_record(record: &ResolutionRecordKey) -> bool {
    match record.record_family.as_str() {
        "addr" => record
            .selector_key
            .as_deref()
            .is_some_and(|selector| selector.as_bytes().iter().all(u8::is_ascii_digit)),
        "contenthash" => record.record_key == "contenthash" && record.selector_key.is_none(),
        "text" => record.selector_key.is_some(),
        _ => false,
    }
}

fn is_resolution_avatar_record(record: &ResolutionRecordKey) -> bool {
    record.record_key == "avatar"
        && record.record_family == "avatar"
        && record.selector_key.is_none()
}

fn resolution_supports_avatar_readback(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> bool {
    resolution_verified_support_boundary(row, record_inventory_row).is_some()
}

fn build_supported_resolution_verified_boundary(row: &NameCurrentRow) -> Option<JsonValue> {
    if row.namespace != "ens"
        || !matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        )
        || row.resource_id.is_none()
    {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    if !chain_position.chain_id.starts_with("ethereum") {
        return None;
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_supported_resolution_declared_boundary(row: &NameCurrentRow) -> Option<JsonValue> {
    let binding_supported = match row.namespace.as_str() {
        "ens" => matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        ),
        BASENAMES_NAMESPACE => {
            row.binding_kind == Some(SurfaceBindingKind::DeclaredRegistryPath)
        }
        _ => false,
    };
    if !binding_supported || row.resource_id.is_none() {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    match row.namespace.as_str() {
        "ens" if chain_position.chain_id.starts_with("ethereum") => {}
        BASENAMES_NAMESPACE if chain_position.chain_id == BASENAMES_COMPAT_SOURCE_CHAIN_ID => {}
        _ => return None,
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn projected_resolution_topology(summary: &JsonValue) -> Option<JsonValue> {
    provenance_field(summary, "topology")
        .filter(|value| value.is_object())
        .cloned()
}

fn projected_resolution_resolver_path(summary: &JsonValue) -> Option<JsonValue> {
    projected_resolution_topology(summary).and_then(|topology| {
        provenance_field(&topology, "resolver_path")
            .filter(|value| value.is_array())
            .cloned()
    })
}

fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<ResolutionVerifiedSupportBoundary> {
    if row.namespace != "ens" {
        return None;
    }

    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        let path_class =
            classify_supported_resolution_topology(&row.logical_name_id, &projected_topology)?;
        let version_boundaries = provenance_field(&projected_topology, "version_boundaries")?;
        let topology_version_boundary =
            provenance_field(version_boundaries, "topology_version_boundary")?.clone();
        let record_version_boundary =
            provenance_field(version_boundaries, "record_version_boundary")?.clone();
        return Some(ResolutionVerifiedSupportBoundary {
            path_class,
            topology_version_boundary,
            record_version_boundary,
        });
    }

    let topology_version_boundary = build_supported_resolution_verified_boundary(row)?;
    let record_version_boundary = resolution_record_version_boundary(row, record_inventory_row)
        .or_else(|| Some(topology_version_boundary.clone()))?;
    let path_class = match row.binding_kind {
        Some(SurfaceBindingKind::ResolverAliasPath) => SupportedResolutionPathClass::AliasOnly,
        _ => SupportedResolutionPathClass::Direct,
    };

    Some(ResolutionVerifiedSupportBoundary {
        path_class,
        topology_version_boundary,
        record_version_boundary,
    })
}

fn classify_supported_resolution_topology(
    logical_name_id: &str,
    topology: &JsonValue,
) -> Option<SupportedResolutionPathClass> {
    if summary_is_unsupported(Some(topology)) || !resolution_topology_transport_is_null(topology) {
        return None;
    }

    let resolver_logical_name_id = resolution_topology_resolver_logical_name_id(topology)?;
    let alias_present = resolution_topology_alias_is_present(topology)?;
    let wildcard_source_logical_name_id = resolution_topology_wildcard_state(topology)?;

    if wildcard_source_logical_name_id.is_some() {
        if alias_present || !resolution_topology_subregistry_path_is_empty(topology) {
            return None;
        }
        return (resolver_logical_name_id == wildcard_source_logical_name_id?)
            .then_some(SupportedResolutionPathClass::WildcardDerived);
    }

    if resolver_logical_name_id != logical_name_id {
        return None;
    }

    if alias_present {
        Some(SupportedResolutionPathClass::AliasOnly)
    } else {
        Some(SupportedResolutionPathClass::Direct)
    }
}

fn resolution_topology_resolver_logical_name_id(topology: &JsonValue) -> Option<String> {
    provenance_field(topology, "resolver_path")
        .and_then(JsonValue::as_array)
        .and_then(|resolver_path| resolver_path.first())
        .and_then(|hop| string_field(provenance_field(hop, "logical_name_id")))
}

fn resolution_topology_alias_is_present(topology: &JsonValue) -> Option<bool> {
    let alias = provenance_field(topology, "alias")?;
    let final_target_present = !matches!(
        provenance_field(alias, "final_target"),
        None | Some(JsonValue::Null)
    );
    let hops = provenance_field(alias, "hops")?.as_array()?;
    let hops_present = !hops.is_empty();

    if final_target_present != hops_present {
        return None;
    }

    Some(final_target_present)
}

fn resolution_topology_wildcard_state(topology: &JsonValue) -> Option<Option<String>> {
    let wildcard = provenance_field(topology, "wildcard")?;
    let matched_labels = provenance_field(wildcard, "matched_labels")?.as_array()?;
    let source = provenance_field(wildcard, "source");

    match source {
        None | Some(JsonValue::Null) => matched_labels.is_empty().then_some(None),
        Some(_) if matched_labels.is_empty() => None,
        Some(source) => string_field(provenance_field(source, "logical_name_id")).map(Some),
    }
}

fn resolution_topology_subregistry_path_is_empty(topology: &JsonValue) -> bool {
    provenance_field(topology, "subregistry_path")
        .and_then(JsonValue::as_array)
        .is_some_and(Vec::is_empty)
}

fn resolution_topology_transport_is_null(topology: &JsonValue) -> bool {
    let Some(transport) = provenance_field(topology, "transport") else {
        return true;
    };

    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        if !matches!(
            provenance_field(transport, field_name),
            None | Some(JsonValue::Null)
        ) {
            return false;
        }
    }

    true
}

fn build_resolution_transport(row: &NameCurrentRow) -> JsonValue {
    if row.namespace == BASENAMES_NAMESPACE {
        return json!({
            "source_chain_id": BASENAMES_COMPAT_SOURCE_CHAIN_ID,
            "target_chain_id": BASENAMES_COMPAT_TARGET_CHAIN_ID,
            "contract_address": BASENAMES_COMPAT_CONTRACT_ADDRESS,
            "latest_event_kind": JsonValue::Null,
        });
    }

    let mut transport = empty_object();
    insert_value_field(&mut transport, "source_chain_id", JsonValue::Null);
    insert_value_field(&mut transport, "target_chain_id", JsonValue::Null);
    insert_value_field(&mut transport, "contract_address", JsonValue::Null);
    insert_value_field(&mut transport, "latest_event_kind", JsonValue::Null);
    transport
}

fn record_version_boundary_has_pointer(record_version_boundary: &JsonValue) -> bool {
    provenance_field(record_version_boundary, "normalized_event_id")
        .is_some_and(|value| !value.is_null())
        && provenance_field(record_version_boundary, "event_kind")
            .is_some_and(|value| !value.is_null())
}

async fn find_supported_record_inventory_boundary(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &JsonValue,
) -> Result<Option<JsonValue>> {
    let logical_name_id = string_field(provenance_field(record_version_boundary, "logical_name_id"))
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include logical_name_id"
            )
        })?;
    let chain_position = provenance_field(record_version_boundary, "chain_position").with_context(
        || {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position"
            )
        },
    )?;
    let chain_id = string_field(provenance_field(chain_position, "chain_id")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.chain_id"
        )
    })?;
    let block_number = provenance_field(chain_position, "block_number")
        .and_then(JsonValue::as_i64)
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position.block_number"
            )
        })?;
    let block_hash = string_field(provenance_field(chain_position, "block_hash")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.block_hash"
        )
    })?;
    let timestamp = string_field(provenance_field(chain_position, "timestamp")).with_context(|| {
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
    .fetch_all(pool)
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
    .collect::<Result<Vec<JsonValue>>>()?;

    let Some(first_boundary) = boundaries.first().cloned() else {
        return Ok(None);
    };
    let second_boundary = boundaries.get(1);
    if let Some(second_boundary) = second_boundary {
        if !(record_version_boundary_has_pointer(&first_boundary)
            && !record_version_boundary_has_pointer(second_boundary))
        {
            anyhow::bail!(
                "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
                resource_id
            );
        }
    }

    Ok(Some(first_boundary))
}
