fn build_name_response(
    row: NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_declared_state(&row, record_inventory_row);

    build_name_declared_response(row, declared_state, selected_snapshot)
}

fn build_name_coverage_response(
    row: NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_coverage_declared_state(&row.coverage);

    build_name_declared_response(row, declared_state, selected_snapshot)
}

fn build_name_surface_binding_explain_response(
    row: NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_surface_binding_explain_declared_state(&row);

    build_name_declared_response(row, declared_state, selected_snapshot)
}

fn build_name_authority_control_explain_response(
    row: NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_authority_control_explain_declared_state(&row);

    build_name_declared_response(row, declared_state, selected_snapshot)
}

fn build_name_declared_response(
    row: NameCurrentRow,
    declared_state: JsonValue,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    NameResponse {
        data: build_name_data(&row),
        declared_state,
        verified_state: None,
        provenance: build_name_provenance(&row.provenance),
        coverage: build_name_coverage(&row.coverage),
        chain_positions: selected_snapshot.chain_positions_value(),
        consistency: selected_snapshot.consistency.as_str().to_owned(),
        last_updated: format_timestamp(row.last_recomputed_at),
    }
}

fn build_resolution_response(
    row: NameCurrentRow,
    mode: ResolutionMode,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    persisted_verified_outcome: Option<&ExecutionOutcome>,
    selected_snapshot: &SelectedSnapshot,
) -> Result<ResolutionResponse> {
    let data = build_name_data(&row);
    let declared_state = mode
        .includes_declared()
        .then(|| build_resolution_declared_state(&row, record_inventory_row, records));
    let verified_state = mode
        .includes_verified()
        .then(|| build_resolution_verified_state(&row, records, persisted_verified_outcome))
        .transpose()?;
    let provenance = build_name_provenance_with_execution_trace(
        &row.provenance,
        persisted_verified_outcome.map(|outcome| outcome.execution_trace_id),
    );
    let coverage = build_name_coverage(&row.coverage);
    let chain_positions = selected_snapshot.chain_positions_value();
    let consistency = selected_snapshot.consistency.as_str().to_owned();
    let last_updated = format_timestamp(row.last_recomputed_at);

    Ok(ResolutionResponse {
        data,
        declared_state,
        verified_state,
        provenance,
        coverage,
        chain_positions,
        consistency,
        last_updated,
    })
}

fn build_resolution_execution_explain_response(
    row: NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<ResolutionResponse> {
    let data = build_name_data(&row);
    let verified_state =
        build_resolution_execution_explain_verified_state(&row, records, trace, outcome)?;
    let provenance =
        build_name_provenance_with_execution_trace(&row.provenance, Some(trace.execution_trace_id));
    let coverage = build_name_coverage(&row.coverage);
    let chain_positions = ensure_object(&row.chain_positions);
    let consistency = canonicality_consistency(&row.canonicality_summary).to_owned();
    let last_updated = format_timestamp(row.last_recomputed_at);

    Ok(ResolutionResponse {
        data,
        declared_state: None,
        verified_state: Some(verified_state),
        provenance,
        coverage,
        chain_positions,
        consistency,
        last_updated,
    })
}

fn build_primary_name_response(
    address: String,
    namespace: String,
    coin_type: String,
    mode: ResolutionMode,
    lookup_state: &PrimaryNameLookupState,
) -> PrimaryNameResponse {
    let data = json!({
        "address": address,
        "namespace": namespace,
        "coin_type": coin_type,
    });
    let declared_state = mode
        .includes_declared()
        .then(|| json!({ "claimed_primary_name": primary_name_claim_result(lookup_state) }));
    let verified_state = mode
        .includes_verified()
        .then(|| json!({ "verified_primary_name": primary_name_verified_result(lookup_state) }));

    PrimaryNameResponse {
        data,
        declared_state,
        verified_state,
        provenance: primary_name_route_provenance(lookup_state),
        coverage: primary_name_route_coverage(&namespace, lookup_state),
        chain_positions: empty_object(),
        consistency: "head".to_owned(),
        last_updated: primary_name_last_updated(lookup_state.persisted_verified.as_ref()),
    }
}

fn build_resolver_response(row: ResolverCurrentRow) -> ResolverResponse {
    ResolverResponse {
        data: build_resolver_data(&row),
        declared_state: build_resolver_declared_state(&row.declared_summary),
        verified_state: None,
        provenance: build_name_provenance(&row.provenance),
        coverage: build_name_coverage(&row.coverage),
        chain_positions: ensure_object(&row.chain_positions),
        consistency: canonicality_consistency(&row.canonicality_summary).to_owned(),
        last_updated: format_timestamp(row.last_recomputed_at),
    }
}

fn primary_name_claim_result(lookup_state: &PrimaryNameLookupState) -> JsonValue {
    match &lookup_state.tuple_state {
        PrimaryNameTupleState::ProjectionUnavailable => primary_name_unsupported_result(
            "declared primary-name claim surface is not yet supported",
        ),
        PrimaryNameTupleState::TupleMissing => primary_name_not_found_result(),
        PrimaryNameTupleState::TuplePresent(row) => {
            let mut result = json!({
                "status": row.claim_status.as_str(),
                "provenance": primary_name_claim_provenance(row),
            });
            if row.claim_status == PrimaryNameClaimStatus::Success
                && let Some(normalized_claim_name) = lookup_state.normalized_claim_name.as_deref()
            {
                insert_string_field(&mut result, "name", normalized_claim_name.to_owned());
            }
            if row.claim_status == PrimaryNameClaimStatus::InvalidName {
                insert_string_field(
                    &mut result,
                    "raw_claim_name",
                    row.raw_claim_name
                        .clone()
                        .expect("invalid_name primary-name rows must include raw_claim_name"),
                );
            }
            result
        }
    }
}

fn primary_name_claim_provenance(row: &PrimaryNameCurrentRow) -> JsonValue {
    let mut provenance = row
        .claim_provenance
        .as_object()
        .cloned()
        .unwrap_or_default();
    provenance.remove(VERIFIED_PRIMARY_NAME_LOOKUP_KEY);
    provenance.remove(VERIFIED_PRIMARY_NAME_INVALIDATION_KEY);
    provenance.remove("execution_trace_id");
    JsonValue::Object(provenance)
}

fn primary_name_verified_result(lookup_state: &PrimaryNameLookupState) -> JsonValue {
    if let Some(persisted_verified) = lookup_state.persisted_verified.as_ref() {
        let mut verified_primary_name = persisted_verified.verified_primary_name.clone();
        insert_value_field(
            &mut verified_primary_name,
            "provenance",
            ensure_object(&persisted_verified.provenance),
        );
        return verified_primary_name;
    }

    match lookup_state.tuple_state {
        PrimaryNameTupleState::TupleMissing => primary_name_not_found_result(),
        PrimaryNameTupleState::ProjectionUnavailable | PrimaryNameTupleState::TuplePresent(_) => {
            primary_name_unsupported_result("verified primary-name entrypoint is not yet supported")
        }
    }
}

fn primary_name_not_found_result() -> JsonValue {
    json!({ "status": "not_found" })
}

fn primary_name_unsupported_result(reason: &str) -> JsonValue {
    json!({
        "status": "unsupported",
        "unsupported_reason": reason,
    })
}

fn primary_name_bootstrap_provenance() -> JsonValue {
    json!({
        "normalized_event_ids": [],
        "raw_fact_refs": [],
        "manifest_versions": [],
        "execution_trace_id": null,
        "derivation_kind": "primary_name_route_bootstrap",
    })
}

fn primary_name_declared_route_provenance(row: &PrimaryNameCurrentRow) -> JsonValue {
    let mut provenance = primary_name_bootstrap_provenance();
    insert_value_field(
        &mut provenance,
        "normalized_event_ids",
        array_value_strings(provenance_field(&row.claim_provenance, "normalized_event_ids")),
    );
    insert_value_field(
        &mut provenance,
        "raw_fact_refs",
        array_or_empty(provenance_field(&row.claim_provenance, "raw_fact_refs")),
    );
    insert_value_field(
        &mut provenance,
        "manifest_versions",
        array_or_empty(provenance_field(&row.claim_provenance, "manifest_versions")),
    );
    insert_string_field(
        &mut provenance,
        "derivation_kind",
        string_field(provenance_field(&row.claim_provenance, "derivation_kind"))
            .unwrap_or_else(|| "primary_name_route_bootstrap".to_owned()),
    );
    provenance
}

fn primary_name_route_provenance(lookup_state: &PrimaryNameLookupState) -> JsonValue {
    let mut provenance = match &lookup_state.tuple_state {
        PrimaryNameTupleState::TuplePresent(row) => primary_name_declared_route_provenance(row),
        PrimaryNameTupleState::ProjectionUnavailable | PrimaryNameTupleState::TupleMissing => {
            primary_name_bootstrap_provenance()
        }
    };

    if let Some(persisted_verified) = lookup_state.persisted_verified.as_ref() {
        insert_value_field(
            &mut provenance,
            "manifest_versions",
            array_or_empty(provenance_field(
                &persisted_verified.provenance,
                "manifest_versions",
            )),
        );
        insert_nullable_string_field(
            &mut provenance,
            "execution_trace_id",
            string_field(provenance_field(
                &persisted_verified.provenance,
                "execution_trace_id",
            )),
        );
    }

    provenance
}

fn primary_name_verified_readback_provenance(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<JsonValue> {
    if let Some(trace_manifest_versions) = trace.manifest_context.get("manifest_versions") {
        let cache_manifest_versions =
            outcome
                .cache_key
                .manifest_versions
                .as_array()
                .ok_or_else(|| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        execution_trace_id = %trace.execution_trace_id,
                        manifest_versions = ?outcome.cache_key.manifest_versions,
                        "persisted verified primary-name outcome manifest_versions malformed"
                    );
                    ApiError::internal_error(format!(
                        "persisted verified primary-name provenance mismatch for address {address}"
                    ))
                })?;
        let trace_manifest_versions = trace_manifest_versions.as_array().ok_or_else(|| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                manifest_context = ?trace.manifest_context,
                "persisted verified primary-name trace manifest_versions malformed"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name provenance mismatch for address {address}"
            ))
        })?;
        if trace_manifest_versions != cache_manifest_versions {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                trace_manifest_versions = ?trace_manifest_versions,
                outcome_manifest_versions = ?cache_manifest_versions,
                "persisted verified primary-name manifest_versions mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name provenance mismatch for address {address}"
            )));
        }
    }

    let mut provenance = empty_object();
    insert_value_field(
        &mut provenance,
        "manifest_versions",
        array_or_empty(Some(&outcome.cache_key.manifest_versions)),
    );
    insert_string_field(
        &mut provenance,
        "execution_trace_id",
        trace.execution_trace_id.to_string(),
    );
    Ok(provenance)
}

fn primary_name_route_coverage(
    namespace: &str,
    lookup_state: &PrimaryNameLookupState,
) -> JsonValue {
    if matches!(
        lookup_state.tuple_state,
        PrimaryNameTupleState::TuplePresent(_)
    ) && lookup_state.persisted_verified.is_some()
    {
        match namespace {
            "ens" => {
                return primary_name_exact_tuple_coverage(&["ens_v1_reverse_l1", "ens_execution"]);
            }
            "basenames" => {
                return primary_name_exact_tuple_coverage(&[
                    "basenames_base_primary",
                    "basenames_execution",
                ]);
            }
            _ => {}
        }
    }

    primary_name_unsupported_exact_tuple_coverage()
}

fn primary_name_exact_tuple_coverage(source_classes: &[&str]) -> JsonValue {
    json!({
        "status": "partial",
        "exhaustiveness": "non_enumerable",
        "source_classes_considered": source_classes,
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": null,
    })
}

fn primary_name_unsupported_exact_tuple_coverage() -> JsonValue {
    json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": "primary-name exact-tuple persisted readback is not supported for the requested tuple",
    })
}

fn primary_name_last_updated(
    persisted_verified: Option<&PersistedPrimaryNameVerifiedReadback>,
) -> String {
    persisted_verified
        .map(|persisted_verified| format_timestamp(persisted_verified.finished_at))
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()))
}
