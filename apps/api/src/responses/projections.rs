fn build_address_name_expansion_facts(row: &NameCurrentRow) -> AddressNameExpansionFacts {
    AddressNameExpansionFacts {
        status: supported_summary_field(
            provenance_field(&row.declared_summary, "control"),
            "status",
        ),
        expiry: supported_summary_field(
            provenance_field(&row.declared_summary, "control"),
            "expiry",
        ),
        record_count: supported_summary_field(
            provenance_field(&row.declared_summary, "record_inventory"),
            "count",
        ),
    }
}

fn supported_summary_field(section: Option<&JsonValue>, key: &str) -> JsonValue {
    if summary_is_unsupported(section) {
        return JsonValue::Null;
    }

    section
        .and_then(|value| provenance_field(value, key))
        .cloned()
        .unwrap_or(JsonValue::Null)
}

fn summary_is_unsupported(section: Option<&JsonValue>) -> bool {
    matches!(
        string_field(section.and_then(|value| provenance_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && string_field(section.and_then(|value| provenance_field(value, "unsupported_reason")))
        .is_some()
}

fn build_name_data(row: &NameCurrentRow) -> JsonValue {
    let mut data = empty_object();
    insert_string_field(&mut data, "logical_name_id", row.logical_name_id.clone());
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut data,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut data, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut data,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut data,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut data,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    data
}

fn build_resolver_data(row: &ResolverCurrentRow) -> JsonValue {
    let mut data = empty_object();
    insert_string_field(&mut data, "chain_id", row.chain_id.clone());
    insert_string_field(&mut data, "resolver_address", row.resolver_address.clone());
    data
}

fn build_name_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "registration",
        declared_summary_section(
            &row.declared_summary,
            "registration",
            "declared registration summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "authority",
        declared_authority_section(row),
    );
    insert_value_field(
        &mut declared_state,
        "control",
        declared_name_control_section(&row.declared_summary),
    );
    insert_value_field(
        &mut declared_state,
        "resolver",
        declared_summary_section(
            &row.declared_summary,
            "resolver",
            "declared resolver summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        build_record_inventory_section(
            record_inventory_row,
            "declared record inventory summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

fn build_resolver_declared_state(summary: &JsonValue) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "bindings",
        declared_summary_section(
            summary,
            "bindings",
            "resolver bindings summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "aliases",
        declared_summary_section(
            summary,
            "aliases",
            "resolver alias summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "permissions",
        declared_summary_section(
            summary,
            "permissions",
            "resolver permissions summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "role_holders",
        declared_summary_section(
            summary,
            "role_holders",
            "resolver role holder summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "event_summary",
        declared_summary_section(
            summary,
            "event_summary",
            "resolver event summary is not yet projected",
        ),
    );
    declared_state
}

fn build_name_provenance(provenance: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_value_field(
        &mut normalized,
        "normalized_event_ids",
        array_value_strings(provenance_field(provenance, "normalized_event_ids")),
    );
    insert_value_field(
        &mut normalized,
        "raw_fact_refs",
        array_or_empty(provenance_field(provenance, "raw_fact_refs")),
    );
    insert_value_field(
        &mut normalized,
        "manifest_versions",
        array_or_empty(provenance_field(provenance, "manifest_versions")),
    );
    insert_nullable_string_field(
        &mut normalized,
        "execution_trace_id",
        string_field(provenance_field(provenance, "execution_trace_id")),
    );
    insert_string_field(
        &mut normalized,
        "derivation_kind",
        string_field(provenance_field(provenance, "derivation_kind"))
            .unwrap_or_else(|| "declared".to_owned()),
    );
    normalized
}

fn build_name_provenance_with_execution_trace(
    provenance: &JsonValue,
    execution_trace_id: Option<Uuid>,
) -> JsonValue {
    let mut normalized = build_name_provenance(provenance);
    insert_nullable_string_field(
        &mut normalized,
        "execution_trace_id",
        execution_trace_id
            .map(|value| value.to_string())
            .or_else(|| string_field(provenance_field(provenance, "execution_trace_id"))),
    );
    normalized
}

fn build_name_coverage(coverage: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_string_field(
        &mut normalized,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut normalized,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut normalized,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut normalized,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut normalized,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    normalized
}

fn build_name_coverage_declared_state(coverage: &JsonValue) -> JsonValue {
    let mut declared_state = empty_object();
    insert_string_field(
        &mut declared_state,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut declared_state,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut declared_state,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut declared_state,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut declared_state,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    declared_state
}

fn build_name_surface_binding_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "surface_binding",
        build_name_surface_binding_explain_summary(row),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

fn build_name_authority_control_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "authority",
        declared_authority_section(row),
    );
    insert_value_field(
        &mut declared_state,
        "control",
        declared_name_control_section(&row.declared_summary),
    );
    declared_state
}

fn build_name_surface_binding_explain_summary(row: &NameCurrentRow) -> JsonValue {
    let has_binding_summary = row.surface_binding_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared surface binding summary is not yet projected");
    }

    let mut surface_binding = empty_object();
    insert_optional_string_field(
        &mut surface_binding,
        "surface_binding_id",
        row.surface_binding_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut surface_binding,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    surface_binding
}

fn declared_authority_section(row: &NameCurrentRow) -> JsonValue {
    if let Some(section) =
        provenance_field(&row.declared_summary, "authority").filter(|value| value.is_object())
    {
        return section.clone();
    }

    let has_binding_summary =
        row.resource_id.is_some() || row.token_lineage_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared authority summary is not yet projected");
    }

    let mut authority = empty_object();
    insert_optional_string_field(
        &mut authority,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    authority
}

fn declared_name_control_section(summary: &JsonValue) -> JsonValue {
    let Some(section) = provenance_field(summary, "control").filter(|value| value.is_object())
    else {
        return unsupported_section("declared control summary is not yet projected");
    };

    if summary_is_unsupported(Some(section)) {
        return section.clone();
    }

    let mut control = empty_object();
    insert_value_field(
        &mut control,
        "registrant",
        provenance_field(section, "registrant")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "registry_owner",
        provenance_field(section, "registry_owner")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "latest_event_kind",
        provenance_field(section, "latest_event_kind")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    control
}

fn declared_summary_section(summary: &JsonValue, key: &str, unsupported_reason: &str) -> JsonValue {
    provenance_field(summary, key)
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

fn build_record_inventory_section(
    row: Option<&RecordInventoryCurrentRow>,
    unsupported_reason: &str,
) -> JsonValue {
    row.map(build_record_inventory_state)
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

fn build_record_cache_section(
    row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    unsupported_reason: &str,
) -> JsonValue {
    row.map(|row| build_record_cache_state(row, records))
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

fn build_record_inventory_state(row: &RecordInventoryCurrentRow) -> JsonValue {
    let mut record_inventory = empty_object();
    insert_value_field(
        &mut record_inventory,
        "record_version_boundary",
        row.record_version_boundary.clone(),
    );
    insert_value_field(
        &mut record_inventory,
        "enumeration_basis",
        ensure_object(&row.enumeration_basis),
    );
    insert_value_field(
        &mut record_inventory,
        "selectors",
        array_or_empty(Some(&row.selectors)),
    );
    insert_value_field(
        &mut record_inventory,
        "explicit_gaps",
        array_or_empty(Some(&row.explicit_gaps)),
    );
    insert_value_field(
        &mut record_inventory,
        "unsupported_families",
        array_or_empty(Some(&row.unsupported_families)),
    );
    insert_value_field(
        &mut record_inventory,
        "last_change",
        row.last_change.clone().unwrap_or(JsonValue::Null),
    );
    record_inventory
}

fn build_record_cache_state(
    row: &RecordInventoryCurrentRow,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let mut record_cache = empty_object();
    insert_value_field(
        &mut record_cache,
        "record_version_boundary",
        row.record_version_boundary.clone(),
    );
    insert_value_field(
        &mut record_cache,
        "entries",
        build_record_cache_entries(row, records),
    );
    record_cache
}

fn build_record_cache_entries(
    row: &RecordInventoryCurrentRow,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let entry_lookup = row
        .entries
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            string_field(provenance_field(entry, "record_key"))
                .map(|record_key| (record_key, entry))
        })
        .map(|(record_key, entry)| (record_key, entry.clone()))
        .collect::<BTreeMap<_, _>>();
    let unsupported_family_lookup = row
        .unsupported_families
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|family| {
            Some((
                string_field(provenance_field(family, "record_family"))?,
                string_field(provenance_field(family, "unsupported_reason"))?,
            ))
        })
        .collect::<BTreeMap<_, _>>();

    if records.is_empty() {
        return JsonValue::Array(
            row.selectors
                .as_array()
                .into_iter()
                .flatten()
                .filter(|selector| {
                    provenance_field(selector, "cacheable").and_then(JsonValue::as_bool)
                        == Some(true)
                })
                .filter_map(|selector| string_field(provenance_field(selector, "record_key")))
                .filter_map(|record_key| entry_lookup.get(&record_key).cloned())
                .collect(),
        );
    }

    JsonValue::Array(
        records
            .iter()
            .map(|record| {
                entry_lookup
                    .get(&record.record_key)
                    .cloned()
                    .unwrap_or_else(|| {
                        build_missing_record_cache_entry(record, &unsupported_family_lookup)
                    })
            })
            .collect(),
    )
}

fn phase_unsupported_record_family_reason(record_family: &str) -> Option<&'static str> {
    match record_family {
        "abi" | "pubkey" => Some("record_family_not_supported_in_phase6_projection"),
        _ => None,
    }
}

fn build_missing_record_cache_entry(
    record: &ResolutionRecordKey,
    unsupported_family_lookup: &BTreeMap<String, String>,
) -> JsonValue {
    let mut entry = empty_object();
    insert_string_field(&mut entry, "record_key", record.record_key.clone());
    insert_string_field(&mut entry, "record_family", record.record_family.clone());
    insert_nullable_string_field(&mut entry, "selector_key", record.selector_key.clone());

    if let Some(unsupported_reason) = unsupported_family_lookup
        .get(&record.record_family)
        .cloned()
        .or_else(|| {
            phase_unsupported_record_family_reason(&record.record_family).map(str::to_owned)
        })
    {
        insert_string_field(&mut entry, "status", "unsupported".to_owned());
        insert_string_field(&mut entry, "unsupported_reason", unsupported_reason);
    } else {
        insert_string_field(&mut entry, "status", "not_found".to_owned());
    }

    entry
}

fn canonicality_consistency(canonicality_summary: &JsonValue) -> &'static str {
    match string_field(provenance_field(canonicality_summary, "status")).as_deref() {
        Some("safe") => "safe",
        Some("finalized") => "finalized",
        _ => "head",
    }
}

fn collection_consistency<'a>(summaries: impl Iterator<Item = &'a JsonValue>) -> &'static str {
    let mut consistency = "finalized";
    let mut saw_any = false;

    for summary in summaries {
        saw_any = true;
        match canonicality_consistency(summary) {
            "head" => return "head",
            "safe" => consistency = "safe",
            "finalized" => {}
            _ => consistency = "head",
        }
    }

    if saw_any { consistency } else { "head" }
}
