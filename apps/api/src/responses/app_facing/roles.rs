pub(crate) type CompactRolesResponse = JsonValue;
pub(crate) type ResourceLookupResponse = JsonValue;

pub(crate) fn build_compact_roles_response(
    rows: &[PermissionsCurrentRow],
    associated_names: &BTreeMap<Uuid, String>,
    summary: &bigname_storage::PermissionsCurrentFullFilterSummary,
    page: HistoryPageResponse,
    meta_mode: MetaMode,
) -> CompactRolesResponse {
    build_roles_response(
        rows,
        associated_names,
        summary.row_count.max(0) as u64,
        page,
        meta_mode,
    )
}

pub(crate) fn build_empty_compact_roles_response(
    page: HistoryPageResponse,
    meta_mode: MetaMode,
) -> CompactRolesResponse {
    build_roles_response(&[], &BTreeMap::new(), 0, page, meta_mode)
}

pub(crate) fn build_resource_lookup_response(
    row: &NameCurrentRow,
    resource_id: Uuid,
    meta_mode: MetaMode,
) -> ResourceLookupResponse {
    let mut data = empty_object();
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "name", row.canonical_display_name.clone());
    insert_string_field(&mut data, "normalized_name", row.normalized_name.clone());
    insert_string_field(&mut data, "resource_id", resource_id.to_string());
    insert_value_field(&mut data, "resource_hex", JsonValue::Null);

    let mut response = empty_object();
    insert_value_field(&mut response, "data", data);
    insert_compact_meta(
        &mut response,
        meta_mode,
        None,
        &["resource_hex"],
        &[],
    );
    response
}

fn build_roles_response(
    rows: &[PermissionsCurrentRow],
    associated_names: &BTreeMap<Uuid, String>,
    total_count: u64,
    page: HistoryPageResponse,
    meta_mode: MetaMode,
) -> CompactRolesResponse {
    let data = rows
        .iter()
        .map(|row| build_role_row(row, associated_names.get(&row.resource_id)))
        .collect::<Vec<_>>();

    let mut response = empty_object();
    insert_value_field(&mut response, "data", JsonValue::Array(data));
    insert_value_field(
        &mut response,
        "page",
        serde_json::to_value(page).expect("roles page response must serialize"),
    );
    insert_compact_meta(
        &mut response,
        meta_mode,
        Some(total_count),
        &["resource_hex", "role_bitmap"],
        &[],
    );
    response
}

fn build_role_row(row: &PermissionsCurrentRow, name: Option<&String>) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "account", row.subject.clone());
    insert_value_field(&mut value, "resource_hex", JsonValue::Null);
    insert_string_field(&mut value, "resource_id", row.resource_id.to_string());
    insert_nullable_string_field(
        &mut value,
        "name",
        name.cloned(),
    );
    // permissions_current currently exposes post-scope powers, not a raw role bitmap.
    insert_value_field(&mut value, "role_bitmap", JsonValue::Null);
    insert_value_field(&mut value, "effective_powers", row.effective_powers.clone());
    insert_value_field(&mut value, "provenance", build_role_row_provenance(row));
    value
}

fn build_role_row_provenance(row: &PermissionsCurrentRow) -> JsonValue {
    let mut provenance = empty_object();
    if let Some(position) = first_chain_position(&row.chain_positions) {
        for field in ["chain_id", "block_number", "block_hash", "timestamp"] {
            if let Some(value) = position.get(field).cloned() {
                insert_value_field(&mut provenance, field, value);
            }
        }
    }

    if let Some(raw_ref) = first_raw_fact_ref(&row.provenance) {
        if let Some(tx_hash) = raw_ref
            .get("transaction_hash")
            .or_else(|| raw_ref.get("tx_hash"))
            .and_then(JsonValue::as_str)
        {
            insert_string_field(&mut provenance, "tx_hash", tx_hash.to_owned());
        }
        if let Some(log_index) = raw_ref.get("log_index").cloned() {
            insert_value_field(&mut provenance, "log_index", log_index);
        }
    }

    provenance
}

fn first_chain_position(chain_positions: &JsonValue) -> Option<&JsonMap<String, JsonValue>> {
    let positions = chain_positions.as_object()?;
    positions
        .iter()
        .filter_map(|(slot, value)| value.as_object().map(|position| (slot, position)))
        .min_by(|(left_slot, _), (right_slot, _)| left_slot.cmp(right_slot))
        .map(|(_, position)| position)
}

fn first_raw_fact_ref(provenance: &JsonValue) -> Option<&JsonMap<String, JsonValue>> {
    provenance
        .get("raw_fact_refs")
        .and_then(JsonValue::as_array)?
        .iter()
        .find_map(JsonValue::as_object)
}

fn insert_compact_meta(
    response: &mut JsonValue,
    meta_mode: MetaMode,
    total_count: Option<u64>,
    unsupported_fields: &[&str],
    unsupported_filters: &[&str],
) {
    if matches!(meta_mode, MetaMode::None) {
        return;
    }

    let mut meta = empty_object();
    insert_string_field(&mut meta, "support_status", "supported".to_owned());
    insert_value_field(
        &mut meta,
        "unsupported_filters",
        JsonValue::Array(
            unsupported_filters
                .iter()
                .map(|field| JsonValue::String((*field).to_owned()))
                .collect(),
        ),
    );
    insert_value_field(
        &mut meta,
        "unsupported_fields",
        JsonValue::Array(
            unsupported_fields
                .iter()
                .map(|field| JsonValue::String((*field).to_owned()))
                .collect(),
        ),
    );
    insert_value_field(
        &mut meta,
        "total_count",
        total_count
            .map(serde_json::Number::from)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(response, "meta", meta);
}
