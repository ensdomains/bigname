fn build_name_record_response_for_coin_type(
    record: &bigname_storage::IdentityNameRecordRow,
    primary_coin_type: &str,
    corrected_input_normalization: bool,
) -> NameRecordResponse {
    let coin_type_addresses = identity_coin_type_addresses(record.record_inventory_current.as_ref());
    let text_records = identity_text_records(record.record_inventory_current.as_ref());
    let mut unsupported_fields = identity_unsupported_fields(record);
    let owner_address = identity_relation_subject(
        &record.relations,
        &[bigname_storage::AddressNameRelation::TokenHolder],
    )
    .or_else(|| identity_json_address(&record.row.declared_summary, &[&["control", "owner"]]))
    .or_else(|| {
        identity_json_address(&record.row.declared_summary, &[&["control", "registry_owner"]])
    });
    let manager_address = identity_relation_subject(
        &record.relations,
        &[bigname_storage::AddressNameRelation::EffectiveController],
    );
    let resolver_address =
        identity_json_address(&record.row.declared_summary, &[&["resolver", "address"]]);
    let expiration = identity_json_timestamp(&record.row.declared_summary, &[
        &["registration", "expiry_date"],
        &["registration", "expiry"],
        &["control", "expiry_date"],
        &["control", "expiry"],
    ]);
    let token_id = identity_json_string(&record.row.declared_summary, &[
        &["authority", "token_id"],
        &["registration", "token_id"],
        &["registration", "upstream_resource"],
        &["control", "token_id"],
    ])
    .or_else(|| identity_labelhash_token_id(&record.row));

    if owner_address.is_none() {
        unsupported_fields.insert("owner_address".to_owned());
    }
    if manager_address.is_none() {
        unsupported_fields.insert("manager_address".to_owned());
    }
    if expiration.is_none() {
        unsupported_fields.insert("expiration".to_owned());
    }
    if token_id.is_none() {
        unsupported_fields.insert("token_id".to_owned());
    }

    NameRecordResponse {
        name: record.row.canonical_display_name.clone(),
        normalized_name: record.row.normalized_name.clone(),
        corrected_input_normalization,
        namehash: record.row.namehash.clone(),
        owner_address,
        manager_address,
        primary_address: coin_type_addresses.get(primary_coin_type).cloned(),
        coin_type_addresses,
        text_records,
        resolver_address,
        expiration,
        token_id,
        network: identity_network(&record.row),
        as_of: IdentityAsOfResponse {
            chain_positions: build_chain_positions_from_values(
                std::iter::once(&record.row.chain_positions).chain(
                    record
                        .record_inventory_current
                        .as_ref()
                        .map(|inventory| &inventory.chain_positions),
                ),
            ),
            as_of_timestamp: Some(identity_as_of_timestamp(record)),
        },
        status: identity_record_status(&record.row),
        unsupported_fields: unsupported_fields.into_iter().collect(),
    }
}

pub(crate) async fn build_indexing_status_response(
    read: &bigname_storage::IndexingStatusRead,
    state: &AppState,
) -> IndexingStatusResponse {
    let mut chains = BTreeMap::new();
    let mut saw_stale = false;
    let mut saw_degraded = read.has_unscoped_pending_invalidations;
    for row in &read.chains {
        let projection_lag_blocks = row
            .canonical_block
            .zip(row.latest_projected_block)
            .map(|(canonical, projected)| canonical.saturating_sub(projected));
        let projection_lag_seconds = row
            .canonical_timestamp
            .zip(row.latest_projected_timestamp)
            .map(|(canonical, projected)| (canonical - projected).whole_seconds().max(0));
        let network_head = state
            .status_freshness
            .compare(
                &state.chain_rpc_urls,
                &row.chain_id,
                row.canonical_block,
                row.canonical_timestamp,
            )
            .await;
        match crate::status_freshness::status_readiness(
            row.canonical_block,
            row.latest_projected_block,
            projection_lag_blocks,
            &network_head,
        ) {
            crate::status_freshness::StatusReadiness::Ready => {}
            crate::status_freshness::StatusReadiness::Degraded => saw_degraded = true,
            crate::status_freshness::StatusReadiness::Stale => saw_stale = true,
        }
        chains.insert(
            row.chain_id.clone(),
            IndexingStatusChainResponse {
                canonical_block: row.canonical_block,
                safe_block: row.safe_block,
                finalized_block: row.finalized_block,
                latest_projected_block: row.latest_projected_block,
                latest_projected_timestamp: row.latest_projected_timestamp.map(format_timestamp),
                projection_lag_blocks,
                projection_lag_seconds,
                network_block: network_head.block,
                network_head_observed_at: network_head.observed_at.map(format_timestamp),
                network_head_age_seconds: network_head.age_seconds,
                network_head_status: network_head.status.as_str().to_owned(),
                ingestion_lag_blocks: network_head.ingestion_lag_blocks,
                ingestion_lag_seconds: network_head.ingestion_lag_seconds,
            },
        );
    }

    let status = if saw_stale {
        "stale"
    } else if saw_degraded || chains.is_empty() {
        "degraded"
    } else {
        "ready"
    };

    IndexingStatusResponse {
        status: status.to_owned(),
        pending_invalidation_count: read.pending_invalidation_count,
        dead_letter_count: read.dead_letter_count,
        chains,
    }
}

fn identity_coin_type_addresses(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> BTreeMap<String, String> {
    record_addresses_from_entries(
        inventory.map(|inventory| &inventory.entries),
        provenance_field,
    )
}

fn identity_text_records(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> BTreeMap<String, String> {
    record_text_records_from_entries(
        inventory.map(|inventory| &inventory.entries),
        provenance_field,
    )
}

fn identity_labelhash_token_id(row: &bigname_storage::IdentityNameCurrentRow) -> Option<String> {
    if !identity_supports_labelhash_token_id(row) {
        return None;
    }

    let labelhash = row.labelhash.as_deref()?;
    let hex = labelhash.strip_prefix("0x").unwrap_or(labelhash);
    alloy_primitives::U256::from_str_radix(hex, 16)
        .ok()
        .map(|value| value.to_string())
}

fn identity_supports_labelhash_token_id(row: &bigname_storage::IdentityNameCurrentRow) -> bool {
    row.namespace == "ens"
        && row.labelhash_count == Some(2)
        && row.normalized_name.ends_with(".eth")
        && row.normalized_name.split('.').count() == 2
}

fn identity_unsupported_fields(
    record: &bigname_storage::IdentityNameRecordRow,
) -> BTreeSet<String> {
    record_unsupported_fields(
        record.record_inventory_current.is_some(),
        record
            .record_inventory_current
            .as_ref()
            .map(|inventory| &inventory.unsupported_families),
        provenance_field,
        V1_IDENTITY_RECORD_UNSUPPORTED_FIELD_NAMES,
    )
}

fn identity_relation_subject(
    relations: &[bigname_storage::IdentityAddressRelationRow],
    accepted: &[bigname_storage::AddressNameRelation],
) -> Option<String> {
    let subjects = relations
        .iter()
        .filter(|relation| accepted.contains(&relation.relation))
        .map(|relation| relation.address.clone())
        .collect::<BTreeSet<_>>();

    if subjects.len() == 1 {
        subjects.into_iter().next()
    } else {
        None
    }
}

fn identity_json_string(value: &JsonValue, paths: &[&[&str]]) -> Option<String> {
    record_json_string_at_paths(value, paths, provenance_field)
}

fn identity_json_address(value: &JsonValue, paths: &[&[&str]]) -> Option<String> {
    identity_json_string(value, paths).map(|value| value.to_ascii_lowercase())
}

fn identity_json_timestamp(value: &JsonValue, paths: &[&[&str]]) -> Option<i64> {
    for path in paths {
        let Some(value) = record_json_path(value, path, provenance_field) else {
            continue;
        };
        if let Some(value) = value.as_i64() {
            return Some(value);
        }
        let Some(text) = value.as_str().filter(|value| !value.trim().is_empty()) else {
            continue;
        };
        if let Ok(value) = text.parse::<i64>() {
            return Some(value);
        }
        if let Ok(value) = parse_rfc3339_utc_timestamp(text) {
            return Some(value.unix_timestamp());
        }
    }
    None
}

fn identity_network(row: &bigname_storage::IdentityNameCurrentRow) -> String {
    identity_network_from_parts(&row.namespace, &row.chain_positions)
}

fn identity_network_from_parts(namespace: &str, chain_positions: &JsonValue) -> String {
    record_network_from_chain_positions(namespace, chain_positions, provenance_field)
}

fn identity_record_status(row: &bigname_storage::IdentityNameCurrentRow) -> String {
    identity_record_status_from_coverage(&row.coverage)
}

fn identity_record_status_from_coverage(coverage: &JsonValue) -> String {
    match string_field(provenance_field(coverage, "status")).as_deref() {
        Some("stale") => "stale".to_owned(),
        Some("unsupported") => "unsupported".to_owned(),
        _ => "success".to_owned(),
    }
}

fn identity_as_of_timestamp(record: &bigname_storage::IdentityNameRecordRow) -> String {
    std::iter::once(record.row.last_recomputed_at)
        .chain(
            record
                .record_inventory_current
                .as_ref()
                .map(|inventory| inventory.last_recomputed_at),
        )
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()))
}
