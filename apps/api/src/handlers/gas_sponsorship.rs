use super::*;

pub(super) async fn gas_sponsorship(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<GasSponsorshipResponse>> {
    let name = parse_exact_name_path_name(&namespace, &name)?;
    let logical_name_id = format!("{namespace}:{name}");

    let name_row = load_gas_sponsorship_current(&state.pool, &logical_name_id)
        .await
        .map_err(|_| {
            ApiError::internal_error(format!(
                "failed to load gas sponsorship accounting for {logical_name_id}"
            ))
        })?;
    let global_row = load_gas_sponsorship_global_current(&state.pool, &namespace)
        .await
        .map_err(|_| {
            ApiError::internal_error(format!(
                "failed to load global gas sponsorship accounting for {namespace}"
            ))
        })?;

    Ok(Json(build_gas_sponsorship_response(
        namespace, name, name_row, global_row,
    )))
}

/// Valid names with no accounting rows answer with zeros: the sponsorship
/// predicate needs a decisive answer and zero earned already denies.
fn build_gas_sponsorship_response(
    namespace: String,
    name: String,
    name_row: Option<GasSponsorshipCurrentRow>,
    global_row: Option<GasSponsorshipGlobalCurrentRow>,
) -> GasSponsorshipResponse {
    let last_updated = name_row
        .as_ref()
        .map(|row| row.last_recomputed_at)
        .into_iter()
        .chain(global_row.as_ref().map(|row| row.last_recomputed_at))
        .max()
        .unwrap_or_else(OffsetDateTime::now_utc);

    let name_accounting = match &name_row {
        Some(row) => json!({
            "logical_name_id": row.logical_name_id,
            "namehash": row.namehash,
            "lease_start_at": row.lease_start_at.map(format_timestamp),
            "registered_seconds_total": row.registered_seconds_total,
            "earned_updates": row.earned_updates,
            "spent_updates": row.spent_updates,
            "last_sponsored_write_at": row.last_sponsored_write_at.map(format_timestamp),
        }),
        None => json!({
            "logical_name_id": format!("{namespace}:{name}"),
            "namehash": JsonValue::Null,
            "lease_start_at": JsonValue::Null,
            "registered_seconds_total": 0,
            "earned_updates": 0,
            "spent_updates": 0,
            "last_sponsored_write_at": JsonValue::Null,
        }),
    };
    let global_accounting = match &global_row {
        Some(row) => json!({
            "sponsored_op_count": row.sponsored_op_count,
            "attributed_op_count": row.attributed_op_count,
            "failed_op_count": row.failed_op_count,
            "gas_wei_total": row.gas_wei_total,
            "failed_gas_wei_total": row.failed_gas_wei_total,
            "usd_e8_total": row.usd_e8_total,
            "unpriced_wei_total": row.unpriced_wei_total,
        }),
        None => json!({
            "sponsored_op_count": 0,
            "attributed_op_count": 0,
            "failed_op_count": 0,
            "gas_wei_total": "0",
            "failed_gas_wei_total": "0",
            "usd_e8_total": "0",
            "unpriced_wei_total": "0",
        }),
    };

    let provenance = name_row
        .as_ref()
        .map(|row| row.provenance.clone())
        .unwrap_or(JsonValue::Null);
    let coverage = name_row
        .as_ref()
        .map(|row| row.coverage.clone())
        .or_else(|| global_row.as_ref().map(|row| row.coverage.clone()))
        .unwrap_or_else(|| {
            json!({
                "status": "partial",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [
                    "ens_gas_sponsorship_l1",
                    "ens_v1_registrar_l1",
                    "ens_v2_registrar_l1",
                ],
                "enumeration_basis": "gas_sponsorship_lookup",
                "unsupported_reason": JsonValue::Null,
            })
        });
    let chain_positions = name_row
        .as_ref()
        .map(|row| row.chain_positions.clone())
        .or_else(|| global_row.as_ref().map(|row| row.chain_positions.clone()))
        .unwrap_or_else(|| json!({}));

    GasSponsorshipResponse {
        data: json!({ "namespace": namespace, "name": name }),
        name_accounting,
        global_accounting,
        provenance,
        coverage,
        chain_positions,
        consistency: "head".to_owned(),
        last_updated: format_timestamp(last_updated),
    }
}
