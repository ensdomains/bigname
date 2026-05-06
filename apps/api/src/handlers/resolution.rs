use super::*;
use super::handler_resolution_on_demand::load_or_execute_resolution_verified_outcome;

include!("resolution_explain.rs");

pub(super) async fn resolution_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ResolutionQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    ensure_public_namespace(&namespace)?;

    Ok(Json(
        resolution_response_for_name(&state, &namespace, &name, query).await?,
    ))
}

pub(super) async fn resolve_current(
    Path(name): Path<String>,
    Query(query): Query<InferredResolutionQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    let namespace = infer_resolution_namespace(&name);
    let query = ResolutionQuery {
        mode: query.mode,
        records: query.records,
        ..ResolutionQuery::default()
    };

    Ok(Json(
        resolution_response_for_name(&state, namespace, &name, query).await?,
    ))
}

async fn resolution_response_for_name(
    state: &AppState,
    namespace: &str,
    name: &str,
    query: ResolutionQuery,
) -> ApiResult<ResolutionResponse> {
    let pool = &state.pool;
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let records = parse_resolution_record_keys(query.records.as_deref(), mode)?;
    let selected_snapshot = resolve_exact_name_selected_snapshot(
        pool,
        namespace,
        ExactNameSnapshotSelector::from(&query),
        namespace == BASENAMES_NAMESPACE && mode.includes_verified(),
    )
    .await?;
    let row =
        load_name_current_for_selected_snapshot(pool, namespace, name, &selected_snapshot).await?;

    let record_inventory_current = if mode.includes_declared() || mode.includes_verified() {
        load_resolution_record_inventory_current_for_snapshot(pool, &row, mode, &selected_snapshot)
            .await
            .map_err(snapshot_selection_api_error)?
    } else {
        None
    };

    let persisted_verified_outcome = if mode.includes_verified() {
        load_or_execute_resolution_verified_outcome(
            state,
            &row,
            &records,
            record_inventory_current.as_ref(),
            &selected_snapshot,
            false,
            true,
        )
        .await
        .map_err(snapshot_selection_api_error)?
    } else {
        None
    };

    let logical_name_id = row.logical_name_id.clone();
    build_resolution_response(
        row,
        mode,
        &records,
        record_inventory_current.as_ref(),
        persisted_verified_outcome.as_ref(),
        &selected_snapshot,
    )
    .map_err(|build_error| {
        error!(
            service = "api",
            namespace = %namespace,
            name = %name,
            logical_name_id = %logical_name_id,
            mode = ?mode,
            records = ?records,
            error = ?build_error,
            "failed to build resolution response"
        );
        ApiError::internal_error(format!(
            "failed to load resolution projection for name {namespace}/{name}"
        ))
    })
}

async fn load_resolution_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    mode: ResolutionMode,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let allow_selected_superset =
        row.namespace == BASENAMES_NAMESPACE && mode.includes_verified();
    match load_supported_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await {
        Ok(Some(record_inventory_row)) => Ok(Some(record_inventory_row)),
        Ok(None) => {
            load_record_inventory_current_matching_selected_snapshot(
                pool,
                row,
                selected_snapshot,
                allow_selected_superset,
            )
            .await
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => {
            if let Some(record_inventory_row) =
                load_explicit_unsupported_record_inventory_current(pool, row).await?
            {
                return Ok(Some(record_inventory_row));
            }

            if allow_selected_superset
                && let Some(record_inventory_row) = load_record_inventory_current_matching_selected_snapshot(
                    pool,
                    row,
                    selected_snapshot,
                    true,
                )
                .await?
            {
                return Ok(Some(record_inventory_row));
            }

            if mode.includes_verified() && resolution_verified_support_boundary(row, None).is_none()
            {
                return Ok(None);
            }

            Err(error)
        }
        Err(error) => Err(error),
    }
}

async fn load_record_inventory_current_matching_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let Some((resource_id, _)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    let rows = sqlx::query(
        r#"
        SELECT ric.record_version_boundary, ric.chain_positions
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load record_inventory_current rows for resource_id {resource_id}: {error}"
        ))
    })?;

    let mut matching_rows = Vec::new();
    for candidate in rows {
        if let Some(record_inventory_row) = probe_record_inventory_current_candidate_for_snapshot(
            pool,
            resource_id,
            selected_snapshot,
            allow_selected_superset,
            candidate,
        )
        .await?
        {
            matching_rows.push(record_inventory_row);
        }
    }

    let Some(record_inventory_row) = matching_rows.pop() else {
        return Ok(None);
    };
    if !matching_rows.is_empty() {
        return Err(SnapshotSelectionError::internal(format!(
            "record_inventory_current lookup for resource_id {resource_id} found multiple projection rows for the selected snapshot"
        )));
    }

    Ok(Some(record_inventory_row))
}

async fn probe_record_inventory_current_candidate_for_snapshot(
    pool: &PgPool,
    resource_id: Uuid,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
    candidate: sqlx::postgres::PgRow,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let record_version_boundary =
        candidate
            .try_get::<JsonValue, _>("record_version_boundary")
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary: {error}"
                ))
            })?;

    if allow_selected_superset {
        let chain_positions = candidate
            .try_get::<JsonValue, _>("chain_positions")
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "record_inventory_current lookup for resource_id {resource_id} returned a row without chain_positions: {error}"
                ))
            })?;
        let projected = ChainPositions::from_value(&chain_positions).map_err(|error| {
            SnapshotSelectionError::stale(format!(
                "record_inventory_current projection has unusable chain_positions: {}",
                error.message()
            ))
        })?;
        if !record_inventory_chain_positions_match_selected_snapshot(
            &projected,
            selected_snapshot,
            true,
        ) {
            return Ok(None);
        }

        let record_inventory_row =
            load_record_inventory_current(pool, resource_id, &record_version_boundary)
                .await
                .map_err(|error| {
                    SnapshotSelectionError::internal(format!(
                        "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    SnapshotSelectionError::internal(format!(
                        "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
                    ))
                })?;
        return Ok(Some(record_inventory_row));
    }

    match load_record_inventory_current_for_snapshot(
        pool,
        resource_id,
        &record_version_boundary,
        &selected_snapshot.chain_positions,
    )
    .await
    {
        Ok(SnapshotProjectionRead::Found(record_inventory_row)) => Ok(Some(record_inventory_row)),
        Ok(SnapshotProjectionRead::NotFound) => Err(SnapshotSelectionError::internal(format!(
            "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
        ))),
        Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn record_inventory_chain_positions_match_selected_snapshot(
    projected: &ChainPositions,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> bool {
    if !allow_selected_superset {
        return selected_snapshot
            .chain_positions
            .equivalent_by_chain_id(projected);
    }

    let Some(projected_authoritative_position) = projected
        .as_map()
        .values()
        .find(|position| position.chain_id == bigname_storage::BASE_MAINNET_CHAIN_ID)
    else {
        return false;
    };
    let base_matches_selected = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .any(|selected_position| {
            selected_position.chain_id == bigname_storage::BASE_MAINNET_CHAIN_ID
                && selected_position.block_number == projected_authoritative_position.block_number
                && selected_position.block_hash == projected_authoritative_position.block_hash
                && selected_position.timestamp == projected_authoritative_position.timestamp
        });
    if !base_matches_selected {
        return false;
    }

    projected.as_map().values().all(|projected_position| {
        selected_snapshot
            .chain_positions
            .as_map()
            .values()
            .any(|selected_position| {
                selected_position.chain_id == projected_position.chain_id
                    && selected_position.block_number == projected_position.block_number
                    && selected_position.block_hash == projected_position.block_hash
                    && selected_position.timestamp == projected_position.timestamp
            })
    })
}

fn record_inventory_current_has_explicit_unsupported_coverage(
    row: &RecordInventoryCurrentRow,
) -> bool {
    string_field(provenance_field(&row.coverage, "unsupported_reason")).is_some()
}

async fn load_explicit_unsupported_record_inventory_current(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let Some((resource_id, record_version_boundary)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    if let Some(record_inventory_row) = load_record_inventory_current(
        pool,
        resource_id,
        &record_version_boundary,
    )
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
        ))
    })? {
        return Ok(record_inventory_current_has_explicit_unsupported_coverage(
            &record_inventory_row,
        )
        .then_some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) =
        find_supported_record_inventory_boundary(pool, resource_id, &record_version_boundary)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to locate supported record_inventory_current boundary for resource_id {resource_id}: {error}"
                ))
            })?
    else {
        return Ok(None);
    };

    let record_inventory_row =
        load_record_inventory_current(pool, resource_id, &persisted_boundary)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
                ))
            })?
            .ok_or_else(|| {
                SnapshotSelectionError::internal(format!(
                    "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
                ))
            })?;

    Ok(
        record_inventory_current_has_explicit_unsupported_coverage(&record_inventory_row)
            .then_some(record_inventory_row),
    )
}

pub(super) fn infer_resolution_namespace(name: &str) -> &'static str {
    if name == "base.eth" {
        return "ens";
    }

    if name
        .strip_suffix(".base.eth")
        .is_some_and(|prefix| !prefix.is_empty())
    {
        "basenames"
    } else {
        "ens"
    }
}
