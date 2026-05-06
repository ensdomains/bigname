use super::*;
use super::handler_resolution_on_demand::load_or_execute_resolution_verified_outcome;

pub(super) async fn name_records(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<NameRecordsQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactNameRecordsResponse>> {
    ensure_public_namespace(&namespace)?;

    Ok(Json(
        compact_name_records_response_for_name(
            &state,
            &namespace,
            &name,
            query,
            CompactNameRecordsDefaultMode::Declared,
        )
        .await?,
    ))
}

pub(super) async fn resolve_records(
    Path(name): Path<String>,
    Query(query): Query<NameRecordsQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactNameRecordsResponse>> {
    let namespace = infer_resolution_namespace(&name);

    Ok(Json(
        compact_name_records_response_for_name(
            &state,
            namespace,
            &name,
            query,
            CompactNameRecordsDefaultMode::Auto,
        )
        .await?,
    ))
}

include!("records_warmup.rs");

async fn compact_name_records_response_for_name(
    state: &AppState,
    namespace: &str,
    name: &str,
    query: NameRecordsQuery,
    default_mode: CompactNameRecordsDefaultMode,
) -> ApiResult<CompactNameRecordsResponse> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is not supported for compact name records",
    )?;

    let request = parse_compact_name_records_request(&query, default_mode)?;
    let target = load_compact_records_target(&state.pool, namespace, name).await?;
    let row = target.row;

    let record_inventory_current = if target.is_wildcard_candidate {
        None
    } else {
        load_compact_records_current_inventory(&state.pool, &row)
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %row.logical_name_id,
                status = %load_error.status,
                code = %load_error.code,
                message = %load_error.message,
                "failed to load declared record inventory for compact records route"
            );
                map_internal_api_error(
                    load_error,
                    format!("failed to load compact records projection for name {namespace}/{name}"),
                )
            })?
    };
    let value_source = if target.is_wildcard_candidate && request.mode != CompactNameRecordsMode::Declared
    {
        CompactNameRecordsValueSource::Verified
    } else {
        compact_name_records_value_source(&row, record_inventory_current.as_ref(), &request)
    };
    let requested_records =
        compact_name_records_requested_records(record_inventory_current.as_ref(), &request);
    let selected_snapshot =
        compact_records_selected_snapshot(state, namespace, &row, value_source).await?;
    let verified_outcome = load_compact_records_verified_outcome(
        state,
        &row,
        record_inventory_current.as_ref(),
        &selected_snapshot,
        &requested_records,
        value_source,
    )
    .await?;

    Ok(build_compact_name_records_response(
        &row,
        record_inventory_current.as_ref(),
        &request,
        value_source,
        verified_outcome.as_ref(),
    ))
}

struct CompactRecordsTarget {
    row: NameCurrentRow,
    is_wildcard_candidate: bool,
}

async fn load_compact_records_target(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> ApiResult<CompactRecordsTarget> {
    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                error = ?load_error,
                "failed to load current exact-name projection for compact records route"
            );
            ApiError::internal_error(format!(
                "failed to load compact records projection for name {namespace}/{name}"
            ))
        })?;
    if let Some(row) = row {
        return Ok(CompactRecordsTarget {
            row,
            is_wildcard_candidate: false,
        });
    }

    if namespace != bigname_storage::ENS_NAMESPACE || !name.contains('.') {
        return Err(name_not_found_error(namespace, name));
    }

    if let Some(source_row) = load_compact_records_wildcard_source(pool, namespace, name).await? {
        return Ok(CompactRecordsTarget {
            row: compact_records_wildcard_candidate_row(source_row, namespace, name),
            is_wildcard_candidate: true,
        });
    }

    Err(name_not_found_error(namespace, name))
}

async fn load_compact_records_wildcard_source(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> ApiResult<Option<NameCurrentRow>> {
    for ancestor_name in compact_records_ancestor_names(name) {
        let logical_name_id = format!("{namespace}:{ancestor_name}");
        let Some(row) = load_name_current(pool, &logical_name_id)
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                    namespace = %namespace,
                    name = %name,
                    ancestor_name = %ancestor_name,
                    error = ?load_error,
                    "failed to load wildcard ancestor projection for compact records route"
                );
                ApiError::internal_error(format!(
                    "failed to load compact records projection for name {namespace}/{name}"
                ))
            })?
        else {
            continue;
        };
        if compact_resolver_address_is_present(&row) {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

fn compact_records_ancestor_names(name: &str) -> Vec<String> {
    let labels = name.split('.').collect::<Vec<_>>();
    if labels.len() < 2 {
        return Vec::new();
    }
    (1..labels.len())
        .map(|index| labels[index..].join("."))
        .collect()
}

fn compact_resolver_address_is_present(row: &NameCurrentRow) -> bool {
    row.declared_summary
        .get("resolver")
        .and_then(|resolver| resolver.get("address"))
        .and_then(JsonValue::as_str)
        .is_some_and(|address| !address.is_empty())
}

fn compact_records_wildcard_candidate_row(
    mut source_row: NameCurrentRow,
    namespace: &str,
    name: &str,
) -> NameCurrentRow {
    source_row.logical_name_id = format!("{namespace}:{name}");
    source_row.canonical_display_name = name.to_owned();
    source_row.normalized_name = name.to_owned();
    source_row.namehash = format!("namehash:{name}");
    source_row.binding_kind = Some(SurfaceBindingKind::DeclaredRegistryPath);
    source_row
}

async fn load_compact_records_current_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> ApiResult<Option<RecordInventoryCurrentRow>> {
    if let Some(record_inventory_current) = load_supported_record_inventory_current(pool, row)
        .await
        .map_err(|error| {
            ApiError::internal_error(format!(
                "failed to load current compact record inventory for {}: {error}",
                row.logical_name_id
            ))
        })?
    {
        return Ok(Some(record_inventory_current));
    }

    let Some(resource_id) = row.resource_id else {
        return Ok(None);
    };
    let current_positions = ChainPositions::from_value(&row.chain_positions)
        .map_err(snapshot_selection_api_error)?;
    let candidates = sqlx::query(
        r#"
        SELECT
            ric.record_version_boundary,
            ric.coverage,
            ric.chain_positions,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
          ((ric.coverage ->> 'status') = 'full'
            AND (ric.coverage ->> 'unsupported_reason') IS NULL) DESC,
          ric.last_recomputed_at DESC
        "#,
    )
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        ApiError::internal_error(format!(
            "failed to list current compact record inventory candidates for {}: {error}",
            row.logical_name_id
        ))
    })?;

    probe_compact_record_inventory_candidates(pool, row, resource_id, &current_positions, candidates)
        .await
}

async fn probe_compact_record_inventory_candidates(
    pool: &PgPool,
    row: &NameCurrentRow,
    resource_id: Uuid,
    current_positions: &ChainPositions,
    candidates: Vec<sqlx::postgres::PgRow>,
) -> ApiResult<Option<RecordInventoryCurrentRow>> {
    for candidate in candidates {
        let candidate_positions = candidate
            .try_get::<JsonValue, _>("chain_positions")
            .map_err(|error| {
                ApiError::internal_error(format!(
                    "record_inventory_current candidate for {} did not include chain_positions: {error}",
                    row.logical_name_id
                ))
            })?;
        let candidate_positions =
            ChainPositions::from_value(&candidate_positions).map_err(snapshot_selection_api_error)?;
        if !current_positions.equivalent_by_chain_id(&candidate_positions) {
            continue;
        }

        let record_version_boundary =
            candidate
                .try_get::<JsonValue, _>("record_version_boundary")
                .map_err(|error| {
                    ApiError::internal_error(format!(
                        "record_inventory_current candidate for {} did not include record_version_boundary: {error}",
                        row.logical_name_id
                    ))
                })?;
        return load_record_inventory_current(pool, resource_id, &record_version_boundary)
            .await
            .map_err(|error| {
                ApiError::internal_error(format!(
                    "failed to load current compact record inventory candidate for {}: {error}",
                    row.logical_name_id
                ))
            });
    }

    Ok(None)
}

async fn compact_records_selected_snapshot(
    state: &AppState,
    namespace: &str,
    row: &NameCurrentRow,
    value_source: CompactNameRecordsValueSource,
) -> ApiResult<SelectedSnapshot> {
    if value_source == CompactNameRecordsValueSource::Verified {
        return resolve_exact_name_selected_snapshot(
            &state.pool,
            namespace,
            ExactNameSnapshotSelector::default(),
            namespace == BASENAMES_NAMESPACE,
        )
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                logical_name_id = %row.logical_name_id,
                status = %load_error.status,
                code = %load_error.code,
                message = %load_error.message,
                "failed to select current verified-execution snapshot for compact records route"
            );
            map_internal_api_error(
                load_error,
                format!(
                    "failed to select compact records verified execution snapshot for {}",
                    row.logical_name_id
                ),
            )
        });
    }

    let chain_positions =
        ChainPositions::from_value(&row.chain_positions).map_err(snapshot_selection_api_error)?;
    Ok(SelectedSnapshot {
        chain_positions,
        consistency: SnapshotConsistency::Head,
    })
}

async fn load_compact_records_verified_outcome(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory_current: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    requested_records: &[ResolutionRecordKey],
    value_source: CompactNameRecordsValueSource,
) -> ApiResult<Option<ExecutionOutcome>> {
    if value_source != CompactNameRecordsValueSource::Verified || requested_records.is_empty() {
        return Ok(None);
    }

    load_or_execute_resolution_verified_outcome(
        state,
        row,
        requested_records,
        record_inventory_current,
        selected_snapshot,
        true,
        false,
    )
    .await
    .map_err(snapshot_selection_api_error)
}
