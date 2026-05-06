use super::*;

use super::handler_resolution_on_demand::load_or_execute_resolution_verified_outcome;

pub(super) struct ResolutionRecordsRead {
    pub(super) row: NameCurrentRow,
    pub(super) mode: ResolutionMode,
    pub(super) records: Vec<ResolutionRecordKey>,
    pub(super) selected_snapshot: SelectedSnapshot,
    pub(super) record_inventory_current: Option<RecordInventoryCurrentRow>,
    pub(super) persisted_verified_outcome: Option<ExecutionOutcome>,
}

pub(super) struct CompactRecordsRead {
    pub(super) row: NameCurrentRow,
    pub(super) record_inventory_current: Option<RecordInventoryCurrentRow>,
    pub(super) value_source: CompactNameRecordsValueSource,
    pub(super) verified_outcome: Option<ExecutionOutcome>,
}

pub(super) fn infer_resolution_namespace(name: &str) -> &'static str {
    if name == "base.eth" {
        return bigname_storage::ENS_NAMESPACE;
    }

    if name
        .strip_suffix(".base.eth")
        .is_some_and(|prefix| !prefix.is_empty())
    {
        bigname_storage::BASENAMES_NAMESPACE
    } else {
        bigname_storage::ENS_NAMESPACE
    }
}

pub(super) async fn load_resolution_records_read(
    state: &AppState,
    namespace: &str,
    name: &str,
    query: ResolutionQuery,
) -> ApiResult<ResolutionRecordsRead> {
    let pool = &state.pool;
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let records = parse_resolution_record_keys(query.records.as_deref(), mode)?;
    let ExactNameRead {
        row,
        selected_snapshot,
    } = load_exact_name_read_for_route(
        pool,
        ExactNameReadRequest::new(namespace, name, ExactNameSnapshotSelector::from(&query))
            .include_resolution_auxiliary(namespace == BASENAMES_NAMESPACE && mode.includes_verified()),
    )
    .await?;

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

    Ok(ResolutionRecordsRead {
        row,
        mode,
        records,
        selected_snapshot,
        record_inventory_current,
        persisted_verified_outcome,
    })
}

pub(super) async fn load_compact_records_read(
    state: &AppState,
    namespace: &str,
    name: &str,
    query: NameRecordsQuery,
    default_mode: CompactNameRecordsDefaultMode,
) -> ApiResult<(CompactNameRecordsRequest, CompactRecordsRead)> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is not supported for compact name records",
    )?;

    let request = parse_compact_name_records_request(&query, default_mode)?;
    let (row, is_wildcard_candidate) =
        load_compact_records_target(&state.pool, namespace, name).await?;
    let record_inventory_current = if is_wildcard_candidate {
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
    let value_source =
        if is_wildcard_candidate && request.mode != CompactNameRecordsMode::Declared {
            CompactNameRecordsValueSource::Verified
        } else {
            compact_name_records_value_source(&row, record_inventory_current.as_ref(), &request)
        };
    let requested_records =
        compact_name_records_requested_records(record_inventory_current.as_ref(), &request);
    let verified_outcome = load_compact_records_verified_outcome(
        state,
        namespace,
        &row,
        record_inventory_current.as_ref(),
        &requested_records,
        value_source,
    )
    .await?;

    Ok((
        request,
        CompactRecordsRead {
            row,
            record_inventory_current,
            value_source,
            verified_outcome,
        },
    ))
}

async fn load_resolution_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    mode: ResolutionMode,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let allow_selected_superset = row.namespace == BASENAMES_NAMESPACE && mode.includes_verified();
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
                && let Some(record_inventory_row) =
                    load_record_inventory_current_matching_selected_snapshot(
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

async fn load_compact_records_target(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> ApiResult<(NameCurrentRow, bool)> {
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
        return Ok((row, false));
    }

    if namespace != bigname_storage::ENS_NAMESPACE || !name.contains('.') {
        return Err(name_not_found_error(namespace, name));
    }

    if let Some(source_row) = load_compact_records_wildcard_source(pool, namespace, name).await? {
        return Ok((
            compact_records_wildcard_candidate_row(source_row, namespace, name),
            true,
        ));
    }

    Err(name_not_found_error(namespace, name))
}

async fn load_compact_records_wildcard_source(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> ApiResult<Option<NameCurrentRow>> {
    let labels = name.split('.').collect::<Vec<_>>();
    for index in 1..labels.len() {
        let ancestor_name = labels[index..].join(".");
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
    let current_positions =
        ChainPositions::from_value(&row.chain_positions).map_err(snapshot_selection_api_error)?;
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

async fn load_compact_records_verified_outcome(
    state: &AppState,
    namespace: &str,
    row: &NameCurrentRow,
    record_inventory_current: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
    value_source: CompactNameRecordsValueSource,
) -> ApiResult<Option<ExecutionOutcome>> {
    if value_source != CompactNameRecordsValueSource::Verified || requested_records.is_empty() {
        return Ok(None);
    }

    let selected_snapshot = resolve_exact_name_selected_snapshot(
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
    })?;

    load_or_execute_resolution_verified_outcome(
        state,
        row,
        requested_records,
        record_inventory_current,
        &selected_snapshot,
        true,
        false,
    )
    .await
    .map_err(snapshot_selection_api_error)
}
