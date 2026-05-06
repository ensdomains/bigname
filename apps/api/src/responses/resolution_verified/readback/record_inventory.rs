use super::*;

pub(super) async fn load_supported_record_inventory_current(
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

pub(super) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let Some((resource_id, record_version_boundary)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    match load_record_inventory_current_for_snapshot(
        pool,
        resource_id,
        &record_version_boundary,
        &selected_snapshot.chain_positions,
    )
    .await?
    {
        SnapshotProjectionRead::Found(record_inventory_row) => {
            return Ok(Some(record_inventory_row));
        }
        SnapshotProjectionRead::NotFound => {}
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
        return load_covering_record_inventory_current_for_snapshot(
            pool,
            resource_id,
            selected_snapshot,
        )
        .await;
    };

    match load_record_inventory_current_for_snapshot(
        pool,
        resource_id,
        &persisted_boundary,
        &selected_snapshot.chain_positions,
    )
    .await?
    {
        SnapshotProjectionRead::Found(record_inventory_row) => Ok(Some(record_inventory_row)),
        SnapshotProjectionRead::NotFound => Err(SnapshotSelectionError::internal(format!(
            "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
        ))),
    }
}

async fn load_covering_record_inventory_current_for_snapshot(
    pool: &PgPool,
    resource_id: Uuid,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let rows = sqlx::query(
        r#"
        SELECT ric.record_version_boundary
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
        let record_version_boundary =
            candidate
                .try_get::<JsonValue, _>("record_version_boundary")
                .map_err(|error| {
                    SnapshotSelectionError::internal(format!(
                        "record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary: {error}"
                    ))
                })?;

        match load_record_inventory_current_for_snapshot(
            pool,
            resource_id,
            &record_version_boundary,
            &selected_snapshot.chain_positions,
        )
        .await
        {
            Ok(SnapshotProjectionRead::Found(record_inventory_row)) => {
                matching_rows.push(record_inventory_row);
            }
            Ok(SnapshotProjectionRead::NotFound) => {
                return Err(SnapshotSelectionError::internal(format!(
                    "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
                )));
            }
            Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => {
                continue;
            }
            Err(error) => return Err(error),
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

pub(super) fn record_inventory_lookup_key(row: &NameCurrentRow) -> Option<(Uuid, JsonValue)> {
    bigname_storage::resolution_record_inventory_lookup_key(row)
}

pub(super) fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
    bigname_storage::resolution_verified_support_boundary(row, record_inventory_row)
}

pub(super) fn record_version_boundary_has_pointer(record_version_boundary: &JsonValue) -> bool {
    provenance_field(record_version_boundary, "normalized_event_id")
        .is_some_and(|value| !value.is_null())
        && provenance_field(record_version_boundary, "event_kind")
            .is_some_and(|value| !value.is_null())
}

pub(super) async fn find_supported_record_inventory_boundary(
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
    if let Some(second_boundary) = second_boundary
        && (!record_version_boundary_has_pointer(&first_boundary)
            || record_version_boundary_has_pointer(second_boundary))
    {
        anyhow::bail!(
            "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
            resource_id
        );
    }

    Ok(Some(first_boundary))
}
