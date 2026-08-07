use super::*;

pub(crate) enum ResolutionLookupError {
    Concurrent(SnapshotSelectionError),
    Snapshot(SnapshotSelectionError),
}

impl ResolutionLookupError {
    pub(crate) fn into_snapshot(self) -> SnapshotSelectionError {
        match self {
            Self::Concurrent(error) | Self::Snapshot(error) => error,
        }
    }
}

impl From<SnapshotSelectionError> for ResolutionLookupError {
    fn from(error: SnapshotSelectionError) -> Self {
        Self::Snapshot(error)
    }
}

/// Executes a fresh schema-v2 lookup. The lookup engine owns any guarded
/// divergence-ledger write; the API never writes a legacy execution outcome.
pub(crate) async fn execute_resolution_lookup(
    state: &AppState,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
) -> std::result::Result<Option<bigname_lookup::LookupResponse>, ResolutionLookupError> {
    if records.is_empty() {
        return Ok(None);
    }

    let logical_name_id = schema_v2_logical_name_id(row)?;
    let request = bigname_lookup::LookupRequest::new(
        logical_name_id,
        records.iter().map(|record| record.record_key.as_str()),
    )
    .map_err(lookup_snapshot_error)?;
    let timer = crate::metrics::verified_execution_timer();
    let admitted_positions = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .map(|position| bigname_lookup::LookupPosition {
            chain_id: position.chain_id.clone(),
            block_number: position.block_number,
            block_hash: position.block_hash.clone(),
            timestamp: format_timestamp(position.timestamp),
        })
        .collect::<Vec<_>>();
    let response =
        bigname_lookup::LookupEngine::new(state.pool.clone(), state.lookup_chain_rpc_urls.clone())
            .lookup_at_positions(request, &admitted_positions)
            .await;
    match response {
        Ok(response) => {
            expose_lookup_positions(selected_snapshot, &response)?;
            let outcome = if response
                .records
                .iter()
                .all(|record| record.status == bigname_lookup::LookupRecordStatus::Success)
            {
                "success"
            } else {
                "partial"
            };
            timer.finish(outcome);
            Ok(Some(response))
        }
        Err(error) if error.kind() == bigname_lookup::ErrorKind::Unsupported => {
            timer.finish("unsupported");
            Ok(None)
        }
        Err(error) => {
            timer.finish("failed");
            Err(lookup_snapshot_error(error))
        }
    }
}

fn expose_lookup_positions(
    selected_snapshot: &mut SelectedSnapshot,
    response: &bigname_lookup::LookupResponse,
) -> std::result::Result<(), SnapshotSelectionError> {
    let mut positions = selected_snapshot.chain_positions.as_map().clone();
    for lookup_position in [
        &response.authoritative_position,
        &response.execution_position,
    ] {
        let Some(slot) = positions.iter().find_map(|(slot, position)| {
            (position.chain_id == lookup_position.chain_id).then(|| slot.clone())
        }) else {
            return Err(SnapshotSelectionError::stale(
                "verified lookup returned a position outside the selected chain scope",
            ));
        };
        let timestamp =
            parse_rfc3339_utc_timestamp(&lookup_position.timestamp).map_err(|error| {
                error!(
                    service = "api",
                    timestamp = %lookup_position.timestamp,
                    error = ?error,
                    "schema-v2 verified lookup returned an invalid timestamp"
                );
                SnapshotSelectionError::internal(
                    "verified lookup returned an invalid execution position",
                )
            })?;
        positions.insert(
            slot.clone(),
            ChainPosition {
                slot,
                chain_id: lookup_position.chain_id.clone(),
                block_number: lookup_position.block_number,
                block_hash: lookup_position.block_hash.clone(),
                timestamp,
            },
        );
    }
    selected_snapshot.chain_positions = ChainPositions::new(positions);
    Ok(())
}

fn schema_v2_logical_name_id(
    row: &NameCurrentRow,
) -> std::result::Result<String, SnapshotSelectionError> {
    let namehash = row.namehash.to_ascii_lowercase();
    let valid = namehash.strip_prefix("0x").is_some_and(|digits| {
        digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit())
    });
    if !valid {
        return Err(SnapshotSelectionError::stale(
            "verified lookup name identity is unavailable",
        ));
    }
    Ok(format!("{}:{namehash}", row.namespace))
}

pub(crate) fn lookup_snapshot_error(error: bigname_lookup::LookupError) -> ResolutionLookupError {
    match error.kind() {
        bigname_lookup::ErrorKind::ConcurrentState => ResolutionLookupError::Concurrent(
            SnapshotSelectionError::stale(error.message().to_owned()),
        ),
        bigname_lookup::ErrorKind::Configuration | bigname_lookup::ErrorKind::Stale => {
            ResolutionLookupError::Snapshot(SnapshotSelectionError::stale(
                error.message().to_owned(),
            ))
        }
        bigname_lookup::ErrorKind::Unsupported => ResolutionLookupError::Snapshot(
            SnapshotSelectionError::stale("verified lookup is not supported for this name"),
        ),
        bigname_lookup::ErrorKind::Transport
        | bigname_lookup::ErrorKind::Execution
        | bigname_lookup::ErrorKind::Database => ResolutionLookupError::Snapshot(
            SnapshotSelectionError::internal(error.message().to_owned()),
        ),
    }
}

pub(crate) fn build_lookup_resolution_verified_state(
    records: &[ResolutionRecordKey],
    lookup_response: Option<&bigname_lookup::LookupResponse>,
) -> JsonValue {
    let lookup_results = lookup_response
        .map(|response| {
            response
                .records
                .iter()
                .map(|record| (record.record_key.as_str(), record))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    json!({
        "verified_queries": records
            .iter()
            .map(|record| {
                let Some(result) = lookup_results.get(record.record_key.as_str()) else {
                    return json!({
                        "record_key": record.record_key,
                        "status": "unsupported",
                        "unsupported_reason": "verified resolution entrypoint is not yet supported",
                    });
                };
                let mut value = json!({
                    "record_key": result.record_key,
                    "status": result.status.as_str(),
                });
                if let Some(answer) = &result.value {
                    value["value"] = answer.clone();
                }
                if let Some(reason) = &result.failure_reason {
                    value["failure_reason"] = JsonValue::String(reason.clone());
                }
                if let Some(reason) = &result.unsupported_reason {
                    value["unsupported_reason"] = JsonValue::String(reason.clone());
                }
                value
            })
            .collect::<Vec<_>>()
    })
}
