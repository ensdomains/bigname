use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{
    ChainPositions, IdentityNameRecordRow, ReadDb, SelectedSnapshot, SnapshotConsistency,
};
use tracing::error;

use crate::v2::{
    Status, V2Error, V2Result, load_subregistry_refs,
    registries::snapshot_block_for_chain,
    support::{lease_row_shape, serve_legacy_lease_in_savepoint},
};

use super::{
    admission::require_name_records_at_served_head,
    build::{build_forward_detail_record, build_forward_feed_record},
    dto::{LookupKind, LookupResult},
    parse::{LookupProfile, ParsedNameLookup},
    result_failure_reason, result_unsupported_reason,
};

pub(super) async fn render_name_lookup_results(
    db: &mut ReadDb<'_>,
    profile: LookupProfile,
    inputs: &[ParsedNameLookup],
    selected_snapshot: Option<&SelectedSnapshot>,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let logical_name_ids = inputs
        .iter()
        .filter_map(|input| {
            input
                .lookup
                .as_ref()
                .map(|lookup| lookup.logical_name_id.clone())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut records =
        load_name_records(db.reborrow(), profile, &logical_name_ids, selected_snapshot).await?;
    serve_legacy_leases(db, &mut records, selected_snapshot).await?;
    let mut names_by_block = BTreeMap::<i64, Vec<String>>::new();
    if let Some(selected) = selected_snapshot {
        for (logical_name_id, record) in &records {
            let block = record
                .row
                .provenance
                .get("chain_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|chain_id| snapshot_block_for_chain(selected, chain_id))
                .ok_or_else(|| V2Error::stale("lookup name has no selected chain position"))?;
            names_by_block
                .entry(block)
                .or_default()
                .push(logical_name_id.clone());
        }
    }
    let mut subregistries = BTreeMap::new();
    for (block, names) in names_by_block {
        subregistries.extend(load_subregistry_refs(db.reborrow(), &names, Some(block)).await?);
    }

    for input in inputs {
        let (status, record) = match input.lookup.as_ref() {
            None => (Status::InvalidName, None),
            Some(lookup) => match records.get(&lookup.logical_name_id) {
                Some(row) => {
                    let mut record = match profile {
                        LookupProfile::Feed => build_forward_feed_record(row),
                        LookupProfile::Detail => build_forward_detail_record(row),
                    }?;
                    if record.read_status != Status::Unsupported {
                        record.subregistry = subregistries.get(&lookup.logical_name_id).cloned();
                    }
                    (record.read_status, Some(record))
                }
                None => (Status::NotFound, None),
            },
        };
        results[input.index] = Some(LookupResult {
            input: input.input.clone(),
            kind: LookupKind::Name,
            status,
            unsupported_reason: result_unsupported_reason(status, record.iter()),
            failure_reason: result_failure_reason(status, record.iter()),
            normalization: input.normalization.clone(),
            record,
            records: None,
            page: None,
        });
    }
    Ok(())
}

async fn load_name_records(
    db: ReadDb<'_>,
    profile: LookupProfile,
    logical_name_ids: &[String],
    selected_snapshot: Option<&SelectedSnapshot>,
) -> V2Result<BTreeMap<String, bigname_storage::IdentityNameRecordRow>> {
    let records = if let Some(selected) = selected_snapshot {
        let chains: Vec<_> = selected
            .chain_positions
            .as_map()
            .values()
            .map(|position| position.chain_id.clone())
            .collect();
        bigname_storage::families::lookup::load_lookup_records(
            db,
            logical_name_ids,
            matches!(profile, LookupProfile::Detail),
            Some(&chains),
        )
        .await
    } else {
        match profile {
            LookupProfile::Feed => {
                bigname_storage::load_phase_identity_name_feed_records_by_ids(db, logical_name_ids)
                    .await
            }
            LookupProfile::Detail => {
                bigname_storage::load_phase_identity_records_by_ids(db, logical_name_ids).await
            }
        }
    }
    .map_err(crate::v2::snapshots::name_rows_error(
        crate::v2::SnapshotReadResource::Name,
        |load_error| {
            error!(
                service = "api",
                input_count = logical_name_ids.len(),
                profile = ?profile,
                error = ?load_error,
                "failed to load v2 lookup name records"
            );
            V2Error::internal_error("failed to load lookup name records")
        },
    ))?;
    if let Some(selected_snapshot) = selected_snapshot {
        require_name_records_at_served_head(&records, selected_snapshot)?;
    }
    Ok(records
        .into_iter()
        .map(|record| (record.row.logical_name_id.clone(), record))
        .collect())
}

/// Fills each loaded row whose stored registration does not carry its ENSv1 lease, as name detail
/// fills its row (`serve_legacy_lease_in_savepoint`), before either profile reads it. A row of no
/// shape issues no statement. With a served head the fill reads on the batch's snapshot at its
/// selection. Without one no publication bounds the read, so a row is read on its own connection
/// at the chain position it carries.
async fn serve_legacy_leases(
    db: &mut ReadDb<'_>,
    records: &mut BTreeMap<String, IdentityNameRecordRow>,
    selected_snapshot: Option<&SelectedSnapshot>,
) -> V2Result<()> {
    for record in records.values_mut() {
        let row = &mut record.row;
        if lease_row_shape(row).is_none() {
            continue;
        }
        let own;
        let snapshot = match selected_snapshot {
            Some(selected) => selected,
            None => {
                let Ok(chain_positions) = ChainPositions::from_value(&row.chain_positions) else {
                    continue;
                };
                own = SelectedSnapshot {
                    chain_positions,
                    consistency: SnapshotConsistency::Head,
                };
                &own
            }
        };
        let served = match db {
            ReadDb::Snapshot(conn) => {
                serve_legacy_lease_in_savepoint(&mut **conn, &mut *row, snapshot).await
            }
            ReadDb::Pool(pool) => match pool.acquire().await {
                Ok(mut conn) => {
                    serve_legacy_lease_in_savepoint(&mut *conn, &mut *row, snapshot).await
                }
                Err(error) => Err(error.into()),
            },
        };
        served.map_err(|error| {
            error!(
                service = "api",
                name = %row.normalized_name,
                error = %error,
                "legacy lease savepoint or rollback failed for a lookup name"
            );
            V2Error::internal_error("failed to read the ENSv1 lease of a lookup name")
        })?;
    }
    Ok(())
}
