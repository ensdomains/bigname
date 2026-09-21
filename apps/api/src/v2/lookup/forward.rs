use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::SelectedSnapshot;
use tracing::error;

use crate::AppState;
use crate::v2::{
    Status, V2Error, V2Result, load_subregistry_refs,
    name_records_inventory::{abi_input_for_identity_row, load_abi_content_types},
    registries::snapshot_block_for_chain,
};

use super::{
    admission::require_name_records_at_served_head,
    build::{build_forward_detail_record, build_forward_feed_record},
    dto::{LookupKind, LookupResult},
    parse::{LookupInclude, LookupProfile, ParsedNameLookup},
    result_failure_reason, result_unsupported_reason,
};

pub(super) async fn render_name_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    include: LookupInclude,
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
    let records = load_name_records(state, profile, &logical_name_ids, selected_snapshot).await?;
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
        subregistries.extend(load_subregistry_refs(&state.pool, &names, Some(block)).await?);
    }

    // Every served inventory container, by input, for one batched ABI read after rendering.
    let mut abi_rows = Vec::new();
    for input in inputs {
        let (status, record) = match input.lookup.as_ref() {
            None => (Status::InvalidName, None),
            Some(lookup) => match records.get(&lookup.logical_name_id) {
                Some(row) => {
                    let mut record = match profile {
                        LookupProfile::Feed => build_forward_feed_record(row),
                        LookupProfile::Detail => build_forward_detail_record(row, include),
                    }?;
                    if record.status != Status::Unsupported {
                        record.subregistry = subregistries.get(&lookup.logical_name_id).cloned();
                    }
                    if record.inventory.is_some()
                        && let Some(inventory) = row.record_inventory_current.as_ref()
                    {
                        abi_rows.push((input.index, inventory));
                    }
                    (record.status, Some(record))
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
    let inputs = abi_rows
        .iter()
        .map(|(_, inventory)| abi_input_for_identity_row(inventory))
        .collect::<Vec<_>>();
    let answers = load_abi_content_types(&state.pool, &inputs).await?;
    for ((index, _), answer) in abi_rows.into_iter().zip(answers) {
        if let Some(inventory) = results[index]
            .as_mut()
            .and_then(|result| result.record.as_mut())
            .and_then(|record| record.inventory.as_mut())
        {
            inventory.set_abi_content_types(answer);
        }
    }
    Ok(())
}

async fn load_name_records(
    state: &AppState,
    profile: LookupProfile,
    logical_name_ids: &[String],
    selected_snapshot: Option<&SelectedSnapshot>,
) -> V2Result<BTreeMap<String, bigname_storage::IdentityNameRecordRow>> {
    let records = match profile {
        LookupProfile::Feed => {
            bigname_storage::load_phase_identity_name_feed_records_by_ids(
                &state.pool,
                logical_name_ids,
            )
            .await
        }
        LookupProfile::Detail => {
            bigname_storage::load_phase_identity_records_by_ids(&state.pool, logical_name_ids).await
        }
    }
    .map_err(|load_error| {
        error!(
            service = "api",
            input_count = logical_name_ids.len(),
            profile = ?profile,
            error = ?load_error,
            "failed to load v2 lookup name records"
        );
        V2Error::internal_error("failed to load lookup name records")
    })?;
    if let Some(selected_snapshot) = selected_snapshot {
        require_name_records_at_served_head(&records, selected_snapshot)?;
    }
    Ok(records
        .into_iter()
        .map(|record| (record.row.logical_name_id.clone(), record))
        .collect())
}
