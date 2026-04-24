use std::collections::BTreeSet;

use bigname_manifests::WatchedSourceSelectorPlan;

use crate::provider::ProviderResolvedBlock;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BackfillLogRangeRequest {
    pub(super) start_index: usize,
    pub(super) end_index: usize,
    pub(super) addresses: Vec<String>,
}

pub(super) fn selected_log_range_requests(
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
) -> Vec<BackfillLogRangeRequest> {
    let mut requests = Vec::new();
    let mut active_start = None;
    let mut active_addresses = BTreeSet::<String>::new();

    for (index, block) in resolved_blocks.iter().enumerate() {
        let addresses = selected_target_addresses_at_block(source_plan, block.block_number);
        if addresses.is_empty() {
            if let Some(start_index) = active_start.take() {
                requests.push(BackfillLogRangeRequest {
                    start_index,
                    end_index: index,
                    addresses: active_addresses.iter().cloned().collect(),
                });
                active_addresses.clear();
            }
            continue;
        }

        match active_start {
            Some(start_index) if active_addresses == addresses => {
                active_start = Some(start_index);
            }
            Some(start_index) => {
                requests.push(BackfillLogRangeRequest {
                    start_index,
                    end_index: index,
                    addresses: active_addresses.iter().cloned().collect(),
                });
                active_start = Some(index);
                active_addresses = addresses;
            }
            None => {
                active_start = Some(index);
                active_addresses = addresses;
            }
        }
    }

    if let Some(start_index) = active_start {
        requests.push(BackfillLogRangeRequest {
            start_index,
            end_index: resolved_blocks.len(),
            addresses: active_addresses.into_iter().collect(),
        });
    }

    requests
}

pub(super) fn selected_target_addresses_at_block(
    source_plan: &WatchedSourceSelectorPlan,
    block_number: i64,
) -> BTreeSet<String> {
    source_plan
        .selected_targets
        .iter()
        .filter(|target| {
            target.effective_from_block <= block_number && block_number <= target.effective_to_block
        })
        .map(|target| target.address.to_ascii_lowercase())
        .collect()
}

pub(super) fn selected_target_sync_scope(
    source_plan: &WatchedSourceSelectorPlan,
) -> Vec<(String, String, i64, i64)> {
    source_plan
        .selected_targets
        .iter()
        .map(|target| {
            (
                target.source_family.clone(),
                target.address.to_ascii_lowercase(),
                target.effective_from_block,
                target.effective_to_block,
            )
        })
        .collect()
}
