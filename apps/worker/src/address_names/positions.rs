use std::collections::BTreeMap;

use bigname_storage::CanonicalityState;
use serde_json::{Value, json};

use super::{
    model::{ChainPositionCandidate, CurrentBindingSeed, RelevantEvent},
    util::{canonicality_rank, chain_slot, format_timestamp, weakest_canonicality},
};

pub(super) fn build_chain_positions(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if let Some(timestamp) = binding.surface_block_timestamp {
        merge_chain_position(
            &mut chain_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.surface_chain_id),
                chain_id: binding.surface_chain_id.clone(),
                block_number: binding.surface_block_number,
                block_hash: binding.surface_block_hash.clone(),
                timestamp,
            },
        );
    }
    if let Some(timestamp) = binding.binding_block_timestamp {
        merge_chain_position(
            &mut chain_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.binding_chain_id),
                chain_id: binding.binding_chain_id.clone(),
                block_number: binding.binding_block_number,
                block_hash: binding.binding_block_hash.clone(),
                timestamp,
            },
        );
    }
    for event in events {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };

        merge_chain_position(
            &mut chain_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(slot, candidate)| {
                (
                    slot,
                    json!({
                        "chain_id": candidate.chain_id,
                        "block_number": candidate.block_number,
                        "block_hash": candidate.block_hash,
                        "timestamp": format_timestamp(candidate.timestamp),
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

pub(super) fn build_canonicality_summary(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
) -> Value {
    let status = weakest_canonicality(
        std::iter::once(binding.surface_state)
            .chain(std::iter::once(binding.binding_state))
            .chain(std::iter::once(binding.resource_state))
            .chain(binding.token_lineage_state)
            .chain(events.iter().map(|event| event.canonicality_state)),
    )
    .unwrap_or(CanonicalityState::Canonical);

    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    merge_chain_state(
        &mut chain_states,
        &binding.surface_chain_id,
        binding.surface_state,
    );
    merge_chain_state(
        &mut chain_states,
        &binding.binding_chain_id,
        binding.binding_state,
    );
    for event in events {
        if let Some(chain_id) = event.chain_id.as_deref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

pub(super) fn max_timestamp(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
) -> Option<sqlx::types::time::OffsetDateTime> {
    let mut timestamps = Vec::new();
    if let Some(timestamp) = binding.surface_block_timestamp {
        timestamps.push(timestamp);
    }
    if let Some(timestamp) = binding.binding_block_timestamp {
        timestamps.push(timestamp);
    }
    timestamps.extend(events.iter().filter_map(|event| event.block_timestamp));
    timestamps.into_iter().max()
}

fn merge_chain_position(
    chain_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    match chain_positions.get(&candidate.slot) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(candidate.slot.clone(), candidate);
        }
    }
}

fn merge_chain_state(
    chain_states: &mut BTreeMap<String, CanonicalityState>,
    chain_id: &str,
    state: CanonicalityState,
) {
    let replacement = chain_states
        .get(chain_id)
        .map(|current| canonicality_rank(state) < canonicality_rank(*current))
        .unwrap_or(true);
    if replacement {
        chain_states.insert(chain_id.to_owned(), state);
    }
}
