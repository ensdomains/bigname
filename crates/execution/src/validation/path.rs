mod basenames;
mod common;
mod ens;
mod manifest;

use anyhow::Result;
use bigname_storage::ExecutionTrace;
use uuid::Uuid;

use super::{RequestedChainPosition, RequestedSelectorSet};

pub(crate) use common::{
    ensure_single_ethereum_mainnet_position, persisted_trace_detail_object,
    required_chain_positions,
};
pub(crate) use ens::{
    classify_supported_resolution_path, ensure_steps_do_not_use_deferred_execution_paths,
};
pub(crate) use manifest::{
    ensure_contains_basenames_l1_resolver_call, ensure_contains_universal_resolver_call,
    manifest_versions_include_source_family_for_context, normalize_address,
};

pub(super) fn ensure_steps_are_supported_exact_surface_path(
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    execution_trace_id: Uuid,
) -> Result<()> {
    ens::ensure_steps_are_supported_exact_surface_path(
        trace,
        requested_selectors,
        execution_trace_id,
    )
}

pub(super) fn ensure_steps_are_supported_basenames_transport_direct_path(
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    execution_trace_id: Uuid,
) -> Result<()> {
    basenames::ensure_steps_are_supported_basenames_transport_direct_path(
        trace,
        requested_selectors,
        execution_trace_id,
    )
}

pub(super) fn ensure_basenames_requested_positions(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    basenames::ensure_basenames_requested_positions(positions, context)
}
