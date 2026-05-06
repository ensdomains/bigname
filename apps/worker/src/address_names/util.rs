use anyhow::Result;
use bigname_storage::{CanonicalityState, SurfaceBindingKind, normalize_evm_address};

pub(super) use crate::projection_json::{dedupe_json_values, format_timestamp, json_str};

pub(super) fn normalize_address(value: impl AsRef<str>) -> String {
    normalize_evm_address(value.as_ref())
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    CanonicalityState::parse(value)
}

pub(super) fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    SurfaceBindingKind::parse(value)
}

pub(super) fn canonicality_rank(state: CanonicalityState) -> u8 {
    state.rank()
}

pub(super) fn weakest_canonicality(
    states: impl Iterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    CanonicalityState::weakest(states)
}

pub(super) fn chain_slot(chain_id: &str) -> String {
    if chain_id.starts_with("ethereum") {
        "ethereum".to_owned()
    } else if chain_id.starts_with("base") {
        "base".to_owned()
    } else {
        chain_id.to_owned()
    }
}
