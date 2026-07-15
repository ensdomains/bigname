/// Active source families that force automatic normalized replay to preserve its first
/// admitted target. These adapters need closure or contextual dependency replay, so advancing
/// the historical cursor to each new raw-log head would rerun the full closure indefinitely.
///
/// The indexer uses this list to choose its target-refresh policy. Read-only operational tools
/// use the same list to interpret a completed cursor whose target is intentionally below head.
pub const CLOSURE_OR_DEPENDENCY_REPLAY_SOURCE_FAMILIES: &[&str] = &[
    "basenames_base_registrar",
    "basenames_base_registry",
    "basenames_base_resolver",
    "ens_v1_registrar_l1",
    "ens_v1_registry_l1",
    "ens_v1_resolver_l1",
    "ens_v1_wrapper_l1",
    "ens_v2_registrar_l1",
    "ens_v2_registry_l1",
    "ens_v2_resolver_l1",
    "ens_v2_root_l1",
];

pub const fn source_family_preserves_normalized_replay_target(source_family: &str) -> bool {
    let mut index = 0;
    while index < CLOSURE_OR_DEPENDENCY_REPLAY_SOURCE_FAMILIES.len() {
        if const_str_eq(
            source_family,
            CLOSURE_OR_DEPENDENCY_REPLAY_SOURCE_FAMILIES[index],
        ) {
            return true;
        }
        index += 1;
    }
    false
}

const fn const_str_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}
