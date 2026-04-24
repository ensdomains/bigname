mod path;
mod resolution;

use bigname_storage::SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey;

pub(crate) use path::{
    classify_supported_resolution_path, ensure_contains_basenames_l1_resolver_call,
    ensure_contains_universal_resolver_call, ensure_single_ethereum_mainnet_position,
    ensure_steps_do_not_use_deferred_execution_paths,
    manifest_versions_include_source_family_for_context, normalize_address,
    persisted_trace_detail_object, required_chain_positions,
};
pub(crate) use resolution::{
    extract_requested_selectors, extract_supported_verified_queries,
    validate_basenames_transport_direct_request, validate_direct_request,
};

pub(crate) fn normalized_request_key(
    namespace: &str,
    surface: &str,
    ordered_record_keys: &[String],
) -> String {
    bigname_storage::normalized_resolution_request_key_from_record_keys(
        namespace,
        surface,
        ordered_record_keys,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerifiedQueryStatus {
    Success,
    NotFound,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedQuerySummary {
    pub(crate) record_key: String,
    pub(crate) selector: SupportedVerifiedRecordKey,
    pub(crate) status: VerifiedQueryStatus,
    pub(crate) value: Option<String>,
    pub(crate) failure_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestedSelectorSet {
    pub(crate) surface: String,
    pub(crate) ordered_record_keys: Vec<String>,
    pub(crate) binding_kind: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestedChainPosition {
    pub(crate) chain_id: String,
    pub(crate) block_number: i64,
    pub(crate) block_hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SupportedResolutionPathClass {
    Direct,
    AliasOnly,
    WildcardDerived,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SupportedResolutionStepSummary {
    pub(crate) saw_universal_resolver_call: bool,
    pub(crate) saw_alias_step: bool,
}
