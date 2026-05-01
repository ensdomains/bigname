use super::*;

pub(super) struct BoundaryEventPayload<'a> {
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
    pub(super) event_kind: &'a str,
    pub(super) before_state: serde_json::Value,
    pub(super) after_state: serde_json::Value,
    pub(super) identity_suffix: String,
}

pub(super) struct BoundaryEventSource {
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) canonicality_state: CanonicalityState,
}

pub(super) fn build_normalized_event(
    reference: &ObservationRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: serde_json::Value,
    after_state: serde_json::Value,
    identity_suffix: String,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, event_kind, identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: reference.source_family.clone(),
        manifest_version: reference.manifest_version,
        source_manifest_id: source_manifest_id_if_known(reference.source_manifest_id),
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: reference.transaction_hash.clone(),
        log_index: reference.log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "transaction_hash": reference.transaction_hash,
            "transaction_index": reference.transaction_index,
            "log_index": reference.log_index,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state: reference.canonicality_state,
        before_state,
        after_state,
    }
}

pub(super) fn build_boundary_event(
    reference: &BoundaryRef,
    payload: BoundaryEventPayload<'_>,
    source: BoundaryEventSource,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, payload.event_kind, payload.identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id: payload.logical_name_id,
        resource_id: payload.resource_id,
        event_kind: payload.event_kind.to_owned(),
        source_family: source.source_family,
        manifest_version: source.manifest_version,
        source_manifest_id: source.source_manifest_id,
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "kind": "raw_block",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "block_timestamp": reference.block_timestamp.unix_timestamp(),
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state: source.canonicality_state,
        before_state: payload.before_state,
        after_state: payload.after_state,
    }
}
