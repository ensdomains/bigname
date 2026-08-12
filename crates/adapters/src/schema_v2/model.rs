use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ManifestInput {
    pub manifest_id: i64,
    pub manifest_version: i64,
    pub namespace: String,
    pub source_family: String,
    pub chain_id: String,
    pub deployment_label: String,
    pub normalizer_version: String,
    pub payload_json: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryRuleInput {
    pub manifest_id: i64,
    pub edge_kind: String,
    pub from_role: Option<String>,
    pub admission: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressAdmissionInput {
    pub address: String,
    pub contract_instance_id: Uuid,
    pub source_manifest_id: Option<i64>,
    pub role: Option<String>,
    pub discovery_edge_kind: Option<String>,
    pub discovery_from_contract_instance_id: Option<Uuid>,
    pub discovery_observation_key: Option<String>,
    pub active_from_block: Option<i64>,
    pub active_to_block: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct RawLogInput {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub block_timestamp: OffsetDateTime,
    pub canonicality_state: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub emitting_address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct RawBlockInput {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub block_timestamp: OffsetDateTime,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriorEventInput {
    pub retained_state_key: String,
    pub chain_id: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub state_scope: Option<String>,
    pub block_timestamp: Option<OffsetDateTime>,
    pub after_state: Value,
}

#[derive(Clone, Debug)]
pub struct BatchInput {
    pub chain_id: String,
    pub manifests: Vec<ManifestInput>,
    pub discovery_rules: Vec<DiscoveryRuleInput>,
    pub admissions: Vec<AddressAdmissionInput>,
    pub prior_events: Vec<PriorEventInput>,
    pub blocks: Vec<RawBlockInput>,
    pub raw_logs: Vec<RawLogInput>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchOutput {
    pub normalized_events: Vec<NormalizedEvent>,
    pub label_preimages: Vec<LabelPreimage>,
    pub name_surfaces: Vec<NameSurface>,
    pub token_lineages: Vec<TokenLineage>,
    pub resources: Vec<Resource>,
    pub surface_bindings: Vec<SurfaceBinding>,
    pub binding_closures: Vec<BindingClosure>,
    pub contract_instances: Vec<ContractInstance>,
    pub contract_addresses: Vec<ContractAddress>,
    pub discovery_edges: Vec<DiscoveryEdge>,
    pub discovery_edge_closures: Vec<DiscoveryEdgeClosure>,
    pub migration_event_associations: Vec<MigrationEventAssociation>,
    pub migration_discovery_associations: Vec<MigrationDiscoveryAssociation>,
    pub migration_candidate_identity_effects: Vec<MigrationCandidateEffect>,
    pub migration_candidate_discovery_effects: Vec<MigrationCandidateEffect>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedEvent {
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: String,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: String,
    pub before_state: Value,
    pub after_state: Value,
    pub migration_correlation_ids: Vec<String>,
    pub consumer_visibility: String,
    /// True when the interpreter deliberately computed `before_state` as a snapshot (rather than
    /// chaining it from the previous emitted event under the same interpreter state key). Not
    /// persisted; it steers the post-reconciliation before-state re-thread only.
    pub before_state_explicit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationEventAssociation {
    pub event_identity: String,
    pub migration_correlation_id: String,
    pub correlation_kind: String,
    pub evidence_refs: Value,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub canonicality_state: String,
    pub consumer_visibility: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationDiscoveryAssociation {
    pub logical_edge_identity: String,
    pub migration_correlation_id: String,
    pub registry_contract_instance_id: Uuid,
    pub registry_address: String,
    pub source_manifest_id: i64,
    pub evidence_refs: Value,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub canonicality_state: String,
    pub consumer_visibility: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationCandidateEffect {
    pub effect_identity: String,
    pub migration_correlation_ids: Vec<String>,
    pub correlation_kind: String,
    pub effect_kind: String,
    pub proposed_effect: Value,
    pub evidence_refs: Value,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub canonicality_state: String,
    pub consumer_visibility: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelPreimage {
    pub labelhash: String,
    pub raw_label: Vec<u8>,
    pub decoded_label: Option<String>,
    pub normalizer_version: String,
    pub normalized_under_version: bool,
    pub normalization_error: Option<String>,
    pub source_kind: String,
    pub source_priority: i32,
    pub provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameSurface {
    pub logical_name_id: String,
    pub namespace: String,
    pub raw_name: String,
    pub raw_labels: Vec<String>,
    pub dns_encoded_name: Vec<u8>,
    pub namehash: String,
    pub labelhashes: Vec<String>,
    pub normalizer_version: String,
    pub visibility_state: String,
    pub normalization_errors: Value,
    pub deactivation_reason: Option<String>,
    pub deactivated_at: Option<OffsetDateTime>,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenLineage {
    pub token_lineage_id: Uuid,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resource {
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SurfaceBinding {
    pub surface_binding_id: Uuid,
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub binding_kind: String,
    pub active_from: OffsetDateTime,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingClosure {
    pub logical_name_id: String,
    pub except_surface_binding_id: Option<Uuid>,
    pub active_to: OffsetDateTime,
    pub block_number: i64,
    pub transaction_index: i64,
    pub log_index: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractInstance {
    pub contract_instance_id: Uuid,
    pub chain_id: String,
    pub contract_kind: String,
    pub provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractAddress {
    pub contract_instance_id: Uuid,
    pub chain_id: String,
    pub address: String,
    pub active_from_block_number: i64,
    pub active_from_block_hash: String,
    pub source_manifest_id: i64,
    pub provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryEdge {
    pub chain_id: String,
    pub edge_kind: String,
    pub from_contract_instance_id: Uuid,
    pub to_contract_instance_id: Uuid,
    pub discovery_source: String,
    pub admission_basis: String,
    pub source_manifest_id: i64,
    pub observation_key: String,
    pub active_from_block_number: i64,
    pub active_from_block_hash: String,
    pub canonicality_state: String,
    pub provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryEdgeClosure {
    pub chain_id: String,
    pub edge_kind: String,
    pub from_contract_instance_id: Uuid,
    pub observation_key: String,
    pub except_to_contract_instance_id: Option<Uuid>,
    pub active_to_block_number: i64,
    pub active_to_block_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
}
