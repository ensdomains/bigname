use crate::ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1;

use super::{AdapterReplayContract, NormalizedEventReplayAdapter, ReplayDependencyModel};

const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(crate) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
const SOURCE_FAMILY_ENS_V1_REVERSE_L1: &str = "ens_v1_reverse_l1";
const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
const SOURCE_FAMILY_BASENAMES_BASE_PRIMARY: &str = "basenames_base_primary";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";

#[rustfmt::skip]
const BLOCK_DERIVED_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, SOURCE_FAMILY_ENS_V1_WRAPPER_L1, SOURCE_FAMILY_ENS_V2_ROOT_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1];
#[rustfmt::skip]
const ENS_V1_REVERSE_CLAIM_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V1_REVERSE_L1, SOURCE_FAMILY_BASENAMES_BASE_PRIMARY];
#[rustfmt::skip]
const ENS_V1_SUBREGISTRY_DISCOVERY_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V1_REGISTRY_L1, SOURCE_FAMILY_BASENAMES_BASE_REGISTRY];
#[rustfmt::skip]
const ENS_V1_UNWRAPPED_AUTHORITY_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, SOURCE_FAMILY_ENS_V1_REGISTRY_L1, SOURCE_FAMILY_ENS_V1_RESOLVER_L1, SOURCE_FAMILY_ENS_V1_WRAPPER_L1, SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR, SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_BASENAMES_BASE_RESOLVER];
#[rustfmt::skip]
const ENS_V2_REGISTRY_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_ROOT_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1];
const ENS_V2_REGISTRAR_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_REGISTRAR_L1];
const ENS_V2_RESOLVER_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_RESOLVER_L1];
#[rustfmt::skip]
const ENS_V2_PERMISSIONS_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_ROOT_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1];
#[rustfmt::skip]
const ENS_V2_REGISTRAR_DEPENDENCY_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_ROOT_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_REGISTRAR_L1];
#[rustfmt::skip]
const ENS_V2_RESOLVER_DEPENDENCY_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_ROOT_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1];

const NO_DEPENDENCIES: &[NormalizedEventReplayAdapter] = &[];
const ENS_V2_REGISTRY_DEPENDENCY: &[NormalizedEventReplayAdapter] =
    &[NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface];

pub(crate) const NORMALIZED_EVENT_REPLAY_CONTRACTS: &[AdapterReplayContract] = &[
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::BlockDerivedNormalizedEvents,
        model: ReplayDependencyModel::StatelessRawFact,
        raw_fact_replay_participant: true,
        source_families: BLOCK_DERIVED_SOURCE_FAMILIES,
        closure_source_families: BLOCK_DERIVED_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/block_derived_normalized_events"],
        stateless_replay_proof_tests: &[
            "replay_normalized_events_is_idempotent_without_checkpoint_mutation",
            "sync_block_derived_normalized_events_is_idempotent",
            "sync_block_derived_normalized_events_replays_scoped_selected_logs_without_payload_rows",
        ],
        closure_replay_supported: true,
        replay_note: "preimage rows are decoded from each selected raw log and manifest event-topic constants",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV1ReverseClaim,
        model: ReplayDependencyModel::StatelessRawFact,
        raw_fact_replay_participant: true,
        source_families: ENS_V1_REVERSE_CLAIM_SOURCE_FAMILIES,
        closure_source_families: ENS_V1_REVERSE_CLAIM_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v1_reverse_claim"],
        stateless_replay_proof_tests: &[
            "replay_normalized_events_runs_full_persisted_raw_adapter_boundary",
            "replay_normalized_events_scoped_block_range_selects_only_requested_targets",
            "replay_normalized_events_skips_noncanonical_raw_logs_in_selected_block_hashes",
        ],
        closure_replay_supported: true,
        replay_note: "reverse rows are decoded from the selected reverse-claim raw log",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V1_SUBREGISTRY_DISCOVERY_SOURCE_FAMILIES,
        closure_source_families: ENS_V1_SUBREGISTRY_DISCOVERY_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v1_subregistry_discovery"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "normalized-event attribution reads manifest contract instances, discovery-derived contract addresses and edges, prior migration state, and the reconciled observation edge",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority,
        model: ReplayDependencyModel::StatefulClosureRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V1_UNWRAPPED_AUTHORITY_SOURCE_FAMILIES,
        closure_source_families: ENS_V1_UNWRAPPED_AUTHORITY_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v1_unwrapped_authority"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "authority state, before_state, resource identity, and permission provenance depend on ordered in-memory history",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
        model: ReplayDependencyModel::StatefulClosureRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_REGISTRY_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_REGISTRY_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v2_registry"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "registry/resource rows depend on ordered token, suffix, binding, and discovery state",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2Registrar,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_REGISTRAR_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_REGISTRAR_DEPENDENCY_SOURCE_FAMILIES,
        dependency_adapters: ENS_V2_REGISTRY_DEPENDENCY,
        producer_paths: &["crates/adapters/src/ens_v2_registrar"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "registrar rows resolve logical_name_id/resource_id through stable ENSv2 registry normalized output",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2Resolver,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_RESOLVER_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_RESOLVER_DEPENDENCY_SOURCE_FAMILIES,
        dependency_adapters: ENS_V2_REGISTRY_DEPENDENCY,
        producer_paths: &["crates/adapters/src/ens_v2_resolver"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "resolver rows resolve name/resource links from stable name_surfaces and surface_bindings",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2Permissions,
        model: ReplayDependencyModel::StatefulClosureRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_PERMISSIONS_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_PERMISSIONS_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v2_permissions"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "permission resources and role events are stateful within root, registry, and resolver emitter histories",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::ManifestNormalizedEvents,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: false,
        source_families: &[],
        closure_source_families: &[],
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/manifest_normalized_events"],
        stateless_replay_proof_tests: &[],
        closure_replay_supported: false,
        replay_note: "manifest rows are derived from manifest, capability, code-hash, and discovery-edge corpus state, not raw-log replay",
    },
];
