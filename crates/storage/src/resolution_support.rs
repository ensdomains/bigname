mod boundaries;
mod record_keys;
mod support_classes;
mod topology;

pub use boundaries::{
    projected_resolution_boundaries_from_topology, record_version_boundary_has_pointer,
    resolution_record_inventory_lookup_key, resolution_record_inventory_lookup_key_any_chain,
    resolution_record_inventory_lookup_key_for_revalidation, resolution_record_version_boundary,
    resolution_record_version_boundary_for_revalidation, resolution_supports_avatar_readback,
    resolution_verified_support_boundary, try_resolution_verified_support_boundary,
};
pub use record_keys::{
    SupportedVerifiedResolutionRecordKey, canonical_addr_coin_type, is_resolution_avatar_record,
    parse_supported_verified_resolution_record_key, supported_resolution_verified_lookup_records,
    supported_resolution_verified_readback_records, supports_resolution_verified_lookup_record,
};
pub use support_classes::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_NAMESPACE, ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID,
    VerifiedResolutionPathClass, VerifiedResolutionRecord,
    VerifiedResolutionRequestedChainPosition, VerifiedResolutionSupportBoundary,
};
pub use topology::{
    classify_supported_resolution_topology, projected_resolution_topology,
    row_has_basenames_supported_chain_positions, try_classify_supported_resolution_topology,
};
