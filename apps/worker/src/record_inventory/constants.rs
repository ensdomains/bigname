pub(super) const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
pub(super) const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
pub(super) const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
pub(super) const DERIVATION_KIND_DECLARED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
pub(super) const DERIVATION_KIND_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
pub(super) const ENS_NAMESPACE: &str = "ens";
pub(super) const BASENAMES_NAMESPACE: &str = "basenames";
pub(super) const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
pub(super) const SOURCE_FAMILY_BASENAMES_EXECUTION: &str = "basenames_execution";
pub(super) const VERIFIED_RESOLUTION_CAPABILITY: &str = "verified_resolution";
pub(super) const BASENAMES_V1_DEPLOYMENT_EPOCH: &str = "basenames_v1";
pub(super) const BASENAMES_L1_RESOLVER_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";
pub(super) const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
pub(super) const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
pub(super) const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
pub(super) const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
pub(super) const RECORD_INVENTORY_CURRENT_DERIVATION_KIND: &str =
    "record_inventory_current_rebuild";
pub(super) const RECORD_INVENTORY_ENUMERATION_BASIS: &str = "declared_record_inventory";
pub(super) const GAP_REASON_NOT_OBSERVED: &str = "not_observed_on_current_resolver";
pub(super) const CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED: &str =
    "value_not_retained_in_normalized_events";
pub(super) const UNSUPPORTED_FAMILY_REASON: &str =
    "record_family_not_supported_in_phase6_projection";
pub(super) const RESOLVER_FAMILY_PENDING_REASON: &str = "resolver_family_pending";
pub(super) const RESOLVER_FAMILY_UNSUPPORTED_REASON: &str = "resolver_family_unsupported";
pub(super) const SUPPORTED_TEXT_RECORD_KEY: &str = "text";
pub(super) const SUPPORTED_TEXT_RECORD_FAMILY: &str = "text";
pub(super) const SUPPORTED_ADDR_RECORD_FAMILY: &str = "addr";
pub(super) const DATA_RESOLVER_RECORD_FAMILY: &str = "data";
pub(super) const PUBKEY_RECORD_FAMILY: &str = "pubkey";
pub(super) const UNSUPPORTED_CONTENTHASH_RECORD_KEY: &str = "contenthash";
pub(super) const UNSUPPORTED_CONTENTHASH_RECORD_FAMILY: &str = "contenthash";
pub(super) const SUPPORTED_NATIVE_ADDR_SELECTOR_KEY: &str = "60";
pub(super) const RESOLVER_PROFILE_FACT_FAMILY_RECORD: &str = "resolver_record";
pub(super) const RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION: &str = "resolver_record_version";
pub(super) const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
pub(super) const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
pub(super) const RESOLVER_PROFILE_STATUS_UNSUPPORTED: &str = "unsupported";
pub(super) const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;
