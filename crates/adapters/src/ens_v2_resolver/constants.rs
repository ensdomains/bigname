pub(super) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(super) const DERIVATION_KIND_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
pub(super) const DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION: &str =
    "raw_log_preimage_observation";
pub(super) const RESOLVER_EDGE_KIND: &str = "resolver";

pub(super) const EVENT_KIND_PREIMAGE_OBSERVED: &str = "PreimageObserved";
pub(super) const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
pub(super) const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
pub(super) const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";

pub(super) const ABI_EVENT_ADDRESS_CHANGED: &str = "AddressChanged";
pub(super) const ABI_EVENT_TEXT_CHANGED: &str = "TextChanged";
pub(super) const ABI_EVENT_CONTENTHASH_CHANGED: &str = "ContenthashChanged";
pub(super) const ABI_EVENT_NAME_CHANGED: &str = "NameChanged";
pub(super) const ABI_EVENT_VERSION_CHANGED: &str = "VersionChanged";
pub(super) const ABI_EVENT_ALIAS_CHANGED: &str = "AliasChanged";
pub(super) const ABI_EVENT_NAMED_RESOURCE: &str = "NamedResource";
pub(super) const ABI_EVENT_NAMED_TEXT_RESOURCE: &str = "NamedTextResource";
pub(super) const ABI_EVENT_NAMED_ADDR_RESOURCE: &str = "NamedAddrResource";

pub(super) const ABI_EVENT_NAMES: [&str; 9] = [
    ABI_EVENT_ADDRESS_CHANGED,
    ABI_EVENT_TEXT_CHANGED,
    ABI_EVENT_CONTENTHASH_CHANGED,
    ABI_EVENT_NAME_CHANGED,
    ABI_EVENT_VERSION_CHANGED,
    ABI_EVENT_ALIAS_CHANGED,
    ABI_EVENT_NAMED_RESOURCE,
    ABI_EVENT_NAMED_TEXT_RESOURCE,
    ABI_EVENT_NAMED_ADDR_RESOURCE,
];
