pub(super) const DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION: &str =
    "raw_log_preimage_observation";
pub(super) const EVENT_KIND_PREIMAGE_OBSERVED: &str = "PreimageObserved";
pub(super) const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(super) const SOURCE_EVENT_LABEL_REGISTERED: &str = "LabelRegistered";
pub(super) const SOURCE_EVENT_LABEL_RESERVED: &str = "LabelReserved";
pub(super) const SOURCE_EVENT_PARENT_UPDATED: &str = "ParentUpdated";
pub(super) const SOURCE_EVENT_NAME_REGISTERED: &str = "NameRegistered";
pub(super) const SOURCE_EVENT_NAME_RENEWED: &str = "NameRenewed";
pub(super) const SOURCE_EVENT_NAME_WRAPPED: &str = "NameWrapped";
pub(super) const SOURCE_EVENT_ALIAS_CHANGED: &str = "AliasChanged";
pub(super) const SOURCE_EVENT_NAMED_RESOURCE: &str = "NamedResource";
pub(super) const SOURCE_EVENT_NAMED_TEXT_RESOURCE: &str = "NamedTextResource";
pub(super) const SOURCE_EVENT_NAMED_ADDR_RESOURCE: &str = "NamedAddrResource";
pub(super) const ABI_EVENT_NAME_WRAPPED: &str = "NameWrapped";
pub(super) const ABI_EVENT_LABEL_REGISTERED: &str = "LabelRegistered";
pub(super) const ABI_EVENT_LABEL_RESERVED: &str = "LabelReserved";
pub(super) const ABI_EVENT_PARENT_UPDATED: &str = "ParentUpdated";
pub(super) const ABI_EVENT_NAME_REGISTERED: &str = "NameRegistered";
pub(super) const ABI_EVENT_NAME_RENEWED: &str = "NameRenewed";
pub(super) const ABI_EVENT_ALIAS_CHANGED: &str = "AliasChanged";
pub(super) const ABI_EVENT_NAMED_RESOURCE: &str = "NamedResource";
pub(super) const ABI_EVENT_NAMED_TEXT_RESOURCE: &str = "NamedTextResource";
pub(super) const ABI_EVENT_NAMED_ADDR_RESOURCE: &str = "NamedAddrResource";

pub(super) const ENS_V1_WRAPPER_PREIMAGE_EVENT_NAMES: [&str; 1] = [ABI_EVENT_NAME_WRAPPED];
pub(super) const ENS_V1_REGISTRAR_PREIMAGE_EVENT_NAMES: [&str; 2] =
    [ABI_EVENT_NAME_REGISTERED, ABI_EVENT_NAME_RENEWED];
pub(super) const ENS_V2_REGISTRY_PREIMAGE_EVENT_NAMES: [&str; 3] = [
    ABI_EVENT_LABEL_REGISTERED,
    ABI_EVENT_LABEL_RESERVED,
    ABI_EVENT_PARENT_UPDATED,
];
pub(super) const ENS_V2_REGISTRAR_PREIMAGE_EVENT_NAMES: [&str; 2] =
    [ABI_EVENT_NAME_REGISTERED, ABI_EVENT_NAME_RENEWED];
pub(super) const ENS_V2_RESOLVER_PREIMAGE_EVENT_NAMES: [&str; 4] = [
    ABI_EVENT_ALIAS_CHANGED,
    ABI_EVENT_NAMED_RESOURCE,
    ABI_EVENT_NAMED_TEXT_RESOURCE,
    ABI_EVENT_NAMED_ADDR_RESOURCE,
];
pub(super) const PREIMAGE_EVENT_NAMES: [&str; 12] = [
    ABI_EVENT_NAME_WRAPPED,
    ABI_EVENT_NAME_REGISTERED,
    ABI_EVENT_NAME_RENEWED,
    ABI_EVENT_LABEL_REGISTERED,
    ABI_EVENT_LABEL_RESERVED,
    ABI_EVENT_PARENT_UPDATED,
    ABI_EVENT_NAME_REGISTERED,
    ABI_EVENT_NAME_RENEWED,
    ABI_EVENT_ALIAS_CHANGED,
    ABI_EVENT_NAMED_RESOURCE,
    ABI_EVENT_NAMED_TEXT_RESOURCE,
    ABI_EVENT_NAMED_ADDR_RESOURCE,
];
