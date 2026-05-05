pub(super) const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(super) const DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE: &str =
    "ens_v2_registry_resource_surface";
pub(super) const RESOLVER_EDGE_KIND: &str = "resolver";
pub(super) const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
pub(super) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
pub(super) const EVENT_KIND_REGISTRATION_RESERVED: &str = "RegistrationReserved";
pub(super) const EVENT_KIND_REGISTRATION_RELEASED: &str = "RegistrationReleased";
pub(super) const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";
pub(super) const EVENT_KIND_EXPIRY_CHANGED: &str = "ExpiryChanged";
pub(super) const EVENT_KIND_AUTHORITY_TRANSFERRED: &str = "AuthorityTransferred";
pub(super) const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
pub(super) const EVENT_KIND_SUBREGISTRY_CHANGED: &str = "SubregistryChanged";
pub(super) const EVENT_KIND_PARENT_CHANGED: &str = "ParentChanged";
pub(super) const EVENT_KIND_TOKEN_RESOURCE_LINKED: &str = "TokenResourceLinked";
pub(super) const EVENT_KIND_TOKEN_REGENERATED: &str = "TokenRegenerated";
pub(super) const EVENT_KIND_SURFACE_BOUND: &str = "SurfaceBound";

pub(super) const ABI_EVENT_LABEL_REGISTERED: &str = "LabelRegistered";
pub(super) const ABI_EVENT_LABEL_RESERVED: &str = "LabelReserved";
pub(super) const ABI_EVENT_LABEL_UNREGISTERED: &str = "LabelUnregistered";
pub(super) const ABI_EVENT_EXPIRY_UPDATED: &str = "ExpiryUpdated";
pub(super) const ABI_EVENT_SUBREGISTRY_UPDATED: &str = "SubregistryUpdated";
pub(super) const ABI_EVENT_RESOLVER_UPDATED: &str = "ResolverUpdated";
pub(super) const ABI_EVENT_TOKEN_REGENERATED: &str = "TokenRegenerated";
pub(super) const ABI_EVENT_PARENT_UPDATED: &str = "ParentUpdated";
pub(super) const ABI_EVENT_TOKEN_RESOURCE: &str = "TokenResource";

pub(super) const ABI_EVENT_NAMES: [&str; 9] = [
    ABI_EVENT_LABEL_REGISTERED,
    ABI_EVENT_LABEL_RESERVED,
    ABI_EVENT_LABEL_UNREGISTERED,
    ABI_EVENT_EXPIRY_UPDATED,
    ABI_EVENT_SUBREGISTRY_UPDATED,
    ABI_EVENT_RESOLVER_UPDATED,
    ABI_EVENT_TOKEN_RESOURCE,
    ABI_EVENT_TOKEN_REGENERATED,
    ABI_EVENT_PARENT_UPDATED,
];
