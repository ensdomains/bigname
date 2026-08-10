/// One documented
/// [emitter-role-independent event](../../../../docs/glossary.md#emitter-role-independent-event).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoleInsensitiveEvent {
    pub source_family: &'static str,
    pub event: &'static str,
    pub justification: &'static str,
    pub adapter_file: &'static str,
}

const V1_RESOLVER_ADAPTER: &str = "crates/adapters/src/schema_v2/protocol/v1/resolver.rs";
const V2_RESOLVER_ADAPTER: &str = "crates/adapters/src/schema_v2/protocol/v2_resolver.rs";
const V1_RESOLVER_JUSTIFICATION: &str =
    "the shared ENSv1/Basenames resolver adapter does not read Selected.emitter_role";
const V2_RESOLVER_JUSTIFICATION: &str =
    "the ENSv2 resolver adapter does not read Selected.emitter_role";

/// The finite set of emitter-role-independent manifest events.
pub const ROLE_INSENSITIVE_EVENTS: &[RoleInsensitiveEvent] = &[
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "ABIChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "AddrChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "AddressChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "ContentChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "ContenthashChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "DNSRecordChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "DNSRecordDeleted",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "DNSZonehashChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "DataChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "InterfaceChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "NameChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "TextChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v1_resolver_l1",
        event: "VersionChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "basenames_base_resolver",
        event: "AddrChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "basenames_base_resolver",
        event: "AddressChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "basenames_base_resolver",
        event: "NameChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "basenames_base_resolver",
        event: "TextChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v1/resolver.rs.
    RoleInsensitiveEvent {
        source_family: "basenames_base_resolver",
        event: "VersionChanged",
        justification: V1_RESOLVER_JUSTIFICATION,
        adapter_file: V1_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "AddressChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "AliasChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "ContenthashChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "EACRolesChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "NameChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "NamedAddrResource",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "NamedResource",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "NamedTextResource",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "TextChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "Upgraded",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
    // Adapter: crates/adapters/src/schema_v2/protocol/v2_resolver.rs.
    RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "VersionChanged",
        justification: V2_RESOLVER_JUSTIFICATION,
        adapter_file: V2_RESOLVER_ADAPTER,
    },
];

pub fn role_insensitivity_justification(source_family: &str, event: &str) -> Option<&'static str> {
    ROLE_INSENSITIVE_EVENTS
        .iter()
        .find(|entry| entry.source_family == source_family && entry.event == event)
        .map(|entry| entry.justification)
}

pub fn event_allows_empty_emitter_roles(
    source_family: &str,
    event: &str,
    has_registry_announcement_rule: bool,
) -> bool {
    role_insensitivity_justification(source_family, event).is_some()
        || (source_family == "ens_v2_registry_l1"
            && event == "RegistryCreated"
            && has_registry_announcement_rule)
}
