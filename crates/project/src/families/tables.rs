//! The family tables and their primary keys. Journalled tables carry owned facts and get a
//! before-image when a block changes them; derived tables are index rows re-derived from their
//! base rows after every block and every undo, and are never journalled.
pub(crate) struct TableSpec {
    pub(crate) name: &'static str,
    pub(crate) key: &'static [&'static str],
}

macro_rules! tables {
    ($($constant:ident = $name:literal [$($column:literal),+];)+) => {
        $(pub(crate) static $constant: TableSpec = TableSpec { name: $name, key: &[$($column),+] };)+
        /// Every journalled family table, in the order a reset clears them.
        pub(crate) static JOURNALLED: &[&TableSpec] = &[$(&$constant),+];
    };
}

tables! {
    NAME_STATE = "project_name_state" ["namespace", "logical_name_id"];
    BINDING_CANDIDATE = "project_binding_candidate" ["surface_binding_id"];
    LIFECYCLE_KEY_STATE = "project_lifecycle_key_state" ["chain_id", "resource_id"];
    LIFECYCLE_TRIPLE_SUMMARY = "project_lifecycle_triple_summary"
        ["chain_id", "logical_name_id", "registry_identifier", "token_id"];
    LIFECYCLE_ASSOCIATION = "project_lifecycle_association"
        ["chain_id", "logical_name_id", "registry_identifier", "token_id"];
    LIFECYCLE_EVENT = "project_lifecycle_event"
        ["chain_id", "state_kind", "state_key", "event_identity"];
    CHILD_REGISTRATION_STATE = "project_child_registration_state"
        ["chain_id", "logical_name_id", "registry_contract_instance_id"];
    WRAPPER_STATE = "project_wrapper_state" ["chain_id", "resource_id"];
    REGISTRY_NODE_STATE = "project_registry_node_state" ["chain_id", "namespace", "node"];
    REGISTRY_BINDING_OBSERVATION = "project_registry_binding_observation"
        ["chain_id", "observation_identity"];
    RESOLVER_CLASSIFICATION = "project_resolver_classification" ["chain_id", "resolver_address"];
    REGISTRY_POINTER = "project_registry_pointer" ["chain_id", "namespace", "node"];
    RESOURCE_POINTER = "project_resource_pointer" ["chain_id", "resource_id"];
    NODE_RECORD_PARTITION = "project_node_record_partition"
        ["chain_id", "resolver_address", "arm", "arm_identity"];
    NODE_RECORD_VALUE = "project_node_record_value"
        ["chain_id", "resolver_address", "arm", "arm_identity", "record_key"];
    RECORD_ID_VALUE = "project_record_id_value"
        ["chain_id", "resolver_address", "record_id", "record_key"];
    RESOLVER_LINK = "project_resolver_link" ["chain_id", "resolver_address", "node"];
    GRANT = "project_grant" ["chain_id", "resource_id", "subject", "scope"];
    RESOURCE_ADMIN_AGGREGATE = "project_resource_admin_aggregate" ["chain_id", "resource_id"];
    ACCOUNT_APPROVAL = "project_account_approval"
        ["chain_id", "authority_kind", "authority_contract", "owner", "subject", "relation_kind"];
    NAME_ALIAS = "project_name_alias" ["chain_id", "logical_name_id"];
    RESOLVER_ALIAS = "project_resolver_alias" ["chain_id", "resolver_address", "alias_identity"];
    CHILD_EDGE_CANDIDATE = "project_child_edge_candidate"
        ["chain_id", "namespace", "parent_node", "child_node", "authority_arm"];
    PARENT_SUBREGISTRY = "project_parent_subregistry" ["chain_id", "logical_name_id"];
    REVERSE_TUPLE = "project_reverse_tuple" ["address", "coin_type", "namespace"];
    REVERSE_NODE_CLAIM = "project_reverse_node_claim"
        ["namespace", "reverse_node", "resolver_address"];
    CLAIM_NORMALIZATION = "project_claim_normalization" ["chain_id", "claim_event_identity"];
    ADDRESS_NAME_FOLD = "project_address_name_fold" ["chain_id", "logical_name_id"];
    ADDRESS_CONTROLLER_CANDIDATE = "project_address_controller_candidate"
        ["chain_id", "logical_name_id", "event_identity"];
}

/// The derived index tables, cleared with the chain and rebuilt from their base rows.
pub(crate) const DERIVED: [&str; 3] = [
    "project_address_name_index",
    "project_address_record_node_index",
    "project_address_record_id_index",
];

/// The spec of a journalled table by name.
pub(crate) fn spec(name: &str) -> &'static TableSpec {
    JOURNALLED
        .iter()
        .copied()
        .find(|table| table.name == name)
        .unwrap_or_else(|| panic!("unknown family table {name}"))
}

/// A journalled table by name, `None` for anything else.
pub(crate) fn find(name: &str) -> Option<&'static TableSpec> {
    JOURNALLED.iter().copied().find(|table| table.name == name)
}
