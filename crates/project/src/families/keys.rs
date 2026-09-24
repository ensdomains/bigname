//! Owned keys: which family rows one block's events own, in before and after forms. The rules are
//! the ones `seed_direct_scope` applies to names, children, resources, account keys and resolvers,
//! plus the keys the design adds (docs/projections.md, "Owned key families"). A key that appears
//! only in an event's before state is derived like any other: its reducer sees the event that
//! removes or replaces the fact.
use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::input::{BlockEvent, text};

/// The key spaces the families own. A family with two key shapes has two spaces.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum Space {
    /// F1: (namespace, logical_name_id).
    Name,
    /// F2a, F2b, F2c, F5, F8: a resource id.
    Resource,
    /// F2a: (logical_name_id, registry identifier, token id) of an ENSv2 lifecycle event.
    Triple,
    /// F2a: (logical_name_id, registry contract instance) of an ENSv2 child registration.
    ChildRegistration,
    /// F2c: (namespace, node) of an ENSv1 or Basenames registry.
    RegistryNode,
    /// F3: a resolver address.
    Resolver,
    /// F4: (namespace, node) of an ENSv1 registry-node pointer.
    RegistryPointer,
    /// F6: (resolver, node) of a node-keyed record write.
    NodeRecord,
    /// F7: (resolver, record id) of a record-id value.
    RecordId,
    /// F7: (resolver, node) of a resolver link.
    Link,
    /// F8: (resource, subject, scope) of a grant.
    Grant,
    /// F9: (authority kind, authority contract, owner, subject, relation kind).
    Approval,
    /// F10: (resolver, alias identity) of per-resolver alias state.
    ResolverAlias,
    /// F11: (namespace, child node) of an ENSv1 or Basenames child edge.
    ChildEdge,
    /// F12: (address, coin type, namespace) of a reverse tuple.
    ReverseTuple,
    /// F12: (namespace, node) of a name record.
    NodeClaim,
    /// F12: the event identity of a direct claim.
    Claim,
}

/// A key: its column values in order.
pub(crate) type Key = Vec<String>;

/// Every key the block owns, by space.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BlockKeys {
    spaces: BTreeMap<Space, BTreeSet<Key>>,
}

impl BlockKeys {
    pub(crate) fn of(&self, space: Space) -> impl Iterator<Item = &Key> {
        self.spaces.get(&space).into_iter().flatten()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, space: Space, key: &[&str]) -> bool {
        self.spaces.get(&space).is_some_and(|keys| {
            keys.contains(&key.iter().map(|part| (*part).to_owned()).collect::<Key>())
        })
    }

    fn add<const N: usize>(&mut self, space: Space, key: [Option<String>; N]) {
        let parts = key.into_iter().collect::<Option<Vec<_>>>();
        if let Some(parts) = parts.filter(|parts| parts.iter().all(|part| !part.trim().is_empty()))
        {
            self.spaces.entry(space).or_default().insert(parts);
        }
    }
}

pub(crate) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const V1_REGISTRIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];
const V1_POINTER_FAMILIES: [&str; 3] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_wrapper_l1",
];
pub(crate) const V2_LIFECYCLE_FAMILIES: [&str; 3] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
];
pub(crate) const LIFECYCLE_KINDS: [&str; 6] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "RegistrationReserved",
    "ExpiryChanged",
    "TokenControlTransferred",
];
const RECORD_EMITTER_KINDS: [&str; 5] = [
    "RecordChanged",
    "RecordVersionChanged",
    "AliasChanged",
    "ResolverRecordLinked",
    "ResolverPermissionArgument",
];
const RESOLVER_FAMILIES: [&str; 3] = [
    "ens_v1_resolver_l1",
    "ens_v2_resolver_l1",
    "basenames_base_resolver",
];

fn lower(value: Option<String>) -> Option<String> {
    value.map(|value| value.to_ascii_lowercase())
}

fn path(value: &Value, parts: &[&str]) -> Option<String> {
    let (last, parents) = parts.split_last()?;
    let object = parents
        .iter()
        .try_fold(value, |value, part| value.get(*part))?;
    text(object, last)
}

/// The registry identifier of an ENSv2 lifecycle event: contract instance, else emitter, else
/// the after-state registry.
pub(crate) fn registry_identifier(event: &BlockEvent) -> Option<String> {
    event
        .after_text("registry_contract_instance_id")
        .or_else(|| text(&event.raw_fact_ref, "emitting_address"))
        .or_else(|| event.after_text("registry"))
}

/// The triple an ENSv2 lifecycle event belongs to: name, registry identifier and token id, the
/// token as empty text when the event carries none.
pub(crate) fn triple(event: &BlockEvent) -> Option<Key> {
    if !V2_LIFECYCLE_FAMILIES.contains(&event.source_family.as_str())
        || !LIFECYCLE_KINDS.contains(&event.event_kind.as_str())
    {
        return None;
    }
    Some(vec![
        event.logical_name_id.clone()?,
        registry_identifier(event)?,
        event.after_text("token_id").unwrap_or_default(),
    ])
}

/// The node an ENSv1 pointer event addresses: child node, else namehash, else node.
pub(crate) fn pointer_node(state: &Value) -> Option<String> {
    lower(
        text(state, "child_node")
            .or_else(|| text(state, "namehash"))
            .or_else(|| text(state, "node")),
    )
}

/// The scope key of a grant, as permissions.rs builds it.
pub(crate) fn grant_scope(after: &Value) -> Option<String> {
    let scope = after.get("scope").filter(|scope| scope.is_object())?;
    Some(match text(scope, "kind")?.as_str() {
        "root" | "registry_root" => "root".to_owned(),
        "registry" => "registry".to_owned(),
        "resource" => "resource".to_owned(),
        "resolver" => format!(
            "resolver:{}:{}",
            text(scope, "chain_id").unwrap_or_default(),
            text(scope, "resolver_address")
                .unwrap_or_default()
                .to_ascii_lowercase()
        ),
        "record_manager" => format!(
            "record_manager:{}:{}",
            text(scope, "chain_id").unwrap_or_default(),
            text(scope, "manager_address")
                .unwrap_or_default()
                .to_ascii_lowercase()
        ),
        _ => return None,
    })
}

/// The alias identity of an AliasChanged at its resolver.
pub(crate) fn alias_identity(event: &BlockEvent) -> String {
    event
        .logical_name_id
        .clone()
        .or_else(|| {
            [
                "from_logical_name_id",
                "from_namehash",
                "from_dns_encoded_name",
                "from_name",
            ]
            .iter()
            .find_map(|field| event.after_text(field).or_else(|| event.before_text(field)))
        })
        .unwrap_or_else(|| event.position.event_identity.clone())
}

/// The resolver an AliasChanged is written at.
pub(crate) fn alias_resolver(event: &BlockEvent) -> Option<String> {
    lower(
        event
            .after_text("resolver")
            .or_else(|| event.before_text("resolver"))
            .or_else(|| text(&event.raw_fact_ref, "emitting_address")),
    )
}

/// The resolver a record write is attributed to: after-state resolver, else the emitter.
pub(crate) fn record_resolver(event: &BlockEvent) -> Option<String> {
    lower(
        event
            .after_text("resolver")
            .or_else(|| text(&event.raw_fact_ref, "emitting_address")),
    )
}

/// Derive every key the block's events own.
pub(crate) fn derive(events: &[BlockEvent]) -> BlockKeys {
    let mut keys = BlockKeys::default();
    for event in events {
        derive_event(event, &mut keys);
    }
    keys
}

fn derive_event(event: &BlockEvent, keys: &mut BlockKeys) {
    let kind = event.event_kind.as_str();
    let family = event.source_family.as_str();
    let namespace = Some(event.namespace.clone());
    keys.add(
        Space::Name,
        [namespace.clone(), event.logical_name_id.clone()],
    );
    keys.add(Space::Resource, [event.resource_id.clone()]);

    if matches!(kind, "SubregistryChanged" | "AuthorityTransferred")
        && V1_REGISTRIES.contains(&family)
    {
        for state in [&event.after, &event.before] {
            keys.add(
                Space::RegistryNode,
                [namespace.clone(), lower(text(state, "child_node"))],
            );
            keys.add(
                Space::RegistryNode,
                [namespace.clone(), lower(text(state, "node"))],
            );
            if kind == "SubregistryChanged" {
                keys.add(
                    Space::ChildEdge,
                    [namespace.clone(), lower(text(state, "child_node"))],
                );
            }
        }
    }
    if kind == "AccountPermissionChanged" {
        for state in [&event.after, &event.before] {
            keys.add(
                Space::Approval,
                [
                    path(state, &["scope", "authority_kind"]),
                    lower(path(state, &["scope", "authority_contract"])),
                    lower(path(state, &["scope", "owner"])),
                    lower(text(state, "subject")),
                    text(state, "relation_kind"),
                ],
            );
        }
    }
    derive_resolvers(event, keys);
    derive_records(event, keys);
    derive_lifecycle(event, keys);
    derive_reverse(event, keys);
    if matches!(kind, "PermissionChanged" | "RootPermissionChanged") {
        for state in [&event.after, &event.before] {
            keys.add(
                Space::Grant,
                [
                    event.resource_id.clone(),
                    lower(text(state, "subject")),
                    grant_scope(state),
                ],
            );
        }
    }
    if kind == "AliasChanged" {
        keys.add(
            Space::ResolverAlias,
            [alias_resolver(event), Some(alias_identity(event))],
        );
    }
}

fn derive_resolvers(event: &BlockEvent, keys: &mut BlockKeys) {
    let kind = event.event_kind.as_str();
    let family = event.source_family.as_str();
    let mut resolvers = Vec::new();
    match kind {
        "ResolverChanged" => {
            resolvers.push(event.after_text("resolver"));
            resolvers.push(event.before_text("resolver"));
            if V1_POINTER_FAMILIES.contains(&family) {
                for state in [&event.after, &event.before] {
                    keys.add(
                        Space::RegistryPointer,
                        [Some(event.namespace.clone()), pointer_node(state)],
                    );
                }
            }
        }
        "Upgraded" => {
            resolvers.push(event.after_text("proxy_address"));
            resolvers.push(event.before_text("proxy_address"));
        }
        "PermissionChanged" => {
            resolvers.push(path(&event.after, &["scope", "resolver_address"]));
            resolvers.push(path(&event.before, &["scope", "resolver_address"]));
        }
        _ if RECORD_EMITTER_KINDS.contains(&kind) && RESOLVER_FAMILIES.contains(&family) => {
            resolvers.push(text(&event.raw_fact_ref, "emitting_address"));
        }
        _ => {}
    }
    for resolver in lower_all(resolvers) {
        if resolver != ZERO_ADDRESS {
            keys.add(Space::Resolver, [Some(resolver)]);
        }
    }
}

fn lower_all(values: Vec<Option<String>>) -> impl Iterator<Item = String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.to_ascii_lowercase())
}

fn derive_records(event: &BlockEvent, keys: &mut BlockKeys) {
    let kind = event.event_kind.as_str();
    match kind {
        "RecordChanged" | "RecordVersionChanged" => {
            let resolver = record_resolver(event);
            if event.after_text("storage_model").as_deref() == Some("resolver_record_id") {
                keys.add(
                    Space::RecordId,
                    [resolver, event.after_text("resolver_record_id")],
                );
            } else {
                keys.add(
                    Space::NodeRecord,
                    [resolver, lower(event.after_text("node"))],
                );
            }
            if kind == "RecordChanged" && event.after_text("record_key").as_deref() == Some("name")
            {
                keys.add(
                    Space::NodeClaim,
                    [
                        Some(event.namespace.clone()),
                        lower(event.after_text("node")),
                    ],
                );
            }
        }
        "ResolverRecordLinked" => keys.add(
            Space::Link,
            [
                lower(event.after_text("resolver")),
                lower(event.after_text("node")),
            ],
        ),
        _ => {}
    }
}

fn derive_lifecycle(event: &BlockEvent, keys: &mut BlockKeys) {
    if let Some(triple) = triple(event) {
        let resource_bearing = event.resource_id.is_some();
        let association = resource_bearing
            && matches!(
                event.event_kind.as_str(),
                "RegistrationGranted" | "RegistrationReserved"
            );
        if !resource_bearing || association {
            let [name, registry, token] = <[String; 3]>::try_from(triple).expect("three parts");
            keys.add(Space::Triple, [Some(name), Some(registry), Some(token)]);
        }
    }
    if matches!(
        event.source_family.as_str(),
        "ens_v2_root_l1" | "ens_v2_registry_l1"
    ) && matches!(
        event.event_kind.as_str(),
        "RegistrationGranted"
            | "RegistrationRenewed"
            | "RegistrationReleased"
            | "RegistrationReserved"
    ) {
        keys.add(
            Space::ChildRegistration,
            [
                event.logical_name_id.clone(),
                event.after_text("registry_contract_instance_id"),
            ],
        );
    }
}

fn derive_reverse(event: &BlockEvent, keys: &mut BlockKeys) {
    let mut tuple = |state: &Value| {
        keys.add(
            Space::ReverseTuple,
            [
                lower(text(state, "address")),
                text(state, "coin_type"),
                text(state, "namespace"),
            ],
        );
    };
    match event.event_kind.as_str() {
        "ReverseChanged" => {
            tuple(&event.after);
            tuple(&event.before);
        }
        "RecordChanged" => {
            for state in [&event.after, &event.before] {
                if let Some(source) = state.get("primary_claim_source") {
                    tuple(source);
                }
            }
            if event.after.get("primary_claim_source").is_some() {
                keys.add(Space::Claim, [Some(event.position.event_identity.clone())]);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "keys_tests.rs"]
mod tests;
