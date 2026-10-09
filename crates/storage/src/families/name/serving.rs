//! The serving pointer and the resolver block of a composed name row.
//!
//! The serving pointer is the resolver a name is read through when it has no selected binding: an
//! ownerless registry node's retained registry pointer, or an ENSv2 root-registry TLD's pointer
//! whose registration was never observed. The resolver block is the latest of the name's admitted
//! `ResolverChanged` and the serving pointer.
//!
//! Both read the F5 resource pointer (`project_resource_pointer`) and the F4 registry-node
//! pointer (`project_registry_pointer`), which keep each key's latest pointer. The name each
//! pointer event carried is read back from `normalized_events` by its identity, a metadata
//! lookup by key. The F5 named pointer (`project_named_resource_pointer`) also retains each
//! name's latest admitted pointer on a resource, so another name's later write on the same
//! resource does not displace it.
use serde_json::Value;

use crate::families::position::Position as FamilyPosition;

pub(super) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const REGISTRY_FAMILIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];

/// One pointer row as the composed read sees it.
#[derive(Clone, Debug)]
pub struct PointerRow {
    /// The resource the pointer is on, none for an F4 row without one.
    pub resource_id: Option<String>,
    /// The lower-cased resolver, none for a pointer event without one.
    pub resolver_address: Option<String>,
    pub position: FamilyPosition,
    pub source_family: String,
    /// The pointer event's name and generated id, read back by identity.
    pub logical_name_id: Option<String>,
    pub normalized_event_id: Option<i64>,
}

impl PointerRow {
    fn resolves(&self) -> bool {
        self.resolver_address
            .as_deref()
            .is_some_and(|address| !address.is_empty() && address != ZERO_ADDRESS)
    }
}

/// `declared_summary.ens_v1_resolver`: the node's `project_registry_pointer` row as the registry
/// getter returns it, `{chain_id, address}`, or null for a zero pointer, a pointer event without a
/// resolver, or a node with no pointer event. It is not admitted against the row's name or arm.
/// It is the node's registry fact, which the served `resolver` may withhold or replace.
pub(super) fn ens_v1_resolver(pointer: Option<&PointerRow>, chain_id: &str) -> Value {
    pointer
        .filter(|pointer| pointer.resolves())
        .and_then(|pointer| pointer.resolver_address.as_deref())
        .map_or(
            Value::Null,
            |address| serde_json::json!({"chain_id": chain_id, "address": address}),
        )
}

/// The serving pointer of a name.
#[derive(Clone, Debug)]
pub struct Serving {
    pub pointer: PointerRow,
    pub basis: &'static str,
    pub owner_getter_reason: Option<String>,
}

impl Serving {
    pub fn resource_id(&self) -> Option<&str> {
        self.pointer.resource_id.as_deref()
    }

    /// `provenance.read_reachability` with its nulls stripped.
    pub fn read_reachability(serving: Option<&Self>) -> Value {
        let mut out = serde_json::Map::new();
        if let Some(serving) = serving {
            if let Some(resource) = serving.resource_id() {
                out.insert("serving_resource_id".into(), Value::from(resource));
            }
            out.insert("basis".into(), Value::from(serving.basis));
            if let Some(reason) = &serving.owner_getter_reason {
                out.insert("owner_getter_reason".into(), Value::from(reason.as_str()));
            }
            if let Some(id) = serving.pointer.normalized_event_id {
                out.insert("pointer_event_id".into(), Value::from(id));
            }
            out.insert(
                "pointer_event_identity".into(),
                Value::from(serving.pointer.position.event_identity.as_str()),
            );
        }
        Value::Object(out)
    }
}

/// The ownerless registry node's serving pointer: the latest registry
/// `ResolverChanged` of the name on the resource of the node's zero-getter transfer, when it
/// names a resolver and the resource has no token lineage.
pub fn ownerless_serving(
    name: &str,
    pointer: Option<&PointerRow>,
    resource_has_token_lineage: bool,
    owner_getter_reason: Option<String>,
) -> Option<Serving> {
    let pointer = pointer?;
    (pointer.logical_name_id.as_deref() == Some(name)
        && REGISTRY_FAMILIES.contains(&pointer.source_family.as_str())
        && pointer.resolves()
        && !resource_has_token_lineage)
        .then(|| Serving {
            pointer: pointer.clone(),
            basis: "retained_registry_resolver_pointer",
            owner_getter_reason,
        })
}

/// The ENSv2 root-registry TLD serving pointer: the latest root-registry
/// pointer on a token resource the name's root pointers name, when it names a resolver and no
/// root-registry release of that resource is at or after it. `released_at` gives the latest
/// root release position of each resource.
pub fn root_tld_serving(
    pointers: &[PointerRow],
    released_at: impl Fn(&str) -> Option<(i64, i64, i64)>,
) -> Option<Serving> {
    let latest = pointers
        .iter()
        .filter(|pointer| {
            pointer.source_family == "ens_v2_root_l1" && pointer.resource_id.is_some()
        })
        .max_by(|left, right| left.position.cmp(&right.position))?;
    let at = bound(&latest.position);
    let released = latest
        .resource_id
        .as_deref()
        .and_then(&released_at)
        .is_some_and(|release| release >= at);
    (latest.resolves() && !released).then(|| Serving {
        pointer: latest.clone(),
        basis: "root_registry_resolver_pointer",
        owner_getter_reason: None,
    })
}

pub(super) fn bound(position: &FamilyPosition) -> (i64, i64, i64) {
    (
        position.block_number,
        position.transaction_index.unwrap_or(-1),
        position.log_index.unwrap_or(-1),
    )
}

/// What the resolver lateral needs to know about the name's selection.
pub struct ResolverScope<'a> {
    pub name: &'a str,
    pub chain_id: &'a str,
    /// The resource the name's admitted pointers sit on: the selected lifecycle key's resource
    /// for an ENSv2 name, the selected resource otherwise.
    pub resource: Option<&'a str>,
    /// The selected arm, for a pointer without a resource.
    pub arm: Option<&'a str>,
    /// Whether the name's authority admits events at all (an unsupported selection admits no
    /// pointer).
    pub admits: bool,
    /// Whether ENSv2 is the selected arm with a released or reserved registration, or the name
    /// is a released ENSv1 tombstone: its admitted pointer is kept but not served.
    pub withholds: bool,
    /// Whether the name resolves to nothing through the Universal Resolver
    /// (`resolvability.rs`): no pointer is served, not even a serving resource's.
    pub unresolvable: bool,
}

/// The resolver block and the pointer's source family.
pub fn resolver_block(
    scope: &ResolverScope<'_>,
    resource_pointer: Option<&PointerRow>,
    node_pointer: Option<&PointerRow>,
    serving: Option<&Serving>,
) -> (Value, Option<String>) {
    let admitted = |pointer: &&PointerRow| {
        scope.admits
            && pointer.logical_name_id.as_deref() == Some(scope.name)
            && match pointer.resource_id.as_deref() {
                Some(resource) => Some(resource) == scope.resource,
                None => {
                    super::selection::arm_of(&pointer.source_family).is_some()
                        && super::selection::arm_of(&pointer.source_family) == scope.arm
                }
            }
    };
    let mut candidates: Vec<(&PointerRow, bool)> = resource_pointer
        .into_iter()
        .chain(node_pointer.filter(|pointer| pointer.resource_id.is_none()))
        .filter(admitted)
        .map(|pointer| (pointer, false))
        .collect();
    if let Some(serving) = serving {
        candidates.push((&serving.pointer, true));
    }
    let latest = candidates
        .into_iter()
        .max_by(|left, right| left.0.position.cmp(&right.0.position));
    let mut block = serde_json::Map::new();
    let shown = latest.filter(|(pointer, is_serving)| {
        !scope.unresolvable && pointer.resolves() && (*is_serving || !scope.withholds)
    });
    block.insert(
        "chain_id".into(),
        shown.map_or(Value::Null, |_| Value::from(scope.chain_id)),
    );
    block.insert(
        "address".into(),
        shown
            .and_then(|(pointer, _)| pointer.resolver_address.clone())
            .map_or(Value::Null, Value::from),
    );
    block.insert(
        "latest_event_kind".into(),
        latest.map_or(Value::Null, |_| Value::from("ResolverChanged")),
    );
    (
        Value::Object(block),
        latest.map(|(pointer, _)| pointer.source_family.clone()),
    )
}
