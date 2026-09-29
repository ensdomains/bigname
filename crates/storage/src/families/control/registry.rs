//! F2c, registry ownership: the registry generation of an ENSv1 name, the zero-owner facts the
//! ownerless registry profile reads, and the per-resource registry binding the permission
//! summary serves.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use super::{
    position::Position,
    rows::{flag, lower, text},
};

pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
/// The ENSv1 and Basenames registry families, whose transfers the ownerless profile reads.
const V1_REGISTRIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];

/// Whether a node is the all-zero root node.
fn is_root(node: &str) -> bool {
    node.strip_prefix("0x")
        .is_some_and(|digits| digits.len() == 64 && digits.bytes().all(|digit| digit == b'0'))
}

/// The parts of one `project_registry_node_state` row the readers use, with the node's
/// owner-setting events. The row's owner group is not read: the control owner and the
/// ownerless profile read the owner events, which keep every owner-setting event. A node with
/// owner events and no row (an ENSv2 name's node) reads as a node with neither registry record.
#[derive(Clone, Debug, Default)]
pub struct RegistryNode {
    pub namespace: String,
    pub node: String,
    pub has_old_record: bool,
    pub first_current_record_block: Option<i64>,
    /// Every owner-setting registry event of the node (`project_registry_owner_event`), in the
    /// canonical order.
    pub owner_events: Vec<OwnerEvent>,
}

/// One owner-setting registry event of a node (`project_registry_owner_event`): an ENSv1 or
/// Basenames AuthorityTransferred or SubregistryChanged, or an ENSv2 registry AuthorityTransferred,
/// with the name, resource, authority kind and owner facts it carried, including its own
/// `registry_owner` and `owner_word_unmasked`.
#[derive(Clone, Debug)]
pub struct OwnerEvent {
    pub position: Position,
    pub transaction_hash: Option<String>,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<String>,
    pub event_kind: String,
    pub source_family: String,
    pub authority_kind: Option<String>,
    pub owner: Option<String>,
    pub registry_owner: Option<String>,
    pub owner_word_unmasked: Option<bool>,
    pub owner_getter: Option<String>,
    pub owner_getter_reason: Option<String>,
}

impl OwnerEvent {
    fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            position: Position::of_row(row)?,
            transaction_hash: text(row, "transaction_hash"),
            logical_name_id: text(row, "logical_name_id"),
            resource_id: text(row, "resource_id"),
            event_kind: text(row, "event_kind")?,
            source_family: text(row, "source_family")?,
            authority_kind: text(row, "authority_kind"),
            owner: lower(row, "owner"),
            registry_owner: lower(row, "registry_owner"),
            owner_word_unmasked: flag(row, "owner_word_unmasked"),
            owner_getter: lower(row, "owner_getter"),
            owner_getter_reason: text(row, "owner_getter_reason"),
        })
    }

    /// The owner this event reports to the served control block, the registry getter's view of
    /// it (`owner(node)`): none when its owner word is unmasked; else the owner getter the
    /// adapter recorded, which is the owner for an ordinary word and zero for a literal zero or,
    /// on a registry whose getter maps its own address to zero, for that address
    /// (`owner_getter_reason = registry_self`); else, for a payload written before the getter
    /// was recorded, its registry_owner, else its owner.
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
    pub fn reported_owner(&self) -> Option<String> {
        if self.owner_word_unmasked == Some(true) {
            None
        } else {
            self.owner_getter
                .clone()
                .or_else(|| self.registry_owner.clone())
                .or_else(|| self.owner.clone())
        }
    }
}

impl RegistryNode {
    /// The node's latest ENSv1 or Basenames registry AuthorityTransferred in the canonical order,
    /// the event the served ownerless-registry profile reads.
    pub fn latest_transfer(&self) -> Option<&OwnerEvent> {
        self.owner_events
            .iter()
            .filter(|event| {
                event.event_kind == "AuthorityTransferred"
                    && V1_REGISTRIES.contains(&event.source_family.as_str())
            })
            .max_by(|left, right| left.position.cmp(&right.position))
    }

    fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            namespace: text(row, "namespace")?,
            node: text(row, "node")?,
            has_old_record: flag(row, "has_old_record").unwrap_or(false),
            first_current_record_block: row
                .get("first_current_record_block")
                .and_then(Value::as_i64),
            owner_events: Vec::new(),
        })
    }
}

/// The registry generation and handoff block of an ENS name: `old` when the 2017 registry recorded
/// the node and the current registry has not, under arm ens_v1; the handoff block is the first
/// current-registry record. The fold reads ENS registry records only and leaves the all-zero root
/// node out, so a node of another namespace, such as a Basenames node F2c also keeps, and the root
/// node have no records and no handoff block.
pub fn registry_generation(
    node: Option<&RegistryNode>,
    authority_arm: Option<&str>,
) -> (Option<&'static str>, Option<i64>) {
    let node = node.filter(|node| node.namespace == "ens" && !is_root(&node.node));
    let generation = (authority_arm == Some("ens_v1")).then(|| match node {
        Some(node) if node.has_old_record && node.first_current_record_block.is_none() => "old",
        _ => "current",
    });
    (
        generation,
        node.and_then(|node| node.first_current_record_block),
    )
}

/// Whether the ownerless-registry profile applies: the
/// node's latest AuthorityTransferred reports the zero address as its owner getter, no binding
/// is selected and the arm is not ENSv2. Only AuthorityTransferred counts. The families key the
/// transfers by node, which is the name's namehash, so an unnamed transfer counts for the node
/// it addresses.
pub fn ownerless_registry(
    node: Option<&RegistryNode>,
    selected_binding: Option<&str>,
    authority_arm: Option<&str>,
) -> bool {
    node.and_then(RegistryNode::latest_transfer)
        .is_some_and(|event| event.owner_getter.as_deref() == Some(ZERO_ADDRESS))
        && selected_binding.is_none()
        && authority_arm != Some("ens_v2")
}

/// The F2c node states of `(namespace, node)` keys.
pub async fn load_registry_nodes(
    pool: &PgPool,
    chain_id: &str,
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), RegistryNode>> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire a connection for registry node states")?;
    load_registry_nodes_on(&mut conn, chain_id, keys).await
}

/// [`load_registry_nodes`] on one connection, so a caller's transaction reads both statements
/// in its snapshot.
pub async fn load_registry_nodes_on(
    conn: &mut PgConnection,
    chain_id: &str,
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), RegistryNode>> {
    let (namespaces, nodes_wanted): (Vec<String>, Vec<String>) = keys.iter().cloned().unzip();
    let rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.registry.nodes */ SELECT jsonb_build_object(
                    'namespace', state.namespace, 'node', state.node,
                    'has_old_record', state.has_old_record,
                    'first_current_record_block', state.first_current_record_block)
         FROM bigname_phase.project_registry_node_state state
         JOIN unnest($2::text[], $3::text[]) wanted(namespace, node)
           ON wanted.namespace = state.namespace AND wanted.node = state.node
         WHERE state.chain_id = $1",
    )
    .bind(chain_id)
    .bind(&namespaces)
    .bind(&nodes_wanted)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load registry node states")?;
    let mut nodes: BTreeMap<(String, String), RegistryNode> = rows
        .iter()
        .filter_map(RegistryNode::from_row)
        .map(|node| ((node.namespace.clone(), node.node.clone()), node))
        .collect();
    let events: Vec<(String, String, Value)> = sqlx::query_as(
        "/* storage:families.control.registry.owner_events */ SELECT event.namespace,
                event.node, to_jsonb(event)
         FROM bigname_phase.project_registry_owner_event event
         JOIN unnest($2::text[], $3::text[]) wanted(namespace, node)
           ON wanted.namespace = event.namespace AND wanted.node = event.node
         WHERE event.chain_id = $1",
    )
    .bind(chain_id)
    .bind(&namespaces)
    .bind(&nodes_wanted)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load registry owner events")?;
    for (namespace, node, row) in events {
        if let Some(event) = OwnerEvent::from_row(&row) {
            nodes
                .entry((namespace.clone(), node.clone()))
                .or_insert_with(|| RegistryNode {
                    namespace,
                    node,
                    ..RegistryNode::default()
                })
                .owner_events
                .push(event);
        }
    }
    for state in nodes.values_mut() {
        state
            .owner_events
            .sort_by(|left, right| left.position.cmp(&right.position));
    }
    Ok(nodes)
}

/// One registry-binding observation (`project_registry_binding_observation`).
#[derive(Clone, Debug)]
pub struct Observation {
    pub resource_id: String,
    /// The event's name, null for an unnamed observation.
    pub logical_name_id: Option<String>,
    pub attributed_via: String,
    /// The resource the observation reaches after the block, under the name's ENSv1 or
    /// Basenames binding for a name-attributed row.
    pub target_resource_id: String,
    pub position: Position,
    pub event_kind: String,
    pub registry_owner: Option<String>,
    pub registry_contract: Option<String>,
    pub provenance: Value,
    pub applicable: bool,
    pub clear_event_identity: Option<String>,
    /// The generated id of the event that wrote the row, attribution only.
    pub normalized_event_id: Option<i64>,
}

impl Observation {
    pub(crate) fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            resource_id: text(row, "resource_id")?,
            logical_name_id: text(row, "logical_name_id"),
            attributed_via: text(row, "attributed_via")?,
            target_resource_id: text(row, "target_resource_id")?,
            position: Position::of_row(row)?,
            event_kind: text(row, "event_kind")?,
            registry_owner: lower(row, "registry_owner"),
            registry_contract: lower(row, "registry_contract"),
            provenance: row.get("provenance").cloned().unwrap_or(Value::Null),
            applicable: flag(row, "applicable").unwrap_or(false),
            clear_event_identity: text(row, "clear_event_identity"),
            normalized_event_id: row.get("normalized_event_id").and_then(Value::as_i64),
        })
    }
}

/// The name facts the attribution reads: the name's current resource and its authority arm,
/// both from the composed name row.
#[derive(Clone, Debug, Default)]
pub struct NameAttribution {
    pub current_resource_id: Option<String>,
    pub authority_arm: Option<String>,
}

/// What the permission summary serves for one resource's registry binding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RegistryBinding {
    pub registry_owner: Option<String>,
    pub registry_contract: Option<String>,
    /// The chain position of the observation when it applies.
    pub position: Option<Position>,
    pub raw_fact_ref: Value,
    /// The identity of the observation that cleared the binding when it does not apply.
    pub clear_event_identity: Option<String>,
    /// The generated id of the selected observation's event, attribution only: the served
    /// summary names the event by it.
    pub normalized_event_id: Option<i64>,
}

/// The registry binding of every resource the observations reach. The observations are keyed by
/// `COALESCE(logical_name_id, resource_id)` and the latest per key is taken; a name-addressed
/// AuthorityTransferred or SubregistryChanged moves to the name's current resource under arm
/// ens_v1 or basenames, and the latest per resource is then taken. F2c keeps one row per that
/// key with the resource it reaches under the name's ENSv1 or Basenames binding. The move reads
/// the composed name's selection, so a name `names` carries is moved by that
/// selection (a name whose arm is ENSv2 stays on the event's own resource), and any other row
/// reaches its stored target.
pub fn registry_bindings(
    observations: &[Observation],
    names: &BTreeMap<String, NameAttribution>,
) -> BTreeMap<String, RegistryBinding> {
    let mut per_resource: BTreeMap<String, &Observation> = BTreeMap::new();
    for observation in observations {
        let resource = match observation
            .logical_name_id
            .as_deref()
            .filter(|_| observation.attributed_via == "name")
            .and_then(|name| names.get(name))
        {
            Some(name) if matches!(name.authority_arm.as_deref(), Some("ens_v1" | "basenames")) => {
                name.current_resource_id
                    .clone()
                    .unwrap_or_else(|| observation.resource_id.clone())
            }
            Some(_) => observation.resource_id.clone(),
            None => observation.target_resource_id.clone(),
        };
        let entry = per_resource.entry(resource).or_insert(observation);
        if observation.position > entry.position {
            *entry = observation;
        }
    }
    per_resource
        .into_iter()
        .map(|(resource, observation)| {
            let binding = if observation.applicable {
                RegistryBinding {
                    registry_owner: observation.registry_owner.clone(),
                    registry_contract: observation.registry_contract.clone(),
                    position: Some(observation.position.clone()),
                    raw_fact_ref: observation
                        .provenance
                        .get("raw_fact_ref")
                        .cloned()
                        .unwrap_or(Value::Null),
                    clear_event_identity: None,
                    normalized_event_id: observation.normalized_event_id,
                }
            } else {
                RegistryBinding {
                    clear_event_identity: Some(observation.position.event_identity.clone()),
                    normalized_event_id: observation.normalized_event_id,
                    ..RegistryBinding::default()
                }
            };
            (resource, binding)
        })
        .collect()
}

impl RegistryBinding {
    /// The chain position the summary serves, without the block hash no family row carries.
    pub fn chain_positions(&self) -> Value {
        self.position.as_ref().map_or(Value::Null, |position| {
            json!({
                "block_number": position.block_number,
                "transaction_index": position.transaction_index,
                "log_index": position.log_index,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(
        resource: &str,
        via: &str,
        block: i64,
        kind: &str,
        name: Option<&str>,
        applicable: bool,
    ) -> Observation {
        Observation {
            resource_id: resource.into(),
            logical_name_id: name.map(str::to_owned),
            attributed_via: via.into(),
            target_resource_id: resource.into(),
            position: Position {
                block_number: block,
                transaction_index: Some(0),
                log_index: Some(0),
                event_identity: format!("e{block}"),
            },
            event_kind: kind.into(),
            registry_owner: applicable.then(|| "0x00000000000000000000000000000000000000aa".into()),
            registry_contract: Some("0x00000000000000000000000000000000000000bb".into()),
            provenance: json!({"raw_fact_ref": {"emitting_address": "0xbb"}}),
            applicable,
            clear_event_identity: (!applicable).then(|| format!("e{block}")),
            normalized_event_id: Some(block),
        }
    }

    #[test]
    fn a_surface_unbound_after_a_bound_clears_with_its_own_identity() {
        let observations = [
            observation("r", "own", 10, "SurfaceBound", None, true),
            observation("r", "own", 20, "SurfaceUnbound", None, false),
        ];
        let bindings = registry_bindings(&observations, &BTreeMap::new());
        let binding = &bindings["r"];
        assert_eq!(binding.registry_owner, None);
        assert_eq!(binding.clear_event_identity.as_deref(), Some("e20"));
    }

    #[test]
    fn a_named_transfer_moves_to_the_names_current_resource_under_ens_v1() {
        let observations = [observation(
            "node",
            "name",
            10,
            "AuthorityTransferred",
            Some("ens:n"),
            true,
        )];
        let names = BTreeMap::from([(
            "ens:n".to_owned(),
            NameAttribution {
                current_resource_id: Some("lease".into()),
                authority_arm: Some("ens_v1".into()),
            },
        )]);
        let bindings = registry_bindings(&observations, &names);
        assert!(bindings.contains_key("lease") && !bindings.contains_key("node"));
        let v2 = BTreeMap::from([(
            "ens:n".to_owned(),
            NameAttribution {
                current_resource_id: Some("lease".into()),
                authority_arm: Some("ens_v2".into()),
            },
        )]);
        assert!(registry_bindings(&observations, &v2).contains_key("node"));
    }

    fn owner_event(block: i64, family: &str, getter: Option<&str>) -> OwnerEvent {
        OwnerEvent {
            position: Position {
                block_number: block,
                transaction_index: Some(0),
                log_index: Some(0),
                event_identity: format!("e{block}"),
            },
            transaction_hash: None,
            logical_name_id: Some("ens:0x01".into()),
            resource_id: None,
            event_kind: "AuthorityTransferred".into(),
            source_family: family.into(),
            authority_kind: None,
            owner: Some("0x00000000000000000000000000000000000000aa".into()),
            registry_owner: None,
            owner_word_unmasked: None,
            owner_getter: getter.map(str::to_owned),
            owner_getter_reason: None,
        }
    }

    /// The ownerless profile reads the ENSv1 and Basenames registry transfers only: a later
    /// ENSv2 registration's owner transfer at the same node does not hide a zero-owner ENSv1
    /// transfer, as the served profile never read ENSv2 transfers.
    #[test]
    fn the_ownerless_profile_reads_only_ens_v1_and_basenames_transfers() {
        let node = RegistryNode {
            namespace: "ens".into(),
            node: "0x01".into(),
            owner_events: vec![
                owner_event(10, "ens_v1_registry_l1", Some(ZERO_ADDRESS)),
                owner_event(12, "ens_v2_registry_l1", None),
            ],
            ..RegistryNode::default()
        };
        assert_eq!(
            node.latest_transfer()
                .map(|event| event.position.block_number),
            Some(10)
        );
        assert!(ownerless_registry(Some(&node), None, Some("ens_v1")));
    }

    #[test]
    fn generation_is_old_only_before_the_current_registry_records_the_node() {
        let mut node = RegistryNode {
            namespace: "ens".into(),
            has_old_record: true,
            ..RegistryNode::default()
        };
        assert_eq!(
            registry_generation(Some(&node), Some("ens_v1")),
            (Some("old"), None)
        );
        node.first_current_record_block = Some(12);
        assert_eq!(
            registry_generation(Some(&node), Some("ens_v1")),
            (Some("current"), Some(12))
        );
        assert_eq!(
            registry_generation(Some(&node), Some("ens_v2")),
            (None, Some(12))
        );
        assert_eq!(
            registry_generation(None, Some("ens_v1")),
            (Some("current"), None)
        );
        node.namespace = "basenames".into();
        assert_eq!(
            registry_generation(Some(&node), Some("ens_v1")),
            (Some("current"), None)
        );
    }

    /// The served records leave the all-zero root node out: an
    /// old-only or current-registry root record gives no records and no handoff block.
    #[test]
    fn the_root_node_has_no_registry_records() {
        let mut root = RegistryNode {
            namespace: "ens".into(),
            node: format!("0x{}", "0".repeat(64)),
            has_old_record: true,
            ..RegistryNode::default()
        };
        for current in [None, Some(12)] {
            root.first_current_record_block = current;
            let generation = registry_generation(Some(&root), Some("ens_v1"));
            assert_eq!(generation, (Some("current"), None), "{current:?}");
        }
    }
}
