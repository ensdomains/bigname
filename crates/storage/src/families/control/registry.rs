//! F2c, registry ownership: the registry generation of an ENSv1 name (the pull request 947
//! fold, name_authority/build.sql:754-791 and :815-819), the zero-owner facts the ownerless
//! registry profile reads (name_authority/stage.rs:201-268), and the per-resource registry
//! binding the permission summary serves (permission_resources.rs:10-79).
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    position::{EventOrder, Position},
    rows::{flag, lower, text},
};

pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// The parts of one `project_registry_node_state` row the readers use, with the node's
/// owner-setting events. The row's owner group is not read: the control owner and the
/// ownerless profile read the owner events, which keep every owner-setting event.
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

/// One owner-setting registry event of a node (`project_registry_owner_event`): an
/// AuthorityTransferred or SubregistryChanged with the name, resource, authority kind and owner
/// facts it carried, including its own `registry_owner` and `owner_word_unmasked`.
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
        })
    }

    /// The owner this event reports to the served control block (build.sql:650-663): null when
    /// its owner word is unmasked, else its registry_owner, else its owner.
    pub fn reported_owner(&self) -> Option<String> {
        if self.owner_word_unmasked == Some(true) {
            None
        } else {
            self.registry_owner.clone().or_else(|| self.owner.clone())
        }
    }
}

impl RegistryNode {
    /// The node's latest AuthorityTransferred in the canonical order, the event the served
    /// ownerless-registry profile reads (name_authority/stage.rs:201-268).
    pub fn latest_transfer(&self) -> Option<&OwnerEvent> {
        self.owner_events
            .iter()
            .filter(|event| event.event_kind == "AuthorityTransferred")
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

/// The registry generation and handoff block of an ENS name (name_authority/build.sql
/// :815-819): `old` when the 2017 registry recorded the node and the current registry has not,
/// under arm ens_v1; the handoff block is the first current-registry record. The served fold
/// reads ENS registry records only (name_authority/build.sql:775-790), so a node of another
/// namespace, such as a Basenames node F2c also keeps, has no handoff block.
pub fn registry_generation(
    node: Option<&RegistryNode>,
    authority_arm: Option<&str>,
) -> (Option<&'static str>, Option<i64>) {
    let node = node.filter(|node| node.namespace == "ens");
    let generation = (authority_arm == Some("ens_v1")).then(|| match node {
        Some(node) if node.has_old_record && node.first_current_record_block.is_none() => "old",
        _ => "current",
    });
    (
        generation,
        node.and_then(|node| node.first_current_record_block),
    )
}

/// Whether the ownerless-registry profile applies (name_authority/build.sql:852-856): the
/// node's latest AuthorityTransferred reports the zero address as its owner getter
/// (stage.rs:201-268 reads AuthorityTransferred only), no binding is selected and the arm is not
/// ENSv2. Today's stage keys the transfers by name: the event's name, else the latest named
/// event of its resource and family, else the active surface of its node. The families key them
/// by node, which is the name's namehash, so they differ only for an unnamed transfer whose
/// resource another name's events carry, or whose node has no active surface: the shadow counts
/// it and today's stage does not, and the comparison fails rather than passing silently.
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
    .fetch_all(pool)
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
    .fetch_all(pool)
    .await
    .context("failed to load registry owner events")?;
    for (namespace, node, row) in events {
        if let (Some(state), Some(event)) = (
            nodes.get_mut(&(namespace, node)),
            OwnerEvent::from_row(&row),
        ) {
            state.owner_events.push(event);
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
    fn from_row(row: &Value) -> Option<Self> {
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
/// both from the served name row (permission_resources.rs:38-42 joins the name row being built).
#[derive(Clone, Debug, Default)]
pub struct NameAttribution {
    pub current_resource_id: Option<String>,
    pub authority_arm: Option<String>,
}

/// What the permission summary serves for one resource's registry binding
/// (permission_resources.rs:71-79).
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

/// The registry binding of every resource the observations reach. Today's builder keys the
/// observations by `COALESCE(logical_name_id, resource_id)`, takes the latest per key, moves a
/// name-addressed AuthorityTransferred or SubregistryChanged to the name's current resource under
/// arm ens_v1 or basenames, then takes the latest per resource. Step 2 keeps one row per that
/// key with the resource it reaches under the name's ENSv1 or Basenames binding. The served
/// move reads the served name row's selection, so a name `names` carries is moved by that
/// selection (a name whose arm is ENSv2 stays on the event's own resource), and any other row
/// reaches its stored target.
pub fn registry_bindings(
    observations: &[Observation],
    names: &BTreeMap<String, NameAttribution>,
) -> BTreeMap<String, RegistryBinding> {
    registry_bindings_in(observations, names, &EventOrder::Canonical)
}

/// `registry_bindings` with the latest observation per resource taken in `order`: the harness
/// reads it in today's (block, transaction, log, generated id) order for its same-block check.
pub fn registry_bindings_in(
    observations: &[Observation],
    names: &BTreeMap<String, NameAttribution>,
    order: &EventOrder,
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
        if order
            .lateral(&observation.position, &entry.position)
            .is_gt()
        {
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

/// Every registry-binding observation of the chain. A resource's binding can come from a row
/// keyed by any name whose target it is, which no column indexes, so the read takes the table
/// whole; it serves the harness only.
pub async fn load_observations(pool: &PgPool, chain_id: &str) -> Result<Vec<Observation>> {
    let rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.registry.observations */ SELECT to_jsonb(observation)
         FROM bigname_phase.project_registry_binding_observation observation
         WHERE observation.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to load registry-binding observations")?;
    Ok(rows.iter().filter_map(Observation::from_row).collect())
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
}
