//! Whether the families hold, for a name or a resource, exactly the retained facts the
//! publication-visible log gives under step 2's retention rules (Pro Q3 on 6c8bdf8b). The name
//! excuses rebuild each retained lifecycle event from its log row and check each identity fact
//! against the log, but they start from what the families hold: a retained row the families
//! lack is read by neither the canonical rebuild nor today's-order reread, so a missing decisive
//! event could make both agree with a wrong shadow value. So before any excuse the expected sets
//! are derived from the log, and the families must hold them exactly, in both directions:
//!
//! - the binding candidates: every publication-visible surface binding of the name
//!   (identity.rs `candidates`), with its opening SurfaceBound, the NameWrapper lease, node,
//!   transaction and emitter that SurfaceBound recorded, and its registry-only handoff
//!   (identity.rs `handoff`). A handoff lease other than the predecessor's resource was set by a
//!   later registrar grant (identity/lease.rs), which this check does not rebuild, so such a
//!   candidate refuses every excuse of the name;
//! - the epoch starts: the canonically latest AuthorityEpochChanged of the name per arm
//!   (identity.rs `names`);
//! - the triples: each null-resource ENSv2 non-transfer event's triple (a summary) and each
//!   resource-bearing ENSv2 grant or reservation's triple with the canonically latest one's
//!   resource and position (the association; lifecycle.rs `association`);
//! - the key states: every resource of the name's scope with a retained non-transfer event;
//! - the retained lifecycle events: every event of the six retained kinds whose key
//!   (lifecycle.rs `state_key`) falls in the name's scope, which the loader builds from the
//!   candidates, the selection, the association targets and the key states the name last named
//!   (load.rs:137-160, :306-338), each filed under that key, since the reader partitions the
//!   events by it (membership.rs:66-99). The decoded name the family also keeps is not read by
//!   this comparison, so its corruption is not detected;
//! - the node's owner-setting registry events and its old-record and first-current-record
//!   facts (registry.rs `registry_nodes`).
//!
//! The older admitted epochs the families do not keep (one start per arm) are not expected
//! here, so a served value that needs one stays a mismatch.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_storage::families::control::{
    lifecycle::NameFacts, position::Position, rows::BindingCandidate,
};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{LogEvent, epoch_arm, published_where, raw_lower, raw_text, reported_control_owner};

/// The kinds step 2 retains as lifecycle events (crates/project/src/families/lifecycle.rs:25-32).
const RETAINED: [&str; 6] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "RegistrationReserved",
    "ExpiryChanged",
    "TokenControlTransferred",
];
/// The families a null-resource event is kept under its triple for (lifecycle.rs:33-37).
const V2_FAMILIES: [&str; 3] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
];
/// The registries whose owner-setting events a node keeps (registry.rs:23, :37-47).
const V1_REGISTRIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];
const TRANSFER: &str = "TokenControlTransferred";

fn sql_list(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("'{value}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One publication-visible surface binding: step 2 keeps each as a binding candidate.
pub struct LogBinding {
    pub id: String,
    pub name: String,
    pub resource: String,
    pub arm: String,
    pub block_number: i64,
    /// The transaction and log index its provenance records, both or neither
    /// (identity.rs `provenance_index`).
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
}

/// The publication-visible log a chunk's retention checks read: the names' surface bindings,
/// their named retained, epoch and SurfaceBound events, every retained event on a resource any
/// of them can reach, and the owner-setting registry events of their nodes.
#[derive(Default)]
pub struct RetentionLog {
    pub bindings: Vec<LogBinding>,
    pub events: BTreeMap<String, LogEvent>,
}

/// The resources the families' facts of a name reach.
fn family_resources(facts: &NameFacts) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for candidate in &facts.candidates {
        out.insert(candidate.resource_id.clone());
        out.extend(candidate.wrapped_registrar_resource_id.clone());
        out.extend(candidate.predecessor_resource_id.clone());
        out.extend(candidate.lease_resource_id.clone());
    }
    out.extend(facts.input.selection.resource_id.clone());
    out.extend(
        facts
            .triples
            .iter()
            .filter_map(|triple| triple.target.clone()),
    );
    out.extend(facts.key_states.keys().cloned());
    out
}

fn namespace_of(name: &str) -> &str {
    name.split_once(':').map_or("", |(namespace, _)| namespace)
}

impl RetentionLog {
    pub async fn load(
        pool: &PgPool,
        chain: &str,
        target: i64,
        facts: &BTreeMap<String, NameFacts>,
    ) -> Result<Self> {
        if facts.is_empty() {
            return Ok(Self::default());
        }
        let names: Vec<String> = facts.keys().cloned().collect();
        let bindings = load_bindings(pool, chain, target, &names).await?;
        let mut kinds = RETAINED.to_vec();
        kinds.extend(["AuthorityEpochChanged", "SurfaceBound"]);
        let mut events = published_where(
            pool,
            chain,
            target,
            &format!(
                "event.logical_name_id = ANY($2) AND event.event_kind IN ({})",
                sql_list(&kinds)
            ),
            &names,
        )
        .await?;
        let mut resources: BTreeSet<String> = facts.values().flat_map(family_resources).collect();
        resources.extend(bindings.iter().map(|binding| binding.resource.clone()));
        for event in events.values() {
            resources.extend(event.resource.clone());
            resources.extend(raw_text(&event.after, "wrapped_registrar_resource_id"));
        }
        let resources: Vec<String> = resources.into_iter().collect();
        events.extend(
            published_where(
                pool,
                chain,
                target,
                &format!(
                    "event.resource_id = ANY($2::uuid[]) AND event.event_kind IN ({})",
                    sql_list(&RETAINED)
                ),
                &resources,
            )
            .await?,
        );
        let nodes: Vec<String> = facts
            .values()
            .map(|facts| {
                format!(
                    "{}|{}",
                    namespace_of(&facts.input.logical_name_id),
                    facts.input.namehash.to_ascii_lowercase()
                )
            })
            .collect();
        events.extend(
            published_where(
                pool,
                chain,
                target,
                &format!(
                    "event.event_kind IN ('SubregistryChanged', 'AuthorityTransferred')
                     AND event.source_family IN ({})
                     AND event.namespace || '|' || lower(COALESCE(
                             NULLIF(event.after_state ->> 'child_node', ''),
                             event.after_state ->> 'node')) = ANY($2)",
                    sql_list(&V1_REGISTRIES)
                ),
                &nodes,
            )
            .await?,
        );
        Ok(Self { bindings, events })
    }
}

/// The publication-visible surface bindings of `names`: canonical, at the canonical lineage's
/// hash for their block, at or below the target (identity.rs:189-207 reads each block's).
async fn load_bindings(
    pool: &PgPool,
    chain: &str,
    target: i64,
    names: &[String],
) -> Result<Vec<LogBinding>> {
    type Row = (String, String, String, String, i64, Value);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT binding.surface_binding_id::text, binding.logical_name_id,
                binding.resource_id::text, binding.authority_arm, binding.block_number,
                binding.provenance
         FROM surface_bindings binding
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_number = binding.block_number
          AND lineage.block_hash = binding.block_hash
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         WHERE binding.chain_id = $1 AND binding.logical_name_id = ANY($2)
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.block_number <= $3",
    )
    .bind(chain)
    .bind(names)
    .bind(target)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, resource, arm, block_number, provenance)| {
            let index =
                |field: &str| raw_text(&provenance, field).and_then(|text| text.parse().ok());
            let (transaction_index, log_index) =
                match (index("transaction_index"), index("log_index")) {
                    (Some(transaction), Some(log)) => (Some(transaction), Some(log)),
                    _ => (None, None),
                };
            LogBinding {
                id,
                name,
                resource,
                arm,
                block_number,
                transaction_index,
                log_index,
            }
        })
        .collect())
}

/// Where step 2 keeps a retained lifecycle event: `("resource", resource)`, or
/// `("triple", [name, registry, token])` for a null-resource ENSv2 event of a name
/// (lifecycle.rs:63-84). None for an event it does not retain.
pub fn retained_key(event: &LogEvent) -> Option<(&'static str, String)> {
    if !RETAINED.contains(&event.kind.as_str()) {
        return None;
    }
    if let Some(resource) = &event.resource {
        return Some(("resource", resource.clone()));
    }
    if !V2_FAMILIES.contains(&event.family.as_str()) {
        return None;
    }
    Some(("triple", triple(event)?))
}

/// The triple key text of an ENSv2 event (lifecycle.rs:63-71, `StateKey::text`).
fn triple(event: &LogEvent) -> Option<String> {
    let after = &event.after;
    let registry = raw_text(after, "registry_contract_instance_id")
        .or_else(|| event.emitter.clone())
        .or_else(|| raw_text(after, "registry"))
        .unwrap_or_default();
    let token = raw_text(after, "token_id").unwrap_or_default();
    Some(json!([event.name.clone()?, registry, token]).to_string())
}

/// A binding candidate as step 2 derives it from the log.
struct Expected<'a> {
    binding: &'a LogBinding,
    /// Its place in the candidate order (identity.rs `binding_position`, `candidate_order`).
    position: Position,
    opening: Option<&'a LogEvent>,
    registry_only: bool,
    predecessor: Option<&'a LogBinding>,
}

fn expected_candidates<'a>(
    name: &str,
    named: &[&'a LogEvent],
    bindings: &'a [LogBinding],
) -> Vec<Expected<'a>> {
    let mut out: Vec<Expected<'a>> = bindings
        .iter()
        .filter(|binding| binding.name == name)
        .map(|binding| {
            // identity.rs `opening_event`: the block's SurfaceBound of the name and resource at
            // the provenance's transaction and log, naming this binding if it names one.
            let opening = named
                .iter()
                .filter(|event| {
                    event.kind == "SurfaceBound"
                        && event.resource.as_deref() == Some(binding.resource.as_str())
                        && event.position.block_number == binding.block_number
                        && event.position.transaction_index == binding.transaction_index
                        && event.position.log_index == binding.log_index
                        && raw_text(&event.after, "surface_binding_id")
                            .is_none_or(|id| id == binding.id)
                })
                .min_by(|left, right| left.position.cmp(&right.position))
                .copied();
            let position = opening.map_or_else(
                || Position {
                    block_number: binding.block_number,
                    transaction_index: binding.transaction_index,
                    log_index: binding.log_index,
                    event_identity: format!("binding:{}", binding.id),
                },
                |event| event.position.clone(),
            );
            // identity.rs:215-225, :283-307: a registry-only epoch of the name and resource in
            // the binding's block or a later one hands the binding off.
            let registry_only = named.iter().any(|event| {
                event.kind == "AuthorityEpochChanged"
                    && raw_text(&event.after, "authority_kind").as_deref() == Some("registry_only")
                    && event.resource.as_deref() == Some(binding.resource.as_str())
                    && event.position.block_number >= binding.block_number
            });
            Expected {
                binding,
                position,
                opening,
                registry_only,
                predecessor: None,
            }
        })
        .collect();
    // identity.rs `handoff`: the latest earlier candidate of the name and arm.
    let orders: Vec<(Position, String, String)> = out
        .iter()
        .map(|candidate| {
            (
                candidate.position.clone(),
                candidate.binding.id.clone(),
                candidate.binding.arm.clone(),
            )
        })
        .collect();
    for index in 0..out.len() {
        if !out[index].registry_only {
            continue;
        }
        let own = (&orders[index].0, &orders[index].1);
        let predecessor = orders
            .iter()
            .enumerate()
            .filter(|(_, (position, id, arm))| *arm == orders[index].2 && (position, id) < own)
            .max_by(|(_, left), (_, right)| (&left.0, &left.1).cmp(&(&right.0, &right.1)))
            .map(|(other, _)| out[other].binding);
        out[index].predecessor = predecessor;
    }
    out
}

/// Why the family candidate differs from its derivation, None when it does not.
fn candidate_differs(candidate: &BindingCandidate, expected: &Expected<'_>) -> Option<String> {
    let binding = expected.binding;
    let wrapper = expected
        .opening
        .filter(|event| event.family == "ens_v1_wrapper_l1");
    let opening_after = expected.opening.map(|event| &event.after);
    let predecessor = expected
        .predecessor
        .map(|predecessor| predecessor.resource.clone());
    let checks = [
        ("name", candidate.logical_name_id == binding.name),
        ("resource", candidate.resource_id == binding.resource),
        ("arm", candidate.authority_arm == binding.arm),
        (
            "position",
            candidate.block_number == expected.position.block_number
                && candidate.transaction_index == expected.position.transaction_index
                && candidate.log_index == expected.position.log_index,
        ),
        (
            "surface_bound_position",
            candidate.surface_bound_position.as_ref()
                == expected.opening.map(|event| &event.position),
        ),
        (
            "state_derived",
            candidate.state_derived
                == opening_after
                    .and_then(|after| after.get("state_derived"))
                    .and_then(Value::as_bool),
        ),
        (
            "authority_kind",
            candidate.authority_kind
                == opening_after.and_then(|after| raw_text(after, "authority_kind")),
        ),
        (
            "authority_key",
            candidate.authority_key
                == opening_after.and_then(|after| raw_text(after, "authority_key")),
        ),
        (
            "bound_owner",
            candidate.bound_owner == opening_after.and_then(reported_control_owner),
        ),
        (
            "wrapped_registrar_resource_id",
            candidate.wrapped_registrar_resource_id
                == wrapper
                    .and_then(|event| raw_text(&event.after, "wrapped_registrar_resource_id")),
        ),
        (
            "node",
            candidate.node == wrapper.and_then(|event| raw_lower(&event.after, "node")),
        ),
        (
            "transaction_hash",
            candidate.transaction_hash == wrapper.and_then(|event| event.transaction_hash.clone()),
        ),
        (
            "emitting_address",
            candidate.emitting_address == wrapper.and_then(|event| event.emitter.clone()),
        ),
        (
            "registry_only",
            candidate.registry_only == expected.registry_only,
        ),
        (
            "predecessor_resource_id",
            candidate.predecessor_resource_id == predecessor,
        ),
        // A lease a later registrar grant replaced (identity/lease.rs) is not rebuilt here.
        (
            "lease_resource_id",
            candidate.lease_resource_id == predecessor,
        ),
    ];
    checks
        .iter()
        .find(|(_, holds)| !holds)
        .map(|(field, _)| format!("candidate {} {field}", binding.id))
}

/// Why the families' facts of the name are not exactly what the log gives under step 2's
/// retention rules, None when they are.
pub fn name_differs(facts: &NameFacts, log: &RetentionLog) -> Option<String> {
    let name = facts.input.logical_name_id.as_str();
    let named: Vec<&LogEvent> = log
        .events
        .values()
        .filter(|event| event.name.as_deref() == Some(name))
        .collect();

    // Binding candidates.
    let expected = expected_candidates(name, &named, &log.bindings);
    let family: BTreeMap<&str, &BindingCandidate> = facts
        .candidates
        .iter()
        .map(|candidate| (candidate.surface_binding_id.as_str(), candidate))
        .collect();
    let ids: BTreeSet<&str> = expected
        .iter()
        .map(|candidate| candidate.binding.id.as_str())
        .collect();
    if family.len() != facts.candidates.len()
        || family.keys().copied().collect::<BTreeSet<_>>() != ids
    {
        return Some("candidates".into());
    }
    for candidate in &expected {
        if let Some(reason) = candidate_differs(family[candidate.binding.id.as_str()], candidate) {
            return Some(reason);
        }
    }

    // Epoch starts: the latest AuthorityEpochChanged of the name per arm.
    let mut starts: BTreeMap<&str, &Position> = BTreeMap::new();
    for event in named
        .iter()
        .filter(|event| event.kind == "AuthorityEpochChanged")
    {
        let start = starts
            .entry(epoch_arm(&event.family))
            .or_insert(&event.position);
        if event.position > **start {
            *start = &event.position;
        }
    }
    let family_starts: BTreeMap<&str, Option<Position>> = facts
        .authority_starts
        .as_object()
        .into_iter()
        .flatten()
        .map(|(arm, start)| (arm.as_str(), Position::from_json(start)))
        .collect();
    let expected_starts: BTreeMap<&str, Option<Position>> = starts
        .into_iter()
        .map(|(arm, position)| (arm, Some(position.clone())))
        .collect();
    if family_starts != expected_starts {
        return Some("epoch starts".into());
    }

    // Triples: summaries of null-resource non-transfer events, and associations.
    let mut triples: BTreeMap<String, Option<&LogEvent>> = BTreeMap::new();
    for event in &named {
        if !V2_FAMILIES.contains(&event.family.as_str()) {
            continue;
        }
        let Some(key) = triple(event) else { continue };
        if event.resource.is_none()
            && RETAINED.contains(&event.kind.as_str())
            && event.kind != TRANSFER
        {
            triples.entry(key).or_insert(None);
        } else if event.resource.is_some()
            && matches!(
                event.kind.as_str(),
                "RegistrationGranted" | "RegistrationReserved"
            )
        {
            let winner = triples.entry(key).or_insert(None);
            if winner.is_none_or(|current| event.position > current.position) {
                *winner = Some(event);
            }
        }
    }
    let family_triples: BTreeMap<String, (Option<&String>, Option<&Position>)> = facts
        .triples
        .iter()
        .map(|triple| {
            (
                triple.state_key(),
                (triple.target.as_ref(), triple.target_position.as_ref()),
            )
        })
        .collect();
    let expected_triples: BTreeMap<String, (Option<&String>, Option<&Position>)> = triples
        .iter()
        .map(|(key, winner)| {
            (
                key.clone(),
                (
                    winner.and_then(|event| event.resource.as_ref()),
                    winner.map(|event| &event.position),
                ),
            )
        })
        .collect();
    if family_triples.len() != facts.triples.len() || family_triples != expected_triples {
        return Some("triples".into());
    }

    // The resource scope, with the key states the name last named.
    let mut resources: BTreeSet<&str> = BTreeSet::new();
    for candidate in &expected {
        resources.insert(candidate.binding.resource.as_str());
        if let Some(predecessor) = candidate.predecessor {
            resources.insert(predecessor.resource.as_str());
        }
    }
    for candidate in &facts.candidates {
        resources.extend(candidate.wrapped_registrar_resource_id.as_deref());
    }
    resources.extend(facts.input.selection.resource_id.as_deref());
    resources.extend(
        triples
            .values()
            .flatten()
            .filter_map(|event| event.resource.as_deref()),
    );
    let mut last_named: BTreeMap<&str, &LogEvent> = BTreeMap::new();
    for event in log.events.values() {
        let (Some(resource), Some(_)) = (event.resource.as_deref(), event.name.as_deref()) else {
            continue;
        };
        if !RETAINED.contains(&event.kind.as_str()) || event.kind == TRANSFER {
            continue;
        }
        let last = last_named.entry(resource).or_insert(event);
        if event.position > last.position {
            *last = event;
        }
    }
    resources.extend(
        last_named
            .iter()
            .filter(|(_, event)| event.name.as_deref() == Some(name))
            .map(|(resource, _)| *resource),
    );

    // Key states: a scope resource with a retained non-transfer event.
    let states: BTreeSet<&str> = log
        .events
        .values()
        .filter(|event| RETAINED.contains(&event.kind.as_str()) && event.kind != TRANSFER)
        .filter_map(|event| event.resource.as_deref())
        .filter(|resource| resources.contains(resource))
        .collect();
    if facts
        .key_states
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != states
    {
        return Some("key states".into());
    }

    // Retained lifecycle events whose key falls in the scope, each filed under the key step 2
    // derives from its log row: the reader partitions the events by that key.
    let expected_events: BTreeMap<&str, (&str, String)> = log
        .events
        .iter()
        .filter_map(|(identity, event)| {
            let (kind, key) = retained_key(event)?;
            let in_scope = match kind {
                "resource" => resources.contains(key.as_str()),
                _ => triples.contains_key(&key),
            };
            in_scope.then_some((identity.as_str(), (kind, key)))
        })
        .collect();
    let family_events: BTreeMap<&str, (&str, String)> = facts
        .events
        .iter()
        .map(|event| {
            (
                event.position.event_identity.as_str(),
                (event.state_kind.as_str(), event.state_key.clone()),
            )
        })
        .collect();
    if family_events.len() != facts.events.len() || family_events != expected_events {
        return Some("retained events".into());
    }

    // The node's owner-setting registry events.
    let node = facts.input.namehash.to_ascii_lowercase();
    let owners: Vec<&LogEvent> = log
        .events
        .values()
        .filter(|event| {
            matches!(
                event.kind.as_str(),
                "SubregistryChanged" | "AuthorityTransferred"
            ) && V1_REGISTRIES.contains(&event.family.as_str())
                && event.namespace == namespace_of(name)
                && raw_lower(&event.after, "child_node")
                    .filter(|child| !child.is_empty())
                    .or_else(|| raw_lower(&event.after, "node"))
                    .as_deref()
                    == Some(node.as_str())
        })
        .collect();
    let role = |event: &LogEvent| raw_text(&event.after, "emitter_role");
    let holds = match &facts.registry_node {
        None => owners.is_empty(),
        Some(state) => {
            let family: BTreeSet<&str> = state
                .owner_events
                .iter()
                .map(|event| event.position.event_identity.as_str())
                .collect();
            let logged: BTreeSet<&str> = owners
                .iter()
                .map(|event| event.position.event_identity.as_str())
                .collect();
            family.len() == state.owner_events.len()
                && family == logged
                && state.has_old_record
                    == owners
                        .iter()
                        .any(|event| role(event).as_deref() == Some("registry_old"))
                && state.first_current_record_block
                    == owners
                        .iter()
                        .filter(|event| role(event).as_deref() == Some("registry"))
                        .map(|event| event.position.block_number)
                        .min()
        }
    };
    (!holds).then(|| "owner events".into())
}

/// Whether the families hold exactly the retained lifecycle events the log gives `resource`:
/// every publication-visible event of the retained kinds that carries it (lifecycle.rs:73-78).
pub async fn resource_events_hold(
    pool: &PgPool,
    chain: &str,
    target: i64,
    resource: &str,
    family: &BTreeSet<String>,
) -> Result<bool> {
    let logged = published_where(
        pool,
        chain,
        target,
        &format!(
            "event.resource_id = ANY($2::uuid[]) AND event.event_kind IN ({})",
            sql_list(&RETAINED)
        ),
        &[resource.to_owned()],
    )
    .await?;
    Ok(logged.keys().cloned().collect::<BTreeSet<_>>() == *family)
}
