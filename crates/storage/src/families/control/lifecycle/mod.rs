//! F2a with the F1 facts it reads: the composed name's `declared_summary.registration` and
//! `.control`.
//!
//! The F1 selection is an input, not a recomputation: the reader takes the authority selection
//! the composed row carries in `provenance.authority_selection`, plus the
//! selected binding's registry-only handoff facts from `project_binding_candidate`. The name
//! authority selection is its own contract, so taking it as input keeps the lifecycle proof
//! apart from the selection proof. The one selection output the row does not carry, the fact
//! that decided a released ENSv2 tombstone, is found again from the retained events and the
//! binding candidates with their identity rows' open windows, by the rules that chose it
//! (`tombstone.rs`).
//!
//! Membership (the registration candidate and the ENSv2 latest kind) reads the F2a maxima merged
//! across the key and the triples associated with it, or the key's retained events folded again
//! when they hold another name's event or a reservation expired when written. Every other value
//! reads the retained events of the name through the authority admission (`admission.rs`),
//! ordered by the canonical order.
mod admission;
mod control;
mod laterals;
mod load;
pub mod membership;
mod select;
mod served;
mod tombstone;
pub mod view;

use std::{collections::BTreeMap, sync::Arc};

use serde_json::{Map, Value};

use super::{
    position::{EventOrder, Position, bound_of},
    registry::RegistryNode,
    rows::{self, BindingCandidate, LifecycleEvent, Maxima, WrapperRow},
};

pub use load::{load_name_facts, load_name_facts_on, namespace_of};

/// The F1 selection outputs the admission reads, as the
/// served row's `provenance.authority_selection` carries them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthoritySelection {
    pub authority_arm: Option<String>,
    pub surface_binding_id: Option<String>,
    pub resource_id: Option<String>,
    /// `authority_epoch_start_position` as a three-part bound.
    pub epoch_start: Option<(i64, i64, i64)>,
    /// Whether an authority proof event exists. Selection history only: no admission rule
    /// reads it.
    pub has_proof: bool,
    pub unsupported_reason: Option<String>,
    pub ownerless_registry: bool,
    /// The selected binding is a released ENSv1 lease binding (`released_v1_tombstone`).
    pub released_tombstone: bool,
}

impl AuthoritySelection {
    /// Read from a composed name's `provenance`.
    pub fn from_provenance(provenance: &Value) -> Self {
        let selection = provenance
            .get("authority_selection")
            .cloned()
            .unwrap_or(Value::Null);
        let text = |field: &str| {
            selection
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        Self {
            authority_arm: text("authority_arm"),
            surface_binding_id: text("surface_binding_id"),
            resource_id: text("resource_id"),
            epoch_start: selection.get("epoch_start_position").and_then(bound_of),
            has_proof: selection
                .get("proof_event_id")
                .is_some_and(|proof| !proof.is_null()),
            unsupported_reason: text("unsupported_reason"),
            ownerless_registry: selection.get("ownerless_registry") == Some(&Value::Bool(true)),
            released_tombstone: selection
                .pointer("/resource_authority_context/released_tombstone")
                .and_then(Value::as_str)
                == Some("ens_v1"),
        }
    }

    /// `COALESCE(selected_authority_arm, 'ens_v2') = 'ens_v2'`.
    pub fn is_v2(&self) -> bool {
        self.authority_arm.as_deref().unwrap_or("ens_v2") == "ens_v2"
    }
}

/// One name to read.
#[derive(Clone, Debug)]
pub struct NameInput {
    pub logical_name_id: String,
    /// The surface namehash, lower case.
    pub namehash: String,
    pub selection: AuthoritySelection,
}

/// The publication the read is for: its block and the block's timestamp, the clock every
/// wrapper mask uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Clock {
    pub block_number: i64,
    pub timestamp_seconds: i64,
}

/// A triple summary with the resource its association currently targets.
#[derive(Clone, Debug)]
pub struct TripleFacts {
    /// `[logical_name_id, registry identifier, token id]`.
    pub key: [String; 3],
    pub maxima: Maxima,
    pub target: Option<String>,
    /// The position of the grant or reservation that won the association.
    pub target_position: Option<Position>,
}

impl TripleFacts {
    /// The retained-event key of the triple (Project families/lifecycle.rs, `StateKey::text`).
    pub fn state_key(&self) -> String {
        Value::from(self.key.to_vec()).to_string()
    }

    /// The lifecycle key the decoder gives an unassociated triple, `registry:token`, null
    /// when both are empty.
    pub fn unassociated_key(&self) -> Option<String> {
        let key = format!("{}:{}", self.key[1], self.key[2]);
        (key != ":").then_some(key)
    }
}

/// Everything the lifecycle read of one name reads.
#[derive(Clone, Debug)]
pub struct NameFacts {
    pub input: NameInput,
    pub candidates: Vec<BindingCandidate>,
    /// Every binding candidate, of any name, of a registrar lease an unnamed retained event of
    /// the name sits on or that a NameWrapper candidate recorded as its lease: the staging passes
    /// name such an event over all of them.
    pub lease_candidates: Vec<BindingCandidate>,
    pub key_states: BTreeMap<String, Maxima>,
    pub triples: Vec<TripleFacts>,
    pub events: Vec<LifecycleEvent>,
    pub wrappers: BTreeMap<String, WrapperRow>,
    /// `to_jsonb(block_timestamp)` per canonical block. This and the snapshot timestamps are
    /// loaded once per batch and shared by every name of it.
    pub block_timestamps: Arc<BTreeMap<i64, Value>>,
    /// Each canonical block's timestamp in whole seconds, which a reservation's expiry is
    /// compared with.
    pub block_seconds: Arc<BTreeMap<i64, i64>>,
    /// `to_jsonb(to_timestamp(seconds))` per registrar snapshot registration time.
    pub snapshot_timestamps: Arc<BTreeMap<i64, Value>>,
    /// F1 `project_name_state.authority_start_positions`: the latest AuthorityEpochChanged per
    /// arm.
    pub authority_starts: Value,
    /// The name's ENSv1 or Basenames registry node (F2c), for the control block.
    pub registry_node: Option<RegistryNode>,
    /// The order the read takes its "latest" in: canonical in every read; the generated-id order
    /// is built only by the order tests.
    pub order: EventOrder,
}

/// The shadow of one name's registration and control blocks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShadowName {
    pub registration: Map<String, Value>,
    pub control: Map<String, Value>,
    /// What the read selected, for a mismatch report.
    pub trace: Map<String, Value>,
}

/// The registration fields the shadow reproduces, compared against the served row. `created_at`
/// is a whole-history minimum no family stores, so it is not listed.
pub const REGISTRATION_FIELDS: [&str; 11] = [
    "status",
    "authority_kind",
    "authority_key",
    "resource_id",
    "registrant",
    "expiry",
    "registered_at",
    "released_at",
    "latest_event_kind",
    "lapsed_registration/registrant",
    "lapsed_registration/released_at",
];

/// The control fields the shadow reproduces.
pub const CONTROL_FIELDS: [&str; 6] = [
    "status",
    "unsupported_reason",
    "expiry",
    "registrant",
    "registry_owner",
    "latest_event_kind",
];

/// Evaluate one name from its loaded facts. A fact the read cannot decide exactly, such as a
/// reservation's fractional expiry (`membership::expired_when_written`), fails it.
pub fn evaluate(facts: &NameFacts, clock: &Clock) -> anyhow::Result<ShadowName> {
    served::evaluate(facts, clock)
}

/// The released ENSv2 tombstone of a name whatever arm its input selection names: the
/// resource the name was last bound to under ENSv2 and the identity of the release that decided
/// it, none when an ENSv2 binding is open at `clock` or the latest lifecycle fact is not a
/// release. The composed name read decides the arm with it.
pub(crate) fn released_v2(
    facts: &NameFacts,
    clock: &Clock,
) -> anyhow::Result<Option<(String, String)>> {
    Ok(tombstone::released_fact(facts, clock)?.map(|tombstone| {
        (
            tombstone.resource,
            tombstone.event.position.event_identity.clone(),
        )
    }))
}

/// Whether `event` carries the name once the two staging passes have run: emitted with it, or
/// an unnamed `.eth` registrar row the passes give it. The
/// passes read the lease candidates only, not the selection.
pub(crate) fn staged_as_own(facts: &NameFacts, event: &rows::LifecycleEvent) -> bool {
    served::authority_of(facts).staged_name(event) == admission::StagedName::Ours
}
