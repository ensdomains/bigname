//! F2a with the F1 facts it reads: the shadow of `name_current.declared_summary.registration`
//! and `.control` (name_current/build.sql:16-133).
//!
//! The F1 selection is an input, not a recomputation: the reader takes the authority selection
//! the served row carries in `provenance.authority_selection` (build.sql:234-252), plus the
//! selected binding's registry-only handoff facts from `project_binding_candidate`. The name
//! authority selection is plan step 6's contract, so taking it as input keeps the lifecycle proof
//! apart from the selection proof.
//!
//! Membership (the registration candidate and the ENSv2 latest kind) reads the F2a maxima merged
//! across the key and the triples associated with it. Every other value reads the retained
//! events of the name through the authority admission, as the laterals of build.sql read
//! `project_authority_events`, ordered by the canonical order instead of the code's orderings.
mod admission;
mod control;
mod laterals;
mod load;
pub mod membership;
mod select;
mod served;
pub mod view;

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::{
    position::{EventOrder, Position, bound_of},
    registry::RegistryNode,
    rows::{BindingCandidate, LifecycleEvent, Maxima, WrapperRow},
};

pub use load::{load_name_facts, load_shadow_names};

/// The F1 selection outputs the admission reads (name_authority/build.sql:793-850), as the
/// served row's `provenance.authority_selection` carries them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthoritySelection {
    pub authority_arm: Option<String>,
    pub surface_binding_id: Option<String>,
    pub resource_id: Option<String>,
    /// `authority_epoch_start_position` as a three-part bound.
    pub epoch_start: Option<(i64, i64, i64)>,
    /// Whether an authority proof event exists, which switches the epoch bound on.
    pub has_proof: bool,
    pub unsupported_reason: Option<String>,
    pub ownerless_registry: bool,
    /// The selected binding is a released ENSv1 lease binding (`released_v1_tombstone`).
    pub released_tombstone: bool,
}

impl AuthoritySelection {
    /// Read from a served `name_current.provenance`.
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

    /// `COALESCE(selected_authority_arm, 'ens_v2') = 'ens_v2'` (build.sql:347).
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
/// wrapper mask uses (D6).
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
    /// The retained-event key of the triple (step 2 families/lifecycle.rs, `StateKey::text`).
    pub fn state_key(&self) -> String {
        Value::from(self.key.to_vec()).to_string()
    }

    /// The lifecycle key today's decoder gives an unassociated triple, `registry:token`, null
    /// when both are empty (v2_lifecycle_events.sql:21-23).
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
    /// `resources.provenance ->> 'authority_kind'` of the resources the read touches.
    pub resource_authority_kinds: BTreeMap<String, String>,
    /// `to_jsonb(block_timestamp)` per canonical block.
    pub block_timestamps: BTreeMap<i64, Value>,
    /// `to_jsonb(to_timestamp(seconds))` per registrar snapshot registration time.
    pub snapshot_timestamps: BTreeMap<i64, Value>,
    /// F1 `project_name_state.authority_start_positions`: the latest AuthorityEpochChanged per
    /// arm.
    pub authority_starts: Value,
    /// The name's ENSv1 or Basenames registry node (F2c), for the control block.
    pub registry_node: Option<RegistryNode>,
    /// The order the read takes its "latest" in: canonical in every read, today's generated-id
    /// order only in the harness's same-block counterfactual.
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

/// Evaluate one name from its loaded facts.
pub fn evaluate(facts: &NameFacts, clock: &Clock) -> ShadowName {
    served::evaluate(facts, clock)
}
