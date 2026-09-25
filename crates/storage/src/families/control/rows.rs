//! The family rows the control readers read, decoded from `to_jsonb(row)` so that the column
//! types of the step 2 migrations (20260926100100 and 20260926100400) need no per-column
//! mapping. Every row keeps the canonical position of the event that last wrote it.
use serde_json::Value;

use super::position::Position;

pub(crate) fn text(row: &Value, field: &str) -> Option<String> {
    match row.get(field)? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(crate) fn lower(row: &Value, field: &str) -> Option<String> {
    text(row, field).map(|value| value.to_ascii_lowercase())
}

pub(crate) fn flag(row: &Value, field: &str) -> Option<bool> {
    row.get(field).and_then(Value::as_bool)
}

pub(crate) fn json(row: &Value, field: &str) -> Value {
    row.get(field).cloned().unwrap_or(Value::Null)
}

/// One retained lifecycle event of `project_lifecycle_event`: an event of the six kinds the
/// authority-admitted readers consume, kept per key and never pruned, with the name the adapter
/// emitted (`original_logical_name_id`) beside the name step 2 decoded.
#[derive(Clone, Debug)]
pub struct LifecycleEvent {
    pub state_kind: String,
    pub state_key: String,
    pub position: Position,
    pub event_kind: String,
    pub original_logical_name_id: Option<String>,
    pub decoded_logical_name_id: Option<String>,
    pub resource_id: Option<String>,
    pub source_family: String,
    /// `COALESCE(NULLIF(after_state ->> 'authority_kind', ''), 'registrar')`, the kind the
    /// admission reads (authority_events.sql).
    pub authority_kind: String,
    /// `after_state ->> 'authority_kind'` as the row stores it, null when absent: the kind the
    /// served name block reports (name_current/build.sql:30).
    pub authority_kind_raw: Option<String>,
    /// `after_state ->> 'authority_key'`.
    pub authority_key: Option<String>,
    pub transaction_hash: Option<String>,
    pub to_address: Option<String>,
    pub namehash: Option<String>,
    pub registrant: Option<String>,
    pub before_registrant: Option<String>,
    pub expiry: Value,
    pub expiry_seconds: Option<i64>,
    pub status: Option<String>,
    pub released_at: Value,
    pub source_event: Option<String>,
    pub derived_from: Option<String>,
    pub terminal_reason: Option<String>,
    pub revived_from_expiry: Option<bool>,
    pub state_derived: Option<bool>,
    pub surface_materialization: Option<bool>,
    pub registrar_surface_snapshot: Option<bool>,
    pub original_registered_at: Option<i64>,
    pub owner_getter: Option<String>,
    pub owner_word_unmasked: Option<bool>,
    pub registry_owner: Option<String>,
}

impl LifecycleEvent {
    /// Decode a `to_jsonb(project_lifecycle_event)` row.
    pub fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            state_kind: text(row, "state_kind")?,
            state_key: text(row, "state_key")?,
            position: Position::of_row(row)?,
            event_kind: text(row, "event_kind")?,
            original_logical_name_id: text(row, "original_logical_name_id"),
            decoded_logical_name_id: text(row, "decoded_logical_name_id"),
            resource_id: text(row, "resource_id"),
            source_family: text(row, "source_family")?,
            authority_kind: text(row, "authority_kind")
                .filter(|kind| !kind.is_empty())
                .unwrap_or_else(|| "registrar".into()),
            authority_kind_raw: text(row, "authority_kind"),
            authority_key: text(row, "authority_key"),
            transaction_hash: text(row, "transaction_hash"),
            to_address: lower(row, "to_address"),
            namehash: lower(row, "namehash"),
            registrant: lower(row, "registrant"),
            before_registrant: lower(row, "before_registrant"),
            expiry: json(row, "expiry"),
            expiry_seconds: row.get("expiry_seconds").and_then(Value::as_i64),
            status: text(row, "status"),
            released_at: json(row, "released_at"),
            source_event: text(row, "source_event"),
            derived_from: text(row, "derived_from"),
            terminal_reason: text(row, "terminal_reason"),
            revived_from_expiry: flag(row, "revived_from_expiry"),
            state_derived: flag(row, "state_derived"),
            surface_materialization: flag(row, "surface_materialization"),
            registrar_surface_snapshot: flag(row, "registrar_surface_snapshot"),
            original_registered_at: row.get("original_registered_at").and_then(Value::as_i64),
            owner_getter: lower(row, "owner_getter"),
            owner_word_unmasked: flag(row, "owner_word_unmasked"),
            registry_owner: lower(row, "registry_owner"),
        })
    }

    /// The ENSv2 registry's path-expiry release (build.sql:324): source event
    /// RegistryPathExpired, derived from interpreter state, terminal reason
    /// registry_name_binding_expired.
    pub fn is_path_expiry(&self) -> bool {
        self.event_kind == "RegistrationReleased"
            && self.source_event.as_deref() == Some("RegistryPathExpired")
            && self.derived_from.as_deref() == Some("interpreter_state")
            && self.terminal_reason.as_deref() == Some("registry_name_binding_expired")
    }

    /// The authority arm the event's family belongs to (authority_events.sql:19-23).
    pub fn family_arm(&self) -> Option<&'static str> {
        family_arm(&self.source_family)
    }

    pub fn is_v2_family(&self) -> bool {
        matches!(
            self.source_family.as_str(),
            "ens_v2_root_l1" | "ens_v2_registry_l1" | "ens_v2_registrar_l1"
        )
    }

    /// Whether `after_state -> 'expiry'` is a JSON number (build.sql:521-527).
    pub fn numeric_expiry(&self) -> bool {
        self.expiry.is_number()
    }
}

pub fn family_arm(source_family: &str) -> Option<&'static str> {
    if source_family.starts_with("ens_v1_") {
        Some("ens_v1")
    } else if source_family.starts_with("ens_v2_") {
        Some("ens_v2")
    } else if source_family.starts_with("basenames_") {
        Some("basenames")
    } else {
        None
    }
}

/// A membership maximum of a key state or triple summary: the position of the event that set it
/// and the fields the reducer kept with it (step 2 families/lifecycle.rs, `maxima`).
#[derive(Clone, Debug)]
pub struct Mark {
    pub position: Position,
    pub detail: Value,
}

impl Mark {
    fn of(row: &Value, field: &str) -> Option<Self> {
        let detail = row.get(field).filter(|value| value.is_object())?.clone();
        Some(Self {
            position: Position::from_json(detail.get("position")?)?,
            detail,
        })
    }

    pub fn kind(&self) -> Option<&str> {
        self.detail.get("kind").and_then(Value::as_str)
    }
}

/// The membership maxima of one lifecycle key: a resource's key state
/// (`project_lifecycle_key_state`) or a triple's summary (`project_lifecycle_triple_summary`).
/// `last_revival` exists on key states only.
#[derive(Clone, Debug, Default)]
pub struct Maxima {
    pub last_grant: Option<Mark>,
    pub last_reservation: Option<Mark>,
    pub last_active: Option<Mark>,
    pub last_release_any: Option<Mark>,
    pub last_path_expiry: Option<Mark>,
    pub last_explicit_release: Option<Mark>,
    pub last_renewal: Option<Mark>,
    pub last_revival: Option<Mark>,
    pub last_expiry_changed: Option<Mark>,
}

impl Maxima {
    pub fn from_row(row: &Value) -> Self {
        Self {
            last_grant: Mark::of(row, "last_grant"),
            last_reservation: Mark::of(row, "last_reservation"),
            last_active: Mark::of(row, "last_active"),
            last_release_any: Mark::of(row, "last_release_any"),
            last_path_expiry: Mark::of(row, "last_path_expiry"),
            last_explicit_release: Mark::of(row, "last_explicit_release"),
            last_renewal: Mark::of(row, "last_renewal"),
            last_revival: Mark::of(row, "last_revival"),
            last_expiry_changed: Mark::of(row, "last_expiry_changed"),
        }
    }
}

/// An F1 binding candidate (`project_binding_candidate`): every binding of the name, selected
/// or not, with the facts of the SurfaceBound that opened it.
#[derive(Clone, Debug)]
pub struct BindingCandidate {
    pub surface_binding_id: String,
    pub logical_name_id: String,
    pub authority_arm: String,
    pub resource_id: String,
    pub canonicality_state: Option<String>,
    pub surface_namehash: Option<String>,
    /// The binding's place: its block and the transaction and log its provenance records, with
    /// the binding id as the identity (stage.rs, `registry_only_handoffs`).
    pub block_number: i64,
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
    pub state_derived: Option<bool>,
    pub authority_kind: Option<String>,
    /// The SurfaceBound's after-state authority key.
    pub authority_key: Option<String>,
    /// The owner the SurfaceBound reports to the served control block (build.sql:650-671),
    /// positioned at `surface_bound_position`.
    pub bound_owner: Option<String>,
    pub registry_only: bool,
    pub predecessor_resource_id: Option<String>,
    pub predecessor_position: Option<Value>,
    pub lease_resource_id: Option<String>,
    pub wrapped_registrar_resource_id: Option<String>,
    pub node: Option<String>,
    pub transaction_hash: Option<String>,
    pub emitting_address: Option<String>,
    pub surface_bound_position: Option<Position>,
}

impl BindingCandidate {
    pub(crate) fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            surface_binding_id: text(row, "surface_binding_id")?,
            logical_name_id: text(row, "logical_name_id")?,
            authority_arm: text(row, "authority_arm").unwrap_or_default(),
            resource_id: text(row, "resource_id")?,
            canonicality_state: text(row, "canonicality_state"),
            surface_namehash: lower(row, "surface_namehash"),
            block_number: row.get("block_number").and_then(Value::as_i64)?,
            transaction_index: row.get("transaction_index").and_then(Value::as_i64),
            log_index: row.get("log_index").and_then(Value::as_i64),
            state_derived: flag(row, "state_derived"),
            authority_kind: text(row, "authority_kind"),
            authority_key: text(row, "authority_key"),
            bound_owner: lower(row, "bound_owner"),
            registry_only: flag(row, "registry_only").unwrap_or(false),
            predecessor_resource_id: text(row, "predecessor_resource_id"),
            predecessor_position: row
                .get("predecessor_position")
                .filter(|value| value.is_object())
                .cloned(),
            lease_resource_id: text(row, "lease_resource_id"),
            wrapped_registrar_resource_id: text(row, "wrapped_registrar_resource_id"),
            node: lower(row, "node"),
            transaction_hash: text(row, "transaction_hash"),
            emitting_address: lower(row, "emitting_address"),
            surface_bound_position: row
                .get("surface_bound_position")
                .and_then(Position::from_json),
        })
    }

    /// A NameWrapper binding: step 2 records the transaction, the emitter, the wrapped registrar
    /// lease and the node only for a SurfaceBound from ens_v1_wrapper_l1
    /// (families/identity.rs, `candidate_row`), so any one of them marks it. A NameWrapper
    /// SurfaceBound with none of the four is not recognised (step 2 retention follow-up: keep
    /// the SurfaceBound's source family on the candidate).
    pub fn is_wrapper(&self) -> bool {
        self.transaction_hash.is_some()
            || self.emitting_address.is_some()
            || self.wrapped_registrar_resource_id.is_some()
            || self.node.is_some()
    }

    /// The order stage.rs compares candidates in: block, transaction and log with a missing one
    /// read as -1, then the binding id. This is not the D12 event order: two candidates at the
    /// same place break the tie by binding id, not by their SurfaceBound identities, as the
    /// served stage does (fixture in admission.rs). The D12 claim covers event-derived latest
    /// selections only.
    pub fn order(&self) -> (i64, i64, i64, &str) {
        (
            self.block_number,
            self.transaction_index.unwrap_or(-1),
            self.log_index.unwrap_or(-1),
            self.surface_binding_id.as_str(),
        )
    }
}

/// F2b, one NameWrapper resource (`project_wrapper_state`).
#[derive(Clone, Debug, Default)]
pub struct WrapperRow {
    pub resource_id: String,
    pub logical_name_id: Option<String>,
    /// `wrapped`, `emancipated` or `locked`; null for any other value the event carried.
    pub wrapper_state: Option<String>,
    pub fuses: Option<i64>,
    /// Whether a PermissionScopeChanged ever set the state.
    pub has_modifier: bool,
    /// The expiry word, 0 to 2^64 - 1, kept as text so no value is truncated.
    pub expiry_seconds: Option<String>,
    pub has_expiry: bool,
    /// Whether the newest wrapper lifecycle event (mint, NameUnwrapped, holder grant or holder
    /// revoke) leaves the resource unwrapped.
    pub lifecycle_unwrapped: Option<bool>,
}

impl WrapperRow {
    pub(crate) fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            resource_id: text(row, "resource_id")?,
            logical_name_id: text(row, "logical_name_id"),
            wrapper_state: text(row, "wrapper_state"),
            fuses: row.get("fuses").and_then(Value::as_i64),
            has_modifier: row
                .get("wrapper_state_position")
                .is_some_and(Value::is_object),
            expiry_seconds: text(row, "expiry_seconds"),
            has_expiry: row.get("expiry_position").is_some_and(Value::is_object),
            lifecycle_unwrapped: flag(row, "lifecycle_unwrapped"),
        })
    }
}
