use std::collections::HashMap;

use serde_json::Value;
use uuid::Uuid;

use crate::schema_v2::model::NormalizedEvent;

pub(super) type Position = (i64, i64, i64);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TargetKey {
    namespace: String,
    block_hash: String,
    transaction_hash: String,
    namehash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceFamily {
    Registry,
    Resolver,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceEvent {
    NameRegistered,
    NewOwner,
    Transfer,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RegistrationWindow {
    PriorLogsOnly,
    WholeTransaction,
}

#[derive(Clone, Debug)]
pub(super) struct EventFields {
    pub(super) position: Option<Position>,
    pub(super) family: SourceFamily,
    pub(super) source_event: SourceEvent,
    pub(super) target_namehash: Option<String>,
    pub(super) resource_id: Option<Uuid>,
    pub(super) registry_only: bool,
    pub(super) permission: bool,
    pub(super) owner: Option<String>,
    pub(super) subject: Option<String>,
    pub(super) scope: Option<Value>,
    pub(super) resource_scope: bool,
    pub(super) grant: bool,
    pub(super) revocation: bool,
    pub(super) current_registry_setup: bool,
}

impl EventFields {
    fn extract(event: &NormalizedEvent) -> Self {
        let state = &event.after_state;
        let source_event = match state.get("source_event").and_then(Value::as_str) {
            Some("NameRegistered") => SourceEvent::NameRegistered,
            Some("NewOwner") => SourceEvent::NewOwner,
            Some("Transfer") => SourceEvent::Transfer,
            Some(_) | None => SourceEvent::Other,
        };
        let scope = state.get("scope").cloned();
        let family = match event.source_family.as_str() {
            "ens_v1_registry_l1" | "basenames_base_registry" => SourceFamily::Registry,
            "ens_v1_resolver_l1" | "basenames_base_resolver" => SourceFamily::Resolver,
            _ => SourceFamily::Other,
        };
        Self {
            position: event_position(event),
            family,
            source_event,
            target_namehash: state
                .get("child_node")
                .or_else(|| state.get("node"))
                .or_else(|| state.get("namehash"))
                .and_then(Value::as_str)
                .map(|value| value.to_ascii_lowercase()),
            resource_id: event.resource_id,
            registry_only: state.get("authority_kind").and_then(Value::as_str)
                == Some("registry_only"),
            permission: event.event_kind == "PermissionChanged",
            owner: state
                .get("owner")
                .or_else(|| state.get("to"))
                .or_else(|| state.get("registrant"))
                .and_then(Value::as_str)
                .map(|value| value.to_ascii_lowercase()),
            subject: state
                .get("subject")
                .and_then(Value::as_str)
                .map(|value| value.to_ascii_lowercase()),
            resource_scope: scope
                .as_ref()
                .and_then(|scope| scope.get("kind"))
                .and_then(Value::as_str)
                == Some("resource"),
            scope,
            grant: state
                .get("grant_source")
                .is_some_and(|source| !source.is_null()),
            revocation: state
                .get("revocation_source")
                .is_some_and(|source| !source.is_null()),
            current_registry_setup: matches!(
                source_event,
                SourceEvent::NewOwner | SourceEvent::Transfer
            ) && family == SourceFamily::Registry
                && state.get("emitter_role").and_then(Value::as_str) == Some("registry"),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Registration {
    pub(super) event_index: usize,
    pub(super) key: TargetKey,
    pub(super) logical_name_id: String,
    pub(super) surface_known: bool,
    pub(super) resource_id: Uuid,
    pub(super) log_index: i64,
    pub(super) authority_key: Option<String>,
    pub(super) _emitter: String,
    pub(super) provisional_owner: String,
    pub(super) window: RegistrationWindow,
    /// Chain position of the registration event; the block number scopes how far reconciliation
    /// may reach back for predecessor-epoch observations.
    pub(super) position: Position,
}

pub(super) struct EventIndex {
    pub(super) fields: Vec<EventFields>,
    pub(super) active: Vec<bool>,
    pub(super) by_target: HashMap<TargetKey, Vec<usize>>,
    pub(super) by_position: HashMap<Position, Vec<usize>>,
    pub(super) by_resource: HashMap<Uuid, Vec<usize>>,
}

impl EventIndex {
    pub(super) fn new(events: &[NormalizedEvent]) -> Self {
        let fields = events.iter().map(EventFields::extract).collect::<Vec<_>>();
        let mut index = Self {
            active: vec![true; events.len()],
            fields,
            by_target: HashMap::new(),
            by_position: HashMap::new(),
            by_resource: HashMap::new(),
        };
        for (event_index, event) in events.iter().enumerate() {
            let fields = &index.fields[event_index];
            if let (Some(block_hash), Some(transaction_hash), Some(namehash)) = (
                event.block_hash.as_ref(),
                event.transaction_hash.as_ref(),
                fields.target_namehash.as_ref(),
            ) {
                index
                    .by_target
                    .entry(TargetKey {
                        namespace: event.namespace.clone(),
                        block_hash: block_hash.clone(),
                        transaction_hash: transaction_hash.clone(),
                        namehash: namehash.clone(),
                    })
                    .or_default()
                    // Iteration follows output order, so candidate vectors remain sorted by index.
                    .push(event_index);
            }
            if let Some(position) = fields.position {
                index
                    .by_position
                    .entry(position)
                    .or_default()
                    .push(event_index);
            }
            if let Some(resource_id) = fields.resource_id {
                index
                    .by_resource
                    .entry(resource_id)
                    .or_default()
                    .push(event_index);
            }
        }
        index
    }

    pub(super) fn registrations(&self, events: &[NormalizedEvent]) -> Vec<Registration> {
        events
            .iter()
            .enumerate()
            .filter(|(_, event)| event.event_kind == "RegistrationGranted")
            .filter(|(index, _)| {
                self.fields[*index].source_event == SourceEvent::NameRegistered
            })
            .filter_map(|(event_index, event)| {
                let registrant = event.after_state.get("registrant").and_then(Value::as_str);
                let emitter = event
                    .raw_fact_ref
                    .get("emitting_address")
                    .and_then(Value::as_str);
                debug_assert!(
                    registrant.is_some(),
                    "RegistrationGranted event {} must carry after_state.registrant for same-transaction reconciliation",
                    event.event_identity,
                );
                debug_assert!(
                    emitter.is_some(),
                    "RegistrationGranted event {} must carry raw_fact_ref.emitting_address for same-transaction reconciliation",
                    event.event_identity,
                );
                let position = event_position(event);
                debug_assert!(
                    position.is_some(),
                    "RegistrationGranted event {} must carry a full chain position for block-scoped reconciliation",
                    event.event_identity,
                );
                registrant?;
                let namehash = event.after_state["namehash"].as_str()?.to_ascii_lowercase();
                Some(Registration {
                    event_index,
                    key: TargetKey {
                        namespace: event.namespace.clone(),
                        block_hash: event.block_hash.clone()?,
                        transaction_hash: event.transaction_hash.clone()?,
                        namehash: namehash.clone(),
                    },
                    logical_name_id: event
                        .logical_name_id
                        .clone()
                        .unwrap_or_else(|| format!("{}:{namehash}", event.namespace)),
                    surface_known: event.logical_name_id.is_some(),
                    resource_id: event.resource_id?,
                    log_index: event.log_index?,
                    authority_key: event
                        .after_state
                        .get("authority_key")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    _emitter: emitter?.to_ascii_lowercase(),
                    provisional_owner: registrant?.to_ascii_lowercase(),
                    window: if event.after_state["registration_window"] == "whole_transaction" {
                        RegistrationWindow::WholeTransaction
                    } else {
                        RegistrationWindow::PriorLogsOnly
                    },
                    position: position?,
                })
            })
            .collect()
    }

    pub(super) fn candidates_at(&self, position: Position) -> Vec<usize> {
        self.by_position.get(&position).cloned().unwrap_or_default()
    }

    pub(super) fn update_resource(&mut self, event_index: usize, resource_id: Uuid) {
        self.fields[event_index].resource_id = Some(resource_id);
        self.by_resource
            .entry(resource_id)
            .or_default()
            .push(event_index);
    }
}

#[derive(Clone)]
pub(super) struct PermissionRevocation {
    pub(super) resource_id: Uuid,
    pub(super) subject: String,
    pub(super) scope: Value,
    pub(super) position: Position,
}

fn event_position(event: &NormalizedEvent) -> Option<Position> {
    Some((
        event.block_number?,
        event.transaction_index?,
        event.log_index?,
    ))
}
