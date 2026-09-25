//! F2a, registration and lease state. The decoder of v2_lifecycle_events.sql:1-24 keys each
//! lifecycle event at write time: a resource-bearing event by its resource, a null-resource
//! event of the three ENSv2 families by its triple (name, registry identifier, token). Each key
//! keeps every event of the six retained kinds as its own row (never pruned, carrying the name
//! the adapter emitted as an immutable original beside the decoded name) and the membership
//! maxima of the reducer table; a resource-bearing ENSv2 grant or reservation points its
//! triple's association at its resource, and nothing else moves when it does. The per-registry
//! child row keeps the latest Granted, Renewed or Released with the existence flag.
mod registrant;

use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    decode,
    input::BlockEvent,
    reduce::{
        Context, current, in_family, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null,
    },
    store::{Row, RowSet},
    tables,
};
use crate::Result;

const RETAINED: [&str; 6] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "RegistrationReserved",
    "ExpiryChanged",
    "TokenControlTransferred",
];
const V2_FAMILIES: [&str; 3] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
];
const CHILD_FAMILIES: [&str; 2] = ["ens_v2_root_l1", "ens_v2_registry_l1"];

/// Where a lifecycle event is kept: its resource, or its ENSv2 triple.
enum StateKey {
    Resource(String),
    Triple([String; 3]),
}

impl StateKey {
    fn kind(&self) -> &'static str {
        match self {
            Self::Resource(_) => "resource",
            Self::Triple(_) => "triple",
        }
    }

    fn text(&self) -> String {
        match self {
            Self::Resource(resource) => resource.clone(),
            Self::Triple(triple) => json!(triple).to_string(),
        }
    }
}

/// The registry identifier and token of the decoder (v2_lifecycle_events.sql:14-18, :20-21).
fn triple(event: &BlockEvent) -> Option<[String; 3]> {
    let after = &event.after;
    let registry = raw_text(after, "registry_contract_instance_id")
        .or_else(|| event.emitting_address())
        .or_else(|| raw_text(after, "registry"))
        .unwrap_or_default();
    let token = raw_text(after, "token_id").unwrap_or_default();
    Some([event.logical_name_id.clone()?, registry, token])
}

fn state_key(event: &BlockEvent) -> Option<StateKey> {
    RETAINED
        .contains(&event.event_kind.as_str())
        .then_some(())?;
    if let Some(resource) = &event.resource_id {
        return Some(StateKey::Resource(resource.clone()));
    }
    V2_FAMILIES
        .contains(&event.source_family.as_str())
        .then_some(())?;
    triple(event).map(StateKey::Triple)
}

fn state_row_key(chain: &Value, key: &StateKey) -> Row {
    match key {
        StateKey::Resource(resource) => key_of(
            &tables::LIFECYCLE_KEY_STATE,
            [chain.clone(), json!(resource)],
        ),
        StateKey::Triple(triple) => key_of(
            &tables::LIFECYCLE_TRIPLE_SUMMARY,
            std::iter::once(chain.clone()).chain(triple.iter().map(|part| json!(part))),
        ),
    }
}

fn event_row_key(chain: &Value, key: &StateKey, event: &BlockEvent) -> Row {
    key_of(
        &tables::LIFECYCLE_EVENT,
        [
            chain.clone(),
            json!(key.kind()),
            json!(key.text()),
            json!(event.position.event_identity),
        ],
    )
}

/// The triple a resource-bearing ENSv2 grant or reservation associates with its resource.
fn association(chain: &Value, event: &BlockEvent) -> Option<Row> {
    matches!(
        event.event_kind.as_str(),
        "RegistrationGranted" | "RegistrationReserved"
    )
    .then_some(())?;
    V2_FAMILIES
        .contains(&event.source_family.as_str())
        .then_some(())?;
    event.resource_id.as_ref()?;
    let triple = triple(event)?;
    Some(key_of(
        &tables::LIFECYCLE_ASSOCIATION,
        std::iter::once(chain.clone()).chain(triple.iter().map(|part| json!(part))),
    ))
}

/// The per-registry child row an ENSv2 registration event writes (children.rs:196-206, :345-362).
fn child(chain: &Value, event: &BlockEvent) -> Option<Row> {
    CHILD_FAMILIES
        .contains(&event.source_family.as_str())
        .then_some(())?;
    let registry = raw_text(&event.after, "registry_contract_instance_id")?;
    Some(key_of(
        &tables::CHILD_REGISTRATION_STATE,
        [
            chain.clone(),
            json!(event.logical_name_id.clone()?),
            json!(registry),
        ],
    ))
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let keyed: Vec<(&BlockEvent, StateKey)> = events
        .iter()
        .filter_map(|event| state_key(event).map(|key| (event, key)))
        .collect();
    let (mut resources, mut triples) = (Vec::new(), Vec::new());
    for (_, key) in &keyed {
        match key {
            StateKey::Resource(_) => resources.push(state_row_key(&chain, key)),
            StateKey::Triple(_) => triples.push(state_row_key(&chain, key)),
        }
    }
    load_rows(transaction, rows, &tables::LIFECYCLE_KEY_STATE, resources).await?;
    load_rows(
        transaction,
        rows,
        &tables::LIFECYCLE_TRIPLE_SUMMARY,
        triples,
    )
    .await?;
    let retained = keyed
        .iter()
        .map(|(event, key)| event_row_key(&chain, key, event))
        .collect();
    load_rows(transaction, rows, &tables::LIFECYCLE_EVENT, retained).await?;
    let associations = events
        .iter()
        .filter_map(|event| association(&chain, event))
        .collect();
    load_rows(
        transaction,
        rows,
        &tables::LIFECYCLE_ASSOCIATION,
        associations,
    )
    .await?;
    let children = events
        .iter()
        .filter_map(|event| child(&chain, event))
        .collect();
    load_rows(
        transaction,
        rows,
        &tables::CHILD_REGISTRATION_STATE,
        children,
    )
    .await?;
    let candidates = decode::Candidates::load(transaction, context, events, rows).await?;

    for (event, key) in &keyed {
        let table = &tables::LIFECYCLE_EVENT;
        let mut row = current(rows, table, &event_row_key(&chain, key, event));
        set(&mut row, "chain_id", context.chain_id);
        retained_columns(&mut row, event);
        let decoded = candidates.decode(&row);
        set(
            &mut row,
            "decoded_logical_name_id",
            text_or_null(decoded.clone()),
        );
        event.write_position(&mut row);
        rows.put(table, row.clone())
            .map_err(in_family(table.name))?;
        if let Some(name) = event.logical_name_id.clone().or(decoded) {
            super::identity::successor_grant(transaction, context, rows, &name, &row).await?;
        }
        if event.event_kind != "TokenControlTransferred" {
            let table = match key {
                StateKey::Resource(_) => &tables::LIFECYCLE_KEY_STATE,
                StateKey::Triple(_) => &tables::LIFECYCLE_TRIPLE_SUMMARY,
            };
            let mut row = current(rows, table, &state_row_key(&chain, key));
            if matches!(key, StateKey::Resource(_)) && event.logical_name_id.is_some() {
                set(
                    &mut row,
                    "logical_name_id",
                    text_or_null(event.logical_name_id.clone()),
                );
            }
            maxima(
                &mut row,
                event,
                context,
                matches!(key, StateKey::Resource(_)),
            );
            put(rows, table, row, event)?;
        }
    }
    for event in events {
        if let Some(key) = association(&chain, event) {
            let table = &tables::LIFECYCLE_ASSOCIATION;
            let mut row = current(rows, table, &key);
            set(
                &mut row,
                "target_resource_id",
                text_or_null(event.resource_id.clone()),
            );
            set(&mut row, "event_kind", event.event_kind.clone());
            put(rows, table, row, event)?;
        }
        if let Some(key) = child(&chain, event) {
            child_row(rows, key, event)?;
        }
    }
    decode::redecode(transaction, context, &candidates, rows).await?;
    registrant::fold_registrants(transaction, context, rows).await
}

fn child_row(rows: &mut RowSet, key: Row, event: &BlockEvent) -> Result<()> {
    let table = &tables::CHILD_REGISTRATION_STATE;
    let existing = rows.get(table, &key).is_some();
    let mut row = current(rows, table, &key);
    let kind = event.event_kind.as_str();
    if !matches!(
        kind,
        "RegistrationReserved"
            | "RegistrationGranted"
            | "RegistrationRenewed"
            | "RegistrationReleased"
    ) {
        return Ok(());
    }
    let exists = row.get("exists").and_then(Value::as_bool).unwrap_or(false);
    set(&mut row, "exists", exists || kind != "RegistrationReleased");
    if kind == "RegistrationReserved" {
        // A reservation is never the selected event; it only proves the child exists.
        row.entry("event_kind").or_insert(Value::Null);
        row.entry("registrant").or_insert(Value::Null);
        if existing {
            return rows.put(table, row).map_err(in_family(table.name));
        }
        return put(rows, table, row, event);
    }
    set(&mut row, "event_kind", kind);
    let registrant = (kind != "RegistrationReleased")
        .then(|| raw_lower(&event.after, "registrant"))
        .flatten();
    set(&mut row, "registrant", text_or_null(registrant));
    put(rows, table, row, event)
}

fn flag(value: &Value, field: &str) -> Value {
    match value.get(field) {
        Some(Value::Bool(flag)) => Value::Bool(*flag),
        Some(Value::String(text)) if text == "true" || text == "false" => {
            Value::Bool(text == "true")
        }
        _ => Value::Null,
    }
}

/// The expiry as build.sql:493-501 converts it: an integral JSON number in range, else null.
fn expiry_seconds(after: &Value) -> Value {
    let Some(Value::Number(number)) = after.get("expiry") else {
        return Value::Null;
    };
    let seconds = number.as_i64().or_else(|| {
        number
            .as_f64()
            .filter(|value| value.fract() == 0.0 && value.abs() < 1e15)
            .map(|value| value as i64)
    });
    seconds
        .filter(|value| (-377_705_116_800..=253_402_300_799).contains(value))
        .map_or(Value::Null, Value::from)
}

fn retained_columns(row: &mut Map<String, Value>, event: &BlockEvent) {
    let after = &event.after;
    let text = |field: &str| text_or_null(raw_text(after, field));
    set(row, "event_kind", event.event_kind.clone());
    set(
        row,
        "original_logical_name_id",
        text_or_null(event.logical_name_id.clone()),
    );
    set(row, "resource_id", text_or_null(event.resource_id.clone()));
    set(row, "source_family", event.source_family.clone());
    // Stored as the payload has it, null when absent: the served name block reports it so
    // (name_current/build.sql:30), and the admission reads default it to registrar
    // (authority_events.sql, COALESCE(NULLIF(authority_kind, ''), 'registrar')).
    set(
        row,
        "authority_kind",
        text_or_null(raw_text(after, "authority_kind")),
    );
    set(
        row,
        "authority_key",
        text_or_null(raw_text(after, "authority_key")),
    );
    set(
        row,
        "transaction_hash",
        text_or_null(event.transaction_hash.clone()),
    );
    let to = (event.event_kind == "TokenControlTransferred").then(|| raw_lower(after, "to"));
    set(row, "to_address", text_or_null(to.flatten()));
    set(row, "namehash", text_or_null(raw_lower(after, "namehash")));
    set(
        row,
        "registrant",
        text_or_null(raw_lower(after, "registrant")),
    );
    let before = (event.event_kind == "RegistrationReleased")
        .then(|| raw_lower(&event.before, "registrant"));
    set(row, "before_registrant", text_or_null(before.flatten()));
    set(
        row,
        "expiry",
        after.get("expiry").cloned().unwrap_or(Value::Null),
    );
    set(row, "expiry_seconds", expiry_seconds(after));
    for field in [
        "status",
        "source_event",
        "derived_from",
        "terminal_reason",
        "owner_getter",
    ] {
        set(row, field, text(field));
    }
    set(
        row,
        "released_at",
        after.get("released_at").cloned().unwrap_or(Value::Null),
    );
    for field in [
        "revived_from_expiry",
        "state_derived",
        "surface_materialization",
        "registrar_surface_snapshot",
        "owner_word_unmasked",
    ] {
        set(row, field, flag(after, field));
    }
    let registered =
        raw_text(after, "original_registered_at").and_then(|value| value.parse::<i64>().ok());
    set(
        row,
        "original_registered_at",
        registered.map_or(Value::Null, Value::from),
    );
    set(
        row,
        "registry_owner",
        text_or_null(raw_lower(after, "registry_owner")),
    );
}

fn path_expiry(after: &Value) -> bool {
    raw_text(after, "source_event").as_deref() == Some("RegistryPathExpired")
        && raw_text(after, "derived_from").as_deref() == Some("interpreter_state")
        && raw_text(after, "terminal_reason").as_deref() == Some("registry_name_binding_expired")
}

/// The membership maxima of the reducer table. Events arrive in the canonical order, so each
/// field is the latest event that sets it.
fn maxima(row: &mut Map<String, Value>, event: &BlockEvent, context: &Context<'_>, resource: bool) {
    for field in [
        "last_grant",
        "last_reservation",
        "last_active",
        "last_release_any",
        "last_path_expiry",
        "last_explicit_release",
        "last_renewal",
        "last_expiry_changed",
    ]
    .into_iter()
    .chain(resource.then_some("last_revival"))
    {
        row.entry(field).or_insert(Value::Null);
    }
    let after = &event.after;
    let position = event.position.to_json();
    let text = |field: &str| text_or_null(raw_text(after, field));
    let expiry = after.get("expiry").cloned().unwrap_or(Value::Null);
    match event.event_kind.as_str() {
        "RegistrationGranted" => {
            // The registrar snapshot's own registration time, else the block's (build.sql:370-376).
            let snapshot = event.source_family == "ens_v1_registrar_l1"
                && [
                    "state_derived",
                    "surface_materialization",
                    "registrar_surface_snapshot",
                ]
                .iter()
                .all(|field| flag(after, field) == Value::Bool(true));
            let registered_at = snapshot
                .then(|| {
                    raw_text(after, "original_registered_at")?
                        .parse::<i64>()
                        .ok()
                })
                .flatten()
                .unwrap_or(context.block.timestamp_seconds);
            set(
                row,
                "last_grant",
                json!({
                    "position": position, "registrant": raw_lower(after, "registrant"),
                    "expiry": expiry, "authority_kind": raw_text(after, "authority_kind"),
                    "authority_key": raw_text(after, "authority_key"),
                    "status": text("status"), "registered_at": registered_at,
                }),
            );
            set(
                row,
                "last_active",
                json!({"kind": event.event_kind, "position": position}),
            );
        }
        "RegistrationReserved" => {
            set(
                row,
                "last_reservation",
                json!({
                    "position": position, "registrant": raw_lower(after, "registrant"),
                    "expiry": expiry, "status": text("status"),
                }),
            );
            set(
                row,
                "last_active",
                json!({"kind": event.event_kind, "position": position}),
            );
        }
        "RegistrationReleased" => {
            set(row, "last_release_any", json!({"position": position}));
            let released_at = after.get("released_at").cloned().unwrap_or(Value::Null);
            if path_expiry(after) {
                set(
                    row,
                    "last_path_expiry",
                    json!({
                        "position": position, "released_at": released_at, "expiry": expiry,
                        "source_event": text("source_event"), "derived_from": text("derived_from"),
                        "terminal_reason": text("terminal_reason"),
                    }),
                );
            } else {
                set(
                    row,
                    "last_explicit_release",
                    json!({"position": position, "released_at": released_at}),
                );
            }
        }
        "RegistrationRenewed" => {
            let revived = flag(after, "revived_from_expiry");
            if resource && revived == Value::Bool(true) && !row["last_path_expiry"].is_null() {
                set(row, "last_revival", json!({"position": position}));
            }
            set(
                row,
                "last_renewal",
                json!({
                    "position": position, "expiry": expiry, "revived_from_expiry": revived,
                }),
            );
        }
        "ExpiryChanged" => set(row, "last_expiry_changed", json!({"position": position})),
        _ => {}
    }
}
