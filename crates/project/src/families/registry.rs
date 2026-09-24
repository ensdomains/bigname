//! F2c, registry ownership. An ENSv1 or Basenames registry node keeps the latest owner the
//! registry reported with its zero-owner getter facts, whether the 2017 registry ever recorded
//! the node and the first block of a current-registry record (name_authority/build.sql,
//! `registry_records`). A resource keeps its latest registry-binding observation, one row for
//! events attributed through the resource itself and one for events attributed through a name
//! (permission_resources.rs).
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, current, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null},
    store::RowSet,
    tables,
};
use crate::Result;

const V1_REGISTRIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];
const V1_REGISTRARS: [&str; 2] = ["ens_v1_registrar_l1", "basenames_base_registrar"];
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    registry_nodes(transaction, context, events, rows).await?;
    observations(transaction, context, events, rows).await
}

/// The node an ENSv1 registry event describes: the child for NewOwner, else its own node.
fn registry_node(event: &BlockEvent) -> Option<String> {
    (V1_REGISTRIES.contains(&event.source_family.as_str())
        && matches!(
            event.event_kind.as_str(),
            "SubregistryChanged" | "AuthorityTransferred"
        ))
    .then_some(())?;
    raw_lower(&event.after, "child_node")
        .filter(|node| !node.is_empty())
        .or_else(|| raw_lower(&event.after, "node"))
}

async fn registry_nodes(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::REGISTRY_NODE_STATE;
    let chain = json!(context.chain_id);
    let relevant: Vec<(&BlockEvent, String)> = events
        .iter()
        .filter_map(|event| Some((event, registry_node(event)?)))
        .collect();
    let keys = relevant
        .iter()
        .map(|(event, node)| key_of(table, [chain.clone(), json!(event.namespace), json!(node)]))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, node) in relevant {
        let mut row = current(
            rows,
            table,
            &key_of(table, [chain.clone(), json!(event.namespace), json!(node)]),
        );
        let after = &event.after;
        if event.event_kind == "AuthorityTransferred" {
            set(&mut row, "owner", text_or_null(raw_lower(after, "owner")));
            set(
                &mut row,
                "owner_getter",
                text_or_null(raw_lower(after, "owner_getter")),
            );
            set(
                &mut row,
                "owner_getter_reason",
                text_or_null(raw_text(after, "owner_getter_reason")),
            );
            set(
                &mut row,
                "owner_word_unmasked",
                after
                    .get("owner_word_unmasked")
                    .and_then(Value::as_bool)
                    .map_or(Value::Null, Value::Bool),
            );
            set(
                &mut row,
                "registry_owner",
                text_or_null(raw_lower(after, "registry_owner")),
            );
        }
        let role = raw_text(after, "emitter_role");
        set(&mut row, "emitter_role", text_or_null(role.clone()));
        set(
            &mut row,
            "registry_contract",
            text_or_null(event.emitting_address()),
        );
        let old = row
            .get("has_old_record")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        set(
            &mut row,
            "has_old_record",
            old || role.as_deref() == Some("registry_old"),
        );
        if role.as_deref() == Some("registry") {
            let first = row
                .get("first_current_record_block")
                .and_then(Value::as_i64)
                .map_or(event.position.block_number, |first| {
                    first.min(event.position.block_number)
                });
            set(&mut row, "first_current_record_block", first);
        }
        put(rows, table, row, event)?;
    }
    Ok(())
}

/// A registry-binding observation the resource summary reads: owner, contract and whether it
/// applies.
fn observation(event: &BlockEvent) -> Option<(&str, &'static str)> {
    let family = event.source_family.as_str();
    let kind = event.event_kind.as_str();
    let producer = matches!(
        kind,
        "AuthorityTransferred" | "SubregistryChanged" | "SurfaceBound" | "SurfaceUnbound"
    ) && (V1_REGISTRIES.contains(&family)
        || (matches!(kind, "SurfaceBound" | "SurfaceUnbound") && V1_REGISTRARS.contains(&family)));
    producer.then_some(())?;
    let via = if event.logical_name_id.is_some() {
        "name"
    } else {
        "own"
    };
    Some((event.resource_id.as_deref()?, via))
}

fn address(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value.len() == 42
            && value.starts_with("0x")
            && value[2..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn observations(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::REGISTRY_BINDING_OBSERVATION;
    let chain = json!(context.chain_id);
    let relevant: Vec<(&BlockEvent, (&str, &str))> = events
        .iter()
        .filter_map(|event| Some((event, observation(event)?)))
        .collect();
    let keys = relevant
        .iter()
        .map(|(_, (resource, via))| key_of(table, [chain.clone(), json!(resource), json!(via)]))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, (resource, via)) in relevant {
        let mut row = current(
            rows,
            table,
            &key_of(table, [chain.clone(), json!(resource), json!(via)]),
        );
        let after = &event.after;
        let owner = (event.event_kind != "SurfaceUnbound")
            .then(|| raw_lower(after, "owner_getter"))
            .flatten();
        let state_derived_registry_only = event.event_kind == "SurfaceBound"
            && after.get("state_derived") == Some(&json!(true))
            && raw_text(after, "authority_kind").as_deref() == Some("registry_only");
        let contract = if V1_REGISTRARS.contains(&event.source_family.as_str())
            || state_derived_registry_only
        {
            raw_lower(after, "registry_contract")
        } else {
            event
                .emitting_address()
                .or_else(|| raw_lower(after, "registry_contract"))
        };
        let applicable = address(owner.as_deref())
            && owner.as_deref() != Some(ZERO_ADDRESS)
            && address(contract.as_deref());
        set(&mut row, "event_kind", event.event_kind.clone());
        set(&mut row, "registry_owner", text_or_null(owner));
        set(&mut row, "registry_contract", text_or_null(contract));
        set(
            &mut row,
            "provenance",
            json!({"raw_fact_ref": event.raw_fact_ref, "logical_name_id": event.logical_name_id}),
        );
        set(&mut row, "applicable", applicable);
        set(
            &mut row,
            "clear_event_identity",
            if applicable {
                Value::Null
            } else {
                json!(event.position.event_identity)
            },
        );
        put(rows, table, row, event)?;
    }
    Ok(())
}
