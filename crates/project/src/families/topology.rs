//! F10 and F11, aliases and child edges. A name's alias row keeps its latest AliasChanged with
//! the event's own target and active flag (name_topology.rs, the alias lateral); a resolver's
//! alias row keeps the latest AliasChanged per resolver and alias identity (resolver
//! alias_summary.rs). An ENSv1 or Basenames child edge row keeps the latest SubregistryChanged
//! for its parent node, child node and registry, ineligible edges included (children.rs,
//! `ranked_v1`); an ENSv2 parent keeps its latest SubregistryChanged, clears included
//! (children.rs, `ranked_v2_subregistries`).
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    keys,
    reduce::{
        Context, current, json_boolean, key_of, load_rows, put, raw_lower, raw_text, set,
        text_or_null,
    },
    store::{Row, RowSet},
    tables,
};
use crate::Result;

const V1_REGISTRIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];
const V2_REGISTRIES: [&str; 2] = ["ens_v2_root_l1", "ens_v2_registry_l1"];

/// How a table's key is read off an event, `None` when the event does not address it.
type KeyOf = fn(&Value, &BlockEvent) -> Option<Row>;

/// A payload field as JSON, null when absent.
fn field(event: &BlockEvent, name: &str) -> Value {
    event.after.get(name).cloned().unwrap_or(Value::Null)
}

fn name_alias_key(chain: &Value, event: &BlockEvent) -> Option<Row> {
    (event.event_kind == "AliasChanged").then_some(())?;
    Some(key_of(
        &tables::NAME_ALIAS,
        [chain.clone(), json!(event.logical_name_id.as_deref()?)],
    ))
}

fn resolver_alias_key(chain: &Value, event: &BlockEvent) -> Option<Row> {
    (event.event_kind == "AliasChanged").then_some(())?;
    Some(key_of(
        &tables::RESOLVER_ALIAS,
        [
            chain.clone(),
            json!(keys::alias_resolver(event)?),
            json!(keys::alias_identity(event)),
        ],
    ))
}

fn edge_key(chain: &Value, event: &BlockEvent) -> Option<Row> {
    let after = &event.after;
    (event.event_kind == "SubregistryChanged"
        && V1_REGISTRIES.contains(&event.source_family.as_str())
        && raw_text(after, "labelhash").is_some())
    .then_some(())?;
    Some(key_of(
        &tables::CHILD_EDGE_CANDIDATE,
        [
            chain.clone(),
            json!(event.namespace),
            json!(raw_lower(after, "node")?),
            json!(raw_lower(after, "child_node")?),
            json!(edge_arm(&event.source_family)),
        ],
    ))
}

/// The authority arm of an ENSv1 or Basenames registry edge, as children.rs:271-273 names it:
/// `basenames` for the Basenames registry, `ens_v1` for the ENSv1 registry.
pub(crate) fn edge_arm(source_family: &str) -> &'static str {
    if source_family == "basenames_base_registry" {
        "basenames"
    } else {
        "ens_v1"
    }
}

fn subregistry_key(chain: &Value, event: &BlockEvent) -> Option<Row> {
    (event.event_kind == "SubregistryChanged"
        && V2_REGISTRIES.contains(&event.source_family.as_str()))
    .then_some(())?;
    Some(key_of(
        &tables::PARENT_SUBREGISTRY,
        [chain.clone(), json!(event.logical_name_id.as_deref()?)],
    ))
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let keyed: [(&'static tables::TableSpec, KeyOf); 4] = [
        (&tables::NAME_ALIAS, name_alias_key),
        (&tables::RESOLVER_ALIAS, resolver_alias_key),
        (&tables::CHILD_EDGE_CANDIDATE, edge_key),
        (&tables::PARENT_SUBREGISTRY, subregistry_key),
    ];
    for (table, key) in keyed {
        let keys = events
            .iter()
            .filter_map(|event| key(&chain, event))
            .collect();
        load_rows(transaction, rows, table, keys).await?;
    }
    for event in events {
        if let Some(key) = name_alias_key(&chain, event) {
            let table = &tables::NAME_ALIAS;
            let mut row = current(rows, table, &key);
            set(&mut row, "active", active(event));
            for name in [
                "alias_state",
                "to_logical_name_id",
                "to_name",
                "to_resource_id",
                "to_normalized_name",
                "to_canonical_display_name",
                "to_namehash",
            ] {
                set(&mut row, name, text_or_null(raw_text(&event.after, name)));
            }
            set(
                &mut row,
                "resolver_address",
                text_or_null(raw_lower(&event.after, "resolver")),
            );
            put(rows, table, row, event)?;
        }
        if let Some(key) = resolver_alias_key(&chain, event) {
            let table = &tables::RESOLVER_ALIAS;
            let mut row = current(rows, table, &key);
            set(&mut row, "active", active(event));
            for name in [
                "alias_state",
                "from_dns_encoded_name",
                "to_dns_encoded_name",
                "from_name",
                "to_logical_name_id",
                "to_name",
                "to_resource_id",
            ] {
                set(&mut row, name, text_or_null(raw_text(&event.after, name)));
            }
            set(
                &mut row,
                "logical_name_id",
                text_or_null(event.logical_name_id.clone()),
            );
            put(rows, table, row, event)?;
        }
        if let Some(key) = edge_key(&chain, event) {
            let table = &tables::CHILD_EDGE_CANDIDATE;
            let mut row = current(rows, table, &key);
            set(
                &mut row,
                "owner",
                text_or_null(raw_lower(&event.after, "owner")),
            );
            set(
                &mut row,
                "owner_getter",
                text_or_null(raw_lower(&event.after, "owner_getter")),
            );
            set(
                &mut row,
                "labelhash",
                text_or_null(raw_lower(&event.after, "labelhash")),
            );
            set(&mut row, "source_family", event.source_family.clone());
            put(rows, table, row, event)?;
        }
        if let Some(key) = subregistry_key(&chain, event) {
            let table = &tables::PARENT_SUBREGISTRY;
            let mut row = current(rows, table, &key);
            set(
                &mut row,
                "subregistry_address",
                raw_lower(&event.after, "subregistry").unwrap_or_default(),
            );
            put(rows, table, row, event)?;
        }
    }
    Ok(())
}

/// The event's active flag, active when it carries none, as both alias readers take it
/// (`COALESCE((after_state ->> 'active')::boolean, true)`). A text or number is read the way
/// PostgreSQL reads a boolean; a spelling it rejects fails the served batch, and is kept active
/// here.
fn active(event: &BlockEvent) -> Value {
    Value::Bool(json_boolean(&field(event, "active")).unwrap_or(true))
}

#[cfg(test)]
mod tests {
    use super::edge_arm;

    #[test]
    fn an_edge_carries_the_canonical_authority_arm_of_its_registry() {
        assert_eq!(edge_arm("ens_v1_registry_l1"), "ens_v1");
        assert_eq!(edge_arm("basenames_base_registry"), "basenames");
    }
}
