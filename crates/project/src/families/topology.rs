//! F11, child edges. An ENSv1 or Basenames child edge row keeps the latest SubregistryChanged
//! for its parent node, child node and registry, ineligible edges included (children.rs,
//! `ranked_v1`); an ENSv2 parent keeps its latest SubregistryChanged, clears included
//! (children.rs, `ranked_v2_subregistries`).
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{
        Context, Preload, current, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null,
    },
    store::{Row, RowSet},
    tables,
};
use crate::Result;

const V1_REGISTRIES: [&str; 2] = ["ens_v1_registry_l1", "basenames_base_registry"];
const V2_REGISTRIES: [&str; 2] = ["ens_v2_root_l1", "ens_v2_registry_l1"];

/// How a table's key is read off an event, `None` when the event does not address it.
type KeyOf = fn(&Value, &BlockEvent) -> Option<Row>;

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

const KEYED: [(&tables::TableSpec, KeyOf); 2] = [
    (&tables::CHILD_EDGE_CANDIDATE, edge_key),
    (&tables::PARENT_SUBREGISTRY, subregistry_key),
];

/// The edge and subregistry keys one block's events name.
pub(super) fn preload(chain: &Value, events: &[BlockEvent], into: &mut Preload) {
    for (table, key) in KEYED {
        into.add(table, events.iter().filter_map(|event| key(chain, event)));
    }
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    for (table, key) in KEYED {
        let keys = events
            .iter()
            .filter_map(|event| key(&chain, event))
            .collect();
        load_rows(transaction, rows, table, keys).await?;
    }
    for event in events {
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

#[cfg(test)]
mod tests {
    use super::edge_arm;

    #[test]
    fn an_edge_carries_the_canonical_authority_arm_of_its_registry() {
        assert_eq!(edge_arm("ens_v1_registry_l1"), "ens_v1");
        assert_eq!(edge_arm("basenames_base_registry"), "basenames");
    }
}
