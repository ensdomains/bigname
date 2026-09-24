//! F4 and F5, the resolver pointers. F4 keeps the ENSv1 registry-node pointer: the latest
//! ResolverChanged of an ENSv1 registry, registrar or wrapper for the node it addresses, clears
//! included (record_inventory/mirror.rs, `registry_state`). F5 keeps a resource's pointer in
//! three groups: the latest named ResolverChanged on the resource, clears included
//! (linked_records.rs, `project_record_pointer_latest`); the latest whose resolver is not the
//! zero address (name_topology.rs, the wildcard source); and the latest of RecordVersionChanged
//! or ResolverChanged, the wildcard version boundary.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    keys::{self, Space, ZERO_ADDRESS},
    reduce::{self, Context, chain_key, current, key_of, load, put, raw_lower, set, text_or_null},
    store::RowSet,
    tables,
};
use crate::Result;

const V1_POINTER_FAMILIES: [&str; 3] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_wrapper_l1",
];

pub(super) async fn registry_pointers(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::REGISTRY_POINTER;
    load(
        transaction,
        rows,
        table,
        context.keys,
        Space::RegistryPointer,
        |key| chain_key(table, context.chain_id, key),
    )
    .await?;
    for event in events.iter().filter(|event| {
        event.event_kind == "ResolverChanged"
            && V1_POINTER_FAMILIES.contains(&event.source_family.as_str())
    }) {
        let Some(node) = keys::pointer_node(&event.after) else {
            continue;
        };
        let key = key_of(
            table,
            [json!(context.chain_id), json!(event.namespace), json!(node)],
        );
        let mut row = current(rows, table, &key);
        set(
            &mut row,
            "resolver_address",
            text_or_null(raw_lower(&event.after, "resolver")),
        );
        set(
            &mut row,
            "resource_id",
            text_or_null(event.resource_id.clone()),
        );
        set(&mut row, "source_family", event.source_family.clone());
        put(rows, table, row, event)?;
    }
    Ok(())
}

pub(super) async fn resource_pointers(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::RESOURCE_POINTER;
    load(
        transaction,
        rows,
        table,
        context.keys,
        Space::Resource,
        |key| chain_key(table, context.chain_id, key),
    )
    .await?;
    for event in events {
        let (Some(resource), Some(name)) = (&event.resource_id, &event.logical_name_id) else {
            continue;
        };
        let pointer = event.event_kind == "ResolverChanged";
        if !pointer && event.event_kind != "RecordVersionChanged" {
            continue;
        }
        let key = key_of(table, [json!(context.chain_id), json!(resource)]);
        let mut row = current(rows, table, &key);
        let position = event.position.to_json();
        if pointer {
            let resolver = raw_lower(&event.after, "resolver");
            set(&mut row, "resolver_address", text_or_null(resolver.clone()));
            set(&mut row, "pointer_position", position.clone());
            set(&mut row, "namespace", event.namespace.clone());
            set(&mut row, "source_family", event.source_family.clone());
            set(
                &mut row,
                "namehash",
                text_or_null(reduce::namehash_of(name)),
            );
            if resolver.as_deref().unwrap_or_default() != ZERO_ADDRESS {
                set(&mut row, "nonzero_resolver_address", text_or_null(resolver));
                set(&mut row, "nonzero_position", position.clone());
            }
        }
        set(&mut row, "boundary_kind", event.event_kind.clone());
        set(&mut row, "boundary_position", position);
        set(
            &mut row,
            "boundary_block_timestamp",
            context.block.timestamp.clone(),
        );
        for column in [
            "resolver_address",
            "pointer_position",
            "namespace",
            "source_family",
            "namehash",
            "nonzero_resolver_address",
            "nonzero_position",
        ] {
            row.entry(column).or_insert(Value::Null);
        }
        put(rows, table, row, event)?;
    }
    Ok(())
}
