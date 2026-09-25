//! F2c, registry ownership. An ENSv1 or Basenames registry node keeps the latest owner the
//! registry reported with its zero-owner getter facts, whether the 2017 registry ever recorded
//! the node and the first block of a current-registry record (name_authority/build.sql,
//! `registry_records`); the owner group carries the position and resource of the event that set
//! it. Every owner-setting event of a node is also kept by position, since the group keeps only
//! the latest. Each observation identity (the name, else the resource) keeps its latest
//! registry-binding observation with the resource it reaches (permission_resources.rs).
use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{
        Context, current, in_family, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null,
    },
    store::RowSet,
    tables,
};
use crate::{ProjectError, Result};

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
    let history = relevant
        .iter()
        .map(|(event, node)| owner_event_key(&chain, event, node))
        .collect();
    load_rows(transaction, rows, &tables::REGISTRY_OWNER_EVENT, history).await?;
    for (event, node) in relevant {
        owner_event(rows, &chain, event, &node)?;
        let mut row = current(
            rows,
            table,
            &key_of(table, [chain.clone(), json!(event.namespace), json!(node)]),
        );
        let after = &event.after;
        // Both registry producers report the owner (name_authority/stage.rs:200-261); the
        // owner group keeps the position and resource of the event that set it, apart from
        // the row's own last-write position.
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
        set(&mut row, "owner_event_kind", event.event_kind.clone());
        set(&mut row, "owner_position", event.position.to_json());
        set(
            &mut row,
            "owner_resource_id",
            text_or_null(event.resource_id.clone()),
        );
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

fn owner_event_key(chain: &Value, event: &BlockEvent, node: &str) -> super::store::Row {
    key_of(
        &tables::REGISTRY_OWNER_EVENT,
        [
            chain.clone(),
            json!(event.namespace),
            json!(node),
            json!(event.position.event_identity),
        ],
    )
}

/// One owner-setting registry event of a node, kept by position with the name, resource and
/// authority kind it carried; the node row keeps only the latest owner group.
fn owner_event(rows: &mut RowSet, chain: &Value, event: &BlockEvent, node: &str) -> Result<()> {
    let table = &tables::REGISTRY_OWNER_EVENT;
    let mut row = current(rows, table, &owner_event_key(chain, event, node));
    let after = &event.after;
    set(
        &mut row,
        "transaction_hash",
        text_or_null(event.transaction_hash.clone()),
    );
    set(
        &mut row,
        "logical_name_id",
        text_or_null(event.logical_name_id.clone()),
    );
    set(
        &mut row,
        "resource_id",
        text_or_null(event.resource_id.clone()),
    );
    set(&mut row, "event_kind", event.event_kind.clone());
    set(&mut row, "source_family", event.source_family.clone());
    set(
        &mut row,
        "authority_kind",
        text_or_null(raw_text(after, "authority_kind")),
    );
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
    // The same fields the node row keeps for its latest event, so an earlier event that wins
    // the control owner still has them.
    set(
        &mut row,
        "registry_owner",
        text_or_null(raw_lower(after, "registry_owner")),
    );
    set(
        &mut row,
        "owner_word_unmasked",
        after
            .get("owner_word_unmasked")
            .and_then(Value::as_bool)
            .map_or(Value::Null, Value::Bool),
    );
    put(rows, table, row, event)
}

/// A registry-binding observation the resource summary reads (permission_resources.rs:10-60):
/// its observation identity, the name when the event carries one, the event's resource and
/// whether it reaches its resource through the name's current resource.
struct Observation<'a> {
    identity: String,
    name: Option<&'a str>,
    resource: &'a str,
    through_name: bool,
}

fn observation(event: &BlockEvent) -> Option<Observation<'_>> {
    let family = event.source_family.as_str();
    let kind = event.event_kind.as_str();
    let producer = matches!(
        kind,
        "AuthorityTransferred" | "SubregistryChanged" | "SurfaceBound" | "SurfaceUnbound"
    ) && (V1_REGISTRIES.contains(&family)
        || (matches!(kind, "SurfaceBound" | "SurfaceUnbound") && V1_REGISTRARS.contains(&family)));
    producer.then_some(())?;
    let resource = event.resource_id.as_deref()?;
    let name = event.logical_name_id.as_deref();
    Some(Observation {
        identity: name.unwrap_or(resource).to_owned(),
        name,
        resource,
        through_name: name.is_some()
            && matches!(kind, "AuthorityTransferred" | "SubregistryChanged"),
    })
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

/// The names' current ENSv1 or Basenames resource after the block: the binding of those arms
/// open at the end of the block, the latest opened when two are. The end of the block is the
/// served cutoff, the block's timestamp plus one second, with a binding open when it starts
/// before the cutoff and ends at or after it (name_authority/build.sql:4-12). A binding opened
/// in the block starts at the block time plus its log's microseconds, so the block's integer
/// time would miss it and keep the binding it closed.
///
/// The cutoff comes from the lineage row at the block's number and hash, which `read_block`
/// read as readable in this transaction; the cutoff read does not filter canonicality, so an
/// orphaning since then still finds it. Were the row gone, the query would return no rows and
/// every named observation of the block would fall back to its own resource: no rows, never a
/// binding read at a wrong time.
async fn current_resources(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    names: &[String],
) -> Result<BTreeMap<String, String>> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows: Vec<(String, String)> = sqlx::query_as(
        "/* project:families.registry.current_resources */ SELECT DISTINCT ON
                (binding.logical_name_id) binding.logical_name_id, binding.resource_id::text
         FROM surface_bindings binding
         CROSS JOIN (
             SELECT lineage.block_timestamp + interval '1 second' AS cutoff
             FROM chain_lineage lineage
             WHERE lineage.chain_id = $1 AND lineage.block_number = $3
               AND lineage.block_hash = $4
         ) target_time
         WHERE binding.chain_id = $1 AND binding.logical_name_id = ANY($2)
           AND binding.authority_arm IN ('ens_v1', 'basenames')
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.block_number <= $3
           AND binding.active_from < target_time.cutoff
           AND (binding.active_to IS NULL OR binding.active_to >= target_time.cutoff)
         ORDER BY binding.logical_name_id, binding.active_from DESC,
                  binding.surface_binding_id DESC",
    )
    .bind(context.chain_id)
    .bind(names)
    .bind(context.block.number)
    .bind(&context.block.hash)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read names' current resources", error))
    .map_err(in_family(tables::REGISTRY_BINDING_OBSERVATION.name))?;
    Ok(rows.into_iter().collect())
}

/// The names whose current binding may have moved in this block: every name a SurfaceBound
/// or SurfaceUnbound of the block names, and every name with a binding row of the block.
async fn rebound_names(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
) -> Result<Vec<String>> {
    let mut names: Vec<String> = sqlx::query_scalar(
        "/* project:families.registry.rebound_names */ SELECT DISTINCT binding.logical_name_id
         FROM surface_bindings binding
         WHERE binding.chain_id = $1 AND binding.block_number = $2 AND binding.block_hash = $3
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(context.chain_id)
    .bind(context.block.number)
    .bind(&context.block.hash)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the block's bound names", error))
    .map_err(in_family(tables::REGISTRY_BINDING_OBSERVATION.name))?;
    names.extend(
        events
            .iter()
            .filter(|event| matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound"))
            .filter_map(|event| event.logical_name_id.clone()),
    );
    names.sort();
    names.dedup();
    Ok(names)
}

/// Each observation identity keeps its latest observation (DISTINCT ON the identity,
/// permission_resources.rs:10-11). A named AuthorityTransferred or SubregistryChanged reaches the
/// name's current resource, else its own (:36-40, COALESCE(current_name.resource_id,
/// event.resource_id)); `target_resource_id` holds that resource as it stands after the block,
/// and a block that moves a name's current binding moves the target of the name's row with it,
/// the name's current resource before and after the block. The resource summary then takes,
/// per target resource, the latest of the rows that reach it (:52-57).
async fn observations(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::REGISTRY_BINDING_OBSERVATION;
    let chain = json!(context.chain_id);
    let relevant: Vec<(&BlockEvent, Observation<'_>)> = events
        .iter()
        .filter_map(|event| Some((event, observation(event)?)))
        .collect();
    let rebound = rebound_names(transaction, context, events).await?;
    if relevant.is_empty() && rebound.is_empty() {
        return Ok(());
    }
    let mut names: Vec<String> = relevant
        .iter()
        .filter(|(_, observation)| observation.through_name)
        .filter_map(|(_, observation)| observation.name.map(str::to_owned))
        .chain(rebound.iter().cloned())
        .collect();
    names.sort();
    names.dedup();
    let targets = current_resources(transaction, context, &names).await?;
    let keys = relevant
        .iter()
        .map(|(_, observation)| key_of(table, [chain.clone(), json!(observation.identity)]))
        .chain(
            rebound
                .iter()
                .map(|name| key_of(table, [chain.clone(), json!(name)])),
        )
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, observation) in relevant {
        let mut row = current(
            rows,
            table,
            &key_of(table, [chain.clone(), json!(observation.identity)]),
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
        let target = observation
            .through_name
            .then(|| observation.name.and_then(|name| targets.get(name)))
            .flatten()
            .map_or(observation.resource, String::as_str);
        set(
            &mut row,
            "logical_name_id",
            text_or_null(observation.name.map(str::to_owned)),
        );
        set(&mut row, "resource_id", observation.resource);
        set(
            &mut row,
            "attributed_via",
            if observation.through_name {
                "name"
            } else {
                "own"
            },
        );
        set(&mut row, "target_resource_id", target);
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
    // A name whose current binding moved: its row, when it reaches the resource through the
    // name, follows the name to its new current resource.
    for name in rebound {
        let key = key_of(table, [chain.clone(), json!(name)]);
        let Some(mut row) = rows.get(table, &key).cloned() else {
            continue;
        };
        if row.get("attributed_via").and_then(Value::as_str) != Some("name") {
            continue;
        }
        let own = row.get("resource_id").cloned().unwrap_or(Value::Null);
        let target = targets.get(&name).map_or(own, |resource| json!(resource));
        if row.get("target_resource_id") != Some(&target) {
            set(&mut row, "target_resource_id", target);
            rows.put(table, row).map_err(in_family(table.name))?;
        }
    }
    Ok(())
}
