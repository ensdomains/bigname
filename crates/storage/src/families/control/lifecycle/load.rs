//! The loaders: one statement per family table for a batch of names, reading family tables and
//! identity rows only (no normalized_events, no project_events; design:597). Every statement
//! carries a `storage:families.control.lifecycle.*` prefix for the slow log.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

use super::{Clock, NameFacts, NameInput, ShadowName, TripleFacts, admission::REGISTRAR, evaluate};
use crate::families::control::{
    position::{EventOrder, Position},
    registry::load_registry_nodes,
    rows::{BindingCandidate, LifecycleEvent, Maxima, text},
    wrapper::load_wrapper_rows,
};

async fn json_rows(
    pool: &PgPool,
    sql: &str,
    chain_id: &str,
    keys: &[String],
) -> Result<Vec<Value>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar(sql)
        .bind(chain_id)
        .bind(keys)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to run {}", sql.lines().next().unwrap_or(sql)))
}

/// The namespace of a logical name id: the part before the first `:`, or `ens` when the id
/// has none. The shadow harness's retention check reads owner events by the same rule.
pub fn namespace_of(name: &str) -> &str {
    name.split_once(':')
        .map_or("ens", |(namespace, _)| namespace)
}

/// Load and evaluate the shadow registration and control blocks of `names` at `clock`.
pub async fn load_shadow_names(
    pool: &PgPool,
    chain_id: &str,
    clock: &Clock,
    names: &[NameInput],
) -> Result<BTreeMap<String, ShadowName>> {
    let facts = load_name_facts(pool, chain_id, names).await?;
    Ok(facts
        .into_iter()
        .map(|facts| {
            let shadow = evaluate(&facts, clock);
            (facts.input.logical_name_id, shadow)
        })
        .collect())
}

/// Load every fact the lifecycle read of `names` reads.
pub async fn load_name_facts(
    pool: &PgPool,
    chain_id: &str,
    names: &[NameInput],
) -> Result<Vec<NameFacts>> {
    let ids: Vec<String> = names
        .iter()
        .map(|name| name.logical_name_id.clone())
        .collect();
    let candidates: Vec<BindingCandidate> = json_rows(
        pool,
        "/* storage:families.control.lifecycle.candidates */ SELECT to_jsonb(candidate)
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.chain_id = $1 AND candidate.logical_name_id = ANY($2)",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(BindingCandidate::from_row)
    .collect();
    let summaries = json_rows(
        pool,
        "/* storage:families.control.lifecycle.triple_summaries */ SELECT to_jsonb(summary)
         FROM bigname_phase.project_lifecycle_triple_summary summary
         WHERE summary.chain_id = $1 AND summary.logical_name_id = ANY($2)",
        chain_id,
        &ids,
    )
    .await?;
    let associations = json_rows(
        pool,
        "/* storage:families.control.lifecycle.associations */ SELECT to_jsonb(association)
         FROM bigname_phase.project_lifecycle_association association
         WHERE association.chain_id = $1 AND association.logical_name_id = ANY($2)",
        chain_id,
        &ids,
    )
    .await?;
    let triple_key = |row: &Value| -> Option<[String; 3]> {
        Some([
            text(row, "logical_name_id")?,
            text(row, "registry_identifier").unwrap_or_default(),
            text(row, "token_id").unwrap_or_default(),
        ])
    };
    let targets: BTreeMap<[String; 3], (String, Option<Position>)> = associations
        .iter()
        .filter_map(|row| {
            Some((
                triple_key(row)?,
                (text(row, "target_resource_id")?, Position::of_row(row)),
            ))
        })
        .collect();
    let mut triples: Vec<TripleFacts> = summaries
        .iter()
        .filter_map(|row| {
            let key = triple_key(row)?;
            let target = targets.get(&key).cloned();
            Some(TripleFacts {
                target: target.as_ref().map(|(resource, _)| resource.clone()),
                target_position: target.and_then(|(_, position)| position),
                key,
                maxima: Maxima::from_row(row),
            })
        })
        .collect();
    // An association whose triple has no null-resource event yet still names a key.
    for (key, (target, position)) in &targets {
        if !triples.iter().any(|triple| &triple.key == key) {
            triples.push(TripleFacts {
                key: key.clone(),
                maxima: Maxima::default(),
                target: Some(target.clone()),
                target_position: position.clone(),
            });
        }
    }

    // Resources the names' events can sit on: binding candidates and what they recorded, the
    // selections, and association targets; key states that carry a name add the rest.
    let mut resources: BTreeSet<String> = BTreeSet::new();
    for candidate in &candidates {
        resources.insert(candidate.resource_id.clone());
        resources.extend(candidate.wrapped_registrar_resource_id.clone());
        resources.extend(candidate.predecessor_resource_id.clone());
        resources.extend(candidate.lease_resource_id.clone());
    }
    for name in names {
        resources.extend(name.selection.resource_id.clone());
    }
    resources.extend(targets.values().map(|(resource, _)| resource.clone()));
    let resource_list: Vec<String> = resources.iter().cloned().collect();
    let key_state_rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.lifecycle.key_states */ SELECT to_jsonb(state)
         FROM bigname_phase.project_lifecycle_key_state state
         WHERE state.chain_id = $1
           AND (state.logical_name_id = ANY($2) OR state.resource_id = ANY($3::uuid[]))",
    )
    .bind(chain_id)
    .bind(&ids)
    .bind(&resource_list)
    .fetch_all(pool)
    .await
    .context("failed to load the lifecycle key states")?;
    let mut key_states: BTreeMap<String, (Option<String>, Maxima)> = BTreeMap::new();
    for row in &key_state_rows {
        if let Some(resource) = text(row, "resource_id") {
            resources.insert(resource.clone());
            key_states.insert(
                resource,
                (text(row, "logical_name_id"), Maxima::from_row(row)),
            );
        }
    }
    let resource_list: Vec<String> = resources.iter().cloned().collect();
    let triple_keys: Vec<String> = triples.iter().map(TripleFacts::state_key).collect();
    let event_rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.lifecycle.retained_events */ SELECT to_jsonb(event)
         FROM bigname_phase.project_lifecycle_event event
         WHERE event.chain_id = $1
           AND ((event.state_kind = 'resource' AND event.state_key = ANY($2))
                OR (event.state_kind = 'triple' AND event.state_key = ANY($3)))",
    )
    .bind(chain_id)
    .bind(&resource_list)
    .bind(&triple_keys)
    .fetch_all(pool)
    .await
    .context("failed to load the retained lifecycle events")?;
    let events: Vec<LifecycleEvent> = event_rows
        .iter()
        .filter_map(LifecycleEvent::from_row)
        .collect();
    // The candidates of every name the staging passes choose among for the unnamed registrar
    // rows loaded (decode.rs:41-106 loads the same set).
    let leases: Vec<String> = events
        .iter()
        .filter(|event| {
            event.original_logical_name_id.is_none() && event.source_family == REGISTRAR
        })
        .filter_map(|event| event.resource_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let lease_candidates: Vec<BindingCandidate> = json_rows(
        pool,
        "/* storage:families.control.lifecycle.lease_candidates */ SELECT to_jsonb(candidate)
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.chain_id = $1
           AND (candidate.resource_id::text = ANY($2)
                OR candidate.wrapped_registrar_resource_id::text = ANY($2))",
        chain_id,
        &leases,
    )
    .await?
    .iter()
    .filter_map(BindingCandidate::from_row)
    .collect();

    let wrappers = load_wrapper_rows(pool, chain_id, &resource_list).await?;
    let blocks: Vec<i64> = events
        .iter()
        .map(|event| event.position.block_number)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let block_timestamps: Arc<BTreeMap<i64, Value>> = Arc::new(
        sqlx::query_as::<_, (i64, Value)>(
            "/* storage:families.control.lifecycle.block_timestamps */
         SELECT lineage.block_number, to_jsonb(lineage.block_timestamp)
         FROM bigname_phase.chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = ANY($2)
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
        )
        .bind(chain_id)
        .bind(&blocks)
        .fetch_all(pool)
        .await
        .context("failed to load block timestamps")?
        .into_iter()
        .collect(),
    );
    let snapshots: Vec<i64> = events
        .iter()
        .filter_map(|event| event.original_registered_at)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let snapshot_timestamps: Arc<BTreeMap<i64, Value>> = Arc::new(
        sqlx::query_as::<_, (i64, Value)>(
            "/* storage:families.control.lifecycle.snapshot_timestamps */
         SELECT seconds, to_jsonb(to_timestamp(seconds)) FROM unnest($1::bigint[]) seconds",
        )
        .bind(&snapshots)
        .fetch_all(pool)
        .await
        .context("failed to convert registration times")?
        .into_iter()
        .collect(),
    );
    let starts: BTreeMap<String, Value> = sqlx::query_as::<_, (String, Value)>(
        "/* storage:families.control.lifecycle.authority_starts */
         SELECT state.logical_name_id, state.authority_start_positions
         FROM bigname_phase.project_name_state state
         WHERE state.chain_id = $1 AND state.logical_name_id = ANY($2)",
    )
    .bind(chain_id)
    .bind(&ids)
    .fetch_all(pool)
    .await
    .context("failed to load name states")?
    .into_iter()
    .collect();
    let node_keys: Vec<(String, String)> = names
        .iter()
        .map(|name| {
            (
                namespace_of(&name.logical_name_id).to_owned(),
                name.namehash.to_ascii_lowercase(),
            )
        })
        .collect();
    let nodes = load_registry_nodes(pool, chain_id, &node_keys).await?;

    let wrappers: BTreeMap<String, _> = wrappers
        .into_iter()
        .map(|row| (row.resource_id.clone(), row))
        .collect();
    let mut out = Vec::new();
    for input in names {
        let name = input.logical_name_id.as_str();
        let own: Vec<BindingCandidate> = candidates
            .iter()
            .filter(|candidate| candidate.logical_name_id == name)
            .cloned()
            .collect();
        let own_triples: Vec<TripleFacts> = triples
            .iter()
            .filter(|triple| triple.key[0] == name)
            .cloned()
            .collect();
        let mut own_resources: BTreeSet<String> = BTreeSet::new();
        for candidate in &own {
            own_resources.insert(candidate.resource_id.clone());
            own_resources.extend(candidate.wrapped_registrar_resource_id.clone());
            own_resources.extend(candidate.predecessor_resource_id.clone());
            own_resources.extend(candidate.lease_resource_id.clone());
        }
        own_resources.extend(input.selection.resource_id.clone());
        own_resources.extend(
            own_triples
                .iter()
                .filter_map(|triple| triple.target.clone()),
        );
        for (resource, (state_name, _)) in &key_states {
            if state_name.as_deref() == Some(name) {
                own_resources.insert(resource.clone());
            }
        }
        let own_triple_keys: BTreeSet<String> =
            own_triples.iter().map(TripleFacts::state_key).collect();
        let own_events: Vec<LifecycleEvent> = events
            .iter()
            .filter(|event| match event.state_kind.as_str() {
                "resource" => own_resources.contains(&event.state_key),
                _ => own_triple_keys.contains(&event.state_key),
            })
            .cloned()
            .collect();
        let own_leases: BTreeSet<&str> = own_events
            .iter()
            .filter_map(|event| event.resource_id.as_deref())
            .collect();
        out.push(NameFacts {
            input: input.clone(),
            candidates: own,
            lease_candidates: lease_candidates
                .iter()
                .filter(|candidate| {
                    own_leases.contains(candidate.resource_id.as_str())
                        || candidate
                            .wrapped_registrar_resource_id
                            .as_deref()
                            .is_some_and(|lease| own_leases.contains(lease))
                })
                .cloned()
                .collect(),
            key_states: key_states
                .iter()
                .filter(|(resource, _)| own_resources.contains(*resource))
                .map(|(resource, (_, maxima))| (resource.clone(), maxima.clone()))
                .collect(),
            triples: own_triples,
            events: own_events,
            wrappers: wrappers
                .iter()
                .filter(|(resource, _)| own_resources.contains(*resource))
                .map(|(resource, row)| (resource.clone(), row.clone()))
                .collect(),
            block_timestamps: Arc::clone(&block_timestamps),
            snapshot_timestamps: Arc::clone(&snapshot_timestamps),
            authority_starts: starts.get(name).cloned().unwrap_or(Value::Null),
            registry_node: nodes
                .get(&(
                    namespace_of(name).to_owned(),
                    input.namehash.to_ascii_lowercase(),
                ))
                .cloned(),
            order: EventOrder::Canonical,
        });
    }
    Ok(out)
}
