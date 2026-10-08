//! The loaders: one statement per family table for a batch of names, reading family tables and
//! identity rows only (no normalized_events). Every statement
//! carries a `storage:families.control.lifecycle.*` prefix for the slow log.
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, PgPool};

use super::{NameFacts, NameInput, TripleFacts, admission::REGISTRAR};
use crate::families::control::{
    cutover::load_cut_over_on,
    position::Position,
    registry::load_registry_nodes_on,
    rows::{BindingCandidate, LifecycleEvent, Maxima, text},
    wrapper::load_wrapper_rows,
};

async fn json_rows(
    conn: &mut PgConnection,
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
        .fetch_all(conn)
        .await
        .with_context(|| format!("failed to run {}", sql.lines().next().unwrap_or(sql)))
}

/// The binding candidates on chain `$1` whose resource or wrapped registrar resource is one of
/// the leases `$2`.
pub(crate) const LEASE_CANDIDATES_SQL: &str =
    "/* storage:families.control.lifecycle.lease_candidates */ SELECT to_jsonb(candidate)
     FROM bigname_phase.project_binding_candidate candidate
     WHERE candidate.chain_id = $1
       AND (candidate.resource_id = ANY($2::uuid[])
            OR candidate.wrapped_registrar_resource_id = ANY($2::uuid[])
            OR candidate.lease_resource_id = ANY($2::uuid[]))";

/// The lifecycle key states on chain `$1` of the names `$2` or the resources `$3`: a BitmapOr of
/// `project_lifecycle_key_state_name_idx` and the primary key.
pub(crate) const KEY_STATES_SQL: &str =
    "/* storage:families.control.lifecycle.key_states */ SELECT to_jsonb(state)
     FROM bigname_phase.project_lifecycle_key_state state
     WHERE state.chain_id = $1
       AND (state.logical_name_id = ANY($2) OR state.resource_id = ANY($3::uuid[]))";

/// The authority epoch starts and latest ENSv1→ENSv2 migration position on chain `$1` of the
/// names `$2`, by `project_name_state_name_idx` (the primary key leads with the namespace, which
/// the names do not bind).
pub(crate) const AUTHORITY_STARTS_SQL: &str =
    "/* storage:families.control.lifecycle.authority_starts */
     SELECT state.logical_name_id, state.authority_start_positions, state.migration_position
     FROM bigname_phase.project_name_state state
     WHERE state.chain_id = $1 AND state.logical_name_id = ANY($2)";

/// The namespace of a logical name id: the part before the first `:`, or `ens` when the id
/// has none.
pub fn namespace_of(name: &str) -> &str {
    name.split_once(':')
        .map_or("ens", |(namespace, _)| namespace)
}

/// Load every fact the lifecycle read of `names` reads.
pub async fn load_name_facts(
    pool: &PgPool,
    chain_id: &str,
    names: &[NameInput],
) -> Result<Vec<NameFacts>> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire a connection for the lifecycle facts")?;
    load_name_facts_on(&mut conn, chain_id, names).await
}

/// [`load_name_facts`] on one connection, so a caller's transaction reads every statement in
/// its snapshot (the composed name reader, `families::name`).
pub async fn load_name_facts_on(
    conn: &mut PgConnection,
    chain_id: &str,
    names: &[NameInput],
) -> Result<Vec<NameFacts>> {
    let ids: Vec<String> = names
        .iter()
        .map(|name| name.logical_name_id.clone())
        .collect();
    let candidates: Vec<BindingCandidate> = json_rows(
        &mut *conn,
        "/* storage:families.control.lifecycle.candidates */ SELECT to_jsonb(candidate)
             || jsonb_build_object(
                 'binding_active_from', extract(epoch FROM binding.active_from)::float8,
                 'binding_active_to', extract(epoch FROM binding.active_to)::float8)
         FROM bigname_phase.project_binding_candidate candidate
         LEFT JOIN bigname_phase.surface_bindings binding
           ON binding.surface_binding_id = candidate.surface_binding_id
         WHERE candidate.chain_id = $1 AND candidate.logical_name_id = ANY($2)",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(BindingCandidate::from_row)
    .collect();
    let summaries = json_rows(
        &mut *conn,
        "/* storage:families.control.lifecycle.triple_summaries */ SELECT to_jsonb(summary)
         FROM bigname_phase.project_lifecycle_triple_summary summary
         WHERE summary.chain_id = $1 AND summary.logical_name_id = ANY($2)",
        chain_id,
        &ids,
    )
    .await?;
    let associations = json_rows(
        &mut *conn,
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
    let summarized: BTreeSet<[String; 3]> =
        triples.iter().map(|triple| triple.key.clone()).collect();
    for (key, (target, position)) in &targets {
        if !summarized.contains(key) {
            triples.push(TripleFacts {
                key: key.clone(),
                maxima: Maxima::default(),
                target: Some(target.clone()),
                target_position: position.clone(),
            });
        }
    }

    let grace_registries = Arc::new(super::policy::load(conn, chain_id, &triples).await?);

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
    let key_state_rows: Vec<Value> = sqlx::query_scalar(KEY_STATES_SQL)
        .bind(chain_id)
        .bind(&ids)
        .bind(&resource_list)
        .fetch_all(&mut *conn)
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
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the retained lifecycle events")?;
    let events: Vec<LifecycleEvent> = event_rows
        .iter()
        .filter_map(LifecycleEvent::from_row)
        .collect();
    // The candidates of every name the staging passes choose among for the unnamed registrar
    // rows loaded (crates/project/src/families/decode.rs loads the same set).
    let leases: Vec<String> = events
        .iter()
        .filter(|event| {
            event.original_logical_name_id.is_none() && event.source_family == REGISTRAR
        })
        .filter_map(|event| event.resource_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let lease_candidates: Vec<BindingCandidate> =
        json_rows(&mut *conn, LEASE_CANDIDATES_SQL, chain_id, &leases)
            .await?
            .iter()
            .filter_map(BindingCandidate::from_row)
            .collect();

    let wrappers = load_wrapper_rows(&mut *conn, chain_id, &resource_list).await?;
    let blocks: Vec<i64> = events
        .iter()
        .map(|event| event.position.block_number)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    // Each retained event's block is on the canonical lineage, so its canonical row at that
    // height is the row whose timestamp the reads take for that block.
    let block_rows = sqlx::query_as::<_, (i64, Value, i64)>(
        "/* storage:families.control.lifecycle.block_timestamps */
         SELECT lineage.block_number, to_jsonb(lineage.block_timestamp),
                floor(extract(epoch FROM lineage.block_timestamp))::bigint
         FROM bigname_phase.chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = ANY($2)
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(&blocks)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load block timestamps")?;
    let block_seconds: Arc<BTreeMap<i64, i64>> = Arc::new(
        block_rows
            .iter()
            .map(|(block, _, seconds)| (*block, *seconds))
            .collect(),
    );
    let block_timestamps: Arc<BTreeMap<i64, Value>> = Arc::new(
        block_rows
            .into_iter()
            .map(|(block, timestamp, _)| (block, timestamp))
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
        .fetch_all(&mut *conn)
        .await
        .context("failed to convert registration times")?
        .into_iter()
        .collect(),
    );
    let states: BTreeMap<String, (Value, Option<Value>)> =
        sqlx::query_as::<_, (String, Value, Option<Value>)>(AUTHORITY_STARTS_SQL)
            .bind(chain_id)
            .bind(&ids)
            .fetch_all(&mut *conn)
            .await
            .context("failed to load name states")?
            .into_iter()
            .map(|(name, starts, migration)| (name, (starts, migration)))
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
    let nodes = load_registry_nodes_on(&mut *conn, chain_id, &node_keys).await?;
    let resolution_cutover = load_cut_over_on(&mut *conn, chain_id).await?;

    let wrappers: BTreeMap<String, _> = wrappers
        .into_iter()
        .map(|row| (row.resource_id.clone(), row))
        .collect();
    // Each name takes its own rows from the batch through these indexes, which keep the batch's
    // order, so the work is linear in the batch rather than one scan of it per name.
    let mut candidates_of: HashMap<&str, Vec<&BindingCandidate>> = HashMap::new();
    for candidate in &candidates {
        candidates_of
            .entry(candidate.logical_name_id.as_str())
            .or_default()
            .push(candidate);
    }
    let mut triples_of: HashMap<&str, Vec<&TripleFacts>> = HashMap::new();
    for triple in &triples {
        triples_of
            .entry(triple.key[0].as_str())
            .or_default()
            .push(triple);
    }
    let mut key_states_of: HashMap<&str, Vec<&String>> = HashMap::new();
    for (resource, (state_name, _)) in &key_states {
        if let Some(state_name) = state_name {
            key_states_of
                .entry(state_name.as_str())
                .or_default()
                .push(resource);
        }
    }
    // Event positions in the batch by the state they sit on: resource states, and every other
    // kind by its triple key.
    let mut resource_events: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut triple_events: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        let by_state = if event.state_kind == "resource" {
            &mut resource_events
        } else {
            &mut triple_events
        };
        by_state
            .entry(event.state_key.as_str())
            .or_default()
            .push(index);
    }
    // Lease candidate positions by the resource and the wrapped registrar resource they carry.
    let mut lease_positions: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, candidate) in lease_candidates.iter().enumerate() {
        lease_positions
            .entry(candidate.resource_id.as_str())
            .or_default()
            .push(index);
        if let Some(lease) = candidate.wrapped_registrar_resource_id.as_deref() {
            lease_positions.entry(lease).or_default().push(index);
        }
        if let Some(lease) = candidate.lease_resource_id.as_deref() {
            lease_positions.entry(lease).or_default().push(index);
        }
    }
    let mut out = Vec::new();
    for input in names {
        let name = input.logical_name_id.as_str();
        let own: Vec<BindingCandidate> = candidates_of
            .get(name)
            .into_iter()
            .flatten()
            .map(|candidate| (*candidate).clone())
            .collect();
        let own_triples: Vec<TripleFacts> = triples_of
            .get(name)
            .into_iter()
            .flatten()
            .map(|triple| (*triple).clone())
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
        for resource in key_states_of.get(name).into_iter().flatten() {
            own_resources.insert((*resource).clone());
        }
        let own_triple_keys: BTreeSet<String> =
            own_triples.iter().map(TripleFacts::state_key).collect();
        let own_event_positions: BTreeSet<usize> = own_resources
            .iter()
            .filter_map(|resource| resource_events.get(resource.as_str()))
            .chain(
                own_triple_keys
                    .iter()
                    .filter_map(|key| triple_events.get(key.as_str())),
            )
            .flatten()
            .copied()
            .collect();
        let own_events: Vec<LifecycleEvent> = own_event_positions
            .into_iter()
            .map(|index| events[index].clone())
            .collect();
        let own_leases: BTreeSet<&str> = own_events
            .iter()
            .filter_map(|event| event.resource_id.as_deref())
            .collect();
        let own_lease_positions: BTreeSet<usize> = own_leases
            .iter()
            .filter_map(|lease| lease_positions.get(lease))
            .flatten()
            .copied()
            .collect();
        out.push(NameFacts {
            input: input.clone(),
            candidates: own,
            lease_candidates: own_lease_positions
                .into_iter()
                .map(|index| lease_candidates[index].clone())
                .collect(),
            key_states: own_resources
                .iter()
                .filter_map(|resource| {
                    key_states
                        .get(resource)
                        .map(|(_, maxima)| (resource.clone(), maxima.clone()))
                })
                .collect(),
            triples: own_triples,
            events: own_events,
            wrappers: own_resources
                .iter()
                .filter_map(|resource| {
                    wrappers
                        .get(resource)
                        .map(|row| (resource.clone(), row.clone()))
                })
                .collect(),
            block_timestamps: Arc::clone(&block_timestamps),
            block_seconds: Arc::clone(&block_seconds),
            snapshot_timestamps: Arc::clone(&snapshot_timestamps),
            authority_starts: states
                .get(name)
                .map_or(Value::Null, |(starts, _)| starts.clone()),
            migration: states
                .get(name)
                .and_then(|(_, migration)| migration.as_ref())
                .and_then(Position::from_json),
            registry_node: nodes
                .get(&(
                    namespace_of(name).to_owned(),
                    input.namehash.to_ascii_lowercase(),
                ))
                .cloned(),
            resolution_cutover,
            grace_registries: Arc::clone(&grace_registries),
        });
    }
    Ok(out)
}
