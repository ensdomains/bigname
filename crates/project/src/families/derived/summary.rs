//! The name summary family (`project_name_summary`, TYR-36 step 7b slice 2b): after a block (or a
//! rebuild range) writes its family rows, the summaries of the names it touched are composed
//! again from the families as they now stand, by the composed name reader's own code
//! (`bigname_storage::families::name::compose_name_summaries`) on the block's transaction, and
//! every row that changed is journalled with its before-image and written. Undo restores the
//! rows from that journal like any other family's, so it composes nothing.
//!
//! The names a block touched are read from its journal: the names, resources and registry nodes
//! of every family row it changed that a name's composition reads (the loaders of
//! `families::control::lifecycle` and `families::name`), each resource widened to the names its
//! binding candidates, lifecycle rows, wrapper row, owner events and pointer name; plus every
//! name whose surface appeared since the family marker's block, since a name is composed only
//! once it has a readable surface; plus every name whose stored `recompose_at` the block's time
//! has reached. The clock enters a composition only through the binding intervals and the
//! NameWrapper expiry masks, and a binding can close at a time ahead of the event that set it
//! (an ENSv2 path expiry), so each row stores the first second at which its composition can
//! change with no fact changing, and the first block at or past it composes the name again.
//! A registry event of any block since the family marker's also adds every name a registry event
//! of its resource carries, since it can move that resource's unnamed Transfers from one name to
//! another; a rebuild range composes once, at its last block, for all of its blocks.
//!
//! So a stored summary is refreshed when a block touches the name or its scheduled boundary
//! passes, and the work list is deliberately no wider. An input that changes in place without
//! either, such as a normalizer recompute of a surface's visibility or a lineage readability
//! flip, is covered by the rebuild: a recompute only happens with a code change that rotates
//! the interpreter fingerprint, which rebuilds the families. A reorg goes through undo, which
//! restores the summaries from the journal.
use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction, types::time::OffsetDateTime};

use crate::{
    ProjectError, Result,
    families::{block, input, store, tables::NAME_SUMMARY},
};

/// Names composed per call, to bound the working set of a rebuild range.
const CHUNK: usize = 1000;

/// What a summary refresh wrote: the `project_name_summary` rows written or removed, and the
/// undo rows journalled for them.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Refreshed {
    pub(crate) rows: u64,
    pub(crate) undo_rows: u64,
}

/// Compose again, journal and write the summaries of the names block `number` touched.
pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
) -> Result<Refreshed> {
    let block = input::read_block(transaction, chain_id, number)
        .await?
        .ok_or_else(|| {
            ProjectError::transient(format!(
                "family block {number} of chain {chain_id} is not readable for its name summaries"
            ))
        })?;
    let names: Vec<String> = sqlx::query_scalar(WORK_LIST)
        .bind(chain_id)
        .bind(number)
        .bind(block.timestamp_seconds)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the names a family block touched", error)
        })?;
    if names.is_empty() {
        return Ok(Refreshed::default());
    }
    let publication = bigname_storage::families::name::FamilyPublication {
        chain_id: chain_id.to_owned(),
        block_number: number,
        block_hash: block.hash.clone(),
        block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp_seconds).map_err(
            |error| ProjectError::data_integrity(format!("block {number} time: {error}")),
        )?,
        block_timestamp_json: block.timestamp.clone(),
    };
    let mut journal = Vec::new();
    let mut keys = Vec::new();
    let mut rows = Vec::new();
    for chunk in names.chunks(CHUNK) {
        let fresh = bigname_storage::families::name::compose_name_summary_publication(
            transaction,
            &publication,
            chunk,
        )
        .await
        .map_err(|error| {
            ProjectError::transient(format!(
                "failed to compose the name summaries of block {number} of chain {chain_id}: \
                 {error:#}"
            ))
        })?;
        if !fresh.null_resolver_names.is_empty() {
            sqlx::query(
                "/* project:families.derived.retire_null_resolver_divergences */
                UPDATE resolution_divergences
                SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
                WHERE logical_name_id = ANY($1) AND resolver_chain_id = 'ethereum-mainnet'
                  AND cleared_at IS NULL",
            )
            .bind(&fresh.null_resolver_names)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to retire null-resolver evidence", error)
            })?;
        }
        let stored: BTreeMap<String, Value> = sqlx::query_as::<_, (String, Value)>(
            "/* project:families.derived.summary_rows */ SELECT summary.logical_name_id,
                    to_jsonb(summary)
             FROM project_name_summary summary
             WHERE summary.chain_id = $1 AND summary.logical_name_id = ANY($2)",
        )
        .bind(chain_id)
        .bind(chunk)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to read the name summaries", error))?
        .into_iter()
        .collect();
        for name in chunk {
            let before = stored.get(name);
            let after = fresh.rows.get(name);
            if before == after {
                continue;
            }
            let key = json!({"chain_id": chain_id, "logical_name_id": name});
            let Value::Object(key_object) = &key else {
                unreachable!("a key is an object")
            };
            journal.push(json!({
                "family": NAME_SUMMARY.name,
                "key": store::key_text(&NAME_SUMMARY, key_object),
                "before_image": before,
            }));
            keys.push(key);
            rows.extend(after.cloned());
        }
    }
    if keys.is_empty() {
        return Ok(Refreshed::default());
    }
    let undo_rows = block::insert_journal(transaction, chain_id, &block, journal).await?;
    let rows = store::replace(transaction, &NAME_SUMMARY, keys, rows).await?;
    Ok(Refreshed { rows, undo_rows })
}

/// The names block `$2` (at time `$3`, in seconds) of chain `$1` touched, read from its journal (each changed row's
/// before-image and key) and the changed rows as they now stand. A name reads its own rows by
/// name or node (name state, triples, associations, history, registry node and pointer, owner
/// events), and the rows of every resource its candidates, key states, association targets,
/// lifecycle events and owner events sit on; each changed resource is widened to those names.
/// A registry event of the block that carries a resource adds every name a registry event of that
/// resource carries, since it can move the resource's unnamed Transfers from one to another.
const WORK_LIST: &str = r#"/* project:families.derived.summary_names */
    WITH journal AS (
        SELECT family, key::jsonb AS key, before_image
        FROM project_family_undo
        WHERE chain_id = $1 AND block_number = $2
          AND family NOT IN ('marker', 'project_name_summary')
    ),
    touched AS (
        SELECT journal.family, journal.key, journal.before_image AS row FROM journal
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(candidate)
        FROM journal JOIN project_binding_candidate candidate
          ON journal.family = 'project_binding_candidate'
         AND candidate.surface_binding_id = (journal.key ->> 0)::uuid
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(state)
        FROM journal JOIN project_lifecycle_key_state state
          ON journal.family = 'project_lifecycle_key_state'
         AND state.chain_id = $1 AND state.resource_id = (journal.key ->> 1)::uuid
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(association)
        FROM journal JOIN project_lifecycle_association association
          ON journal.family = 'project_lifecycle_association'
         AND association.chain_id = $1 AND association.logical_name_id = journal.key ->> 1
         AND association.registry_identifier = journal.key ->> 2
         AND association.token_id = journal.key ->> 3
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(event)
        FROM journal JOIN project_lifecycle_event event
          ON journal.family = 'project_lifecycle_event'
         AND event.chain_id = $1 AND event.state_kind = journal.key ->> 1
         AND event.state_key = journal.key ->> 2 AND event.event_identity = journal.key ->> 3
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(wrapper)
        FROM journal JOIN project_wrapper_state wrapper
          ON journal.family = 'project_wrapper_state'
         AND wrapper.chain_id = $1 AND wrapper.resource_id = (journal.key ->> 1)::uuid
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(event)
        FROM journal JOIN project_registry_owner_event event
          ON journal.family = 'project_registry_owner_event'
         AND event.chain_id = $1 AND event.namespace = journal.key ->> 1
         AND event.node = journal.key ->> 2 AND event.event_identity = journal.key ->> 3
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(state)
        FROM journal JOIN project_registry_node_state state
          ON journal.family = 'project_registry_node_state'
         AND state.chain_id = $1 AND state.namespace = journal.key ->> 1
         AND state.node = journal.key ->> 2
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(observation)
        FROM journal JOIN project_registry_binding_observation observation
          ON journal.family = 'project_registry_binding_observation'
         AND observation.chain_id = $1
         AND observation.observation_identity = journal.key ->> 1
        UNION ALL
        SELECT journal.family, journal.key, to_jsonb(pointer)
        FROM journal JOIN project_resource_pointer pointer
          ON journal.family = 'project_resource_pointer'
         AND pointer.chain_id = $1 AND pointer.resource_id = (journal.key ->> 1)::uuid
    ),
    named AS (
        SELECT touched.key ->> 2 AS logical_name_id FROM touched
        WHERE touched.family = 'project_name_state'
        UNION
        SELECT touched.key ->> 1 FROM touched
        WHERE touched.family IN ('project_lifecycle_triple_summary',
                                 'project_lifecycle_association', 'project_name_history')
        UNION
        SELECT (touched.key ->> 1) || ':' || lower(touched.key ->> 2) FROM touched
        WHERE touched.family IN ('project_registry_node_state', 'project_registry_owner_event',
                                 'project_registry_pointer')
        UNION
        SELECT (touched.key ->> 2)::jsonb ->> 0 FROM touched
        WHERE touched.family = 'project_lifecycle_event' AND touched.key ->> 1 = 'triple'
        UNION
        SELECT name.value FROM touched
        CROSS JOIN LATERAL (VALUES (touched.row ->> 'logical_name_id'),
                                   (touched.row ->> 'original_logical_name_id'),
                                   (touched.row ->> 'decoded_logical_name_id')) name (value)
        WHERE touched.row IS NOT NULL
        UNION
        SELECT (touched.row ->> 'namespace') || ':' || lower(touched.row ->> 'namehash')
        FROM touched
        WHERE touched.family = 'project_resource_pointer'
    ),
    resourced AS (
        SELECT DISTINCT resource.value::uuid AS resource_id
        FROM (
            SELECT touched.key ->> 1 AS value FROM touched
            WHERE touched.family IN ('project_lifecycle_key_state', 'project_wrapper_state',
                                     'project_resource_pointer')
            UNION ALL
            SELECT touched.key ->> 2 FROM touched
            WHERE touched.family = 'project_lifecycle_event'
              AND touched.key ->> 1 = 'resource'
            UNION ALL
            SELECT column_value.value FROM touched
            CROSS JOIN LATERAL (VALUES (touched.row ->> 'resource_id'),
                                       (touched.row ->> 'wrapped_registrar_resource_id'),
                                       (touched.row ->> 'predecessor_resource_id'),
                                       (touched.row ->> 'lease_resource_id'),
                                       (touched.row ->> 'target_resource_id'),
                                       (touched.row ->> 'owner_resource_id'))
                column_value (value)
            WHERE touched.row IS NOT NULL
        ) resource
        WHERE resource.value ~ '^[0-9a-fA-F-]{36}$'
    ),
    widened AS (
        SELECT candidate.logical_name_id
        FROM resourced JOIN project_binding_candidate candidate
          ON candidate.chain_id = $1 AND candidate.resource_id = resourced.resource_id
        UNION
        SELECT candidate.logical_name_id
        FROM resourced JOIN project_binding_candidate candidate
          ON candidate.chain_id = $1
         AND candidate.wrapped_registrar_resource_id = resourced.resource_id
        UNION
        SELECT candidate.logical_name_id
        FROM resourced JOIN project_binding_candidate candidate
          ON candidate.chain_id = $1
         AND candidate.predecessor_resource_id = resourced.resource_id
        UNION
        SELECT candidate.logical_name_id
        FROM resourced JOIN project_binding_candidate candidate
          ON candidate.chain_id = $1 AND candidate.lease_resource_id = resourced.resource_id
        UNION
        SELECT association.logical_name_id
        FROM resourced JOIN project_lifecycle_association association
          ON association.chain_id = $1
         AND association.target_resource_id = resourced.resource_id
        UNION
        SELECT state.logical_name_id
        FROM resourced JOIN project_lifecycle_key_state state
          ON state.chain_id = $1 AND state.resource_id = resourced.resource_id
        UNION
        SELECT name.value
        FROM resourced JOIN project_lifecycle_event event
          ON event.chain_id = $1 AND event.state_kind = 'resource'
         AND event.state_key = resourced.resource_id::text
        CROSS JOIN LATERAL (VALUES (event.original_logical_name_id),
                                   (event.decoded_logical_name_id)) name (value)
        UNION
        SELECT wrapper.logical_name_id
        FROM resourced JOIN project_wrapper_state wrapper
          ON wrapper.chain_id = $1 AND wrapper.resource_id = resourced.resource_id
        UNION
        SELECT name.value
        FROM resourced JOIN project_registry_owner_event event
          ON event.chain_id = $1 AND event.resource_id = resourced.resource_id
        CROSS JOIN LATERAL (VALUES (event.logical_name_id),
                                   (event.namespace || ':' || lower(event.node))) name (value)
        UNION
        SELECT pointer.namespace || ':' || lower(pointer.namehash)
        FROM resourced JOIN project_resource_pointer pointer
          ON pointer.chain_id = $1 AND pointer.resource_id = resourced.resource_id
    ),
    -- A registry event that carries a resource can move that resource's unnamed Transfers to
    -- another name: the zero-owner attribution links them to the resource's latest named
    -- registry event of any kind (the served `project_latest_registry_owner`). Every name a
    -- registry event of the resource carries is composed again. The events are those of every
    -- block since the family marker's, as for `surfaced`: a rebuild range composes once, at its
    -- last block, for all of its blocks.
    linked AS (
        SELECT DISTINCT carried.logical_name_id
        FROM normalized_events registry_event
        LEFT JOIN project_family_marker marker ON marker.chain_id = registry_event.chain_id
        JOIN normalized_events carried
          ON carried.resource_id = registry_event.resource_id
         AND carried.chain_id = $1
         AND carried.source_family = registry_event.source_family
         AND carried.logical_name_id IS NOT NULL
         AND carried.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE registry_event.chain_id = $1 AND registry_event.block_number <= $2
          AND registry_event.block_number > COALESCE(marker.current_block_number, -1)
          AND registry_event.resource_id IS NOT NULL
          AND registry_event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
    ),
    -- A name whose composition the clock changes by this block's time.
    clocked AS (
        SELECT summary.logical_name_id
        FROM project_name_summary summary
        WHERE summary.chain_id = $1 AND summary.recompose_at <= $3
    ),
    -- A name is composed once it has a surface: every surface since the marker's block.
    surfaced AS (
        SELECT surface.logical_name_id
        FROM name_surfaces surface
        LEFT JOIN project_family_marker marker ON marker.chain_id = surface.chain_id
        WHERE surface.chain_id = $1 AND surface.block_number <= $2
          AND surface.block_number > COALESCE(marker.current_block_number, -1)
    )
    SELECT logical_name_id FROM (
        SELECT logical_name_id FROM named
        UNION SELECT logical_name_id FROM widened
        UNION SELECT logical_name_id FROM linked
        UNION SELECT logical_name_id FROM clocked
        UNION SELECT logical_name_id FROM surfaced
    ) every
    WHERE logical_name_id IS NOT NULL AND logical_name_id <> ''
    ORDER BY logical_name_id"#;
