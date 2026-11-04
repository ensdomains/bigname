//! The child listing family's work list (`children.rs`): the children and the parents a family
//! block can have moved.
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, families::input};

/// The names whose listing rows block `$2` (at time `$4`, in seconds) of chain `$1` can have
/// moved, after the family marker's block `$3`: `child` rows are children derived again under
/// every parent that can list them, `parent` rows are parents derived whole. `$5` holds the names
/// the block's summary refresh composed and `$6` the names whose raw name it swept.
const WORK_LIST: &str = r#"/* project:families.derived.child_listing_work */
    WITH journal AS MATERIALIZED (
        SELECT family, key::jsonb AS key, before_image
        FROM project_family_undo
        WHERE chain_id = $1 AND block_number = $2
          AND family IN ('project_name_summary', 'project_child_edge_candidate',
                         'project_child_registration_state', 'project_registry_node_state',
                         'project_wrapper_state', 'project_lifecycle_event', 'project_name_state',
                         'project_parent_subregistry')
    ),
    -- Every join from the journal to a family table probes that table's key, one journal row at a
    -- time (`OFFSET 0` keeps each probe its own subquery). The journal's rows are this block's,
    -- written in this transaction after the last ANALYZE, so the planner can count them as one
    -- row and otherwise scan a family table whole for each.
    lifecycle AS (
        SELECT journal.before_image AS row FROM journal
        WHERE journal.family = 'project_lifecycle_event' AND journal.before_image IS NOT NULL
        UNION ALL
        SELECT probe.row FROM journal
        CROSS JOIN LATERAL (
            SELECT to_jsonb(event) AS row FROM project_lifecycle_event event
            WHERE event.chain_id = $1 AND event.state_kind = journal.key ->> 1
              AND event.state_key = journal.key ->> 2 AND event.event_identity = journal.key ->> 3
            OFFSET 0
        ) probe
        WHERE journal.family = 'project_lifecycle_event'
    ),
    -- Each observation state is probed on its own, as the summary work list's `revealed` does,
    -- so `normalized_events_projection_idx` bounds the range to the blocks. An observation of a
    -- label with no name has no `logical_name_id` and carries its label hash in its after state.
    revealed AS (
        SELECT revealed_event.logical_name_id, revealed_event.labelhash
        FROM unnest('{canonical,safe,finalized}'::canonicality_state[]) AS state(canonicality_state)
        CROSS JOIN LATERAL (
            SELECT observed.logical_name_id, observed.after_state ->> 'labelhash' AS labelhash
            FROM normalized_events observed
            WHERE observed.event_kind = 'PreimageObserved'
              AND observed.canonicality_state = state.canonicality_state
              AND observed.chain_id = $1
              AND observed.block_number > $3 AND observed.block_number <= $2
            OFFSET 0
        ) revealed_event
    ),
    -- The observed labels, as the stored rows keep them: those of each observed name's surface,
    -- and the label an observation names directly. A label observed for one name spells every
    -- other name that carries it.
    observed AS MATERIALIZED (
        SELECT array_agg(DISTINCT label.labelhash) AS labelhashes
        FROM (
            SELECT lower(surface_label.labelhash) AS labelhash
            FROM revealed
            JOIN name_surfaces surface ON surface.logical_name_id = revealed.logical_name_id
            CROSS JOIN LATERAL unnest(surface.labelhashes) surface_label (labelhash)
            UNION ALL
            SELECT lower(revealed.labelhash) FROM revealed WHERE revealed.labelhash IS NOT NULL
        ) label
    ),
    children AS (
        SELECT name.id FROM unnest($5::text[]) name (id)
        UNION SELECT journal.key ->> 1 FROM journal WHERE journal.family = 'project_name_summary'
        UNION SELECT (journal.key ->> 1) || ':' || (journal.key ->> 3) FROM journal
        WHERE journal.family = 'project_child_edge_candidate'
        UNION SELECT journal.key ->> 1 FROM journal
        WHERE journal.family = 'project_child_registration_state'
        UNION SELECT (journal.key ->> 1) || ':' || lower(journal.key ->> 2) FROM journal
        WHERE journal.family = 'project_registry_node_state'
        UNION SELECT journal.before_image ->> 'logical_name_id' FROM journal
        WHERE journal.family = 'project_wrapper_state'
        UNION SELECT wrapper.logical_name_id FROM journal
        CROSS JOIN LATERAL (
            SELECT wrapper.logical_name_id FROM project_wrapper_state wrapper
            WHERE wrapper.chain_id = $1 AND wrapper.resource_id = (journal.key ->> 1)::uuid
            OFFSET 0
        ) wrapper
        WHERE journal.family = 'project_wrapper_state'
        -- A registrar lease row names its node; the relation reads it by node
        -- (children_page/child_flags.rs), under the registrar's namespace.
        UNION SELECT CASE lifecycle.row ->> 'source_family'
                         WHEN 'ens_v1_registrar_l1' THEN 'ens'
                         WHEN 'basenames_base_registrar' THEN 'basenames' END
                     || ':' || lower(lifecycle.row ->> 'namehash')
        FROM lifecycle
        UNION SELECT revealed.logical_name_id FROM revealed
        -- A stored row whose name still spells an observed label as `[labelhash]`, its own or
        -- its parent's, under any parent (project_child_listing_unhealed_idx). Only those rows
        -- can spell it: a row whose name holds none of the labels keeps its name, and nothing
        -- is derived by parent.
        UNION SELECT listing.child_logical_name_id FROM project_child_listing listing
        WHERE listing.chain_id = $1 AND listing.unhealed_labelhashes <> '{}'
          AND listing.unhealed_labelhashes && (SELECT observed.labelhashes FROM observed)
        UNION SELECT listing.child_logical_name_id FROM project_child_listing listing
        WHERE listing.chain_id = $1 AND listing.recompose_at <= $4
    ),
    parents AS (
        -- The migration columns the gate reads (topology/children.rs, `parent_migration`).
        SELECT journal.key ->> 2 AS id
        FROM journal
        LEFT JOIN LATERAL (
            SELECT state.migration_path, state.migration_evidence, state.migration_position
            FROM project_name_state state
            WHERE state.chain_id = $1 AND state.namespace = journal.key ->> 1
              AND state.logical_name_id = journal.key ->> 2
            OFFSET 0
        ) state ON true
        WHERE journal.family = 'project_name_state'
          AND jsonb_build_array(
                  COALESCE(journal.before_image, '{}'::jsonb) ->> 'migration_path',
                  COALESCE(journal.before_image, '{}'::jsonb) -> 'migration_evidence',
                  COALESCE(journal.before_image, '{}'::jsonb) -> 'migration_position')
              IS DISTINCT FROM jsonb_build_array(
                  state.migration_path, state.migration_evidence, state.migration_position)
        UNION SELECT journal.key ->> 1 FROM journal
        WHERE journal.family = 'project_parent_subregistry'
        UNION SELECT name.id FROM unnest($6::text[]) name (id)
        UNION SELECT surface.logical_name_id FROM name_surfaces surface
        WHERE surface.chain_id = $1 AND surface.block_number > $3 AND surface.block_number <= $2
        UNION SELECT pointer.logical_name_id
        FROM contract_instance_addresses address
        JOIN project_parent_subregistry pointer
          ON pointer.chain_id = $1 AND pointer.subregistry_address = lower(address.address)
        WHERE address.chain_id = $1
          AND ((address.active_from_block_number > $3 AND address.active_from_block_number <= $2)
               OR (address.active_to_block_number > $3 AND address.active_to_block_number <= $2))
    )
    SELECT 'child', children.id FROM children
    WHERE children.id IS NOT NULL AND children.id LIKE '%:%'
    UNION
    SELECT 'parent', parents.id FROM parents
    WHERE parents.id IS NOT NULL
      AND (EXISTS (SELECT 1 FROM project_child_edge_candidate edge
                   WHERE edge.chain_id = $1 AND edge.namespace = split_part(parents.id, ':', 1)
                     AND edge.parent_node = lower(split_part(parents.id, ':', 2)))
           OR EXISTS (SELECT 1 FROM project_parent_subregistry pointer
                      WHERE pointer.chain_id = $1 AND pointer.logical_name_id = parents.id)
           OR EXISTS (SELECT 1 FROM project_child_listing listing
                      WHERE listing.chain_id = $1
                        AND listing.parent_logical_name_id = parents.id))
    ORDER BY 1, 2"#;

/// The work list of block `block`, which follows the family marker's block `after` (-1 with
/// none): the children to derive again and the parents to derive whole. `names` are the names the
/// block's summary refresh composed and `swept` those whose raw name it swept.
pub(super) async fn list(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    after: i64,
    (names, swept): (&[String], &[String]),
) -> Result<(Vec<String>, Vec<String>)> {
    let rows: Vec<(String, String)> = sqlx::query_as(WORK_LIST)
        .bind(chain_id)
        .bind(block.number)
        .bind(after)
        .bind(block.timestamp_seconds)
        .bind(names)
        .bind(swept)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the children a family block touched", error)
        })?;
    let mut children = Vec::new();
    let mut parents = Vec::new();
    for (kind, id) in rows {
        if kind == "parent" {
            parents.push(id);
        } else {
            children.push(id);
        }
    }
    Ok((children, parents))
}

#[cfg(test)]
#[path = "work_tests.rs"]
mod tests;
