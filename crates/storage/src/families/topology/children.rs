//! The declared direct children of one parent, read from the family tables: the edge candidates
//! (`project_child_edge_candidate`) and parent subregistry (`project_parent_subregistry`), the
//! per-registry child registrations (`project_child_registration_state`), child wrapper fuses
//! (`project_wrapper_state`) and the parent's migration state (`project_name_state`), with the
//! identity tables, label preimages and the children's name summaries
//! (`project_name_summary`: selected arm, serving resource, zero-owner transfer). It builds the
//! served child relation (the edge candidates, then the arm selection) and the read filter of
//! crates/storage/src/children/page.rs, evaluated at read against the family marker's block: no
//! stored eligibility and no maintained count.
use sqlx::{Postgres, QueryBuilder};

use super::{
    name_summary::{selected_authority_arm, serving, zero_owner},
    shims::{effective_child_fuses, row_position},
};

pub(super) const READABLE: &str = "('canonical', 'safe', 'finalized')";
const ZERO_ADDRESS: &str = "'0x0000000000000000000000000000000000000000'";

/// A surface or edge row is readable history on a readable block at or below the clock.
fn readable_surface(alias: &str) -> String {
    format!(
        "{alias}.canonicality_state::text IN {READABLE}
         AND {alias}.block_number <= clock.block_number
         AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage {alias}_lineage
                     WHERE {alias}_lineage.chain_id = {alias}.chain_id
                       AND {alias}_lineage.block_hash = {alias}.block_hash
                       AND {alias}_lineage.block_number = {alias}.block_number
                       AND {alias}_lineage.canonicality_state::text IN {READABLE})"
    )
}

/// The contract instance active at the clock block for the subregistry address `address`.
fn active_instance(alias: &str, chain: &str, address: &str) -> String {
    format!(
        "{alias}.chain_id = {chain} AND lower({alias}.address) = {address}
         AND ({alias}.active_from_block_number IS NULL
              OR {alias}.active_from_block_number <= clock.block_number)
         AND ({alias}.active_to_block_number IS NULL
              OR {alias}.active_to_block_number > clock.block_number)
         AND {alias}.deactivated_at IS NULL"
    )
}

/// The parents a child read covers: one, for a page, or many, for the per-parent counts.
#[derive(Clone, Copy)]
pub(super) enum Parents<'a> {
    One(&'a str),
    Many(&'a [String]),
}

/// Push `clock`, `parent`, `parent_migration`, `candidates` and `selected` CTE definitions (each
/// followed by a comma) for `parents`. `clock` holds one row per chain of the parents, and
/// `selected` one row per served child with `pair_rank = 1`, before the page read filter, with
/// its parent as `parent_logical_name_id`; an ENSv2 child carries the emitter of its
/// registration event as `registry_address`, others null.
pub(super) fn push_selected<'a>(builder: &mut QueryBuilder<'a, Postgres>, parents: Parents<'a>) {
    builder.push(
        "parent_surface AS (
            SELECT surface.* FROM bigname_phase.name_surfaces surface
            WHERE surface.logical_name_id = ",
    );
    match parents {
        Parents::One(parent) => {
            builder.push_bind(parent);
        }
        Parents::Many(parents) => {
            builder.push("ANY(");
            builder.push_bind(parents);
            builder.push(")");
        }
    }
    builder.push(
        "
        ), clock AS (
            -- The clock: the family marker's block of each parent's chain, never NOW(), when
            -- the marker is servable by the fence's rule (`servable_family_marker`,
            -- snapshot_selection/project.rs): live, written by this build, on a readable block.
            -- The readers check that first (`children_page::require_publication`); without a
            -- servable marker there is no clock and no child.
            SELECT DISTINCT marker.chain_id, marker.current_block_number AS block_number,
                   marker.block_timestamp,
                   extract(epoch FROM marker.block_timestamp) AS epoch_seconds
            FROM bigname_phase.project_family_marker marker
            JOIN parent_surface ON parent_surface.chain_id = marker.chain_id
            JOIN bigname_phase.chain_lineage marker_lineage
              ON marker_lineage.chain_id = marker.chain_id
             AND marker_lineage.block_number = marker.current_block_number
             AND marker_lineage.block_hash = marker.current_block_hash
             AND marker_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            WHERE marker.state = 'live' AND marker.input_content_hash = ",
    );
    builder.push_bind(bigname_content_hash::INTERPRETER_CONTENT_HASH);
    builder.push(format!(
        "
        ), parent AS (
            SELECT surface.logical_name_id, surface.namespace, surface.chain_id, surface.raw_name,
                   lower(surface.namehash) AS node, surface.labelhashes
            FROM parent_surface surface JOIN clock ON clock.chain_id = surface.chain_id
            WHERE surface.visibility_state = 'active' AND {parent_readable}
        ), parent_migration AS (
            -- The ENSv1 migration gate: a migrated parent's migration path and migration
            -- registry, which decide below which ENSv1 edges the parent still lists.
            SELECT parent.logical_name_id AS parent_logical_name_id, state.migration_path,
                   registry.registry_contract_instance_id::text
                       AS migration_registry_contract_instance_id
            FROM parent JOIN clock ON clock.chain_id = parent.chain_id
            JOIN bigname_phase.project_name_state state
              ON state.chain_id = parent.chain_id
             AND state.namespace = parent.namespace
             AND state.logical_name_id = parent.logical_name_id
             AND state.migration_position IS NOT NULL
            LEFT JOIN bigname_phase.project_parent_subregistry subregistry
              ON subregistry.chain_id = parent.chain_id
             AND subregistry.logical_name_id = parent.logical_name_id
             AND subregistry.subregistry_address NOT IN ('', {ZERO_ADDRESS})
            LEFT JOIN bigname_phase.contract_instance_addresses address
              ON {migration_address}
            LEFT JOIN LATERAL (
                SELECT association.registry_contract_instance_id
                FROM bigname_phase.migration_discovery_associations association
                JOIN bigname_phase.manifest_versions manifest
                  ON (manifest.manifest_id, manifest.chain_id) =
                     (association.source_manifest_id, association.chain_id)
                WHERE association.chain_id = state.chain_id
                  AND association.registry_contract_instance_id = address.contract_instance_id
                  AND lower(association.registry_address) = subregistry.subregistry_address
                  AND association.correlation_kind = 'migration_registry_creation'
                  AND association.canonicality_state::text IN {READABLE}
                  AND jsonb_array_length(association.evidence_refs) > 0
                  AND NOT EXISTS (
                      SELECT 1 FROM jsonb_array_elements(association.evidence_refs) evidence(reference)
                      WHERE jsonb_typeof(evidence.reference) <> 'object'
                         OR evidence.reference = '{{}}'::jsonb)
                  AND state.migration_evidence @> association.evidence_refs
                  AND EXISTS (
                      SELECT 1 FROM bigname_phase.chain_lineage lineage
                      WHERE lineage.chain_id = association.chain_id
                        AND lineage.block_hash = association.block_hash
                        AND lineage.block_number = association.block_number
                        AND lineage.canonicality_state::text IN {READABLE})
                  AND EXISTS (
                      SELECT 1 FROM bigname_phase.discovery_edges edge
                      WHERE edge.chain_id = association.chain_id
                        AND edge.edge_kind = 'registry_announcement'
                        AND edge.to_contract_instance_id = association.registry_contract_instance_id
                        AND edge.source_manifest_id = association.source_manifest_id
                        AND (edge.active_from_block_number, edge.active_from_block_hash) =
                            (association.block_number, association.block_hash)
                        AND (edge.provenance ->> 'transaction_index')::bigint =
                            association.transaction_index
                        AND (edge.provenance ->> 'log_index')::bigint = association.log_index
                        AND edge.canonicality_state::text IN {READABLE}
                        AND edge.active_from_block_number <= clock.block_number
                        AND edge.deactivated_at IS NULL
                        AND (edge.active_to_block_number IS NULL
                             OR edge.active_to_block_number > clock.block_number))
                ORDER BY association.logical_edge_identity DESC,
                         association.migration_correlation_id DESC
                LIMIT 1
            ) registry ON TRUE
        ), v1_edges AS (
            -- The latest edge for the child across parents and arms (ranked_v1), so a child is
            -- never served under a parent it has left.
            -- The owner is the child node's current registry owner, `owner(node)`: the node's
            -- latest owner-setting registry event (F2c, `project_registry_node_state`), so a
            -- Transfer after the NewOwner moves it. That is the registry's owner getter view
            -- (zero for an owner the registry reads as zero), else the reported owner. An
            -- unmasked LLL owner word serves its low 20 bytes as the display owner, as the
            -- fallback registry's typed read returns it (docs/architecture.md, the 2017
            -- registry); the admitted Graveyard holding a record it claimed or cleared names
            -- no owner. The edge's own owner serves only a node
            -- with no F2c row, which the NewOwner that writes the edge always writes. A zero
            -- owner in the child's name summary (its node's latest registry transfer names the
            -- zero owner) still overrides.
            SELECT edge.*, parent.logical_name_id AS edge_parent_logical_name_id,
                   parent.namespace || ':' || edge.child_node AS child_logical_name_id,
                   CASE WHEN {zero_owner} THEN {ZERO_ADDRESS}
                        WHEN node_state.node IS NULL
                            THEN lower(COALESCE(edge.owner_getter, edge.owner))
                        WHEN node_state.owner_word_unmasked THEN lower(node_state.owner)
                        WHEN node_state.owner_getter_reason = 'graveyard' THEN NULL
                        ELSE lower(COALESCE(node_state.owner_getter, node_state.registry_owner,
                                            node_state.owner))
                   END AS served_owner
            FROM parent JOIN clock ON clock.chain_id = parent.chain_id
            JOIN bigname_phase.project_child_edge_candidate edge
              ON edge.chain_id = parent.chain_id AND edge.namespace = parent.namespace
             AND edge.parent_node = parent.node
            LEFT JOIN bigname_phase.project_registry_node_state node_state
              ON node_state.chain_id = edge.chain_id AND node_state.namespace = edge.namespace
             AND node_state.node = edge.child_node
            WHERE NOT EXISTS (
                SELECT 1 FROM bigname_phase.project_child_edge_candidate other
                WHERE other.chain_id = edge.chain_id AND other.namespace = edge.namespace
                  AND other.child_node = edge.child_node
                  AND {other_position} > {edge_position})
        ), candidates AS (
            SELECT parent.logical_name_id AS parent_logical_name_id, parent.chain_id,
                   edge.child_logical_name_id, edge.namespace,
                   {v1_raw_name} AS raw_name, {v1_decoded_name} AS decoded_name,
                   edge.child_node AS namehash, edge.labelhash, edge.served_owner AS owner,
                   NULL::text AS registrant,
                   CASE WHEN edge.source_family = 'basenames_base_registry' THEN 'basenames'
                        ELSE 'ens_v1' END AS authority_arm,
                   edge.block_number, edge.transaction_index, edge.log_index, edge.event_identity,
                   NULL::text AS registry_address
            FROM v1_edges edge
            JOIN parent ON parent.logical_name_id = edge.edge_parent_logical_name_id
            JOIN clock ON clock.chain_id = parent.chain_id
            LEFT JOIN bigname_phase.label_preimages preimage ON preimage.labelhash = edge.labelhash
            LEFT JOIN parent_migration migration
              ON migration.parent_logical_name_id = parent.logical_name_id
            WHERE (COALESCE(edge.served_owner, '') NOT IN ('', {ZERO_ADDRESS})
                   OR {serving})
              AND (edge.source_family <> 'ens_v1_registry_l1'
                   OR migration.migration_path IS NULL
                   OR (migration.migration_path IN ('locked_wrapped', 'locked_child')
                       AND migration.migration_registry_contract_instance_id IS NOT NULL
                       AND ({fuses} & 196608) = 65536
                       AND COALESCE(edge.served_owner, '') NOT IN ('', {ZERO_ADDRESS})
                       AND NOT EXISTS (
                           SELECT 1 FROM bigname_phase.project_child_registration_state history
                           WHERE history.chain_id = edge.chain_id
                             AND history.logical_name_id = edge.child_logical_name_id
                             AND history.registry_contract_instance_id =
                                 migration.migration_registry_contract_instance_id
                             AND history.exists)))
            UNION ALL
            SELECT parent.logical_name_id, parent.chain_id, child.logical_name_id, child.namespace,
                   {v2_raw_name}, {v2_decoded_name},
                   child.namehash, lower(child.labelhashes[1]), NULL::text,
                   registration.registrant, 'ens_v2',
                   GREATEST(registration.block_number, subregistry.block_number),
                   registration.transaction_index, registration.log_index,
                   registration.event_identity,
                   lower(registration_event.raw_fact_ref ->> 'emitting_address')
            FROM parent JOIN clock ON clock.chain_id = parent.chain_id
            JOIN bigname_phase.project_parent_subregistry subregistry
              ON subregistry.chain_id = parent.chain_id
             AND subregistry.logical_name_id = parent.logical_name_id
             AND subregistry.subregistry_address NOT IN ('', {ZERO_ADDRESS})
            JOIN bigname_phase.contract_instance_addresses address ON {v2_address}
            JOIN bigname_phase.project_child_registration_state registration
              ON registration.chain_id = parent.chain_id
             AND registration.registry_contract_instance_id = address.contract_instance_id::text
             AND registration.event_kind IS NOT NULL
             AND registration.event_kind <> 'RegistrationReleased'
            JOIN bigname_phase.name_surfaces child
              ON child.logical_name_id = registration.logical_name_id
             AND child.namespace = parent.namespace AND child.chain_id = parent.chain_id
             AND child.visibility_state = 'active'
             AND cardinality(child.labelhashes) = cardinality(parent.labelhashes) + 1
             AND child.labelhashes[2:cardinality(child.labelhashes)] = parent.labelhashes
             AND {child_readable}
            LEFT JOIN bigname_phase.label_preimages preimage
              ON preimage.labelhash = lower(child.labelhashes[1])
            -- The registry a registry's labels read is the registration's emitter, as the
            -- served labels read `raw_fact_refs[0].registration.emitting_address`.
            LEFT JOIN bigname_phase.normalized_events registration_event
              ON registration_event.event_identity = registration.event_identity
            WHERE parent.raw_name <> ''
        ), selected AS (
            -- publish's arm rule: the child's selected arm, or the only arm when none is selected;
            -- the canonical event order picks within the arm: recency, the emission ordinal at
            -- one log (docs/glossary.md#emission-ordinal), then event identity.
            SELECT candidate.*,
                   row_number() OVER (
                       PARTITION BY candidate.child_logical_name_id
                       ORDER BY {candidate_position} DESC
                   ) AS pair_rank
            FROM candidates candidate
            WHERE {arm} = candidate.authority_arm
               OR ({arm} IS NULL AND NOT EXISTS (
                   SELECT 1 FROM candidates other
                   WHERE other.child_logical_name_id = candidate.child_logical_name_id
                     AND other.authority_arm <> candidate.authority_arm))
        ),",
        parent_readable = readable_surface("surface"),
        child_readable = readable_surface("child"),
        migration_address = active_instance(
            "address",
            "subregistry.chain_id",
            "subregistry.subregistry_address"
        ),
        v2_address = active_instance(
            "address",
            "subregistry.chain_id",
            "subregistry.subregistry_address"
        ),
        zero_owner = zero_owner("edge.chain_id", "edge.namespace || ':' || edge.child_node"),
        other_position = row_position("other"),
        edge_position = row_position("edge"),
        candidate_position = row_position("candidate"),
        serving = serving("edge.chain_id", "edge.child_logical_name_id"),
        fuses = effective_child_fuses(
            "edge.chain_id",
            "edge.child_logical_name_id",
            "clock.epoch_seconds"
        ),
        arm = selected_authority_arm("candidate.chain_id", "candidate.child_logical_name_id"),
        v1_raw_name = label_raw_name("parent.raw_name = ''"),
        v1_decoded_name = label_decoded_name("parent.raw_name = ''"),
        v2_raw_name = label_raw_name("FALSE"),
        v2_decoded_name = label_decoded_name("FALSE"),
    ));
}

/// The child's raw name from its label preimage: null for no preimage and for a decoded label
/// that fails normalization; the label alone under the root.
fn label_raw_name(under_root: &str) -> String {
    format!(
        "CASE WHEN preimage.raw_label IS NULL THEN NULL
              WHEN preimage.decoded_label IS NOT NULL
                   AND NOT preimage.normalized_under_version THEN NULL
              WHEN {under_root} THEN preimage.raw_label
              ELSE preimage.raw_label || decode('2e', 'hex') || convert_to(parent.raw_name, 'UTF8')
         END"
    )
}

fn label_decoded_name(under_root: &str) -> String {
    format!(
        "CASE WHEN preimage.decoded_label IS NULL THEN NULL
              WHEN NOT preimage.normalized_under_version THEN NULL
              WHEN {under_root} THEN preimage.decoded_label
              ELSE preimage.decoded_label || '.' || parent.raw_name
         END"
    )
}

/// The served child name: decoded, else the escaped raw bytes, else the labelhash placeholder
/// under the parent's spelling (crates/storage/src/children/reads.rs,
/// `CHILD_DISPLAY_NAME_EXPR`).
pub(super) const CHILD_DISPLAY_NAME: &str = "COALESCE(
    selected.decoded_name,
    encode(selected.raw_name, 'escape'),
    '[' || substring(lower(selected.labelhash) FROM 3) || '].' || parent.raw_name
)";

/// A child without a surface passes. A surfaced child requires both its surface and
/// stored block to be canonical, safe, or finalized.
pub(super) const CHILD_SURFACE_FILTER: &str = "
    AND (child_surface.logical_name_id IS NULL
         OR (child_surface.canonicality_state::text IN ('canonical', 'safe', 'finalized')
             AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage child_lineage
                         WHERE child_lineage.chain_id = child_surface.chain_id
                           AND child_lineage.block_hash = child_surface.block_hash
                           AND child_lineage.canonicality_state::text
                               IN ('canonical', 'safe', 'finalized'))))";
