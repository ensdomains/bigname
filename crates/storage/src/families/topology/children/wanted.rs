//! The candidates of the children a narrowed read wants (`push_selected`'s `children`), each
//! read by its own key: the child's surface by node (`name_surfaces_project_node_idx`), its
//! ENSv2 registrations by the registration key's prefix, and its ENSv1 and Basenames edges by
//! node (`project_child_edge_candidate_child_idx`). The candidate arms join these in place of
//! the base tables under the same conditions, so they keep the same rows, and hold only the
//! wanted children's.
//!
//! Each probe is its own subquery (`OFFSET 0`), so its cost follows the children wanted, never
//! the planner's estimate of the parents: the Project writer derives a block's touched children
//! under every parent that can list them, and its parents are this block's, which the planner
//! can count as one row and otherwise scan every edge and surface of the chain for each.
use sqlx::{Postgres, QueryBuilder};

/// Push the `wanted_node`, `wanted_child`, `wanted_registration` and `wanted_edge` CTEs for the
/// child nodes `children` (lowercase, no namespace, each once however often it is given),
/// closing the CTE before them and leaving the last open.
pub(super) fn push<'a>(builder: &mut QueryBuilder<'a, Postgres>, children: &'a [String]) {
    builder.push(
        "
        ), wanted_node AS MATERIALIZED (
            SELECT DISTINCT wanted.node FROM unnest(",
    );
    builder.push_bind(children);
    builder.push(
        "::text[]) wanted (node)
        ), wanted_child AS MATERIALIZED (
            SELECT child.*
            FROM (SELECT DISTINCT parent.namespace FROM parent) scope
            CROSS JOIN wanted_node wanted
            CROSS JOIN LATERAL (
                SELECT child.* FROM bigname_phase.name_surfaces child
                WHERE child.namespace = scope.namespace AND lower(child.namehash) = wanted.node
                OFFSET 0
            ) child
        ), wanted_registration AS MATERIALIZED (
            SELECT registration.*
            FROM wanted_child child
            CROSS JOIN LATERAL (
                SELECT registration.*
                FROM bigname_phase.project_child_registration_state registration
                WHERE registration.chain_id = child.chain_id
                  AND registration.logical_name_id = child.logical_name_id
                OFFSET 0
            ) registration
        ), wanted_edge AS MATERIALIZED (
            SELECT edge.*
            FROM (SELECT DISTINCT parent.chain_id, parent.namespace FROM parent) scope
            CROSS JOIN wanted_node wanted
            CROSS JOIN LATERAL (
                SELECT edge.* FROM bigname_phase.project_child_edge_candidate edge
                WHERE edge.chain_id = scope.chain_id AND edge.namespace = scope.namespace
                  AND edge.child_node = wanted.node
                OFFSET 0
            ) edge",
    );
}
