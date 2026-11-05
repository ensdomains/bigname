//! The ENSv1 registry children with no name surface that an address owns, read through the
//! subnames relation (`children_page::push_children`), so each is the row
//! `GET /v1/names/{parent}/subnames` serves for it: the same served name, the same owner (the
//! holder of the node's `.eth` lease when the registrar retains one, else the child node's
//! current registry owner) and the same manager (its registry owner). A child is one an
//! `ens_v1_registry_l1` NewOwner created
//! (a `setSubnodeOwner` or `setSubnodeRecord`, which proves the child node and its labelhash but
//! not its label). Its parent is the node that NewOwner names, found by the child through the
//! retained SubregistryChanged events; the child relation then decides whether the parent lists
//! it. The row's resource is the one the node's latest owner-setting registry event carried
//! (`project_registry_node_state.owner_resource_id`): the node's registry-only resource.
use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{
    Execute, PgConnection, Postgres, QueryBuilder, Row, postgres::PgArguments, types::Uuid,
};

use crate::{ChildrenCurrentPageFilter, families::name::rendered::composed_surface_sql};

use super::{
    children::{ListingBlock, Parents, Relation, push_listed},
    children_page::push_children,
    listing::{DISPLAY_KEY_BYTES, DISPLAY_KEY_CHARS, MISSING_TIMESTAMP},
    require_publication,
};

/// One surface-less registry child as the subnames route serves it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegistryChildRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub display_name: String,
    pub namehash: String,
    pub owner: String,
    pub resource_id: Uuid,
    pub authority: Option<String>,
    /// The child has only a shadow surface a NameWrapper or ENSv1 registrar event observed
    /// (`FamilyChildRow::lifecycle_shadow`).
    pub lifecycle_shadow: bool,
    /// The NameWrapper that observed the shadow is the registry owner
    /// (`FamilyChildRow::wrapper_held`).
    pub wrapper_held: bool,
    /// The node's registrar lease has been released (`FamilyChildRow::released_lease`).
    pub released_lease: bool,
    /// The holder of the node's registrar lease (`FamilyChildRow::token_holder`).
    pub token_holder: Option<String>,
}

/// The children among `candidates` (name ids of `chain_id` with no name surface) that a parent
/// lists and whose served owner or manager is `address`.
pub(crate) async fn load_owned_registry_children(
    conn: &mut PgConnection,
    chain_id: &str,
    address: &str,
    candidates: &[String],
    published_block: i64,
) -> Result<Vec<RegistryChildRow>> {
    let parents = registry_child_parents(conn, chain_id, candidates).await?;
    if parents.is_empty() {
        return Ok(Vec::new());
    }
    require_publication(conn, &parents).await?;
    let nodes = candidate_nodes(candidates);
    let mut builder = owned_children_query(address, candidates, &nodes, &parents, published_block);
    let rows = builder
        .build()
        .fetch_all(&mut *conn)
        .await
        .context("failed to read the surface-less registry children")?;
    let mut seen = BTreeSet::new();
    let mut children = Vec::with_capacity(rows.len());
    for row in rows {
        let child = RegistryChildRow {
            logical_name_id: row.try_get("child_logical_name_id")?,
            namespace: row.try_get("namespace")?,
            display_name: row.try_get("canonical_display_name")?,
            namehash: row.try_get("namehash")?,
            owner: row.try_get("owner")?,
            resource_id: row.try_get("owner_resource_id")?,
            authority: row.try_get("registry_authority")?,
            lifecycle_shadow: row.try_get("lifecycle_shadow")?,
            wrapper_held: row.try_get("wrapper_held")?,
            released_lease: row.try_get("released_lease")?,
            token_holder: row.try_get("token_holder")?,
        };
        if seen.insert(child.logical_name_id.clone()) {
            children.push(child);
        }
    }
    Ok(children)
}

/// The parents an `ens_v1_registry_l1` NewOwner named for the child ids `candidates` of
/// `chain_id`, as `<namespace>:<node>`: the nodes the retained SubregistryChanged events of each
/// child carry.
pub async fn registry_child_parents(
    conn: &mut PgConnection,
    chain_id: &str,
    candidates: &[String],
) -> Result<Vec<String>> {
    // The predicates repeat `normalized_events_v1_subregistry_after_child_scope_idx`'s, so the
    // lookup by child reads that index.
    sqlx::query_scalar(
        "/* storage:families.topology.registry_child_parents */
         SELECT DISTINCT event.namespace || ':' || lower(event.after_state ->> 'node')
         FROM bigname_phase.normalized_events event
         WHERE event.chain_id = $1
           AND (event.namespace || ':' || lower(event.after_state ->> 'child_node')) = ANY($2)
           AND event.event_kind = 'SubregistryChanged'
           AND event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
           AND event.source_family = 'ens_v1_registry_l1'
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND event.after_state ->> 'node' IS NOT NULL
           AND btrim(event.after_state ->> 'node') <> ''
           AND event.after_state ->> 'child_node' IS NOT NULL
           AND btrim(event.after_state ->> 'child_node') <> ''",
    )
    .bind(chain_id)
    .bind(candidates)
    .fetch_all(&mut *conn)
    .await
    .context("failed to read the parents of the surface-less registry children")
}

/// The child nodes of `<namespace>:<node>` name ids.
fn candidate_nodes(candidates: &[String]) -> Vec<String> {
    candidates
        .iter()
        .filter_map(|id| id.split_once(':').map(|(_, node)| node.to_owned()))
        .collect()
}

/// The statement of [`load_owned_registry_children`]: the children relation of `parents`
/// narrowed to `nodes`, the nodes of `candidates`, keeping the children whose served owner (the
/// lease's holder, else the registry owner) or registry owner is `address`.
pub(super) fn owned_children_query<'a>(
    address: &'a str,
    candidates: &'a [String],
    nodes: &'a [String],
    parents: &'a [String],
    published_block: i64,
) -> QueryBuilder<'a, Postgres> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:families.topology.owned_registry_children */ WITH ",
    );
    push_children(
        &mut builder,
        Parents::Many(parents),
        &ChildrenCurrentPageFilter::default(),
        None,
        Some(nodes),
    );
    builder.push(
        ") SELECT children.child_logical_name_id, children.namespace,
                  children.canonical_display_name, children.namehash, children.owner,
                  children.registry_authority, children.lifecycle_shadow,
                  children.wrapper_held, children.released_lease, children.token_holder,
                  state.owner_resource_id
           FROM children
           JOIN parent ON parent.logical_name_id = children.parent_logical_name_id
           JOIN bigname_phase.project_registry_node_state state
             ON state.chain_id = parent.chain_id AND state.namespace = children.namespace
            AND state.node = lower(children.namehash)
           WHERE children.child_logical_name_id = ANY(",
    );
    builder.push_bind(candidates);
    builder.push(") AND (COALESCE(children.token_holder, children.owner) = lower(");
    builder.push_bind(address);
    builder.push(") OR children.owner = lower(");
    builder.push_bind(address);
    builder.push(")) AND state.owner_resource_id IS NOT NULL AND NOT ");
    push_published_surface_exists(
        &mut builder,
        "children.child_logical_name_id",
        published_block,
    );
    builder
}

/// The published-surface test of a name id, as SQL with `$block` naming the publication block
/// parameter: a surface the ordinary compositor would read (`composed_surface_sql`, canonical in
/// a canonical block, `name::loaders::surfaces`) written at or before the publication
/// (`name::batch::load_base`). A registry child is surface-less for a publication exactly when
/// this is false, so the two paths hand a child over at the publication that composes it.
pub(crate) fn published_surface_exists(id: &str, block: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM bigname_phase.name_surfaces surface
                 JOIN bigname_phase.chain_lineage lineage
                   ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
                 WHERE surface.logical_name_id = {id}
                   AND {composed}
                   AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND surface.block_number <= {block})",
        composed = composed_surface_sql("surface")
    )
}

/// [`published_surface_exists`] on a query builder, binding the publication block.
fn push_published_surface_exists(
    builder: &mut QueryBuilder<'_, Postgres>,
    id: &str,
    published_block: i64,
) {
    let (head, tail) = published_surface_exists(id, "\u{0}")
        .split_once("\u{0}")
        .map(|(head, tail)| (head.to_owned(), tail.to_owned()))
        .expect("the surface test names its block once");
    builder.push(head);
    builder.push_bind(published_block);
    builder.push(tail);
}

/// The address listing rows (`project_address_listing`, F18, in the order of
/// `records::ADDRESS_LISTING_COLUMNS`) of the surface-less registry children that `parents` list
/// at `block`, narrowed to the child nodes `nodes` and the child ids `candidates` when given:
/// the rows [`load_owned_registry_children`] serves for each address, at the block Project
/// publishes and with the chain-data spelling ([`Relation::Stored`]). Each child is listed for
/// its served owner (the lease's holder, else its registry owner) as `token_holder` and its
/// registry owner as `effective_controller`, only where the address index lists the child for
/// the address as the route's candidate query reads it, and never while the NameWrapper holds it
/// or its lease is released (`records::registry_children`). Returned as owned SQL and arguments
/// for a caller whose future must be `Send` (`topology::listing_statement`).
pub fn registry_child_listing_statement(
    head: &str,
    block: ListingBlock<'_>,
    parents: &[String],
    nodes: Option<&[String]>,
    candidates: Option<&[String]>,
) -> Result<(String, PgArguments), sqlx::Error> {
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "/* storage:families.topology.registry_child_listing */ {head} WITH "
    ));
    push_listed(
        &mut builder,
        Parents::Many(parents),
        None,
        nodes,
        None,
        Relation::Stored(block),
    );
    builder.push(
        "), child AS (
            SELECT children.chain_id, children.parent_logical_name_id,
                   children.child_logical_name_id, children.namespace,
                   children.canonical_display_name, children.namehash, children.owner,
                   children.registry_authority, children.token_holder,
                   children.unhealed_labelhashes, state.owner_resource_id
            FROM children
            JOIN bigname_phase.project_registry_node_state state
              ON state.chain_id = children.chain_id AND state.namespace = children.namespace
             AND state.node = lower(children.namehash)
            WHERE state.owner_resource_id IS NOT NULL
              AND NOT children.wrapper_held AND NOT children.released_lease",
    );
    if let Some(candidates) = candidates {
        builder.push(" AND children.child_logical_name_id = ANY(");
        builder.push_bind(candidates.to_vec());
        builder.push(")");
    }
    builder.push(" AND NOT ");
    push_published_surface_exists(&mut builder, "children.child_logical_name_id", block.number);
    // The route reads a child only under a parent an `ens_v1_registry_l1` NewOwner named for it
    // (`registry_child_parents`), never a Basenames registry's child.
    builder.push(
        " AND EXISTS (
              SELECT 1 FROM bigname_phase.normalized_events event
              WHERE event.chain_id = children.chain_id
                AND (event.namespace || ':' || lower(event.after_state ->> 'child_node'))
                    = children.child_logical_name_id
                AND event.event_kind = 'SubregistryChanged'
                AND event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
                AND event.source_family = 'ens_v1_registry_l1'
                AND event.consumer_visibility = 'activated'
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND event.namespace || ':' || lower(event.after_state ->> 'node')
                    = children.parent_logical_name_id)",
    );
    builder.push(format!(
        "
        ), related AS (
            SELECT child.*, related.address,
                   bool_or(related.relation = 0) OVER pair AS token_holder_relation,
                   bool_or(related.relation = 1) OVER pair AS controller_relation
            FROM child
            CROSS JOIN LATERAL (VALUES (COALESCE(child.token_holder, child.owner), 0),
                                       (child.owner, 1)) related (address, relation)
            WHERE related.address IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM bigname_phase.project_address_name_index indexed
                  WHERE indexed.address = related.address
                    AND indexed.logical_name_id = child.child_logical_name_id
                    AND indexed.relation = 'effective_controller'
                    AND indexed.chain_id = child.chain_id)
            WINDOW pair AS (PARTITION BY related.address, child.child_logical_name_id)
        )
        SELECT DISTINCT ON (related.address, related.child_logical_name_id)
               related.chain_id, related.address, related.child_logical_name_id,
               related.namespace,
               CASE WHEN octet_length(related.canonical_display_name) <= {DISPLAY_KEY_BYTES}
                    THEN related.canonical_display_name
                    ELSE left(related.canonical_display_name, {DISPLAY_KEY_CHARS}) END,
               related.canonical_display_name, related.canonical_display_name,
               CASE WHEN strpos(related.canonical_display_name, '.') > 1
                    THEN substr(related.canonical_display_name,
                                strpos(related.canonical_display_name, '.') + 1) END,
               related.namehash, related.owner_resource_id,
               related.token_holder_relation, related.controller_relation, FALSE, TRUE, FALSE,
               TRUE, related.parent_logical_name_id, FALSE, related.registry_authority, FALSE,
               {MISSING_TIMESTAMP}, {MISSING_TIMESTAMP}, {MISSING_TIMESTAMP}, {MISSING_TIMESTAMP},
               NULL::bigint, NULL::bigint, NULL::bigint, related.unhealed_labelhashes
        FROM related
        ORDER BY related.address, related.child_logical_name_id,
                 related.parent_logical_name_id"
    ));
    let arguments = builder
        .build()
        .take_arguments()
        .map_err(sqlx::Error::Encode)?
        .unwrap_or_default();
    Ok((builder.into_sql(), arguments))
}
