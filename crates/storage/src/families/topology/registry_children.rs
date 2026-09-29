//! The ENSv1 registry children with no name surface that an address owns, read through the
//! subnames relation (`children_page::push_children`), so each is the row
//! `GET /v1/names/{parent}/subnames` serves for it: the same served name and the same owner, the
//! child node's current registry owner. A child is one an `ens_v1_registry_l1` NewOwner created
//! (a `setSubnodeOwner` or `setSubnodeRecord`, which proves the child node and its labelhash but
//! not its label). Its parent is the node that NewOwner names, found by the child through the
//! retained SubregistryChanged events; the child relation then decides whether the parent lists
//! it. The row's resource is the one the node's latest owner-setting registry event carried
//! (`project_registry_node_state.owner_resource_id`): the node's registry-only resource.
use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, types::Uuid};

use crate::ChildrenCurrentPageFilter;

use super::{children::Parents, children_page::push_children, require_publication};

/// One surface-less registry child as the subnames route serves it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegistryChildRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub display_name: String,
    pub namehash: String,
    pub owner: String,
    pub resource_id: Uuid,
}

/// The children among `candidates` (name ids of `chain_id` with no name surface) that a parent
/// lists and whose served owner is `address`.
pub(crate) async fn load_owned_registry_children(
    conn: &mut PgConnection,
    chain_id: &str,
    address: &str,
    candidates: &[String],
) -> Result<Vec<RegistryChildRow>> {
    // The predicates repeat `normalized_events_v1_subregistry_after_child_scope_idx`'s, so the
    // lookup by child reads that index.
    let parents: Vec<String> = sqlx::query_scalar(
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
    .context("failed to read the parents of the surface-less registry children")?;
    if parents.is_empty() {
        return Ok(Vec::new());
    }
    require_publication(conn, &parents).await?;
    let filter = ChildrenCurrentPageFilter::default();
    let mut builder = QueryBuilder::<Postgres>::new("WITH ");
    push_children(&mut builder, Parents::Many(&parents), &filter, None);
    builder.push(
        ") SELECT children.child_logical_name_id, children.namespace,
                  children.canonical_display_name, children.namehash, children.owner,
                  state.owner_resource_id
           FROM children
           JOIN parent ON parent.logical_name_id = children.parent_logical_name_id
           JOIN bigname_phase.project_registry_node_state state
             ON state.chain_id = parent.chain_id AND state.namespace = children.namespace
            AND state.node = lower(children.namehash)
           WHERE children.child_logical_name_id = ANY(",
    );
    builder.push_bind(candidates);
    builder.push(") AND children.owner = lower(");
    builder.push_bind(address);
    builder.push(
        ") AND state.owner_resource_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM bigname_phase.name_surfaces surface
                           WHERE surface.logical_name_id = children.child_logical_name_id)",
    );
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
        };
        if seen.insert(child.logical_name_id.clone()) {
            children.push(child);
        }
    }
    Ok(children)
}
