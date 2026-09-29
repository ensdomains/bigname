//! The address-names rows of ENSv1 registry children with no name surface: a node an
//! `ens_v1_registry_l1` NewOwner created (`setSubnodeOwner` or `setSubnodeRecord`) and no
//! registrar, NameWrapper or other label-bearing event ever named. No name row composes for such a
//! node, so the ordinary read (`address_names.rs`) lists nothing for it; this read lists it for
//! its current registry owner as `effective_controller`, as its parent's subnames route serves it
//! (`families::topology::load_owned_registry_children`): the same served name, a non-name form
//! when the label is unproven (docs/glossary.md#non-name-form), and the same owner.
//!
//! The candidates are the address index's `effective_controller` ids with no name surface, which
//! Project derives for such a node from its registry owner facts (crates/project/src/families/
//! derived.rs, `registry_children`). A child that gains a surface is read by the ordinary path
//! under the same id and never here. The row has no surface binding, binding kind or token
//! lineage; its resource is the node's registry-only resource.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;

use super::address_names::publication_stamps;
use crate::families::{
    name::servable_publication,
    topology::{RegistryChildRow, load_owned_registry_children},
};

/// The composed rows of the surface-less registry children `address` owns, in `namespace` when
/// given, in the relation-row shape of `address_names.rs` with `registry_child` set and the
/// served owner.
pub(super) async fn compose_registry_child_rows(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
) -> Result<Vec<Value>> {
    let candidates: Vec<(String, String)> = sqlx::query_as(
        "/* storage:families.records.address_registry_child_index */
         SELECT DISTINCT indexed.chain_id, indexed.logical_name_id
         FROM bigname_phase.project_address_name_index indexed
         WHERE indexed.address = lower($1) AND indexed.relation = 'effective_controller'
           AND ($2::text IS NULL OR split_part(indexed.logical_name_id, ':', 1) = $2)
           AND NOT EXISTS (SELECT 1 FROM bigname_phase.name_surfaces surface
                           WHERE surface.logical_name_id = indexed.logical_name_id)",
    )
    .bind(address)
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to load the registry child candidates of {address}"))?;
    let mut by_chain: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (chain_id, name) in candidates {
        by_chain.entry(chain_id).or_default().push(name);
    }
    let mut rows = Vec::new();
    for (chain_id, ids) in by_chain {
        let children = load_owned_registry_children(conn, &chain_id, address, &ids).await?;
        if children.is_empty() {
            continue;
        }
        let publication = servable_publication(conn, &chain_id).await?;
        let (provenance, chain_positions, canonicality_summary) = publication_stamps(&publication);
        let last_recomputed_at = crate::time::format_timestamp(publication.block_timestamp);
        rows.extend(children.into_iter().map(|child: RegistryChildRow| {
            json!({
                "address": child.owner,
                "logical_name_id": child.logical_name_id,
                "relation": "effective_controller",
                "namespace": child.namespace,
                "raw_name": child.display_name,
                "normalized_name": child.display_name,
                "namehash": child.namehash,
                "surface_binding_id": null,
                "resource_id": child.resource_id,
                "token_lineage_id": null,
                "binding_kind": null,
                "support_status": "supported",
                "unsupported_reason": null,
                "provenance": provenance,
                "chain_positions": chain_positions,
                "canonicality_summary": canonicality_summary,
                // Composed name rows carry manifest version 1 (families::name::compose).
                "manifest_version": 1,
                "last_recomputed_at": last_recomputed_at,
                "registry_child": true,
                "served_owner": child.owner,
            })
        }));
    }
    Ok(rows)
}
