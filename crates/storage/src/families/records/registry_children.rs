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
    topology::{RegistryChildRow, load_owned_registry_children, published_surface_exists},
};

/// The composed rows of the surface-less registry children `address` owns, in `namespace` when
/// given, in the relation-row shape of `address_names.rs` with `registry_child` set and the
/// served owner.
///
/// "Surface-less" is relative to the chain's Project publication: a surface Interpret wrote
/// after it is not part of it, exactly as the ordinary compositor leaves such a surface out
/// (`name::batch::load_base`), so the child stays listed here until the publication that
/// composes its name row.
pub(super) async fn compose_registry_child_rows(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
) -> Result<Vec<Value>> {
    let chains: Vec<String> = sqlx::query_scalar(
        "/* storage:families.records.address_registry_child_chains */
         SELECT DISTINCT indexed.chain_id
         FROM bigname_phase.project_address_name_index indexed
         WHERE indexed.address = lower($1) AND indexed.relation = 'effective_controller'
           AND ($2::text IS NULL OR split_part(indexed.logical_name_id, ':', 1) = $2)",
    )
    .bind(address)
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to load the registry child chains of {address}"))?;
    let mut by_chain: BTreeMap<String, (i64, Vec<String>)> = BTreeMap::new();
    for chain_id in chains {
        let published = servable_publication(conn, &chain_id).await?;
        let candidates: Vec<String> = sqlx::query_scalar(&format!(
            "/* storage:families.records.address_registry_child_index */
             SELECT DISTINCT indexed.logical_name_id
             FROM bigname_phase.project_address_name_index indexed
             WHERE indexed.address = lower($1) AND indexed.relation = 'effective_controller'
               AND indexed.chain_id = $3
               AND ($2::text IS NULL OR split_part(indexed.logical_name_id, ':', 1) = $2)
               AND NOT {}",
            published_surface_exists("indexed.logical_name_id", "$4")
        ))
        .bind(address)
        .bind(namespace)
        .bind(&chain_id)
        .bind(published.block_number)
        .fetch_all(&mut *conn)
        .await
        .with_context(|| format!("failed to load the registry child candidates of {address}"))?;
        if !candidates.is_empty() {
            by_chain.insert(chain_id, (published.block_number, candidates));
        }
    }
    let mut rows = Vec::new();
    for (chain_id, (published_block, ids)) in by_chain {
        let children =
            load_owned_registry_children(conn, &chain_id, address, &ids, published_block).await?;
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
                "served_authority": child.authority,
            })
        }));
    }
    Ok(rows)
}
