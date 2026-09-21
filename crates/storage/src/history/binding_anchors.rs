use anyhow::Result;
use sqlx::PgPool;
use std::collections::BTreeSet;
use uuid::Uuid;

use super::wrapped_registrar::{
    load_node_logical_name_ids, load_wrapped_registrar_resource_ids, load_wrapping_logical_name_ids,
};

/// The resources bound to one exact name, plus the BaseRegistrar leases its `NameWrapped`
/// rows link to. Both sets honour the same per-chain published block.
pub(super) async fn load_resource_ids_for_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
    canonical_only: bool,
    published: Option<&std::collections::BTreeMap<String, i64>>,
) -> Result<Vec<Uuid>> {
    let bindings = if canonical_only {
        crate::load_surface_bindings_by_logical_name_id(pool, logical_name_id).await
    } else {
        crate::load_surface_bindings_by_logical_name_id_including_noncanonical(
            pool,
            logical_name_id,
        )
        .await
    }?;

    Ok(bindings
        .into_iter()
        .filter(|binding| {
            published.is_none_or(|bounds| {
                bounds
                    .get(&binding.chain_id)
                    .is_some_and(|block| binding.block_number <= *block)
            })
        })
        .map(|binding| binding.resource_id)
        .chain(
            load_wrapped_registrar_resource_ids(pool, logical_name_id, canonical_only, published)
                .await?,
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// The exact names one resource is bound to, plus the names whose `NameWrapped` rows link to
/// it as their wrapped BaseRegistrar lease.
pub(super) async fn load_logical_name_ids_for_resource_id(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
    published: Option<&std::collections::BTreeMap<String, i64>>,
) -> Result<Vec<String>> {
    let bindings = if canonical_only {
        crate::load_surface_bindings_by_resource_id(pool, resource_id).await
    } else {
        crate::load_surface_bindings_by_resource_id_including_noncanonical(pool, resource_id).await
    }?;

    Ok(bindings
        .into_iter()
        .filter(|binding| {
            published.is_none_or(|bounds| {
                bounds
                    .get(&binding.chain_id)
                    .is_some_and(|block| binding.block_number <= *block)
            })
        })
        .map(|binding| binding.logical_name_id)
        .chain(load_wrapping_logical_name_ids(pool, resource_id, canonical_only, published).await?)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// The exact names a resource may currently serve as their registration: the names it is bound
/// to, and the exact names of the node its own rows carry. A BaseRegistrar lease granted after
/// its name was wrapped has no binding and no `NameWrapped` link, but its grant names the node.
/// Candidates only: the caller proves membership from each name's current projection row.
pub(super) async fn load_candidate_logical_name_ids_for_resource_id(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<String>> {
    let mut logical_name_ids = crate::load_surface_bindings_by_resource_id(pool, resource_id)
        .await?
        .into_iter()
        .map(|binding| binding.logical_name_id)
        .collect::<BTreeSet<_>>();
    logical_name_ids.extend(load_node_logical_name_ids(pool, resource_id, true).await?);
    Ok(logical_name_ids.into_iter().collect())
}
