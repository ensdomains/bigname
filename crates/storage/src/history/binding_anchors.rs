use anyhow::Result;
use sqlx::PgPool;
use std::collections::BTreeSet;
use uuid::Uuid;

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
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

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
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}
