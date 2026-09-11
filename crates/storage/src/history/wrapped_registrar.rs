use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

/// Load registrar resources that canonical wrapper-binding events associate with one exact name.
pub async fn load_wrapped_registrar_resource_ids_by_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<Uuid>> {
    load_wrapped_registrar_resource_ids(pool, logical_name_id, true).await
}

pub(super) async fn load_wrapped_registrar_resource_ids(
    pool: &PgPool,
    logical_name_id: &str,
    canonical_only: bool,
) -> Result<Vec<Uuid>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_wrapped_registrar_resources_query(&mut builder, logical_name_id, canonical_only);
    builder
        .build_query_scalar()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load wrapped registrar resources for logical_name_id {logical_name_id}"
            )
        })
}

pub(super) fn push_wrapped_registrar_resources_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    logical_name_id: &'a str,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            (ne.after_state ->> 'wrapped_registrar_resource_id')::uuid
        FROM bigname_phase.normalized_events ne
        LEFT JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = ne.chain_id
         AND lineage.block_hash = ne.block_hash
        WHERE ne.logical_name_id = "#,
    );
    builder.push_bind(logical_name_id);
    builder.push(
        r#"
          AND ne.event_kind = 'SurfaceBound'
          AND ne.source_family = 'ens_v1_wrapper_l1'
          AND ne.consumer_visibility = 'activated'
          AND ne.after_state ->> 'wrapped_registrar_resource_id' IS NOT NULL
        "#,
    );
    if canonical_only {
        push_canonical_row_filter(builder, "ne", "lineage");
    }
    builder.push(" ORDER BY 1");
}

pub(super) async fn load_resource_ids_for_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
    canonical_only: bool,
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
    let mut resource_ids = bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>();
    resource_ids
        .extend(load_wrapped_registrar_resource_ids(pool, logical_name_id, canonical_only).await?);
    Ok(resource_ids.into_iter().collect())
}

pub(super) async fn load_logical_name_ids_for_resource_id(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
) -> Result<Vec<String>> {
    let bindings = if canonical_only {
        crate::load_surface_bindings_by_resource_id(pool, resource_id).await
    } else {
        crate::load_surface_bindings_by_resource_id_including_noncanonical(pool, resource_id).await
    }?;
    let mut logical_name_ids = bindings
        .into_iter()
        .map(|binding| binding.logical_name_id)
        .collect::<BTreeSet<_>>();
    let mut candidates = BTreeSet::new();
    for anchor in load_registrar_namehash_anchors(pool, resource_id, canonical_only).await? {
        candidates.extend(load_namehash_surfaces(pool, &anchor, canonical_only).await?);
    }
    for logical_name_id in candidates {
        if logical_name_ids.contains(&logical_name_id) {
            continue;
        }
        let wrapped_registrars =
            load_wrapped_registrar_resource_ids(pool, &logical_name_id, canonical_only).await?;
        if wrapped_registrars.contains(&resource_id) {
            logical_name_ids.insert(logical_name_id);
        }
    }
    Ok(logical_name_ids.into_iter().collect())
}

#[derive(sqlx::FromRow)]
pub(super) struct ResourceNamehashAnchor {
    pub(super) chain_id: String,
    pub(super) namespace: String,
    pub(super) namehash: String,
}

async fn load_registrar_namehash_anchors(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
) -> Result<Vec<ResourceNamehashAnchor>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_registrar_namehash_anchors_query(&mut builder, resource_id, canonical_only);
    builder
        .build_query_as()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load namehash anchors for registrar resource_id {resource_id}")
        })
}

pub(super) fn push_registrar_namehash_anchors_query(
    builder: &mut QueryBuilder<'_, Postgres>,
    resource_id: Uuid,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT DISTINCT
            anchor.chain_id,
            anchor.namespace,
            anchor.after_state ->> 'namehash' AS namehash
        FROM bigname_phase.normalized_events anchor
        LEFT JOIN bigname_phase.chain_lineage anchor_lineage
          ON anchor_lineage.chain_id = anchor.chain_id
         AND anchor_lineage.block_hash = anchor.block_hash
        WHERE anchor.resource_id = "#,
    );
    builder.push_bind(resource_id);
    builder.push(
        r#"
          AND anchor.after_state ->> 'namehash' IS NOT NULL
          AND anchor.after_state ->> 'namehash' <> ''
        "#,
    );
    if canonical_only {
        push_canonical_row_filter(builder, "anchor", "anchor_lineage");
    }
    builder.push(" ORDER BY anchor.chain_id, anchor.namespace, namehash");
}

async fn load_namehash_surfaces(
    pool: &PgPool,
    anchor: &ResourceNamehashAnchor,
    canonical_only: bool,
) -> Result<Vec<String>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_namehash_surfaces_query(&mut builder, anchor, canonical_only);
    builder
        .build_query_scalar()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load exact surfaces for registrar namehash {}",
                anchor.namehash
            )
        })
}

pub(super) fn push_namehash_surfaces_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    anchor: &'a ResourceNamehashAnchor,
    canonical_only: bool,
) {
    builder.push(
        r#"
        SELECT surface.logical_name_id
        FROM (
            SELECT scoped.*
            FROM bigname_phase.name_surfaces scoped
            WHERE scoped.namespace = "#,
    );
    builder.push_bind(&anchor.namespace);
    builder.push(" AND scoped.namehash = ");
    builder.push_bind(&anchor.namehash);
    builder.push(
        r#"
            OFFSET 0
        ) surface
        LEFT JOIN bigname_phase.chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
        WHERE surface.chain_id = "#,
    );
    builder.push_bind(&anchor.chain_id);
    if canonical_only {
        push_canonical_row_filter(builder, "surface", "surface_lineage");
    }
    builder.push(" ORDER BY surface.logical_name_id");
}

fn push_canonical_row_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
    lineage_alias: &str,
) {
    builder.push(format!(
        r#"
        AND {row_alias}.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
        AND (
            {row_alias}.block_hash IS NULL
            OR {lineage_alias}.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
            )
        )
        "#,
    ));
}
