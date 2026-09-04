use anyhow::Result;
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter,
    paging::{
        push_history_filters, push_history_order, push_history_select,
        push_product_history_duplicate_filter,
    },
    wrapped_registrar::{
        ResourceNamehashAnchor, push_namehash_surfaces_query,
        push_registrar_namehash_anchors_query, push_wrapped_registrar_resources_query,
    },
};

pub(super) async fn explain_history_filter_for_test(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    lookup: HistoryPlanLookup<'_>,
    canonical_only: bool,
) -> Result<String> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;

    let mut forward = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_wrapped_registrar_resources_query(&mut forward, lookup.logical_name_id, canonical_only);
    let forward_plan = forward
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let mut anchors = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_registrar_namehash_anchors_query(&mut anchors, lookup.registration_id, canonical_only);
    let anchor_plan = anchors
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let anchor = ResourceNamehashAnchor {
        chain_id: lookup.chain_id.to_owned(),
        namespace: lookup.namespace.to_owned(),
        namehash: lookup.namehash.to_owned(),
    };
    let mut surfaces = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_namehash_surfaces_query(&mut surfaces, &anchor, canonical_only);
    let surface_plan = surfaces
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let mut builder = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_history_select(&mut builder, &filter, canonical_only, false, false);
    push_history_filters(&mut builder, &filter, canonical_only);
    push_product_history_duplicate_filter(&mut builder);
    push_history_order(&mut builder);
    let plan = builder
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");
    transaction.rollback().await?;
    Ok(format!(
        "name-to-registrar association:\n{forward_plan}\n\nregistrar namehash anchors:\n{anchor_plan}\n\nexact-namehash surfaces:\n{surface_plan}\n\nhistory page:\n{plan}"
    ))
}

pub(super) struct HistoryPlanLookup<'a> {
    pub(super) logical_name_id: &'a str,
    pub(super) registration_id: uuid::Uuid,
    pub(super) chain_id: &'a str,
    pub(super) namespace: &'a str,
    pub(super) namehash: &'a str,
}
