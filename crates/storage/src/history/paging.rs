use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};

use super::redo::{InterpretRedoFence, ensure_interpret_redo_fence};
use super::{
    EventHistoryReadFilter, HistoryBlockWindow, HistoryCursor, HistoryEvent, HistoryOrder,
    HistoryPage, HistorySummaryMode, InvalidHistoryCursor,
    decoders::decode_history_event,
    duplicates::push_product_history_duplicate_filter,
    registration_identity::{
        push_product_event_kind_predicate, push_product_registration_id,
        push_registration_binding_at_event,
    },
    selector_filter::{push_selector_filter, push_string_filter},
    selectors::HistorySelector,
    source::{push_history_canonicality_filter, push_history_source_for_filter},
    summary::load_history_summary,
};
use crate::projection_helpers::{
    checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
};

pub(super) async fn load_history(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        false,
    )
    .await
}

pub(super) async fn load_history_head(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    let mut rows = load_history_internal(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        true,
    )
    .await?;
    Ok(rows.drain(..).next())
}

pub(super) async fn load_event_history_rows(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(pool, filter, canonical_only, false).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_history_page(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    include_candidates: bool,
    interpret_redo_fence: Option<&InterpretRedoFence>,
) -> Result<HistoryPage> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin normalized-event history page transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure normalized-event history page transaction")?;

    if let Some(interpret_redo_fence) = interpret_redo_fence {
        ensure_interpret_redo_fence(&mut transaction, interpret_redo_fence)
            .await
            .context("normalized-event history page refused during Interpret redo")?;
    }

    if let Some(cursor) = cursor {
        ensure_history_cursor_exists(
            &mut transaction,
            &filter,
            canonical_only,
            cursor,
            include_candidates,
        )
        .await?;
    }

    let summary =
        load_history_summary(&mut transaction, &filter, canonical_only, summary_mode).await?;

    if filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
    {
        transaction
            .commit()
            .await
            .context("failed to commit normalized-event history page transaction")?;
        return Ok(HistoryPage {
            rows: Vec::new(),
            next_cursor: None,
            summary,
            interpret_redo_fence: interpret_redo_fence.cloned(),
        });
    }

    let page_size = checked_page_size_usize(
        page_size,
        "history page_size must be positive",
        "history page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "history page_size is too large",
        "history page_size exceeds SQL limit",
    )?;

    let mut builder = QueryBuilder::<Postgres>::new("");
    if let Some(cursor) = cursor {
        push_history_cursor_cte(&mut builder, cursor);
    }
    push_history_select(
        &mut builder,
        &filter,
        canonical_only,
        cursor.is_some(),
        include_candidates,
    );
    push_history_filters(&mut builder, &filter, canonical_only);
    if !include_candidates {
        push_product_history_duplicate_filter(&mut builder, &filter, canonical_only);
    }

    if cursor.is_some() {
        builder.push(" AND ");
        push_history_cursor_after(&mut builder, filter.order);
    }

    push_history_order(&mut builder, filter.order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder
        .build()
        .fetch_all(&mut *transaction)
        .await
        .context("failed to fetch normalized-event history page")?;
    let rows = rows
        .into_iter()
        .map(decode_history_event)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, history_cursor_from_row);

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event history page transaction")?;

    Ok(HistoryPage {
        rows,
        next_cursor,
        summary,
        interpret_redo_fence: interpret_redo_fence.cloned(),
    })
}

async fn load_history_internal(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    head_only: bool,
) -> Result<Vec<HistoryEvent>> {
    if filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
    {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_select(&mut builder, &filter, canonical_only, false, false);
    push_history_filters(&mut builder, &filter, canonical_only);
    push_product_history_duplicate_filter(&mut builder, &filter, canonical_only);
    push_history_order(&mut builder, filter.order);

    if head_only {
        builder.push(" LIMIT 1");
    }

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized-event history rows")?;

    rows.into_iter().map(decode_history_event).collect()
}

pub(super) fn push_history_select<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    builder.push(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
        "#,
    );
    push_product_registration_id(builder, canonical_only);
    builder.push(
        r#" AS registration_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
        "#,
    );
    if include_candidates {
        builder.push(
            r#"
            ne.migration_correlation_ids,
            ne.consumer_visibility,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'migration_correlation_ids',
                            ARRAY[association.migration_correlation_id],
                            'correlation_kind', association.correlation_kind,
                            'consumer_visibility', association.consumer_visibility
                        )
                        ORDER BY association.migration_correlation_id,
                                 association.correlation_kind,
                                 association.consumer_visibility
                    )
                    FROM migration_event_associations AS association
                    WHERE association.event_identity = ne.event_identity
                ),
                '[]'::jsonb
            ) AS migration_associations,
            "#,
        );
    } else {
        builder.push(
            r#"
            ARRAY[]::text[] AS migration_correlation_ids,
            'activated'::text AS consumer_visibility,
            '[]'::jsonb AS migration_associations,
            "#,
        );
    }
    builder.push(
        r#"
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        "#,
    );
    push_history_source_for_filter(
        builder,
        filter,
        canonical_only,
        include_cursor_row,
        include_candidates,
    );
}

pub(super) fn push_history_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    for selector in &filter.selectors {
        builder.push(" AND ");
        push_selector_filter(builder, selector);
    }

    if let Some(namespace) = filter.namespace.as_ref() {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    if let Some(contract_address) = filter.contract_address.as_ref() {
        builder.push(" AND lower(ne.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(contract_address);
    }

    if let Some(registration_id) = filter.registration_id.as_ref() {
        builder.push(" AND ");
        builder.push_bind(filter.registration_id_is_public);
        builder.push(" AND ((ne.resource_id IS NULL AND ");
        push_product_event_kind_predicate(builder);
        builder.push(" AND ");
        push_registration_binding_at_event(builder, *registration_id, canonical_only);
        builder.push(") OR (");
        super::registration_identity::push_public_registration_at_event(
            builder,
            *registration_id,
            canonical_only,
        );
        builder.push(" AND ");
        push_product_registration_id(builder, canonical_only);
        builder.push(" = ");
        builder.push_bind(registration_id);
        builder.push("))");
    }

    if !filter.event_kinds.is_empty() {
        builder.push(" AND ");
        push_string_filter(builder, "ne.event_kind", &filter.event_kinds);
    }

    if let Some(from_block) = filter.from_block {
        builder.push(" AND ne.block_number >= ");
        builder.push_bind(from_block);
    }

    if let Some(to_block) = filter.to_block {
        builder.push(" AND ne.block_number <= ");
        builder.push_bind(to_block);
    }

    if let Some(window) = filter.block_window.as_ref() {
        push_history_block_window(builder, window);
    }

    if let Some(resolver) = filter.resolver.as_ref() {
        builder.push(" AND ne.chain_id = ");
        builder.push_bind(&resolver.chain_id);
        builder.push(" AND (lower(ne.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(&resolver.address);
        builder.push(
            " OR (ne.event_kind = 'ResolverChanged' AND (lower(ne.after_state ->> 'resolver') = ",
        );
        builder.push_bind(&resolver.address);
        builder.push(" OR lower(ne.before_state ->> 'resolver') = ");
        builder.push_bind(&resolver.address);
        builder.push(")))");
    }

    push_history_canonicality_filter(builder, canonical_only);
}

pub(super) async fn load_history_events_by_ids(
    pool: &PgPool,
    ids: &[i64],
) -> Result<Vec<HistoryEvent>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut builder = QueryBuilder::<Postgres>::new("");
    let filter = EventHistoryReadFilter::default();
    push_history_select(&mut builder, &filter, true, false, false);
    builder.push(" AND ne.normalized_event_id = ANY(");
    builder.push_bind(ids);
    builder.push("::bigint[])");
    push_history_canonicality_filter(&mut builder, true);
    push_history_order(&mut builder, HistoryOrder::Desc);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized events by id")?;
    rows.into_iter().map(decode_history_event).collect()
}

/// One inclusive block range per chain; a window without ranges matches nothing.
fn push_history_block_window<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    window: &'a HistoryBlockWindow,
) {
    if window.ranges.is_empty() {
        builder.push(" AND FALSE");
        return;
    }
    builder.push(" AND (");
    for (index, range) in window.ranges.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        builder.push("(ne.chain_id = ");
        builder.push_bind(&range.chain_id);
        if let Some(from_block) = range.from_block {
            builder.push(" AND ne.block_number >= ");
            builder.push_bind(from_block);
        }
        if let Some(to_block) = range.to_block {
            builder.push(" AND ne.block_number <= ");
            builder.push_bind(to_block);
        }
        if range.from_block.is_none() && range.to_block.is_none() {
            builder.push(" AND ne.block_number IS NOT NULL");
        }
        builder.push(")");
    }
    builder.push(")");
}

pub(super) fn push_history_order(builder: &mut QueryBuilder<'_, Postgres>, order: HistoryOrder) {
    builder.push(" ORDER BY ");
    push_history_order_terms(builder, order);
}

/// `Asc` is the exact reverse of the canonical `Desc` sort, including null
/// placement, so a backward scan of the same index serves it and the keyset
/// predicate can be derived by swapping the compared rows.
pub(super) fn push_history_order_terms(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: HistoryOrder,
) {
    match order {
        HistoryOrder::Desc => builder.push(
            r#"
            ne.block_number DESC NULLS LAST,
            ne.chain_id ASC NULLS LAST,
            ne.block_hash DESC NULLS LAST,
            ne.transaction_hash DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.event_identity DESC
        "#,
        ),
        HistoryOrder::Asc => builder.push(
            r#"
            ne.block_number ASC NULLS FIRST,
            ne.chain_id DESC NULLS FIRST,
            ne.block_hash ASC NULLS FIRST,
            ne.transaction_hash ASC NULLS FIRST,
            ne.log_index ASC NULLS FIRST,
            ne.event_identity ASC
        "#,
        ),
    };
}

async fn ensure_history_cursor_exists(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    cursor: &HistoryCursor,
    include_candidates: bool,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT EXISTS (
            SELECT 1
        "#,
    );
    let mut cursor_filter = filter.clone();
    if !cursor_filter.bind_cursor_anchor_to_event_kinds {
        cursor_filter.event_kinds.clear();
    }
    push_history_source_for_filter(
        &mut builder,
        &cursor_filter,
        canonical_only,
        false,
        include_candidates,
    );
    push_history_filters(&mut builder, &cursor_filter, canonical_only);
    if !include_candidates {
        push_product_history_duplicate_filter(&mut builder, &cursor_filter, canonical_only);
    }
    builder.push(" AND ne.event_identity = ");
    builder.push_bind(&cursor.event_identity);
    builder.push(" LIMIT 1)");

    let exists = builder
        .build_query_scalar::<bool>()
        .fetch_one(&mut *connection)
        .await
        .context("failed to validate normalized-event history cursor")?;

    if exists {
        Ok(())
    } else {
        Err(InvalidHistoryCursor.into())
    }
}

fn push_history_cursor_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a HistoryCursor,
) {
    builder.push(
        r#"
        WITH history_cursor_row AS (
            SELECT
                block_number,
                chain_id,
                block_hash,
                transaction_hash,
                log_index,
                event_identity
            FROM normalized_events
            WHERE event_identity =
        "#,
    );
    builder.push_bind(&cursor.event_identity);
    builder.push(") ");
}

/// Keyset continuation predicate. `later` sorts after `earlier` in the canonical
/// descending order; the ascending direction swaps the two rows because it is
/// the exact reverse of that order.
fn push_history_cursor_after(builder: &mut QueryBuilder<'_, Postgres>, order: HistoryOrder) {
    let (later, earlier) = match order {
        HistoryOrder::Desc => ("ne", "cursor_row"),
        HistoryOrder::Asc => ("cursor_row", "ne"),
    };
    builder.push(format!(
        r#"
        (
            CASE WHEN {later}.block_number IS NULL THEN 1 ELSE 0 END
                > CASE WHEN {earlier}.block_number IS NULL THEN 1 ELSE 0 END
            OR (
                CASE WHEN {later}.block_number IS NULL THEN 1 ELSE 0 END
                    = CASE WHEN {earlier}.block_number IS NULL THEN 1 ELSE 0 END
                AND (
                    {later}.block_number < {earlier}.block_number
                    OR (
                        {later}.block_number IS NOT DISTINCT FROM {earlier}.block_number
                        AND (
                            CASE WHEN {later}.chain_id IS NULL THEN 1 ELSE 0 END
                                > CASE WHEN {earlier}.chain_id IS NULL THEN 1 ELSE 0 END
                            OR (
                                CASE WHEN {later}.chain_id IS NULL THEN 1 ELSE 0 END
                                    = CASE WHEN {earlier}.chain_id IS NULL THEN 1 ELSE 0 END
                                AND (
                                    {later}.chain_id > {earlier}.chain_id
                                    OR (
                                        {later}.chain_id IS NOT DISTINCT FROM {earlier}.chain_id
                                        AND (
                                            CASE WHEN {later}.block_hash IS NULL THEN 1 ELSE 0 END
                                                > CASE WHEN {earlier}.block_hash IS NULL THEN 1 ELSE 0 END
                                            OR (
                                                CASE WHEN {later}.block_hash IS NULL THEN 1 ELSE 0 END
                                                    = CASE WHEN {earlier}.block_hash IS NULL THEN 1 ELSE 0 END
                                                AND (
                                                    {later}.block_hash < {earlier}.block_hash
                                                    OR (
                                                        {later}.block_hash IS NOT DISTINCT FROM {earlier}.block_hash
                                                        AND (
                                                            CASE WHEN {later}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                > CASE WHEN {earlier}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                            OR (
                                                                CASE WHEN {later}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                    = CASE WHEN {earlier}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                AND (
                                                                    {later}.transaction_hash < {earlier}.transaction_hash
                                                                    OR (
                                                                        {later}.transaction_hash IS NOT DISTINCT FROM {earlier}.transaction_hash
                                                                        AND (
                                                                            COALESCE({later}.log_index, -1) < COALESCE({earlier}.log_index, -1)
                                                                            OR (
                                                                                COALESCE({later}.log_index, -1) = COALESCE({earlier}.log_index, -1)
                                                                                AND {later}.event_identity < {earlier}.event_identity
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
        "#,
    ));
}

fn history_cursor_from_row(row: &HistoryEvent) -> HistoryCursor {
    HistoryCursor {
        normalized_event_id: row.normalized_event_id,
        event_identity: row.event_identity.clone(),
    }
}
