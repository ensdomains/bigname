use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    EventHistoryFilter, HistoryCursor, HistoryPage, HistorySummaryMode, event_history_read_filter,
    paging, redo::capture_fence_if,
};

/// Load one SQL-keyset page for event-history filters, optionally including rows whose
/// `consumer_visibility` is `candidate`.
pub async fn load_event_history_page(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    include_candidates: bool,
) -> Result<HistoryPage> {
    load_event_history_page_with_redo_policy(
        pool,
        filter,
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        include_candidates,
        false,
    )
    .await
}

/// Load one SQL-keyset event-history page with an optional `redo_in_progress` check.
#[allow(clippy::too_many_arguments)]
pub async fn load_event_history_page_with_redo_policy(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    include_candidates: bool,
    require_interpret_not_redo: bool,
) -> Result<HistoryPage> {
    let interpret_redo_fence = capture_fence_if(pool, require_interpret_not_redo).await?;
    let read_filter =
        event_history_read_filter(pool, filter, canonical_only, include_candidates).await?;
    #[cfg(any(test, feature = "test-support"))]
    if require_interpret_not_redo {
        super::history_anchor_read_test_hooks::run(
            pool,
            super::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterAnchors,
        )
        .await?;
    }
    paging::load_history_page(
        pool,
        read_filter,
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        include_candidates,
        interpret_redo_fence.as_ref(),
    )
    .await
    .context("failed to load app-facing event history page")
}
