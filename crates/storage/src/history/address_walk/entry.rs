//! Route supported product reads to the bounded walker; retain explicit legacy storage modes.

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::AddressNameRelation;
#[cfg(any(test, feature = "test-support"))]
use crate::history::history_anchor_read_test_hooks;
use crate::history::{
    EventHistoryReadFilter, HistoryCursor, HistoryPage, HistoryPageOptions, HistoryScope,
    HistorySummaryMode, address_matches::load_address_history_selector, paging, redo,
};

/// Load one SQL-keyset page for one address-derived anchor set.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_history_page_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    options: &HistoryPageOptions,
    require_interpret_not_redo: bool,
) -> Result<HistoryPage> {
    let interpret_redo_fence = redo::capture_fence_if(pool, require_interpret_not_redo).await?;
    let normalized_address = address.to_ascii_lowercase();
    if canonical_only
        && options.publication_block_bounds.is_some()
        && summary_mode != HistorySummaryMode::Full
    {
        // The hook separates the captured redo fence from the read snapshot. Membership,
        // attribution, count and page payloads below all share that one snapshot.
        #[cfg(any(test, feature = "test-support"))]
        history_anchor_read_test_hooks::run_if(pool, require_interpret_not_redo).await?;
        return super::load_page(
            pool,
            &normalized_address,
            namespace,
            relations,
            scope,
            cursor,
            page_size,
            summary_mode,
            options,
            interpret_redo_fence.as_ref(),
        )
        .await;
    }
    let selector = load_address_history_selector(
        pool,
        &normalized_address,
        namespace,
        relations,
        scope,
        canonical_only,
        false,
        options.publication_block_bounds.as_ref(),
    )
    .await?;

    #[cfg(any(test, feature = "test-support"))]
    history_anchor_read_test_hooks::run_if(pool, require_interpret_not_redo).await?;

    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        }
        .with_page_options(options),
        canonical_only,
        cursor,
        page_size,
        summary_mode,
        false,
        interpret_redo_fence.as_ref(),
    )
    .await
    .with_context(|| {
        let mut parts = vec![format!("address {}", normalized_address)];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
            parts.push(format!(
                "relations {}",
                relations
                    .iter()
                    .map(|relation| relation.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        parts.push(format!("scope {}", scope.as_str()));
        format!("failed to load history page for {}", parts.join(" "))
    })
}

/// A bounded product page sharing the caller's already admitted read-only snapshot.
/// The pool entry above remains available for storage callers with independent admission.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_history_page_for_relations_on(
    connection: &mut sqlx::PgConnection,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    options: &HistoryPageOptions,
) -> Result<HistoryPage> {
    anyhow::ensure!(
        options.publication_block_bounds.is_some() && summary_mode != HistorySummaryMode::Full,
        "shared address-history reads require bounded product options"
    );
    let fence = redo::capture_interpret_redo_fence_on(connection).await?;
    #[cfg(any(test, feature = "test-support"))]
    history_anchor_read_test_hooks::run_on(
        connection,
        history_anchor_read_test_hooks::HistoryReadHookPoint::AfterAnchors,
    )
    .await?;
    super::load_page_on(
        connection,
        &address.to_ascii_lowercase(),
        namespace,
        relations,
        scope,
        cursor,
        page_size,
        summary_mode,
        options,
        Some(&fence),
    )
    .await
}
