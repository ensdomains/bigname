//! Permanent family listing walks and diagnostic operation counts.
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use bigname_storage::{
    NameCurrentExpiringFilter, NameCurrentListCursor, NameCurrentListFilter, NameCurrentListOrder,
    NameCurrentListPage,
    families::name::{
        load_family_expiring_page, load_family_search_page, seams::with_submitted_rows_counter,
    },
};
use sqlx::{PgPool, types::time::OffsetDateTime};

/// What one listing walk cost.
#[derive(Debug, Default)]
pub struct Walk {
    pub pages: u64,
    pub rows: u64,
    /// Composed rows submitted to the page statement, summed over every batch of every page.
    pub submitted: u64,
    /// Whether the walk stopped at `max_pages` before the listing ended.
    pub capped: bool,
}

/// Walks the composed /v1/search listing (the namespace, supported names only, no name filter)
/// and the composed expiring listing (every expiry, ascending) at `page_size` for at most
/// `max_pages` pages each, and prints what each submitted: the walks must stay linear in the rows
/// they serve, so the caller bounds `submitted` against `rows`.
pub async fn measure_walks(
    pool: &PgPool,
    target: i64,
    chain: &str,
    namespace: &str,
    page_size: u64,
    max_pages: u64,
) -> Result<(Walk, Walk)> {
    let search_filter = NameCurrentListFilter {
        namespace: Some(namespace.to_owned()),
        supported_only: true,
        ..NameCurrentListFilter::default()
    };
    let search = walk(max_pages, |cursor| {
        let filter = search_filter.clone();
        async move { load_family_search_page(pool, &filter, cursor.as_ref(), page_size).await }
    })
    .await?;
    let expiring_filter = NameCurrentExpiringFilter {
        namespace: namespace.to_owned(),
        expires_after: Some(OffsetDateTime::UNIX_EPOCH.into()),
        expires_before: None,
    };
    let expiring = walk(max_pages, |cursor| {
        let filter = expiring_filter.clone();
        async move {
            load_family_expiring_page(
                pool,
                &filter,
                NameCurrentListOrder::Asc,
                cursor.as_ref(),
                page_size,
                &[chain.to_owned()],
            )
            .await
        }
    })
    .await?;
    for (listing, walk) in [("search", &search), ("expiring", &expiring)] {
        eprintln!(
            "SEPOLIA_END_TO_END_WALK target={target} listing={listing} page_size={page_size} \
             pages={} rows={} submitted={} capped={}",
            walk.pages, walk.rows, walk.submitted, walk.capped
        );
    }
    Ok((search, expiring))
}

async fn walk<F, Fut>(max_pages: u64, mut page: F) -> Result<Walk>
where
    F: FnMut(Option<NameCurrentListCursor>) -> Fut,
    Fut: Future<Output = Result<NameCurrentListPage>>,
{
    let counter = Arc::new(AtomicU64::new(0));
    let mut walk = Walk::default();
    let mut cursor = None;
    loop {
        if walk.pages == max_pages {
            walk.capped = true;
            break;
        }
        let served = with_submitted_rows_counter(counter.clone(), page(cursor.take())).await?;
        walk.pages += 1;
        walk.rows += served.rows.len() as u64;
        match served.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    walk.submitted = counter.load(Ordering::Relaxed);
    Ok(walk)
}
