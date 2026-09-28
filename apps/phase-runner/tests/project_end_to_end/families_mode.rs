//! The harness's publication-switch mode (TYR-36 step 7b-6). The corpus and disposable-copy tests
//! run with the switch the binaries would hold: `BIGNAME_SERVE_FROM_FAMILIES` over the build's
//! default (`bigname_storage::publication_source::configured`), scoped to the test's task. So
//! the flip, which changes only that default, also moves the harness onto the families. In that
//! mode the fixture brings the families to the previous publication before the first target; a
//! disposable copy must already have live families, as a deployment would.
//!
//! With the switch on, four things change in `run`:
//! - the runner's Project batch is the family run (the served batch stops), so the served clock
//!   is the family run and stops when the marker's generation is servable;
//! - the served tables the shadow comparisons read are published outside that clock by the served
//!   engine and hydrator driven directly (`served_batch.rs`), after the harness checked that the
//!   batch itself wrote no served row;
//! - the route readers (the name and subname readers of `endpoint.rs`, and the composed read in
//!   the clock) serve the owned key families, so the rebuild comparison compares the families'
//!   incremental and rebuilt answers through the routes' own readers;
//! - the listing walks' cost is measured in submitted rows ([`measure_walks`]).
//!
//! The shadow comparisons (control, name composition, records, topology) keep comparing the
//! served tables with the family readers: their served side runs with the switch off
//! ([`served_side`]), since with it on the switch-aware served readers would read the families
//! and compare them with themselves.
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Result, anyhow};
use bigname_storage::{
    NameCurrentExpiringFilter, NameCurrentListCursor, NameCurrentListFilter, NameCurrentListOrder,
    NameCurrentListPage,
    families::name::{
        load_family_expiring_page, load_family_search_page, seams::with_submitted_rows_counter,
    },
    publication_source::{self, serve_from_families, with_serve_from_families},
};
use sqlx::{PgPool, types::time::OffsetDateTime};

/// The switch the binaries would hold.
pub fn configured() -> Result<bool> {
    publication_source::configured().map_err(|message| anyhow!(message))
}

/// Runs `future` with the switch the binaries would hold, and says which.
pub async fn scoped<F: Future>(future: F) -> Result<F::Output> {
    let on = configured()?;
    eprintln!("SEPOLIA_END_TO_END_MODE serve_from_families={on}");
    Ok(with_serve_from_families(on, future).await)
}

/// Runs a shadow comparison's `future` with the switch off, so its served side reads the served
/// tables whatever the harness mode.
pub async fn served_side<F: Future>(future: F) -> F::Output {
    with_serve_from_families(false, future).await
}

/// Whether the harness runs with the switch on.
pub fn on() -> bool {
    serve_from_families()
}

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
/// `max_pages` pages each, and prints what each submitted (flip prerequisite 1: the walks must
/// stay linear in the rows they serve; the flip commit bounds `submitted` against `rows`).
pub async fn measure_walks(
    pool: &PgPool,
    target: i64,
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
        expires_after: Some(OffsetDateTime::UNIX_EPOCH),
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
