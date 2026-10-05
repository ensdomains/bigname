//! Task-local test seams for the composed name reader. Parallel tests do not share scoped
//! hooks; without a hook the production path runs. The seams are compiled by `test-support`,
//! which workspace feature unification can enable in a release build.
#[cfg(any(test, feature = "test-support"))]
mod scoped {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use tokio::sync::Notify;

    tokio::task_local! {
        static PAUSE_BEFORE: (Arc<Notify>, Arc<Notify>);
        static PAUSE: (Arc<Notify>, Arc<Notify>);
        static BATCH_SIZE: usize;
        static SUBMITTED_ROWS: Arc<AtomicU64>;
        static COMPOSED_NAMES: Arc<AtomicU64>;
        static PEAK_SOURCE: Arc<AtomicU64>;
    }

    /// Runs `future` adding to `counter` every composed row a listing walk in it submits to its
    /// page statement.
    pub async fn with_submitted_rows_counter<F: Future>(
        counter: Arc<AtomicU64>,
        future: F,
    ) -> F::Output {
        SUBMITTED_ROWS.scope(counter, future).await
    }

    /// Runs `future` adding to `counter` every name a listing in it asks to compose, so a test
    /// can bound composition work apart from what reaches the page statement.
    pub async fn with_composed_names_counter<F: Future>(
        counter: Arc<AtomicU64>,
        future: F,
    ) -> F::Output {
        COMPOSED_NAMES.scope(counter, future).await
    }

    /// Runs `future` raising `peak` to the largest composed-row source a listing in it binds to
    /// one page statement.
    pub async fn with_peak_source_counter<F: Future>(peak: Arc<AtomicU64>, future: F) -> F::Output {
        PEAK_SOURCE.scope(peak, future).await
    }

    pub(in crate::families::name) fn note_submitted_rows(rows: usize) {
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        let _ = SUBMITTED_ROWS.try_with(|counter| counter.fetch_add(rows, Ordering::Relaxed));
        let _ = PEAK_SOURCE.try_with(|peak| peak.fetch_max(rows, Ordering::Relaxed));
    }

    pub(in crate::families::name) fn note_composed_names(names: usize) {
        let _ = COMPOSED_NAMES.try_with(|counter| {
            counter.fetch_add(u64::try_from(names).unwrap_or(u64::MAX), Ordering::Relaxed)
        });
    }

    /// Runs `future` so that every composed read in it, and every request read snapshot
    /// ([`crate::begin_read_snapshot`]), before it opens its snapshot, notifies `reached` and
    /// waits for `resume`.
    pub async fn with_pause_before_snapshot<F: Future>(
        reached: Arc<Notify>,
        resume: Arc<Notify>,
        future: F,
    ) -> F::Output {
        PAUSE_BEFORE.scope((reached, resume), future).await
    }

    /// Runs `future` so that every composed load in it, once it has read its publication,
    /// notifies `reached` and waits for `resume`.
    pub async fn with_pause_after_publication<F: Future>(
        reached: Arc<Notify>,
        resume: Arc<Notify>,
        future: F,
    ) -> F::Output {
        PAUSE.scope((reached, resume), future).await
    }

    /// Runs `future` with every composed listing in it walking `size` candidates per batch.
    pub async fn with_batch_size<F: Future>(size: usize, future: F) -> F::Output {
        BATCH_SIZE.scope(size.max(1), future).await
    }

    pub(crate) async fn before_snapshot() {
        if let Ok((reached, resume)) = PAUSE_BEFORE.try_with(Clone::clone) {
            reached.notify_one();
            resume.notified().await;
        }
    }

    pub(in crate::families::name) async fn after_publication() {
        if let Ok((reached, resume)) = PAUSE.try_with(Clone::clone) {
            reached.notify_one();
            resume.notified().await;
        }
    }

    pub(in crate::families::name) fn batch_size(production: usize) -> usize {
        BATCH_SIZE.try_with(|size| *size).unwrap_or(production)
    }
}

#[cfg(any(test, feature = "test-support"))]
pub use super::list::load_family_expiring_page_unbounded;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use scoped::before_snapshot;
#[cfg(any(test, feature = "test-support"))]
pub(super) use scoped::{after_publication, batch_size, note_composed_names, note_submitted_rows};
#[cfg(any(test, feature = "test-support"))]
pub use scoped::{
    with_batch_size, with_composed_names_counter, with_pause_after_publication,
    with_pause_before_snapshot, with_peak_source_counter, with_submitted_rows_counter,
};

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) async fn before_snapshot() {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) async fn after_publication() {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn batch_size(production: usize) -> usize {
    production
}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn note_submitted_rows(_rows: usize) {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn note_composed_names(_names: usize) {}
