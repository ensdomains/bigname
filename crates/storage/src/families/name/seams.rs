//! Test seams of the composed name reader. Like the
//! [publication switch](crate::publication_source)'s scoped value each is a task-local, so tests
//! running in parallel do not see each other's, and each is inert unless a test sets its scope:
//! with no scope set it takes the production path. They are compiled in wherever the
//! `test-support` feature is enabled, which feature unification can do for a release build of
//! the whole workspace.
//!
//! - A pause before a composed read opens its snapshot, so a test can change the family marker
//!   after a route's fence passed and before the composed read sees it
//!   (apps/api/src/tests/v2_switch_names.rs).
//! - A pause inside a composed load, between its publication read and the statements that
//!   follow, so a test can commit the next block in between and prove the load still reads one
//!   snapshot (crates/project/tests/families_name_snapshot.rs).
//! - The candidate batch size of the listings (`list.rs`, `bound.rs`), so a test over a handful
//!   of names can make a page straddle candidate batches.
#[cfg(any(test, feature = "test-support"))]
mod scoped {
    use std::{future::Future, sync::Arc};

    use tokio::sync::Notify;

    tokio::task_local! {
        static PAUSE_BEFORE: (Arc<Notify>, Arc<Notify>);
        static PAUSE: (Arc<Notify>, Arc<Notify>);
        static BATCH_SIZE: usize;
    }

    /// Runs `future` so that every composed read in it, before it opens its snapshot, notifies
    /// `reached` and waits for `resume`.
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

    pub(in crate::families::name) async fn before_snapshot() {
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
pub(super) use scoped::{after_publication, batch_size, before_snapshot};
#[cfg(any(test, feature = "test-support"))]
pub use scoped::{with_batch_size, with_pause_after_publication, with_pause_before_snapshot};

#[cfg(not(any(test, feature = "test-support")))]
pub(super) async fn before_snapshot() {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) async fn after_publication() {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn batch_size(production: usize) -> usize {
    production
}
