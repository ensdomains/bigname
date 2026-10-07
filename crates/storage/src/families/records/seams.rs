//! Task-local test seams for the record family readers. Parallel tests do not share scoped
//! hooks; without a hook the production path runs. The seams are compiled by `test-support`,
//! which workspace feature unification can enable in a release build.
#[cfg(any(test, feature = "test-support"))]
mod scoped {
    use std::{
        future::Future,
        sync::{Arc, Mutex},
    };

    tokio::task_local! {
        static INVENTORY_READS: Arc<Mutex<Vec<usize>>>;
        static LOOKUP_WORK: Arc<Mutex<Vec<serde_json::Value>>>;
        static ROLE_READS: Arc<Mutex<Vec<&'static str>>>;
        static COMPOSE_CHUNK: usize;
        static COMPOSED_NAME_BATCHES: Arc<Mutex<Vec<usize>>>;
        static EXACT_TOTAL_CAP: usize;
        static ADDRESS_NAME_PATHS: Arc<Mutex<Vec<&'static str>>>;
    }

    /// Passive work/stage observations for one caller. No selection or write behavior changes.
    pub async fn with_lookup_work<F: Future>(
        observations: Arc<Mutex<Vec<serde_json::Value>>>,
        future: F,
    ) -> F::Output {
        LOOKUP_WORK.scope(observations, future).await
    }

    pub fn lookup_work_timer() -> Option<std::time::Instant> {
        LOOKUP_WORK.try_with(|_| std::time::Instant::now()).ok()
    }

    pub fn note_lookup_work(observation: impl FnOnce() -> serde_json::Value) {
        let _ = LOOKUP_WORK.try_with(|observations| {
            if let Ok(mut observations) = observations.lock() {
                observations.push(observation());
            }
        });
    }

    /// Runs `future` with an address-names page walking instead of composing every candidate
    /// once the address has more than `cap` candidate names, instead of
    /// [`super::EXACT_TOTAL_CAP`].
    pub async fn with_exact_total_cap<F: Future>(cap: usize, future: F) -> F::Output {
        EXACT_TOTAL_CAP.scope(cap, future).await
    }

    pub(crate) fn exact_total_cap() -> usize {
        EXACT_TOTAL_CAP
            .try_with(|cap| *cap)
            .unwrap_or(super::EXACT_TOTAL_CAP)
    }

    /// Runs `future` appending to `paths` how each capped address-names page was read:
    /// `exact`, `walk`, or `fallback` when a walk gave way to the exact read.
    pub async fn with_address_name_paths<F: Future>(
        paths: Arc<Mutex<Vec<&'static str>>>,
        future: F,
    ) -> F::Output {
        ADDRESS_NAME_PATHS.scope(paths, future).await
    }

    pub(crate) fn note_address_name_path(path: &'static str) {
        let _ = ADDRESS_NAME_PATHS.try_with(|paths| {
            if let Ok(mut paths) = paths.lock() {
                paths.push(path);
            }
        });
    }

    /// Runs `future` with the address reads composing names `size` at a time instead of
    /// [`super::COMPOSE_CHUNK`].
    pub async fn with_compose_chunk<F: Future>(size: usize, future: F) -> F::Output {
        COMPOSE_CHUNK.scope(size.max(1), future).await
    }

    pub(crate) fn compose_chunk() -> usize {
        COMPOSE_CHUNK
            .try_with(|size| *size)
            .unwrap_or(super::COMPOSE_CHUNK)
    }

    /// Runs `future` appending to `batches` the name count of every composition call the
    /// address reads (address names, former owners, resolves-to, address history) make in it.
    pub async fn with_composed_name_batches<F: Future>(
        batches: Arc<Mutex<Vec<usize>>>,
        future: F,
    ) -> F::Output {
        COMPOSED_NAME_BATCHES.scope(batches, future).await
    }

    pub(in crate::families::records) fn note_composed_batch(names: usize) {
        let _ = COMPOSED_NAME_BATCHES.try_with(|batches| {
            if let Ok(mut batches) = batches.lock() {
                batches.push(names);
            }
        });
    }

    /// Records only the role-candidate and role-grant queries a request executes. This does
    /// not count ordinary ownership candidates, composition, or other permission reads.
    pub async fn with_role_read_counter<F: Future>(
        reads: Arc<Mutex<Vec<&'static str>>>,
        future: F,
    ) -> F::Output {
        ROLE_READS.scope(reads, future).await
    }

    pub(in crate::families::records) fn note_role_read(query: &'static str) {
        let _ = ROLE_READS.try_with(|reads| {
            if let Ok(mut reads) = reads.lock() {
                reads.push(query);
            }
        });
    }

    /// Runs `future` appending to `reads` the resource count of every record inventory read in
    /// it. Apart from the mirror walk, which reads per mirror pointer, each read runs a fixed
    /// number of statements whatever its resource count, so the list shows how many inventory
    /// round trips a request made. It does not count the request's other statements.
    pub async fn with_inventory_read_counter<F: Future>(
        reads: Arc<Mutex<Vec<usize>>>,
        future: F,
    ) -> F::Output {
        INVENTORY_READS.scope(reads, future).await
    }

    pub(in crate::families::records) fn note_inventory_read(resources: usize) {
        let _ = INVENTORY_READS.try_with(|reads| {
            if let Ok(mut reads) = reads.lock() {
                reads.push(resources);
            }
        });
    }
}

/// How many names an address read composes at a time, so its working set is one chunk of
/// composed rows rather than every name the address holds.
const COMPOSE_CHUNK: usize = 256;

/// The most candidate names an address-names page composes in full for an exact total
/// without `include=total_count`; above it the page walks and its total is null.
const EXACT_TOTAL_CAP: usize = 1_000;

#[cfg(any(test, feature = "test-support"))]
pub(crate) use scoped::{compose_chunk, exact_total_cap, note_address_name_path};
#[cfg(any(test, feature = "test-support"))]
pub use scoped::{
    lookup_work_timer, note_lookup_work, with_address_name_paths, with_compose_chunk,
    with_composed_name_batches, with_exact_total_cap, with_inventory_read_counter,
    with_lookup_work, with_role_read_counter,
};
#[cfg(any(test, feature = "test-support"))]
pub(super) use scoped::{note_composed_batch, note_inventory_read, note_role_read};

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) fn exact_total_cap() -> usize {
    EXACT_TOTAL_CAP
}

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) fn note_address_name_path(_path: &'static str) {}

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) fn compose_chunk() -> usize {
    COMPOSE_CHUNK
}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn note_composed_batch(_names: usize) {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn note_inventory_read(_resources: usize) {}

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn note_role_read(_query: &'static str) {}

#[cfg(not(any(test, feature = "test-support")))]
pub fn lookup_work_timer() -> Option<std::time::Instant> {
    None
}

#[cfg(not(any(test, feature = "test-support")))]
pub fn note_lookup_work(_observation: impl FnOnce() -> serde_json::Value) {}
