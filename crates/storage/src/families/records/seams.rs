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
        static WALK_FILTER_NAMES: Arc<Mutex<Vec<usize>>>;
        static WALK_CREATED_AT_CLEARED: String;
        static LEGACY_WALK_BATCHES: ();
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

    /// Runs `future` with an address-names walk sizing its steps as it first did: four pages
    /// of candidates, at least 32, then doubling. Tests compare its pages with the current
    /// step sizes'.
    pub async fn with_legacy_walk_batches<F: Future>(future: F) -> F::Output {
        LEGACY_WALK_BATCHES.scope((), future).await
    }

    pub(crate) fn legacy_walk_batches() -> bool {
        LEGACY_WALK_BATCHES.try_with(|_| ()).is_ok()
    }

    /// Runs `future` appending to `paths` how each capped address-names or resolves-to page
    /// was read: `exact`, `walk`, or `fallback` when a walk gave way to the exact read.
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

    /// Runs `future` appending to `names` how many names' rows each filter statement of an
    /// address-names or resolves-to walk reads: its steps, its registration closure and its
    /// read of every candidate.
    pub async fn with_walk_filter_names<F: Future>(
        names: Arc<Mutex<Vec<usize>>>,
        future: F,
    ) -> F::Output {
        WALK_FILTER_NAMES.scope(names, future).await
    }

    pub(crate) fn note_walk_filter(names: usize) {
        let _ = WALK_FILTER_NAMES.try_with(|filters| {
            if let Ok(mut filters) = filters.lock() {
                filters.push(names);
            }
        });
    }

    /// Runs `future` with a `sort=created_at` walk recording the name `logical_name_id` as first
    /// observed at no time when it reads every candidate, as if its key disagreed with
    /// composition: the walk must give way to the full read once it composes that name.
    pub async fn with_walk_created_at_cleared<F: Future>(
        logical_name_id: String,
        future: F,
    ) -> F::Output {
        WALK_CREATED_AT_CLEARED.scope(logical_name_id, future).await
    }

    pub(crate) fn walk_created_at_cleared() -> Option<String> {
        WALK_CREATED_AT_CLEARED.try_with(Clone::clone).ok()
    }

    /// The statement the inverse address read finds its candidate resources with, binding the
    /// address and its coin types. Tests compare its rows and plan with earlier statements'.
    pub fn candidate_statement() -> &'static str {
        super::super::candidates::CANDIDATES
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

/// The most candidate names an address-names or resolves-to page composes in full; above it
/// the page walks, and its total is null unless `include=total_count` asks for it.
const EXACT_TOTAL_CAP: usize = 1_000;

#[cfg(any(test, feature = "test-support"))]
pub use scoped::{
    candidate_statement, lookup_work_timer, note_lookup_work, with_address_name_paths,
    with_compose_chunk, with_composed_name_batches, with_exact_total_cap,
    with_inventory_read_counter, with_legacy_walk_batches, with_lookup_work,
    with_role_read_counter, with_walk_created_at_cleared, with_walk_filter_names,
};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use scoped::{
    compose_chunk, exact_total_cap, legacy_walk_batches, note_address_name_path, note_walk_filter,
    walk_created_at_cleared,
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
pub(crate) fn note_walk_filter(_names: usize) {}

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) fn walk_created_at_cleared() -> Option<String> {
    None
}

#[cfg(not(any(test, feature = "test-support")))]
pub(crate) fn legacy_walk_batches() -> bool {
    false
}

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
