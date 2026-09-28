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

#[cfg(any(test, feature = "test-support"))]
pub(super) use scoped::note_inventory_read;
#[cfg(any(test, feature = "test-support"))]
pub use scoped::with_inventory_read_counter;

#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn note_inventory_read(_resources: usize) {}
