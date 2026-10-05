//! Request-local measurements at live materialization boundaries of the production reader.

#[cfg(any(test, feature = "test-support"))]
mod scoped {
    use std::{
        collections::BTreeMap,
        future::Future,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Debug)]
    pub struct AddressHistoryWorkingSet {
        pub batch_size: usize,
        pub cache_capacity: usize,
        pub live: BTreeMap<&'static str, usize>,
        pub peak: BTreeMap<&'static str, usize>,
        pub counters: BTreeMap<&'static str, usize>,
        pub combined_peak: usize,
    }

    impl Default for AddressHistoryWorkingSet {
        fn default() -> Self {
            Self {
                batch_size: 256,
                cache_capacity: 1_024,
                live: BTreeMap::new(),
                peak: BTreeMap::new(),
                counters: BTreeMap::new(),
                combined_peak: 0,
            }
        }
    }

    tokio::task_local! { static WORKING_SET: Arc<Mutex<AddressHistoryWorkingSet>>; }

    pub async fn with_address_history_working_set<F: Future>(
        stats: Arc<Mutex<AddressHistoryWorkingSet>>,
        future: F,
    ) -> F::Output {
        WORKING_SET.scope(stats, future).await
    }

    pub(super) fn settings() -> (usize, usize) {
        WORKING_SET
            .try_with(|stats| {
                let stats = stats.lock().expect("address-history metrics lock");
                (
                    stats.batch_size.clamp(1, 256),
                    stats.cache_capacity.max(256),
                )
            })
            .unwrap_or((256, 1_024))
    }

    pub(super) fn change(kind: &'static str, before: usize, after: usize) {
        let _ = WORKING_SET.try_with(|stats| {
            let mut stats = stats.lock().expect("address-history metrics lock");
            let live = stats.live.entry(kind).or_default();
            *live = live.checked_sub(before).expect("balanced live allocation") + after;
            let live = *live;
            let peak = stats.peak.entry(kind).or_default();
            *peak = (*peak).max(live);
            stats.combined_peak = stats.combined_peak.max(stats.live.values().sum());
        });
    }

    pub(super) fn count(kind: &'static str, amount: usize) {
        let _ = WORKING_SET.try_with(|stats| {
            *stats
                .lock()
                .expect("address-history metrics lock")
                .counters
                .entry(kind)
                .or_default() += amount;
        });
    }
}

#[cfg(any(test, feature = "test-support"))]
pub use scoped::{AddressHistoryWorkingSet, with_address_history_working_set};

pub(super) fn batch_size() -> usize {
    #[cfg(any(test, feature = "test-support"))]
    {
        scoped::settings().0
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        256
    }
}
pub(super) fn cache_capacity() -> usize {
    #[cfg(any(test, feature = "test-support"))]
    {
        scoped::settings().1
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        1_024
    }
}

pub(in crate::history) struct Live {
    #[cfg(any(test, feature = "test-support"))]
    kind: &'static str,
    #[cfg(any(test, feature = "test-support"))]
    rows: usize,
}
impl Live {
    pub(in crate::history) fn new(kind: &'static str, rows: usize) -> Self {
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = kind;
        let mut live = Self {
            #[cfg(any(test, feature = "test-support"))]
            kind,
            #[cfg(any(test, feature = "test-support"))]
            rows: 0,
        };
        live.set(rows);
        live
    }
    pub(in crate::history) fn set(&mut self, rows: usize) {
        #[cfg(any(test, feature = "test-support"))]
        scoped::change(self.kind, self.rows, rows);
        #[cfg(any(test, feature = "test-support"))]
        {
            self.rows = rows;
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = rows;
    }
}
impl Drop for Live {
    fn drop(&mut self) {
        self.set(0);
    }
}

pub(in crate::history) fn count(kind: &'static str, amount: usize) {
    #[cfg(any(test, feature = "test-support"))]
    scoped::count(kind, amount);
    #[cfg(not(any(test, feature = "test-support")))]
    let _ = (kind, amount);
}
