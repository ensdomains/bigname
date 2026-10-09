use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// Cumulative work performed by this engine's speculative preparation workers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpeculationStats {
    /// Complete lookahead batches successfully prepared by workers.
    pub prepared: u64,
    /// Prepared batches accepted after their database inputs still matched.
    pub accepted: u64,
    /// Prepared batches discarded and interpreted again at their ordered turn.
    pub retried: u64,
    /// Batches using the serial path because speculative preparation was unavailable.
    pub fallback: u64,
    pub validations: u64,
    pub validation_nanoseconds: u64,
    /// Workers currently reading or preparing, including cancelled work still finishing.
    pub active_workers: usize,
    pub peak_active_workers: usize,
}

#[derive(Default)]
pub(super) struct Counters {
    pub(super) prepared: AtomicU64,
    pub(super) accepted: AtomicU64,
    pub(super) retried: AtomicU64,
    pub(super) fallback: AtomicU64,
    pub(super) validations: AtomicU64,
    pub(super) validation_nanoseconds: AtomicU64,
    active_workers: AtomicUsize,
    peak_active_workers: AtomicUsize,
}

impl Counters {
    pub(super) fn snapshot(&self) -> SpeculationStats {
        SpeculationStats {
            prepared: self.prepared.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            fallback: self.fallback.load(Ordering::Relaxed),
            validations: self.validations.load(Ordering::Relaxed),
            validation_nanoseconds: self.validation_nanoseconds.load(Ordering::Relaxed),
            active_workers: self.active_workers.load(Ordering::Relaxed),
            peak_active_workers: self.peak_active_workers.load(Ordering::Relaxed),
        }
    }

    pub(super) fn enter(self: &Arc<Self>) -> ActiveWorker {
        let active = self.active_workers.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_active_workers
            .fetch_max(active, Ordering::Relaxed);
        ActiveWorker(Arc::clone(self))
    }
}

pub(super) struct ActiveWorker(Arc<Counters>);

impl Drop for ActiveWorker {
    fn drop(&mut self) {
        self.0.active_workers.fetch_sub(1, Ordering::Relaxed);
    }
}
