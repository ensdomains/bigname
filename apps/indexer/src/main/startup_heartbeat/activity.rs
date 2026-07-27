use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::sync::Notify;

#[derive(Clone, Default)]
pub(crate) struct RequiredSubtaskActivity {
    gate: Arc<RequiredSubtaskActivityGate>,
}

pub(crate) struct RequiredSubtaskActivityGuard {
    gate: Arc<RequiredSubtaskActivityGate>,
    activity_id: u64,
}

pub(crate) struct RequiredSubtaskExclusionGuard {
    gate: Arc<RequiredSubtaskActivityGate>,
}

#[derive(Default)]
struct RequiredSubtaskActivityGate {
    state: Mutex<RequiredSubtaskActivityState>,
    changed: Notify,
}

#[derive(Default)]
struct RequiredSubtaskActivityState {
    next_activity_id: u64,
    active: BTreeSet<u64>,
    parent_waiting: bool,
    parent_blockers: BTreeSet<u64>,
    parent_active: bool,
}

struct RequiredSubtaskExclusionReservation {
    gate: Arc<RequiredSubtaskActivityGate>,
    acquired: bool,
}

impl RequiredSubtaskActivity {
    pub(crate) async fn begin(&self) -> RequiredSubtaskActivityGuard {
        loop {
            let changed = self.gate.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = lock_required_subtask_activity(&self.gate);
                // A parent queued behind one wedged lane must not stop peer lanes at their next
                // iteration boundary. Peers may keep joining until every lane that was active
                // when the parent arrived has drained. Admission then closes, later peers drain,
                // and the parent gets an exclusive boundary.
                let admission_open = !state.parent_active
                    && (!state.parent_waiting || !state.parent_blockers.is_empty());
                if admission_open {
                    let activity_id = state.next_activity_id;
                    state.next_activity_id = state
                        .next_activity_id
                        .checked_add(1)
                        .expect("required subtask activity id overflowed");
                    assert!(
                        state.active.insert(activity_id),
                        "required subtask activity id must be unique"
                    );
                    return RequiredSubtaskActivityGuard {
                        gate: Arc::clone(&self.gate),
                        activity_id,
                    };
                }
            }
            changed.await;
        }
    }

    pub(crate) async fn exclude_required_subtask(&self) -> RequiredSubtaskExclusionGuard {
        RequiredSubtaskExclusionReservation::reserve(Arc::clone(&self.gate))
            .await
            .acquire()
            .await
    }

    pub(crate) async fn exclude_required_subtask_or_shutdown<F>(
        &self,
        shutdown: F,
    ) -> Option<RequiredSubtaskExclusionGuard>
    where
        F: Future,
    {
        tokio::select! {
            biased;
            _ = shutdown => None,
            exclusion = self.exclude_required_subtask() => Some(exclusion),
        }
    }
}

impl Drop for RequiredSubtaskActivityGuard {
    fn drop(&mut self) {
        {
            let mut state = lock_required_subtask_activity(&self.gate);
            assert!(
                state.active.remove(&self.activity_id),
                "required subtask activity guard must own an active id"
            );
            state.parent_blockers.remove(&self.activity_id);
        }
        self.gate.changed.notify_waiters();
    }
}

impl Drop for RequiredSubtaskExclusionGuard {
    fn drop(&mut self) {
        {
            let mut state = lock_required_subtask_activity(&self.gate);
            assert!(
                state.parent_active,
                "required subtask exclusion guard must own the parent boundary"
            );
            state.parent_active = false;
        }
        self.gate.changed.notify_waiters();
    }
}

impl RequiredSubtaskExclusionReservation {
    async fn reserve(gate: Arc<RequiredSubtaskActivityGate>) -> Self {
        loop {
            let changed = gate.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = lock_required_subtask_activity(&gate);
                if !state.parent_waiting && !state.parent_active {
                    state.parent_waiting = true;
                    state.parent_blockers = state.active.clone();
                    return Self {
                        gate: Arc::clone(&gate),
                        acquired: false,
                    };
                }
            }
            changed.await;
        }
    }

    async fn acquire(mut self) -> RequiredSubtaskExclusionGuard {
        loop {
            let changed = self.gate.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = lock_required_subtask_activity(&self.gate);
                if state.active.is_empty() {
                    assert!(
                        state.parent_waiting,
                        "required subtask exclusion reservation must remain installed"
                    );
                    state.parent_waiting = false;
                    state.parent_blockers.clear();
                    state.parent_active = true;
                    self.acquired = true;
                    return RequiredSubtaskExclusionGuard {
                        gate: Arc::clone(&self.gate),
                    };
                }
            }
            changed.await;
        }
    }
}

impl Drop for RequiredSubtaskExclusionReservation {
    fn drop(&mut self) {
        if self.acquired {
            return;
        }
        {
            let mut state = lock_required_subtask_activity(&self.gate);
            assert!(
                state.parent_waiting,
                "required subtask exclusion reservation must remain installed"
            );
            state.parent_waiting = false;
            state.parent_blockers.clear();
        }
        self.gate.changed.notify_waiters();
    }
}

fn lock_required_subtask_activity(
    gate: &RequiredSubtaskActivityGate,
) -> MutexGuard<'_, RequiredSubtaskActivityState> {
    gate.state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
