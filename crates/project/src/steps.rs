/// The top-level steps of a Project run in the order they run; builder steps carry
/// the builder names.
pub const PROJECT_STEPS: [&str; 20] = [
    "prepare",
    "scope",
    "inputs",
    "name_authority",
    "account_permissions",
    "permissions",
    "name_current",
    "permission_resources",
    "resolver",
    "linked_records",
    "record_inventory",
    "name_topology",
    "children",
    "address_names",
    "address_records",
    "primary_names",
    "child_registrations",
    "integrity",
    "publish",
    "commit",
];

/// Told which step a full rebuild or a redo is in. Such a run is one transaction
/// that can take tens of minutes, so its steps are the only progress it shows.
/// The normal incremental path never reports. An observer shared across runs needs
/// at most one reporting run per chain at a time, since a later run's `None` would
/// clear an earlier run's step; the engine does not enforce this, and the phase
/// runner's per-chain phase lock provides it.
pub trait StepObserver: Send + Sync {
    /// `step` is one of [`PROJECT_STEPS`], or `None` once the run commits or stops.
    fn project_step(&self, chain_id: &str, step: Option<&'static str>);
}

/// Reports one run's steps when the run reports at all, and tells the observer the
/// run is over when dropped: after the commit, on an error, or when the run's
/// future is dropped.
#[derive(Default)]
pub(crate) struct Steps<'a> {
    observer: Option<&'a dyn StepObserver>,
    chain_id: &'a str,
}

impl<'a> Steps<'a> {
    pub(crate) fn new(observer: Option<&'a dyn StepObserver>, chain_id: &'a str) -> Self {
        Self { observer, chain_id }
    }

    pub(crate) fn enter(&self, step: &'static str) {
        if let Some(observer) = self.observer {
            debug_assert!(PROJECT_STEPS.contains(&step), "unknown Project step {step}");
            observer.project_step(self.chain_id, Some(step));
        }
    }
}

impl Drop for Steps<'_> {
    fn drop(&mut self) {
        if let Some(observer) = self.observer {
            observer.project_step(self.chain_id, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct Recorded(Mutex<Vec<Option<&'static str>>>);

    impl StepObserver for Recorded {
        fn project_step(&self, _chain_id: &str, step: Option<&'static str>) {
            self.0.lock().expect("recorded steps").push(step);
        }
    }

    #[test]
    fn a_run_that_stops_early_still_reports_that_it_is_over() {
        let recorded = Recorded::default();
        {
            let steps = Steps::new(Some(&recorded), "chain");
            steps.enter("prepare");
        }
        assert_eq!(
            *recorded.0.lock().expect("recorded steps"),
            [Some("prepare"), None]
        );
    }
}
