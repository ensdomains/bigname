//! Which Project publication the serving reads fence on, chosen once per process.
//!
//! Off (the default), every serving fence reads the Project row of
//! `bigname_phase.chain_phase_state`: its position while the phase is `completed` or `running`,
//! and its row version (`xmin`) as the served generation. On, the same fences read the
//! [family marker](../../../docs/glossary.md#family-marker) instead: its position while its
//! `state` is `live`, and its `sequence` as the served generation, and a collection's expiry
//! clock is the published block's timestamp rather than the request time. See the
//! [publication switch](../../../docs/glossary.md#publication-switch).
//!
//! The switch exists only while the reads move from the served tables to the owned key families
//! one route group at a time (TYR-36 step 7b); step 7c deletes it with the served batch.

use std::sync::atomic::{AtomicBool, Ordering};

/// The environment variable apps/api and apps/phase-runner read once at startup: `1` or `true`
/// turns the switch on; anything else, or leaving it unset, keeps it off.
pub const SERVE_FROM_FAMILIES_ENV: &str = "BIGNAME_SERVE_FROM_FAMILIES";

static SERVE_FROM_FAMILIES: AtomicBool = AtomicBool::new(false);

#[cfg(any(test, feature = "test-support"))]
tokio::task_local! {
    static SCOPED_SERVE_FROM_FAMILIES: bool;
}

/// Reads [`SERVE_FROM_FAMILIES_ENV`] and holds the result for the rest of the process. Returns
/// the value now held, for the caller's startup log.
pub fn init_from_env() -> bool {
    let on = parse(std::env::var(SERVE_FROM_FAMILIES_ENV).ok().as_deref());
    SERVE_FROM_FAMILIES.store(on, Ordering::Relaxed);
    on
}

/// Whether the serving fences read the family marker.
pub fn serve_from_families() -> bool {
    #[cfg(any(test, feature = "test-support"))]
    if let Ok(on) = SCOPED_SERVE_FROM_FAMILIES.try_with(|on| *on) {
        return on;
    }
    SERVE_FROM_FAMILIES.load(Ordering::Relaxed)
}

/// Runs `future` with the switch fixed to `on` for that task only. Tests share one process and
/// run in parallel, so they cannot flip the process-wide value.
#[cfg(any(test, feature = "test-support"))]
pub async fn with_serve_from_families<F: std::future::Future>(on: bool, future: F) -> F::Output {
    SCOPED_SERVE_FROM_FAMILIES.scope(on, future).await
}

fn parse(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_and_true_turn_the_switch_on() {
        assert!(parse(Some("1")));
        assert!(parse(Some("true")));
        for off in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("TRUE"),
            Some("yes"),
        ] {
            assert!(!parse(off), "{off:?} must leave the switch off");
        }
    }

    #[tokio::test]
    async fn a_scoped_value_overrides_the_process_value_for_its_task_only() {
        assert!(!serve_from_families());
        assert!(with_serve_from_families(true, async { serve_from_families() }).await);
        assert!(!serve_from_families());
    }
}
