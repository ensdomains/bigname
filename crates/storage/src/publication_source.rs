//! Which Project publication the serving reads fence on, chosen once per process.
//!
//! Off ([`SERVE_FROM_FAMILIES_DEFAULT`] until the flip), every serving fence reads the Project row of
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

/// The environment variable apps/api and apps/phase-runner read once at startup to override
/// [`SERVE_FROM_FAMILIES_DEFAULT`]: `1` or `true` turns the switch on, `0` or `false` turns it
/// off, and unset or empty keeps the default. Any other value refuses to start, so a mistyped
/// override fails loudly instead of silently keeping the default.
pub const SERVE_FROM_FAMILIES_ENV: &str = "BIGNAME_SERVE_FROM_FAMILIES";

/// The switch's value when [`SERVE_FROM_FAMILIES_ENV`] is unset or empty. The flip (TYR-36
/// step 7b-6) is the one-line change of this value to `true`, once every route group reads the
/// owned key families.
pub const SERVE_FROM_FAMILIES_DEFAULT: bool = false;

static SERVE_FROM_FAMILIES: AtomicBool = AtomicBool::new(SERVE_FROM_FAMILIES_DEFAULT);

#[cfg(any(test, feature = "test-support"))]
tokio::task_local! {
    static SCOPED_SERVE_FROM_FAMILIES: bool;
}

/// Reads [`SERVE_FROM_FAMILIES_ENV`] over [`SERVE_FROM_FAMILIES_DEFAULT`] and holds the result
/// for the rest of the process. Returns the value now held, for the caller's startup log, or an
/// error naming the variable when its value is not one of the accepted spellings.
pub fn init_from_env() -> Result<bool, String> {
    let on = configured()?;
    SERVE_FROM_FAMILIES.store(on, Ordering::Relaxed);
    Ok(on)
}

/// The value [`init_from_env`] would hold, without holding it: for a harness that scopes the
/// switch per task (`with_serve_from_families`) the way the binaries set it per process.
pub fn configured() -> Result<bool, String> {
    parse(std::env::var(SERVE_FROM_FAMILIES_ENV).ok().as_deref())
}

/// Whether the serving fences read the family marker.
pub fn serve_from_families() -> bool {
    #[cfg(any(test, feature = "test-support"))]
    if let Ok(on) = SCOPED_SERVE_FROM_FAMILIES.try_with(|on| *on) {
        return on;
    }
    SERVE_FROM_FAMILIES.load(Ordering::Relaxed)
}

/// Holds the switch at `on` for the rest of this test process, over the build's default: for a
/// test binary whose fixtures seed the Project row as the served publication, so its tests keep
/// that publication when the default flips. A scoped value ([`with_serve_from_families`]) still
/// wins, so the switch tests in the same binary keep choosing their state. The binaries never
/// call it; they read the environment once ([`init_from_env`]).
#[cfg(any(test, feature = "test-support"))]
pub fn hold_for_test_process(on: bool) {
    SERVE_FROM_FAMILIES.store(on, Ordering::Relaxed);
}

/// Runs `future` with the switch fixed to `on` for that task only. Tests share one process and
/// run in parallel, so a test that needs a state other than its binary's scopes it here rather
/// than flipping the process-wide value.
#[cfg(any(test, feature = "test-support"))]
pub async fn with_serve_from_families<F: std::future::Future>(on: bool, future: F) -> F::Output {
    SCOPED_SERVE_FROM_FAMILIES.scope(on, future).await
}

fn parse(value: Option<&str>) -> Result<bool, String> {
    match value {
        None | Some("") => Ok(SERVE_FROM_FAMILIES_DEFAULT),
        Some("1" | "true") => Ok(true),
        Some("0" | "false") => Ok(false),
        Some(other) => Err(format!(
            "{SERVE_FROM_FAMILIES_ENV} must be 1, true, 0, false or unset, not {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_override_accepts_four_spellings_and_unset_keeps_the_default() {
        for on in ["1", "true"] {
            assert_eq!(parse(Some(on)), Ok(true), "{on}");
        }
        for off in ["0", "false"] {
            assert_eq!(parse(Some(off)), Ok(false), "{off}");
        }
        for unset in [None, Some("")] {
            assert_eq!(parse(unset), Ok(SERVE_FROM_FAMILIES_DEFAULT), "{unset:?}");
        }
        for invalid in ["TRUE", "yes", "on", "flase", " 1"] {
            let error = parse(Some(invalid)).expect_err(invalid);
            assert!(error.contains(SERVE_FROM_FAMILIES_ENV), "{error}");
        }
    }

    #[tokio::test]
    async fn a_scoped_value_overrides_the_process_value_for_its_task_only() {
        let process = serve_from_families();
        for on in [true, false] {
            assert_eq!(
                with_serve_from_families(on, async { serve_from_families() }).await,
                on
            );
        }
        assert_eq!(serve_from_families(), process);
    }
}
