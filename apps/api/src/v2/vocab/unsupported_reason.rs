//! Which unsupported projection reasons may still serve a name.
//!
//! A route that serves a projected row decides on the row's own coverage status, not on a list of
//! reasons it happens to recognize, so a reason added to the projection later cannot silently
//! start serving `ok` ([#487](https://github.com/ensdomains/bigname/issues/487)).

/// Stands in when a projection row is unsupported but names no reason.
pub(crate) const MISSING_UNSUPPORTED_REASON: &str = "unsupported_reason_missing";

/// The one unsupported projection reason with a ratified partial-serve contract: the name still
/// serves the identity and registration fields that can be served, minus resolver and
/// record-serving fields.
pub(crate) const PARTIAL_SERVE_UNSUPPORTED_REASON: &str = "current_authority_not_projected";

/// Whether an unsupported projection row must be reduced to the identity-only object instead of
/// serving a name detail. Fails closed: a reason this build has never seen downgrades rather than
/// serving a row the projection declined to support. Takes the projection's own reason, not the
/// mapped public one, so the rule cannot drift with the public vocabulary.
pub(crate) fn downgrades_unsupported_name(reason: &str) -> bool {
    reason != PARTIAL_SERVE_UNSUPPORTED_REASON
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two things keep a reason-less unsupported row from reaching a route: name_current's build
    // SQL pairs the status with a reason, and the projection schema CHECKs the pair. The
    // fail-closed default below is what still holds if either ever breaks, and this is the only
    // place it can be reached directly.
    #[test]
    fn only_the_ratified_partial_serve_reason_keeps_serving_a_name() {
        assert!(!downgrades_unsupported_name(
            PARTIAL_SERVE_UNSUPPORTED_REASON
        ));
        for reason in [
            "conflicting_current_ens_authority",
            "independent_ens_deployments_overlap",
            "ensv2_exact_name_profile_shadow",
            "a_reason_this_build_has_never_seen",
            MISSING_UNSUPPORTED_REASON,
            "",
        ] {
            assert!(downgrades_unsupported_name(reason), "{reason} served ok");
        }
    }
}
