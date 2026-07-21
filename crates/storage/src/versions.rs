/// Bump whenever the consumed input set for a current-state read model changes semantically.
///
/// Version 5 covers RootPermissionChanged and registry-scope permission consumption added in PR
/// #24. Version 6 covers an expiry-output correction in the name projection. Version 7 covers an
/// exact-name evidence input expansion. Version 8 covers
/// `permissions_current_resource_summary` backfill and atomic publication with
/// `permissions_current`, including resources with zero permission rows, and the `name_current`
/// control-output correction, while retaining version 7 exact-name evidence. Version 9 forces the
/// full permission cutover that seeds publication compatibility and its monotonic read-consistency
/// revision, including zero-event resources discovered from canonical resource identity evidence.
/// Its all-current replay preserves the version 8 `name_current` output; exact-name reads have no
/// replay-version compatibility gate, so deployments must keep them drained until all version 9
/// markers are current and pending invalidations drain.
pub const CURRENT_PROJECTION_REPLAY_VERSION: i32 = 9;

/// Latest checked-in migration version expected by this binary.
pub fn latest_migration_version() -> i64 {
    super::MIGRATOR
        .migrations
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_version_matches_the_checked_in_migrator() {
        assert_eq!(
            latest_migration_version(),
            crate::MIGRATOR
                .migrations
                .last()
                .expect("bigname must keep at least one database migration")
                .version
        );
    }
}
