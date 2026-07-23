use anyhow::{Context, Result};

pub const DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV: &str =
    "BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS";

const DEACTIVATION_GUARD_FLOOR: usize = 10_000;
const CANDIDATE_LOAD_CAP_FLOOR: usize = 100_000;

pub(super) fn default_max_deactivations(active_edge_count: usize) -> usize {
    DEACTIVATION_GUARD_FLOOR.max(active_edge_count / 100)
}

pub(super) fn default_max_deactivation_candidates(active_edge_count: usize) -> usize {
    CANDIDATE_LOAD_CAP_FLOOR.max(active_edge_count / 10)
}

pub(super) fn max_deactivations_override_from_env() -> Result<Option<usize>> {
    match std::env::var(DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV) {
        Ok(value) => parse_max_deactivations_override(Some(&value)),
        Err(std::env::VarError::NotPresent) => parse_max_deactivations_override(None),
        Err(error) => Err(error).context(format!(
            "failed to read {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV}"
        )),
    }
}

fn parse_max_deactivations_override(value: Option<&str>) -> Result<Option<usize>> {
    match value {
        None => Ok(None),
        Some(value) => value
            .trim()
            .parse::<usize>()
            .map(Some)
            .with_context(|| {
                format!(
                    "failed to parse {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV} as a deactivation count: {value:?}"
                )
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_deactivation_guard_uses_floor_and_percentage() {
        assert_eq!(default_max_deactivations(0), 10_000);
        assert_eq!(default_max_deactivations(500_000), 10_000);
        assert_eq!(default_max_deactivations(7_620_084), 76_200);
    }

    #[test]
    fn default_candidate_load_cap_uses_floor_and_percentage() {
        assert_eq!(default_max_deactivation_candidates(0), 100_000);
        assert_eq!(default_max_deactivation_candidates(500_000), 100_000);
        assert_eq!(default_max_deactivation_candidates(7_620_084), 762_008);
    }

    #[test]
    fn deactivation_guard_override_parses_or_rejects() {
        assert_eq!(
            parse_max_deactivations_override(None).expect("no value must read as no override"),
            None
        );
        assert_eq!(
            parse_max_deactivations_override(Some("123456")).expect("numeric override must parse"),
            Some(123_456)
        );
        assert_eq!(
            parse_max_deactivations_override(Some(" 42 "))
                .expect("surrounding whitespace is trimmed"),
            Some(42)
        );
        assert!(parse_max_deactivations_override(Some("not-a-count")).is_err());
    }
}
