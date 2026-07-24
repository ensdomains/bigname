use anyhow::{Context, Result, ensure};

const DEFAULT_OBSERVATION_PAGE_LIMIT: i64 = 10_000;
const DEFAULT_MUTATION_BATCH_SIZE: usize = 50_000;
const DEFAULT_DEACTIVATION_PAGE_SIZE: usize = 50_000;

pub const DISCOVERY_FULL_RECONCILE_MUTATION_BATCH_SIZE_ENV: &str =
    "BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MUTATION_BATCH_SIZE";
pub const DISCOVERY_FULL_RECONCILE_DEACTIVATION_PAGE_SIZE_ENV: &str =
    "BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_DEACTIVATION_PAGE_SIZE";

#[derive(Clone, Copy, Debug)]
pub(crate) struct StreamedDiscoveryReconciliationOptions {
    /// Replaces the default `max(10_000, 1% of active edges)` precise
    /// deactivation guard bound when set, and raises the coarse candidate
    /// load cap to at least the same value so an operator override stays
    /// effective end to end.
    pub(crate) max_deactivations_override: Option<usize>,
    /// Replaces the default `max(100_000, 10% of active edges)` coarse cap
    /// on how many deactivation candidates may be materialized in memory.
    /// Test hook; production overrides go through the env-driven precise
    /// bound, which raises this cap alongside it.
    pub(crate) coarse_deactivation_cap_override: Option<usize>,
    pub(crate) observation_page_limit: i64,
    pub(crate) mutation_batch_size: usize,
    pub(crate) deactivation_page_size: usize,
    /// Inject a failure if a test scans more than this many stored-edge
    /// pages while computing deactivation candidates.
    #[cfg(test)]
    pub(crate) fail_after_deactivation_source_pages: Option<usize>,
}

impl Default for StreamedDiscoveryReconciliationOptions {
    fn default() -> Self {
        Self {
            max_deactivations_override: None,
            coarse_deactivation_cap_override: None,
            observation_page_limit: DEFAULT_OBSERVATION_PAGE_LIMIT,
            mutation_batch_size: DEFAULT_MUTATION_BATCH_SIZE,
            deactivation_page_size: DEFAULT_DEACTIVATION_PAGE_SIZE,
            #[cfg(test)]
            fail_after_deactivation_source_pages: None,
        }
    }
}

impl StreamedDiscoveryReconciliationOptions {
    pub(super) fn from_env() -> Result<Self> {
        Ok(Self {
            max_deactivations_override: super::guard::max_deactivations_override_from_env()?,
            mutation_batch_size: positive_usize_from_env(
                DISCOVERY_FULL_RECONCILE_MUTATION_BATCH_SIZE_ENV,
                DEFAULT_MUTATION_BATCH_SIZE,
            )?,
            deactivation_page_size: positive_usize_from_env(
                DISCOVERY_FULL_RECONCILE_DEACTIVATION_PAGE_SIZE_ENV,
                DEFAULT_DEACTIVATION_PAGE_SIZE,
            )?,
            ..Self::default()
        })
    }
}

fn positive_usize_from_env(name: &str, default: usize) -> Result<usize> {
    let value = match std::env::var(name) {
        Ok(value) => parse_positive_usize(name, &value)?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error).with_context(|| format!("failed to read {name}")),
    };
    Ok(value)
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize> {
    let parsed = value
        .trim()
        .parse::<usize>()
        .with_context(|| format!("failed to parse {name} as a positive row count: {value:?}"))?;
    ensure!(parsed > 0, "{name} must be greater than zero");
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_reconcile_batch_defaults_are_amortized() {
        let options = StreamedDiscoveryReconciliationOptions::default();
        assert_eq!(options.mutation_batch_size, 50_000);
        assert_eq!(options.deactivation_page_size, 50_000);
    }

    #[test]
    fn streamed_reconcile_batch_overrides_require_positive_counts() {
        assert_eq!(
            parse_positive_usize("TEST_BATCH_SIZE", " 123 ").expect("count must parse"),
            123
        );
        assert!(parse_positive_usize("TEST_BATCH_SIZE", "0").is_err());
        assert!(parse_positive_usize("TEST_BATCH_SIZE", "not-a-count").is_err());
    }
}
