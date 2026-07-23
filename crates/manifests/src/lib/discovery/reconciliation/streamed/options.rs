const DEFAULT_OBSERVATION_PAGE_LIMIT: i64 = 10_000;
const DEFAULT_MUTATION_BATCH_SIZE: usize = 1_000;

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
            #[cfg(test)]
            fail_after_deactivation_source_pages: None,
        }
    }
}
