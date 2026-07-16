use std::{error::Error, fmt};

/// ENSv2 reconciliation requires an exact authoritative watched interval
/// whose current raw-log retention generation has not been fetched yet.
///
/// Automatic startup, normalized replay catch-up, or live polling may
/// downcast this error to run bounded, provider-backed recovery convergence.
/// Other sync failures must continue to propagate unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2MissingCoverage {
    pub chain: String,
    pub retention_generation: i64,
    pub source_family: String,
    pub address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
}

impl fmt::Display for EnsV2MissingCoverage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ENSv2 full reconciliation on {} requires missing generation {} coverage for {} {} over {}..={}",
            self.chain,
            self.retention_generation,
            self.source_family,
            self.address,
            self.required_from_block,
            self.required_to_block
        )
    }
}

impl Error for EnsV2MissingCoverage {}

pub fn ens_v2_missing_coverage(error: &anyhow::Error) -> Option<&EnsV2MissingCoverage> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<EnsV2MissingCoverage>())
}

pub fn is_ens_v2_missing_coverage(error: &anyhow::Error) -> bool {
    ens_v2_missing_coverage(error).is_some()
}
