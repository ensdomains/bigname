mod basenames;
mod ens;
mod selectors;
mod terminal;

use anyhow::{Result, bail};

use crate::persistence::PersistEnsExactNameVerifiedResolutionRequest;

use super::VerifiedQuerySummary;

pub(crate) use selectors::{extract_requested_selectors, extract_supported_verified_queries};

pub(crate) fn validate_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<Vec<VerifiedQuerySummary>> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    selectors::ensure_requested_selectors_match_queries(&requested_selectors, &queries)?;
    ens::validate_trace(
        &request.trace,
        &request.outcome,
        &requested_selectors,
        &queries,
    )?;
    ens::validate_outcome(&request.outcome, &request.trace, &queries)?;
    ens::validate_raw_call_snapshots(
        &request.raw_call_snapshots,
        &request.outcome,
        &requested_selectors,
    )?;
    Ok(queries)
}

pub(crate) fn validate_basenames_transport_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<Vec<VerifiedQuerySummary>> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    selectors::ensure_requested_selectors_match_queries(&requested_selectors, &queries)?;
    basenames::validate_basenames_transport_direct_trace(
        &request.trace,
        &request.outcome,
        &requested_selectors,
        &queries,
    )?;
    basenames::validate_basenames_transport_direct_outcome(
        &request.outcome,
        &request.trace,
        &requested_selectors,
        &queries,
    )?;
    if !request.raw_call_snapshots.is_empty() {
        bail!(
            "Basenames transport-assisted direct persistence does not admit raw_call_snapshots yet"
        );
    }
    Ok(queries)
}
