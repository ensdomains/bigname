use bigname_ingest::{VerificationBatch, VerificationProviderKind};
use tracing::{info, warn};

use super::{VerificationReferenceProvider, VerificationSource};
use crate::{
    error::{RunnerError, RunnerResult},
    verify_compare,
    verify_store::StoredVerificationBatch,
};

pub(super) async fn fetch_matching(
    provider: &dyn VerificationReferenceProvider,
    source: &VerificationSource,
    stored: &StoredVerificationBatch,
    from: i64,
    to: i64,
) -> RunnerResult<VerificationBatch> {
    let mut requests = 0usize;
    for attempt in 0..2 {
        let mut reference = provider
            .fetch(source, stored.filter.clone(), from, to)
            .await?;
        requests = requests.saturating_add(reference.rpc_request_count);
        let Some(mismatch) = verify_compare::compare(stored, &reference) else {
            if attempt > 0 {
                info!(
                    chain_id = source.chain_id(),
                    source_key = source.source_key(),
                    from_block = from,
                    to_block = to,
                    "verification reference matched on its second fetch"
                );
            }
            reference.rpc_request_count = requests;
            return Ok(reference);
        };
        let context = format!(
            "chain {} source {} range {from}..={to}: {mismatch}",
            source.chain_id(),
            source.source_key(),
        );
        if attempt > 0 || source.provider_kind() != VerificationProviderKind::IndependentRpc {
            return Err(RunnerError::verification_mismatch(context));
        }
        warn!(
            chain_id = source.chain_id(),
            source_key = source.source_key(),
            from_block = from,
            to_block = to,
            mismatch = %context,
            "verification reference mismatch; fetching the same batch once more"
        );
    }
    unreachable!("the second comparison always returns a result")
}

#[cfg(test)]
#[path = "verify_reference_retry_tests.rs"]
mod tests;
