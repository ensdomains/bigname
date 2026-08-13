use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{PhaseContext, PhaseProgress},
};

use super::{VerificationPlan, VerifyPhase, verification_plan};

pub(crate) fn provider_trusted_verify_required(
    chain_id: &str,
    sources: &[SourceConfig],
) -> RunnerResult<bool> {
    if !provider_trusted_verify_chain(chain_id) {
        return Ok(false);
    }
    is_required(chain_id, sources)
}

pub(crate) fn provider_trusted_verify_chain(chain_id: &str) -> bool {
    chain_id == "ethereum-sepolia"
}

pub(super) fn is_required(chain_id: &str, sources: &[SourceConfig]) -> RunnerResult<bool> {
    Ok(matches!(
        verification_plan(chain_id, sources)?,
        VerificationPlan::ProviderTrusted { .. }
    ))
}

pub(super) fn require_frozen_target(
    chain_id: &str,
    current: &BlockMarker,
    target: &BlockMarker,
) -> RunnerResult<()> {
    if current != target {
        return Err(RunnerError::data_integrity(format!(
            "verification final marker for chain {chain_id} differs from its frozen target"
        )));
    }
    Ok(())
}

pub(super) async fn revalidate(
    phase: &VerifyPhase,
    context: PhaseContext,
) -> RunnerResult<Option<PhaseProgress>> {
    let VerificationPlan::ProviderTrusted { level, source } =
        verification_plan(&context.chain_id, &context.sources)?
    else {
        return Ok(None);
    };
    let current = context.resume.current.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "completed provider-trusted verification for chain {} has no recorded current block",
            context.chain_id
        ))
    })?;
    let target = context.resume.target.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "completed provider-trusted verification for chain {} has no recorded target block",
            context.chain_id
        ))
    })?;
    let finalized_target = phase
        .store
        .finalized_marker(&context.chain_id, target.number)
        .await?;
    if finalized_target != target {
        return Err(RunnerError::data_integrity(format!(
            "completed provider-trusted verification target for chain {} does not match finalized \
             lineage at block {}",
            context.chain_id, target.number
        )));
    }
    require_frozen_target(&context.chain_id, &current, &target)?;
    phase
        .store
        .require_provider_trusted_extent(&context.chain_id, &source, &target)
        .await?;
    Ok(Some(PhaseProgress {
        current: Some(current),
        target: Some(target),
        verification_level: Some(level),
        ..PhaseProgress::default()
    }))
}

#[cfg(test)]
mod tests {
    use crate::{error::ErrorKind, heads::BlockMarker};

    #[test]
    fn frozen_target_error_applies_to_every_verification_plan() {
        let current = BlockMarker {
            number: 7,
            hash: "current".to_owned(),
        };
        let target = BlockMarker {
            number: 7,
            hash: "target".to_owned(),
        };
        let error = super::require_frozen_target("test-chain", &current, &target)
            .expect_err("different marker identities must fail closed");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity);
        assert!(error.to_string().contains("verification final marker"));
        assert!(!error.to_string().contains("provider-trusted"));
    }
}
