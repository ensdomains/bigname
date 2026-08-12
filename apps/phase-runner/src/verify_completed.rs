use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    phase::{PhaseContext, PhaseProgress},
};

use super::{VerificationPlan, VerifyPhase, verification_plan};

pub(super) fn is_required(chain_id: &str, sources: &[SourceConfig]) -> RunnerResult<bool> {
    Ok(matches!(
        verification_plan(chain_id, sources)?,
        VerificationPlan::ProviderTrusted { .. }
    ))
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
    if current != target {
        return Err(RunnerError::data_integrity(format!(
            "completed provider-trusted verification for chain {} has different current and \
             target markers",
            context.chain_id
        )));
    }
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
