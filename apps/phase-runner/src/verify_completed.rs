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
    let target_number = context
        .available_heads
        .as_ref()
        .and_then(|heads| heads.finalized.as_ref())
        .ok_or_else(|| {
            RunnerError::transient(format!(
                "chain {} has no finalized head available for verification",
                context.chain_id
            ))
        })?
        .number;
    let target = phase
        .store
        .finalized_marker(&context.chain_id, target_number)
        .await?;
    phase
        .store
        .require_provider_trusted_extent(&context.chain_id, &source, &target)
        .await?;
    Ok(Some(PhaseProgress {
        current: Some(target.clone()),
        target: Some(target),
        verification_level: Some(level),
        ..PhaseProgress::default()
    }))
}
