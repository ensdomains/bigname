use anyhow::Result;

use super::super::replay::{
    NormalizedEventReplayAdapter, RawFactReplayContractPlan, source_scope_includes_adapter,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistedRawPayloadAdapterSyncMode {
    LivePoll,
    LiveOrBackfill,
    RawFactReplay {
        canonical_raw_log_count: usize,
        replay_contract_plan: RawFactReplayContractPlan,
    },
}

impl PersistedRawPayloadAdapterSyncMode {
    pub(super) fn uses_stateless_replay_authority(self) -> bool {
        matches!(
            self,
            Self::RawFactReplay {
                replay_contract_plan,
                ..
            } if replay_contract_plan.uses_stateless_replay_authority()
        )
    }

    pub(super) fn selects_adapter(
        self,
        source_scope: Option<&[(String, String, i64, i64)]>,
        adapter: NormalizedEventReplayAdapter,
    ) -> bool {
        source_scope.is_none_or(|scope| source_scope_includes_adapter(scope, adapter))
            && match self {
                Self::RawFactReplay {
                    replay_contract_plan,
                    ..
                } => replay_contract_plan.uses_restricted_sync_for(adapter),
                Self::LivePoll | Self::LiveOrBackfill => true,
            }
    }
}

pub(super) fn ensure_raw_fact_adapter_allowed(
    mode: PersistedRawPayloadAdapterSyncMode,
    adapter: NormalizedEventReplayAdapter,
) -> Result<()> {
    if let PersistedRawPayloadAdapterSyncMode::RawFactReplay {
        replay_contract_plan,
        ..
    } = mode
    {
        replay_contract_plan.ensure_adapter_allowed(adapter)?;
    }
    Ok(())
}
