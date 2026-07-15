use std::collections::BTreeSet;

use anyhow::{Result, bail};
use bigname_manifests::{
    ManifestBootstrapTarget, WatchedBackfillTarget, WatchedSourceSelectorKind,
    WatchedSourceSelectorPlan,
};

use super::{CatchupChunk, CatchupTarget};

impl CatchupChunk {
    pub(crate) fn narrow_historical_source_plan(
        &self,
        source_plan: &mut WatchedSourceSelectorPlan,
    ) -> Result<()> {
        if source_plan.selector_kind != WatchedSourceSelectorKind::WatchedTargetSet {
            bail!(
                "ops catch-up historical source plan used selector kind {} instead of watched_target_set",
                source_plan.selector_kind.as_str()
            );
        }
        let expected_targets = self
            .covered_targets()
            .into_iter()
            .map(|target| WatchedBackfillTarget {
                source_family: target.source_family.clone(),
                contract_instance_id: target.contract_instance_id,
                address: target.address.clone(),
                effective_from_block: self.range.from_block,
                effective_to_block: self.range.to_block,
            })
            .collect::<BTreeSet<_>>();
        for expected in &expected_targets {
            if !source_plan.selected_targets.contains(expected) {
                bail!(
                    "ops catch-up historical source plan did not select authoritative target {}/{} at {} over {}..={}",
                    expected.source_family,
                    expected.contract_instance_id,
                    expected.address,
                    expected.effective_from_block,
                    expected.effective_to_block
                );
            }
        }
        source_plan.selected_targets = expected_targets.into_iter().collect();
        Ok(())
    }
}

pub(crate) fn merge_retained_history_recovery_targets(
    targets: &mut Vec<CatchupTarget>,
    recovery_targets: &[ManifestBootstrapTarget],
) {
    let mut merged = targets.iter().cloned().collect::<BTreeSet<_>>();
    merged.extend(recovery_targets.iter().map(|target| CatchupTarget {
        source_family: target.source_family.clone(),
        contract_instance_id: target.contract_instance_id,
        address: target.address.clone(),
        from_block: target.effective_from_block,
        to_block: target.effective_to_block,
    }));
    *targets = merged.into_iter().collect();
}
