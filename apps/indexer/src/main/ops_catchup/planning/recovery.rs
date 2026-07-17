use std::collections::BTreeSet;

use bigname_manifests::ManifestBootstrapTarget;

use super::CatchupTarget;

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
