use std::{collections::BTreeSet, path::Path};

use anyhow::{Result, bail};

use crate::SourceManifest;

pub(super) fn validate(manifest: &SourceManifest, path: &Path) -> Result<()> {
    let declared_roles = manifest
        .roots
        .iter()
        .map(|root| root.name.as_str())
        .chain(
            manifest
                .contracts
                .iter()
                .map(|contract| contract.role.as_str()),
        )
        .collect::<BTreeSet<_>>();

    for (index, rule) in manifest.discovery_rules.iter().enumerate() {
        if !declared_roles.contains(rule.from_role.as_str()) {
            bail!(
                "manifest discovery_rules[{index}] edge_kind={} from_role={} in {} references an unknown [[roots]].name / [[contracts]].role",
                rule.edge_kind,
                rule.from_role,
                path.display(),
            );
        }

        if rule.edge_kind != "registry_announcement" {
            continue;
        }
        if manifest.source_family != crate::ENS_V2_REGISTRY_SOURCE_FAMILY {
            bail!(
                "manifest discovery_rules[{index}] edge_kind={} from_role={} in {} has source_family={}; registry announcements require source_family={}",
                rule.edge_kind,
                rule.from_role,
                path.display(),
                manifest.source_family,
                crate::ENS_V2_REGISTRY_SOURCE_FAMILY,
            );
        }

        let registry_created_topic0 = crate::registry_announcement_topic0();
        let mut has_registry_created = false;
        for event in &manifest.abi.events {
            if event.topic0()?.as_deref() == Some(registry_created_topic0.as_str()) {
                has_registry_created = true;
                break;
            }
        }
        if !has_registry_created {
            bail!(
                "manifest discovery_rules[{index}] edge_kind={} from_role={} in {} requires ABI event RegistryCreated()",
                rule.edge_kind,
                rule.from_role,
                path.display(),
            );
        }
    }

    Ok(())
}
