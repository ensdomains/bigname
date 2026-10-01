//! The per-chain verified capabilities of the namespace summary.
use std::collections::{BTreeMap, BTreeSet};

use bigname_domain::vocabulary::{ChainId, Namespace as NamespaceId, SourceFamily};
use bigname_lookup::{ChainRpcUrls, verified_execution_entrypoint};
use bigname_manifests::{ActiveManifestVersion, CapabilitySupportStatus, ExecutionManifestVersion};

use super::{
    Completeness, NamespaceCapability, NamespaceChainCapability, UNSUPPORTED_REASON,
    slug_to_numeric,
};

const NOT_SUPPORTED_FOR_CHAIN: &str = "not_supported_for_chain";
const EXECUTION_ENTRYPOINT_NOT_DECLARED: &str = "execution_entrypoint_not_declared";
const EXECUTION_PROVIDER_NOT_CONFIGURED: &str = "execution_provider_not_configured";
const VERIFIED_RECORDS_CAPABILITY: &str = "verified_records";
const VERIFIED_PRIMARY_NAME_CAPABILITY: &str = "verified_primary_name";
const VERIFIED_RESOLUTION_FLAG: &str = "verified_resolution";
const ACTIVE_ROLLOUT_STATUS: &str = "active";
const SHADOW_ROLLOUT_STATUS: &str = "shadow";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerifiedCapability {
    Records,
    PrimaryName,
}

impl VerifiedCapability {
    const fn name(self) -> &'static str {
        match self {
            Self::Records => VERIFIED_RECORDS_CAPABILITY,
            Self::PrimaryName => VERIFIED_PRIMARY_NAME_CAPABILITY,
        }
    }
}

/// `verified_records` and `verified_primary_name`, decided per declared network from what the
/// lookup engine will actually execute: the route table's entrypoint for the namespace and
/// chain, a manifest declaring that entrypoint with a supported `verified_resolution` flag
/// (manifests with `rollout_status = shadow` count where the route admits them, as ENS does),
/// and a configured provider for the execution chain. Per-name support classes still apply on
/// the routes themselves; this is deployment-level support.
pub(super) fn verified_capabilities(
    namespace: &str,
    manifests: &[ActiveManifestVersion],
    execution_manifests: &[ExecutionManifestVersion],
    rpc_urls: &ChainRpcUrls,
) -> BTreeMap<String, NamespaceCapability> {
    [VerifiedCapability::Records, VerifiedCapability::PrimaryName]
        .into_iter()
        .map(|kind| {
            (
                kind.name().to_owned(),
                verified_capability(kind, namespace, manifests, execution_manifests, rpc_urls),
            )
        })
        .collect()
}

fn verified_capability(
    kind: VerifiedCapability,
    namespace: &str,
    manifests: &[ActiveManifestVersion],
    execution_manifests: &[ExecutionManifestVersion],
    rpc_urls: &ChainRpcUrls,
) -> NamespaceCapability {
    let chains = manifests
        .iter()
        .map(|manifest| manifest.chain.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|chain| {
            (
                chain_capability_key(chain),
                verified_chain_capability(
                    kind,
                    namespace,
                    chain,
                    manifests,
                    execution_manifests,
                    rpc_urls,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let supported_count = chains
        .values()
        .filter(|chain| chain.completeness == Completeness::Full)
        .count();
    let completeness = if supported_count == 0 {
        Completeness::Unsupported
    } else if supported_count == chains.len() {
        Completeness::Full
    } else {
        Completeness::Partial
    };
    let unsupported_reason = (completeness == Completeness::Unsupported).then(|| {
        let reasons = chains
            .values()
            .filter_map(|chain| chain.unsupported_reason.as_deref())
            .collect::<BTreeSet<_>>();
        match reasons.iter().next() {
            Some(reason) if reasons.len() == 1 => (*reason).to_owned(),
            Some(_) => NOT_SUPPORTED_FOR_CHAIN.to_owned(),
            None => UNSUPPORTED_REASON.to_owned(),
        }
    });
    NamespaceCapability {
        completeness,
        unsupported_reason,
        chains,
    }
}

fn chain_capability_key(chain: &str) -> String {
    slug_to_numeric(chain).map_or_else(|| chain.to_owned(), |numeric| numeric.to_string())
}

fn verified_chain_capability(
    kind: VerifiedCapability,
    namespace: &str,
    chain: &str,
    manifests: &[ActiveManifestVersion],
    execution_manifests: &[ExecutionManifestVersion],
    rpc_urls: &ChainRpcUrls,
) -> NamespaceChainCapability {
    let Ok(namespace_id) = namespace.parse::<NamespaceId>() else {
        return NamespaceChainCapability::unsupported(UNSUPPORTED_REASON);
    };
    if kind == VerifiedCapability::PrimaryName && namespace_id != NamespaceId::Ens {
        return NamespaceChainCapability::unsupported(UNSUPPORTED_REASON);
    }
    let Some(entrypoint) = chain
        .parse::<ChainId>()
        .ok()
        .and_then(|chain_id| verified_execution_entrypoint(namespace_id, chain_id))
    else {
        return NamespaceChainCapability::unsupported(NOT_SUPPORTED_FOR_CHAIN);
    };
    let execution_declared = execution_manifests.iter().any(|manifest| {
        manifest.source_family == entrypoint.source_family.as_str()
            && manifest.chain == entrypoint.chain_id.as_str()
            && (manifest.rollout_status == ACTIVE_ROLLOUT_STATUS
                || (entrypoint.allow_shadow && manifest.rollout_status == SHADOW_ROLLOUT_STATUS))
            && entrypoint
                .required_manifest_version
                .is_none_or(|version| i64::try_from(manifest.manifest_version) == Ok(version))
            && manifest
                .capability_flags
                .get(VERIFIED_RESOLUTION_FLAG)
                .is_some_and(|flag| flag.status == CapabilitySupportStatus::Supported)
    });
    if !execution_declared {
        return NamespaceChainCapability::unsupported(EXECUTION_ENTRYPOINT_NOT_DECLARED);
    }
    // Primary-name lookup also reads the ENSv1 registry on the same chain for the reverse leg.
    if kind == VerifiedCapability::PrimaryName
        && !manifests.iter().any(|manifest| {
            manifest.source_family == SourceFamily::EnsV1RegistryL1.as_str()
                && manifest.chain == chain
        })
    {
        return NamespaceChainCapability::unsupported(EXECUTION_ENTRYPOINT_NOT_DECLARED);
    }
    if rpc_urls.url_for(entrypoint.chain_id.as_str()).is_none() {
        return NamespaceChainCapability::unsupported(EXECUTION_PROVIDER_NOT_CONFIGURED);
    }
    NamespaceChainCapability::full()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bigname_manifests::CapabilityFlag;

    use super::super::tests::manifest;
    use super::*;

    fn rpc_urls(entries: &[&str]) -> ChainRpcUrls {
        ChainRpcUrls::from_entries(
            &entries
                .iter()
                .map(|entry| format!("{entry}=http://rpc.test"))
                .collect::<Vec<_>>(),
        )
        .expect("test RPC map must be valid")
    }

    fn chain_entry(reason: Option<&str>) -> NamespaceChainCapability {
        reason.map_or(NamespaceChainCapability::full(), |reason| {
            NamespaceChainCapability::unsupported(reason)
        })
    }

    fn execution_manifest(
        source_family: &str,
        chain: &str,
        manifest_version: u64,
        rollout_status: &str,
        status: CapabilitySupportStatus,
    ) -> ExecutionManifestVersion {
        ExecutionManifestVersion {
            manifest_version,
            source_family: source_family.to_owned(),
            chain: chain.to_owned(),
            rollout_status: rollout_status.to_owned(),
            capability_flags: BTreeMap::from([(
                "verified_resolution".to_owned(),
                CapabilityFlag {
                    status,
                    notes: None,
                },
            )]),
        }
    }

    #[test]
    fn sepolia_ens_verified_capabilities_follow_manifests_and_provider_configuration() {
        let active = vec![
            manifest(
                "ens_v1_registry_l1",
                "ethereum-sepolia",
                [("declared_children", CapabilitySupportStatus::Supported)],
            ),
            manifest(
                "ens_v2_registry_l1",
                "ethereum-sepolia",
                [("declared_children", CapabilitySupportStatus::Supported)],
            ),
        ];
        let execution = vec![execution_manifest(
            "ens_execution",
            "ethereum-sepolia",
            1,
            "shadow",
            CapabilitySupportStatus::Supported,
        )];

        let configured = verified_capabilities(
            "ens",
            &active,
            &execution,
            &rpc_urls(&["ethereum-sepolia", "ethereum-mainnet"]),
        );
        for capability in ["verified_records", "verified_primary_name"] {
            assert_eq!(
                configured[capability],
                NamespaceCapability {
                    completeness: Completeness::Full,
                    unsupported_reason: None,
                    chains: BTreeMap::from([("11155111".to_owned(), chain_entry(None))]),
                },
                "{capability}"
            );
        }

        let unconfigured =
            verified_capabilities("ens", &active, &execution, &rpc_urls(&["ethereum-mainnet"]));
        for capability in ["verified_records", "verified_primary_name"] {
            assert_eq!(
                unconfigured[capability],
                NamespaceCapability {
                    completeness: Completeness::Unsupported,
                    unsupported_reason: Some(EXECUTION_PROVIDER_NOT_CONFIGURED.to_owned()),
                    chains: BTreeMap::from([(
                        "11155111".to_owned(),
                        chain_entry(Some(EXECUTION_PROVIDER_NOT_CONFIGURED)),
                    )]),
                },
                "{capability}"
            );
        }

        let without_execution =
            verified_capabilities("ens", &active, &[], &rpc_urls(&["ethereum-sepolia"]));
        assert_eq!(
            without_execution["verified_records"]
                .unsupported_reason
                .as_deref(),
            Some(EXECUTION_ENTRYPOINT_NOT_DECLARED)
        );
        let unsupported_flag = vec![execution_manifest(
            "ens_execution",
            "ethereum-sepolia",
            1,
            "active",
            CapabilitySupportStatus::Unsupported,
        )];
        let unsupported_flag = verified_capabilities(
            "ens",
            &active,
            &unsupported_flag,
            &rpc_urls(&["ethereum-sepolia"]),
        );
        assert_eq!(
            unsupported_flag["verified_records"]
                .unsupported_reason
                .as_deref(),
            Some(EXECUTION_ENTRYPOINT_NOT_DECLARED)
        );
        let without_registry = verified_capabilities(
            "ens",
            &active[1..],
            &execution,
            &rpc_urls(&["ethereum-sepolia"]),
        );
        assert_eq!(
            without_registry["verified_records"].completeness,
            Completeness::Full
        );
        assert_eq!(
            without_registry["verified_primary_name"]
                .unsupported_reason
                .as_deref(),
            Some(EXECUTION_ENTRYPOINT_NOT_DECLARED)
        );
    }

    #[test]
    fn verified_capabilities_report_chains_outside_the_execution_table() {
        let active = vec![
            manifest("basenames_l1_compat", "ethereum-mainnet", []),
            manifest("basenames_base_registry", "base-mainnet", []),
        ];
        let execution = vec![execution_manifest(
            "basenames_execution",
            "ethereum-mainnet",
            2,
            "active",
            CapabilitySupportStatus::Supported,
        )];
        let capabilities = verified_capabilities(
            "basenames",
            &active,
            &execution,
            &rpc_urls(&["ethereum-mainnet"]),
        );
        assert_eq!(
            capabilities["verified_records"],
            NamespaceCapability {
                completeness: Completeness::Partial,
                unsupported_reason: None,
                chains: BTreeMap::from([
                    ("1".to_owned(), chain_entry(Some(NOT_SUPPORTED_FOR_CHAIN))),
                    ("8453".to_owned(), chain_entry(None)),
                ]),
            }
        );
        assert_eq!(
            capabilities["verified_primary_name"],
            NamespaceCapability {
                completeness: Completeness::Unsupported,
                unsupported_reason: Some(UNSUPPORTED_REASON.to_owned()),
                chains: BTreeMap::from([
                    ("1".to_owned(), chain_entry(Some(UNSUPPORTED_REASON))),
                    ("8453".to_owned(), chain_entry(Some(UNSUPPORTED_REASON))),
                ]),
            }
        );

        // Basenames execution is pinned to manifest version 2 and never admits shadow.
        for (version, rollout_status) in [(1, "active"), (2, "shadow")] {
            let pinned = vec![execution_manifest(
                "basenames_execution",
                "ethereum-mainnet",
                version,
                rollout_status,
                CapabilitySupportStatus::Supported,
            )];
            let pinned = verified_capabilities(
                "basenames",
                &active,
                &pinned,
                &rpc_urls(&["ethereum-mainnet"]),
            );
            assert_eq!(
                pinned["verified_records"].chains["8453"],
                chain_entry(Some(EXECUTION_ENTRYPOINT_NOT_DECLARED)),
                "version {version} {rollout_status}"
            );
        }

        let none = verified_capabilities("ens", &[], &[], &rpc_urls(&[]));
        assert_eq!(
            none["verified_records"],
            NamespaceCapability {
                completeness: Completeness::Unsupported,
                unsupported_reason: Some(UNSUPPORTED_REASON.to_owned()),
                chains: BTreeMap::new(),
            }
        );
    }
}
