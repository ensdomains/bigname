use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_domain::vocabulary::{ChainId, Namespace as NamespaceId, SourceFamily};
use bigname_lookup::{ChainRpcUrls, verified_execution_entrypoint};
use bigname_manifests::{
    ActiveManifestVersion, CapabilitySupportStatus, ExecutionManifestVersion,
    NamespaceManifestSnapshot, load_execution_manifests_for_namespace,
    load_namespace_manifest_snapshot,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use super::support::ensure_public_namespace;
use crate::AppState;

use super::{
    Completeness, Envelope, Meta, NoQueryParams, V2Error, V2Result, api_error_to_v2,
    numeric_to_slug, slug_to_numeric,
};

const UNSUPPORTED_REASON: &str = "not_supported_for_namespace";
const NOT_SUPPORTED_FOR_CHAIN: &str = "not_supported_for_chain";
const EXECUTION_ENTRYPOINT_NOT_DECLARED: &str = "execution_entrypoint_not_declared";
const EXECUTION_PROVIDER_NOT_CONFIGURED: &str = "execution_provider_not_configured";
const VERIFIED_RECORDS_CAPABILITY: &str = "verified_records";
const VERIFIED_PRIMARY_NAME_CAPABILITY: &str = "verified_primary_name";
const VERIFIED_RESOLUTION_FLAG: &str = "verified_resolution";
const ACTIVE_ROLLOUT_STATUS: &str = "active";
const SHADOW_ROLLOUT_STATUS: &str = "shadow";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Namespace {
    pub(crate) namespace: String,
    pub(crate) capabilities: BTreeMap<String, NamespaceCapability>,
    pub(crate) networks: Vec<NamespaceNetwork>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceCapability {
    pub(crate) completeness: Completeness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    /// Per-chain support for capabilities this deployment decides chain by chain (the verified
    /// ones), keyed by numeric chain id. Absent for capabilities aggregated from manifest flags.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) chains: BTreeMap<String, NamespaceChainCapability>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceChainCapability {
    pub(crate) completeness: Completeness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
}

impl NamespaceChainCapability {
    const fn full() -> Self {
        Self {
            completeness: Completeness::Full,
            unsupported_reason: None,
        }
    }

    fn unsupported(reason: &str) -> Self {
        Self {
            completeness: Completeness::Unsupported,
            unsupported_reason: Some(reason.to_owned()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceNetwork {
    pub(crate) network: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chain_id: Option<u64>,
}

pub(crate) async fn get_namespace(
    Path(namespace): Path<String>,
    _no_query: NoQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Namespace>>> {
    ensure_public_namespace(&namespace).map_err(api_error_to_v2)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load v2 namespace metadata"
            );
            V2Error::internal_error(format!(
                "failed to load namespace metadata for namespace {namespace}"
            ))
        })?;

    let execution_manifests = load_execution_manifests_for_namespace(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load v2 namespace execution manifests"
            );
            V2Error::internal_error(format!(
                "failed to load namespace metadata for namespace {namespace}"
            ))
        })?;

    Ok(Json(Envelope {
        data: build_namespace(
            namespace,
            snapshot,
            &execution_manifests,
            &state.lookup_chain_rpc_urls,
        )?,
        page: None,
        meta: Meta::default(),
    }))
}

fn build_namespace(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
    execution_manifests: &[ExecutionManifestVersion],
    rpc_urls: &ChainRpcUrls,
) -> V2Result<Namespace> {
    let mut capabilities = aggregate_capabilities(&snapshot.manifests)?;
    capabilities.extend(verified_capabilities(
        &namespace,
        &snapshot.manifests,
        execution_manifests,
        rpc_urls,
    ));
    Ok(Namespace {
        namespace,
        capabilities,
        networks: namespace_networks(&snapshot.manifests),
    })
}

fn aggregate_capabilities(
    manifests: &[ActiveManifestVersion],
) -> V2Result<BTreeMap<String, NamespaceCapability>> {
    let mut capability_counts = BTreeMap::<String, (usize, usize)>::new();

    for manifest in manifests {
        for (raw_name, flag) in &manifest.capability_flags {
            let product_name = product_capability_name(raw_name)?.to_owned();
            // Verified capabilities are decided per chain from the execution table below, not
            // from the manifest flag alone, which stays `shadow` on ENS while the route serves.
            if product_name == VERIFIED_RECORDS_CAPABILITY {
                continue;
            }
            let (declared_count, supported_count) =
                capability_counts.entry(product_name).or_default();
            *declared_count += 1;
            if flag.status == CapabilitySupportStatus::Supported {
                *supported_count += 1;
            }
        }
    }

    Ok(capability_counts
        .into_iter()
        .map(|(capability, (declared_count, supported_count))| {
            let completeness = if supported_count == declared_count {
                Completeness::Full
            } else if supported_count > 0 {
                Completeness::Partial
            } else {
                Completeness::Unsupported
            };
            let unsupported_reason =
                (completeness == Completeness::Unsupported).then(|| UNSUPPORTED_REASON.to_owned());

            (
                capability,
                NamespaceCapability {
                    completeness,
                    unsupported_reason,
                    chains: BTreeMap::new(),
                },
            )
        })
        .collect())
}

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
/// chain, a manifest declaring that entrypoint with a usable `verified_resolution` flag (shadow
/// manifests count where the route admits them, as ENS does), and a configured provider for the
/// execution chain. Per-name support classes still apply on the routes themselves; this is
/// deployment-level support.
fn verified_capabilities(
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
                .is_some_and(|flag| {
                    flag.status == CapabilitySupportStatus::Supported
                        || (entrypoint.allow_shadow
                            && flag.status == CapabilitySupportStatus::Shadow)
                })
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

fn product_capability_name(raw_name: &str) -> V2Result<&'static str> {
    match raw_name {
        "declared_children" => Ok("subnames"),
        "exact_name_profile" => Ok("name_profile"),
        "name_history" => Ok("name_history"),
        "verified_resolution" => Ok("verified_records"),
        _ => {
            error!(
                service = "api",
                raw_capability = %raw_name,
                "missing v2 product capability mapping"
            );
            Err(V2Error::internal_error(
                "namespace capability mapping is missing",
            ))
        }
    }
}

fn namespace_networks(manifests: &[ActiveManifestVersion]) -> Vec<NamespaceNetwork> {
    manifests
        .iter()
        .map(|manifest| manifest.chain.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(namespace_network)
        .collect()
}

fn namespace_network(chain: &str) -> NamespaceNetwork {
    let chain_id = slug_to_numeric(chain);
    let canonical_slug = chain_id.and_then(numeric_to_slug).unwrap_or(chain);

    NamespaceNetwork {
        network: display_network_slug(canonical_slug).to_owned(),
        chain_id,
    }
}

fn display_network_slug(chain_slug: &str) -> &str {
    match chain_slug {
        bigname_storage::BASE_MAINNET_CHAIN_ID => "base",
        bigname_storage::ETHEREUM_MAINNET_CHAIN_ID => "ethereum",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::{
        extract::{FromRequestParts, Path},
        http::{Request, StatusCode},
        response::IntoResponse,
    };
    use bigname_manifests::{CapabilityFlag, CapabilitySupportStatus};
    use sqlx::PgPool;

    use super::*;

    #[test]
    fn capability_aggregation_maps_full_partial_and_unsupported() {
        let manifests = vec![
            manifest(
                "ens_l1",
                "ethereum-mainnet",
                [
                    ("declared_children", CapabilitySupportStatus::Supported),
                    ("exact_name_profile", CapabilitySupportStatus::Supported),
                    ("verified_resolution", CapabilitySupportStatus::Supported),
                ],
            ),
            manifest(
                "ens_l2",
                "base-mainnet",
                [
                    ("verified_resolution", CapabilitySupportStatus::Unsupported),
                    ("name_history", CapabilitySupportStatus::Unsupported),
                ],
            ),
            manifest(
                "ens_l3",
                "ethereum-mainnet",
                [
                    ("exact_name_profile", CapabilitySupportStatus::Shadow),
                    ("name_history", CapabilitySupportStatus::Unsupported),
                ],
            ),
        ];

        let capabilities =
            aggregate_capabilities(&manifests).expect("capability aggregation must succeed");

        assert_eq!(
            capabilities["subnames"],
            NamespaceCapability {
                completeness: Completeness::Full,
                unsupported_reason: None,
                chains: BTreeMap::new(),
            }
        );
        assert!(
            !capabilities.contains_key("verified_records"),
            "verified capabilities are decided per chain, not from the manifest flag"
        );
        assert_eq!(
            capabilities["name_history"],
            NamespaceCapability {
                completeness: Completeness::Unsupported,
                unsupported_reason: Some(UNSUPPORTED_REASON.to_owned()),
                chains: BTreeMap::new(),
            }
        );
        assert_eq!(
            capabilities["name_profile"],
            NamespaceCapability {
                completeness: Completeness::Partial,
                unsupported_reason: None,
                chains: BTreeMap::new(),
            }
        );
    }

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
            CapabilitySupportStatus::Shadow,
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

    #[test]
    fn namespace_networks_use_display_slugs_and_numeric_chain_ids() {
        let manifests = vec![
            manifest("base_registry", "base-mainnet", []),
            manifest("ens_registry", "ethereum-mainnet", []),
            manifest("unknown_registry", "future-testnet", []),
        ];

        assert_eq!(
            namespace_networks(&manifests),
            vec![
                NamespaceNetwork {
                    network: "base".to_owned(),
                    chain_id: Some(8453),
                },
                NamespaceNetwork {
                    network: "ethereum".to_owned(),
                    chain_id: Some(1),
                },
                NamespaceNetwork {
                    network: "future-testnet".to_owned(),
                    chain_id: None,
                },
            ]
        );
    }

    #[test]
    fn missing_product_mapping_is_an_internal_error_without_leaking_the_raw_key() {
        let error = product_capability_name("declared_internal_pipeline").expect_err(
            "unmapped capability keys must not be exposed on the product namespace route",
        );

        let envelope = error.envelope();
        assert_eq!(envelope.error.code, "internal_error");
        assert_eq!(
            envelope.error.message,
            "namespace capability mapping is missing"
        );
    }

    #[tokio::test]
    async fn get_namespace_returns_not_found_for_unsupported_namespace() {
        let state = AppState::new(
            PgPool::connect_lazy_with(
                "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                    .parse()
                    .expect("static test database URL must parse"),
            ),
            bigname_lookup::ChainRpcUrls::default(),
        );

        let error = get_namespace(Path("unknown".to_owned()), NoQueryParams, State(state))
            .await
            .expect_err("unsupported namespace must return an error");
        let envelope = error.envelope();
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(envelope.error.code, "not_found");
        assert_eq!(envelope.error.message, "namespace unknown is not supported");
    }

    #[tokio::test]
    async fn no_query_params_rejects_namespace_controls() {
        let request = Request::builder()
            .uri("/v1/namespaces/ens?at=2026-06-10T00:00:00Z")
            .body(())
            .expect("request must build");
        let (mut parts, ()) = request.into_parts();

        let error = NoQueryParams::from_request_parts(&mut parts, &())
            .await
            .expect_err("namespace metadata route must reject query params");

        assert_eq!(error.envelope().error.code, "invalid_input");
    }

    fn manifest<const N: usize>(
        source_family: &str,
        chain: &str,
        capabilities: [(&str, CapabilitySupportStatus); N],
    ) -> ActiveManifestVersion {
        ActiveManifestVersion {
            manifest_version: 1,
            source_family: source_family.to_owned(),
            chain: chain.to_owned(),
            deployment_epoch: "test".to_owned(),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            capability_flags: capabilities
                .into_iter()
                .map(|(name, status)| {
                    (
                        name.to_owned(),
                        CapabilityFlag {
                            status,
                            notes: None,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        }
    }
}
