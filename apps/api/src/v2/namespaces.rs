use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_domain::vocabulary::SourceFamily;
use bigname_lookup::ChainRpcUrls;
use bigname_manifests::{
    ActiveManifestVersion, CapabilitySupportStatus, ExecutionManifestVersion,
    NamespaceManifestSnapshot, load_execution_manifests_for_namespace,
    load_namespace_manifest_snapshot,
};
use bigname_storage::{
    Protocol, begin_read_snapshot, load_resolution_state_on, load_served_project_generation,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::error;

use super::support::ensure_public_namespace;
use crate::AppState;

use super::{
    Completeness, Envelope, Meta, NoQueryParams, V2Error, V2Result, api_error_to_v2,
    numeric_to_slug, slug_to_numeric,
};

mod verified;

use verified::verified_capabilities;

const UNSUPPORTED_REASON: &str = "not_supported_for_namespace";
const SUBNAMES_CAPABILITY: &str = "subnames";
const NAME_PROFILE_CAPABILITY: &str = "name_profile";
const NAME_HISTORY_CAPABILITY: &str = "name_history";

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resolution: Option<NamespaceResolution>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceResolution {
    pub(crate) protocol: ResolutionProtocol,
    pub(crate) since_block: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResolutionProtocol {
    EnsV1,
    EnsV2,
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

    let resolutions = load_resolutions(&state.pool, &execution_manifests)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load v2 namespace resolution state"
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
            &resolutions,
            &state.lookup_chain_rpc_urls,
        )?,
        page: None,
        meta: Meta::default(),
    }))
}

/// Per chain with an ENS execution entrypoint and a servable family publication, keyed by chain
/// slug. A chain with no client-facing proxy row has observed no `Upgraded`, so ENSv1 governs
/// with no known start.
async fn load_resolutions(
    pool: &PgPool,
    execution_manifests: &[ExecutionManifestVersion],
) -> anyhow::Result<BTreeMap<String, NamespaceResolution>> {
    let chains: BTreeSet<&str> = execution_manifests
        .iter()
        .filter(|manifest| manifest.source_family == SourceFamily::EnsExecution.as_str())
        .map(|manifest| manifest.chain.as_str())
        .collect();
    let mut resolutions = BTreeMap::new();
    if chains.is_empty() {
        return Ok(resolutions);
    }
    let mut snapshot = begin_read_snapshot(pool).await?;
    for chain in chains {
        // The publication fence name reads apply: a bootstrapping, lagging, redoing or orphaned
        // publication holds proxy rows name reads would refuse to serve.
        let servable = load_served_project_generation(
            &mut *snapshot,
            chain,
            0,
            "",
            false,
            false,
            crate::state::publication_lag_tolerance_blocks(),
        )
        .await?;
        if servable.is_none() {
            continue;
        }
        let resolution = match load_resolution_state_on(&mut snapshot, chain).await? {
            Some(state) => NamespaceResolution {
                protocol: match state.protocol {
                    Protocol::EnsV1 => ResolutionProtocol::EnsV1,
                    Protocol::EnsV2 => ResolutionProtocol::EnsV2,
                },
                since_block: Some(state.since_block),
            },
            None => NamespaceResolution {
                protocol: ResolutionProtocol::EnsV1,
                since_block: None,
            },
        };
        resolutions.insert(chain.to_owned(), resolution);
    }
    snapshot.commit().await?;
    Ok(resolutions)
}

fn build_namespace(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
    execution_manifests: &[ExecutionManifestVersion],
    resolutions: &BTreeMap<String, NamespaceResolution>,
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
        networks: namespace_networks(&snapshot.manifests, resolutions),
    })
}

fn aggregate_capabilities(
    manifests: &[ActiveManifestVersion],
) -> V2Result<BTreeMap<String, NamespaceCapability>> {
    let (mut declared_count, mut supported_count) = (0_usize, 0_usize);
    for manifest in manifests {
        for (raw_name, flag) in &manifest.capability_flags {
            // Only `declared_children` feeds the summary. Name reads and name history serve every
            // name of a served namespace, and verified capabilities are decided per chain.
            if product_capability_name(raw_name)? != SUBNAMES_CAPABILITY {
                continue;
            }
            declared_count += 1;
            if flag.status == CapabilitySupportStatus::Supported {
                supported_count += 1;
            }
        }
    }

    let mut capabilities = BTreeMap::new();
    if declared_count > 0 {
        let completeness = if supported_count == declared_count {
            Completeness::Full
        } else if supported_count > 0 {
            Completeness::Partial
        } else {
            Completeness::Unsupported
        };
        capabilities.insert(
            SUBNAMES_CAPABILITY.to_owned(),
            NamespaceCapability {
                completeness,
                unsupported_reason: (completeness == Completeness::Unsupported)
                    .then(|| UNSUPPORTED_REASON.to_owned()),
                chains: BTreeMap::new(),
            },
        );
    }
    let served = NamespaceCapability {
        completeness: if manifests.is_empty() {
            Completeness::Unsupported
        } else {
            Completeness::Full
        },
        unsupported_reason: manifests.is_empty().then(|| UNSUPPORTED_REASON.to_owned()),
        chains: BTreeMap::new(),
    };
    for capability in [NAME_PROFILE_CAPABILITY, NAME_HISTORY_CAPABILITY] {
        capabilities.insert(capability.to_owned(), served.clone());
    }
    Ok(capabilities)
}

fn product_capability_name(raw_name: &str) -> V2Result<&'static str> {
    match raw_name {
        "declared_children" => Ok(SUBNAMES_CAPABILITY),
        "exact_name_profile" => Ok(NAME_PROFILE_CAPABILITY),
        "name_history" => Ok(NAME_HISTORY_CAPABILITY),
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

fn namespace_networks(
    manifests: &[ActiveManifestVersion],
    resolutions: &BTreeMap<String, NamespaceResolution>,
) -> Vec<NamespaceNetwork> {
    manifests
        .iter()
        .map(|manifest| manifest.chain.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|chain| namespace_network(chain, resolutions.get(chain).cloned()))
        .collect()
}

fn namespace_network(chain: &str, resolution: Option<NamespaceResolution>) -> NamespaceNetwork {
    let chain_id = slug_to_numeric(chain);
    let canonical_slug = chain_id.and_then(numeric_to_slug).unwrap_or(chain);

    NamespaceNetwork {
        network: display_network_slug(canonical_slug).to_owned(),
        chain_id,
        resolution,
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
    fn capability_aggregation_maps_subnames_and_serves_profile_and_history() {
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
                    ("declared_children", CapabilitySupportStatus::Unsupported),
                    ("verified_resolution", CapabilitySupportStatus::Unsupported),
                    ("name_history", CapabilitySupportStatus::Unsupported),
                ],
            ),
            manifest(
                "ens_l3",
                "ethereum-mainnet",
                [
                    ("exact_name_profile", CapabilitySupportStatus::Unsupported),
                    ("name_history", CapabilitySupportStatus::Unsupported),
                ],
            ),
        ];
        let flag_free = vec![manifest("basenames_base_registrar", "base-mainnet", [])];

        let capabilities =
            aggregate_capabilities(&manifests).expect("capability aggregation must succeed");
        let full = NamespaceCapability {
            completeness: Completeness::Full,
            unsupported_reason: None,
            chains: BTreeMap::new(),
        };
        let unsupported = NamespaceCapability {
            completeness: Completeness::Unsupported,
            unsupported_reason: Some(UNSUPPORTED_REASON.to_owned()),
            chains: BTreeMap::new(),
        };

        assert_eq!(
            capabilities["subnames"],
            NamespaceCapability {
                completeness: Completeness::Partial,
                unsupported_reason: None,
                chains: BTreeMap::new(),
            }
        );
        assert!(
            !capabilities.contains_key("verified_records"),
            "verified capabilities are decided per chain, not from the manifest flag"
        );
        let flag_free =
            aggregate_capabilities(&flag_free).expect("a namespace without flags aggregates");
        let empty = aggregate_capabilities(&[]).expect("an empty namespace aggregates");
        assert!(!flag_free.contains_key("subnames") && !empty.contains_key("subnames"));
        for capability in ["name_profile", "name_history"] {
            assert_eq!(
                capabilities[capability], full,
                "{capability} ignores the manifests' unsupported flags"
            );
            assert_eq!(flag_free[capability], full, "{capability}");
            assert_eq!(empty[capability], unsupported, "{capability}");
        }
    }

    #[test]
    fn namespace_networks_use_display_slugs_and_numeric_chain_ids() {
        let manifests = vec![
            manifest("base_registry", "base-mainnet", []),
            manifest("ens_registry", "ethereum-mainnet", []),
            manifest("unknown_registry", "future-testnet", []),
        ];

        let v1 = NamespaceResolution {
            protocol: ResolutionProtocol::EnsV1,
            since_block: None,
        };
        let resolutions = BTreeMap::from([("ethereum-mainnet".to_owned(), v1.clone())]);

        assert_eq!(
            namespace_networks(&manifests, &resolutions),
            vec![
                NamespaceNetwork {
                    network: "base".to_owned(),
                    chain_id: Some(8453),
                    resolution: None,
                },
                NamespaceNetwork {
                    network: "ethereum".to_owned(),
                    chain_id: Some(1),
                    resolution: Some(v1),
                },
                NamespaceNetwork {
                    network: "future-testnet".to_owned(),
                    chain_id: None,
                    resolution: None,
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

    pub(super) fn manifest<const N: usize>(
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
