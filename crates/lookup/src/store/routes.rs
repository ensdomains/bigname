use bigname_domain::{
    resolution_topology::{
        ResolutionAlias, ResolutionResolverHop, ResolutionRoute, ResolutionRoutePolicy,
        ResolutionTopology, ResolutionTransport, ResolutionTransportContract,
        ResolutionVersionBoundaries, ResolutionWildcard,
    },
    vocabulary::{ChainId, EvmAddress, Namespace, SourceFamily},
};

use crate::{
    BASENAMES_L1_RESOLVER_ROLE, ENS_UNIVERSAL_RESOLVER_ROLE, ETHEREUM_MAINNET_CHAIN_ID,
    LookupError, Result, abi::ResolutionResultAbi,
};

use super::manifests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LookupRoute {
    Projected,
    EnsUniversalResolverDiscovery,
}

pub(super) struct DiscoveryRouteCandidate<'a> {
    pub namespace: Namespace,
    pub resource_chain_id: &'a str,
    pub logical_name_id: &'a str,
    pub namehash: &'a str,
    pub dns_name: &'a [u8],
    pub exact_resolver_is_null: bool,
    pub topology: &'a ResolutionTopology,
    pub path: ResolutionRoute,
}

pub(super) struct EntrypointAuthority {
    pub source_family: SourceFamily,
    pub chain_id: ChainId,
    pub role: &'static str,
    pub follow_ccip: bool,
    pub result_abi: ResolutionResultAbi,
    pub allow_shadow: bool,
    pub required_manifest_version: Option<i64>,
}

pub(super) fn entrypoint_authority(
    namespace: Namespace,
    resolver_chain_id: ChainId,
) -> Result<EntrypointAuthority> {
    match (namespace, resolver_chain_id) {
        (Namespace::Ens, ChainId::EthereumMainnet) => Ok(EntrypointAuthority {
            source_family: SourceFamily::EnsExecution,
            chain_id: ChainId::EthereumMainnet,
            role: ENS_UNIVERSAL_RESOLVER_ROLE,
            follow_ccip: false,
            result_abi: ResolutionResultAbi::EnsUniversalResolver,
            allow_shadow: true,
            required_manifest_version: None,
        }),
        (Namespace::Basenames, ChainId::BaseMainnet) => Ok(EntrypointAuthority {
            source_family: SourceFamily::BasenamesExecution,
            chain_id: ChainId::EthereumMainnet,
            role: BASENAMES_L1_RESOLVER_ROLE,
            follow_ccip: true,
            result_abi: ResolutionResultAbi::BasenamesL1Resolver,
            allow_shadow: false,
            required_manifest_version: Some(2),
        }),
        _ => Err(LookupError::unsupported(
            "projected resolution topology is outside the supported lookup paths",
        )),
    }
}

pub(super) fn route_policy(
    namespace: Namespace,
    entrypoint_manifest: &manifests::ManifestEntry,
) -> Result<ResolutionRoutePolicy> {
    match namespace {
        Namespace::Ens => Ok(ResolutionRoutePolicy::Ens),
        Namespace::Basenames => {
            let contract_address = entrypoint_manifest
                .declared_address
                .parse::<EvmAddress>()
                .map_err(|error| {
                    LookupError::unsupported(format!(
                        "execution manifest declares an invalid transport address: {error}"
                    ))
                })?;
            Ok(ResolutionRoutePolicy::Basenames {
                expected_transport: ResolutionTransportContract {
                    source_chain_id: ChainId::BaseMainnet,
                    target_chain_id: ChainId::EthereumMainnet,
                    contract_address,
                },
            })
        }
    }
}

pub(super) fn preflight_route_policy(
    namespace: Namespace,
    topology: &ResolutionTopology,
) -> Result<ResolutionRoutePolicy> {
    match namespace {
        Namespace::Ens => Ok(ResolutionRoutePolicy::Ens),
        Namespace::Basenames => {
            let contract_address = topology
                .transport
                .as_ref()
                .and_then(|transport| transport.contract_address)
                .ok_or_else(|| {
                    LookupError::unsupported(
                        "projected Basenames topology must include a transport contract",
                    )
                })?;
            Ok(ResolutionRoutePolicy::Basenames {
                expected_transport: ResolutionTransportContract {
                    source_chain_id: ChainId::BaseMainnet,
                    target_chain_id: ChainId::EthereumMainnet,
                    contract_address,
                },
            })
        }
    }
}

pub(super) fn classify_lookup_route(candidate: DiscoveryRouteCandidate<'_>) -> LookupRoute {
    if candidate.namespace == Namespace::Ens
        && candidate.resource_chain_id == ETHEREUM_MAINNET_CHAIN_ID
        && candidate.exact_resolver_is_null
        && is_ens_universal_resolver_discovery_topology(
            candidate.topology,
            candidate.path,
            candidate.logical_name_id,
        )
        && !candidate.dns_name.is_empty()
        && crate::abi::parse_node(candidate.namehash).is_ok()
    {
        LookupRoute::EnsUniversalResolverDiscovery
    } else {
        LookupRoute::Projected
    }
}

pub(super) fn classify_absent_topology_route(
    namespace: Namespace,
    resource_chain_id: &str,
    logical_name_id: &str,
    namehash: &str,
    dns_name: &[u8],
    exact_resolver_is_null: bool,
) -> Option<ResolutionTopology> {
    let topology = direct_null_topology(logical_name_id);
    (classify_lookup_route(DiscoveryRouteCandidate {
        namespace,
        resource_chain_id,
        logical_name_id,
        namehash,
        dns_name,
        exact_resolver_is_null,
        topology: &topology,
        path: ResolutionRoute::Direct,
    }) == LookupRoute::EnsUniversalResolverDiscovery)
        .then_some(topology)
}

fn direct_null_topology(logical_name_id: &str) -> ResolutionTopology {
    ResolutionTopology {
        status: None,
        unsupported_reason: None,
        registry_path: Some(Vec::new()),
        subregistry_path: Some(Vec::new()),
        resolver_path: Some(vec![ResolutionResolverHop {
            logical_name_id: Some(logical_name_id.to_owned()),
            namespace: Some(Namespace::Ens),
            normalized_name: None,
            canonical_display_name: None,
            resource_id: None,
            chain_id: Some(ChainId::EthereumMainnet),
            address: None,
            latest_event_kind: None,
        }]),
        wildcard: Some(ResolutionWildcard {
            source: None,
            matched_labels: Some(Vec::new()),
        }),
        alias: Some(ResolutionAlias {
            final_target: None,
            hops: Some(Vec::new()),
        }),
        version_boundaries: Some(ResolutionVersionBoundaries {
            topology_version_boundary: None,
            record_version_boundary: None,
        }),
        transport: Some(ResolutionTransport {
            source_chain_id: None,
            target_chain_id: None,
            contract_address: None,
            latest_event_kind: None,
            additional_fields: Default::default(),
        }),
    }
}

pub(super) fn selected_resolver(
    route: LookupRoute,
    topology: &ResolutionTopology,
) -> Result<(ChainId, EvmAddress)> {
    if route == LookupRoute::EnsUniversalResolverDiscovery {
        return Ok((ChainId::EthereumMainnet, EvmAddress::from_bytes([0_u8; 20])));
    }
    let hop = topology
        .resolver_path
        .as_ref()
        .and_then(|path| path.last())
        .ok_or_else(|| LookupError::unsupported("projected topology has no selected resolver"))?;
    match (hop.chain_id, hop.address) {
        (Some(chain_id), Some(address)) => Ok((chain_id, address)),
        _ => Err(LookupError::unsupported(
            "projected topology has no concrete resolver",
        )),
    }
}

pub(super) fn is_ens_universal_resolver_discovery_topology(
    topology: &ResolutionTopology,
    path: ResolutionRoute,
    logical_name_id: &str,
) -> bool {
    let Some([hop]) = topology.resolver_path.as_deref() else {
        return false;
    };
    path == ResolutionRoute::Direct
        && topology
            .subregistry_path
            .as_ref()
            .is_some_and(Vec::is_empty)
        && hop.logical_name_id.as_deref() == Some(logical_name_id)
        && matches!(hop.chain_id, None | Some(ChainId::EthereumMainnet))
        && hop
            .address
            .is_none_or(|address| address == EvmAddress::from_bytes([0_u8; 20]))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    const LOGICAL_NAME_ID: &str =
        "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";
    const NAMEHASH: &str = "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";

    #[test]
    fn discovery_route_admits_only_the_exact_ens_mainnet_shape() {
        assert_eq!(
            route(
                topology(),
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                NAMEHASH,
                true,
                b"\x05alice\x03eth\0",
            ),
            LookupRoute::EnsUniversalResolverDiscovery
        );
        for (label, namespace, chain, exact_null, dns, namehash) in [
            (
                "namespace",
                Namespace::Basenames,
                ETHEREUM_MAINNET_CHAIN_ID,
                true,
                &b"x"[..],
                NAMEHASH,
            ),
            (
                "chain",
                Namespace::Ens,
                "base-mainnet",
                true,
                &b"x"[..],
                NAMEHASH,
            ),
            (
                "resolver",
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                false,
                &b"x"[..],
                NAMEHASH,
            ),
            (
                "dns",
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                true,
                &b""[..],
                NAMEHASH,
            ),
            (
                "identity",
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                true,
                &b"x"[..],
                "bad",
            ),
        ] {
            assert_eq!(
                route(topology(), namespace, chain, namehash, exact_null, dns),
                LookupRoute::Projected,
                "{label}"
            );
        }

        let mut alias = topology();
        alias["alias"] = json!({"final_target":{},"hops":[{}]});
        let mut wildcard = topology();
        wildcard["wildcard"] =
            json!({"source":{"logical_name_id":"ens:ancestor"},"matched_labels":["alice"]});
        wildcard["resolver_path"][0]["logical_name_id"] = json!("ens:ancestor");
        let mut subregistry = topology();
        subregistry["subregistry_path"] = json!([{}]);
        let mut transport = topology();
        transport["transport"] = json!({
            "source_chain_id":"ethereum-mainnet",
            "target_chain_id":"ethereum-mainnet",
            "contract_address":"0x1000000000000000000000000000000000000001"
        });
        let mut malformed = topology();
        malformed["resolver_path"] = json!([]);
        for (label, shaped) in [
            ("alias", alias),
            ("wildcard", wildcard),
            ("subregistry", subregistry),
            ("transport", transport),
            ("malformed", malformed),
        ] {
            assert_eq!(
                route(
                    shaped,
                    Namespace::Ens,
                    ETHEREUM_MAINNET_CHAIN_ID,
                    NAMEHASH,
                    true,
                    b"x"
                ),
                LookupRoute::Projected,
                "{label}"
            );
        }
    }

    #[test]
    fn absent_topology_admits_only_the_projected_direct_null_shape() {
        assert!(
            classify_absent_topology_route(
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                LOGICAL_NAME_ID,
                NAMEHASH,
                b"\x05alice\x03eth\0",
                true,
            )
            .is_some()
        );
        for (namespace, chain, namehash, dns_name, exact_null) in [
            (
                Namespace::Basenames,
                ETHEREUM_MAINNET_CHAIN_ID,
                NAMEHASH,
                &b"x"[..],
                true,
            ),
            (Namespace::Ens, "base-mainnet", NAMEHASH, &b"x"[..], true),
            (
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                "bad",
                &b"x"[..],
                true,
            ),
            (
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                NAMEHASH,
                &b""[..],
                true,
            ),
            (
                Namespace::Ens,
                ETHEREUM_MAINNET_CHAIN_ID,
                NAMEHASH,
                &b"x"[..],
                false,
            ),
        ] {
            assert!(
                classify_absent_topology_route(
                    namespace,
                    chain,
                    LOGICAL_NAME_ID,
                    namehash,
                    dns_name,
                    exact_null,
                )
                .is_none()
            );
        }
    }

    fn topology() -> Value {
        json!({
            "registry_path": [],
            "subregistry_path": [],
            "resolver_path": [{
                "logical_name_id": LOGICAL_NAME_ID,
                "chain_id": "ethereum-mainnet",
                "address": null
            }],
            "wildcard": {"source": null, "matched_labels": []},
            "alias": {"final_target": null, "hops": []},
            "version_boundaries": {},
            "transport": {
                "source_chain_id": null,
                "target_chain_id": null,
                "contract_address": null,
                "latest_event_kind": null
            }
        })
    }

    fn route(
        topology: Value,
        namespace: Namespace,
        resource_chain_id: &str,
        namehash: &str,
        exact_resolver_is_null: bool,
        dns_name: &[u8],
    ) -> LookupRoute {
        let topology: ResolutionTopology = serde_json::from_value(topology).expect("topology");
        let Ok(path) = topology.classify(LOGICAL_NAME_ID, ResolutionRoutePolicy::Ens) else {
            return LookupRoute::Projected;
        };
        classify_lookup_route(DiscoveryRouteCandidate {
            namespace,
            resource_chain_id,
            logical_name_id: LOGICAL_NAME_ID,
            namehash,
            dns_name,
            exact_resolver_is_null,
            topology: &topology,
            path,
        })
    }
}
