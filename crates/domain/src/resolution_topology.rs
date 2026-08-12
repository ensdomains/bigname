//! Projected resolution topology and its public
//! [verified lookup](../../../docs/glossary.md#verified-lookup) route classifier.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::vocabulary::{ChainId, EvmAddress, Namespace};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionTopology {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported_reason: Option<String>,
    #[serde(default)]
    pub registry_path: Option<Vec<ResolutionNameReference>>,
    #[serde(default)]
    pub subregistry_path: Option<Vec<ResolutionNameReference>>,
    #[serde(default)]
    pub resolver_path: Option<Vec<ResolutionResolverHop>>,
    #[serde(default)]
    pub wildcard: Option<ResolutionWildcard>,
    #[serde(default)]
    pub alias: Option<ResolutionAlias>,
    #[serde(default)]
    pub version_boundaries: Option<ResolutionVersionBoundaries>,
    #[serde(default)]
    pub transport: Option<ResolutionTransport>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionNameReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_name_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<Namespace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namehash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_kind: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionResolverHop {
    #[serde(default)]
    pub logical_name_id: Option<String>,
    #[serde(default)]
    pub namespace: Option<Namespace>,
    #[serde(default)]
    pub normalized_name: Option<String>,
    #[serde(default)]
    pub canonical_display_name: Option<String>,
    #[serde(default)]
    pub resource_id: Option<String>,
    #[serde(default)]
    pub chain_id: Option<ChainId>,
    #[serde(default)]
    pub address: Option<EvmAddress>,
    #[serde(default)]
    pub latest_event_kind: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionWildcard {
    #[serde(default)]
    pub source: Option<ResolutionNameReference>,
    #[serde(default)]
    pub matched_labels: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionAlias {
    #[serde(default)]
    pub final_target: Option<ResolutionNameReference>,
    #[serde(default)]
    pub hops: Option<Vec<ResolutionNameReference>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionVersionBoundaries {
    #[serde(default)]
    pub topology_version_boundary: Option<ResolutionBoundary>,
    #[serde(default)]
    pub record_version_boundary: Option<ResolutionBoundary>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionBoundary {
    #[serde(default)]
    pub logical_name_id: Option<String>,
    #[serde(default)]
    pub resource_id: Option<String>,
    #[serde(default)]
    pub normalized_event_id: Option<Value>,
    #[serde(default)]
    pub event_kind: Option<String>,
    #[serde(default)]
    pub chain_position: Option<ResolutionChainPosition>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionChainPosition {
    pub chain_id: ChainId,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolutionTransport {
    #[serde(default)]
    pub source_chain_id: Option<ChainId>,
    #[serde(default)]
    pub target_chain_id: Option<ChainId>,
    #[serde(default)]
    pub contract_address: Option<EvmAddress>,
    #[serde(default)]
    pub latest_event_kind: Option<String>,
    #[serde(flatten)]
    pub additional_fields: BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolutionTransportContract {
    pub source_chain_id: ChainId,
    pub target_chain_id: ChainId,
    pub contract_address: EvmAddress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionRoutePolicy {
    Ens,
    Basenames {
        expected_transport: ResolutionTransportContract,
    },
}

impl ResolutionRoutePolicy {
    pub const fn namespace(self) -> Namespace {
        match self {
            Self::Ens => Namespace::Ens,
            Self::Basenames { .. } => Namespace::Basenames,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionRoute {
    Direct,
    AliasOnly,
    WildcardDerived,
    BasenamesTransportDirect,
}

impl ResolutionTopology {
    pub fn classify(
        &self,
        logical_name_id: &str,
        policy: ResolutionRoutePolicy,
    ) -> Result<ResolutionRoute, ResolutionTopologyError> {
        if self.status.as_deref() == Some("unsupported") {
            return Err(error("projected topology is unsupported"));
        }

        let resolver_logical_name_id = self
            .resolver_path
            .as_ref()
            .and_then(|path| path.first())
            .and_then(|hop| hop.logical_name_id.as_deref())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                error("projected topology must include resolver_path[0].logical_name_id")
            })?;
        let alias_present = self.alias_is_present()?;
        let wildcard_source = self.wildcard_source()?;
        let transport_is_null = self.transport_is_null();

        match policy {
            ResolutionRoutePolicy::Basenames { expected_transport } => {
                if transport_is_null {
                    return Err(error(
                        "projected Basenames topology must include supported transport detail",
                    ));
                }
                if !self.subregistry_path_is_empty() {
                    return Err(error(
                        "projected Basenames topology must keep subregistry_path empty",
                    ));
                }
                if resolver_logical_name_id != logical_name_id {
                    return Err(error(
                        "projected Basenames topology must anchor resolver_path[0] to the request name",
                    ));
                }
                if alias_present {
                    return Err(error(
                        "projected Basenames topology must keep alias detail empty",
                    ));
                }
                if wildcard_source.is_some() {
                    return Err(error(
                        "projected Basenames topology must keep wildcard detail empty",
                    ));
                }
                if !self.transport_matches(expected_transport) {
                    return Err(error(
                        "projected Basenames topology transport is outside the supported class",
                    ));
                }
                Ok(ResolutionRoute::BasenamesTransportDirect)
            }
            ResolutionRoutePolicy::Ens => {
                if !transport_is_null {
                    return Err(error(
                        "projected ENS topology must keep transport detail null",
                    ));
                }

                if let Some(wildcard_source) = wildcard_source {
                    if alias_present || !self.subregistry_path_is_empty() {
                        return Err(error(
                            "projected wildcard-derived ENS topology must keep alias detail empty and subregistry_path empty",
                        ));
                    }
                    if resolver_logical_name_id != wildcard_source {
                        return Err(error(
                            "projected wildcard-derived ENS topology must anchor resolver_path[0] to wildcard.source.logical_name_id",
                        ));
                    }
                    return Ok(ResolutionRoute::WildcardDerived);
                }

                if resolver_logical_name_id != logical_name_id {
                    return Err(error(
                        "projected ENS topology must anchor resolver_path[0] to the request name",
                    ));
                }
                if alias_present {
                    Ok(ResolutionRoute::AliasOnly)
                } else {
                    Ok(ResolutionRoute::Direct)
                }
            }
        }
    }

    fn alias_is_present(&self) -> Result<bool, ResolutionTopologyError> {
        let alias = self
            .alias
            .as_ref()
            .ok_or_else(|| error("projected topology must include alias"))?;
        let hops = alias
            .hops
            .as_ref()
            .ok_or_else(|| error("projected topology alias must include hops"))?;
        let final_target_present = alias.final_target.is_some();
        if final_target_present == hops.is_empty() {
            return Err(error(
                "projected topology alias must set final_target and non-empty hops together",
            ));
        }
        Ok(final_target_present)
    }

    fn wildcard_source(&self) -> Result<Option<&str>, ResolutionTopologyError> {
        let wildcard = self
            .wildcard
            .as_ref()
            .ok_or_else(|| error("projected topology must include wildcard"))?;
        let labels = wildcard
            .matched_labels
            .as_ref()
            .ok_or_else(|| error("projected topology wildcard must include matched_labels"))?;
        match wildcard.source.as_ref() {
            None if labels.is_empty() => Ok(None),
            None => Err(error(
                "projected topology wildcard with null source must keep matched_labels empty",
            )),
            Some(_) if labels.is_empty() => Err(error(
                "projected topology wildcard must keep matched_labels non-empty when source is present",
            )),
            Some(source) => source
                .logical_name_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(Some)
                .ok_or_else(|| {
                    error("projected topology wildcard source must include logical_name_id")
                }),
        }
    }

    fn subregistry_path_is_empty(&self) -> bool {
        self.subregistry_path.as_ref().is_some_and(Vec::is_empty)
    }

    fn transport_is_null(&self) -> bool {
        self.transport.as_ref().is_none_or(|transport| {
            transport.source_chain_id.is_none()
                && transport.target_chain_id.is_none()
                && transport.contract_address.is_none()
                && transport.latest_event_kind.is_none()
                && transport.additional_fields.values().all(Value::is_null)
        })
    }

    fn transport_matches(&self, expected: ResolutionTransportContract) -> bool {
        self.transport.as_ref().is_some_and(|transport| {
            transport.source_chain_id == Some(expected.source_chain_id)
                && transport.target_chain_id == Some(expected.target_chain_id)
                && transport.contract_address == Some(expected.contract_address)
                && transport.additional_fields.values().all(Value::is_null)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolutionTopologyError {
    message: &'static str,
}

impl fmt::Display for ResolutionTopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for ResolutionTopologyError {}

const fn error(message: &'static str) -> ResolutionTopologyError {
    ResolutionTopologyError { message }
}
