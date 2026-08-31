use bigname_domain::resolution_topology::{
    ResolutionRoute, ResolutionRoutePolicy, ResolutionTopology,
};
use bigname_domain::vocabulary::{ChainId, EvmAddress};
use bigname_storage::NameCurrentRow;
use serde_json::Value;

use super::string_field;

pub(super) fn terminal_no_declared_resolver(row: &NameCurrentRow) -> bool {
    let Some(resolver) = row
        .declared_summary
        .get("resolver")
        .filter(|value| value.is_object())
    else {
        return false;
    };
    if string_field(resolver.get("status")).as_deref() == Some("unsupported") {
        return false;
    }

    string_field(resolver.get("chain_id")).is_none()
        && string_field(resolver.get("address")).is_none()
}

pub(crate) fn ens_universal_resolver_discovery_candidate(row: &NameCurrentRow) -> bool {
    if row.namespace != "ens"
        || !row
            .declared_summary
            .get("resolver")
            .filter(|resolver| resolver.is_object())
            .is_some_and(|resolver| {
                resolver.get("chain_id").is_some_and(Value::is_null)
                    && resolver.get("address").is_some_and(Value::is_null)
                    && string_field(resolver.get("status")).as_deref() != Some("unsupported")
            })
        || !row.namehash.strip_prefix("0x").is_some_and(|digits| {
            digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        || row
            .chain_positions
            .pointer("/ethereum/chain_id")
            .and_then(Value::as_str)
            != Some("ethereum-mainnet")
    {
        return false;
    }
    let Some(topology_value) = row.declared_summary.get("topology") else {
        // Ordinary direct projected rows omit topology. Special alias, wildcard,
        // subregistry, and transport shapes carry explicit topology and are
        // checked below.
        return true;
    };
    let Ok(topology) = serde_json::from_value::<ResolutionTopology>(topology_value.clone()) else {
        return false;
    };
    if topology.classify(&row.logical_name_id, ResolutionRoutePolicy::Ens)
        != Ok(ResolutionRoute::Direct)
        || !topology
            .subregistry_path
            .as_ref()
            .is_some_and(Vec::is_empty)
    {
        return false;
    }
    let Some([hop]) = topology.resolver_path.as_deref() else {
        return false;
    };
    hop.logical_name_id.as_deref() == Some(row.logical_name_id.as_str())
        && matches!(hop.chain_id, None | Some(ChainId::EthereumMainnet))
        && hop
            .address
            .is_none_or(|address| address == EvmAddress::from_bytes([0_u8; 20]))
}
