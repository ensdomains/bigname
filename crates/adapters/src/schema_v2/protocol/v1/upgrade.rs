use alloy_sol_types::sol;
use serde_json::json;

use super::super::{DiscoveryDraft, Interpreted, ensure_declared};
use super::support::single_event;
use crate::{
    evm_abi::{address_hex, decode_event_log},
    schema_v2::{catalog::Selected, model::RawLogInput},
};

sol! { event Upgraded(address indexed implementation); }

pub(super) fn interpret(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<Upgraded>(&raw.topics, &raw.data, "Upgraded log is malformed")?;
    ensure_declared(selected, &["Upgraded"])?;
    let implementation = address_hex(event.implementation);
    let mut output = single_event(
        "Upgraded",
        None,
        None,
        json!({"source_event":"Upgraded","proxy_address":raw.emitting_address,"implementation":implementation}),
    );
    output.discovery.push(DiscoveryDraft::Edge {
        edge_kind: "proxy_implementation".to_owned(),
        to_address: implementation,
        admission_basis: "erc1967_upgrade_event".to_owned(),
        observation_key: format!(
            "proxy-implementation:{}",
            raw.emitting_address.to_ascii_lowercase()
        ),
    });
    Ok(output)
}
