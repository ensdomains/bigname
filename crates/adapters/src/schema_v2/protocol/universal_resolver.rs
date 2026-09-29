//! The `ens_execution` family's one admitted event: `Upgraded(address)` from a declared Universal
//! Resolver proxy. The event records which implementation the proxy now forwards to, classified
//! against the manifest: an admitted UniversalResolverV2 implementation
//! (`universal_resolver_implementations`), another declared Universal Resolver proxy (the
//! client-facing proxy pointing at the managed one), or anything else. Project keeps the latest
//! per proxy, and a block counts as resolving through ENSv2 while the client-facing proxy's chain
//! ends at an admitted implementation (docs/manifests.md § `universal_resolver_implementations`).
//! The proxy forwards every call to its implementation; `upgradeTo` emits the event, and the
//! constructor sets the first implementation without one.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L71-L75 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L111-L115 @ ens_v2_sepolia_20260916@366de741)
use alloy_sol_types::sol;
use serde_json::json;

use super::{EventDraft, Interpreted, ensure_declared};
use crate::{
    evm_abi::{address_hex, decode_event_log},
    schema_v2::{catalog::Selected, model::RawLogInput},
};

sol! { event Upgraded(address indexed implementation); }

/// The signatures the family decodes.
pub(super) fn supports(signature: &str) -> bool {
    signature == "Upgraded(address)"
}

/// What an upgraded proxy now forwards to.
fn implementation_kind(selected: &Selected, implementation: &str) -> &'static str {
    let source = &selected.source;
    if source
        .universal_resolver_implementations
        .iter()
        .any(|admitted| admitted == implementation)
    {
        "admitted_universal_resolver"
    } else if source
        .universal_resolver_proxies
        .iter()
        .any(|proxy| proxy == implementation)
    {
        "universal_resolver_proxy"
    } else {
        "other"
    }
}

pub(super) fn interpret(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<Upgraded>(&raw.topics, &raw.data, "Upgraded log is malformed")?;
    ensure_declared(selected, &["Upgraded"])?;
    let implementation = address_hex(event.implementation).to_ascii_lowercase();
    let mut output = Interpreted::new();
    output.events.push(EventDraft {
        event_kind: "Upgraded".to_owned(),
        logical_name_id: None,
        resource_id: None,
        identity_suffix: "Upgraded".to_owned(),
        explicit_before: None,
        after_state: json!({
            "source_event": "Upgraded",
            "proxy_address": raw.emitting_address.to_ascii_lowercase(),
            "proxy_role": selected.emitter_role,
            "implementation": implementation,
            "implementation_kind": implementation_kind(selected, &implementation),
        }),
        state_scope: String::new(),
    });
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy_primitives::Address;
    use alloy_sol_types::SolEvent;
    use serde_json::json;

    use super::{Upgraded, interpret};
    use crate::schema_v2::{
        catalog::Selected,
        manifest::{ManifestEvent, ManifestSource},
        model::RawLogInput,
    };

    const TOP: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
    const MANAGED: &str = "0x6d80f2172cfdec5730fe683860c33d26fc42e6f1";
    const ADMITTED: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";

    fn upgrade(emitter: &str, role: &str, implementation: &str) -> (Selected, RawLogInput) {
        let encoded = Upgraded {
            implementation: implementation.parse::<Address>().expect("an address"),
        }
        .encode_log_data();
        let event = ManifestEvent {
            name: "Upgraded".to_owned(),
            signature: "Upgraded(address)".to_owned(),
            topic0: format!("{:#x}", Upgraded::SIGNATURE_HASH),
            emitter_roles: vec![
                "universal_resolver".into(),
                "universal_resolver_managed".into(),
            ],
            normalized_events: vec!["Upgraded".to_owned()],
        };
        let source = ManifestSource {
            manifest_id: 1,
            manifest_version: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_execution".to_owned(),
            chain_id: "ethereum-sepolia".to_owned(),
            deployment_label: "unit-test".to_owned(),
            correlation_addresses: BTreeMap::new(),
            resolver_implementations: Vec::new(),
            universal_resolver_implementations: vec![ADMITTED.to_owned()],
            universal_resolver_proxies: vec![TOP.to_owned(), MANAGED.to_owned()],
            events: vec![event.clone()],
        };
        let selected = Selected {
            source,
            event,
            contract_instance_id: uuid::Uuid::from_u128(1),
            emitter_role: Some(role.to_owned()),
            match_all: false,
            manifest_declared_emitter: true,
        };
        let raw = RawLogInput {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: format!("0x{:064x}", 1_u64),
            block_number: 1,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_800_000_000)
                .expect("a time"),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: format!("0x{:064x}", 2_u64),
            transaction_index: 0,
            log_index: 0,
            emitting_address: emitter.to_owned(),
            topics: encoded
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            data: encoded.data.to_vec(),
        };
        (selected, raw)
    }

    fn after(emitter: &str, role: &str, implementation: &str) -> serde_json::Value {
        let (selected, raw) = upgrade(emitter, role, implementation);
        let output = interpret(&selected, &raw).expect("an Upgraded decodes");
        assert_eq!(output.events.len(), 1);
        assert!(
            output.discovery.is_empty(),
            "an upgrade discovers no contract"
        );
        output.events[0].after_state.clone()
    }

    #[test]
    fn an_upgrade_is_classified_against_the_manifest() {
        assert_eq!(
            after(TOP, "universal_resolver", MANAGED),
            json!({"source_event": "Upgraded", "proxy_address": TOP,
                   "proxy_role": "universal_resolver", "implementation": MANAGED,
                   "implementation_kind": "universal_resolver_proxy"})
        );
        assert_eq!(
            after(MANAGED, "universal_resolver_managed", ADMITTED)["implementation_kind"],
            json!("admitted_universal_resolver")
        );
        assert_eq!(
            after(
                MANAGED,
                "universal_resolver_managed",
                "0x2f8a180600000000000000000000000000000000"
            )["implementation_kind"],
            json!("other")
        );
    }
}
