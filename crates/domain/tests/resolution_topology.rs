use std::str::FromStr;

use bigname_domain::{
    resolution_topology::{
        ResolutionRoute, ResolutionRoutePolicy, ResolutionTopology, ResolutionTransportContract,
    },
    vocabulary::{ChainId, EvmAddress},
};
use serde_json::{Value, json};

const NAME_ID: &str = "ens:0xname";
const WILDCARD_ID: &str = "ens:0xwildcard";
const TRANSPORT: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";

fn direct_topology() -> Value {
    json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": NAME_ID,
            "namespace": "ens",
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000001"
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": {
            "topology_version_boundary": null,
            "record_version_boundary": null
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null
        }
    })
}

fn parse(value: Value) -> ResolutionTopology {
    serde_json::from_value(value).expect("fixture must be a typed topology")
}

fn basenames_policy(address: &str) -> ResolutionRoutePolicy {
    ResolutionRoutePolicy::Basenames {
        expected_transport: ResolutionTransportContract {
            source_chain_id: ChainId::BaseMainnet,
            target_chain_id: ChainId::EthereumMainnet,
            contract_address: EvmAddress::from_str(address)
                .expect("fixture transport must be an EVM address"),
        },
    }
}

#[test]
fn classifies_every_admitted_route() {
    let direct = parse(direct_topology());
    assert_eq!(
        direct.classify(NAME_ID, ResolutionRoutePolicy::Ens),
        Ok(ResolutionRoute::Direct)
    );

    let mut alias = direct_topology();
    alias["alias"] = json!({
        "final_target": { "logical_name_id": "ens:0xtarget" },
        "hops": [{ "logical_name_id": "ens:0xtarget" }]
    });
    assert_eq!(
        parse(alias).classify(NAME_ID, ResolutionRoutePolicy::Ens),
        Ok(ResolutionRoute::AliasOnly)
    );

    let mut wildcard = direct_topology();
    wildcard["resolver_path"][0]["logical_name_id"] = json!(WILDCARD_ID);
    wildcard["wildcard"] = json!({
        "source": { "logical_name_id": WILDCARD_ID },
        "matched_labels": ["alice"]
    });
    assert_eq!(
        parse(wildcard).classify(NAME_ID, ResolutionRoutePolicy::Ens),
        Ok(ResolutionRoute::WildcardDerived)
    );

    let mut basenames = direct_topology();
    basenames["resolver_path"][0]["namespace"] = json!("basenames");
    basenames["resolver_path"][0]["chain_id"] = json!("base-mainnet");
    basenames["transport"] = json!({
        "source_chain_id": "base-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": TRANSPORT,
        "latest_event_kind": null
    });
    assert_eq!(
        parse(basenames).classify(NAME_ID, basenames_policy(TRANSPORT)),
        Ok(ResolutionRoute::BasenamesTransportDirect)
    );
}

#[test]
fn rejects_the_invalid_alias_and_wildcard_states_both_old_classifiers_rejected() {
    let mut missing_alias_hops = direct_topology();
    missing_alias_hops["alias"] = json!({ "final_target": null });
    assert!(
        parse(missing_alias_hops)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );

    let mut disagreeing_alias = direct_topology();
    disagreeing_alias["alias"] = json!({
        "final_target": { "logical_name_id": "ens:0xtarget" },
        "hops": []
    });
    assert!(
        parse(disagreeing_alias)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );

    let mut source_without_labels = direct_topology();
    source_without_labels["wildcard"]["source"] = json!({
        "logical_name_id": WILDCARD_ID
    });
    assert!(
        parse(source_without_labels)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );

    let mut labels_without_source = direct_topology();
    labels_without_source["wildcard"]["matched_labels"] = json!(["alice"]);
    assert!(
        parse(labels_without_source)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );

    let mut wildcard_with_alias = direct_topology();
    wildcard_with_alias["resolver_path"][0]["logical_name_id"] = json!(WILDCARD_ID);
    wildcard_with_alias["wildcard"] = json!({
        "source": { "logical_name_id": WILDCARD_ID },
        "matched_labels": ["alice"]
    });
    wildcard_with_alias["alias"] = json!({
        "final_target": { "logical_name_id": "ens:0xtarget" },
        "hops": [{ "logical_name_id": "ens:0xtarget" }]
    });
    assert!(
        parse(wildcard_with_alias)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );
}

#[test]
fn expected_transport_contract_is_policy_input_not_embedded_classifier_state() {
    let mut topology = direct_topology();
    topology["resolver_path"][0]["namespace"] = json!("basenames");
    topology["resolver_path"][0]["chain_id"] = json!("base-mainnet");
    topology["transport"] = json!({
        "source_chain_id": "base-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": TRANSPORT,
        "latest_event_kind": null
    });
    let topology = parse(topology);

    assert_eq!(
        topology.classify(NAME_ID, basenames_policy(TRANSPORT)),
        Ok(ResolutionRoute::BasenamesTransportDirect)
    );
    assert!(
        topology
            .classify(
                NAME_ID,
                basenames_policy("0x0000000000000000000000000000000000000002"),
            )
            .is_err()
    );
}

#[test]
fn explicitly_unsupported_and_malformed_transport_topologies_never_classify() {
    let mut unsupported = direct_topology();
    unsupported["status"] = json!("unsupported");
    assert!(
        parse(unsupported)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );

    let mut extra_transport_policy = direct_topology();
    extra_transport_policy["transport"]["gateway"] = json!("https://example.test");
    assert!(
        parse(extra_transport_policy)
            .classify(NAME_ID, ResolutionRoutePolicy::Ens)
            .is_err()
    );
}

#[test]
fn typed_topology_rejects_unknown_chain_ids_and_malformed_evm_addresses() {
    let mut unknown_chain = direct_topology();
    unknown_chain["resolver_path"][0]["chain_id"] = json!("ethereum-future");
    assert!(serde_json::from_value::<ResolutionTopology>(unknown_chain).is_err());

    let mut malformed_address = direct_topology();
    malformed_address["resolver_path"][0]["address"] = json!("0x1234");
    assert!(serde_json::from_value::<ResolutionTopology>(malformed_address).is_err());
}

#[test]
fn serializer_canonicalizes_typed_addresses_without_reshaping_the_object() {
    let mut topology = direct_topology();
    topology["transport"] = json!({
        "source_chain_id": "base-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": TRANSPORT,
        "latest_event_kind": null
    });
    let serialized = serde_json::to_value(parse(topology)).expect("topology must serialize");
    assert_eq!(
        serialized["transport"]["contract_address"],
        "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31"
    );
    assert!(serialized["alias"]["final_target"].is_null());
    assert_eq!(serialized["wildcard"]["matched_labels"], json!([]));
}
