use bigname_storage::{
    VerifiedResolutionPathClass, classify_supported_resolution_topology,
    try_classify_supported_resolution_topology,
};
use serde_json::{Value, json};

const NAME_ID: &str = "ens:0xname";

fn topology() -> Value {
    json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": NAME_ID,
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

fn assert_classification(namespace: &str, topology: &Value, expected: VerifiedResolutionPathClass) {
    assert_eq!(
        classify_supported_resolution_topology(namespace, NAME_ID, topology),
        Some(expected)
    );
    assert_eq!(
        try_classify_supported_resolution_topology(namespace, NAME_ID, topology)
            .expect("strict storage adapter must accept the topology"),
        expected
    );
}

#[test]
fn storage_adapter_preserves_the_domain_route_matrix() {
    let direct = topology();
    assert_classification("ens", &direct, VerifiedResolutionPathClass::Direct);

    let mut alias = topology();
    alias["alias"] = json!({
        "final_target": { "logical_name_id": "ens:0xtarget" },
        "hops": [{ "logical_name_id": "ens:0xtarget" }]
    });
    assert_classification("ens", &alias, VerifiedResolutionPathClass::AliasOnly);

    let mut wildcard = topology();
    wildcard["resolver_path"][0]["logical_name_id"] = json!("ens:0xancestor");
    wildcard["wildcard"] = json!({
        "source": { "logical_name_id": "ens:0xancestor" },
        "matched_labels": ["alice"]
    });
    assert_classification(
        "ens",
        &wildcard,
        VerifiedResolutionPathClass::WildcardDerived,
    );

    let mut basenames = topology();
    basenames["resolver_path"][0]["chain_id"] = json!("base-mainnet");
    basenames["transport"] = json!({
        "source_chain_id": "base-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
        "latest_event_kind": null
    });
    assert_classification(
        "basenames",
        &basenames,
        VerifiedResolutionPathClass::BasenamesTransportDirect,
    );
}

#[test]
fn storage_option_and_strict_adapters_both_reject_out_of_class_topology() {
    assert_eq!(
        classify_supported_resolution_topology("unknown", NAME_ID, &topology()),
        None
    );
    assert!(try_classify_supported_resolution_topology("unknown", NAME_ID, &topology()).is_err());

    let mut wrong_transport = topology();
    wrong_transport["resolver_path"][0]["chain_id"] = json!("base-mainnet");
    wrong_transport["transport"] = json!({
        "source_chain_id": "base-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": "0x0000000000000000000000000000000000000002",
        "latest_event_kind": null
    });
    assert_eq!(
        classify_supported_resolution_topology("basenames", NAME_ID, &wrong_transport),
        None
    );
    assert!(
        try_classify_supported_resolution_topology("basenames", NAME_ID, &wrong_transport).is_err()
    );

    let mut malformed_resolver = topology();
    malformed_resolver["resolver_path"][0]["address"] = json!("not-an-address");
    assert_eq!(
        classify_supported_resolution_topology("ens", NAME_ID, &malformed_resolver),
        None
    );
    assert!(
        try_classify_supported_resolution_topology("ens", NAME_ID, &malformed_resolver).is_err()
    );
}
