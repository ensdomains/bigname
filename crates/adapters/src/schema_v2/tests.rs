use alloy_primitives::{Address, B256, U256, hex, keccak256};
use alloy_sol_types::{SolEvent, sol};
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;

const CHAIN: &str = "adapter-test";
const CONTRACT: &str = "0x0000000000000000000000000000000000000042";

sol! {
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires);
    event NameRenewed(string name, bytes32 indexed label, uint256 expires);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event ExpiryExtended(bytes32 indexed node, uint64 expiry);
    event FusesSet(bytes32 indexed node, uint32 fuses);
    event RegistryCreated();
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
}

mod v1_registrar {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }
}

mod raw_v1_registrar {
    use alloy_sol_types::sol;

    sol! {
        event RawNameRegistered(bytes name, bytes32 indexed label, address indexed owner, uint256 expires);
        event RawNameRenewed(bytes name, bytes32 indexed label, uint256 expires);
    }
}

mod v2_registry {
    use alloy_sol_types::sol;

    sol! {
        event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
        event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
        event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
        event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
        event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
        event ParentUpdated(address indexed parent, string label, address indexed sender);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
        event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
        event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    }
}

mod raw_v2_registry {
    use alloy_sol_types::sol;

    sol! {
        event RawLabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, bytes label, address owner, uint64 expiry, address indexed sender);
        event RawParentUpdated(address indexed parent, bytes label, address indexed sender);
    }
}

mod v2_resolver {
    use alloy_sol_types::sol;

    sol! {
        event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
        event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName);
        event NamedResource(uint256 indexed resource, bytes name);
        event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);
    }
}

mod resolver {
    use alloy_sol_types::sol;

    sol! {
        event AddrChanged(bytes32 indexed node, address a);
        event VersionChanged(bytes32 indexed node, uint64 newVersion);
    }
}

mod resolver_name {
    use alloy_sol_types::sol;

    sol! {
        event NameChanged(bytes32 indexed node, string name);
    }
}

mod resolver_strings {
    use alloy_sol_types::sol;

    sol! {
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
        event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData);
        event NameForAddrChanged(address indexed addr, string name);
        event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);
    }
}

mod raw_resolver_strings {
    use alloy_sol_types::sol;

    sol! {
        event RawNameChanged(bytes32 indexed node, bytes name);
        event RawTextChanged(bytes32 indexed node, bytes32 indexed indexedKey, bytes key, bytes value);
        event RawDataChanged(bytes32 indexed node, bytes32 indexed indexedKey, bytes key, bytes32 indexed indexedData);
        event RawNameForAddrChanged(address indexed addr, bytes name);
        event RawNamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, bytes key);
    }
}

mod legacy_text_without_value {
    use alloy_sol_types::sol;

    sol! {
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key);
    }
}

mod v1_registry {
    use alloy_sol_types::sol;

    sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event NewTTL(bytes32 indexed node, uint64 ttl);
    }
}

mod v2_registrar {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
    }
}

mod raw_v2_registrar {
    use alloy_sol_types::sol;

    sol! {
        event RawNameRegistered(uint256 indexed tokenId, bytes label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
        event RawNameRenewed(uint256 indexed tokenId, bytes label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    }
}

#[test]
fn wrapper_adapter_expands_the_manifest_wrapper_transition() -> anyhow::Result<()> {
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let encoded = NameWrapped {
        node: super::common::namehash(&labels).parse::<B256>()?,
        name: b"\x07wrapped\x03eth\0".to_vec().into(),
        owner: CONTRACT.parse::<Address>()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            4,
            "ens_v1_wrapper_l1",
            "NameWrapped",
            "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            &["name_wrapper"],
            &[
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "PreimageObserved",
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(4, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(kinds.contains("TokenControlTransferred"));
    assert!(kinds.contains("ExpiryChanged"));
    assert!(kinds.contains("PermissionScopeChanged"));
    assert!(kinds.contains("SurfaceBound"));
    assert!(kinds.contains("AuthorityEpochChanged"));
    assert!(kinds.contains("PreimageObserved"));
    assert_eq!(output.name_surfaces[0].raw_name, "wrapped.eth");
    Ok(())
}

#[test]
fn non_utf8_wrapper_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    assert_hostile_wrapper_label(vec![0xff], None)
}

#[test]
fn non_utf8_v1_registrar_string_payload_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe, 0xfd];
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    let encoded = with_topic0(
        raw_v1_registrar::RawNameRegistered {
            name: raw_label.clone().into(),
            label: labelhash,
            owner: CONTRACT.parse()?,
            expires: U256::from(42),
        }
        .encode_log_data(),
        NameRegistered::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            63,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(63, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn non_utf8_v2_registry_string_payload_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe];
    let token_id = U256::from_be_bytes(*keccak256(&raw_label));
    let encoded = with_topic0(
        raw_v2_registry::RawLabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(&raw_label),
            label: raw_label.clone().into(),
            owner: CONTRACT.parse()?,
            expiry: 42,
            sender: CONTRACT.parse()?,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            64,
            "ens_v2_registry_l1",
            "LabelRegistered",
            "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
            &["registry"],
            &["RegistrationGranted", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(64, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash =
        super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_label, None);
    Ok(())
}

#[test]
fn hostile_parent_update_retracts_a_bound_descendant_and_records_the_shadow_claim()
-> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000066";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let hostile_label = b"a\0b".to_vec();
    let manifest = manifest_with_events(
        66,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut child_admission = admission(66, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let hostile_parent_update = with_topic0(
        raw_v2_registry::RawParentUpdated {
            parent: CONTRACT.parse()?,
            label: hostile_label.clone().into(),
            sender,
        }
        .encode_log_data(),
        v2_registry::ParentUpdated::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 66,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(66, "registry"), child_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(7),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
            raw_at(hostile_parent_update, 6, 0, CHILD),
        ],
    })?;

    let active = output
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name == "leaf.sub.eth")
        .expect("the valid mutual claim first binds the descendant");
    assert!(output.binding_closures.iter().any(|closure| {
        closure.logical_name_id == active.logical_name_id && closure.block_number == 6
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "ParentChanged"
            && event.block_number == Some(6)
            && event.after_state["raw_label_hex"] == hex::encode(&hostile_label)
            && event.after_state["decoded_label"].is_null()
    }));
    let shadow_namehash =
        super::common::namehash_raw([hostile_label.as_slice(), b"eth"].into_iter());
    assert_shadow_output(&output, &shadow_namehash, &hostile_label, None);
    Ok(())
}

#[test]
fn announced_registry_hostile_registration_waits_for_suffix_authority() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000067";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let raw_label = b"a\0b".to_vec();
    let child_token = versioned_token_bytes(&raw_label, 1);
    let parent_token = versioned_token("sub", 1);
    let manifest = manifest_with_events(
        67,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut child_admission = admission(67, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let admissions = || vec![admission(67, "registry"), child_admission.clone()];
    let rules = || {
        vec![DiscoveryRuleInput {
            manifest_id: 67,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }]
    };
    let hostile_registration = with_topic0(
        raw_v2_registry::RawLabelRegistered {
            tokenId: child_token,
            labelHash: keccak256(&raw_label),
            label: raw_label.clone().into(),
            owner,
            expiry: 1_000,
            sender,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered::SIGNATURE_HASH,
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(hostile_registration, 1, 0, CHILD)],
    })?;

    assert!(
        first.name_surfaces.is_empty(),
        "an unattached registry cannot turn a token ID into a logical name"
    );
    assert!(first.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationGranted"
            && event.logical_name_id.is_none()
            && event.after_state["raw_label_hex"] == hex::encode(&raw_label)
            && event.after_state["decoded_label"].is_null()
    }));

    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CHILD,
            ),
        ],
    })?;
    let shadow_namehash = super::common::namehash_raw(
        [raw_label.as_slice(), b"sub".as_slice(), b"eth".as_slice()].into_iter(),
    );
    assert_shadow_output(&second, &shadow_namehash, &raw_label, None);
    assert!(second.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn non_utf8_v2_registrar_string_payload_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe];
    let token_id = U256::from_be_bytes(*keccak256(&raw_label));
    let encoded = with_topic0(
        raw_v2_registrar::RawNameRegistered {
            tokenId: token_id,
            label: raw_label.clone().into(),
            owner: CONTRACT.parse()?,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
            duration: 42,
            paymentToken: Address::ZERO,
            referrer: B256::ZERO,
            base: U256::ZERO,
            premium: U256::ZERO,
        }
        .encode_log_data(),
        v2_registrar::NameRegistered::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            65,
            "ens_v2_registrar_l1",
            "NameRegistered",
            "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium)",
            &["registrar"],
            &["RegistrarNameRegistered", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(65, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash =
        super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_label, None);
    Ok(())
}

#[test]
fn hostile_v1_registrar_label_retains_registration_renewal_and_transfer_lifecycle()
-> anyhow::Result<()> {
    let raw_label = b"a\0b".to_vec();
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let next_owner: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let registered = with_topic0(
        raw_v1_registrar::RawNameRegistered {
            name: raw_label.clone().into(),
            label: labelhash,
            owner,
            expires: U256::from(42),
        }
        .encode_log_data(),
        NameRegistered::SIGNATURE_HASH,
    );
    let renewed = with_topic0(
        raw_v1_registrar::RawNameRenewed {
            name: raw_label.clone().into(),
            label: labelhash,
            expires: U256::from(84),
        }
        .encode_log_data(),
        NameRenewed::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            68,
            "ens",
            "ens_v1_registrar_l1",
            &[
                (
                    "NameRegistered",
                    "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                    &["registrar"],
                    &["RegistrationGranted"],
                ),
                (
                    "Transfer",
                    "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                    &["registrar"],
                    &["TokenControlTransferred"],
                ),
                (
                    "NameRenewed",
                    "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
                    &["registrar"],
                    &["RegistrationRenewed"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(68, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(
                v1_registrar::Transfer {
                    from: owner,
                    to: next_owner,
                    tokenId: U256::from_be_bytes(*labelhash),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(renewed, 3, 0, CONTRACT),
        ],
    })?;

    for kind in [
        "RegistrationGranted",
        "TokenControlTransferred",
        "RegistrationRenewed",
    ] {
        assert!(
            output
                .normalized_events
                .iter()
                .any(|event| event.event_kind == kind),
            "missing hostile-label lifecycle event {kind}"
        );
    }
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationGranted" && event.after_state["decoded_label"].is_null()
    }));
    assert!(!output.resources.is_empty());
    assert!(!output.token_lineages.is_empty());
    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn normalization_changed_v1_label_remains_shadow_after_registry_authority_transition()
-> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000072";
    let raw_label = b"Alice".to_vec();
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let next_owner: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let registrar_manifest = manifest(
        72,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registrar_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(72, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            NameRegistered {
                name: "Alice".to_owned(),
                label: labelhash,
                owner,
                expires: U256::from(1_000),
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let first_surface = first
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == node)
        .expect("normalization-changed shadow identity");
    assert_eq!(first_surface.visibility_state, "shadow");
    assert!(first.surface_bindings.is_empty());
    assert!(first.label_preimages.iter().any(|preimage| {
        preimage.raw_label == raw_label
            && preimage.decoded_label.as_deref() == Some("Alice")
            && !preimage.normalized_under_version
    }));

    let registry_manifest = manifest(
        73,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &["SubregistryChanged", "AuthorityTransferred"],
    );
    let mut registry_admission = admission(73, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewOwner {
                node: super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner: next_owner,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    })?;
    let logical_name_id = format!("ens:{node}");
    assert!(
        second
            .surface_bindings
            .iter()
            .all(|binding| binding.logical_name_id != logical_name_id)
    );
    assert!(second.normalized_events.iter().all(|event| {
        event.event_kind != "SurfaceBound"
            || event.logical_name_id.as_deref() != Some(logical_name_id.as_str())
    }));
    Ok(())
}

#[test]
fn hostile_wrapper_label_retains_transfer_and_unwrap_authority_lifecycle() -> anyhow::Result<()> {
    let raw_label = vec![0xff];
    let dns_name = vec![1, 0xff, 3, b'e', b't', b'h', 0];
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let next_owner: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            69,
            "ens",
            "ens_v1_wrapper_l1",
            &[
                (
                    "NameWrapped",
                    "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
                (
                    "NameUnwrapped",
                    "event NameUnwrapped(bytes32 indexed node, address owner)",
                    &["name_wrapper"],
                    &["SurfaceUnbound"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(69, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameWrapped {
                    node: node.parse()?,
                    name: dns_name.into(),
                    owner,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferSingle {
                    operator: owner,
                    from: owner,
                    to: next_owner,
                    id: node.parse()?,
                    value: U256::from(1),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                NameUnwrapped {
                    node: node.parse()?,
                    owner: next_owner,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
        ],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "TokenControlTransferred")
            .count()
            >= 2
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "AuthorityEpochChanged"
            && event.after_state["source_event"] == "NameUnwrapped"
    }));
    assert!(!output.resources.is_empty());
    assert!(!output.token_lineages.is_empty());
    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn hostile_v2_registrar_label_retains_registration_and_renewal_events() -> anyhow::Result<()> {
    let raw_label = b"a\0b".to_vec();
    let token_id = versioned_token_bytes(&raw_label, 1);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let registered = with_topic0(
        raw_v2_registrar::RawNameRegistered {
            tokenId: token_id,
            label: raw_label.clone().into(),
            owner,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
            duration: 42,
            paymentToken: Address::ZERO,
            referrer: B256::ZERO,
            base: U256::ZERO,
            premium: U256::ZERO,
        }
        .encode_log_data(),
        v2_registrar::NameRegistered::SIGNATURE_HASH,
    );
    let renewed = with_topic0(
        raw_v2_registrar::RawNameRenewed {
            tokenId: token_id,
            label: raw_label.clone().into(),
            duration: 42,
            newExpiry: 84,
            paymentToken: Address::ZERO,
            referrer: B256::ZERO,
            amount: U256::ZERO,
        }
        .encode_log_data(),
        alloy_primitives::keccak256(
            b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
        ),
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            70,
            "ens",
            "ens_v2_registrar_l1",
            &[
                (
                    "NameRegistered",
                    "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium)",
                    &["registrar"],
                    &["RegistrarNameRegistered"],
                ),
                (
                    "NameRenewed",
                    "event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount)",
                    &["registrar"],
                    &["RegistrationRenewed"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(70, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(renewed, 2, 0, CONTRACT),
        ],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "RegistrarNameRegistered")
    );
    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "RegistrationRenewed")
    );
    assert!(output.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrarNameRegistered" | "RegistrationRenewed"
        ) || event.after_state["decoded_label"].is_null()
    }));
    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn embedded_dot_wrapper_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    assert_hostile_wrapper_label(b"a.b".to_vec(), Some("a.b"))
}

#[test]
fn embedded_nul_wrapper_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    assert_hostile_wrapper_label(b"a\0b".to_vec(), None)
}

#[test]
fn two_hundred_fifty_six_byte_registrar_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![b'a'; 256];
    let label = String::from_utf8(raw_label.clone())?;
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    let encoded = NameRegistered {
        name: label.clone(),
        label: keccak256(&raw_label),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            61,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(61, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_shadow_output(&output, &node, &raw_label, Some(&label));
    Ok(())
}

fn assert_hostile_wrapper_label(
    raw_label: Vec<u8>,
    decoded_label: Option<&str>,
) -> anyhow::Result<()> {
    let mut dns_name = Vec::with_capacity(raw_label.len() + 6);
    dns_name.push(u8::try_from(raw_label.len())?);
    dns_name.extend_from_slice(&raw_label);
    dns_name.extend_from_slice(b"\x03eth\0");
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    let encoded = NameWrapped {
        node: node.parse()?,
        name: dns_name.into(),
        owner: CONTRACT.parse()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            62,
            "ens_v1_wrapper_l1",
            "NameWrapped",
            "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            &["name_wrapper"],
            &["TokenControlTransferred", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(62, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_shadow_output(&output, &node, &raw_label, decoded_label);
    Ok(())
}

fn assert_shadow_output(
    output: &BatchOutput,
    namehash: &str,
    raw_label: &[u8],
    decoded_label: Option<&str>,
) {
    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == namehash)
        .expect("hostile label shadow identity");
    assert_eq!(surface.logical_name_id, format!("ens:{namehash}"));
    assert_eq!(surface.visibility_state, "shadow");
    assert_eq!(
        surface.deactivation_reason.as_deref(),
        Some("normalization_gate")
    );
    assert!(
        output
            .surface_bindings
            .iter()
            .all(|binding| binding.logical_name_id != surface.logical_name_id)
    );
    assert!(
        output
            .binding_closures
            .iter()
            .all(|closure| closure.logical_name_id != surface.logical_name_id)
    );
    let preimage = output
        .label_preimages
        .iter()
        .find(|preimage| preimage.raw_label == raw_label)
        .expect("hostile raw label preimage");
    assert_eq!(preimage.decoded_label.as_deref(), decoded_label);
    assert!(!preimage.normalized_under_version);
    assert!(preimage.normalization_error.is_some());
    let shadow = output
        .normalized_events
        .iter()
        .find(|event| event.after_state["visibility_state"] == "shadow")
        .expect("shadow identity observation");
    assert_eq!(
        shadow.logical_name_id.as_deref(),
        Some(surface.logical_name_id.as_str())
    );
    assert_eq!(shadow.after_state["namehash"], namehash);
    assert_eq!(
        shadow.after_state["logical_name_id"],
        format!("ens:{namehash}")
    );
}

#[test]
fn wrapper_incremental_updates_keep_identity_and_prior_state_across_batches() -> anyhow::Result<()>
{
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let manifest = manifest_with_events(
        56,
        "ens",
        "ens_v1_wrapper_l1",
        &[
            (
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                &["name_wrapper"],
                &[
                    "TokenControlTransferred",
                    "ExpiryChanged",
                    "PermissionScopeChanged",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "ExpiryExtended",
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
                &["name_wrapper"],
                &["ExpiryChanged"],
            ),
            (
                "FusesSet",
                "event FusesSet(bytes32 indexed node, uint32 fuses)",
                &["name_wrapper"],
                &["PermissionScopeChanged"],
            ),
        ],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            NameWrapped {
                node,
                name: b"\x07wrapped\x03eth\0".to_vec().into(),
                owner: CONTRACT.parse()?,
                fuses: 1,
                expiry: 42,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let wrapped_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "SurfaceBound")
        .and_then(|event| event.resource_id)
        .expect("wrapped resource");
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "name_wrapper")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                ExpiryExtended { node, expiry: 84 }.encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                FusesSet { node, fuses: 3 }.encode_log_data(),
                2,
                1,
                CONTRACT,
            ),
        ],
    })?;

    let expiry = second
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "ExpiryExtended")
        .expect("expiry extension");
    let fuses = second
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "FusesSet")
        .expect("fuse update");
    assert_eq!(expiry.resource_id, Some(wrapped_resource));
    assert_eq!(expiry.before_state["expiry"], 42);
    assert_eq!(fuses.resource_id, Some(wrapped_resource));
    assert_eq!(fuses.before_state["fuses"], 1);
    Ok(())
}

#[test]
fn registrar_adapter_emits_raw_namehash_identity_and_preimages() -> anyhow::Result<()> {
    let encoded = NameRegistered {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            1,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(1, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registrar event");
    assert!(
        event
            .logical_name_id
            .as_deref()
            .is_some_and(|id| id.starts_with("ens:0x"))
    );
    assert_eq!(output.name_surfaces[0].raw_labels, ["alice", "eth"]);
    assert_eq!(output.surface_bindings.len(), 1);
    assert_eq!(
        output.surface_bindings[0].binding_kind,
        "declared_registry_path"
    );
    let kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "RegistrationGranted",
        "ExpiryChanged",
        "PermissionChanged",
        "SurfaceBound",
        "AuthorityEpochChanged",
    ] {
        assert!(kinds.contains(expected), "missing {expected}");
    }
    assert_eq!(
        output
            .label_preimages
            .iter()
            .filter_map(|preimage| preimage.decoded_label.as_deref())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["alice", "eth"]),
    );
    Ok(())
}

#[test]
fn first_seen_renewal_synthesizes_the_retained_registration_anchor() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            60,
            "ens_v1_registrar_l1",
            "NameRenewed",
            "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
            &["registrar"],
            &[
                "RegistrationGranted",
                "RegistrationRenewed",
                "ExpiryChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(60, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(NameRenewed {
            name: "renewed".to_owned(),
            label: keccak256(b"renewed"),
            expires: U256::from(42),
        }
        .encode_log_data())],
    })?;

    let kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "RegistrationGranted",
        "RegistrationRenewed",
        "ExpiryChanged",
        "SurfaceBound",
        "AuthorityEpochChanged",
        "PreimageObserved",
    ] {
        assert!(kinds.contains(expected), "missing {expected}");
    }
    let grant = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("synthetic registration grant");
    assert_eq!(
        grant.after_state["registrant"],
        "0x0000000000000000000000000000000000000000"
    );
    assert_eq!(output.surface_bindings.len(), 1);
    Ok(())
}

#[test]
fn registry_created_emits_the_ruled_self_edge() -> anyhow::Result<()> {
    let encoded = RegistryCreated {}.encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            2,
            "ens_v2_registry_l1",
            "RegistryCreated",
            "event RegistryCreated()",
            &[],
            &["RegistryCreated"],
        )],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 2,
            edge_kind: "registry_announcement".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_eq!(output.discovery_edges.len(), 1);
    let edge = &output.discovery_edges[0];
    assert_eq!(edge.edge_kind, "registry_announcement");
    assert_eq!(edge.from_contract_instance_id, edge.to_contract_instance_id);
    assert_eq!(edge.active_from_block_number, 1);
    Ok(())
}

#[test]
fn announced_registry_prefers_a_same_namespace_declaring_manifest() -> anyhow::Result<()> {
    let event = (
        "RegistryCreated",
        "event RegistryCreated()",
        &["registry"][..],
        &["RegistryCreated"][..],
    );
    let mut announced = admission(65, "registry");
    announced.role = None;
    announced.discovery_edge_kind = Some("registry_announcement".to_owned());
    announced.discovery_from_contract_instance_id = Some(announced.contract_instance_id);
    announced.discovery_observation_key = Some("registry-announcement:fixture".to_owned());
    let mut declared = admission(66, "registry");
    declared.contract_instance_id = announced.contract_instance_id;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(65, "ens", "ens_v2_registry_l1", &[event]),
            manifest_with_events(66, "ens", "ens_v2_registry_l1", &[event]),
        ],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 65,
                edge_kind: "registry_announcement".to_owned(),
                from_role: None,
                admission: "event_announcement".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 66,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "declared_deployment".to_owned(),
            },
        ],
        admissions: vec![announced, declared],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(RegistryCreated {}.encode_log_data())],
    })?;

    let normalized = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistryCreated")
        .expect("registry announcement event");
    assert_eq!(normalized.source_manifest_id, Some(66));
    assert_eq!(output.discovery_edges[0].source_manifest_id, 66);
    Ok(())
}

#[test]
fn foreign_announcement_does_not_make_declared_admission_tie_match_all() -> anyhow::Result<()> {
    let resolver_event = (
        "AddrChanged",
        "event AddrChanged(bytes32 indexed node, address a)",
        &[][..],
        &["RecordChanged"][..],
    );
    let mut foreign_announcement = admission(68, "registry");
    foreign_announcement.role = None;
    foreign_announcement.discovery_edge_kind = Some("registry_announcement".to_owned());
    foreign_announcement.discovery_from_contract_instance_id =
        Some(foreign_announcement.contract_instance_id);
    foreign_announcement.discovery_observation_key =
        Some("registry-announcement:foreign".to_owned());
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(67, "ens", "ens_v1_resolver_l1", &[resolver_event]),
            manifest_with_events(
                68,
                "foreign",
                "ens_v2_registry_l1",
                &[(
                    "RegistryCreated",
                    "event RegistryCreated()",
                    &[][..],
                    &["RegistryCreated"][..],
                )],
            ),
            manifest_with_events(
                69,
                "basenames",
                "basenames_base_resolver",
                &[resolver_event],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(67, "resolver"), foreign_announcement],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(resolver::AddrChanged {
            node: B256::repeat_byte(0x67),
            a: CONTRACT.parse()?,
        }
        .encode_log_data())],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("declared resolver admission");
    assert_eq!(event.source_manifest_id, Some(67));
    assert_eq!(event.namespace, "ens");
    Ok(())
}

#[test]
fn registry_permission_adapter_selects_one_root_event() -> anyhow::Result<()> {
    let encoded = EACRolesChanged {
        resource: U256::ZERO,
        account: CONTRACT.parse::<Address>()?,
        oldRoleBitmap: U256::from(1),
        newRoleBitmap: U256::from(2),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            3,
            "ens_v2_registry_l1",
            "EACRolesChanged",
            "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
            &["registry"],
            &["PermissionChanged", "RootPermissionChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(3, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let permission_events = output
        .normalized_events
        .iter()
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "PermissionChanged" | "RootPermissionChanged"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(permission_events.len(), 1);
    assert_eq!(permission_events[0].event_kind, "RootPermissionChanged");
    assert_eq!(
        permission_events[0].before_state["role_bitmap"],
        json!(format!("{:#066x}", U256::from(1)))
    );
    Ok(())
}

#[test]
fn registrar_transfer_reuses_and_materializes_the_registration_resource() -> anyhow::Result<()> {
    let labelhash = keccak256(b"alice");
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: labelhash,
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let transfer = v1_registrar::Transfer {
        from: "0x0000000000000000000000000000000000000001".parse()?,
        to: "0x0000000000000000000000000000000000000002".parse()?,
        tokenId: U256::from_be_slice(labelhash.as_slice()),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            10,
            "ens",
            "ens_v1_registrar_l1",
            &[
                (
                    "NameRegistered",
                    "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                    &["registrar"],
                    &["RegistrationGranted"],
                ),
                (
                    "Transfer",
                    "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                    &["registrar"],
                    &["TokenControlTransferred"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(10, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration, 1, 0, CONTRACT),
            raw_at(transfer, 2, 0, CONTRACT),
        ],
    })?;
    let registered_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let transfer_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenControlTransferred")
        .and_then(|event| event.resource_id)
        .expect("transfer resource");

    assert_eq!(transfer_resource, registered_resource);
    assert!(
        output
            .resources
            .iter()
            .any(|resource| resource.resource_id == transfer_resource),
        "every normalized-event resource must be materialized for the schema foreign key"
    );
    Ok(())
}

#[test]
fn ens_v1_reregistration_mints_a_fresh_registrar_anchor() -> anyhow::Result<()> {
    let registration = || {
        NameRegistered {
            name: "alice".to_owned(),
            label: keccak256(b"alice"),
            owner: CONTRACT.parse::<Address>().unwrap(),
            expires: U256::from(42),
        }
        .encode_log_data()
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            28,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(28, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration(), 1, 0, CONTRACT),
            raw_at(registration(), 3, 0, CONTRACT),
        ],
    })?;

    let resources = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationGranted")
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    let lineages = output
        .resources
        .iter()
        .filter_map(|resource| resource.token_lineage_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(resources.len(), 2);
    assert_eq!(lineages.len(), 2);
    assert_eq!(output.surface_bindings.len(), 2);
    assert_ne!(
        output.surface_bindings[0].surface_binding_id,
        output.surface_bindings[1].surface_binding_id
    );
    Ok(())
}

#[test]
fn same_block_v1_releases_have_distinct_permission_event_identities() -> anyhow::Result<()> {
    let manifest = manifest(
        58,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let registration = |label: &str, log_index| {
        raw_at(
            NameRegistered {
                name: label.to_owned(),
                label: keccak256(label.as_bytes()),
                owner: CONTRACT.parse::<Address>().unwrap(),
                expires: U256::from(2),
            }
            .encode_log_data(),
            1,
            log_index,
            CONTRACT,
        )
    };
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(58, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![registration("alice", 0), registration("bob", 1)],
    })?;
    let boundary = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(58, "registrar")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "release-block".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10_000_000),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;

    let releases = boundary
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 2);
    assert_ne!(releases[0].logical_name_id, releases[1].logical_name_id);
    assert_ne!(releases[0].event_identity, releases[1].event_identity);
    Ok(())
}

#[test]
fn same_transaction_controller_setup_is_attributed_to_the_registration() -> anyhow::Result<()> {
    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let labels = vec![label.to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let eth_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let registry_address = "0x0000000000000000000000000000000000000043";
    let resolver_address = "0x0000000000000000000000000000000000000044";
    let transient_owner_address = "0x0000000000000000000000000000000000000045";
    let registration = NameRegistered {
        name: label.to_owned(),
        label: labelhash,
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let transient_owner = v1_registry::NewOwner {
        node: eth_node,
        label: labelhash,
        owner: transient_owner_address.parse()?,
    }
    .encode_log_data();
    let owner = v1_registry::NewOwner {
        node: eth_node,
        label: labelhash,
        owner: CONTRACT.parse()?,
    }
    .encode_log_data();
    let resolver = v1_registry::NewResolver {
        node,
        resolver: resolver_address.parse()?,
    }
    .encode_log_data();
    let registry = manifest_with_events(
        30,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            ),
        ],
    );
    let mut registry_admission = admission(30, "registry");
    registry_admission.address = registry_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            registry,
            manifest(
                31,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(31, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(transient_owner, 1, 0, registry_address),
            raw_at(resolver, 1, 1, registry_address),
            raw_at(owner, 1, 2, registry_address),
            raw_at(registration, 1, 3, CONTRACT),
        ],
    })?;

    let registrar_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    for kind in [
        "SubregistryChanged",
        "AuthorityTransferred",
        "ResolverChanged",
        "PermissionChanged",
    ] {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.event_kind == kind && event.resource_id == Some(registrar_resource)
            }),
            "missing linked {kind}"
        );
    }
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "AuthorityTransferred")
            .count(),
        1,
        "only the final same-transaction registry owner is authoritative"
    );
    Ok(())
}

#[test]
fn registry_owner_observation_mints_a_node_scoped_registry_authority() -> anyhow::Result<()> {
    let parent = B256::repeat_byte(0x61);
    let labelhash = keccak256(b"child");
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            57,
            "ens_v1_registry_l1",
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            &["registry"],
            &["SubregistryChanged", "AuthorityTransferred"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(57, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v1_registry::NewOwner {
            node: parent,
            label: labelhash,
            owner: "0x0000000000000000000000000000000000000061".parse()?,
        }
        .encode_log_data())],
    })?;

    let authority = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("registry-only authority resource");
    assert!(output.resources.iter().any(|resource| {
        resource.resource_id == authority && resource.token_lineage_id.is_none()
    }));
    Ok(())
}

#[test]
fn migrated_v1_node_ignores_later_old_registry_updates_across_batches() -> anyhow::Result<()> {
    const CURRENT: &str = "0x0000000000000000000000000000000000000062";
    const OLD: &str = "0x0000000000000000000000000000000000000063";
    let parent = B256::repeat_byte(0x62);
    let labelhash = keccak256(b"migrated");
    let manifest = manifest_with_events(
        58,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry", "registry_old"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            (
                "NewTTL",
                "event NewTTL(bytes32 indexed node, uint64 ttl)",
                &["registry", "registry_old"],
                &["AuthorityEpochChanged"],
            ),
        ],
    );
    let mut current = admission(58, "registry");
    current.address = CURRENT.to_owned();
    let mut old = admission(58, "registry_old");
    old.address = OLD.to_owned();
    old.contract_instance_id = Uuid::from_u128(580);
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![current.clone(), old.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewOwner {
                node: parent,
                label: labelhash,
                owner: "0x0000000000000000000000000000000000000064".parse()?,
            }
            .encode_log_data(),
            1,
            0,
            CURRENT,
        )],
    })?;
    let child = {
        let mut input = [0u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(labelhash.as_slice());
        B256::from(keccak256(input))
    };
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![current, old],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent,
                    label: labelhash,
                    owner: "0x0000000000000000000000000000000000000065".parse()?,
                }
                .encode_log_data(),
                2,
                0,
                OLD,
            ),
            raw_at(
                v1_registry::NewTTL {
                    node: child,
                    ttl: 60,
                }
                .encode_log_data(),
                2,
                1,
                OLD,
            ),
        ],
    })?;

    assert!(second.normalized_events.iter().all(|event| {
        !matches!(
            event.after_state["source_event"].as_str(),
            Some("NewOwner" | "NewTTL")
        )
    }));
    Ok(())
}

#[test]
fn same_block_binding_transitions_follow_log_order() -> anyhow::Result<()> {
    let labels = vec!["alice".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let wrapper_address = "0x0000000000000000000000000000000000000043";
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let wrapped = NameWrapped {
        node,
        name: b"\x05alice\x03eth\0".to_vec().into(),
        owner: CONTRACT.parse()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let mut wrapper_admission = admission(33, "name_wrapper");
    wrapper_admission.address = wrapper_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                32,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            manifest(
                33,
                "ens_v1_wrapper_l1",
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                &["name_wrapper"],
                &[
                    "TokenControlTransferred",
                    "ExpiryChanged",
                    "PermissionScopeChanged",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(32, "registrar"), wrapper_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration, 1, 0, CONTRACT),
            raw_at(wrapped, 1, 1, wrapper_address),
        ],
    })?;

    assert_eq!(output.surface_bindings.len(), 2);
    assert!(output.surface_bindings[0].active_from < output.surface_bindings[1].active_from);
    assert!(output.binding_closures.iter().any(|closure| {
        closure.log_index == 1 && closure.active_to == output.surface_bindings[1].active_from
    }));
    Ok(())
}

#[test]
fn ens_v1_registry_and_match_all_resolver_reuse_the_registered_authority() -> anyhow::Result<()> {
    let label = "alice";
    let labels = vec![label.to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let registry_address = "0x0000000000000000000000000000000000000043";
    let resolver_address = "0x0000000000000000000000000000000000000044";
    let registration = NameRegistered {
        name: label.to_owned(),
        label: keccak256(label.as_bytes()),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let resolver_change = v1_registry::NewResolver {
        node,
        resolver: resolver_address.parse()?,
    }
    .encode_log_data();
    let record = resolver::AddrChanged {
        node,
        a: "0x0000000000000000000000000000000000000045".parse()?,
    }
    .encode_log_data();
    let mut registry_admission = admission(22, "registry");
    registry_admission.address = registry_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                21,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            manifest(
                22,
                "ens_v1_registry_l1",
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            ),
            manifest(
                23,
                "ens_v1_resolver_l1",
                "AddrChanged",
                "event AddrChanged(bytes32 indexed node, address a)",
                &[],
                &["RecordChanged"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(21, "registrar"), registry_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration, 1, 0, CONTRACT),
            raw_at(resolver_change, 1, 1, registry_address),
            raw_at(record, 1, 2, resolver_address),
        ],
    })?;
    let resource_id = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    for kind in ["ResolverChanged", "PermissionChanged", "RecordChanged"] {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.event_kind == kind && event.resource_id == Some(resource_id)
            })
        );
    }
    Ok(())
}

#[test]
fn ens_v2_token_resource_unifies_registration_binding_and_permissions() -> anyhow::Result<()> {
    let label = "alice";
    let token_id = versioned_token(label, 1);
    let upstream_resource = U256::from(99);
    let registry = manifest_with_events(
        11,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &["registry"],
                &["PermissionChanged", "RootPermissionChanged"],
            ),
        ],
    );
    let registered = v2_registry::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        expiry: 42,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: upstream_resource,
    }
    .encode_log_data();
    let permission = EACRolesChanged {
        resource: upstream_resource,
        account: "0x0000000000000000000000000000000000000003".parse()?,
        oldRoleBitmap: U256::ZERO,
        newRoleBitmap: U256::from(1),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry],
        discovery_rules: Vec::new(),
        admissions: vec![admission(11, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(linked, 1, 1, CONTRACT),
            raw_at(permission, 1, 2, CONTRACT),
        ],
    })?;

    let linked_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("linked EAC resource");
    let permission_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged")
        .and_then(|event| event.resource_id)
        .expect("permission EAC resource");
    assert_eq!(linked_resource, permission_resource);
    assert_eq!(output.surface_bindings.len(), 1);
    assert_eq!(output.surface_bindings[0].resource_id, linked_resource);
    for kind in [
        "RegistrationGranted",
        "AuthorityTransferred",
        "ExpiryChanged",
    ] {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.event_kind == kind && event.resource_id == Some(linked_resource)
            }),
            "TokenResource must complete the resource-bound {kind} transition"
        );
    }
    assert!(
        output
            .resources
            .iter()
            .filter(|resource| resource.resource_id == linked_resource)
            .all(|resource| resource.token_lineage_id.is_some())
    );
    Ok(())
}

#[test]
fn ens_v2_registrar_intent_reuses_the_registry_eac_resource_without_rebinding() -> anyhow::Result<()>
{
    let label = "alice";
    let token_id = versioned_token(label, 1);
    let registrar_address = "0x0000000000000000000000000000000000000055";
    let registry = manifest_with_events(
        17,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let registrar = manifest_with_events(
        18,
        "ens",
        "ens_v2_registrar_l1",
        &[(
            "NameRegistered",
            "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium)",
            &["registrar"],
            &["RegistrarNameRegistered"],
        )],
    );
    let registered = v2_registry::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        expiry: 42,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: U256::from(99),
    }
    .encode_log_data();
    let intent = v2_registrar::NameRegistered {
        tokenId: token_id,
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        subregistry: Address::ZERO,
        resolver: Address::ZERO,
        duration: 100,
        paymentToken: Address::ZERO,
        referrer: B256::ZERO,
        base: U256::from(1),
        premium: U256::ZERO,
    }
    .encode_log_data();
    let mut registrar_admission = admission(18, "registrar");
    registrar_admission.address = registrar_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry, registrar],
        discovery_rules: Vec::new(),
        admissions: vec![admission(17, "registry"), registrar_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(linked, 1, 1, CONTRACT),
            raw_at(intent, 1, 2, registrar_address),
        ],
    })?;
    let resource = |kind: &str| {
        output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == kind)
            .and_then(|event| event.resource_id)
            .expect("linked normalized resource")
    };
    assert_eq!(
        resource("TokenResourceLinked"),
        resource("RegistrarNameRegistered")
    );
    assert_eq!(output.surface_bindings.len(), 1);
    Ok(())
}

#[test]
fn ens_v2_resource_and_lineage_survive_prior_state_and_token_regeneration() -> anyhow::Result<()> {
    let label = "alice";
    let old_token = versioned_token(label, 1);
    let new_token = versioned_token(label, 2);
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            19,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "LabelRegistered",
                    "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                    &["registry"],
                    &["RegistrationGranted"],
                ),
                (
                    "TokenResource",
                    "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                    &["registry"],
                    &["TokenResourceLinked"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(19, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: old_token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 42,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: old_token,
                    resource: U256::from(99),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let linked_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("linked resource");
    let prior_events = first
        .normalized_events
        .iter()
        .filter(|event| event.block_number.is_some())
        .map(|event| PriorEventInput {
            retained_state_key: seam::retained_prior_state_key(
                event
                    .raw_fact_ref
                    .get(seam::INTERPRETER_STATE_KEY)
                    .and_then(serde_json::Value::as_str),
                &event.event_identity,
            ),
            chain_id: event.chain_id.clone(),
            namespace: event.namespace.clone(),
            logical_name_id: event.logical_name_id.clone(),
            resource_id: event.resource_id,
            event_kind: event.event_kind.clone(),
            source_family: event.source_family.clone(),
            manifest_version: event.manifest_version,
            source_manifest_id: event.source_manifest_id,
            state_scope: event
                .raw_fact_ref
                .get("state_scope")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            block_timestamp: event
                .block_number
                .map(|number| OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number)),
            after_state: event.after_state.clone(),
        })
        .collect();
    let regenerated = v2_registry::TokenRegenerated {
        oldTokenId: old_token,
        newTokenId: new_token,
    }
    .encode_log_data();
    let transferred = v2_registry::TransferSingle {
        operator: "0x0000000000000000000000000000000000000001".parse()?,
        from: "0x0000000000000000000000000000000000000001".parse()?,
        to: "0x0000000000000000000000000000000000000002".parse()?,
        id: new_token,
        value: U256::from(1),
    }
    .encode_log_data();
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            19,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "TokenRegenerated",
                    "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                    &["registry"],
                    &["TokenRegenerated"],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["registry"],
                    &["TokenControlTransferred"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(19, "registry")],
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(regenerated, 2, 0, CONTRACT),
            raw_at(transferred, 2, 1, CONTRACT),
        ],
    })?;
    for kind in ["TokenRegenerated", "TokenControlTransferred"] {
        assert_eq!(
            second
                .normalized_events
                .iter()
                .find(|event| event.event_kind == kind)
                .and_then(|event| event.resource_id),
            Some(linked_resource),
        );
    }
    assert!(
        second
            .resources
            .iter()
            .filter(|resource| resource.resource_id == linked_resource)
            .all(|resource| resource.token_lineage_id.is_some())
    );
    assert_eq!(
        second
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "SurfaceBound")
            .count(),
        0,
        "restoring a live v2 name must not synthesize another binding at a batch boundary"
    );
    Ok(())
}

#[test]
fn ens_v2_prior_state_re_registration_discards_the_displaced_token() -> anyhow::Result<()> {
    let label = "alice";
    let old_token = versioned_token(label, 1);
    let new_token = versioned_token(label, 2);
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let manifest = manifest_with_events(
        57,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let registration = |token_id: U256, block_number: i64| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token_id,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: 100,
                sender,
            }
            .encode_log_data(),
            block_number,
            0,
            CONTRACT,
        )
    };
    let link = |token_id: U256, resource: u64, block_number: i64| {
        raw_at(
            v2_registry::TokenResource {
                tokenId: token_id,
                resource: U256::from(resource),
            }
            .encode_log_data(),
            block_number,
            1,
            CONTRACT,
        )
    };
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(57, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![registration(old_token, 1), link(old_token, 99, 1)],
    })?;
    let old_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("old resource");
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(57, "registry")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![registration(new_token, 2), link(new_token, 100, 2)],
    })?;
    let new_resource = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("new resource");
    assert_ne!(old_resource, new_resource);

    let third = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(57, "registry")],
        prior_events: first
            .normalized_events
            .iter()
            .chain(&second.normalized_events)
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![link(new_token, 100, 3)],
    })?;
    assert!(
        third
            .normalized_events
            .iter()
            .all(|event| event.resource_id != Some(old_resource))
    );
    assert!(
        third
            .resources
            .iter()
            .all(|resource| resource.resource_id != old_resource)
    );
    Ok(())
}

#[test]
fn ens_v2_expiry_extension_orders_the_old_unbind_before_the_new_binding() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000054";
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let manifest = manifest_with_events(
        54,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &["registry"],
                &["ExpiryChanged"],
            ),
        ],
    );
    let mut child_admission = admission(54, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = Uuid::from_u128(540);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(540));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 54,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(54, "registry"), child_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 7,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: parent_token,
                    resource: U256::from(99),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 100,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(100),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::ExpiryUpdated {
                    tokenId: parent_token,
                    newExpiry: 100,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                7,
                0,
                CONTRACT,
            ),
        ],
    })?;

    let boundary_kinds = output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(7))
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    let unbound = boundary_kinds
        .iter()
        .position(|kind| *kind == "SurfaceUnbound")
        .expect("expired surface is unbound before renewal");
    let rebound = boundary_kinds
        .iter()
        .rposition(|kind| *kind == "SurfaceBound")
        .expect("extended registration restores the surface");
    assert!(
        unbound < rebound,
        "the final topology state must remain bound"
    );
    Ok(())
}

#[test]
fn ens_v2_parent_expiry_retracts_descendant_at_the_block_boundary() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000059";
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let manifest = manifest_with_events(
        59,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &["registry"],
                &["ExpiryChanged", "RegistrationRenewed"],
            ),
        ],
    );
    let mut child_admission = admission(59, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = Uuid::from_u128(590);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(590));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 7,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner,
                    expiry: 100,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(100),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
        ],
    })?;
    let leaf = first
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name == "leaf.sub.eth")
        .expect("descendant surface before parent expiry");
    assert!(
        first
            .surface_bindings
            .iter()
            .any(|binding| binding.logical_name_id == leaf.logical_name_id)
    );

    let boundary = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission.clone()],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "expiry-boundary".to_owned(),
            block_number: 7,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(7),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    assert!(
        boundary
            .binding_closures
            .iter()
            .any(|closure| closure.logical_name_id == leaf.logical_name_id)
    );
    for kind in ["SurfaceUnbound", "RegistrationReleased"] {
        assert!(boundary.normalized_events.iter().any(|event| {
            event.event_kind == kind
                && event.logical_name_id.as_deref() == Some(&leaf.logical_name_id)
                && event.block_number == Some(7)
                && event.transaction_index.is_none()
        }));
    }
    let restored = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission],
        prior_events: first
            .normalized_events
            .iter()
            .chain(&boundary.normalized_events)
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::ExpiryUpdated {
                tokenId: parent_token,
                newExpiry: 100,
                sender,
            }
            .encode_log_data(),
            8,
            0,
            CONTRACT,
        )],
    })?;
    assert!(restored.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id.as_deref() == Some(&leaf.logical_name_id)
    }));
    Ok(())
}

#[test]
fn shadow_only_v2_descendant_expiry_is_a_non_binding_boundary() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000068";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let parent_token = versioned_token("sub", 1);
    let raw_label = b"a\0b".to_vec();
    let child_token = versioned_token_bytes(&raw_label, 1);
    let manifest = manifest_with_events(
        68,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut child_admission = admission(68, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let rules = || {
        vec![DiscoveryRuleInput {
            manifest_id: 68,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }]
    };
    let admissions = || vec![admission(68, "registry"), child_admission.clone()];
    let hostile_registration = with_topic0(
        raw_v2_registry::RawLabelRegistered {
            tokenId: child_token,
            labelHash: keccak256(&raw_label),
            label: raw_label.clone().into(),
            owner,
            expiry: 100,
            sender,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered::SIGNATURE_HASH,
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 7,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(hostile_registration, 2, 0, CHILD),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CHILD,
            ),
        ],
    })?;
    let shadow_namehash = super::common::namehash_raw(
        [raw_label.as_slice(), b"sub".as_slice(), b"eth".as_slice()].into_iter(),
    );
    assert_shadow_output(&first, &shadow_namehash, &raw_label, None);

    let boundary = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "shadow-expiry-boundary".to_owned(),
            block_number: 7,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(7),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    let shadow_id = format!("ens:{shadow_namehash}");
    assert!(
        boundary
            .binding_closures
            .iter()
            .all(|closure| closure.logical_name_id != shadow_id)
    );
    assert!(
        boundary
            .normalized_events
            .iter()
            .all(|event| { event.logical_name_id.as_deref() != Some(shadow_id.as_str()) })
    );
    Ok(())
}

#[test]
fn ens_v2_unregister_emits_a_surface_binding_closure() -> anyhow::Result<()> {
    let label = "terminal";
    let token_id = versioned_token(label, 1);
    let manifest = manifest_with_events(
        12,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["registry"],
                &["RegistrationReleased"],
            ),
        ],
    );
    let registered = v2_registry::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        expiry: 42,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: U256::from(123),
    }
    .encode_log_data();
    let released = v2_registry::LabelUnregistered {
        tokenId: token_id,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(12, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(linked, 1, 1, CONTRACT),
            raw_at(released, 2, 0, CONTRACT),
        ],
    })?;

    let binding = output.surface_bindings.first().expect("opened binding");
    assert!(
        output
            .binding_closures
            .iter()
            .any(|closure| closure.logical_name_id == binding.logical_name_id),
        "LabelUnregistered must close the active surface binding"
    );
    Ok(())
}

#[test]
fn ens_v2_unregister_closes_attached_topology_with_null_boundaries() -> anyhow::Result<()> {
    let label = "terminal";
    let token_id = versioned_token(label, 1);
    let target = "0x0000000000000000000000000000000000000066";
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            20,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "LabelRegistered",
                    "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                    &["registry"],
                    &["RegistrationGranted"],
                ),
                (
                    "TokenResource",
                    "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                    &["registry"],
                    &["TokenResourceLinked"],
                ),
                (
                    "SubregistryUpdated",
                    "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                    &["registry"],
                    &["SubregistryChanged"],
                ),
                (
                    "LabelUnregistered",
                    "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                    &["registry"],
                    &["RegistrationReleased"],
                ),
            ],
        )],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 20,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: vec![admission(20, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: token_id,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 42,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token_id,
                    resource: U256::from(123),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: token_id,
                    subregistry: target.parse()?,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                2,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelUnregistered {
                    tokenId: token_id,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
        ],
    })?;

    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceUnbound"
            && event.after_state["source_event"] == "LabelUnregistered"
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SubregistryChanged"
            && event.after_state["source_event"] == "LabelUnregistered"
            && event.after_state["subregistry"].is_null()
    }));
    Ok(())
}

#[test]
fn discovery_reconciliation_keys_are_scoped_per_registry_token() -> anyhow::Result<()> {
    let known_target = "0x0000000000000000000000000000000000000011";
    let known_target_id = Uuid::from_u128(1_313);
    let manifest = manifest_with_events(
        13,
        "ens",
        "ens_v2_registry_l1",
        &[(
            "SubregistryUpdated",
            "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
            &["registry"],
            &["SubregistryChanged"],
        )],
    );
    let update = |token_id, target: &str| {
        v2_registry::SubregistryUpdated {
            tokenId: token_id,
            subregistry: target.parse().unwrap(),
            sender: "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
        }
        .encode_log_data()
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 13,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: vec![
            admission(13, "registry"),
            AddressAdmissionInput {
                address: known_target.to_owned(),
                contract_instance_id: known_target_id,
                source_manifest_id: Some(13),
                role: Some("registry".to_owned()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            },
        ],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                update(versioned_token("alice", 1), known_target),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                update(
                    versioned_token("bob", 1),
                    "0x0000000000000000000000000000000000000022",
                ),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                update(
                    versioned_token("alice", 2),
                    "0x0000000000000000000000000000000000000033",
                ),
                2,
                0,
                CONTRACT,
            ),
        ],
    })?;
    let keys = output
        .discovery_edges
        .iter()
        .map(|edge| edge.provenance["observation_key"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 3);
    assert_ne!(keys[0], keys[1]);
    assert_eq!(keys[0], keys[2]);
    assert_eq!(
        output.discovery_edges[0].to_contract_instance_id,
        known_target_id
    );
    Ok(())
}

#[test]
fn ens_v2_topology_restatement_restores_against_the_transitioned_registry() -> anyhow::Result<()> {
    const PARENT: &str = "0x0000000000000000000000000000000000000062";
    const CHILD: &str = "0x0000000000000000000000000000000000000063";
    const CHILD_RESOLVER: &str = "0x0000000000000000000000000000000000000064";
    const PARENT_RESOLVER: &str = "0x0000000000000000000000000000000000000065";
    let token = versioned_token("same", 1);
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let manifest = manifest_with_events(
        62,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let rules = || {
        vec![
            DiscoveryRuleInput {
                manifest_id: 62,
                edge_kind: "resolver".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "protocol_event".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 62,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ]
    };
    let mut parent_admission = admission(62, "registry");
    parent_admission.address = PARENT.to_owned();
    parent_admission.contract_instance_id = Uuid::from_u128(620);
    let mut child_admission = admission(62, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = Uuid::from_u128(621);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(621));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let admissions = || vec![parent_admission.clone(), child_admission.clone()];
    let registration = |emitter: &str, block| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token,
                labelHash: keccak256(b"same"),
                label: "same".to_owned(),
                owner: sender,
                expiry: 100,
                sender,
            }
            .encode_log_data(),
            block,
            0,
            emitter,
        )
    };
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            registration(PARENT, 1),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token,
                    resource: U256::from(1),
                }
                .encode_log_data(),
                2,
                0,
                PARENT,
            ),
            registration(CHILD, 3),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token,
                    resource: U256::from(2),
                }
                .encode_log_data(),
                4,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: token,
                    resolver: CHILD_RESOLVER.parse()?,
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: PARENT.parse()?,
                    label: "same".to_owned(),
                    sender,
                }
                .encode_log_data(),
                6,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                7,
                0,
                PARENT,
            ),
        ],
    })?;
    let child_restatement = first
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "SubregistryUpdated"
        })
        .expect("parent topology change restates the child resolver");
    assert!(
        child_restatement.raw_fact_ref["state_scope"]
            .as_str()
            .is_some_and(|scope| scope.starts_with(&CHILD.to_ascii_lowercase()))
    );
    let parent_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked" && event.block_number == Some(2))
        .and_then(|event| event.resource_id)
        .expect("parent resource");

    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::ResolverUpdated {
                tokenId: token,
                resolver: PARENT_RESOLVER.parse()?,
                sender,
            }
            .encode_log_data(),
            8,
            0,
            PARENT,
        )],
    })?;
    let parent_update = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("parent resolver update");
    assert_eq!(parent_update.resource_id, Some(parent_resource));
    assert_ne!(
        parent_update
            .before_state
            .get("resolver")
            .and_then(serde_json::Value::as_str),
        Some(CHILD_RESOLVER)
    );
    Ok(())
}

#[test]
fn erc1155_transfers_require_positive_value_nonzero_endpoints_and_keep_lineage()
-> anyhow::Result<()> {
    let token_id = U256::from(7);
    let upstream_resource = U256::from(77);
    let manifest = manifest_with_events(
        14,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "TransferSingle",
                "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                &["registry"],
                &["TokenControlTransferred"],
            ),
            (
                "TransferBatch",
                "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                &["registry"],
                &["TokenControlTransferred"],
            ),
        ],
    );
    let operator: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let from: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let to: Address = "0x0000000000000000000000000000000000000003".parse()?;
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: upstream_resource,
    }
    .encode_log_data();
    let batch = v2_registry::TransferBatch {
        operator,
        from,
        to,
        ids: vec![token_id, token_id],
        values: vec![U256::ZERO, U256::from(1)],
    }
    .encode_log_data();
    let mint = v2_registry::TransferSingle {
        operator,
        from: Address::ZERO,
        to,
        id: token_id,
        value: U256::from(1),
    }
    .encode_log_data();
    let burn = v2_registry::TransferSingle {
        operator,
        from,
        to: Address::ZERO,
        id: token_id,
        value: U256::from(1),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(14, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(linked, 1, 0, CONTRACT),
            raw_at(batch, 1, 1, CONTRACT),
            raw_at(mint, 1, 2, CONTRACT),
            raw_at(burn, 1, 3, CONTRACT),
        ],
    })?;
    let transfers = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "TokenControlTransferred")
        .collect::<Vec<_>>();
    assert_eq!(transfers.len(), 1);
    let resource_id = transfers[0].resource_id.expect("transfer resource");
    assert!(
        output.resources.iter().any(|resource| {
            resource.resource_id == resource_id && resource.token_lineage_id.is_some()
        }),
        "a transfer must retain the linked resource's token lineage"
    );
    Ok(())
}

#[test]
fn resolver_before_state_is_scoped_by_emitter_node_and_selector() -> anyhow::Result<()> {
    let node = B256::repeat_byte(0x11);
    let emitter_one = "0x0000000000000000000000000000000000000011";
    let emitter_two = "0x0000000000000000000000000000000000000022";
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            15,
            "ens_v1_resolver_l1",
            "VersionChanged",
            "event VersionChanged(bytes32 indexed node, uint64 newVersion)",
            &[],
            &["RecordVersionChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                resolver::VersionChanged {
                    node,
                    newVersion: 1,
                }
                .encode_log_data(),
                1,
                0,
                emitter_one,
            ),
            raw_at(
                resolver::VersionChanged {
                    node,
                    newVersion: 2,
                }
                .encode_log_data(),
                1,
                1,
                emitter_two,
            ),
            raw_at(
                resolver::VersionChanged {
                    node,
                    newVersion: 3,
                }
                .encode_log_data(),
                2,
                0,
                emitter_one,
            ),
        ],
    })?;
    let events = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RecordVersionChanged")
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].before_state, json!({}));
    assert_eq!(events[1].before_state, json!({}));
    assert_eq!(events[2].before_state, events[0].after_state);
    Ok(())
}

#[test]
fn legacy_text_changed_without_value_uses_its_three_argument_decoder() -> anyhow::Result<()> {
    let encoded = legacy_text_without_value::TextChanged {
        node: B256::repeat_byte(0x33),
        indexedKey: keccak256(b"avatar"),
        key: "avatar".to_owned(),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            24,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("legacy text transition");
    assert_eq!(event.after_state["record_key"], "text:avatar");
    assert_eq!(event.after_state["selector_key"], "avatar");
    assert!(event.after_state.get("value").is_none());
    Ok(())
}

#[test]
fn ens_v1_unwrap_prior_state_reactivates_the_live_registrar_anchor() -> anyhow::Result<()> {
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let wrapper_address = "0x0000000000000000000000000000000000000043";
    let wrapper = manifest_with_events(
        25,
        "ens",
        "ens_v1_wrapper_l1",
        &[
            (
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                &["name_wrapper"],
                &[
                    "TokenControlTransferred",
                    "ExpiryChanged",
                    "PermissionScopeChanged",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "AuthorityEpochChanged"],
            ),
        ],
    );
    let mut wrapper_admission = admission(25, "name_wrapper");
    wrapper_admission.address = wrapper_address.to_owned();
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                24,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            wrapper,
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(24, "registrar"), wrapper_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameRegistered {
                    name: "wrapped".to_owned(),
                    label: keccak256(b"wrapped"),
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                NameWrapped {
                    node,
                    name: b"\x07wrapped\x03eth\0".to_vec().into(),
                    owner: CONTRACT.parse()?,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                2,
                0,
                wrapper_address,
            ),
            raw_at(
                NameUnwrapped {
                    node,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                3,
                0,
                wrapper_address,
            ),
        ],
    })?;
    let registrar_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar resource");
    assert!(first.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.block_number == Some(3)
            && event.resource_id == Some(registrar_resource)
    }));
    let prior_events = first
        .normalized_events
        .iter()
        .filter(|event| event.block_number.is_some())
        .map(prior_event)
        .collect();
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            26,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::AddrChanged {
                node,
                a: CONTRACT.parse()?,
            }
            .encode_log_data(),
            4,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;

    let record = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("resolver record");
    assert_eq!(record.resource_id, Some(registrar_resource));
    assert_eq!(
        record.logical_name_id.as_deref(),
        Some(format!("ens:{}", super::common::namehash(&labels)).as_str())
    );
    Ok(())
}

#[test]
fn released_registration_restores_registry_authority_across_batches() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000066";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000067";
    let parent_labels = vec!["eth".to_owned()];
    let parent = super::common::namehash(&parent_labels).parse::<B256>()?;
    let labelhash = keccak256(b"released");
    let child_labels = vec!["released".to_owned(), "eth".to_owned()];
    let child = super::common::namehash(&child_labels);
    let registry_manifest = manifest(
        61,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &["SubregistryChanged", "AuthorityTransferred"],
    );
    let registrar_manifest = manifest(
        62,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let mut registry_admission = admission(61, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest.clone(), registrar_manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission.clone(), admission(62, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent,
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                NameRegistered {
                    name: "released".to_owned(),
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{child}"));
    let registrar_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar authority");
    assert_ne!(registry_resource, registrar_resource);

    let released = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest, registrar_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(62, "registrar")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "release-block".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH
                + time::Duration::seconds(42 + 90 * 24 * 60 * 60 + 1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    assert!(released.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound" && event.resource_id == Some(registry_resource)
    }));

    let mut prior_events = first
        .normalized_events
        .iter()
        .chain(&released.normalized_events)
        .map(prior_event)
        .collect::<Vec<_>>();
    prior_events.sort_by_key(|event| event.block_timestamp);
    let linked = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            63,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::AddrChanged {
                node: child.parse()?,
                a: CONTRACT.parse()?,
            }
            .encode_log_data(),
            3,
            0,
            RESOLVER,
        )],
    })?;
    let record = linked
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("resolver record after release");
    assert_eq!(record.resource_id, Some(registry_resource));
    assert_eq!(
        record.logical_name_id.as_deref(),
        Some(format!("ens:{child}").as_str())
    );
    Ok(())
}

#[test]
fn shadow_v1_expiry_releases_resource_without_surface_boundary() -> anyhow::Result<()> {
    let raw_label = vec![0xff];
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let registered = with_topic0(
        raw_v1_registrar::RawNameRegistered {
            name: raw_label.clone().into(),
            label: labelhash,
            owner,
            expires: U256::from(42),
        }
        .encode_log_data(),
        NameRegistered::SIGNATURE_HASH,
    );
    let manifest = manifest(
        71,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(71, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(registered, 1, 0, CONTRACT)],
    })?;
    assert_shadow_output(&first, &node, &raw_label, None);

    let released = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(71, "registrar")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "shadow-v1-release".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH
                + time::Duration::seconds(42 + 90 * 24 * 60 * 60 + 1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    assert!(released.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationReleased"
            && event.logical_name_id.as_deref() == Some(format!("ens:{node}").as_str())
    }));
    assert!(released.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "SurfaceUnbound" | "SurfaceBound" | "AuthorityEpochChanged"
        )
    }));
    assert!(released.binding_closures.is_empty());
    assert!(released.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn lapsed_registration_after_registry_divergence_preserves_the_registry_anchor()
-> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000066";
    const DIVERGED_OWNER: &str = "0x0000000000000000000000000000000000000068";
    let parent = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let labelhash = keccak256(b"diverged");
    let child = super::common::namehash(&["diverged".to_owned(), "eth".to_owned()]);
    let registry_manifest = manifest(
        64,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &["SubregistryChanged", "AuthorityTransferred"],
    );
    let registrar_manifest = manifest(
        65,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let mut registry_admission = admission(64, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let admissions = vec![registry_admission.clone(), admission(65, "registrar")];
    let manifests = vec![registry_manifest.clone(), registrar_manifest.clone()];
    let registered = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent,
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                NameRegistered {
                    name: "diverged".to_owned(),
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let registrar_resource = registered
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar authority");
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{child}"));

    let diverged = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: registered
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewOwner {
                node: parent,
                label: labelhash,
                owner: DIVERGED_OWNER.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    })?;
    assert!(diverged.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound" && event.resource_id == Some(registry_resource)
    }));
    assert!(
        diverged
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "PermissionChanged"),
        "the divergence must perform the registrar-to-registry permission transition"
    );

    let mut prior_events = registered
        .normalized_events
        .iter()
        .chain(&diverged.normalized_events)
        .map(prior_event)
        .collect::<Vec<_>>();
    prior_events.sort_by_key(|event| event.block_timestamp);
    let lapsed = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events,
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "release-after-divergence".to_owned(),
            block_number: 3,
            block_timestamp: OffsetDateTime::UNIX_EPOCH
                + time::Duration::seconds(42 + 90 * 24 * 60 * 60 + 1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;

    let releases = lapsed
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationReleased")
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].resource_id, Some(registrar_resource));
    assert_eq!(
        releases[0].logical_name_id.as_deref(),
        Some(format!("ens:{child}").as_str())
    );
    assert!(lapsed.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "PermissionChanged" | "SurfaceUnbound" | "SurfaceBound" | "AuthorityEpochChanged"
        )
    }));
    assert!(lapsed.binding_closures.is_empty());
    assert!(lapsed.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn wrapper_transfers_fan_out_only_supported_moves_with_identity() -> anyhow::Result<()> {
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let token_id = U256::from_be_slice(node.as_slice());
    let operator: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let from: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let to: Address = "0x0000000000000000000000000000000000000003".parse()?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            27,
            "ens",
            "ens_v1_wrapper_l1",
            &[
                (
                    "NameWrapped",
                    "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                    &["name_wrapper"],
                    &[
                        "TokenControlTransferred",
                        "ExpiryChanged",
                        "PermissionScopeChanged",
                        "SurfaceBound",
                        "AuthorityEpochChanged",
                    ],
                ),
                (
                    "TransferBatch",
                    "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(27, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameWrapped {
                    node,
                    name: b"\x07wrapped\x03eth\0".to_vec().into(),
                    owner: from,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferBatch {
                    operator,
                    from,
                    to,
                    ids: vec![token_id, U256::from(999)],
                    values: vec![U256::from(1), U256::ZERO],
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferSingle {
                    operator,
                    from: Address::ZERO,
                    to,
                    id: token_id,
                    value: U256::from(1),
                }
                .encode_log_data(),
                1,
                2,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferSingle {
                    operator,
                    from,
                    to,
                    id: token_id,
                    value: U256::from(2),
                }
                .encode_log_data(),
                1,
                3,
                CONTRACT,
            ),
        ],
    })?;

    let transfers = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "TokenControlTransferred")
        .collect::<Vec<_>>();
    assert_eq!(transfers.len(), 2, "one wrap transition plus one transfer");
    let transfer = transfers
        .iter()
        .find(|event| event.after_state["source_event"] == "TransferBatch")
        .expect("supported batch transfer");
    assert!(transfer.logical_name_id.is_some());
    assert!(transfer.resource_id.is_some());
    Ok(())
}

#[test]
fn basenames_resolver_signatures_match_all_emitters() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            16,
            "basenames",
            "basenames_base_resolver",
            &[(
                "VersionChanged",
                "event VersionChanged(bytes32 indexed node, uint64 newVersion)",
                &[],
                &["RecordVersionChanged"],
            )],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::VersionChanged {
                node: B256::repeat_byte(0x22),
                newVersion: 1,
            }
            .encode_log_data(),
            1,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;

    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "RecordVersionChanged")
            .count(),
        1
    );
    Ok(())
}

#[test]
fn ens_v2_shared_resolver_signature_requires_address_admission() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            55,
            "ens_v2_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver_name::NameChanged {
                node: B256::repeat_byte(0x55),
                name: "unadmitted.eth".to_owned(),
            }
            .encode_log_data(),
            1,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "RecordChanged")
    );
    Ok(())
}

#[test]
fn equal_alias_endpoints_emit_one_name_preimage_observation() -> anyhow::Result<()> {
    let encoded_name = b"\x05alice\x03eth\0".to_vec();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            56,
            "ens_v2_resolver_l1",
            "AliasChanged",
            "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
            &[],
            &["AliasChanged", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v2_resolver::AliasChanged {
            indexedFromName: keccak256(&encoded_name),
            indexedToName: keccak256(&encoded_name),
            fromName: encoded_name.clone().into(),
            toName: encoded_name.into(),
        }
        .encode_log_data())],
    })?;

    assert_eq!(output.name_surfaces.len(), 1);
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "AliasChanged")
            .count(),
        1
    );
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "PreimageObserved")
            .count(),
        1
    );
    let identities = output
        .normalized_events
        .iter()
        .map(|event| &event.event_identity)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(identities.len(), output.normalized_events.len());
    Ok(())
}

#[test]
fn hostile_alias_endpoints_emit_shadow_preimages() -> anyhow::Result<()> {
    let from_label = vec![0xff];
    let to_label = b"a\0b".to_vec();
    let from_name = vec![1, 0xff, 3, b'e', b't', b'h', 0];
    let to_name = vec![3, b'a', 0, b'b', 3, b'e', b't', b'h', 0];
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            74,
            "ens_v2_resolver_l1",
            "AliasChanged",
            "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
            &[],
            &["AliasChanged", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(74, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v2_resolver::AliasChanged {
            indexedFromName: keccak256(&from_name),
            indexedToName: keccak256(&to_name),
            fromName: from_name.into(),
            toName: to_name.into(),
        }
        .encode_log_data())],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "AliasChanged")
    );
    for (raw_label, namehash) in [
        (
            from_label.as_slice(),
            super::common::namehash_raw([from_label.as_slice(), b"eth"].into_iter()),
        ),
        (
            to_label.as_slice(),
            super::common::namehash_raw([to_label.as_slice(), b"eth"].into_iter()),
        ),
    ] {
        assert!(output.name_surfaces.iter().any(|surface| {
            surface.namehash == namehash && surface.visibility_state == "shadow"
        }));
        assert!(
            output
                .label_preimages
                .iter()
                .any(|preimage| preimage.raw_label == raw_label)
        );
    }
    assert!(output.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn malformed_dns_wire_names_emit_no_normalized_identity_observation() -> anyhow::Result<()> {
    let malformed = vec![3, b'a', 0];
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            77,
            "ens",
            "ens_v2_resolver_l1",
            &[
                (
                    "AliasChanged",
                    "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
                    &[],
                    &["AliasChanged", "PreimageObserved"],
                ),
                (
                    "NamedResource",
                    "event NamedResource(uint256 indexed resource, bytes name)",
                    &[],
                    &["PreimageObserved"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(77, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_resolver::AliasChanged {
                    indexedFromName: keccak256(&malformed),
                    indexedToName: keccak256([]),
                    fromName: malformed.clone().into(),
                    toName: Vec::new().into(),
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_resolver::NamedResource {
                    resource: U256::from(1),
                    name: malformed.into(),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;

    assert!(output.normalized_events.is_empty());
    assert!(output.name_surfaces.is_empty());
    assert!(output.label_preimages.is_empty());
    Ok(())
}

#[test]
fn non_utf8_named_resource_emits_shadow_without_resolver_hint() -> anyhow::Result<()> {
    assert_named_resource_shadow_without_hint(vec![0xff], None)
}

#[test]
fn normalization_changed_named_resource_does_not_seed_resolver_hint() -> anyhow::Result<()> {
    assert_named_resource_shadow_without_hint(b"Alice".to_vec(), Some("Alice"))
}

#[test]
fn nul_named_text_key_remains_lossless_in_later_permission_hint() -> anyhow::Result<()> {
    let encoded_name = b"\x05alice\x03eth\0".to_vec();
    let raw_key = b"x\0y";
    let resource = U256::from(8);
    let manifest = manifest_with_events(
        76,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "NamedTextResource",
                "event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key)",
                &[],
                &["PreimageObserved"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
        ],
    );
    let named = with_topic0(
        raw_resolver_strings::RawNamedTextResource {
            resource,
            name: encoded_name.into(),
            keyHash: keccak256(raw_key),
            key: raw_key.to_vec().into(),
        }
        .encode_log_data(),
        v2_resolver::NamedTextResource::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(76, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(named, 1, 0, CONTRACT),
            raw_at(
                v2_resolver::EACRolesChanged {
                    resource,
                    account: CONTRACT.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;

    let permission = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged")
        .expect("permission using named-text hint");
    assert_eq!(
        permission.after_state["selector"]["key"],
        json!({"encoding":"hex","bytes":"0x780079"})
    );
    Ok(())
}

fn assert_named_resource_shadow_without_hint(
    raw_label: Vec<u8>,
    decoded_label: Option<&str>,
) -> anyhow::Result<()> {
    let mut encoded_name = vec![u8::try_from(raw_label.len())?];
    encoded_name.extend_from_slice(&raw_label);
    encoded_name.extend_from_slice(&[3, b'e', b't', b'h', 0]);
    let resource = U256::from(7);
    let manifest = manifest_with_events(
        75,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "NamedResource",
                "event NamedResource(uint256 indexed resource, bytes name)",
                &[],
                &["PreimageObserved"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
        ],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(75, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_resolver::NamedResource {
                resource,
                name: encoded_name.into(),
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let namehash = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let surface = first
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == namehash)
        .expect("named-resource shadow identity");
    assert_eq!(surface.visibility_state, "shadow");
    let preimage = first
        .label_preimages
        .iter()
        .find(|preimage| preimage.raw_label == raw_label)
        .expect("named-resource raw preimage");
    assert_eq!(preimage.decoded_label.as_deref(), decoded_label);
    assert!(!preimage.normalized_under_version);
    assert!(first.surface_bindings.is_empty());

    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(75, "resolver")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_resolver::EACRolesChanged {
                resource,
                account: CONTRACT.parse()?,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    })?;
    assert!(second.normalized_events.iter().any(|event| {
        event.event_kind == "PermissionChanged" && event.logical_name_id.is_none()
    }));
    assert!(second.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn malformed_match_all_lookalike_is_ignored() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            40,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: "transaction-1".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x0000000000000000000000000000000000000099".to_owned(),
            topics: vec![format!("{:#x}", resolver::AddrChanged::SIGNATURE_HASH)],
            data: vec![0x01],
        }],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "RecordChanged")
    );
    Ok(())
}

#[test]
fn detached_resolver_loses_address_scoped_admission_in_the_same_batch() -> anyhow::Result<()> {
    const OLD_RESOLVER: &str = "0x0000000000000000000000000000000000000050";
    const NEW_RESOLVER: &str = "0x0000000000000000000000000000000000000051";
    let registry_id = Uuid::from_u128(50);
    let old_resolver_id = Uuid::from_u128(51);
    let mut registry_admission = admission(50, "registry");
    registry_admission.contract_instance_id = registry_id;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                50,
                "ens_v2_registry_l1",
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            manifest(
                51,
                "ens_v2_resolver_l1",
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
        ],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 50,
            edge_kind: "resolver".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "protocol_event".to_owned(),
        }],
        admissions: vec![
            registry_admission,
            AddressAdmissionInput {
                address: OLD_RESOLVER.to_owned(),
                contract_instance_id: old_resolver_id,
                source_manifest_id: Some(50),
                role: None,
                discovery_edge_kind: Some("resolver".to_owned()),
                discovery_from_contract_instance_id: Some(registry_id),
                discovery_observation_key: Some(format!("resolver:{CONTRACT}:0x0")),
                active_from_block: Some(0),
                active_to_block: None,
            },
        ],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: U256::from(7),
                    resolver: NEW_RESOLVER.parse()?,
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_resolver::EACRolesChanged {
                    resource: U256::from(8),
                    account: CONTRACT.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1),
                }
                .encode_log_data(),
                1,
                1,
                OLD_RESOLVER,
            ),
        ],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "PermissionChanged"),
        "a resolver detached at an earlier log position must lose address-scoped admission"
    );
    Ok(())
}

#[test]
fn restored_reservation_does_not_become_a_registration_on_resource_link() -> anyhow::Result<()> {
    let manifest = manifest_with_events(
        52,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationReserved"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let token = U256::from(7);
    let reserved = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(52, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::LabelReserved {
                tokenId: token,
                labelHash: keccak256(b"reserved"),
                label: "reserved".to_owned(),
                expiry: 1_000,
                sender: CONTRACT.parse()?,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let linked = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(52, "registry")],
        prior_events: reserved.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::TokenResource {
                tokenId: token,
                resource: U256::from(8),
            }
            .encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    })?;

    assert!(linked.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "AuthorityTransferred"
        )
    }));
    Ok(())
}

#[test]
fn nul_text_value_is_retained_as_tagged_raw_bytes() -> anyhow::Result<()> {
    let key = b"url";
    let raw_value = b"a\0b";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x31),
            indexedKey: keccak256(key),
            key: key.to_vec().into(),
            value: raw_value.to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            53,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("NUL text value observation");
    assert_eq!(event.after_state["selector_key"], "url");
    assert_eq!(
        event.after_state["value"],
        json!({"encoding":"hex","bytes":"0x610062"})
    );
    Ok(())
}

#[test]
fn nul_text_key_hashes_raw_bytes_and_uses_a_lossless_selector() -> anyhow::Result<()> {
    let raw_key = b"x\0y";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x32),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"ok".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            54,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("NUL text key observation");
    assert_eq!(event.after_state["record_key"], "text_opaque:0x780079");
    assert_eq!(event.after_state["record_family"], "text_opaque");
    assert_eq!(event.after_state["selector_key"], "0x780079");
    assert_eq!(
        event.after_state["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x780079"})
    );
    assert_eq!(event.after_state["value"], "ok");
    Ok(())
}

#[test]
fn leading_tilde_text_key_remains_a_plain_projection_selector() -> anyhow::Result<()> {
    let raw_key = b"~url";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x34),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"ok".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            82,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("leading-tilde text key observation");
    assert_eq!(event.after_state["record_key"], "text:~url");
    assert_eq!(event.after_state["record_family"], "text");
    assert_eq!(event.after_state["selector_key"], "~url");
    assert!(event.after_state.get("raw_selector_key").is_none());
    Ok(())
}

#[test]
fn nul_v2_text_key_uses_a_projection_safe_opaque_selector() -> anyhow::Result<()> {
    let raw_key = b"v2\0key";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x35),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"ok".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            83,
            "ens_v2_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(83, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("ENSv2 NUL text key observation");
    assert_eq!(event.after_state["record_family"], "text_opaque");
    assert_eq!(event.after_state["selector_key"], "0x7632006b6579");
    assert_eq!(
        event.after_state["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x7632006b6579"})
    );
    Ok(())
}

#[test]
fn nul_data_key_uses_a_projection_safe_opaque_selector() -> anyhow::Result<()> {
    let raw_key = b"data\0key";
    let encoded = with_topic0(
        raw_resolver_strings::RawDataChanged {
            node: B256::repeat_byte(0x36),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            indexedData: B256::repeat_byte(0x37),
        }
        .encode_log_data(),
        resolver_strings::DataChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            84,
            "ens_v1_resolver_l1",
            "DataChanged",
            "event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("NUL data key observation");
    assert_eq!(event.after_state["record_family"], "data_opaque");
    assert_eq!(event.after_state["selector_key"], "0x64617461006b6579");
    assert_eq!(
        event.after_state["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x64617461006b6579"})
    );
    Ok(())
}

#[test]
fn nul_resolver_name_emits_a_shadow_with_exact_raw_preimage() -> anyhow::Result<()> {
    assert_hostile_resolver_name(b"bad\0name".to_vec(), 55)
}

#[test]
fn invalid_utf8_resolver_name_emits_no_lossy_fabricated_hash() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe];
    let output = hostile_resolver_name_output(raw_label.clone(), 56)?;
    let raw_namehash = super::common::namehash_raw([raw_label.as_slice()].into_iter());
    assert_shadow_output(&output, &raw_namehash, &raw_label, None);
    assert!(
        output
            .label_preimages
            .iter()
            .all(|preimage| preimage.raw_label != "��".as_bytes()),
        "lossy UTF-8 replacement bytes must never become a chain preimage"
    );
    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("invalid UTF-8 name record");
    assert_eq!(
        event.after_state["raw_name"],
        json!({"encoding":"hex","bytes":"0xfffe"})
    );
    Ok(())
}

#[test]
fn invalid_utf8_reverse_name_emits_exact_shadow_preimage() -> anyhow::Result<()> {
    let raw_name = vec![0xff];
    let encoded = with_topic0(
        raw_resolver_strings::RawNameForAddrChanged {
            addr: CONTRACT.parse()?,
            name: raw_name.clone().into(),
        }
        .encode_log_data(),
        resolver_strings::NameForAddrChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            78,
            "basenames_base_primary",
            "NameForAddrChanged",
            "event NameForAddrChanged(address indexed addr, string name)",
            &[],
            &["ReverseChanged", "RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(78, "reverse_registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash = super::common::namehash_raw([raw_name.as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_name, None);
    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("reverse name record");
    assert_eq!(record.after_state["raw_name"], json!(null));
    assert_eq!(
        record.after_state["raw_name_bytes"],
        json!({"encoding":"hex","bytes":"0xff"})
    );
    Ok(())
}

#[test]
fn nul_v2_resolver_name_emits_exact_shadow_preimage() -> anyhow::Result<()> {
    let raw_name = b"bad\0name".to_vec();
    let encoded = with_topic0(
        raw_resolver_strings::RawNameChanged {
            node: B256::repeat_byte(0x79),
            name: raw_name.clone().into(),
        }
        .encode_log_data(),
        resolver_name::NameChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            79,
            "ens_v2_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(79, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash = super::common::namehash_raw([raw_name.as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_name, None);
    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("ENSv2 hostile name record");
    assert_eq!(
        record.after_state["raw_name"],
        json!({"encoding":"hex","bytes":"0x626164006e616d65"})
    );
    Ok(())
}

#[test]
fn trailing_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b"alice.eth.", 85)
}

#[test]
fn leading_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b".alice.eth", 86)
}

#[test]
fn consecutive_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b"a..b", 87)
}

#[test]
fn bare_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b".", 88)
}

#[test]
fn empty_registrar_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    let raw_label = b"";
    let encoded = NameRegistered {
        name: String::new(),
        label: keccak256(raw_label),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            89,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(89, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    assert_empty_segment_shadow(&output, &namehash, ".eth");
    Ok(())
}

fn assert_empty_segment_resolver_name(raw_name: &[u8], manifest_id: i64) -> anyhow::Result<()> {
    let output = hostile_resolver_name_output(raw_name.to_vec(), manifest_id)?;
    let raw_labels = raw_name.split(|byte| *byte == b'.').collect::<Vec<_>>();
    let namehash = super::common::namehash_raw(raw_labels.iter().copied());
    assert_empty_segment_shadow(&output, &namehash, std::str::from_utf8(raw_name)?);
    Ok(())
}

fn assert_empty_segment_shadow(output: &BatchOutput, namehash: &str, raw_name: &str) {
    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == namehash)
        .expect("empty-segment shadow identity");
    assert_eq!(surface.visibility_state, "shadow");
    assert_eq!(surface.raw_name, raw_name);
    assert!(
        output
            .label_preimages
            .iter()
            .all(|preimage| !preimage.raw_label.is_empty()),
        "empty label content must not reach label_preimages"
    );
    assert!(
        surface.dns_encoded_name.is_empty(),
        "an empty label segment has no valid DNS wire encoding"
    );
}

fn assert_hostile_resolver_name(raw_name: Vec<u8>, manifest_id: i64) -> anyhow::Result<()> {
    let output = hostile_resolver_name_output(raw_name.clone(), manifest_id)?;
    let raw_labels = raw_name
        .split(|byte| *byte == b'.')
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let raw_namehash = super::common::namehash_raw(raw_labels.iter().map(Vec::as_slice));
    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == raw_namehash)
        .expect("hostile resolver-name shadow");
    assert_eq!(surface.visibility_state, "shadow");
    assert!(output.surface_bindings.is_empty());
    for raw_label in raw_labels {
        assert!(
            output
                .label_preimages
                .iter()
                .any(|preimage| preimage.raw_label == raw_label)
        );
    }
    Ok(())
}

fn hostile_resolver_name_output(
    raw_name: Vec<u8>,
    manifest_id: i64,
) -> anyhow::Result<BatchOutput> {
    let encoded = with_topic0(
        raw_resolver_strings::RawNameChanged {
            node: B256::repeat_byte(0x33),
            name: raw_name.into(),
        }
        .encode_log_data(),
        resolver_name::NameChanged::SIGNATURE_HASH,
    );
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            manifest_id,
            "ens_v1_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })
}

#[test]
fn announced_registry_gains_a_suffix_only_after_mutual_parent_agreement() -> anyhow::Result<()> {
    const PARENT: &str = "0x0000000000000000000000000000000000000060";
    const CHILD: &str = "0x0000000000000000000000000000000000000061";
    let parent_label = "sub";
    let child_label = "leaf";
    let parent_token = U256::from(1);
    let child_token = U256::from(2);
    let sender = CONTRACT.parse::<Address>()?;
    let manifest = manifest_with_events(
        41,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "RegistryCreated",
                "event RegistryCreated()",
                &["registry"],
                &["RegistryCreated"],
            ),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut parent_admission = admission(41, "registry");
    parent_admission.address = PARENT.to_owned();
    let logs = vec![
        raw_at(RegistryCreated {}.encode_log_data(), 1, 0, CHILD),
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: parent_token,
                labelHash: keccak256(parent_label.as_bytes()),
                label: parent_label.to_owned(),
                owner: sender,
                expiry: 1_000,
                sender,
            }
            .encode_log_data(),
            2,
            0,
            PARENT,
        ),
        raw_at(
            v2_registry::SubregistryUpdated {
                tokenId: parent_token,
                subregistry: CHILD.parse()?,
                sender,
            }
            .encode_log_data(),
            3,
            0,
            PARENT,
        ),
        raw_at(
            v2_registry::ParentUpdated {
                parent: PARENT.parse()?,
                label: parent_label.to_owned(),
                sender,
            }
            .encode_log_data(),
            4,
            0,
            CHILD,
        ),
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: child_token,
                labelHash: keccak256(child_label.as_bytes()),
                label: child_label.to_owned(),
                owner: sender,
                expiry: 1_000,
                sender,
            }
            .encode_log_data(),
            5,
            0,
            CHILD,
        ),
        raw_at(
            v2_registry::TokenResource {
                tokenId: child_token,
                resource: U256::from(7),
            }
            .encode_log_data(),
            6,
            0,
            CHILD,
        ),
    ];
    let mut later_logs = logs;
    let first_logs = later_logs.drain(..4).collect::<Vec<_>>();
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ],
        admissions: vec![parent_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: first_logs,
    })?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ],
        admissions: vec![
            parent_admission,
            AddressAdmissionInput {
                address: CHILD.to_owned(),
                contract_instance_id: super::common::contract_id(CHAIN, CHILD),
                source_manifest_id: Some(41),
                role: None,
                discovery_edge_kind: Some("registry_announcement".to_owned()),
                discovery_from_contract_instance_id: Some(super::common::contract_id(CHAIN, CHILD)),
                discovery_observation_key: Some(format!("registry-announcement:{CHILD}")),
                active_from_block: Some(1),
                active_to_block: None,
            },
        ],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: later_logs,
    })?;

    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.raw_name == "leaf.sub.eth")
    );
    assert!(output.surface_bindings.iter().any(|binding| {
        output.name_surfaces.iter().any(|surface| {
            surface.raw_name == "leaf.sub.eth" && surface.logical_name_id == binding.logical_name_id
        })
    }));
    Ok(())
}

#[test]
fn mutual_parent_changes_rebind_and_retract_existing_child_resources() -> anyhow::Result<()> {
    const PARENT: &str = "0x0000000000000000000000000000000000000070";
    const CHILD: &str = "0x0000000000000000000000000000000000000071";
    let sender = CONTRACT.parse::<Address>()?;
    let parent_token = U256::from(1);
    let child_token = U256::from(2);
    let manifest = manifest_with_events(
        42,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "RegistryCreated",
                "event RegistryCreated()",
                &["registry"],
                &["RegistryCreated"],
            ),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut parent_admission = admission(42, "registry");
    parent_admission.address = PARENT.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 42,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 42,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ],
        admissions: vec![parent_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(RegistryCreated {}.encode_log_data(), 1, 0, CHILD),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner: sender,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(7),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner: sender,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                PARENT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                PARENT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: PARENT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                6,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: Address::ZERO,
                    sender,
                }
                .encode_log_data(),
                7,
                0,
                PARENT,
            ),
        ],
    })?;

    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name == "leaf.sub.eth")
        .expect("mutual agreement binds the retained child resource");
    assert!(
        output
            .surface_bindings
            .iter()
            .any(|binding| binding.logical_name_id == surface.logical_name_id)
    );
    assert!(output.binding_closures.iter().any(|closure| {
        closure.logical_name_id == surface.logical_name_id && closure.block_number == 7
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id == Some(surface.logical_name_id.clone())
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceUnbound"
            && event.logical_name_id == Some(surface.logical_name_id.clone())
    }));
    Ok(())
}

#[cfg(feature = "legacy")]
#[test]
fn checked_in_manifest_event_corpus_has_typed_schema_v2_adapters() -> anyhow::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests");
    let mut manifest_id = 0i64;
    for environment in ["mainnet", "sepolia"] {
        let repository = bigname_manifests::load_repository(root.join(environment))?;
        for loaded in repository.manifests() {
            manifest_id += 1;
            let manifest = &loaded.manifest;
            let source = super::manifest::decode(ManifestInput {
                manifest_id,
                manifest_version: i64::try_from(manifest.manifest_version)?,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                chain_id: manifest.chain.clone(),
                deployment_label: manifest.deployment_epoch.clone(),
                normalizer_version: manifest.normalizer_version.clone(),
                payload_json: serde_json::to_string(manifest)?,
            })?;
            super::protocol::validate_manifest(&source)?;
        }
    }
    Ok(())
}

#[test]
fn schema_v2_flags_require_the_compiled_manifest_normalizer_version() {
    let mut source = manifest(
        99,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    source.normalizer_version = "ensip15@different".to_owned();

    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![source],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: Vec::new(),
    })
    .expect_err("the flag engine must reject a differently versioned manifest");

    assert!(error.to_string().contains("schema-v2 label flags use"));
}

#[test]
fn raw_log_without_a_loaded_block_errors_loudly() -> anyhow::Result<()> {
    let encoded = NameRegistered {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let input = BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            63,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(63, "registrar")],
        prior_events: Vec::new(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: vec![raw_at(encoded, 2, 0, CONTRACT)],
    };

    let error = super::interpret_schema_v2_batch(input)
        .expect_err("an unanchored raw log must not be interpreted out of position");
    assert!(
        error
            .to_string()
            .contains("no matching loaded live-lineage block")
    );
    Ok(())
}

#[test]
fn multiple_live_hashes_at_one_height_error_loudly() {
    let error = super::interpret_schema_v2_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            64,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: vec![
            RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-a".to_owned(),
                block_number: 1,
                block_timestamp: OffsetDateTime::UNIX_EPOCH,
                canonicality_state: "canonical".to_owned(),
            },
            RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-b".to_owned(),
                block_number: 1,
                block_timestamp: OffsetDateTime::UNIX_EPOCH,
                canonicality_state: "safe".to_owned(),
            },
        ],
        raw_logs: Vec::new(),
    })
    .expect_err("one height cannot have multiple live hashes");
    assert!(error.to_string().contains("multiple live-lineage hashes"));
}

fn manifest(
    manifest_id: i64,
    source_family: &str,
    name: &str,
    fragment: &str,
    emitter_roles: &[&str],
    normalized_events: &[&str],
) -> ManifestInput {
    manifest_with_events(
        manifest_id,
        "ens",
        source_family,
        &[(name, fragment, emitter_roles, normalized_events)],
    )
}

fn interpret_test_batch(mut input: BatchInput) -> anyhow::Result<BatchOutput> {
    if input.blocks.is_empty() {
        let mut blocks = std::collections::BTreeMap::new();
        for raw in &input.raw_logs {
            blocks
                .entry((raw.block_number, raw.block_hash.clone()))
                .or_insert_with(|| RawBlockInput {
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: raw.block_number,
                    block_timestamp: raw.block_timestamp,
                    canonicality_state: raw.canonicality_state.clone(),
                });
        }
        input.blocks = blocks.into_values().collect();
    }
    super::interpret_schema_v2_batch(input)
}

type EventSpec<'a> = (&'a str, &'a str, &'a [&'a str], &'a [&'a str]);

fn manifest_with_events(
    manifest_id: i64,
    namespace: &str,
    source_family: &str,
    events: &[EventSpec<'_>],
) -> ManifestInput {
    let events = events
        .iter()
        .map(|(name, fragment, emitter_roles, normalized_events)| {
            json!({
                "name": name,
                "fragment": fragment,
                "emitter_roles": emitter_roles,
                "normalized_events": normalized_events,
            })
        })
        .collect::<Vec<_>>();
    ManifestInput {
        manifest_id,
        manifest_version: 1,
        namespace: namespace.to_owned(),
        source_family: source_family.to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: "fixture".to_owned(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        payload_json: json!({"abi": {"events": events}}).to_string(),
    }
}

fn admission(manifest_id: i64, role: &str) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: CONTRACT.to_owned(),
        contract_instance_id: Uuid::from_u128(u128::try_from(manifest_id).unwrap()),
        source_manifest_id: Some(manifest_id),
        role: Some(role.to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn raw(encoded: alloy_primitives::LogData) -> RawLogInput {
    raw_at(encoded, 1, 0, CONTRACT)
}

fn with_topic0(mut encoded: alloy_primitives::LogData, topic0: B256) -> alloy_primitives::LogData {
    encoded.topics_mut()[0] = topic0;
    encoded
}

fn raw_at(
    encoded: alloy_primitives::LogData,
    block_number: i64,
    log_index: i64,
    emitting_address: &str,
) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block_number),
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("transaction-{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: emitting_address.to_owned(),
        topics: encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: encoded.data.to_vec(),
    }
}

fn versioned_token(label: &str, version: u32) -> U256 {
    versioned_token_bytes(label.as_bytes(), version)
}

fn versioned_token_bytes(label: &[u8], version: u32) -> U256 {
    let mut bytes = *keccak256(label);
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}

fn prior_event(event: &NormalizedEvent) -> PriorEventInput {
    PriorEventInput {
        retained_state_key: seam::retained_prior_state_key(
            event
                .raw_fact_ref
                .get(seam::INTERPRETER_STATE_KEY)
                .and_then(serde_json::Value::as_str),
            &event.event_identity,
        ),
        chain_id: event.chain_id.clone(),
        namespace: event.namespace.clone(),
        logical_name_id: event.logical_name_id.clone(),
        resource_id: event.resource_id,
        event_kind: event.event_kind.clone(),
        source_family: event.source_family.clone(),
        manifest_version: event.manifest_version,
        source_manifest_id: event.source_manifest_id,
        state_scope: event
            .raw_fact_ref
            .get("state_scope")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        block_timestamp: event
            .block_number
            .map(|number| OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number)),
        after_state: event.after_state.clone(),
    }
}
