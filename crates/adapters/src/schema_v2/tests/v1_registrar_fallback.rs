//! The BaseRegistrar's own `NameRegistered` / `NameRenewed` as the fallback
//! source of `.eth` registration and expiry facts when no admitted controller
//! event carries the label in the same transaction.

use super::*;

const REGISTRAR: &str = "0x00000000000000000000000000000000000000c2";
const CONTROLLER: &str = "0x00000000000000000000000000000000000000c3";
const OWNER: &str = "0x00000000000000000000000000000000000000c4";
const MANIFEST_ID: i64 = 7141;
const LABEL: &[u8] = b"rotated";
const GRACE: i64 = 90 * 24 * 60 * 60;

mod registrar_lifecycle {
    use alloy_sol_types::sol;
    sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NameRenewed(uint256 indexed id, uint256 expires);
    }
}

fn manifest() -> ManifestInput {
    manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                &["TokenControlTransferred", "PermissionChanged"],
            ),
            (
                "NameRegistered",
                "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationReleased",
                    "RegistrationGranted",
                    "ExpiryChanged",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
            (
                "NameRenewed",
                "event NameRenewed(uint256 indexed id, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationGranted",
                    "RegistrationRenewed",
                    "ExpiryChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
            (
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["legacy_registrar_controller"],
                &[
                    "RegistrationGranted",
                    "ExpiryChanged",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                    "PreimageObserved",
                ],
            ),
        ],
    )
}

fn admissions() -> Vec<AddressAdmissionInput> {
    let at = |role: &str, address: &str, instance: u128| AddressAdmissionInput {
        address: address.to_owned(),
        contract_instance_id: Uuid::from_u128(instance),
        source_manifest_id: Some(MANIFEST_ID),
        role: Some(role.to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    };
    vec![
        at("registrar", REGISTRAR, 71),
        at("legacy_registrar_controller", CONTROLLER, 72),
    ]
}

fn token() -> U256 {
    U256::from_be_bytes(keccak256(LABEL).0)
}

fn registrar_registered(block: i64, log_index: i64, expiry: i64) -> RawLogInput {
    raw_at(
        registrar_lifecycle::NameRegistered {
            id: token(),
            owner: OWNER.parse().unwrap(),
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRAR,
    )
}

fn registrar_renewed(block: i64, log_index: i64, expiry: i64) -> RawLogInput {
    raw_at(
        registrar_lifecycle::NameRenewed {
            id: token(),
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRAR,
    )
}

fn controller_registered(block: i64, log_index: i64, expiry: i64) -> RawLogInput {
    raw_at(
        with_topic0(
            raw_v1_registrar::RawNameRegistered {
                name: LABEL.to_vec().into(),
                label: keccak256(LABEL),
                owner: OWNER.parse().unwrap(),
                expires: U256::from(expiry),
            }
            .encode_log_data(),
            NameRegistered::SIGNATURE_HASH,
        ),
        block,
        log_index,
        CONTROLLER,
    )
}

fn empty_block(block_number: i64) -> RawBlockInput {
    RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block_number),
        canonicality_state: "canonical".to_owned(),
    }
}

fn interpret(raw_logs: Vec<RawLogInput>, extra_blocks: &[i64]) -> anyhow::Result<BatchOutput> {
    let mut blocks = raw_logs
        .iter()
        .map(|raw| raw.block_number)
        .chain(extra_blocks.iter().copied())
        .collect::<Vec<_>>();
    blocks.sort_unstable();
    blocks.dedup();
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest()],
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: blocks.into_iter().map(empty_block).collect(),
        raw_logs,
    })
}

fn kinds<'a>(output: &'a BatchOutput, kind: &str) -> Vec<&'a NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == kind)
        .collect()
}

#[test]
fn an_admitted_controller_event_in_the_transaction_claims_the_registrar_log() -> anyhow::Result<()>
{
    // The registrar emits first, the controller second, in one transaction.
    let output = interpret(
        vec![
            registrar_registered(10, 0, 1_000),
            controller_registered(10, 1, 1_000),
        ],
        &[],
    )?;
    let grants = kinds(&output, "RegistrationGranted");
    assert_eq!(grants.len(), 1, "{:#?}", output.normalized_events);
    assert_eq!(grants[0].log_index, Some(1));
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.after_state.get("controller_admitted").is_none()),
        "{:#?}",
        output.normalized_events
    );
    Ok(())
}

#[test]
fn a_registration_through_an_unadmitted_controller_is_served_flagged_without_a_label()
-> anyhow::Result<()> {
    let output = interpret(vec![registrar_registered(10, 0, 1_000)], &[])?;
    let grants = kinds(&output, "RegistrationGranted");
    assert_eq!(grants.len(), 1, "{:#?}", output.normalized_events);
    let grant = grants[0];
    assert_eq!(grant.source_family, "ens_v1_registrar_l1");
    assert_eq!(grant.after_state["controller_admitted"], false);
    assert_eq!(grant.after_state["surface_known"], false);
    assert_eq!(grant.after_state["registrant"], OWNER);
    assert_eq!(grant.after_state["expiry"], 1_000);
    assert!(grant.after_state.get("raw_label_hex").is_none());
    assert!(grant.logical_name_id.is_none());
    let resource = grant
        .resource_id
        .expect("the fact is carried by a resource");
    assert!(
        output
            .resources
            .iter()
            .any(|row| row.resource_id == resource)
    );
    assert!(output.name_surfaces.is_empty());
    assert_eq!(kinds(&output, "ExpiryChanged").len(), 1);
    Ok(())
}

#[test]
fn a_renewal_through_an_unadmitted_controller_keeps_the_expiry_current() -> anyhow::Result<()> {
    let first_release = 1_000 + GRACE + 1;
    let output = interpret(
        vec![
            registrar_registered(10, 0, 1_000),
            controller_registered(10, 1, 1_000),
            registrar_renewed(20, 0, 20_000_000),
        ],
        &[first_release],
    )?;
    let renewals = kinds(&output, "RegistrationRenewed");
    assert_eq!(renewals.len(), 1, "{:#?}", output.normalized_events);
    assert_eq!(renewals[0].block_number, Some(20));
    assert_eq!(renewals[0].after_state["controller_admitted"], false);
    assert_eq!(renewals[0].after_state["expiry"], 20_000_000);
    assert_eq!(renewals[0].before_state["expiry"], 1_000);
    assert!(
        renewals[0].logical_name_id.is_some(),
        "the admitted registration materialized the surface the renewal links to"
    );
    assert!(
        kinds(&output, "RegistrationReleased").is_empty(),
        "the refreshed expiry must not settle a release at the stale boundary: {:#?}",
        output.normalized_events
    );
    Ok(())
}

#[test]
fn without_the_fallback_renewal_the_stale_expiry_settles_a_release() -> anyhow::Result<()> {
    // The control for the test above: the same boundary with no renewal releases.
    let first_release = 1_000 + GRACE + 1;
    let output = interpret(
        vec![
            registrar_registered(10, 0, 1_000),
            controller_registered(10, 1, 1_000),
        ],
        &[first_release],
    )?;
    let releases = kinds(&output, "RegistrationReleased");
    assert_eq!(releases.len(), 1, "{:#?}", output.normalized_events);
    assert_eq!(releases[0].block_number, Some(first_release));
    Ok(())
}

#[test]
fn a_renewal_of_a_registration_this_family_does_not_hold_is_not_invented() -> anyhow::Result<()> {
    let output = interpret(vec![registrar_renewed(20, 0, 20_000_000)], &[])?;
    assert!(
        output.normalized_events.is_empty(),
        "{:#?}",
        output.normalized_events
    );
    assert!(output.resources.is_empty());
    Ok(())
}
