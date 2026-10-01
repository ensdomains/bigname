//! A `.eth` name registered through the Sepolia wrapped controller with a resolver: the registry
//! `NewResolver` written after `NameWrapped` in the same transaction belongs to the wrapper
//! resource that `NameWrapped` bound, not to the registrar resource the NameWrapper holds.
use super::node_record_events::{declared_address, profile};
use super::*;

mod events {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
        event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    }
}

const OWNER: &str = "0x00000000000000000000000000000000000000a3";
const RESOLVER: &str = "0x8fade66b79cc9f707ab26799354482eb93a5b7dd";
const BLOCK: i64 = 4_052_977;
const REGISTRAR_EXPIRY: u64 = 1_900_000_000;
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`.
const DOT_ETH_FUSES: u32 = 0x10000 | 0x20000;

/// The wrapped controller's `register` calls `NameWrapper.registerAndWrapETH2LD`: the
/// BaseRegistrar mints to the NameWrapper, names it the registry owner and emits `NameRegistered`;
/// `_wrapETH2LD` mints the ERC-1155 token, emits `NameWrapped`, then sets the registry resolver.
/// (upstream: .refs/basenames/lib/ens-contracts/contracts/ethregistrar/ETHRegistrarController.sol:L178-L184 @ basenames@1809bbc)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1019 @ ens_v1@91c966f)
fn registration_logs(
    chain: &str,
    admissions: &[AddressAdmissionInput],
) -> anyhow::Result<Vec<RawLogInput>> {
    let registry = declared_address(admissions, "registry");
    let registrar = declared_address(admissions, "registrar");
    let wrapper = declared_address(admissions, "name_wrapper");
    let label = b"taytems";
    let labelhash = keccak256(label);
    let node: B256 = super::common::namehash(&["taytems".to_owned(), "eth".to_owned()]).parse()?;
    let token = U256::from_be_bytes(labelhash.0);
    let wrapper_address: Address = wrapper.parse()?;
    let mut logs = vec![
        raw_at(
            events::Transfer {
                from: Address::ZERO,
                to: wrapper_address,
                tokenId: token,
            }
            .encode_log_data(),
            BLOCK,
            0,
            &registrar,
        ),
        raw_at(
            events::NewOwner {
                node: super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner: wrapper_address,
            }
            .encode_log_data(),
            BLOCK,
            1,
            &registry,
        ),
        raw_at(
            events::NameRegistered {
                id: token,
                owner: wrapper_address,
                expires: U256::from(REGISTRAR_EXPIRY),
            }
            .encode_log_data(),
            BLOCK,
            2,
            &registrar,
        ),
        raw_at(
            events::TransferSingle {
                operator: wrapper_address,
                from: Address::ZERO,
                to: OWNER.parse()?,
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            BLOCK,
            3,
            &wrapper,
        ),
        raw_at(
            events::NameWrapped {
                node,
                name: b"\x07taytems\x03eth\0".to_vec().into(),
                owner: OWNER.parse()?,
                fuses: DOT_ETH_FUSES,
                expiry: REGISTRAR_EXPIRY + GRACE_PERIOD,
            }
            .encode_log_data(),
            BLOCK,
            4,
            &wrapper,
        ),
        raw_at(
            events::NewResolver {
                node,
                resolver: RESOLVER.parse()?,
            }
            .encode_log_data(),
            BLOCK,
            5,
            &registry,
        ),
    ];
    for log in &mut logs {
        log.chain_id = chain.to_owned();
    }
    Ok(logs)
}

/// The resource and effective state of every row the registry `NewResolver` produced, with the
/// wrapper and registrar resources of the registration.
type ResolverRows = (Uuid, Uuid, Vec<(String, Option<Uuid>, serde_json::Value)>);

fn resolver_rows(split_resolver_write: bool) -> anyhow::Result<ResolverRows> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let mut raw_logs = registration_logs(&chain, &admissions)?;
    if split_resolver_write {
        let write = raw_logs.last_mut().expect("resolver write");
        *write = RawLogInput {
            block_hash: format!("block-{}", BLOCK + 1),
            block_number: BLOCK + 1,
            block_timestamp: write.block_timestamp + time::Duration::seconds(12),
            transaction_hash: format!("transaction-{}", BLOCK + 1),
            log_index: 0,
            ..write.clone()
        };
    }
    let write_position = raw_logs
        .last()
        .map(|write| (write.block_number, write.log_index))
        .expect("resolver write");
    let output = interpret_test_batch(BatchInput {
        chain_id: chain,
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    })?;
    let resource_of = |kind: &str, family: &str| {
        output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == kind && event.source_family == family)
            .and_then(|event| event.resource_id)
    };
    let wrapper = resource_of("SurfaceBound", "ens_v1_wrapper_l1").expect("wrapper resource");
    let registrar =
        resource_of("RegistrationGranted", "ens_v1_registrar_l1").expect("registrar resource");
    let mut rows = output
        .normalized_events
        .iter()
        .filter(|event| {
            (event.block_number, event.log_index)
                == (Some(write_position.0), Some(write_position.1))
        })
        .map(|event| {
            let state = if event.event_kind == "PermissionChanged" {
                json!({
                    "scope": event.after_state["scope"],
                    "subject": event.after_state["subject"],
                    "effective_powers": event.after_state["effective_powers"],
                })
            } else {
                event.after_state["resolver"].clone()
            };
            (event.event_kind.clone(), event.resource_id, state)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| format!("{row:?}"));
    Ok((wrapper, registrar, rows))
}

/// taytems.eth on Sepolia (block 4052977): `NameWrapped` at log 21, registry `NewResolver` at log
/// 22. The resolver pointer must land on the wrapper resource the name is served from, exactly as
/// it does when the owner sets the resolver in a later transaction.
#[test]
fn registry_resolver_set_after_wrapping_stays_on_the_wrapper_resource() -> anyhow::Result<()> {
    let (wrapper, registrar, same_transaction) = resolver_rows(false)?;
    assert_ne!(wrapper, registrar);
    assert!(
        same_transaction
            .iter()
            .any(|(kind, resource, resolver)| kind == "ResolverChanged"
                && *resource == Some(wrapper)
                && *resolver == RESOLVER),
        "the registry resolver write stays on the wrapper resource {wrapper}: {same_transaction:?}"
    );
    let (_, _, later_transaction) = resolver_rows(true)?;
    assert_eq!(same_transaction, later_transaction);
    Ok(())
}
