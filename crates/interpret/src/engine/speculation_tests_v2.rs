use alloy_primitives::{Address, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use sqlx::PgPool;

use super::support::*;
use crate::{RunMode, StateLoader};

const V2_CHAIN: &str = "ethereum-sepolia";
const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const ROOT_REGISTRY: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
const SUBREGISTRY: &str = "0x0000000000000000000000000000000000000772";
const ROLES: &str = "97409655027181761882228017414928043062435250176";

sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event RegistryCreated();
    event ParentUpdated(address indexed parent, string label, address indexed sender);
}

fn token(label: &str) -> U256 {
    let mut bytes = keccak256(label.as_bytes()).0;
    bytes[28..].fill(0);
    U256::from_be_bytes(bytes)
}

fn register(registry: &'static str, label: &str) -> TestResult<Vec<(&'static str, LogData)>> {
    let owner: Address = OWNER.parse()?;
    let id = token(label);
    Ok(vec![
        (
            registry,
            LabelRegistered {
                tokenId: id,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: u64::try_from(START + 10 * GRACE)?,
                sender: owner,
            }
            .encode_log_data(),
        ),
        (
            registry,
            TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            registry,
            TokenResource {
                tokenId: id,
                resource: id,
            }
            .encode_log_data(),
        ),
        (
            registry,
            EACRolesChanged {
                resource: id,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: ROLES.parse()?,
            }
            .encode_log_data(),
        ),
    ])
}

async fn topology_history(pool: &PgPool) -> TestResult {
    let owner: Address = OWNER.parse()?;
    let mut first = vec![(
        ETH_REGISTRY,
        ParentUpdated {
            parent: ROOT_REGISTRY.parse()?,
            label: "eth".to_owned(),
            sender: owner,
        }
        .encode_log_data(),
    )];
    first.extend(register(ETH_REGISTRY, "parent")?);
    first.push((
        ETH_REGISTRY,
        SubregistryUpdated {
            tokenId: token("parent"),
            subregistry: SUBREGISTRY.parse()?,
            sender: owner,
        }
        .encode_log_data(),
    ));
    seed_block(pool, V2_CHAIN, 0, 0, first).await?;
    let mut children = vec![
        (SUBREGISTRY, RegistryCreated {}.encode_log_data()),
        (
            SUBREGISTRY,
            ParentUpdated {
                parent: ETH_REGISTRY.parse()?,
                label: "parent".to_owned(),
                sender: owner,
            }
            .encode_log_data(),
        ),
    ];
    children.extend(register(SUBREGISTRY, "leaf")?);
    seed_block(pool, V2_CHAIN, 1, 12, children).await?;
    for (offset, subregistry) in [(2, Address::ZERO), (3, SUBREGISTRY.parse()?)] {
        seed_block(
            pool,
            V2_CHAIN,
            offset,
            offset * 12,
            vec![(
                ETH_REGISTRY,
                SubregistryUpdated {
                    tokenId: token("parent"),
                    subregistry,
                    sender: owner,
                }
                .encode_log_data(),
            )],
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_ancestor_changes_refresh_quiet_children_and_match_full_state() -> TestResult {
    let serial = database("interpret_spec_v2_serial", "sepolia").await?;
    let parallel = database("interpret_spec_v2_parallel", "sepolia").await?;
    for pool in [serial.pool(), parallel.pool()] {
        topology_history(pool).await?;
    }
    let full_state = engine(serial.pool(), 1, 1).with_full_state_loader_forced(true);
    complete(&full_state, V2_CHAIN, FIRST, FIRST + 3, RunMode::Normal).await?;
    let speculative = engine(parallel.pool(), 4, 1);
    complete(&speculative, V2_CHAIN, FIRST, FIRST + 3, RunMode::Normal).await?;
    let stats = speculative.speculation_stats();
    assert!(
        stats.accepted > 0 && stats.retried > 0,
        "V2 topology did not exercise acceptance and invalidation: {stats:?}"
    );
    assert_eq!(
        stats.fallback, 0,
        "V2 topology unexpectedly required full-state fallback: {stats:?}"
    );
    assert_eq!(
        speculative.chosen_loader(V2_CHAIN)?,
        Some(StateLoader::Lookahead)
    );
    let actual = snapshot(parallel.pool(), V2_CHAIN).await?;
    let leaf = keccak256([node("parent").as_slice(), keccak256(b"leaf").as_slice()].concat());
    let identity = format!("ens:{leaf:#x}");
    let child_events = events(&actual)
        .into_iter()
        .filter(|row| row["logical_name_id"] == identity)
        .collect::<Vec<_>>();
    for (offset, kind) in [
        (1, "RegistrationGranted"),
        (2, "RegistrationReleased"),
        (3, "RegistrationGranted"),
    ] {
        assert!(
            child_events
                .iter()
                .any(|row| row["block_number"] == FIRST + offset && row["event_kind"] == kind),
            "missing child {kind} at block offset {offset}: {child_events:?}"
        );
    }
    assert_snapshots(&actual, &snapshot(serial.pool(), V2_CHAIN).await?);
    drop(full_state);
    drop(speculative);
    serial.cleanup().await?;
    parallel.cleanup().await?;
    Ok(())
}
