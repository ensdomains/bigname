//! Actual producer ABI fixtures for the Oct 1 locked wrapper model.
//! Initialization binds the immutable fallback node; mounting the registry elsewhere does not
//! change it. These fixtures execute admission, interpretation, Project and public readers.
#[path = "wrapper/alias.rs"]
mod alias;
#[path = "wrapper/getter.rs"]
mod getter;
#[path = "wrapper/physical.rs"]
mod physical;
use super::*;
use alloy_primitives::B256;
const V1: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const LOCKED: &str = "0x6029a063d69b09d23c52a754a90e4fe43adac3a8";
const FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const IMPL: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const REGISTRY: &str = "0x0000000000000000000000000000000000001051";
const GRAVE: &str = "0xb58a90a39d13cce1d0e192b5da5c47640855b04d";
const NEW_PARENT: &str = "rebound105.eth";
const NEW_CHILD: &str = "child.rebound105.eth";
const WRAPPED_EXPIRY: u64 = 1_907_776_000;
sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event RegistryCreated();
    event ParentUpdated(address indexed parent, string label, address indexed sender);
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}
fn roles(bits: &[usize]) -> U256 {
    bits.iter()
        .fold(U256::ZERO, |v, b| v | (U256::from(1) << b))
}
fn wrapped(name: &str, owner: Address, fuses: u32) -> Result<alloy_primitives::LogData> {
    Ok(NameWrapped {
        node: bigname_lookup::ens_namehash_hex(name)?.parse()?,
        name: bigname_domain::normalization::normalize_name(name)?
            .dns_encoded_name
            .into(),
        owner,
        fuses,
        expiry: WRAPPED_EXPIRY,
    }
    .encode_log_data())
}
async fn wrapped_setup() -> Result<(TestDatabase, Address)> {
    let (database, mut logs, resolver) = setup().await?;
    logs.retain(|log| log.block_number < BASE + 122);
    let v1 = V1.parse()?;
    let wrapper = WRAPPER.parse()?;
    let owner = HOLDER.parse()?;
    let child_owner = GRANTEE.parse()?;
    let eth = B256::from_str(&bigname_lookup::ens_namehash_hex("eth")?)?;
    let parent = B256::from_str(&bigname_lookup::ens_namehash_hex(NAME)?)?;
    let child = B256::from_str(&bigname_lookup::ens_namehash_hex(CHILD)?)?;
    let base = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85".parse()?;
    logs.extend(transaction(
        120,
        5,
        vec![
            (
                base,
                Transfer {
                    from: owner,
                    to: wrapper,
                    tokenId: U256::from_be_bytes(*keccak256(LABEL)),
                }
                .encode_log_data(),
            ),
            (
                v1,
                NewOwner {
                    node: eth,
                    label: keccak256(LABEL),
                    owner: wrapper,
                }
                .encode_log_data(),
            ),
            (
                wrapper,
                TransferSingle {
                    operator: owner,
                    from: Address::ZERO,
                    to: owner,
                    id: U256::from_be_bytes(parent.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (wrapper, wrapped(NAME, owner, 1 | 65536 | 131072)?),
        ],
    ));
    // setSubnodeRecord makes ENS ownership the wrapper and mints an emancipated child. The
    // child does not burn CANNOT_UNWRAP and can unwrap without losing the stored parent fuse.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L590-L627 @ ens_v1@91c966f)
    let original = logs
        .iter_mut()
        .find(|log| {
            log.block_number == BASE + 121 && log.transaction_index == 0 && log.log_index == 0
        })
        .context("child creation")?;
    let data = NewOwner {
        node: parent,
        label: keccak256("child"),
        owner: wrapper,
    }
    .encode_log_data();
    let replacement = emitted(data, v1, 121, original.log_index);
    original.topics = replacement.topics;
    original.data = replacement.data;
    logs.extend(
        transaction(
            121,
            0,
            vec![
                (
                    wrapper,
                    TransferSingle {
                        operator: owner,
                        from: Address::ZERO,
                        to: child_owner,
                        id: U256::from_be_bytes(child.0),
                        value: U256::from(1),
                    }
                    .encode_log_data(),
                ),
                (wrapper, wrapped(CHILD, child_owner, 65536)?),
            ],
        )
        .into_iter()
        .map(|mut log| {
            log.log_index += 90;
            log
        }),
    );
    logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    let mut previous = None;
    let mut index = 0;
    for log in &mut logs {
        if previous != Some(log.block_number) {
            previous = Some(log.block_number);
            index = 0;
        }
        log.log_index = index;
        index += 1;
    }
    seed_and_run(&database, &logs, 120, 121).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let locked = LOCKED.parse()?;
    let grave = GRAVE.parse()?;
    let registry = REGISTRY.parse()?;
    let eth_registry = ETH.parse()?;
    let implementation = IMPL.parse()?;
    // LockedWrapperReceiver transfers the wrapped parent to the graveyard, deploys and
    // initializes a wrapper registry, then consumes the reserved ETH entry in the same call.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L138-L177 @ ens_v2_sepolia_20261001@07e55a05)
    let migration = transaction(
        122,
        0,
        vec![
            (
                wrapper,
                TransferSingle {
                    operator: owner,
                    from: owner,
                    to: locked,
                    id: U256::from_be_bytes(parent.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                v1,
                NewResolver {
                    node: parent,
                    resolver: Address::ZERO,
                }
                .encode_log_data(),
            ),
            (
                wrapper,
                TransferSingle {
                    operator: locked,
                    from: locked,
                    to: grave,
                    id: U256::from_be_bytes(parent.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (registry, Upgraded { implementation }.encode_log_data()),
            (registry, RegistryCreated {}.encode_log_data()),
            (
                registry,
                ParentUpdated {
                    parent: eth_registry,
                    label: LABEL.into(),
                    sender: Address::ZERO,
                }
                .encode_log_data(),
            ),
            (
                registry,
                EACRolesChanged {
                    resource: U256::ZERO,
                    account: eth_registry,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: roles(&[0, 16, 120, 124, 128, 144, 248, 252]),
                }
                .encode_log_data(),
            ),
            (
                FACTORY.parse()?,
                ProxyDeployed {
                    sender: locked,
                    proxyAddress: registry,
                    salt: U256::from_be_bytes(parent.0),
                    implementation,
                }
                .encode_log_data(),
            ),
            (
                eth_registry,
                LabelRegistered {
                    tokenId: token(0),
                    labelHash: keccak256(LABEL),
                    label: LABEL.into(),
                    owner,
                    expiry: RESERVATION_EXPIRY,
                    sender: locked,
                }
                .encode_log_data(),
            ),
            (
                eth_registry,
                TransferSingle {
                    operator: locked,
                    from: Address::ZERO,
                    to: owner,
                    id: token(0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                eth_registry,
                TokenResource {
                    tokenId: token(0),
                    resource: token(0),
                }
                .encode_log_data(),
            ),
            (
                eth_registry,
                EACRolesChanged {
                    resource: token(0),
                    account: owner,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: roles(&[24, 32, 152, 156]),
                }
                .encode_log_data(),
            ),
            (
                eth_registry,
                SubregistryUpdated {
                    tokenId: token(0),
                    subregistry: registry,
                    sender: locked,
                }
                .encode_log_data(),
            ),
        ],
    );
    seed_and_run(&database, &migration, 122, 122).await?;
    let migration:Value=sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_kind='MigrationApplied' AND consumer_visibility='activated'").fetch_one(&database.pool).await?;
    assert_eq!(
        migration["migration_path"], "locked_wrapped",
        "{migration:#}"
    );
    assert_consumers(&database, true, resolver, None).await?;
    Ok((database, resolver))
}

async fn unwrapped_rebound() -> Result<(TestDatabase, Address)> {
    let (database, resolver) = wrapped_setup().await?;
    let owner = HOLDER.parse()?;
    let child_owner = GRANTEE.parse()?;
    let v1 = V1.parse()?;
    let parent = bigname_lookup::ens_namehash_hex(NEW_PARENT)?.parse()?;
    let child = bigname_lookup::ens_namehash_hex(NEW_CHILD)?.parse()?;
    let eth = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
    let mut rebound = transaction(
        123,
        0,
        vec![
            (
                v1,
                NewOwner {
                    node: eth,
                    label: keccak256("rebound105"),
                    owner,
                }
                .encode_log_data(),
            ),
            (
                v1,
                NewOwner {
                    node: parent,
                    label: keccak256("child"),
                    owner: child_owner,
                }
                .encode_log_data(),
            ),
            (
                v1,
                NewResolver {
                    node: child,
                    resolver,
                }
                .encode_log_data(),
            ),
        ],
    );
    rebound.extend(transaction(
        123,
        1,
        vec![(
            resolver,
            NameChanged {
                node: parent,
                name: NEW_PARENT.into(),
            }
            .encode_log_data(),
        )],
    ));
    rebound.extend(transaction(
        123,
        2,
        vec![
            (
                resolver,
                NameChanged {
                    node: child,
                    name: NEW_CHILD.into(),
                }
                .encode_log_data(),
            ),
            (
                resolver,
                AddressChanged {
                    node: child,
                    coinType: U256::from(60),
                    newAddress: child_owner.to_vec().into(),
                }
                .encode_log_data(),
            ),
        ],
    ));
    rebound.extend(transaction(
        123,
        3,
        register(
            ETH.parse()?,
            "rebound105",
            Address::ZERO,
            REGISTRY.parse()?,
            u64::MAX,
            DEPLOYER.parse()?,
        )?,
    ));
    seed_and_run_with(
        &database,
        &rebound,
        123,
        123,
        &[(123, 2, GRANTEE), (123, 3, DEPLOYER)],
        None,
    )
    .await?;
    let detail = path_get(&database, &format!("/v1/names/{NEW_CHILD}")).await?;
    assert_eq!(
        detail["data"]["primary_address"], GRANTEE,
        "fallback must query the rebound node, never the original child records: {detail:#}"
    );
    let original: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let unwrapped = transaction(
        124,
        0,
        vec![
            (
                WRAPPER.parse()?,
                TransferSingle {
                    operator: child_owner,
                    from: child_owner,
                    to: Address::ZERO,
                    id: U256::from_be_bytes(original.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                v1,
                registry::Transfer {
                    node: original,
                    owner: child_owner,
                }
                .encode_log_data(),
            ),
            (
                WRAPPER.parse()?,
                NameUnwrapped {
                    node: original,
                    owner: child_owner,
                }
                .encode_log_data(),
            ),
        ],
    );
    seed_and_run_with(&database, &unwrapped, 124, 124, &[(124, 0, GRANTEE)], None).await?;
    let original_detail = path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    assert_eq!(
        original_detail["data"]["resolution_unsupported_reason"],
        "ens_v2_path_target_not_projected",
        "ordinary post-unwrap authority selection does not inherit the old wrapper-bound pointer: {original_detail:#}"
    );
    assert!(original_detail["data"]["unresolvable_reason"].is_null());
    let detail = path_get(&database, &format!("/v1/names/{NEW_CHILD}")).await?;
    assert_eq!(
        detail["data"]["primary_address"], GRANTEE,
        "unwrapped eligibility is retained: {detail:#}"
    );
    Ok((database, resolver))
}

#[tokio::test]
async fn migrated_subname_wrapper_fallback_unwrapped_and_rebound_requested_node() -> Result<()> {
    let (database, resolver) = unwrapped_rebound().await?;
    let original: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let owner = HOLDER.parse()?;
    let v1 = V1.parse()?;
    let clear = transaction(
        125,
        0,
        vec![(
            v1,
            registry::Transfer {
                node: original,
                owner: Address::ZERO,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &clear, 125, 125, &[(125, 0, GRANTEE)], None).await?;
    let detail = path_get(&database, &format!("/v1/names/{NEW_CHILD}")).await?;
    assert!(
        detail["data"]["resolver"].is_null(),
        "original node owner controls rebound fallback eligibility: {detail:#}"
    );
    assert_eq!(
        detail["data"]["unresolvable_reason"], "ens_v2_path_no_resolver",
        "{detail:#}"
    );
    assert_name_consumers(&database, NEW_CHILD, GRANTEE, false, resolver, None).await?;
    // The public factory accepts initializer bytes independently of the salt. A generic
    // known-wrapper deployment cannot establish its original node from these logs alone.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L125-L147 @ ens_v2_sepolia_20261001@07e55a05)
    let generic: Address = "0x0000000000000000000000000000000000001052".parse()?;
    let generic_logs = transaction(
        126,
        0,
        vec![
            (
                generic,
                Upgraded {
                    implementation: IMPL.parse()?,
                }
                .encode_log_data(),
            ),
            (generic, RegistryCreated {}.encode_log_data()),
            (
                generic,
                ParentUpdated {
                    parent: ETH.parse()?,
                    label: "rebound105".into(),
                    sender: Address::ZERO,
                }
                .encode_log_data(),
            ),
            (
                generic,
                EACRolesChanged {
                    resource: U256::ZERO,
                    account: ETH.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: roles(&[0, 16, 120, 124, 128, 144, 248, 252]),
                }
                .encode_log_data(),
            ),
            (
                FACTORY.parse()?,
                ProxyDeployed {
                    sender: owner,
                    proxyAddress: generic,
                    salt: U256::from_be_bytes(original.0),
                    implementation: IMPL.parse()?,
                }
                .encode_log_data(),
            ),
        ],
    );
    let mut generic_logs = generic_logs;
    generic_logs.extend(transaction(
        126,
        1,
        vec![(
            ETH.parse()?,
            SubregistryUpdated {
                tokenId: label_token("rebound105"),
                subregistry: generic,
                sender: owner,
            }
            .encode_log_data(),
        )],
    ));
    seed_and_run(&database, &generic_logs, 126, 126).await?;
    let detail = path_get(&database, &format!("/v1/names/{NEW_CHILD}")).await?;
    assert_eq!(
        detail["data"]["resolution_unsupported_reason"], "ens_v2_path_not_projected",
        "{detail:#}"
    );
    assert!(detail["data"]["unresolvable_reason"].is_null());
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_post_unwrap_ordinary_selection_evidence() -> Result<()> {
    let (database, resolver) = wrapped_setup().await?;
    let logical = format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?);
    let before = bigname_storage::families::name::load_family_name(&database.pool, &logical)
        .await?
        .context("wrapped child")?;
    assert_eq!(
        before.declared_summary["resolver"]["address"],
        format!("{resolver:#x}")
    );
    let node: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let child_owner = GRANTEE.parse()?;
    let logs = transaction(
        123,
        0,
        vec![
            (
                WRAPPER.parse()?,
                TransferSingle {
                    operator: child_owner,
                    from: child_owner,
                    to: Address::ZERO,
                    id: U256::from_be_bytes(node.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                V1.parse()?,
                registry::Transfer {
                    node,
                    owner: child_owner,
                }
                .encode_log_data(),
            ),
            (
                WRAPPER.parse()?,
                NameUnwrapped {
                    node,
                    owner: child_owner,
                }
                .encode_log_data(),
            ),
        ],
    );
    seed_and_run_with(&database, &logs, 123, 123, &[(123, 0, GRANTEE)], None).await?;
    let after = bigname_storage::families::name::load_family_name(&database.pool, &logical)
        .await?
        .context("unwrapped child")?;
    assert!(after.declared_summary["resolver"]["address"].is_null());
    eprintln!(
        "TYR105 ordinary unwrap evidence {}",
        json!({"fingerprint":bigname_content_hash::INTERPRETER_CONTENT_HASH,"before":{"resource":before.resource_id,"resolver":before.declared_summary["resolver"],"selection":before.provenance["authority_selection"]},"after":{"resource":after.resource_id,"resolver":after.declared_summary["resolver"],"selection":after.provenance["authority_selection"]}})
    );
    database.cleanup().await
}
