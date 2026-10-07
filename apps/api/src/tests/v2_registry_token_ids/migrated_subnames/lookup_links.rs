//! Real resolver initialization and setters cover absent/default/exact link dependencies.
use super::*;
use alloy_primitives::B256;

const LINKED: &str = "0x0000000000000000000000000000000000025910";
const IMPLEMENTATION: &str = "0x115eb53f0c60696633855f90b138178fb40b2b2c";
const FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
sol! {
    event ResolverCreated();
    event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name);
    event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value);
    event AddressUpdated(uint256 indexed recordId, uint256 coinType, bytes addressBytes);
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}

fn link(name: &str, id: u64) -> Result<(Address, alloy_primitives::LogData)> {
    let dns = if name.is_empty() {
        vec![0]
    } else {
        bigname_domain::normalization::normalize_name(name)?.dns_encoded_name
    };
    Ok((
        LINKED.parse()?,
        Linked {
            recordId: U256::from(id),
            node: bigname_lookup::ens_namehash_hex(name)?.parse()?,
            name: dns.into(),
        }
        .encode_log_data(),
    ))
}

fn text(id: u64, key: &str, value: &str) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        LINKED.parse()?,
        TextUpdated {
            recordId: U256::from(id),
            keyHash: keccak256(key),
            key: key.into(),
            value: value.into(),
        }
        .encode_log_data(),
    ))
}

fn address(id: u64, value: &str) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        LINKED.parse()?,
        AddressUpdated {
            recordId: U256::from(id),
            coinType: U256::from(60),
            addressBytes: value.parse::<Address>()?.to_vec().into(),
        }
        .encode_log_data(),
    ))
}

async fn update(
    database: &TestDatabase,
    block: i64,
    events: Vec<(Address, alloy_primitives::LogData)>,
) -> Result<Value> {
    seed_and_run_with(
        database,
        &transaction(block, 0, events),
        block,
        block,
        &[(block, 0, GRANTEE)],
        None,
    )
    .await?;
    lookup_publication::assert_name_prepared_parity(database, CHILD).await
}

async fn dependency(database: &TestDatabase, kind: &str, key2: &str) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_lookup_dependency dependency
        JOIN project_lookup_name name ON name.chain_id=dependency.chain_id
          AND name.record_serving_resource_id=dependency.resource_id
        WHERE name.logical_name_id=$1 AND dependency.kind=$2 AND dependency.key1=$3 AND dependency.key2=$4)")
        .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?))
        .bind(kind).bind(LINKED).bind(key2).fetch_one(&database.pool).await?)
}

#[tokio::test]
async fn lookup_precomputation_admitted_link_defaults_arrival_clear_and_record_updates()
-> Result<()> {
    let (database, mut logs, _) = setup().await?;
    // Keep ENSv1 as the active resolution path for this independently admitted resolver.
    // Other actual producer cases cover the later Universal Resolver cutover and mirror path.
    logs.retain(|log| {
        log.block_number <= BASE + 121
            && !(log.block_number == BASE + 120 && log.transaction_index == 0)
    });
    seed_and_run(&database, &logs, 120, 121).await?;
    let resolver: Address = LINKED.parse()?;
    let owner: Address = GRANTEE.parse()?;
    let implementation: Address = IMPLEMENTATION.parse()?;
    // The known PermissionedResolver proxy initializes root setter/link grants before use.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L119-L132 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L10-L58 @ ens_v2_sepolia_20261001@07e55a05)
    let mut creation = transaction(
        122,
        0,
        vec![
            (resolver, Upgraded { implementation }.encode_log_data()),
            (resolver, ResolverCreated {}.encode_log_data()),
            (
                resolver,
                EACRolesChanged {
                    resource: U256::ZERO,
                    account: owner,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: [0, 4, 28, 128, 132, 156]
                        .into_iter()
                        .fold(U256::ZERO, |roles, bit| roles | (U256::from(1) << bit)),
                }
                .encode_log_data(),
            ),
            (
                FACTORY.parse()?,
                ProxyDeployed {
                    sender: owner,
                    proxyAddress: resolver,
                    salt: U256::from(25910),
                    implementation,
                }
                .encode_log_data(),
            ),
        ],
    );
    creation.extend(transaction(
        122,
        1,
        vec![(
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?,
            NewResolver {
                node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
                resolver,
            }
            .encode_log_data(),
        )],
    ));
    for (index, log) in creation.iter_mut().enumerate() {
        log.log_index = index as i64;
    }
    seed_and_run_with_targets(
        &database,
        &creation,
        122,
        122,
        &[(122, 0, GRANTEE), (122, 1, GRANTEE)],
        &[(122, 0, FACTORY)],
        None,
    )
    .await?;
    let empty = lookup_publication::assert_name_prepared_parity(&database, CHILD).await?;
    assert_eq!(empty["resolver"]["address"], LINKED, "{empty:#}");
    assert!(empty["primary_address"].is_null());
    assert!(
        dependency(&database, "link", &format!("{:#x}", B256::ZERO)).await?,
        "the missing default must already be tracked"
    );
    assert!(dependency(&database, "link", &bigname_lookup::ens_namehash_hex(CHILD)?).await?);

    // The first setter for a name allocates its record and emits Linked; the empty DNS name
    // is the resolver-wide default. Later setters retain that same record id.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L352-L370 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L382-L390 @ ens_v2_sepolia_20261001@07e55a05)
    let default = update(
        &database,
        123,
        vec![
            link("", 1)?,
            address(1, HOLDER)?,
            text(1, "display name,a", "default")?,
        ],
    )
    .await?;
    assert_eq!(default["primary_address"], HOLDER, "{default:#}");
    assert_eq!(default["records"]["texts"]["display name,a"], "default");
    assert!(dependency(&database, "record_id", "1").await?);
    let exact = update(
        &database,
        124,
        vec![link(CHILD, 2)?, text(2, "exact", "kept")?],
    )
    .await?;
    assert!(
        exact["primary_address"].is_null(),
        "the exact record does not merge default keys: {exact:#}"
    );
    assert_eq!(exact["records"]["texts"]["exact"], "kept");
    assert!(exact["records"]["texts"].get("display name,a").is_none());
    assert!(dependency(&database, "record_id", "2").await?);
    let value = update(&database, 125, vec![address(2, GRANTEE)?]).await?;
    assert_eq!(value["primary_address"], GRANTEE);

    let components = lookup_publication::components(&database).await?;
    // A losing default link is still consulted, but its change cannot rewrite the current
    // exact-record result. Both ids already exist, satisfying linkToRecord's guard.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L242-L251 @ ens_v2_sepolia_20261001@07e55a05)
    update(&database, 126, vec![link("", 2)?]).await?;
    assert_eq!(lookup_publication::components(&database).await?, components);
    let cleared = update(&database, 127, vec![link(CHILD, 0)?]).await?;
    assert_eq!(
        cleared["primary_address"], GRANTEE,
        "exact zero falls back to the current default"
    );
    let none = update(&database, 128, vec![link("", 0)?]).await?;
    assert!(none["primary_address"].is_null(), "{none:#}");
    let restored = update(&database, 129, vec![link("", 1)?]).await?;
    assert_eq!(restored["primary_address"], HOLDER);
    assert_eq!(restored["records"]["texts"]["display name,a"], "default");
    let restored_components = lookup_publication::components(&database).await?;
    let edited = update(&database, 130, vec![text(1, "display name,a", "changed")?]).await?;
    assert_eq!(edited["records"]["texts"]["display name,a"], "changed");
    assert_eq!(edited["primary_address"], HOLDER);
    bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 129).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        restored_components
    );
    publish(&database, 130).await?;
    lookup_publication::assert_name_prepared_parity(&database, CHILD).await?;
    replay::assert_rebuild(&database, 130).await?;
    lookup_publication::assert_name_prepared_parity(&database, CHILD).await?;
    database.cleanup().await
}
