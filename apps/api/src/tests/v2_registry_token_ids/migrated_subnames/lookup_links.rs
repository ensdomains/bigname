//! Real resolver initialization and setters cover absent/default/exact link dependencies.
use super::*;
use alloy_primitives::B256;

/// ENSv2 `.eth` registrations whose registry entries name the linked resolver.
const SUBJECT: &str = "linked105.eth";
const COMPANION: &str = "companion105.eth";
const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
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

fn address_bytes(
    id: u64,
    coin: u64,
    value: Vec<u8>,
) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        LINKED.parse()?,
        AddressUpdated {
            recordId: U256::from(id),
            coinType: U256::from(coin),
            addressBytes: value.into(),
        }
        .encode_log_data(),
    ))
}

fn address(id: u64, value: &str) -> Result<(Address, alloy_primitives::LogData)> {
    address_bytes(id, 60, value.parse::<Address>()?.to_vec())
}

async fn update(
    database: &TestDatabase,
    block: i64,
    events: Vec<(Address, alloy_primitives::LogData)>,
) -> Result<Value> {
    // Bound the debug test frame while retaining the real producer and every assertion.
    Box::pin(seed_and_run_with(
        database,
        &transaction(block, 0, events),
        block,
        block,
        &[(block, 0, GRANTEE)],
        None,
    ))
    .await?;
    lookup_publication::assert_name_prepared_parity(database, SUBJECT).await
}

async fn dependency(database: &TestDatabase, kind: &str, key2: &str) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_lookup_dependency dependency
        JOIN project_lookup_name name ON name.chain_id=dependency.chain_id
          AND name.record_serving_resource_id=dependency.resource_id
        WHERE name.logical_name_id=$1 AND dependency.kind=$2 AND dependency.key1=$3 AND dependency.key2=$4)")
        .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(SUBJECT)?))
        .bind(kind).bind(LINKED).bind(key2).fetch_one(&database.pool).await?)
}

async fn child_inventory(database: &TestDatabase) -> Result<Value> {
    let resource: Uuid = sqlx::query_scalar(
        "SELECT record_serving_resource_id FROM project_lookup_name WHERE logical_name_id=$1",
    )
    .bind(format!(
        "ens:{}",
        bigname_lookup::ens_namehash_hex(SUBJECT)?
    ))
    .fetch_one(&database.pool)
    .await?;
    let mut out = serde_json::Map::new();
    for table in [
        "project_lookup_inventory",
        "project_lookup_record",
        "project_lookup_dependency",
    ] {
        let rows: Vec<Value> = sqlx::query_scalar(&format!("SELECT to_jsonb(row) FROM {table} row WHERE resource_id=$1 ORDER BY to_jsonb(row)::text"))
            .bind(resource).fetch_all(&database.pool).await?;
        out.insert(table.into(), json!(rows));
    }
    Ok(Value::Object(out))
}

#[tokio::test]
async fn lookup_precomputation_admitted_link_defaults_arrival_clear_and_record_updates()
-> Result<()> {
    let (database, mut logs, _) = Box::pin(setup()).await?;
    logs.retain(|log| log.block_number <= BASE + 121);
    Box::pin(seed_and_run(&database, &logs, 120, 121)).await?;
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
    // Each name is registered in the ETH registry with the linked resolver on its own entry,
    // so the ENSv2 path reaches that resolver directly.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20261001@07e55a05)
    let expiry = (1_700_000_000 + BASE + 100_000) as u64;
    for (tx, name) in [(1, SUBJECT), (2, COMPANION)] {
        let label = name.trim_end_matches(".eth");
        creation.extend(transaction(
            122,
            tx,
            register(
                ETH.parse()?,
                label,
                resolver,
                Address::ZERO,
                expiry,
                DEPLOYER.parse()?,
            )?,
        ));
    }
    creation.extend(transaction(122, 3, vec![link(COMPANION, 0)?]));
    for (index, log) in creation.iter_mut().enumerate() {
        log.log_index = index as i64;
    }
    Box::pin(seed_and_run_with_targets(
        &database,
        &creation,
        122,
        122,
        &[
            (122, 0, GRANTEE),
            (122, 1, DEPLOYER),
            (122, 2, DEPLOYER),
            (122, 3, GRANTEE),
        ],
        &[(122, 0, FACTORY)],
        None,
    ))
    .await?;
    let empty = lookup_publication::assert_name_prepared_parity(&database, SUBJECT).await?;
    assert_eq!(empty["resolver"]["address"], LINKED, "{empty:#}");
    assert!(empty["primary_address"].is_null());
    assert!(
        dependency(&database, "link", &format!("{:#x}", B256::ZERO)).await?,
        "the missing default must already be tracked"
    );
    assert!(
        dependency(
            &database,
            "link",
            &bigname_lookup::ens_namehash_hex(SUBJECT)?
        )
        .await?
    );

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
            text(1, "unchanged-1", "one")?,
            text(1, "unchanged-2", "two")?,
            text(1, "unchanged-3", "three")?,
        ],
    )
    .await?;
    assert_eq!(default["primary_address"], HOLDER, "{default:#}");
    assert_eq!(default["records"]["texts"]["display name,a"], "default");
    assert!(dependency(&database, "record_id", "1").await?);
    let exact = update(
        &database,
        124,
        vec![link(SUBJECT, 2)?, text(2, "exact", "kept")?],
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

    let components = child_inventory(&database).await?;
    // A losing default link is still consulted, but its change cannot rewrite the current
    // exact-record result. Both ids already exist, satisfying linkToRecord's guard.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L242-L251 @ ens_v2_sepolia_20261001@07e55a05)
    update(&database, 126, vec![link("", 2)?]).await?;
    assert_eq!(child_inventory(&database).await?, components);
    let cleared = update(&database, 127, vec![link(SUBJECT, 0)?]).await?;
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
    let full_reads = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (result, work) = lookup_publication::observed(
        bigname_storage::families::records::seams::with_inventory_read_counter(
            full_reads.clone(),
            Box::pin(seed_and_run_with(
                &database,
                &transaction(130, 0, vec![text(1, "display name,a", "changed")?]),
                130,
                130,
                &[(130, 0, GRANTEE)],
                None,
            )),
        ),
    )
    .await;
    result?;
    lookup_publication::assert_key_work(&work, 2, 2);
    assert_eq!(
        lookup_publication::count(&work, "record_id_sources", "candidate_rows_loaded"),
        1,
        "shared source key is read once: {work:#?}"
    );
    eprintln!("incremental_f7_work={}", json!(work));
    let companion = lookup_publication::assert_name_prepared_parity(&database, COMPANION).await?;
    assert_eq!(companion["records"]["texts"]["display name,a"], "changed");
    let edited = lookup_publication::assert_name_prepared_parity(&database, SUBJECT).await?;
    assert_eq!(edited["records"]["texts"]["display name,a"], "changed");
    assert_eq!(edited["primary_address"], HOLDER);
    assert!(
        full_reads.lock().unwrap().is_empty(),
        "one value edit must not compose full inventories: {:?}",
        full_reads.lock().unwrap()
    );
    bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 129).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        restored_components
    );
    publish(&database, 130).await?;
    lookup_publication::assert_name_prepared_parity(&database, SUBJECT).await?;
    // Empty exact EVM bytes select the default at read time; these remain separate keys.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/AbstractRecordResolver.sol:L170-L177 @ ens_v2_sepolia_20261001@07e55a05)
    let fallback = update(
        &database,
        131,
        vec![
            address_bytes(1, 2147483648, GRANTEE.parse::<Address>()?.to_vec())?,
            address_bytes(1, 60, Vec::new())?,
        ],
    )
    .await?;
    assert_eq!(fallback["primary_address"], GRANTEE);
    let exact = update(&database, 132, vec![address(1, HOLDER)?]).await?;
    assert_eq!(exact["primary_address"], HOLDER);
    let cleared = update(&database, 133, vec![address_bytes(1, 60, Vec::new())?]).await?;
    assert_eq!(cleared["primary_address"], GRANTEE);
    let fallback = update(
        &database,
        134,
        vec![address_bytes(
            1,
            2147483648,
            HOLDER.parse::<Address>()?.to_vec(),
        )?],
    )
    .await?;
    assert_eq!(fallback["primary_address"], HOLDER);
    let names = path_get(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=resolves_to&coin_type=60"),
    )
    .await?;
    assert!(
        names["data"]
            .as_array()
            .context("resolves-to names")?
            .iter()
            .any(|record| record["name"] == SUBJECT),
        "{names:#}"
    );
    Box::pin(replay::assert_rebuild(&database, 134)).await?;
    lookup_publication::assert_name_prepared_parity(&database, SUBJECT).await?;
    lookup_publication::assert_name_prepared_parity(&database, COMPANION).await?;
    database.cleanup().await
}
