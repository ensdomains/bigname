//! The exact-key path keeps address pairs, opaque keys, ABI observations and version cutoffs
//! coupled to the same compositor used by the full reader. Inputs are actual admitted ABI logs.
use super::lookup_publication::{assert_name_prepared_parity, components, count, observed};
use super::*;

sol! {
    // (upstream: .refs/ens_v1_sepolia_ac32490/contracts/resolvers/profiles/AddrResolver.sol:L50-L54 @ ens_v1_sepolia_ac32490@ac32490)
    event AddrChanged(bytes32 indexed node, address a);
    // (upstream: .refs/ens_v1_sepolia_ac32490/contracts/resolvers/profiles/ABIResolver.sol:L18-L28 @ ens_v1_sepolia_ac32490@ac32490)
    event ABIChanged(bytes32 indexed node, uint256 indexed contentType);
    // (upstream: .refs/ens_v1_sepolia_ac32490/contracts/resolvers/profiles/ContentHashResolver.sol:L16-L22 @ ens_v1_sepolia_ac32490@ac32490)
    event ContenthashChanged(bytes32 indexed node, bytes hash);
    // (upstream: .refs/ens_v1_sepolia_ac32490/contracts/resolvers/ResolverBase.sol:L24-L26 @ ens_v1_sepolia_ac32490@ac32490)
    event VersionChanged(bytes32 indexed node, uint64 newVersion);
}

fn text(resolver: Address, key: &str, value: &str) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        resolver,
        lookup_publication::TextChanged {
            node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
            indexedKey: keccak256(key),
            key: key.into(),
            value: value.into(),
        }
        .encode_log_data(),
    ))
}

fn pair(resolver: Address, address: Address) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let node = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    Ok(vec![
        (
            resolver,
            AddressChanged {
                node,
                coinType: U256::from(60),
                newAddress: address.to_vec().into(),
            }
            .encode_log_data(),
        ),
        (resolver, AddrChanged { node, a: address }.encode_log_data()),
    ])
}

async fn apply(
    database: &TestDatabase,
    block: i64,
    events: Vec<(Address, alloy_primitives::LogData)>,
) -> Result<Vec<Value>> {
    let logs = transaction(block, 0, events);
    // Keep the producer future out of this helper's frame on the default debug test stack.
    let (result, work) = observed(Box::pin(seed_and_run_with(
        database,
        &logs,
        block,
        block,
        &[(block, 0, GRANTEE)],
        None,
    )))
    .await;
    result?;
    Ok(work)
}

#[tokio::test]
async fn lookup_precomputation_exact_keys_preserve_pairs_opaque_abi_and_mixed_boundaries()
-> Result<()> {
    let (database, logs, resolver) = Box::pin(setup()).await?;
    let initial: Vec<_> = logs
        .into_iter()
        .filter(|log| log.block_number <= BASE + 121)
        .collect();
    Box::pin(seed_and_run(&database, &initial, 120, 121)).await?;
    let node = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let mut events = vec![
        text(resolver, "kept", "value")?,
        text(resolver, "  ", "opaque")?,
        (
            resolver,
            ContenthashChanged {
                node,
                hash: Vec::<u8>::new().into(),
            }
            .encode_log_data(),
        ),
        (
            resolver,
            ABIChanged {
                node,
                contentType: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            resolver,
            NameChanged {
                node,
                name: "".into(),
            }
            .encode_log_data(),
        ),
    ];
    events.extend(pair(resolver, GRANTEE.parse()?)?);
    let work = apply(&database, 122, events).await?;
    assert_eq!(
        count(&work, "resource_work", "full_resources"),
        0,
        "{work:#?}"
    );
    assert_eq!(
        count(&work, "key_components", "requested_resource_keys"),
        6,
        "{work:#?}"
    );
    assert_eq!(
        count(&work, "key_components", "constructed_components"),
        6,
        "{work:#?}"
    );
    let result = assert_name_prepared_parity(&database, CHILD).await?;
    assert_eq!(result["primary_address"], GRANTEE);
    assert_eq!(result["records"]["seen_abis"], json!(["1"]));
    let opaque: Value = sqlx::query_scalar(
        "SELECT payload FROM project_lookup_record WHERE record_key='text_opaque:0x2020'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(opaque["unsupported_family"], "text_opaque");
    assert_eq!(opaque["entries"], json!([]));
    let mut events = pair(resolver, Address::ZERO)?;
    events.push(text(resolver, "kept", "")?);
    events.push((
        resolver,
        ABIChanged {
            node,
            contentType: U256::from(2),
        }
        .encode_log_data(),
    ));
    let work = apply(&database, 123, events).await?;
    assert_eq!(
        count(&work, "resource_work", "full_resources"),
        0,
        "{work:#?}"
    );
    assert_eq!(
        count(&work, "key_components", "requested_resource_keys"),
        3,
        "{work:#?}"
    );
    let result = assert_name_prepared_parity(&database, CHILD).await?;
    assert!(result["primary_address"].is_null(), "{result:#}");
    assert_eq!(result["records"]["texts"].get("kept"), Some(&Value::Null));
    assert_eq!(result["records"]["seen_abis"], json!(["1", "2"]));
    let before_version = components(&database).await?;
    let work = apply(
        &database,
        124,
        vec![
            (
                resolver,
                VersionChanged {
                    node,
                    newVersion: 1,
                }
                .encode_log_data(),
            ),
            text(resolver, "post-version", "one")?,
        ],
    )
    .await?;
    assert_eq!(
        count(&work, "resource_work", "full_resources"),
        1,
        "{work:#?}"
    );
    assert_eq!(
        count(&work, "key_components", "requested_resource_keys"),
        0,
        "full refresh dominates: {work:#?}"
    );
    let result = assert_name_prepared_parity(&database, CHILD).await?;
    assert_eq!(result["records"]["seen_abis"], json!([]));
    assert_eq!(result["records"]["texts"]["post-version"], "one");
    assert!(result["records"]["texts"].get("kept").is_none());
    let work = apply(&database, 125, vec![text(resolver, "post-version", "two")?]).await?;
    lookup_publication::assert_key_work(&work, 1, 1);
    assert_name_prepared_parity(&database, CHILD).await?;
    let resource: Uuid = sqlx::query_scalar(
        "SELECT record_serving_resource_id FROM project_lookup_name WHERE logical_name_id=$1",
    )
    .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?))
    .fetch_one(&database.pool)
    .await?;
    let publication =
        bigname_storage::families::name::load_family_publication(&database.pool, PATH_CHAIN)
            .await?
            .context("publication")?;
    let mut conn = database.pool.acquire().await?;
    let selected = bigname_storage::families::lookup::compose_lookup_record_keys_at(
        &mut conn,
        &publication,
        &std::collections::BTreeMap::from([(
            resource,
            std::collections::BTreeSet::from([
                "abi:1".into(),
                "text:post-version".into(),
                "text:never".into(),
            ]),
        )]),
    )
    .await?;
    assert!(selected[&resource]["abi:1"].is_none(), "old ABI is cut off");
    assert!(
        selected[&resource]["text:never"].is_none(),
        "missing keys are explicit"
    );
    assert!(selected[&resource]["text:post-version"].is_some());
    drop(conn);
    let registry = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let work = apply(
        &database,
        126,
        vec![
            (
                registry,
                NewResolver {
                    node,
                    resolver: Address::ZERO,
                }
                .encode_log_data(),
            ),
            text(resolver, "post-version", "hidden")?,
        ],
    )
    .await?;
    assert_eq!(
        count(&work, "resource_work", "removed_resources"),
        1,
        "{work:#?}"
    );
    assert_eq!(
        count(&work, "key_components", "requested_resource_keys"),
        0,
        "reference cleanup dominates: {work:#?}"
    );
    apply(
        &database,
        127,
        vec![
            (registry, NewResolver { node, resolver }.encode_log_data()),
            text(resolver, "post-version", "restored")?,
        ],
    )
    .await?;
    assert_name_prepared_parity(&database, CHILD).await?;
    let final_components = components(&database).await?;
    bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 123).await?;
    assert_eq!(components(&database).await?, before_version);
    publish(&database, 127).await?;
    assert_eq!(components(&database).await?, final_components);
    Box::pin(replay::assert_rebuild(&database, 127)).await?;
    assert_name_prepared_parity(&database, CHILD).await?;
    database.cleanup().await
}
