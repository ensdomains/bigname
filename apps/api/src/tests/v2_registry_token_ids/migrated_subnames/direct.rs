use super::*;

const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";

#[tokio::test]
async fn migrated_subname_direct_leaf_and_deeper_parent_clock() -> Result<()> {
    let (database, logs, resolver) = setup().await?;
    seed_and_run(&database, &logs, 120, 122).await?;
    assert_consumers(&database, false, resolver, None).await?;
    let registry = ETH_REGISTRY.parse()?;
    let owner = HOLDER.parse()?;
    let deployer = DEPLOYER.parse()?;
    let clock = 1_700_000_000 + BASE + 131;
    let expiry = (clock + 20) as u64;
    let child = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let v1 = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let unnamed_log = transaction(
        123,
        0,
        vec![(
            v1,
            NewOwner {
                node: child,
                label: keccak256("never-current105"),
                owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(
        &database,
        &unnamed_log,
        123,
        123,
        &[(123, 0, GRANTEE)],
        None,
    )
    .await?;
    let unnamed = format!(
        "ens:{}",
        bigname_lookup::ens_namehash_hex(&format!("never-current105.{CHILD}"))?
    );
    let raw_name: Option<String> =
        sqlx::query_scalar("SELECT raw_name FROM name_surfaces WHERE logical_name_id=$1")
            .bind(&unnamed)
            .fetch_one(&database.pool)
            .await?;
    assert!(
        raw_name.is_none(),
        "fixture must exercise a hash-only never-current descendant"
    );
    // A legal mount of the same known registry uses labels, not its canonical parent. A leaf
    // PublicResolver still reads the requested child node, and therefore the retained record.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L22-L85 @ ens_v2_sepolia_20261001@07e55a05)
    let mut direct = transaction(
        130,
        0,
        register(registry, "child", resolver, registry, expiry, deployer)?,
    );
    direct.extend(transaction(
        130,
        1,
        vec![(
            registry,
            SubregistryUpdated {
                tokenId: token(0),
                subregistry: registry,
                sender: owner,
            }
            .encode_log_data(),
        )],
    ));
    seed_and_run_with(&database, &direct, 124, 130, &[(130, 0, DEPLOYER)], None).await?;
    assert_consumers(&database, true, resolver, None).await?;
    let unnamed_deadline: Option<i64> = sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
    )
    .bind(&unnamed)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        unnamed_deadline,
        Some(expiry as i64),
        "parent-only route change refreshes never-current hash-only descendants"
    );
    let grand = format!("grand.{CHILD}");
    let node = bigname_lookup::ens_namehash_hex(&grand)?.parse()?;
    let child = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let v1 = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let mut deeper = transaction(
        131,
        0,
        vec![
            (
                v1,
                NewOwner {
                    node: child,
                    label: keccak256("grand"),
                    owner,
                }
                .encode_log_data(),
            ),
            (v1, NewResolver { node, resolver }.encode_log_data()),
        ],
    );
    deeper.extend(transaction(
        131,
        1,
        vec![
            (
                resolver,
                NameChanged {
                    node,
                    name: grand.clone(),
                }
                .encode_log_data(),
            ),
            (
                resolver,
                AddressChanged {
                    node,
                    coinType: U256::from(60),
                    newAddress: owner.to_vec().into(),
                }
                .encode_log_data(),
            ),
        ],
    ));
    deeper.extend(transaction(
        131,
        2,
        register(
            registry,
            "grand",
            resolver,
            Address::ZERO,
            u64::MAX,
            deployer,
        )?,
    ));
    seed_and_run_with(
        &database,
        &deeper,
        131,
        131,
        &[(131, 0, GRANTEE), (131, 2, DEPLOYER)],
        None,
    )
    .await?;
    let before = path_get(&database, &format!("/v1/names/{grand}")).await?;
    assert_eq!(before["data"]["primary_address"], HOLDER, "{before:#}");
    let logical = format!("ens:{node:#x}");
    let deadline: Option<i64> = sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
    )
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(deadline, Some(expiry as i64));
    // No child event occurs when the intermediate registry entry expires.
    seed_and_run_with(&database, &[], 132, 132, &[], Some(expiry as i64)).await?;
    let after = path_get(&database, &format!("/v1/names/{grand}")).await?;
    assert!(after["data"]["resolver"].is_null(), "{after:#}");
    assert_eq!(
        after["data"]["unresolvable_reason"], "ens_v2_path_no_resolver",
        "{after:#}"
    );
    let deadline: Option<i64> = sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
    )
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?;
    assert!(
        deadline.is_none_or(|next| next > expiry as i64),
        "expired path deadline must be consumed: {deadline:?}"
    );
    let unnamed_deadline: Option<i64> = sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
    )
    .bind(&unnamed)
    .fetch_one(&database.pool)
    .await?;
    assert!(unnamed_deadline.is_none_or(|next| next > expiry as i64));
    replay::assert_rebuild(&database, 132).await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_nearest_nonextended_resolver_hides_ancestor_mirror() -> Result<()> {
    let (database, logs, resolver) = setup().await?;
    seed_and_run(&database, &logs, 120, 122).await?;
    let registry = ETH_REGISTRY.parse()?;
    let v1 = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let owner = HOLDER.parse()?;
    let child = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let grand = format!("wildcard.{CHILD}");
    let node = bigname_lookup::ens_namehash_hex(&grand)?.parse()?;
    let mut logs = transaction(
        123,
        0,
        vec![
            (
                v1,
                NewOwner {
                    node: child,
                    label: keccak256("wildcard"),
                    owner,
                }
                .encode_log_data(),
            ),
            (v1, NewResolver { node, resolver }.encode_log_data()),
        ],
    );
    logs.extend(transaction(
        123,
        1,
        vec![
            (
                resolver,
                NameChanged {
                    node,
                    name: grand.clone(),
                }
                .encode_log_data(),
            ),
            (
                resolver,
                AddressChanged {
                    node,
                    coinType: U256::from(60),
                    newAddress: owner.to_vec().into(),
                }
                .encode_log_data(),
            ),
        ],
    ));
    logs.extend(transaction(
        123,
        2,
        register(
            registry,
            "child",
            resolver,
            registry,
            u64::MAX,
            DEPLOYER.parse()?,
        )?,
    ));
    logs.extend(transaction(
        123,
        3,
        vec![
            (
                registry,
                SubregistryUpdated {
                    tokenId: token(0),
                    subregistry: registry,
                    sender: owner,
                }
                .encode_log_data(),
            ),
            (
                registry,
                ResolverUpdated {
                    tokenId: token(0),
                    resolver: "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0".parse()?,
                    sender: owner,
                }
                .encode_log_data(),
            ),
        ],
    ));
    seed_and_run_with(
        &database,
        &logs,
        123,
        123,
        &[(123, 0, GRANTEE), (123, 2, DEPLOYER)],
        None,
    )
    .await?;
    let blocked = path_get(&database, &format!("/v1/names/{grand}")).await?;
    assert_eq!(
        blocked["data"]["unresolvable_reason"], "ens_v2_path_no_resolver",
        "nearest non-extended resolver must hide the ancestor mirror: {blocked:#}"
    );
    assert!(blocked["data"]["primary_address"].is_null());
    let clear = transaction(
        124,
        0,
        vec![(
            registry,
            ResolverUpdated {
                tokenId: label_token("child"),
                resolver: Address::ZERO,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run(&database, &clear, 124, 124).await?;
    let restored = path_get(&database, &format!("/v1/names/{grand}")).await?;
    assert_eq!(
        restored["data"]["primary_address"], HOLDER,
        "clear restores ancestor mirror traversal to requested node: {restored:#}"
    );
    database.cleanup().await
}
