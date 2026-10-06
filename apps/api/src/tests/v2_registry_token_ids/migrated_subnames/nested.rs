use super::*;
use alloy_primitives::B256;
sol! {
    event RegistryCreated();
    event ParentUpdated(address indexed parent, string label, address indexed sender);
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
}
const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const USER: &str = "0x9bd8a88719068d09ecee662f36c0e3856708366a";
const W: &str = "0x0000000000000000000000000000000000001053";
const X: &str = "0x0000000000000000000000000000000000001054";
const DEEP: &str = "deep.branch.child.envoy1084.eth";
fn deploy(
    registry: Address,
    salt: u64,
    owner: Address,
) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    Ok(vec![
        (
            registry,
            Upgraded {
                implementation: USER.parse()?,
            }
            .encode_log_data(),
        ),
        (registry, RegistryCreated {}.encode_log_data()),
        (
            registry,
            EACRolesChanged {
                resource: U256::ZERO,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: [0, 8, 12, 16, 20, 24, 128, 136, 140, 144, 148, 152]
                    .into_iter()
                    .fold(U256::ZERO, |value, bit| value | (U256::from(1) << bit)),
            }
            .encode_log_data(),
        ),
        (
            "0xda70306c98e97ece36f997a21368e53298572991".parse()?,
            ProxyDeployed {
                sender: owner,
                proxyAddress: registry,
                salt: U256::from(salt),
                implementation: USER.parse()?,
            }
            .encode_log_data(),
        ),
    ])
}
async fn deadline(database: &TestDatabase) -> Result<Option<i64>> {
    Ok(
        sqlx::query_scalar(
            "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
        )
        .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(DEEP)?))
        .fetch_one(&database.pool)
        .await?,
    )
}
#[tokio::test]
async fn migrated_subname_nested_rebound_physical_mutation_updates_only_affected_path() -> Result<()>
{
    let (database, logs, resolver) = setup().await?;
    seed_and_run(&database, &logs, 120, 122).await?;
    let owner = HOLDER.parse()?;
    let eth = ETH.parse()?;
    let w = W.parse()?;
    let x = X.parse()?;
    let v1 = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let expiry = 1_700_000_000 + BASE + 200;
    let mut logs = transaction(123, 0, deploy(w, 1053, owner)?);
    logs.extend(transaction(123, 1, deploy(x, 1054, owner)?));
    logs.extend(transaction(
        123,
        2,
        vec![(
            w,
            ParentUpdated {
                parent: eth,
                label: "canonical105".into(),
                sender: owner,
            }
            .encode_log_data(),
        )],
    ));
    logs.extend(transaction(
        123,
        3,
        vec![(
            x,
            ParentUpdated {
                parent: w,
                label: "branch".into(),
                sender: owner,
            }
            .encode_log_data(),
        )],
    ));
    logs.extend(transaction(
        123,
        4,
        register(w, "branch", Address::ZERO, x, u64::MAX, owner)?,
    ));
    logs.extend(transaction(
        123,
        5,
        register(x, "deep", resolver, Address::ZERO, expiry as u64, owner)?,
    ));
    logs.extend(transaction(
        123,
        6,
        register(eth, "child", Address::ZERO, w, u64::MAX, DEPLOYER.parse()?)?,
    ));
    logs.extend(transaction(
        123,
        7,
        vec![(
            eth,
            SubregistryUpdated {
                tokenId: token(0),
                subregistry: eth,
                sender: owner,
            }
            .encode_log_data(),
        )],
    ));
    let child = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let branch: B256 = bigname_lookup::ens_namehash_hex(&format!("branch.{CHILD}"))?.parse()?;
    let deep = bigname_lookup::ens_namehash_hex(DEEP)?.parse()?;
    logs.extend(transaction(
        123,
        8,
        vec![(
            v1,
            NewOwner {
                node: child,
                label: keccak256("branch"),
                owner,
            }
            .encode_log_data(),
        )],
    ));
    logs.extend(transaction(
        123,
        9,
        vec![(
            resolver,
            NameChanged {
                node: branch,
                name: format!("branch.{CHILD}"),
            }
            .encode_log_data(),
        )],
    ));
    logs.extend(transaction(
        123,
        10,
        vec![
            (
                v1,
                NewOwner {
                    node: branch,
                    label: keccak256("deep"),
                    owner,
                }
                .encode_log_data(),
            ),
            (
                v1,
                NewResolver {
                    node: deep,
                    resolver,
                }
                .encode_log_data(),
            ),
        ],
    ));
    logs.extend(transaction(
        123,
        11,
        vec![
            (
                resolver,
                NameChanged {
                    node: deep,
                    name: DEEP.into(),
                }
                .encode_log_data(),
            ),
            (
                resolver,
                AddressChanged {
                    node: deep,
                    coinType: U256::from(60),
                    newAddress: owner.to_vec().into(),
                }
                .encode_log_data(),
            ),
        ],
    ));
    logs.extend(transaction(
        123,
        12,
        register(
            eth,
            "unrelated105",
            Address::ZERO,
            Address::ZERO,
            u64::MAX,
            DEPLOYER.parse()?,
        )?,
    ));
    seed_and_run_with(
        &database,
        &logs,
        123,
        123,
        &[(123, 6, DEPLOYER), (123, 8, GRANTEE), (123, 12, DEPLOYER)],
        None,
    )
    .await?;
    let record = path_get(&database, &format!("/v1/names/{DEEP}")).await?;
    assert_eq!(record["data"]["primary_address"], HOLDER, "{record:#}");
    assert_eq!(deadline(&database).await?, Some(expiry));
    // Only the deeply nested physical entry changes. Its canonical logical name differs from
    // this requested path at two mounts, and neither requested name receives a new event.
    let renew = transaction(
        124,
        0,
        vec![(
            x,
            ExpiryUpdated {
                tokenId: label_token("deep"),
                newExpiry: (expiry + 100) as u64,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run(&database, &renew, 124, 124).await?;
    assert_eq!(
        deadline(&database).await?,
        Some(expiry + 100),
        "nested rebound dependency was missed"
    );
    let unrelated = transaction(
        125,
        0,
        vec![(
            eth,
            ResolverUpdated {
                tokenId: label_token("unrelated105"),
                resolver,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run(&database, &unrelated, 125, 125).await?;
    assert_eq!(deadline(&database).await?, Some(expiry + 100));
    let clear = transaction(
        126,
        0,
        vec![(
            w,
            SubregistryUpdated {
                tokenId: label_token("branch"),
                subregistry: Address::ZERO,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run(&database, &clear, 126, 126).await?;
    let record = path_get(&database, &format!("/v1/names/{DEEP}")).await?;
    assert_eq!(
        record["data"]["unresolvable_reason"], "ens_v2_path_no_resolver",
        "{record:#}"
    );
    assert!(
        deadline(&database)
            .await?
            .is_none_or(|next| next > expiry + 100)
    );
    let restore = transaction(
        127,
        0,
        vec![(
            w,
            SubregistryUpdated {
                tokenId: label_token("branch"),
                subregistry: x,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run(&database, &restore, 127, 127).await?;
    assert_eq!(deadline(&database).await?, Some(expiry + 100));
    let record = path_get(&database, &format!("/v1/names/{DEEP}")).await?;
    assert_eq!(record["data"]["primary_address"], HOLDER, "{record:#}");
    replay::assert_rebuild(&database, 127).await?;
    if let Ok(path) = std::env::var("BIGNAME_TYR105_NESTED_EVIDENCE") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(
                &json!({"database":database.database_name,"fingerprint":bigname_content_hash::INTERPRETER_CONTENT_HASH,"name":DEEP,"logical_name_id":format!("ens:{deep:#x}"),"unrelated":format!("ens:{}",bigname_lookup::ens_namehash_hex("unrelated105.eth")?)}),
            )?,
        )?;
        database.pool.close().await;
        database.lookup_pool.close().await;
        Ok(())
    } else {
        database.cleanup().await
    }
}
