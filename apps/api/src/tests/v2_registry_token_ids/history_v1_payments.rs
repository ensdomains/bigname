//! Source layouts follow the three admitted mainnet controller generations and Sepolia's
//! numeric lifecycle plus wrapped-controller enrichment; see docs/manifests.md.
//! (upstream: .refs/ens_v1/deployments/archive/ETHRegistrarController_mainnet_9380471.sol/ETHRegistrarController_mainnet_9380471.json:L33 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L120 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333-L368 @ ens_v1@91c966f)
//! (upstream: .refs/basenames/lib/ens-contracts/deployments/sepolia/ETHRegistrarController.json:L164 @ basenames@1809bbc)
use super::compatibility::{admit_family_from, role_address};
use super::*;

mod legacy {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    }
}
mod wrapped {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires);
    }
}
mod current {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires, bytes32 referrer);
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires, bytes32 referrer);
    }
}
mod wrapper {
    use super::*;
    sol! {event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);}
}
mod base {
    use super::*;
    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NameRenewed(uint256 indexed id, uint256 expires);
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    }
}

pub(super) async fn seed_and_run(
    database: &TestDatabase,
    chain: &str,
    logs: &[RawLogInput],
    end: i64,
) -> Result<()> {
    let blocks = (120..=end)
        .map(|n| raw_block(chain, &format!("0xhistory{n}"), None, n, 1_700_000_000 + n))
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        chain,
        end,
        &format!("0xhistory{end}"),
        &bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + end)).internal_string(),
    )
    .await?;
    for log in logs {
        sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
            .bind(chain).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(HOLDER).bind(&log.emitting_address).execute(&database.pool).await?;
        sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(chain).bind(&log.block_hash).bind(log.block_number).bind(&log.transaction_hash).bind(log.transaction_index).bind(log.log_index).bind(&log.emitting_address).bind(&log.topics).bind(&log.data).execute(&database.pool).await?;
    }
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    for block in 120..=end {
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: chain.into(),
                from_block: block,
                to_block: block,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
    }
    publish_test_families_on(&database.pool, chain, end).await?;
    database.seed_snapshot_selector_chain_positions(&json!({chain:{"chain_id":chain,"block_number":end,"block_hash":format!("0xhistory{end}"),"timestamp":bigname_storage::UnixSeconds::from(timestamp(1_700_000_000+end)).internal_string()}})).await
}

fn log(
    data: alloy_primitives::LogData,
    chain: &str,
    emitter: Address,
    block: i64,
    index: i64,
) -> RawLogInput {
    let mut log = raw(data, block, index);
    log.chain_id = chain.into();
    log.emitting_address = format!("{emitter:#x}");
    log
}

#[tokio::test]
async fn v2_history_mainnet_controller_costs_preserve_all_three_admitted_formats() -> Result<()> {
    for role in [
        "legacy_registrar_controller",
        "wrapped_registrar_controller",
        "unwrapped_registrar_controller",
    ] {
        let database = TestDatabase::new_migrated().await?;
        let registrar =
            admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registrar_l1", 970).await?;
        let registry =
            admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 971).await?;
        let registrar_address = role_address(&registrar, "registrar");
        let controller = role_address(&registrar, role);
        let label = keccak256(LABEL);
        let id = U256::from_be_bytes(*label);
        let owner = HOLDER.parse()?;
        let wrapper_address = if role == "wrapped_registrar_controller" {
            Some(role_address(
                &admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 972).await?,
                "name_wrapper",
            ))
        } else {
            None
        };
        let token_owner = wrapper_address.unwrap_or(owner);
        let expires = U256::from(1_900_000_000_u64);
        let renewed = expires + U256::from(1000);
        let registered = match role {
            "legacy_registrar_controller" => legacy::NameRegistered {
                name: LABEL.into(),
                label,
                owner,
                cost: U256::MAX,
                expires,
            }
            .encode_log_data(),
            "wrapped_registrar_controller" => wrapped::NameRegistered {
                name: LABEL.into(),
                label,
                owner,
                baseCost: U256::MAX,
                premium: U256::ZERO,
                expires,
            }
            .encode_log_data(),
            _ => current::NameRegistered {
                name: LABEL.into(),
                label,
                owner,
                baseCost: U256::MAX,
                premium: U256::ZERO,
                expires,
                referrer: Default::default(),
            }
            .encode_log_data(),
        };
        let renewal = if role == "unwrapped_registrar_controller" {
            current::NameRenewed {
                name: LABEL.into(),
                label,
                cost: U256::ZERO,
                expires: renewed,
                referrer: Default::default(),
            }
            .encode_log_data()
        } else {
            legacy::NameRenewed {
                name: LABEL.into(),
                label,
                cost: U256::ZERO,
                expires: renewed,
            }
            .encode_log_data()
        };
        let mut logs = vec![
            log(
                base::Transfer {
                    from: Address::ZERO,
                    to: token_owner,
                    tokenId: id,
                }
                .encode_log_data(),
                CHAIN,
                registrar_address,
                120,
                0,
            ),
            log(
                base::NewOwner {
                    node: bigname_lookup::ens_namehash_hex("eth")?.parse()?,
                    label,
                    owner: token_owner,
                }
                .encode_log_data(),
                CHAIN,
                role_address(&registry, "registry"),
                120,
                1,
            ),
            log(
                base::NameRegistered {
                    id,
                    owner: token_owner,
                    expires,
                }
                .encode_log_data(),
                CHAIN,
                registrar_address,
                120,
                2,
            ),
            log(registered, CHAIN, controller, 120, 3),
            log(
                base::NameRenewed {
                    id,
                    expires: renewed,
                }
                .encode_log_data(),
                CHAIN,
                registrar_address,
                121,
                0,
            ),
            log(renewal, CHAIN, controller, 121, 1),
            log(
                base::Transfer {
                    from: owner,
                    to: GRANTEE.parse()?,
                    tokenId: id,
                }
                .encode_log_data(),
                CHAIN,
                registrar_address,
                122,
                0,
            ),
        ];
        if let Some(wrapper_address) = wrapper_address {
            logs.retain(|log| log.block_number != 122);
            logs.iter_mut()
                .find(|log| log.block_number == 120 && log.log_index == 3)
                .unwrap()
                .log_index = 5;
            let node: alloy_primitives::B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
            let wrapper_id = U256::from_be_bytes(*node);
            let mut dns = vec![LABEL.len() as u8];
            dns.extend_from_slice(LABEL.as_bytes());
            dns.extend_from_slice(b"\x03eth\x00");
            logs.push(log(
                TransferSingle {
                    operator: controller,
                    from: Address::ZERO,
                    to: owner,
                    id: wrapper_id,
                    value: U256::from(1),
                }
                .encode_log_data(),
                CHAIN,
                wrapper_address,
                120,
                3,
            ));
            logs.push(log(
                wrapper::NameWrapped {
                    node,
                    name: dns.into(),
                    owner,
                    fuses: 0,
                    expiry: 1_900_000_000,
                }
                .encode_log_data(),
                CHAIN,
                wrapper_address,
                120,
                4,
            ));
        }
        seed_and_run(&database, CHAIN, &logs, 122).await?;
        let retained:Vec<Value>=sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_kind='RegistrationGranted' AND after_state->>'source_event'='NameRegistered'").fetch_all(&database.pool).await?;
        assert!(
            retained
                .iter()
                .any(|a| a[if role == "legacy_registrar_controller" {
                    "cost"
                } else {
                    "base_cost"
                }] == U256::MAX.to_string()),
            "{role}: {retained:?}"
        );
        for route in [
            format!("/v1/names/{NAME}/history?scope=both"),
            format!("/v1/events?name={NAME}"),
            format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
        ] {
            let (status, body) = read_family_response(
                &database,
                &format!("{route}&include=data,raw&order=asc&page_size=200"),
            )
            .await?;
            assert_eq!(status, StatusCode::OK, "{body:#}");
            let rows = body["data"].as_array().unwrap();
            let paid = rows
                .iter()
                .filter(|r| r["kind"] == "RegistrationGranted" && r["block_number"] == 120)
                .collect::<Vec<_>>();
            assert!(!paid.is_empty(), "{role}: {body:#}");
            for row in paid {
                let data = &row["data"];
                assert_eq!(
                    data[if role == "legacy_registrar_controller" {
                        "cost"
                    } else {
                        "base_cost"
                    }],
                    U256::MAX.to_string(),
                    "{row:#}"
                );
                assert_eq!(
                    data.get("referrer").is_some(),
                    role == "unwrapped_registrar_controller"
                );
            }
            let renewals = rows
                .iter()
                .filter(|r| r["kind"] == "RegistrationRenewed" && r["data"]["cost"] == "0")
                .collect::<Vec<_>>();
            assert_eq!(renewals.len(), 1, "{role}: {body:#}");
            for row in rows {
                assert!(
                    row["data"].get("operator").is_none(),
                    "ERC721 has no retained operator: {row:#}"
                );
                assert!(row["data"].get("canonical_id").is_none());
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_history_sepolia_numeric_renewals_match_their_controller_costs() -> Result<()> {
    let chain = "ethereum-sepolia";
    let database = TestDatabase::new_migrated().await?;
    let registrar =
        admit_family_from(&database, "sepolia", chain, "ens_v1_registrar_l1", 970).await?;
    let registry =
        admit_family_from(&database, "sepolia", chain, "ens_v1_registry_l1", 971).await?;
    let registrar_address = role_address(&registrar, "registrar");
    let controller = role_address(&registrar, "wrapped_registrar_controller");
    let label = keccak256(LABEL);
    let id = U256::from_be_bytes(*label);
    let owner = HOLDER.parse()?;
    let expires = U256::from(1_900_000_000_u64);
    let mut logs = vec![
        log(
            base::Transfer {
                from: Address::ZERO,
                to: owner,
                tokenId: id,
            }
            .encode_log_data(),
            chain,
            registrar_address,
            120,
            0,
        ),
        log(
            base::NewOwner {
                node: bigname_lookup::ens_namehash_hex("eth")?.parse()?,
                label,
                owner,
            }
            .encode_log_data(),
            chain,
            role_address(&registry, "registry"),
            120,
            1,
        ),
        log(
            base::NameRegistered { id, owner, expires }.encode_log_data(),
            chain,
            registrar_address,
            120,
            2,
        ),
    ];
    for index in 0..2_i64 {
        let expires = expires + U256::from((index + 1) as u64 * 1000);
        logs.push(log(
            base::NameRenewed { id, expires }.encode_log_data(),
            chain,
            registrar_address,
            121,
            index * 2,
        ));
        logs.push(log(
            legacy::NameRenewed {
                name: LABEL.into(),
                label,
                cost: U256::from(index as u64),
                expires,
            }
            .encode_log_data(),
            chain,
            controller,
            121,
            index * 2 + 1,
        ));
    }
    seed_and_run(&database, chain, &logs, 121).await?;
    let retained:Vec<Value>=sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_kind='PreimageObserved' AND after_state->>'source_event'='NameRenewed' ORDER BY log_index").fetch_all(&database.pool).await?;
    assert_eq!(retained.len(), 2, "{retained:#?}");
    assert_eq!(retained[0]["cost"], "0");
    assert_eq!(retained[1]["cost"], "1");
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/events?name={NAME}&include=data,raw&order=asc&page_size=200"
                ))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let rows = body["data"].as_array().unwrap();
    let renewals = rows
        .iter()
        .filter(|r| r["kind"] == "RegistrationRenewed")
        .collect::<Vec<_>>();
    assert_eq!(renewals.len(), 2, "{body:#}");
    assert_eq!(renewals[0]["data"]["cost"], "0", "{body:#}");
    assert_eq!(renewals[1]["data"]["cost"], "1", "{body:#}");
    for row in rows {
        if row["kind"] != "RegistrationRenewed" {
            assert!(row["data"].get("cost").is_none(), "{row:#}");
        }
        assert_ne!(row["kind"], "PreimageObserved");
    }
    database.cleanup().await
}
