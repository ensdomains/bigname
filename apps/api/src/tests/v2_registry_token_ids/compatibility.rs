//! Migration reclaims and clears ENSv1 before registering the reserved name in ENSv2;
//! ordinary registrar registration has the two admitted referrer topic layouts.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L111-L165 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L501-L506 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2_sepolia_dev@554c309b)
use super::*;

mod legacy {
    use super::*;
    sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 referrer, uint256 base, uint256 premium);
    }
}
mod current {
    use super::*;
    sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
    }
}
mod registry {
    use super::*;
    sol! {
        event Transfer(bytes32 indexed node, address owner);
    }
}
sol! {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
}

async fn admit_family(database: &TestDatabase, family: &str, id: i64) -> Result<Value> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut source = repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == family)
        .context("fixture source manifest")?
        .manifest
        .clone();
    source.chain = CHAIN.into();
    source.rollout_status = bigname_manifests::RolloutStatus::Active;
    let manifest = ManifestInput {
        manifest_id: id,
        manifest_version: source.manifest_version as i64,
        namespace: source.namespace.clone(),
        source_family: source.source_family.clone(),
        chain_id: CHAIN.into(),
        deployment_label: source.deployment_epoch.clone(),
        normalizer_version: source.normalizer_version.clone(),
        payload_json: serde_json::to_string(&source)?,
    };
    v2_history_bounded_rebinding::persist_with_manifests(
        &database.pool,
        &[manifest],
        &BatchOutput::default(),
    )
    .await?;
    for (index, contract) in source.contracts.iter().enumerate() {
        let instance = Uuid::from_u128((id * 100 + index as i64) as u128);
        sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1,$2,'contract')")
            .bind(instance).bind(CHAIN).execute(&database.pool).await?;
        sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,source_manifest_id) VALUES ($1,$2,lower($3),0,$4)")
            .bind(instance).bind(CHAIN).bind(&contract.address).bind(id).execute(&database.pool).await?;
        sqlx::query("INSERT INTO manifest_contract_instances (manifest_id,chain_id,declaration_kind,declaration_name,contract_instance_id,declared_address,role,proxy_kind,start_block_number) VALUES ($1,$2,'contract',$3,$4,lower($5),$3,$6,0)")
            .bind(id).bind(CHAIN).bind(&contract.role).bind(instance).bind(&contract.address).bind(&contract.proxy_kind).execute(&database.pool).await?;
    }
    Ok(serde_json::to_value(&source)?)
}

fn role_address(manifest: &Value, role: &str) -> Address {
    manifest["contracts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["role"] == role)
        .unwrap()["address"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn registry_token_ids_follow_migration_and_decode_both_registrar_layouts() -> Result<()> {
    // A migration controller calls the registry directly; registrar observations belong to
    // ordinary registrations. Keep those real transaction shapes in separate databases.
    for registrar_topics in [None, Some(false), Some(true)] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_blocks(&database, 119..=134).await?;
        let migration = admit_family(&database, "ens_v2_migration_l1", 960).await?;
        let v1 = admit_family(&database, "ens_v1_registrar_l1", 961).await?;
        let v1_registry = admit_family(&database, "ens_v1_registry_l1", 964).await?;
        admit_family(&database, "ens_v1_wrapper_l1", 962).await?;
        let registrar = admit_family(&database, "ens_v2_registrar_l1", 963).await?;
        let controller = role_address(&migration, "unlocked_migration_controller");
        let graveyard = role_address(&migration, "graveyard");
        let (manifest, _, lifecycle) = fixture(false)?;
        let mut cleanup = raw(
            Transfer {
                from: controller,
                to: graveyard,
                tokenId: U256::from_be_bytes(*keccak256(LABEL)),
            }
            .encode_log_data(),
            120,
            3,
        );
        cleanup.emitting_address = format!("{:#x}", role_address(&v1, "registrar"));
        let mut predecessor = raw(
            NameRegistered {
                id: U256::from_be_bytes(*keccak256(LABEL)),
                owner: HOLDER.parse()?,
                expires: U256::from(1_900_000_000_u64),
            }
            .encode_log_data(),
            119,
            2,
        );
        predecessor.emitting_address = cleanup.emitting_address.clone();
        let mut registry_owner = raw(
            NewOwner {
                node: bigname_lookup::ens_namehash_hex("eth")?.parse()?,
                label: keccak256(LABEL),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            119,
            1,
        );
        registry_owner.emitting_address = format!("{:#x}", role_address(&v1_registry, "registry"));
        let mut logs = lifecycle
            .iter()
            .filter(|log| log.block_number == 119)
            .cloned()
            .collect::<Vec<_>>();
        let v1_registry_address = registry_owner.emitting_address.clone();
        logs.extend([registry_owner, predecessor]);
        let mut receive = raw(
            Transfer {
                from: HOLDER.parse()?,
                to: controller,
                tokenId: U256::from_be_bytes(*keccak256(LABEL)),
            }
            .encode_log_data(),
            120,
            0,
        );
        receive.emitting_address = cleanup.emitting_address.clone();
        logs.push(receive);
        let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
        for (index, data) in [
            NewOwner {
                node: bigname_lookup::ens_namehash_hex("eth")?.parse()?,
                label: keccak256(LABEL),
                owner: controller,
            }
            .encode_log_data(),
            registry::Transfer {
                node,
                owner: graveyard,
            }
            .encode_log_data(),
        ]
        .into_iter()
        .enumerate()
        {
            let mut log = raw(data, 120, index as i64 + 1);
            log.emitting_address = v1_registry_address.clone();
            logs.push(log);
        }
        logs.push(cleanup);
        let mut registration = registration(0, 0, 120);
        registration[0] = raw(
            LabelRegistered {
                tokenId: token(0),
                labelHash: keccak256(LABEL),
                label: LABEL.into(),
                owner: HOLDER.parse()?,
                expiry: 1_900_000_000,
                sender: controller,
            }
            .encode_log_data(),
            120,
            0,
        );
        registration[1] = raw(
            TransferSingle {
                operator: controller,
                from: Address::ZERO,
                to: HOLDER.parse()?,
                id: token(0),
                value: U256::from(1),
            }
            .encode_log_data(),
            120,
            1,
        );
        for log in &mut registration {
            log.log_index += 4;
        }
        logs.extend(registration);
        // Both ABI eras observe the registration, but the later registry regeneration is
        // authoritative. A registrar observation never becomes a token-selector candidate.
        let historical_topics = registrar_topics.unwrap_or(false);
        let data = if historical_topics {
            legacy::NameRegistered {
                tokenId: token(0),
                label: LABEL.into(),
                owner: HOLDER.parse()?,
                subregistry: Address::ZERO,
                resolver: Address::ZERO,
                duration: 31536000,
                paymentToken: Address::ZERO,
                referrer: Default::default(),
                base: U256::ZERO,
                premium: U256::ZERO,
            }
            .encode_log_data()
        } else {
            current::NameRegistered {
                tokenId: token(0),
                label: LABEL.into(),
                owner: HOLDER.parse()?,
                subregistry: Address::ZERO,
                resolver: Address::ZERO,
                duration: 31536000,
                paymentToken: Address::ZERO,
                referrer: Default::default(),
                base: U256::ZERO,
                premium: U256::ZERO,
            }
            .encode_log_data()
        };
        assert_eq!(data.topics().len(), if historical_topics { 2 } else { 3 });
        let mut observation = raw(data, 120, 10);
        observation.emitting_address = format!("{:#x}", role_address(&registrar, "registrar"));
        if registrar_topics.is_some() {
            logs = super::registration(0, 0, 120);
            observation.log_index = 4;
            logs.push(observation);
        }
        logs.extend(lifecycle.into_iter().filter(|log| log.block_number == 121));
        seed_interpret(&database, &manifest, &logs).await?;
        let options = database.pool.connect_options().as_ref().clone().options([(
            "bigname.interpreter_content_hash",
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )]);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await?;
        let engine = bigname_interpret::Engine::new(pool.clone());
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: CHAIN.into(),
                from_block: 119,
                to_block: 119,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
        let mut resource = None;
        for block in [120, 121] {
            engine
                .run_batch(bigname_interpret::BatchRequest {
                    chain_id: CHAIN.into(),
                    from_block: block,
                    to_block: block,
                    resume_current: None,
                    mode: bigname_interpret::RunMode::Normal,
                })
                .await?;
            routes::publish(&database, block).await?;
            let detail = v2_names_payload(&database, &format!("/v1/names/{NAME}")).await?;
            assert_eq!(detail["data"]["authority"], "ens_v2", "{detail}");
            assert_eq!(
                detail["data"]["token_id"],
                token((block - 120) as u32).to_string(),
                "{detail}"
            );
            let id = detail["data"]["registration_id"].as_str().unwrap();
            assert_eq!(id, resource.get_or_insert_with(|| id.to_owned()));
        }
        let evidence: Vec<(String,String,Value)> = sqlx::query_as("SELECT event_kind,consumer_visibility,after_state FROM normalized_events WHERE event_kind IN ('MigrationApplied','RegistrarNameRegistered') ORDER BY event_kind")
            .fetch_all(&database.pool).await?;
        assert_eq!(evidence.len(), 1, "{evidence:?}");
        assert_eq!(evidence[0].1, "activated");
        if registrar_topics.is_none() {
            assert_eq!(evidence[0].0, "MigrationApplied");
            assert_eq!(evidence[0].2["migration_path"], "unwrapped");
        } else {
            assert_eq!(evidence[0].0, "RegistrarNameRegistered");
            assert_eq!(evidence[0].2["token_id"], format!("{:#066x}", token(0)));
        }
        pool.close().await;
        database.cleanup().await?;
    }
    Ok(())
}
