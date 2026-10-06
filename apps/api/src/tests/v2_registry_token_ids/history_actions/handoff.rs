use super::*;
sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
}

mod registrar {
    use super::*;
    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

mod controller {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires, bytes32 referrer);
    }
}

#[tokio::test]
async fn history_actions_handoff_enriches_first_current_owner_log_without_a_new_row() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let registry =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 1003).await?;
    let old = role_address(&registry, "registry_old");
    let current = role_address(&registry, "registry");
    let parent = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
    let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let logs = vec![
        emitted(
            NewOwner {
                node: parent,
                label: keccak256(LABEL),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            old,
            120,
            0,
        ),
        emitted(
            NewOwner {
                node: parent,
                label: keccak256("current-only"),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            current,
            120,
            1,
        ),
        emitted(
            NewOwner {
                node: parent,
                label: keccak256(LABEL),
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            current,
            121,
            0,
        ),
        emitted(
            Transfer {
                node,
                owner: GRANTEE.parse()?,
            }
            .encode_log_data(),
            current,
            121,
            1,
        ),
        emitted(
            Transfer {
                node: alloy_primitives::B256::ZERO,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            old,
            121,
            2,
        ),
        emitted(
            Transfer {
                node: alloy_primitives::B256::ZERO,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            current,
            121,
            3,
        ),
        emitted(
            Transfer {
                node,
                owner: HOLDER.parse()?,
            }
            .encode_log_data(),
            old,
            122,
            0,
        ),
    ];
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 122).await?;
    let body = page(
        &database,
        "/v1/events?namespace=ens&kind=AuthorityTransferred,SubregistryChanged",
    )
    .await?;
    let handoffs = action_rows(&body, "registry_handoff");
    assert_eq!(handoffs.len(), 1, "{body:#}");
    let row = handoffs[0];
    assert_eq!(row["block_number"], 121);
    assert_eq!(row["log_index"], 0);
    assert_eq!(row["type"], "subregistry");
    assert_eq!(row["data"]["node"], format!("{node:#x}"));
    assert_eq!(row["data"]["owner"], HOLDER);
    assert_eq!(
        row["data"]["from_registry"],
        json!({"chain_id":1,"address":format!("{old:#x}")})
    );
    assert_eq!(
        row["data"]["to_registry"],
        json!({"chain_id":1,"address":format!("{current:#x}")})
    );
    assert!(row["data"].get("migration_path").is_none());
    let identity:String=sqlx::query_scalar("SELECT event_identity FROM normalized_events WHERE block_number=121 AND log_index=0 AND event_kind='SubregistryChanged'").fetch_one(&database.pool).await?;
    use sha2::Digest;
    assert_eq!(
        row["id"],
        hex::encode(sha2::Sha256::digest(identity.as_bytes()))
    );
    let exact = page(
        &database,
        &format!("/v1/events?contract_address={current:#x}&kind=SubregistryChanged"),
    )
    .await?;
    let matching = action_rows(&exact, "registry_handoff");
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0]["id"], row["id"]);
    database.cleanup().await
}

#[tokio::test]
async fn history_actions_handoff_reconciled_registration_with_old_resolver_stays_ordinary()
-> Result<()> {
    reconciled_registration(true).await
}

#[tokio::test]
async fn history_actions_handoff_reconciled_registration_without_old_resolver_stays_ordinary()
-> Result<()> {
    reconciled_registration(false).await
}

async fn reconciled_registration(has_old_resolver: bool) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = async {
        let registry = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 1003).await?;
        let registrar = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registrar_l1", 1004).await?;
        let old = role_address(&registry, "registry_old");
        let current = role_address(&registry, "registry");
        let base = role_address(&registrar, "registrar");
        let controller = role_address(&registrar, "unwrapped_registrar_controller");
        let parent = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
        let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
        let label = keccak256(LABEL);
        let id = U256::from_be_bytes(*label);
        let buyer = GRANTEE.parse()?;
        let resolver = "0x000000000000000000000000000000000000beef".parse()?;
        let expires = U256::from(1_900_000_000_u64);
        let mut logs = vec![emitted(NewOwner { node: parent, label, owner: HOLDER.parse()? }.encode_log_data(), old, 120, 0)];
        if has_old_resolver {
            logs.push(emitted(NewResolver { node, resolver }.encode_log_data(), old, 120, 1));
        }
        // The admitted nonzero-resolver controller path registers to itself, writes the
        // buyer's registry record, then transfers the token and emits the named registration.
        // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L293-L341 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L152 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L34-L40 @ ens_v1@91c966f)
        logs.extend([
            emitted(registrar::Transfer { from: Address::ZERO, to: controller, tokenId: id }.encode_log_data(), base, 121, 0),
            emitted(NewOwner { node: parent, label, owner: controller }.encode_log_data(), current, 121, 1),
            emitted(registrar::NameRegistered { id, owner: controller, expires }.encode_log_data(), base, 121, 2),
            emitted(Transfer { node, owner: buyer }.encode_log_data(), current, 121, 3),
            emitted(NewResolver { node, resolver }.encode_log_data(), current, 121, 4),
            emitted(registrar::Transfer { from: controller, to: buyer, tokenId: id }.encode_log_data(), base, 121, 5),
            emitted(controller::NameRegistered { name: LABEL.into(), label, owner: buyer, baseCost: U256::from(1), premium: U256::ZERO, expires, referrer: Default::default() }.encode_log_data(), controller, 121, 6),
            emitted(Transfer { node, owner: HOLDER.parse()? }.encode_log_data(), current, 122, 0),
        ]);
        history_v1_payments::seed_and_run(&database, CHAIN, &logs, 122).await?;

        let old_count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE block_number=120 AND event_kind='SubregistryChanged' AND after_state->>'emitter_role'='registry_old'")
            .fetch_one(&database.pool).await?;
        assert_eq!(old_count, 1, "the earlier old-owner witness must survive");
        let initial_count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE block_number=121 AND log_index=1 AND event_kind IN ('SubregistryChanged','AuthorityTransferred') AND source_family='ens_v1_registry_l1'")
            .fetch_one(&database.pool).await?;
        assert_eq!(initial_count, 0, "reconciliation must remove the transient owner's direct rows");
        let (identity, after, resource): (String, Value, Uuid) = sqlx::query_as("SELECT event_identity,after_state,resource_id FROM normalized_events WHERE block_number=121 AND log_index=3 AND event_kind='AuthorityTransferred' AND source_family='ens_v1_registry_l1'")
            .fetch_one(&database.pool).await?;
        assert_eq!(after["source_event"], "Transfer");
        assert_eq!(after["emitter_role"], "registry");
        assert!(after.get("authority_kind").is_none() && after.get("authority_key").is_none(), "{after}");
        let registration: Uuid = sqlx::query_scalar("SELECT resource_id FROM normalized_events WHERE block_number=121 AND event_kind='RegistrationGranted'")
            .fetch_one(&database.pool).await?;
        assert_eq!(resource, registration, "the surviving owner row must be retargeted");
        let clears: Vec<(i64, i64, String)> = sqlx::query_as("SELECT block_number,log_index,after_state->>'source_event' FROM normalized_events WHERE event_kind='ResolverChanged' AND after_state->>'registry_fallback_handoff'='true' AND consumer_visibility='activated' AND canonicality_state IN ('canonical','safe','finalized')")
            .fetch_all(&database.pool).await?;
        if !has_old_resolver {
            assert!(clears.is_empty(), "{clears:?}");
        } else {
            assert!(!clears.is_empty());
            assert!(clears.iter().all(|(block, log, source)| *block == 121 && *log == 1 && source == "NewOwner"), "{clears:?}");
        }

        let body = page(&database, &format!("/v1/events?contract_address={current:#x}&kind=AuthorityTransferred")).await?;
        let rows = body["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "{body:#}");
        assert_eq!(body["page"]["total_count"], 2);
        use sha2::Digest;
        assert_eq!(rows[0]["id"], hex::encode(sha2::Sha256::digest(identity.as_bytes())));
        assert_eq!(rows[0]["block_number"], 121);
        assert_eq!(rows[0]["log_index"], 3);
        assert_eq!(rows[1]["block_number"], 122);
        assert_eq!(rows[1]["log_index"], 0);
        for row in rows {
            assert_ordinary_owner(row);
        }
        let name = page(&database, &format!("/v1/names/{NAME}/history?scope=both&kind=AuthorityTransferred")).await?;
        assert_eq!(name["page"]["total_count"], name["data"].as_array().unwrap().len());
        for row in rows {
            let matching = name["data"].as_array().unwrap().iter().find(|candidate| candidate["id"] == row["id"]).context("name history must retain the same owner row")?;
            assert_eq!(matching, row);
        }
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

fn assert_ordinary_owner(row: &Value) {
    assert_eq!(row["data"]["action"], "authority_changed", "{row:#}");
    assert!(row["data"].get("from_registry").is_none(), "{row:#}");
    assert!(row["data"].get("to_registry").is_none(), "{row:#}");
}

#[tokio::test]
async fn history_actions_handoff_earlier_same_owner_fallback_clear_vetoes_later_transfer()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = async {
        let registry = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 1003).await?;
        let old = role_address(&registry, "registry_old");
        let current = role_address(&registry, "registry");
        let parent = bigname_lookup::ens_namehash_hex("eth")?.parse()?;
        let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
        let logs = [
            emitted(NewOwner { node: parent, label: keccak256(LABEL), owner: HOLDER.parse()? }.encode_log_data(), old, 120, 0),
            emitted(NewResolver { node, resolver: GRANTEE.parse()? }.encode_log_data(), old, 120, 1),
            emitted(Transfer { node, owner: HOLDER.parse()? }.encode_log_data(), current, 121, 0),
            emitted(Transfer { node, owner: GRANTEE.parse()? }.encode_log_data(), current, 122, 0),
        ];
        history_v1_payments::seed_and_run(&database, CHAIN, &logs, 122).await?;
        let initial: Vec<(String, Value)> = sqlx::query_as("SELECT event_kind,after_state FROM normalized_events WHERE block_number=121 AND log_index=0 AND source_family='ens_v1_registry_l1'")
            .fetch_all(&database.pool).await?;
        assert!(!initial.iter().any(|(kind, _)| kind == "AuthorityTransferred"));
        assert!(initial.iter().any(|(kind, after)| kind == "ResolverChanged" && after["registry_fallback_handoff"] == true && after["source_event"] == "Transfer"), "{initial:#?}");
        let later: Value = sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE block_number=122 AND event_kind='AuthorityTransferred'")
            .fetch_one(&database.pool).await?;
        assert!(later.get("authority_kind").is_some() && later.get("authority_key").is_some());
        let body = page(&database, &format!("/v1/events?contract_address={current:#x}&kind=AuthorityTransferred,ResolverChanged")).await?;
        assert!(action_rows(&body, "registry_handoff").is_empty(), "{body:#}");
        let owners = action_rows(&body, "authority_changed");
        assert_eq!(owners.len(), 1, "{body:#}");
        assert_eq!(owners[0]["block_number"], 122);
        assert_ordinary_owner(owners[0]);
        assert_eq!(action_rows(&body, "resolver_changed").len(), 1, "{body:#}");
        assert_eq!(body["page"]["total_count"], 2);
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}
