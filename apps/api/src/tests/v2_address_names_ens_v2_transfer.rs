// The `manager` relation of an ENSv2 name across registry token transfers, over registry logs
// run through the production adapter. A transfer moves the sender's roles on the name to the
// recipient in the same transaction, so the recipient becomes the manager.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L517-L547 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L236-L252 @ ens_v2_sepolia_20261001@07e55a05)
// Every published state is checked against the rule in docs/api-v1.md § Manager: an account
// is listed under `manager` exactly when the name serves it as its `manager`, on the address
// names route, in reverse lookup and in address history.
use super::*;

const FAR: u64 = 1_800_000_000;
const ACCOUNTS: [&str; 3] = [ALICE, BOB, OPERATOR];

/// `from` sends the token to `to`, which moves `roles`, every role `from` holds on the name, to
/// `to`.
fn send(logs: &mut Logs, block: i64, token: U256, from: &str, to: &str, roles: U256) {
    logs.transfer(block, token, from, to)
        .roles(block, versioned(0), from, roles, U256::ZERO)
        .roles(block, versioned(0), to, U256::ZERO, roles);
}

/// alice.eth registered to Alice at 121, sent with all her roles to Bob at 124.
fn sent_to_bob() -> Logs {
    let mut logs = Logs::default();
    logs.roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO, bit(0) | bit(128))
        .register(121, FAR);
    send(&mut logs, 124, versioned(0), ALICE, BOB, owner_roles());
    logs
}

/// The owner of token `version` changes the roles of `account` from `old` to `new`, which
/// regenerates the token.
fn change_roles(logs: &mut Logs, block: i64, version: u32, owner: &str, account: &str,
    old: U256, new: U256) {
    let (token, next) = (versioned(version), versioned(version + 1));
    logs.roles(block, versioned(0), account, old, new)
        .transfer(block, token, owner, ZERO)
        .push(block, TokenRegenerated { oldTokenId: token, newTokenId: next }.encode_log_data())
        .transfer(block, next, ZERO, owner);
}

async fn get(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

/// `(name, relations)` of each address-names row of `address` under `query`.
async fn listed(database: &TestDatabase, address: &str, query: &str) -> Result<Vec<(String, Value)>> {
    let page = get(database, &format!("/v1/addresses/{address}/names?namespace=ens&{query}")).await?;
    Ok(row_names_and_relations(&page))
}

/// The names reverse lookup returns for `address` under `relation`.
async fn looked_up(database: &TestDatabase, address: &str, relation: &str) -> Result<Vec<String>> {
    let payload = v2_lookup_json(database,
        json!({"inputs": [{"address": address, "relation": relation}]})).await?;
    Ok(lookup_record_names(&payload).into_iter().map(str::to_owned).collect())
}

async fn history(database: &TestDatabase, address: &str, relation: &str) -> Result<Value> {
    Ok(get(database, &format!("/v1/addresses/{address}/history?relation={relation}")).await?["data"]
        .clone())
}

/// Every read the invariant covers, for each account, at the current publication.
async fn reads(database: &TestDatabase) -> Result<Value> {
    reads_of(database, ETH_REGISTRY.name).await
}

/// `reads` of the name `name`.
async fn reads_of(database: &TestDatabase, name: &str) -> Result<Value> {
    let detail = get(database, &format!("/v1/names/{name}")).await?;
    let mut accounts = serde_json::Map::new();
    for account in ACCOUNTS {
        accounts.insert(account.to_owned(), json!({
            "manager": listed(database, account, "relation=manager").await?,
            "owner": listed(database, account, "relation=owner").await?,
            "owner_manager": listed(database, account, "relation=owner,manager").await?,
            "any": get(database, &format!(
                "/v1/addresses/{account}/names?namespace=ens&relation=any&include=role_summary"))
                .await?["data"].clone(),
            "lookup_manager": looked_up(database, account, "manager").await?,
            "lookup_any": looked_up(database, account, "any").await?,
            "history_manager": history(database, account, "manager").await?,
            "history_owner": history(database, account, "owner").await?,
        }));
    }
    Ok(json!({"name": name, "owner": detail["data"]["owner"], "manager": detail["data"]["manager"],
        "accounts": accounts}))
}

/// The served `manager` of the name is the one account the name is listed for under
/// `manager`, and, as an ENSv2 token's holder is also its manager, under `owner`, on the
/// address names route and in reverse lookup. Each account's address history lists the name's
/// events under `manager` as it does under `owner`, so an account that held the token keeps the
/// name's history under both, and the manager's is not empty.
fn assert_manager_agrees(reads: &Value, at: &str) {
    let manager = reads["manager"].as_str();
    assert_eq!(reads["owner"].as_str(), manager, "{at}: {reads:#}");
    for account in ACCOUNTS {
        let read = &reads["accounts"][account];
        let names = if manager == Some(account) { json!([reads["name"]]) } else { json!([]) };
        let listed_under = |key: &str| json!(read[key].as_array().unwrap().iter()
            .map(|row| row[0].clone()).collect::<Vec<_>>());
        assert_eq!(listed_under("manager"), names, "{at}: {account} manager: {reads:#}");
        assert_eq!(listed_under("owner"), names, "{at}: {account} owner: {reads:#}");
        assert_eq!(read["lookup_manager"], names, "{at}: {account} lookup: {reads:#}");
        assert_eq!(read["lookup_any"], names, "{at}: {account} lookup any: {reads:#}");
        let named = |key: &str| named_history(reads, account, key);
        assert_eq!(named("history_manager"), named("history_owner"),
            "{at}: {account} history: {reads:#}");
        if manager == Some(account) {
            assert!(!named("history_manager").is_empty(), "{at}: {account} history: {reads:#}");
        }
    }
}

/// The name's rows of `account`'s address history under one relation.
fn named_history(reads: &Value, account: &str, key: &str) -> Vec<Value> {
    reads["accounts"][account][key].as_array().unwrap().iter()
        .filter(|row| row["name"] == reads["name"]).cloned().collect()
}

fn relations_of<'a>(reads: &'a Value, account: &str, key: &str) -> &'a Value {
    &reads["accounts"][account][key]
}

/// Every family table's rows, as the publication left them. The history catalogue marker is
/// left out: an undo moves it to the publication it returns to, which can be below the block
/// asked for.
async fn families(database: &TestDatabase) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for table in bigname_project::families::family_tables() {
        if table == "project_history_catalogue_marker" {
            continue;
        }
        let data: String = sqlx::query_scalar(&format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]')::text FROM {table} t"))
            .fetch_one(&database.pool).await?;
        out.push((table.into(), data));
    }
    Ok(out)
}

/// Alice registers, an operator gets a role of its own (which regenerates the token on the
/// same resource), and Alice sends the token with all her roles to Bob. Alice then has no relation to the name, on the live publication, after a rebuild, and
/// after an undo below the transfer and its replay.
#[tokio::test]
async fn the_sender_of_an_ens_v2_transfer_is_no_longer_the_manager() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&database, registry_logs(), true).await?;

    publish(&database, &output, 123).await?;
    let before = reads(&database).await?;
    let stored_before = families(&database).await?;
    assert_manager_agrees(&before, "123");
    assert_eq!(before["manager"], json!(ALICE), "{before:#}");
    // The operator's role grant regenerated the token and left Alice the manager.
    assert_eq!(relations_of(&before, ALICE, "manager"), &json!([["alice.eth", ["manager"]]]),
        "{before:#}");
    assert_eq!(relations_of(&before, ALICE, "any")[0]["relations"],
        json!(["owner", "manager", "role_holder"]), "{before:#}");

    publish(&database, &output, 124).await?;
    let after = reads(&database).await?;
    let stored_after = families(&database).await?;
    assert_manager_agrees(&after, "124");
    assert_eq!(after["manager"], json!(BOB), "{after:#}");
    assert_eq!(relations_of(&after, ALICE, "manager"), &json!([]), "{after:#}");
    assert_eq!(relations_of(&after, ALICE, "any"), &json!([]), "{after:#}");
    assert_eq!(relations_of(&after, ALICE, "owner_manager"), &json!([]), "{after:#}");
    assert_eq!(relations_of(&after, BOB, "owner_manager"),
        &json!([["alice.eth", ["owner", "manager"]]]), "{after:#}");
    assert_eq!(relations_of(&after, BOB, "manager"), &json!([["alice.eth", ["manager"]]]),
        "{after:#}");
    assert_eq!(relations_of(&after, BOB, "any")[0]["relations"],
        json!(["owner", "manager", "role_holder"]), "{after:#}");
    assert_eq!(relations_of(&after, ALICE, "lookup_manager"), &json!([]), "{after:#}");
    // Alice's history keeps the name's events under both relations she held.
    assert!(!named_history(&after, ALICE, "history_manager").is_empty(), "{after:#}");

    rebuild_fixture_families(&database.pool, CHAIN, 124, "0xhistory124").await?;
    assert_eq!(families(&database).await?, stored_after, "after a rebuild at 124");
    assert_eq!(reads(&database).await?, after, "after a rebuild at 124");

    // The API serves no snapshot between an undo and its replay, so the undo is checked on the
    // stored rows.
    assert_eq!(bigname_project::families::undo_to(&database.pool, CHAIN, 123).await?, 1);
    assert_eq!(families(&database).await?, stored_before, "after an undo to 123");
    publish(&database, &output, 124).await?;
    assert_eq!(families(&database).await?, stored_after, "after replaying 124");
    assert_eq!(reads(&database).await?, after, "after replaying 124");
    database.cleanup().await
}

/// Bob grants Alice a role in the block after the transfer. Alice holds a role and is not the
/// manager. One publication over both blocks serves what two publications serve.
#[tokio::test]
async fn a_role_granted_back_after_a_transfer_is_not_the_manager() -> Result<()> {
    let mut logs = sent_to_bob();
    change_roles(&mut logs, 125, 0, BOB, ALICE, U256::ZERO, bit(24));
    let copy = |logs: &Logs| Logs(logs.0.clone(), logs.1);
    let split = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&split, copy(&logs), true).await?;
    publish(&split, &output, 124).await?;
    assert_manager_agrees(&reads(&split).await?, "split 124");
    publish(&split, &output, 125).await?;
    let stepped = reads(&split).await?;

    let once = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&once, copy(&logs), true).await?;
    publish(&once, &output, 125).await?;
    let whole = reads(&once).await?;

    assert_manager_agrees(&stepped, "125");
    assert_eq!(stepped["manager"], json!(BOB), "{stepped:#}");
    assert_eq!(relations_of(&stepped, ALICE, "any")[0]["relations"], json!(["role_holder"]),
        "{stepped:#}");
    assert_eq!(whole, stepped, "one publication over 121..=125");
    split.cleanup().await?;
    once.cleanup().await
}

/// Alice gives up `set_resolver` and keeps the token: she is still the manager, which is the
/// registry owner, not a role.
#[tokio::test]
async fn a_partial_revoke_keeps_the_owner_the_manager() -> Result<()> {
    let mut logs = Logs::default();
    logs.roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO, bit(0) | bit(128))
        .register(121, FAR);
    change_roles(&mut logs, 122, 0, ALICE, ALICE, owner_roles(), owner_roles() ^ bit(24));
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&database, logs, true).await?;
    publish(&database, &output, 121).await?;
    let before = reads(&database).await?;
    publish(&database, &output, 122).await?;
    let after = reads(&database).await?;
    for (at, reads) in [("121", &before), ("122", &after)] {
        assert_manager_agrees(reads, at);
        assert_eq!(reads["manager"], json!(ALICE), "{at}: {reads:#}");
    }
    assert_eq!(relations_of(&after, ALICE, "manager"), relations_of(&before, ALICE, "manager"));
    database.cleanup().await
}

/// An operator Alice approved, and later one Bob approved, has no relation to the name: it
/// shows in `role_summary` only.
#[tokio::test]
async fn an_approved_operator_has_no_relation_before_or_after_a_transfer() -> Result<()> {
    let mut logs = Logs::default();
    logs.roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO, bit(0) | bit(128))
        .register(121, FAR)
        .approve(122, ALICE, OPERATOR, true);
    send(&mut logs, 124, versioned(0), ALICE, BOB, owner_roles());
    logs.approve(125, BOB, OPERATOR, true);
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&database, logs, true).await?;
    for (block, owner) in [(123, ALICE), (124, BOB), (125, BOB)] {
        publish(&database, &output, block).await?;
        let reads = reads(&database).await?;
        assert_manager_agrees(&reads, &block.to_string());
        assert_eq!(reads["manager"], json!(owner), "{block}: {reads:#}");
        assert_eq!(relations_of(&reads, OPERATOR, "any"), &json!([]), "{block}: {reads:#}");
        let summary = relations_of(&reads, owner, "any")[0]["role_summary"].as_array()
            .context("role_summary")?;
        let operators = summary.iter().filter(|holder| holder["address"] == OPERATOR)
            .flat_map(|holder| holder["grants"].as_array().unwrap().iter()
                .map(|grant| grant["grant_scope"]["detail"]["owner"].clone()))
            .collect::<Vec<_>>();
        let approved = if block == 124 { vec![] } else { vec![json!(owner)] };
        assert_eq!(operators, approved, "{block}: {reads:#}");
    }
    database.cleanup().await
}

/// Bob sends the token back with all his roles: Alice is the manager again and Bob has no
/// relation. Bob was the manager only by the transfer, and his history keeps the name's events
/// under `manager`, on the live publication and after a rebuild.
#[tokio::test]
async fn a_transfer_back_returns_the_manager_relation() -> Result<()> {
    let mut logs = sent_to_bob();
    send(&mut logs, 125, versioned(0), BOB, ALICE, owner_roles());
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&database, logs, true).await?;
    for (block, manager) in [(123, ALICE), (124, BOB), (125, ALICE)] {
        publish(&database, &output, block).await?;
        let reads = reads(&database).await?;
        assert_manager_agrees(&reads, &block.to_string());
        assert_eq!(reads["manager"], json!(manager), "{block}: {reads:#}");
    }
    let back = reads(&database).await?;
    assert_eq!(relations_of(&back, BOB, "any"), &json!([]), "{back:#}");
    assert!(!named_history(&back, BOB, "history_manager").is_empty(), "{back:#}");
    rebuild_fixture_families(&database.pool, CHAIN, 125, "0xhistory125").await?;
    assert_eq!(reads(&database).await?, back, "after a rebuild at 125");
    database.cleanup().await
}

/// Alice sends the token to herself, which revokes her roles and grants them back. She stays the
/// manager, and her history keeps the name's earlier events under `manager` and adds the
/// self-transfer's, as under `owner`, on the live publication and after a rebuild.
#[tokio::test]
async fn a_self_transfer_keeps_the_manager_and_its_history() -> Result<()> {
    let mut logs = Logs::default();
    logs.roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO, bit(0) | bit(128))
        .register(121, FAR);
    send(&mut logs, 124, versioned(0), ALICE, ALICE, owner_roles());
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&database, logs, true).await?;
    let mut held = Vec::new();
    for block in [123, 124] {
        publish(&database, &output, block).await?;
        let reads = reads(&database).await?;
        assert_manager_agrees(&reads, &block.to_string());
        assert_eq!(reads["manager"], json!(ALICE), "{block}: {reads:#}");
        assert_eq!(relations_of(&reads, ALICE, "owner_manager"),
            &json!([["alice.eth", ["owner", "manager"]]]), "{block}: {reads:#}");
        held.push(named_history(&reads, ALICE, "history_manager"));
    }
    let after = reads(&database).await?;
    assert!(held[0].iter().all(|row| held[1].contains(row)), "{after:#}");
    assert!(held[1].iter().any(|row| row["block_number"] == 124), "{after:#}");
    rebuild_fixture_families(&database.pool, CHAIN, 124, "0xhistory124").await?;
    assert_eq!(reads(&database).await?, after, "after a rebuild at 124");
    database.cleanup().await
}

/// The root registry sends the top-level name alice with all Alice's roles to Bob, as the ETH
/// registry sends alice.eth: Bob is the manager and Alice has no relation to the name.
#[tokio::test]
async fn a_top_level_name_sent_on_the_root_registry_moves_the_manager_relation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed_in(&database, ROOT_REGISTRY, sent_to_bob(), true).await?;
    for (block, manager) in [(123, ALICE), (124, BOB)] {
        publish_in(&database, ROOT_REGISTRY, &output, block).await?;
        let reads = reads_of(&database, ROOT_REGISTRY.name).await?;
        assert_manager_agrees(&reads, &block.to_string());
        assert_eq!(reads["manager"], json!(manager), "{block}: {reads:#}");
    }
    let sent = reads_of(&database, ROOT_REGISTRY.name).await?;
    assert_eq!(relations_of(&sent, ALICE, "any"), &json!([]), "{sent:#}");
    assert_eq!(relations_of(&sent, BOB, "owner_manager"),
        &json!([["alice", ["owner", "manager"]]]), "{sent:#}");
    database.cleanup().await
}

/// The root holder unregisters Alice's name, revives the entry, and registers the reservation
/// to Bob. Bob's registration sets the manager and Alice has no relation to the name.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L227-L238 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn a_new_registration_to_another_account_moves_the_manager_relation() -> Result<()> {
    let mut logs = Logs::default();
    logs.roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO,
            bit(0) | bit(4) | bit(12) | bit(16) | bit(128))
        .register(121, FAR)
        .push(124, LabelUnregistered { tokenId: versioned(0), sender: account(ROOT_HOLDER) }.encode_log_data())
        .transfer(124, versioned(0), ALICE, ZERO)
        .push(125, ExpiryUpdated { tokenId: versioned(1), newExpiry: FAR,
            sender: account(ROOT_HOLDER) }.encode_log_data())
        .register_as(126, FAR, (1, 1), BOB, owner_roles() | bit(32));
    let database = TestDatabase::new_migrated().await?;
    let (_, output) = seed(&database, logs, true).await?;
    publish(&database, &output, 123).await?;
    let held = reads(&database).await?;
    assert_manager_agrees(&held, "123");
    assert_eq!(held["manager"], json!(ALICE), "{held:#}");
    publish(&database, &output, 126).await?;
    let reregistered = reads(&database).await?;
    assert_manager_agrees(&reregistered, "126");
    assert_eq!(reregistered["manager"], json!(BOB), "{reregistered:#}");
    database.cleanup().await
}

/// A name that migrated from ENSv1 to an ENSv2 registration, whose token then moves to
/// another account. The registration's own controller event names the registrant, as the
/// registry adapter writes it, and the transfer names the recipient.
#[tokio::test]
async fn a_migrated_name_sent_on_ens_v2_moves_the_manager_relation() -> Result<()> {
    const RECIPIENT: &str = "0x00000000000000000000000000000000000000f7";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let resource = bind_address_name_ens_v2(&database, "alpha.eth", 0xa200, true).await?;
    let (block, hash) = address_fixture_head(&database).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alpha.eth");
    let ordinal = NEXT_TEST_ID.fetch_add(2, Ordering::Relaxed) as i64 + 100;
    let registered = address_fixture_event("address-alpha-v2-authority", Some(&logical),
        Some(resource), "AuthorityTransferred", "ens_v2_registry_l1", block, &hash, ordinal,
        json!({"source_event": "LabelRegistered", "owner": V2_ADDRESS}));
    let mut sent = address_fixture_event("address-alpha-v2-transfer", Some(&logical),
        Some(resource), "TokenControlTransferred", "ens_v2_registry_l1", block, &hash,
        ordinal + 1, json!({"source_event": "TransferSingle", "to": RECIPIENT}));
    sent.before_state = json!({"from": V2_ADDRESS});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[registered, sent]).await?;
    rebuild_address_fixture(&database).await?;

    let detail = get(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(detail["data"]["manager"], json!(RECIPIENT), "{detail:#}");
    assert_eq!(detail["data"]["owner"], json!(RECIPIENT), "{detail:#}");
    let alpha = |rows: Vec<(String, Value)>| rows.into_iter()
        .filter(|(name, _)| name == "alpha.eth").map(|(_, relations)| relations).collect::<Vec<_>>();
    assert_eq!(alpha(listed(&database, V2_ADDRESS, "relation=any").await?), Vec::<Value>::new());
    assert_eq!(alpha(listed(&database, RECIPIENT, "relation=manager").await?),
        vec![json!(["manager"])]);
    assert_eq!(alpha(listed(&database, RECIPIENT, "relation=any").await?),
        vec![json!(["owner", "manager"])]);
    assert!(!looked_up(&database, V2_ADDRESS, "manager").await?.contains(&"alpha.eth".to_owned()));
    assert_eq!(looked_up(&database, RECIPIENT, "manager").await?, vec!["alpha.eth".to_owned()]);
    database.cleanup().await
}

sol! {
    event RegistryCreated();
}

/// A registry that no root reaches, admitted by the `RegistryCreated` its constructor emits,
/// registers the label to Alice, who sends the token with all her roles to Bob. The token has
/// a registration and no name, so the transfer moves no `manager`: Bob's history holds the
/// transfer under `owner` and nothing under `manager`, as no name lists him as its manager.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L117-L120 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L185-L190 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn a_transfer_in_a_registry_no_root_reaches_moves_no_manager_history() -> Result<()> {
    let mut logs = Logs::default();
    logs.push(120, RegistryCreated {}.encode_log_data())
        .roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO, bit(0) | bit(128))
        .register(121, FAR);
    send(&mut logs, 124, versioned(0), ALICE, BOB, owner_roles());
    let output = interpret_in(ETH_REGISTRY, logs, false)?;
    let sent = output.normalized_events.iter()
        .filter(|event| event.event_kind == "TokenControlTransferred" && event.block_number == Some(124))
        .collect::<Vec<_>>();
    assert_eq!(sent.len(), 1, "{sent:#?}");
    assert_eq!((sent[0].logical_name_id.as_deref(), sent[0].resource_id.is_some(),
        sent[0].source_family.as_str()), (None, true, "ens_v2_registry_l1"), "{sent:#?}");
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=132).await?;
    store_interpreted(&database, &output).await?;
    publish(&database, &output, 124).await?;
    let owner = history(&database, BOB, "owner").await?;
    assert!(owner.as_array().unwrap().iter().any(|row| row["block_number"] == 124), "{owner:#}");
    assert_eq!(history(&database, BOB, "manager").await?, json!([]));
    assert_eq!(listed(&database, BOB, "relation=manager").await?, Vec::<(String, Value)>::new());
    database.cleanup().await
}
