// TYR-85: batch lookup classifies a name as name detail does.

/// The name-level fields a `profile=detail` lookup record and `GET /v1/names/{name}` share.
const SHARED_DETAIL_FIELDS: &[&str] = &[
    "status",
    "unsupported_reason",
    "failure_reason",
    "unsupported_fields",
    "registration_status",
    "registration_id",
    "token_id",
    "owner",
    "registrant",
    "registered_at",
    "created_at",
    "expires_at",
    "lapsed_registration",
    "authority",
    "migrated_at",
    "resolver",
    "subregistry",
    "records",
    "primary_address",
    "primary_name",
    "chain_id",
    "network",
];

async fn assert_lookup_detail_matches_name_detail(
    database: &TestDatabase,
    name: &str,
) -> Result<Value> {
    let detail = v2_name_record_payload_for_database(database, &format!("/v1/names/{name}")).await?;
    let batch = v2_lookup_json(
        database,
        json!({"profile": "detail", "inputs": [{"name": name}]}),
    )
    .await?;
    let result = &batch["data"][0];
    let record = &result["record"];
    for field in SHARED_DETAIL_FIELDS {
        assert_eq!(
            record.get(*field),
            detail["data"].get(*field),
            "{name}: `{field}` differs\nlookup: {record}\ndetail: {}",
            detail["data"]
        );
    }
    assert_eq!(result["status"], record["status"], "{name}: {result}");
    assert_eq!(
        batch["meta"]["as_of"], detail["meta"]["as_of"],
        "{name}: both routes read one snapshot"
    );
    Ok(detail["data"].clone())
}

#[tokio::test]
async fn lookup_detail_status_matches_name_detail_for_a_registered_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_reverse_fixture(&database, "0x0000000000000000000000000000000000000abc").await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, "bob.eth").await?;
    assert_eq!(detail["status"], json!("ok"));
    database.cleanup().await
}

#[tokio::test]
async fn lookup_detail_status_matches_name_detail_without_projected_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // alice.eth keeps retained registration inputs, but no binding is observed for its surface,
    // so the row is `current_authority_not_projected`: the $2442.eth shape.
    seed_alice_state_inputs(&database, AliceInputState::Unbound).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, "alice.eth").await?;
    assert_eq!(detail["status"], json!("ok"), "{detail}");
    assert!(detail.get("unsupported_reason").is_none(), "{detail}");
    assert_eq!(detail["registration_status"], json!("unregistered"));
    assert!(detail.get("owner").is_none(), "{detail}");
    assert!(detail.get("authority").is_none(), "{detail}");
    assert!(detail.get("resolver").is_none(), "{detail}");

    let feed = v2_lookup_json(
        &database,
        json!({"profile": "feed", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    assert_eq!(feed["data"][0]["status"], json!("ok"), "{feed}");
    assert!(feed["data"][0]["record"].get("unsupported_reason").is_none());
    database.cleanup().await
}

#[tokio::test]
async fn lookup_detail_status_matches_name_detail_for_a_root_registry_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_unbound_name_inputs(&database, "eth", true).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, "eth").await?;
    assert_eq!(detail["status"], json!("ok"), "{detail}");
    assert_eq!(detail["registration_status"], json!("unregistered"));
    assert!(detail["resolver"].is_object(), "{detail}");
    database.cleanup().await
}

#[tokio::test]
async fn lookup_detail_status_matches_name_detail_for_released_and_reserved_names() -> Result<()> {
    for (state, registration_status) in [
        (AliceInputState::Released, "released"),
        (AliceInputState::Reserved, "unregistered"),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, state).await?;
        let detail = assert_lookup_detail_matches_name_detail(&database, "alice.eth").await?;
        assert_eq!(detail["status"], json!("ok"), "{detail}");
        assert_eq!(detail["registration_status"], json!(registration_status));
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn lookup_detail_status_matches_name_detail_with_a_historical_arm_and_no_binding() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // No binding is observed for alice.eth, but a named ENSv1 registration event votes the arm.
    // The row stays `current_authority_not_projected` and unregistered, and `authority` follows
    // the voted arm on both routes: no selected binding does not by itself omit `authority`.
    seed_alice_state_inputs(&database, AliceInputState::Unbound).await?;
    append_alice_name_input(
        &database,
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        json!({"authority_kind": "registrar",
            "registrant": "0x00000000000000000000000000000000000000aa",
            "expiry": 4_000_000_000_u64}),
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, "alice.eth").await?;
    assert_eq!(detail["status"], json!("ok"), "{detail}");
    assert_eq!(detail["registration_status"], json!("unregistered"), "{detail}");
    assert!(detail.get("registration_id").is_none(), "{detail}");
    assert!(detail.get("resolver").is_none(), "{detail}");
    assert_eq!(detail["authority"], json!("ens_v1"), "{detail}");
    database.cleanup().await
}

#[tokio::test]
async fn lookup_detail_status_matches_name_detail_for_wrapped_and_ownerless_names() -> Result<()> {
    for state in [AliceInputState::Wrapped, AliceInputState::Ownerless] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, state).await?;
        let detail = assert_lookup_detail_matches_name_detail(&database, "alice.eth").await?;
        assert_eq!(detail["status"], json!("ok"), "{detail}");
        database.cleanup().await?;
    }
    Ok(())
}

/// Lookup serves name detail's grouped `records`, and omits it where name detail does.
#[tokio::test]
async fn lookup_detail_records_match_name_detail() -> Result<()> {
    for (name, served) in [("eth", true), ("alice.eth", false)] {
        let database = TestDatabase::new_migrated().await?;
        if served {
            seed_unbound_name_inputs(&database, name, true).await?;
        } else {
            seed_alice_state_inputs(&database, AliceInputState::Unbound).await?;
        }
        let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
        assert_eq!(detail.get("records").is_some(), served, "{name}: {detail}");
        database.cleanup().await?;
    }
    Ok(())
}

/// A resolver whose implementation is not an admitted profile: the keys it was seen writing are
/// listed on both routes, with no value.
#[tokio::test]
async fn lookup_detail_records_list_an_unknown_resolvers_keys_without_values() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, "alice.eth").await?;
    let records = &detail["records"];
    assert_eq!(records["address_keys"], json!(["60"]), "{detail}");
    assert_eq!(records["addresses"], json!({}), "{detail}");
    assert_eq!(records["text_keys"], json!(["description"]), "{detail}");
    assert_eq!(records["texts"], json!({}), "{detail}");
    assert!(records.get("contenthash").is_none(), "{detail}");
    assert!(records.get("name").is_none(), "{detail}");
    database.cleanup().await
}

const GROUPED_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";

async fn seed_grouped_records_name(database: &TestDatabase, name: &str, id: u128) -> Result<()> {
    seed_identity_name(
        database,
        &format!("ens:{name}"),
        name,
        name,
        &format!("namehash:{name}"),
        Uuid::from_u128(id),
        Uuid::from_u128(id + 1),
        Uuid::from_u128(id + 2),
        GROUPED_RESOLVER,
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await
}

/// A cleared value is a listed key mapped to `null` (a cleared singleton is `null`) on both routes.
#[tokio::test]
async fn lookup_detail_records_serve_cleared_values_as_null() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let name = "cleared.eth";
    seed_grouped_records_name(&database, name, 0x5a0c10).await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        name,
        GROUPED_RESOLVER,
        38,
        "0xname26",
        &[
            family_fixture_record_write("text:url", Some(json!("https://cleared.example"))),
            family_fixture_record_write("text:email", Some(json!("kept@example.test"))),
            json!({"source_event":"ContenthashChanged", "record_key":"contenthash",
                "record_family":"contenthash", "selector_key":null,
                "contenthash_hex":"0xe3010170"}),
            family_fixture_record_write("text:url", Some(json!(""))),
            json!({"source_event":"ContenthashChanged", "record_key":"contenthash",
                "record_family":"contenthash", "selector_key":null, "contenthash_hex":"0x"}),
        ],
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 38, "0xname26").await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
    let records = &detail["records"];
    // `seed_identity_name` also writes the fixture's own text records.
    let text_keys = records["text_keys"].as_array().expect("text keys");
    assert!(text_keys.contains(&json!("url")) && text_keys.contains(&json!("email")), "{detail}");
    assert_eq!(records["texts"]["email"], json!("kept@example.test"), "{detail}");
    assert_eq!(records["texts"].get("url"), Some(&Value::Null), "{detail}");
    assert_eq!(records.get("contenthash"), Some(&Value::Null), "{detail}");
    database.cleanup().await
}

/// The forward `name` record on the name's own node: a write, a clear, a rewrite and a
/// record-version reset, served alike on both routes. It is not the primary name.
#[tokio::test]
async fn lookup_detail_records_follow_the_forward_name_record() -> Result<()> {
    const CHAIN: &str = "ethereum-mainnet";
    const FAMILY: &str = "ens_v1_resolver_l1";
    let database = TestDatabase::new_migrated().await?;
    let name = "forward.eth";
    seed_grouped_records_name(&database, name, 0x5a0c20).await?;
    let manifest =
        declare_family_fixture_resolver(&database.pool, "ens", CHAIN, FAMILY, GROUPED_RESOLVER)
            .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let publish = |kind: &'static str, after: Value| {
        let database = &database;
        async move {
            publish_node_record_event(
                database,
                CHAIN,
                "ens",
                kind,
                FAMILY,
                Some(manifest),
                GROUPED_RESOLVER,
                (None, None),
                after,
            )
            .await
        }
    };
    let name_changed = |value: &str| {
        json!({"source_event":"NameChanged", "resolver":GROUPED_RESOLVER, "node":node,
            "record_key":"name", "record_family":"name", "selector_key":null,
            "value_retained":false, "raw_name":value})
    };
    let forward_name = |detail: &Value| detail["records"].get("name").cloned();

    // Never written, on an authoritative inventory: unset.
    let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
    assert_eq!(forward_name(&detail), Some(Value::Null), "{detail}");

    publish("RecordChanged", name_changed("forward-target.eth")).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
    assert_eq!(forward_name(&detail), Some(json!("forward-target.eth")), "{detail}");
    assert!(detail.get("primary_name").is_none(), "{detail}");

    // `setName("")` clears it.
    publish("RecordChanged", name_changed("")).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
    assert_eq!(forward_name(&detail), Some(Value::Null), "{detail}");

    // A rewrite counts again, and a record-version reset drops it.
    publish("RecordChanged", name_changed("rewritten.eth")).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
    assert_eq!(forward_name(&detail), Some(json!("rewritten.eth")), "{detail}");
    publish("RecordVersionChanged", version_after(&node, GROUPED_RESOLVER, 1)).await?;
    let detail = assert_lookup_detail_matches_name_detail(&database, name).await?;
    assert_eq!(forward_name(&detail), Some(Value::Null), "{detail}");
    database.cleanup().await
}
