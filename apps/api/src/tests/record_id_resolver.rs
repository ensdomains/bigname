#[tokio::test]
async fn record_id_resolver_inventory_serves_explicit_empty_values() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_record_id_resolver_inputs(&database, &[
        family_fixture_record_write("text:url", Some(json!(""))),
        family_fixture_record_write("addr:60", Some(json!("0x"))),
        family_fixture_record_write("contenthash", Some(json!("0x"))),
    ], false).await?;
    let payload = v2_name_record_payload_for_database(&database,
        "/v1/names/alice.eth/records?keys=text:url,addr:60,contenthash&include=inventory").await?;
    assert_eq!(payload["data"]["records"]["text:url"]["status"], "ok");
    assert_eq!(payload["data"]["records"]["text:url"]["value"], "");
    for key in ["addr:60", "contenthash"] {
        assert_eq!(payload["data"]["records"][key]["status"], "not_found");
    }
    database.cleanup().await
}

#[tokio::test]
async fn record_id_resolver_permissions_preserve_generation_specific_powers() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    replace_permission_resolver_roles(&database, 120,
        json!(["set_abi", "set_interface", "set_name", "set_data", "link", "admin_link"]), None).await?;
    let payload = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;
    let row = payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["grant_scope"]["kind"] == "resolver")
        .expect("resolver permission");
    assert_eq!(
        row["powers"],
        json!([
            "set_abi",
            "set_interface",
            "set_name",
            "set_data",
            "link",
            "admin_link"
        ])
    );
    database.cleanup().await
}

// A grant on a record-ID resolver is scoped to a setter argument -- the resource is the
// keccak of the argument decodeSetter extracts -- and the row says which record that
// argument names, on /v1/permissions and on the resolver's /roles rows.
// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L307-L338 @ ens_v2@a971bd64)
#[tokio::test]
async fn record_id_resolver_permissions_describe_the_argument_scoped_record() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let hash = "0x00000000000000000000000000000000000000000000000000000000000000aa";
    replace_permission_resolver_roles(&database, 120, json!(["set_text", "link"]),
        Some(json!({"kind":"text", "key":"url", "hash":hash}))).await?;
    let payload = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;
    let rows = payload["data"].as_array().unwrap();
    let row = rows
        .iter()
        .find(|row| row["grant_scope"]["kind"] == "resolver")
        .expect("resolver permission");
    assert_eq!(
        row["record_resource"],
        json!({"kind": "text", "hash": hash, "key": "url"})
    );
    assert!(
        rows.iter()
            .filter(|row| row["grant_scope"]["kind"] != "resolver")
            .all(|row| row.get("record_resource").is_none()),
        "only argument-scoped resolver grants describe a record"
    );
    assert!(row["grant_scope"]["detail"].get("resource_selector").is_none());

    let roles = v2_resolver_payload_for_database(
        &database,
        "/v1/resolvers/1/0x0000000000000000000000000000000000000abc/roles",
    )
    .await?;
    let role = roles["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["registration_id"] == v2_permissions_current_resource_id().to_string())
        .expect("role row for the argument-scoped resource");
    assert_eq!(
        role["record_resource"],
        json!({"kind": "text", "hash": hash, "key": "url"})
    );
    assert!(role.get("record_resource_selector").is_none());
    assert_eq!(role["powers"], json!(["set_text", "link"]));

    // A coin type is a number on the wire, on both routes.
    replace_permission_resolver_roles(&database, 121, json!(["set_addr"]),
        Some(json!({"kind":"address", "key":"2147483658", "hash":hash}))).await?;
    let payload = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;
    let row = payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["grant_scope"]["kind"] == "resolver")
        .expect("resolver permission");
    assert_eq!(
        row["record_resource"],
        json!({"kind": "address", "hash": hash, "coin_type": 2147483658u64})
    );
    let roles = v2_resolver_payload_for_database(
        &database,
        "/v1/resolvers/1/0x0000000000000000000000000000000000000abc/roles",
    )
    .await?;
    let role = roles["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["registration_id"] == v2_permissions_current_resource_id().to_string())
        .expect("role row for the argument-scoped resource");
    assert!(role["record_resource"]["coin_type"].is_u64(), "{role}");

    // The text setter revoked: the argument still names the resource, but it is no
    // longer a record this holder may set, so neither row describes it.
    replace_permission_resolver_roles(&database, 122, json!(["link"]),
        Some(json!({"kind":"text", "key":"url", "hash":hash}))).await?;
    let payload = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;
    let row = payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["grant_scope"]["kind"] == "resolver")
        .expect("resolver permission");
    assert!(row.get("record_resource").is_none(), "{row}");
    let roles = v2_resolver_payload_for_database(
        &database,
        "/v1/resolvers/1/0x0000000000000000000000000000000000000abc/roles",
    )
    .await?;
    let role = roles["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["registration_id"] == v2_permissions_current_resource_id().to_string())
        .expect("role row for the argument-scoped resource");
    assert!(role.get("record_resource").is_none(), "{role}");
    database.cleanup().await
}

#[tokio::test]
async fn record_id_resolver_default_rule_derives_eth_address_in_indexed_and_auto() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_record_id_resolver_inputs(&database, &[
        family_fixture_record_write("addr:2147483648", Some(json!("0x3333333333333333333333333333333333333333"))),
    ], true).await?;
    for source in ["indexed", "auto"] {
        let payload = v2_name_record_payload_for_database(&database,
            &format!("/v1/names/alice.eth/records?source={source}&keys=addr:60")).await?;
        assert_eq!(payload["meta"]["source"], "indexed");
        assert_eq!(payload["data"]["records"]["addr:60"]["status"], "ok");
        assert_eq!(
            payload["data"]["records"]["addr:60"]["value"],
            "0x3333333333333333333333333333333333333333"
        );
        assert_eq!(
            payload["data"]["records"]["addr:60"]["meta"]["rule"],
            "ensip19_default_address"
        );
    }
    database.cleanup().await
}

/// Publish linked record-ID writes with a retained upgrade to a declared implementation.
async fn seed_record_id_resolver_inputs(
    database: &TestDatabase,
    writes: &[Value],
    default_address: bool,
) -> Result<()> {
    seed_unknown_resolver_inputs(database, writes).await?;
    let implementation = "0x0000000000000000000000000000000000000fed";
    let (manifest, mut payload): (i64, Value) = sqlx::query_as(
        "SELECT manifest_id,manifest_payload FROM manifest_versions
        WHERE source_family = 'ens_v2_resolver_l1'",
    )
    .fetch_one(&database.pool)
    .await?;
    payload["resolver_implementations"] = json!([{"role":"permissioned_resolver","address":implementation,
        "read_features":if default_address { json!(["ensip19_default_address"]) } else { json!([]) }}]);
    sqlx::query("UPDATE manifest_versions SET manifest_payload=$2 WHERE manifest_id=$1")
        .bind(manifest)
        .bind(&payload)
        .execute(&database.pool)
        .await?;
    seed_fixture_manifest_update(
        &database.pool,
        manifest,
        "ethereum-mainnet",
        "ens",
        "ens_v2_resolver_l1",
        &payload,
    )
    .await?;
    let mut upgrade = history_event(
        "record-id-fixture-upgrade",
        None,
        None,
        Some("ethereum-mainnet"),
        Some(21_000_003),
        Some("0xbinding"),
        Some("0xrecord-id-upgrade"),
        Some(10),
        CanonicalityState::Canonical,
    );
    upgrade.event_kind = "Upgraded".into();
    upgrade.source_family = "ens_v2_resolver_l1".into();
    upgrade.source_manifest_id = Some(manifest);
    upgrade.manifest_version = 1;
    upgrade.before_state = json!({});
    upgrade.after_state = json!({"source_event":"Upgraded", "proxy_address":"0x0000000000000000000000000000000000000abc", "implementation":implementation});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[upgrade]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}
