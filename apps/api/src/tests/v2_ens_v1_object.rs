// The `ens_v1` object on name-shaped rows (TYR-115): the BaseRegistrar lease date and the
// NameWrapper state while ENSv1 decides a name, absent while ENSv2 does.

/// Alice's lease, renewed to the given expiry, wrapped emancipated and premigrated into ENSv2
/// with the 62-day continuity bonus, with the Universal Resolver cut over: the shape of a live
/// wrapped Sepolia name such as nick.eth.
async fn seed_alice_wrapped_reserved_after_cutover(database: &TestDatabase) -> Result<()> {
    const LEASE_EXPIRY: u64 = 1_798_608_633;
    const RESERVED_EXPIRY: u64 = LEASE_EXPIRY + 62 * 86_400;
    seed_alice_name_inputs(database).await?;
    sqlx::query(
        "UPDATE normalized_events SET after_state = jsonb_set(after_state, '{expiry}', $1::jsonb)
         WHERE event_identity = 'alice-RegistrationGranted'",
    )
    .bind(json!(LEASE_EXPIRY))
    .execute(&database.pool)
    .await?;
    let holder = "0x00000000000000000000000000000000000000aa";
    append_alice_name_input(database, "AuthorityEpochChanged", "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "authority_kind":"wrapper", "owner":holder})).await?;
    append_alice_name_input(database, "TokenControlTransferred", "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "owner":holder})).await?;
    // PARENT_CANNOT_CONTROL | IS_DOT_ETH, as every wrapped `.eth` second-level name has.
    append_alice_name_input(database, "PermissionScopeChanged", "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "wrapper_state":"emancipated", "fuses":196_608})).await?;
    append_alice_name_input(database, "ExpiryChanged", "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "expiry":LEASE_EXPIRY + 90 * 86_400})).await?;

    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    let mut reservation = history_event(
        &format!("alice-premigrated-{ordinal}"),
        Some(&logical),
        None,
        Some("ethereum-mainnet"),
        Some(21_000_003),
        Some("0xbinding"),
        Some("0xalice-premigrated"),
        Some(ordinal),
        CanonicalityState::Canonical,
    );
    reservation.event_kind = "RegistrationReserved".into();
    reservation.source_family = "ens_v2_registry_l1".into();
    reservation.before_state = json!({});
    reservation.after_state = json!({"source_event":"LabelReserved", "status":"reserved",
        "expiry":RESERVED_EXPIRY, "token_id":"4294967297", "current_token_id":"4294967297",
        "registry_contract_instance_id":Uuid::from_u128(0x4400).to_string()});

    let execution_manifest = database
        .insert_manifest("ens", "ens_execution", "ethereum-mainnet", "cutover-fixture", 1, "active", "test")
        .await?;
    seed_fixture_manifest_update(
        &database.pool, execution_manifest, "ethereum-mainnet", "ens", "ens_execution",
        &json!({"contracts": [{"role": "universal_resolver",
            "address": "0xeeeeeeee14d718c2b47d9923deab1335e144eeee", "start_block": 0}],
            "universal_resolver_implementations": ["0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3"]}),
    ).await?;
    let mut upgraded = history_event(
        "alice-ens-v1-cutover-upgraded",
        None,
        None,
        Some("ethereum-mainnet"),
        Some(21_000_003),
        Some("0xbinding"),
        Some("0xcutover"),
        Some(900),
        CanonicalityState::Canonical,
    );
    upgraded.event_kind = "Upgraded".into();
    upgraded.source_family = "ens_execution".into();
    upgraded.before_state = json!({});
    upgraded.after_state = json!({"source_event": "Upgraded",
        "proxy_address": "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        "proxy_role": "universal_resolver",
        "implementation": "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3",
        "implementation_kind": "admitted_universal_resolver"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[reservation, upgraded])
        .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}

#[tokio::test]
async fn v2_ens_v1_object_serves_the_lease_and_wrapper_beside_the_ens_v2_reservation_expiry()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_wrapped_reserved_after_cutover(&database).await?;
    let expected = json!({
        "expires_at": "1798608633",
        "wrapper_state": "emancipated",
        "wrapper_fuses": {
            "fuses": 196_608, "cannot_unwrap": false, "cannot_burn_fuses": false,
            "cannot_transfer": false, "cannot_set_resolver": false, "cannot_set_ttl": false,
            "cannot_create_subdomain": false, "cannot_approve": false,
            "parent_cannot_control": true, "is_dot_eth": true, "can_extend_expiry": false
        }
    });
    let check = |row: &Value, route: &str| {
        assert_eq!(row["authority"], json!("ens_v1"), "{route}: {row:#}");
        assert_eq!(row["expires_at"], json!("1803965433"), "{route}: {row:#}");
        assert_eq!(row["grace_ends_at"], json!("1806384633"), "{route}: {row:#}");
        assert_eq!(row["ens_v1"], expected, "{route}: {row:#}");
        assert!(row.get("wrapper_state").is_none(), "{route}: {row:#}");
        assert!(row.get("wrapper_fuses").is_none(), "{route}: {row:#}");
    };

    let detail = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    check(&detail["data"], "name detail");
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    check(&lookup["data"][0]["record"], "lookup detail");
    let feed = v2_lookup_json(
        &database,
        json!({"profile": "feed", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    assert!(feed["data"][0]["record"].get("ens_v1").is_none(), "{feed:#}");
    let owned = v2_name_record_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000aa/names?namespace=ens",
    )
    .await?;
    check(&owned["data"][0], "address names");

    // The listing reads the same row; it has no `authority` field of its own.
    let listed = v2_name_record_payload_for_database(
        &database,
        "/v1/names?namespace=ens&expires_after=1803965432&expires_before=1803965434",
    )
    .await?;
    let row = &listed["data"][0];
    assert_eq!(row["name"], json!("alice.eth"), "{listed:#}");
    assert_eq!(row["ens_v1"], expected, "{listed:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_ens_v1_object_on_an_unwrapped_lease_and_its_absence_under_ens_v2() -> Result<()> {
    let unwrapped = v2_name_record_payload("/v1/names/alice.eth").await?;
    assert_eq!(unwrapped["data"]["authority"], json!("ens_v1"), "{unwrapped:#}");
    assert_eq!(
        unwrapped["data"]["ens_v1"],
        json!({"expires_at": "1798859045"}),
        "an unwrapped lease omits the wrapper fields: {unwrapped:#}"
    );

    let registered =
        v2_alice_state_payload("/v1/names/alice.eth", AliceInputState::Registry).await?;
    assert_eq!(registered["data"]["authority"], json!("ens_v2"), "{registered:#}");
    assert!(registered["data"].get("ens_v1").is_none(), "{registered:#}");
    Ok(())
}
