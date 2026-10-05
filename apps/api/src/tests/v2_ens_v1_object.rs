// The `ens_v1` object on name-shaped rows (TYR-115): the BaseRegistrar lease date and the
// NameWrapper state and expiry while ENSv1 decides a name, absent while ENSv2 does.

const ALICE_LEASE_EXPIRY: u64 = 1_798_608_633;

/// Alice's lease, registered with nick.eth's lease date and marked wrapped emancipated on the
/// lease resource itself, premigrated into ENSv2 with the Universal Resolver cut over. The
/// Sepolia testnet premigration registrar registers the BaseRegistrar lease and then reserves
/// the label in ENSv2 at the lease expiry plus the 62-day continuity bonus
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L177-L178 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L249-L266 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L38-L42 @ ens_v2_sepolia_20260916@366de741).
/// No manifest declares the premigration registrar's own address: it acts as a controller of
/// the BaseRegistrar declared in `manifests/sepolia/ethereum/ens/ens_v1_registrar_l1/v1.toml`
/// (`docs/manifests.md`, ENSv2 migration driver)
/// (upstream: .refs/ens_v2/contracts/script/migration.ts:L1594-L1607 @ ens_v2@a971bd64).
/// This is the response shape of a live wrapped Sepolia name such as nick.eth, whose lease
/// date the Sepolia check in `docs/deployment.md` records. The faithful wrapped-resource and
/// renewal paths are in `crates/project/tests/families_expiry_grace.rs`.
async fn seed_alice_wrapped_reserved_after_cutover(database: &TestDatabase) -> Result<()> {
    const LEASE_EXPIRY: u64 = ALICE_LEASE_EXPIRY;
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
    // PARENT_CANNOT_CONTROL | IS_DOT_ETH, as every wrapped `.eth` second-level name has: each
    // wrap path goes through `_wrapETH2LD`, which ORs both fuses in
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L272 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L298 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L808 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1015 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18-L19 @ ens_v1@91c966f).
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
        },
        "wrapper_expires_at": "1806384633"
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
    let feed = &feed["data"][0]["record"];
    assert_eq!(feed["expires_at"], json!("1803965433"), "lookup feed: {feed:#}");
    assert_eq!(feed["grace_ends_at"], json!("1806384633"), "lookup feed: {feed:#}");
    assert_eq!(feed["ens_v1"], expected, "lookup feed: {feed:#}");
    assert!(feed.get("authority").is_none(), "lookup feed: {feed:#}");
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
    let searched = alice_search_row(&database).await?;
    assert_eq!(searched["ens_v1"], expected, "search: {searched:#}");
    database.cleanup().await
}

async fn alice_search_row(database: &TestDatabase) -> Result<Value> {
    let search =
        v2_name_record_payload_for_database(database, "/v1/search?q=alice&namespace=ens").await?;
    search["data"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["name"] == json!("alice.eth"))
        .cloned()
        .with_context(|| format!("no alice.eth search row: {search:#}"))
}

/// The `ens_v1` object of alice.eth on name detail, lookup detail and feed, `/v1/names` and
/// search.
async fn alice_ens_v1_objects(database: &TestDatabase) -> Result<Vec<(&'static str, Value)>> {
    let detail = v2_name_record_payload_for_database(database, "/v1/names/alice.eth").await?;
    let lookup = |profile: &'static str| {
        v2_lookup_json(database, json!({"profile": profile, "inputs": [{"name": "alice.eth"}]}))
    };
    let lookup_detail = lookup("detail").await?;
    let feed = lookup("feed").await?;
    let listed = v2_name_record_payload_for_database(
        database,
        "/v1/names?namespace=ens&expires_after=1803965432&expires_before=1803965434",
    )
    .await?;
    let listed = listed["data"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["name"] == json!("alice.eth"))
        .cloned()
        .with_context(|| format!("no alice.eth listing row: {listed:#}"))?;
    Ok(vec![
        ("name detail", detail["data"]["ens_v1"].clone()),
        ("lookup detail", lookup_detail["data"][0]["record"]["ens_v1"].clone()),
        ("lookup feed", feed["data"][0]["record"]["ens_v1"].clone()),
        ("names listing", listed["ens_v1"].clone()),
        ("search", alice_search_row(database).await?["ens_v1"].clone()),
    ])
}

/// The current ENSv1 `ETHRegistrarController.renew` calls only `BaseRegistrar.renew`, never
/// `NameWrapper.renew`, so the NameWrapper entry keeps its expiry while the lease moves on
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352-L368 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L337 @ ens_v1@91c966f).
/// The served wrapper expiry is the entry's own, behind the renewed lease, not lease plus 90 days.
#[tokio::test]
async fn v2_ens_v1_wrapper_expiry_stays_behind_a_controller_only_renewal() -> Result<()> {
    const RENEWED: u64 = ALICE_LEASE_EXPIRY + 365 * 86_400;
    let database = TestDatabase::new_migrated().await?;
    seed_alice_wrapped_reserved_after_cutover(&database).await?;
    append_alice_name_input(&database, "RegistrationRenewed", "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed", "expiry":RENEWED})).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let mut objects = alice_ens_v1_objects(&database).await?;
    // The listing filters on the top-level expiry, the ENSv2 reservation, which did not move.
    let owned = v2_name_record_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000aa/names?namespace=ens",
    )
    .await?;
    objects.push(("address names", owned["data"][0]["ens_v1"].clone()));
    for (route, ens_v1) in objects {
        assert_eq!(ens_v1["expires_at"], json!(RENEWED.to_string()), "{route}: {ens_v1:#}");
        assert_eq!(ens_v1["wrapper_state"], json!("emancipated"), "{route}: {ens_v1:#}");
        assert_eq!(ens_v1["wrapper_expires_at"], json!("1806384633"), "{route}: {ens_v1:#}");
    }
    database.cleanup().await
}

/// Past its own expiry the NameWrapper drops an emancipated or locked name's owner and every
/// name's fuses (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f).
/// A lapsed emancipated or locked wrapper serves no state or fuses but keeps its past expiry; a
/// plain wrapped one keeps its state, zero fuses and the past expiry.
#[tokio::test]
async fn v2_ens_v1_wrapper_expiry_is_served_backed_and_lapsed() -> Result<()> {
    for (state, fuses, kept) in [
        ("emancipated", 196_608, false),
        ("locked", 196_609, false),
        ("wrapped", 0, true),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_wrapped_reserved_after_cutover(&database).await?;
        append_alice_name_input(&database, "PermissionScopeChanged", "ens_v1_wrapper_l1",
            json!({"source_event":"NameWrapped", "wrapper_state":state, "fuses":fuses})).await?;
        append_alice_name_input(&database, "ExpiryChanged", "ens_v1_wrapper_l1",
            json!({"source_event":"ExpiryExtended", "expiry":1_000_000})).await?;
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding")
            .await?;
        for (route, ens_v1) in alice_ens_v1_objects(&database).await? {
            let context = format!("{state} {route}: {ens_v1:#}");
            assert_eq!(ens_v1["expires_at"], json!("1798608633"), "{context}");
            assert_eq!(ens_v1["wrapper_expires_at"], json!("1000000"), "{context}");
            assert!(ens_v1.get("wrapper_expires_at_reason").is_none(), "{context}");
            if kept {
                assert_eq!(ens_v1["wrapper_state"], json!("wrapped"), "{context}");
                assert_eq!(ens_v1["wrapper_fuses"]["fuses"], json!(0), "{context}");
            } else {
                assert!(ens_v1.get("wrapper_state").is_none(), "{context}");
                assert!(ens_v1.get("wrapper_fuses").is_none(), "{context}");
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}

/// An unwrap burns the NameWrapper token, clearing its owner, while the burnt entry keeps its
/// fuses and expiry (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L279 @ ens_v1@91c966f).
/// The composed name does not read the wrapper lifecycle, so it still serves the old
/// `wrapper_state` (the known gap `crates/project/tests/families_retention.rs` pins), but the
/// expiry of the entry that was unwrapped is not served beside it. A rewrap serves it again.
#[tokio::test]
async fn v2_ens_v1_wrapper_expiry_is_omitted_once_the_wrapper_is_unwrapped() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_wrapped_reserved_after_cutover(&database).await?;
    append_alice_name_input(&database, "AuthorityEpochChanged", "ens_v1_wrapper_l1",
        json!({"source_event":"NameUnwrapped", "authority_kind":"wrapper"})).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let retained: (Option<String>, Option<bool>, bool) = sqlx::query_as(
        "SELECT wrapper_state, lifecycle_unwrapped, expiry_seconds IS NOT NULL
         FROM project_wrapper_state WHERE logical_name_id = $1",
    )
    .bind(bigname_storage::logical_name_id_for_name("ens", "alice.eth"))
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(retained, (Some("emancipated".to_owned()), Some(true), true));
    for (route, ens_v1) in alice_ens_v1_objects(&database).await? {
        let context = format!("{route}: {ens_v1:#}");
        assert_eq!(ens_v1["wrapper_state"], json!("emancipated"), "{context}");
        assert!(ens_v1.get("wrapper_expires_at").is_none(), "{context}");
        assert!(ens_v1.get("wrapper_expires_at_reason").is_none(), "{context}");
    }

    append_alice_name_input(&database, "TokenControlTransferred", "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped",
               "owner":"0x00000000000000000000000000000000000000aa"})).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    for (route, ens_v1) in alice_ens_v1_objects(&database).await? {
        assert_eq!(ens_v1["wrapper_expires_at"], json!("1806384633"), "{route}: {ens_v1:#}");
    }
    database.cleanup().await
}

/// The NameWrapper maximum expiry serves null with `no_expiry`
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L57 @ ens_v1@91c966f), and a zero
/// expiry null with `not_set`, on a backed and on a lapsed wrapper.
#[tokio::test]
async fn v2_ens_v1_wrapper_expiry_classifies_the_maximum_and_zero() -> Result<()> {
    for (state, fuses, expiry, reason) in [
        ("emancipated", 196_608, u64::MAX, "no_expiry"),
        ("wrapped", 0, 0, "not_set"),
        ("emancipated", 196_608, 0, "not_set"),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_wrapped_reserved_after_cutover(&database).await?;
        append_alice_name_input(&database, "PermissionScopeChanged", "ens_v1_wrapper_l1",
            json!({"source_event":"NameWrapped", "wrapper_state":state, "fuses":fuses})).await?;
        append_alice_name_input(&database, "ExpiryChanged", "ens_v1_wrapper_l1",
            json!({"source_event":"ExpiryExtended", "expiry":expiry})).await?;
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding")
            .await?;
        for (route, ens_v1) in alice_ens_v1_objects(&database).await? {
            let context = format!("{state} {expiry} {route}: {ens_v1:#}");
            assert_eq!(ens_v1["wrapper_expires_at"], Value::Null, "{context}");
            assert!(ens_v1.get("wrapper_expires_at").is_some(), "{context}");
            assert_eq!(ens_v1["wrapper_expires_at_reason"], json!(reason), "{context}");
            let backed = state == "wrapped" || expiry == u64::MAX;
            assert_eq!(ens_v1.get("wrapper_state").is_some(), backed, "{context}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

/// Lookup reads the wrapper expiries after its rows. When Project undoes the wrapping in between,
/// the wrapper's stored row is gone by then; the lookup answers stale, as for any served data
/// that moved under it, not an internal error.
#[tokio::test]
async fn v2_lookup_answers_stale_when_the_wrapper_is_undone_before_its_expiry_is_read() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_alice_wrapped_reserved_after_cutover(&database).await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"profile": "feed",
                            "inputs": [{"name": "alice.eth"}]}))
                        .expect("body must serialize"),
                    ))
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query("DELETE FROM normalized_events WHERE source_family = 'ens_v1_wrapper_l1'")
        .execute(&database.pool)
        .await?;
    republish_fixture_chain(&database, "ethereum-mainnet").await?;
    sqlx::query("DELETE FROM bigname_phase.project_wrapper_state")
        .execute(&database.pool)
        .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("lookup request task panicked")?
        .context("lookup request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload:#}");
    assert_eq!(payload["error"]["code"], json!("stale"), "{payload:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_ens_v1_object_on_an_unwrapped_lease_and_its_absence_under_ens_v2() -> Result<()> {
    let unwrapped = v2_name_record_payload("/v1/names/alice.eth").await?;
    assert_eq!(unwrapped["data"]["authority"], json!("ens_v1"), "{unwrapped:#}");
    assert_eq!(
        unwrapped["data"]["ens_v1"],
        json!({"expires_at": "1798859045"}),
        "a never-wrapped lease omits the wrapper fields, its expiry included: {unwrapped:#}"
    );

    let registered =
        v2_alice_state_payload("/v1/names/alice.eth", AliceInputState::Registry).await?;
    assert_eq!(registered["data"]["authority"], json!("ens_v2"), "{registered:#}");
    assert!(registered["data"].get("ens_v1").is_none(), "{registered:#}");
    Ok(())
}

/// A BaseRegistrar lease expiry is a `uint256`
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L96-L98 @ ens_v1@91c966f),
/// and the admitted Sepolia testnet premigration registrar checks only a minimum duration
/// before passing the caller's duration to the BaseRegistrar
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L161-L165 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L214-L217 @ ens_v2_sepolia_20260916@366de741),
/// so a lease expiry above `i64::MAX` is reachable there. Our registrar producer saturates such
/// an expiry (`evm_abi`), and the served lease date is then `i64::MAX`.
#[tokio::test]
async fn v2_ens_v1_object_serves_a_saturated_lease_expiry_as_i64_max() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    sqlx::query(
        "UPDATE normalized_events SET after_state = jsonb_set(after_state, '{expiry}', $1::jsonb)
         WHERE event_identity = 'alice-RegistrationGranted'",
    )
    .bind(json!(i64::MAX))
    .execute(&database.pool)
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let detail = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    assert_eq!(
        detail["data"]["ens_v1"],
        json!({"expires_at": "9223372036854775807"}),
        "{detail:#}"
    );
    database.cleanup().await
}
