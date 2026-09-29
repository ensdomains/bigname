// Permission history rows separate the roles a change granted and revoked from the resulting set
// when the log states the previous roles (TYR-65). Uses the registry role fixture of
// v2_address_names_roles.rs.

/// ROLE_HOLDER's registry roles on beta.eth change from `old` to `new`, with the log's old
/// bitmap retained in the before state as the registry adapter stores it.
async fn change_role_holder_powers(
    database: &TestDatabase,
    old: Value,
    new: Value,
) -> Result<()> {
    let (block, hash) = address_fixture_head(database).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let mut event = address_role_event(
        Some(&name),
        Uuid::from_u128(ROLE_RESOURCE),
        ROLE_HOLDER,
        false,
        new,
        block,
        &hash,
    );
    event.before_state = json!({"subject":ROLE_HOLDER, "role_bitmap":"0x01",
        "effective_powers":old});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_address_fixture(database).await
}

#[tokio::test]
async fn v2_name_history_separates_granted_and_revoked_registry_roles() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    bind_address_name_ens_v2(&database, "beta.eth", ROLE_RESOURCE, false).await?;
    change_role_holder_powers(&database, json!([]), json!(["set_resolver"])).await?;
    change_role_holder_powers(
        &database,
        json!(["set_resolver"]),
        json!(["set_resolver", "set_subregistry"]),
    )
    .await?;
    change_role_holder_powers(
        &database,
        json!(["set_resolver", "set_subregistry"]),
        json!(["set_subregistry"]),
    )
    .await?;
    change_role_holder_powers(&database, json!(["set_subregistry"]), json!([])).await?;
    let payload = v2_history_payload_for_database(
        &database,
        "/v1/names/beta.eth/history?type=permission&include=data&order=asc",
    )
    .await?;
    let data = payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["data"].clone())
        .collect::<Vec<_>>();
    let change = |powers: Value, added: Value, removed: Value| {
        json!({"address":ROLE_HOLDER, "grant_scope":{"kind":"registry", "detail":{}},
            "powers":powers, "added_powers":added, "removed_powers":removed})
    };
    assert_eq!(
        data,
        vec![
            change(json!(["set_resolver"]), json!(["set_resolver"]), json!([])),
            change(
                json!(["set_resolver", "set_subregistry"]),
                json!(["set_subregistry"]),
                json!([])
            ),
            change(json!(["set_subregistry"]), json!([]), json!(["set_resolver"])),
            change(json!([]), json!([]), json!(["set_subregistry"])),
        ],
        "{payload}"
    );
    database.cleanup().await
}
