#[tokio::test]
async fn record_id_resolver_inventory_serves_explicit_empty_values() -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v1/names/alice.eth/records?keys=text:url,addr:60,contenthash&include=inventory",
        |row| {
            row.declared_summary["topology"] = json!({"version_boundaries":{"record_version_boundary":record_inventory_boundary_with_pointer(&bigname_storage::logical_name_id_for_name("ens", "alice.eth"), row.resource_id.unwrap(), Some(808), Some("ResolverRecordLinked"))}});
        },
        |_, _, inventory| {
            inventory.record_version_boundary["normalized_event_id"] = json!(808);
            inventory.record_version_boundary["event_kind"] = json!("ResolverRecordLinked");
            inventory.selectors = json!([
                {"record_key":"addr:60","record_family":"addr","selector_key":"60","cacheable":true},
                {"record_key":"contenthash","record_family":"contenthash","selector_key":null,"cacheable":true},
                {"record_key":"text:url","record_family":"text","selector_key":"url","cacheable":true}
            ]);
            inventory.entries = json!([
                {"record_key":"text:url","record_family":"text","selector_key":"url","status":"success","value":""},
                {"record_key":"addr:60","record_family":"addr","selector_key":"60","status":"not_found"},
                {"record_key":"contenthash","record_family":"contenthash","selector_key":null,"status":"not_found"}
            ]);
            inventory.provenance["record_link_event_ids"] = json!([808]);
        },
    ).await?;
    assert_eq!(payload["data"]["records"]["text:url"]["status"], "ok");
    assert_eq!(payload["data"]["records"]["text:url"]["value"], "");
    for key in ["addr:60", "contenthash"] {
        assert_eq!(payload["data"]["records"][key]["status"], "not_found");
    }
    Ok(())
}

#[tokio::test]
async fn record_id_resolver_permissions_preserve_generation_specific_powers() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.permissions_current SET effective_powers = $1 WHERE resource_id = $2 AND scope_kind = 'resolver'")
        .bind(json!(["set_abi", "set_interface", "set_name", "set_data", "link", "admin_link"]))
        .bind(v2_permissions_current_resource_id()).execute(&database.pool).await?;
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

#[tokio::test]
async fn record_id_resolver_default_rule_derives_eth_address_in_indexed_and_auto() -> Result<()> {
    // The Project regression loads the actual manifest and checks rule emission;
    // this route fixture checks both consumer modes using that projected rule.
    for source in ["indexed", "auto"] {
        let payload = v2_name_records_payload_with_setup(
            &format!("/v1/names/alice.eth/records?source={source}&keys=addr:60"),
            |_, _, inventory| {
                inventory.selectors = json!([{
                    "record_key":"addr:2147483648","record_family":"addr","selector_key":"2147483648","cacheable":true
                }]);
                inventory.entries = json!([{
                    "record_key":"addr:2147483648","record_family":"addr","selector_key":"2147483648",
                    "status":"success","value":"0x3333333333333333333333333333333333333333"
                }]);
                inventory.provenance["read_rules"] = json!([{
                    "kind":"ensip19_default_address","source_record_key":"addr:2147483648"
                }]);
                inventory.explicit_gaps = json!([]);
                inventory.unsupported_families = json!([]);
            },
        ).await?;
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
    Ok(())
}
