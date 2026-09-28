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
    "expires_at",
    "lapsed_registration",
    "authority",
    "migrated_at",
    "resolver",
    "subregistry",
    "addresses",
    "text_records",
    "content_hash",
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
