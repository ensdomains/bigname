use super::*;

#[tokio::test]
async fn normalized_registry_proof_fails_closed_without_changing_stored_grants() -> Result<()> {
    let database = setup(&manual_logs(U256::ZERO, (TIME + 100) as u64)).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    let original = registry_page(&database, WRAPPER).await?;
    assert_derived(&original, ALICE, "holder", PARENT, ALICE);
    let manifest: (i64, Value) = sqlx::query_as(
        "SELECT normalized_event_id, after_state FROM normalized_events
         WHERE event_kind = 'SourceManifestUpdated' AND source_family = 'ens_v2_migration_l1'
         ORDER BY normalized_event_id DESC LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    for field in ["contracts", "deployment_epoch", "chain", "namespace"] {
        let mut malformed = manifest.1.clone();
        malformed["manifest_payload"][field] = Value::Null;
        sqlx::query("UPDATE normalized_events SET after_state = $2 WHERE normalized_event_id = $1")
            .bind(manifest.0)
            .bind(&malformed)
            .execute(&database.pool)
            .await?;
        assert_direct(&registry_page(&database, WRAPPER).await?);
    }
    sqlx::query("UPDATE normalized_events SET after_state = $2 WHERE normalized_event_id = $1")
        .bind(manifest.0)
        .bind(&manifest.1)
        .execute(&database.pool)
        .await?;
    let announcement: (i64, Value) = sqlx::query_as(
        "SELECT normalized_event_id, after_state FROM normalized_events
         WHERE event_kind = 'RegistryCreated' AND lower(raw_fact_ref ->> 'emitting_address') = $1",
    )
    .bind(WRAPPER)
    .fetch_one(&database.pool)
    .await?;
    for instance in [
        Value::Null,
        json!("malformed"),
        json!(Uuid::new_v4().to_string()),
    ] {
        let mut malformed = announcement.1.clone();
        malformed["contract_instance_id"] = instance;
        sqlx::query("UPDATE normalized_events SET after_state = $2 WHERE normalized_event_id = $1")
            .bind(announcement.0)
            .bind(&malformed)
            .execute(&database.pool)
            .await?;
        assert_direct(&registry_page(&database, WRAPPER).await?);
    }
    sqlx::query("UPDATE normalized_events SET after_state = $2 WHERE normalized_event_id = $1")
        .bind(announcement.0)
        .bind(&announcement.1)
        .execute(&database.pool)
        .await?;
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE normalized_event_id = $1")
        .bind(announcement.0).execute(&database.pool).await?;
    assert_direct(&registry_page(&database, WRAPPER).await?);
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'canonical' WHERE normalized_event_id = $1")
        .bind(announcement.0).execute(&database.pool).await?;
    let restored = registry_page(&database, WRAPPER).await?;
    assert_eq!(restored["data"], original["data"]);
    database.cleanup().await
}

fn assert_direct(page: &Value) {
    let owner = for_subject(page, ALICE);
    assert_eq!(owner.len(), 1, "{page:#}");
    assert_eq!(owner[0]["powers"], json!(["set_subregistry"]));
    assert!(owner[0].get("grant_relation").is_none());
    let operator = for_subject(page, OPERATOR);
    assert_eq!(operator.len(), 1, "{page:#}");
    assert_eq!(operator[0]["powers"], json!(["set_resolver"]));
    assert!(operator[0].get("grant_relation").is_none());
}
