//! The actual API role reads Project-produced lookup rows without access to maintenance
//! dependencies or any write privilege. Existing provider cases live in the parent module.
use super::*;

async fn lookup(pool: PgPool, request: Value) -> Result<Value> {
    let response = app_router(
        AppState::new(pool, bigname_lookup::ChainRpcUrls::default())
            .with_public_namespaces_for_test(["ens"]),
    )
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/lookup")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&request)?))?,
    )
    .await?;
    let status = response.status();
    let body = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    Ok(body)
}

async fn state(database: &TestDatabase) -> Result<Value> {
    let mut state = serde_json::Map::new();
    for table in [
        "project_lookup_name",
        "project_lookup_relation",
        "project_lookup_inventory",
        "project_lookup_record",
        "project_lookup_dependency",
        "project_family_marker",
        "project_family_undo",
    ] {
        let rows: Vec<Value> = sqlx::query_scalar(&format!(
            "SELECT to_jsonb(row) FROM {table} row ORDER BY to_jsonb(row)::text"
        ))
        .fetch_all(&database.pool)
        .await?;
        state.insert(table.into(), json!(rows));
    }
    Ok(Value::Object(state))
}

#[tokio::test]
async fn read_only_api_prepared_lookup_needs_no_dependency_access_or_writes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_reverse_page_fixture(&database).await?;
    let before = state(&database).await?;
    let (pool, role) = read_only_pool(&database).await?;
    sqlx::query(&format!(
        "REVOKE SELECT ON bigname_phase.project_lookup_dependency FROM {role}"
    ))
    .execute(&database.pool)
    .await?;
    assert!(
        bigname_storage::load_missing_api_lookup_ddl(&pool)
            .await?
            .is_empty()
    );
    let privileges: (bool, bool) = sqlx::query_as(
        "SELECT has_table_privilege(current_user, 'bigname_phase.project_lookup_dependency', 'SELECT'),
         EXISTS (SELECT 1 FROM unnest(ARRAY['project_lookup_name', 'project_lookup_relation',
             'project_lookup_inventory', 'project_lookup_record', 'project_lookup_dependency']) t
           WHERE has_table_privilege(current_user, 'bigname_phase.' || t, 'INSERT,UPDATE,DELETE'))",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(privileges, (false, false));
    for profile in ["feed", "detail"] {
        for relation in ["any", "owner", "manager", "resolves_to"] {
            let request = json!({"profile":profile,"inputs":[
                {"name":"alpha.eth"},
                {"name":"beta.eth"},
                {"address":FAMILY_ALICE,"relation":relation,"page_size":1}
            ]});
            let expected = lookup(database.pool.clone(), request.clone()).await?;
            let actual = lookup(pool.clone(), request).await?;
            assert_eq!(actual, expected, "{profile}/{relation}");
            assert_eq!(actual["data"][0]["status"], "ok");
            assert_eq!(actual["data"][1]["status"], "ok");
            assert!(
                !actual["data"][2]["records"]
                    .as_array()
                    .context("reverse records")?
                    .is_empty()
            );
        }
    }
    assert_eq!(
        state(&database).await?,
        before,
        "all API reads leave Project state unchanged"
    );
    cleanup_role(&database, pool, role).await?;
    database.cleanup().await
}
