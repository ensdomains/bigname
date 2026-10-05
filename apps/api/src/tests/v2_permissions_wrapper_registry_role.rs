use super::*;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

#[tokio::test]
async fn documented_non_superuser_api_role_serves_wrapper_roots_and_preflight_requires_parent_table()
-> Result<()> {
    let database = setup(&manual_logs(U256::ZERO, (TIME + 100) as u64)).await?;
    interpret(&database, false, false).await?;
    publish(&database, 4).await?;
    let role = format!("wrapper_permission_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS"
    ))
    .execute(&database.pool)
    .await?;
    sqlx::query(&format!("GRANT USAGE ON SCHEMA bigname_phase TO {role}"))
        .execute(&database.pool)
        .await?;
    let docs = include_str!("../../../../docs/deployment.md");
    let (_, grant) = docs
        .split_once("GRANT SELECT ON TABLE\n")
        .context("documented API table grants")?;
    let (tables, _) = grant
        .split_once("TO bigname_api;")
        .context("documented API role")?;
    sqlx::query(&format!("GRANT SELECT ON TABLE {tables} TO {role}"))
        .execute(&database.pool)
        .await?;
    sqlx::query(&format!(
        "GRANT EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state_read_only(
        text, bigint, text, jsonb, jsonb, uuid, text, text) TO {role}"
    ))
    .execute(&database.pool)
    .await?;
    let config = database.database_config(2)?;
    let options = PgConnectOptions::from_str(config.database_url.as_deref().context("test URL")?)?
        .options([("search_path", "bigname_phase")]);
    let set_role = format!("SET ROLE {role}");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |connection, _| {
            let set_role = set_role.clone();
            Box::pin(async move { sqlx::query(&set_role).execute(connection).await.map(|_| ()) })
        })
        .connect_with(options)
        .await?;
    let superuser: bool =
        sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&pool)
            .await?;
    assert!(!superuser);
    crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool).await?;
    let response = app_router(state(pool.clone()))
        .oneshot(
            Request::builder()
                .uri(format!("/v1/permissions?registry=11155111:{WRAPPER}"))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_derived(&body, ALICE, "holder", PARENT, ALICE);
    sqlx::query(&format!(
        "REVOKE SELECT ON bigname_phase.project_ens_v2_registry_parent FROM {role}"
    ))
    .execute(&database.pool)
    .await?;
    let missing = bigname_storage::load_missing_api_lookup_ddl(&pool).await?;
    assert_eq!(missing.len(), 1, "{missing:?}");
    assert_eq!(
        missing[0].identity,
        "bigname_phase.project_ens_v2_registry_parent"
    );
    let error = crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool)
        .await
        .expect_err("the actual new reader privilege must be a startup prerequisite");
    assert!(format!("{error:#}").contains("project_ens_v2_registry_parent"));
    pool.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}"))
        .execute(&database.pool)
        .await?;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&database.pool)
        .await?;
    database.cleanup().await
}
