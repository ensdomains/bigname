use std::sync::atomic::AtomicUsize;

use super::*;

#[tokio::test]
async fn concurrent_first_use_builds_one_template_and_both_copies_work() -> Result<()> {
    // A fingerprint no other run has used, so both calls race on a template that does not exist.
    let fresh = unique_database_name("concurrency")?;
    let fingerprint = [fresh.as_bytes()];
    let builds = AtomicUsize::new(0);
    let create = || {
        TestDatabase::create_from_template(
            TestDatabaseConfig::new("bigname_tpl_test"),
            "concurrency_test",
            &fingerprint,
            |pool| {
                let builds = &builds;
                async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    sqlx::raw_sql("CREATE TABLE built (id int); INSERT INTO built VALUES (1);")
                        .execute(&pool)
                        .await?;
                    Ok(())
                }
            },
        )
    };
    let (first, second) = tokio::join!(create(), create());
    let (first, second) = (first?, second?);

    assert_eq!(builds.load(Ordering::SeqCst), 1);
    for database in [&first, &second] {
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM built")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(rows, 1);
    }

    first.cleanup().await?;
    second.cleanup().await?;
    let admin_pool = connect_admin_pool(
        &TestDatabaseConfig::new("bigname_tpl_test"),
        &PgConnectOptions::from_str(&database_url_from_env())?,
    )
    .await?;
    let mut connection = admin_pool.acquire().await?;
    let template =
        template_database_name(&mut connection, "concurrency_test", &fingerprint).await?;
    let flags: (bool, bool) =
        sqlx::query_as("SELECT datistemplate, datallowconn FROM pg_database WHERE datname = $1")
            .bind(&template)
            .fetch_one(&mut *connection)
            .await?;
    assert_eq!(flags, (true, false));
    for statement in [
        format!(
            "ALTER DATABASE {} IS_TEMPLATE false",
            quote_identifier(&template)
        ),
        format!("DROP DATABASE {}", quote_identifier(&template)),
    ] {
        sqlx::query(&statement).execute(&mut *connection).await?;
    }
    drop(connection);
    admin_pool.close().await;
    Ok(())
}
