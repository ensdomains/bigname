use std::{sync::atomic::AtomicUsize, time::Duration};

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

/// Drops `name` if `cleanup` left it behind, and says whether it had.
async fn drop_leftover(name: &str) -> Result<bool> {
    let admin_pool = connect_admin_pool(
        &TestDatabaseConfig::new("cleanup_test"),
        &PgConnectOptions::from_str(&database_url_from_env())?,
    )
    .await?;
    let left: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(&admin_pool)
            .await?;
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS {} WITH (FORCE)",
        quote_identifier(name)
    ))
    .execute(&admin_pool)
    .await?;
    admin_pool.close().await;
    Ok(left)
}

#[tokio::test]
async fn cleanup_with_a_held_connection_drops_the_database_and_reports_it() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("cleanup_held")).await?;
    let name = database.database_name().to_owned();
    let held = database.pool().acquire().await?;

    let cleaned = tokio::time::timeout(Duration::from_secs(30), database.cleanup()).await;
    drop(held);
    let left = drop_leftover(&name).await?;

    let error = cleaned
        .context("cleanup waited over 30 s for the held connection")?
        .expect_err("cleanup must report the held connection");
    assert!(
        error.to_string().contains("pooled connection"),
        "unexpected error: {error:#}"
    );
    assert!(!left, "cleanup left {name} behind");
    Ok(())
}

#[tokio::test]
async fn cleanup_with_a_held_connection_and_an_idle_one_ends_and_drops_the_database() -> Result<()>
{
    let database = TestDatabase::create(TestDatabaseConfig::new("cleanup_idle")).await?;
    let name = database.database_name().to_owned();
    let held = database.pool().acquire().await?;
    sqlx::query("SELECT 1").execute(database.pool()).await?;
    // The pool takes the query's connection back on a spawned task.
    tokio::time::timeout(Duration::from_secs(5), async {
        while database.pool().num_idle() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the query's connection never became idle")?;

    let cleaned = tokio::time::timeout(Duration::from_secs(30), database.cleanup()).await;
    drop(held);
    let left = drop_leftover(&name).await?;

    // Whether the pool's close waits for the held connection here is the pool's own business.
    if let Err(error) = cleaned.context("cleanup waited over 30 s for the held connection")? {
        assert!(
            error.to_string().contains("pooled connection"),
            "unexpected error: {error:#}"
        );
    }
    assert!(!left, "cleanup left {name} behind");
    Ok(())
}
