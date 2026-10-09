//! What every database-backed api test pays before its own work. nextest runs each test in a
//! process of its own, so any lazy initialisation is paid once per test.
use super::*;

/// The first ENS normalisation in a process builds the normaliser's tables.
#[test]
fn ens_normalisation_tables_build_quickly_on_first_use() {
    let started = std::time::Instant::now();
    bigname_lookup::ens_namehash_hex("warm.eth").expect("warm.eth must normalise");
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "the first ens_namehash_hex call took {elapsed:?}"
    );
}

#[tokio::test]
async fn api_fixture_holds_one_connection_on_its_database() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let connections: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND backend_type = 'client backend'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(connections, 1);
    database.cleanup().await
}

#[tokio::test]
async fn concurrent_api_fixtures_get_empty_databases_of_their_own() -> Result<()> {
    let (first, second) = tokio::join!(TestDatabase::new_migrated(), TestDatabase::new_migrated());
    let (first, second) = (first?, second?);
    assert_ne!(first.database_name, second.database_name);
    for database in [&first, &second] {
        let current: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(current, database.database_name);
        for table in ["chain_lineage", "project_name_state"] {
            let rows: i64 =
                sqlx::query_scalar(&format!("SELECT count(*) FROM bigname_phase.{table}"))
                    .fetch_one(&database.pool)
                    .await?;
            assert_eq!(rows, 0, "{table} in {current}");
        }
    }
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp)
         VALUES ('ethereum-mainnet', '0xisolated', 1, now())",
    )
    .execute(&first.pool)
    .await?;
    for (database, expected) in [(&first, 1), (&second, 0)] {
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_lineage")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(rows, expected, "{}", database.database_name);
    }
    first.cleanup().await?;
    second.cleanup().await
}
