use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

#[tokio::test]
async fn registration_sort_reads_postgres_lifecycle_timestamps() -> anyhow::Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("registration_sort_timestamp").pool_max_connections(1),
    )
    .await?;
    // The lifecycle loader uses to_jsonb(timestamptz), whose UTC form ends in +00:00.
    // Read the same generated value through the actual registration sort expression.
    for value in ["2026-08-03T12:34:56Z", "2026-08-03T12:34:56.123456+10:00"] {
        let mut query = QueryBuilder::<Postgres>::new("SELECT ");
        push_registered_at_timestamp_expr(&mut query);
        query.push(" AS parsed, expected FROM (SELECT expected, jsonb_build_object('registration', jsonb_build_object('registered_at', to_jsonb(expected))) AS declared_summary FROM (SELECT ");
        query.push_bind(value);
        query.push("::timestamptz AS expected) source) nc");
        let (parsed, expected): (Option<OffsetDateTime>, OffsetDateTime) =
            query.build_query_as().fetch_one(database.pool()).await?;
        assert_eq!(
            parsed,
            Some(expected),
            "PostgreSQL lifecycle timestamp {value}"
        );
    }
    database.cleanup().await?;
    Ok(())
}
