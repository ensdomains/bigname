use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

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

#[tokio::test]
async fn expiry_sort_preserves_large_seconds_and_classified_nulls() -> anyhow::Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("exact_expiry_sort").pool_max_connections(1))
            .await?;
    for value in [
        "253402300800",
        "9007199254740993",
        "9223372036854775808",
        "18446744073709551614",
        "18446744073709551614.000000001",
    ] {
        // Both retained numeric inputs and the canonical composed decimal string stay exact.
        let mut encodings = vec![json!(value)];
        if !value.contains('.') {
            encodings.push(serde_json::from_str(value)?);
        }
        for encoded in encodings {
            let summary = json!({"registration": {"expiry": encoded}});
            assert_eq!(
                read_expiry(database.pool(), &summary).await?,
                Some(value.parse::<UnixSeconds>()?),
                "{summary}"
            );
        }
    }
    for summary in [
        json!({"registration": {"expiry": null}, "control": {"expiry": "18446744073709551614"}}),
        json!({"registration": {"expiry": null, "expiry_date": "100"}}),
    ] {
        assert_eq!(read_expiry(database.pool(), &summary).await?, None);
    }
    assert_eq!(
        read_expiry(
            database.pool(),
            &json!({"registration": {"expiry_date": "100", "expiry": "200"}})
        )
        .await?,
        Some("100".parse()?)
    );
    assert_eq!(
        read_expiry(database.pool(), &json!({"control": {"expiry": "250"}})).await?,
        Some("250".parse()?)
    );
    database.cleanup().await?;
    Ok(())
}

async fn read_expiry(
    pool: &sqlx::PgPool,
    summary: &serde_json::Value,
) -> anyhow::Result<Option<UnixSeconds>> {
    let mut query = QueryBuilder::<Postgres>::new("SELECT ");
    push_expires_at_timestamp_expr(&mut query);
    query.push(" FROM (SELECT ");
    query.push_bind(summary);
    query.push("::jsonb AS declared_summary) nc");
    Ok(query.build_query_scalar().fetch_one(pool).await?)
}

#[tokio::test]
async fn expiry_keysets_preserve_numeric_order_nulls_and_fractional_windows() -> anyhow::Result<()>
{
    let database = TestDatabase::create(
        TestDatabaseConfig::new("expiry_numeric_keyset").pool_max_connections(1),
    )
    .await?;
    let source = json!([
        {"logical_name_id": "a", "resource_id": Uuid::from_u128(1), "declared_summary": {"registration": {"expiry": "9007199254740992"}}},
        {"logical_name_id": "b", "resource_id": Uuid::from_u128(2), "declared_summary": {"registration": {"expiry": "9007199254740993"}}},
        {"logical_name_id": "c", "resource_id": Uuid::from_u128(3), "declared_summary": {"registration": {"expiry": "18446744073709551614"}}},
        {"logical_name_id": "d", "resource_id": Uuid::from_u128(4), "declared_summary": {"registration": {"expiry": "18446744073709551614"}}},
        {"logical_name_id": "e", "resource_id": Uuid::from_u128(5), "declared_summary": {"registration": {"expiry": null}, "control": {"expiry": "1"}}}
    ]);
    for (order, expected) in [
        (AddressNamesCurrentOrder::Asc, vec!["a", "b", "c", "d", "e"]),
        (
            AddressNamesCurrentOrder::Desc,
            vec!["e", "c", "d", "b", "a"],
        ),
    ] {
        let mut cursor = None;
        let mut names = Vec::new();
        for _ in 0..=expected.len() {
            let mut query = expiry_rows_query(&source);
            if let Some(cursor) = &cursor {
                push_address_names_current_cursor_after(
                    &mut query,
                    AddressNamesCurrentSort::ExpiresAt,
                    order,
                    cursor,
                );
            }
            push_address_names_current_order(&mut query, AddressNamesCurrentSort::ExpiresAt, order);
            query.push(" LIMIT 1");
            let row: Option<(String, Uuid, Option<UnixSeconds>)> = query
                .build_query_as()
                .fetch_optional(database.pool())
                .await?;
            let Some((logical_name_id, resource_id, expiry)) = row else {
                break;
            };
            names.push(logical_name_id.clone());
            cursor = Some(AddressNamesCurrentSortedCursor {
                sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(expiry),
                logical_name_id,
                resource_id,
            });
        }
        assert_eq!(names, expected);
    }
    // The decimal bounds distinguish consecutive integer expiries above the f64 safe range.
    let mut query = expiry_rows_query(&source);
    query.push(" AND sort_timestamp >= ");
    query.push_bind("9007199254740992.000000001".parse::<UnixSeconds>()?);
    query.push(" AND sort_timestamp < ");
    query.push_bind("9007199254740993.000000001".parse::<UnixSeconds>()?);
    let rows: Vec<(String, Uuid, Option<UnixSeconds>)> =
        query.build_query_as().fetch_all(database.pool()).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "b");
    database.cleanup().await?;
    Ok(())
}

fn expiry_rows_query(source: &serde_json::Value) -> QueryBuilder<'_, Postgres> {
    let mut query =
        QueryBuilder::<Postgres>::new("WITH rows AS (SELECT logical_name_id, resource_id, ");
    push_expires_at_timestamp_expr(&mut query);
    query.push(" AS sort_timestamp FROM jsonb_to_recordset(");
    query.push_bind(source);
    query.push(") nc(logical_name_id text, resource_id uuid, declared_summary jsonb)) SELECT logical_name_id, resource_id, sort_timestamp FROM rows WHERE TRUE");
    query
}
