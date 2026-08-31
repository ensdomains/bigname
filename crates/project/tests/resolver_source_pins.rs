use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn production_order(source: &str, selection: &str) -> String {
    let normalized = source.split_whitespace().collect::<Vec<_>>().join(" ");
    let selection_start = normalized.find(selection).expect("production selection");
    let selection = &normalized[selection_start..];
    let order_start = selection.find("ORDER BY").expect("selection ordering");
    let order_end = selection[order_start..]
        .find("LIMIT 1")
        .expect("selection limit");
    selection[order_start..order_start + order_end].to_owned()
}

#[tokio::test]
async fn duplicate_declarations_source_role_and_features_from_the_same_latest_epoch() -> TestResult
{
    let resolver = include_str!("../src/builders/resolver.rs");
    let features = include_str!("../src/builders/resolver/read_features.rs");
    let contract_role_order = production_order(resolver, "SELECT declaration ->> 'role'");
    let contract_feature_order =
        production_order(features, "SELECT declaration -> 'read_features'");
    let implementation_role_order = production_order(resolver, "SELECT implementation ->> 'role'");
    let implementation_feature_order =
        production_order(features, "SELECT admitted -> 'read_features'");
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "project_resolver_declaration_alignment",
    ))
    .await?;
    let payload = json!({
        "contracts": [
            {"address":"0x01", "role":"old", "read_features":["old"], "start_block":10},
            {"address":"0x01", "role":"first", "read_features":["first"], "start_block":20},
            {"address":"0x01", "role":"latest", "read_features":["latest"], "start_block":20},
            {"address":"0x01", "role":"future", "read_features":["future"], "start_block":30}
        ],
        "resolver_implementations": [
            {"address":"0x02", "role":"old_impl", "read_features":["old"], "start_block":10},
            {"address":"0x02", "role":"first_impl", "read_features":["first"], "start_block":20},
            {"address":"0x02", "role":"latest_impl", "read_features":["latest"], "start_block":20},
            {"address":"0x02", "role":"future_impl", "read_features":["future"], "start_block":30}
        ]
    });
    let query = format!(
        "SELECT
           (SELECT declaration ->> 'role'
            FROM jsonb_array_elements($1::jsonb -> 'contracts') WITH ORDINALITY
                 declarations(declaration, declaration_ordinality)
            WHERE lower(declaration ->> 'address') = $2
              AND (declaration ->> 'start_block' IS NULL
                   OR (declaration ->> 'start_block')::bigint <= $4)
            {contract_role_order} LIMIT 1),
           (SELECT declaration -> 'read_features'
            FROM jsonb_array_elements($1::jsonb -> 'contracts') WITH ORDINALITY
                 declarations(declaration, declaration_ordinality)
            WHERE lower(declaration ->> 'address') = $2
              AND (declaration ->> 'start_block' IS NULL
                   OR (declaration ->> 'start_block')::bigint <= $4)
            {contract_feature_order} LIMIT 1),
           (SELECT implementation ->> 'role'
            FROM jsonb_array_elements($1::jsonb -> 'resolver_implementations') WITH ORDINALITY
                 implementations(implementation, implementation_ordinality)
            WHERE lower(implementation ->> 'address') = $3
              AND (implementation ->> 'start_block' IS NULL
                   OR (implementation ->> 'start_block')::bigint <= $4)
            {implementation_role_order} LIMIT 1),
           (SELECT admitted -> 'read_features'
            FROM jsonb_array_elements($1::jsonb -> 'resolver_implementations') WITH ORDINALITY
                 implementations(admitted, admitted_ordinality)
            WHERE lower(admitted ->> 'address') = $3
              AND (admitted ->> 'start_block' IS NULL
                   OR (admitted ->> 'start_block')::bigint <= $4)
            {implementation_feature_order} LIMIT 1)"
    );
    let selected: (String, Value, String, Value) = sqlx::query_as(&query)
        .bind(payload)
        .bind("0x01")
        .bind("0x02")
        .bind(20_i64)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(
        selected,
        (
            "latest".into(),
            json!(["latest"]),
            "latest_impl".into(),
            json!(["latest"])
        )
    );
    database.cleanup().await?;
    Ok(())
}
