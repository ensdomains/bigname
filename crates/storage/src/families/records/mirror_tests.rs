use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::{super::FamilyPosition, latest_registry_first};

/// Positions at one block, including an ordinal-bearing tie at one log (`e:2` against `e:10`),
/// facts without an ordinal at that log, boundary facts and the `u32` bound.
fn positions() -> Vec<FamilyPosition> {
    let at = |transaction: Option<i64>, log: Option<i64>, identity: &str| FamilyPosition {
        block_number: 5,
        transaction_index: transaction,
        log_index: log,
        event_identity: identity.to_owned(),
    };
    vec![
        at(Some(0), Some(0), "e:2"),
        at(Some(0), Some(0), "e:10"),
        at(Some(0), Some(0), "e:holder"),
        at(Some(0), Some(0), "e:007"),
        at(Some(0), Some(0), "t:4294967296"),
        at(Some(0), Some(0), "u:4294967295"),
        at(Some(0), Some(1), "a:0"),
        at(None, None, "p:10"),
        at(None, None, "p:9"),
    ]
}

/// The walk's order at one depth agrees with `FamilyPosition`'s: at one log, the higher emission
/// ordinal is the later fact, so `e:10` wins over `e:2`, where identity bytes alone pick `e:2`.
#[tokio::test]
async fn the_walk_order_follows_the_emission_ordinal() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("families_mirror_order").pool_max_connections(1),
    )
    .await?;
    let result = check_order(database.pool()).await;
    database.cleanup().await?;
    result
}

async fn check_order(pool: &sqlx::PgPool) -> Result<()> {
    let positions = positions();
    let sql = format!(
        "SELECT registry.event_identity
         FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[])
              registry (block_number, transaction_index, log_index, event_identity)
         ORDER BY {}",
        latest_registry_first("registry")
    );
    let identities: Vec<String> = sqlx::query_scalar(&sql)
        .bind(positions.iter().map(|p| p.block_number).collect::<Vec<_>>())
        .bind(
            positions
                .iter()
                .map(|p| p.transaction_index)
                .collect::<Vec<_>>(),
        )
        .bind(positions.iter().map(|p| p.log_index).collect::<Vec<_>>())
        .bind(
            positions
                .iter()
                .map(|p| p.event_identity.clone())
                .collect::<Vec<_>>(),
        )
        .fetch_all(pool)
        .await?;
    let mut expected = positions;
    expected.sort_by(|left, right| right.cmp(left));
    let expected: Vec<String> = expected.into_iter().map(|p| p.event_identity).collect();
    assert_eq!(identities, expected);
    let tie: Vec<&str> = identities
        .iter()
        .map(String::as_str)
        .filter(|identity| matches!(*identity, "e:2" | "e:10"))
        .collect();
    assert_eq!(tie, ["e:10", "e:2"], "the walk picks ordinal 10");
    Ok(())
}
