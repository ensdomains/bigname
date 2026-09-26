//! The canonical event order of `row_position` and `json_position` in PostgreSQL: block,
//! transaction index, log index, the emission ordinal when both indexes are present, then the
//! identity bytes (docs/glossary.md, "Canonical event order"). Each case is a pair of events at
//! one position, earlier then later, checked with the composite greater-than both ways and with
//! a descending selection, over the row columns and over the same position stored as JSON. One
//! more label holds three events at one position, ordinals 2 and 10 and none, checked in full
//! ascending order and by a descending selection.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;

use super::{json_position, row_position};

/// `(case, earlier identity, later identity, transaction index, log index)`, all at block 1.
type Case = (&'static str, String, String, Option<i64>, Option<i64>);

fn cases() -> Vec<Case> {
    let both = (Some(0), Some(5));
    let case =
        |label, earlier: &str, later: &str, (transaction, log): (Option<i64>, Option<i64>)| {
            (
                label,
                earlier.to_owned(),
                later.to_owned(),
                transaction,
                log,
            )
        };
    vec![
        // Ordinal 10 is later than ordinal 2, though "e:10" sorts first as bytes.
        case("2 before 10", "e:2", "e:10", both),
        // The ordinal decides before the identity prefix does.
        case("differing prefixes", "z:1", "a:2", both),
        // No suffix, or an invalid one, has no ordinal and sorts before ordinal 0.
        case("absent suffix", "zzz", "a:0", both),
        case("letters in the suffix", "z:12a", "a:1", both),
        case("signed suffix", "z:-1", "a:1", both),
        case("digits in an earlier segment", "z:12:x", "a:1", both),
        // Leading zeros are allowed: 0010 is ordinal 10, later than 9.
        case("leading zeros", "a:9", "a:0010", both),
        case(
            "zero-padded u32 max",
            "z:4294967294",
            "a:0004294967295",
            both,
        ),
        // 4294967295 is the largest ordinal; one more has none.
        case("u32 max", "z:4294967294", "a:4294967295", both),
        case("out of range", "z:4294967296", "a:1", both),
        case("eleven digits", "z:10000000000", "a:1", both),
        // A suffix far beyond any numeric cast has none and does not error.
        case(
            "131073 nines",
            &format!("z:{}", "9".repeat(131_073)),
            "a:1",
            both,
        ),
        // One ordinal on both sides: the identity bytes decide.
        case("equal ordinals", "a:5", "b:5", both),
        // Without a transaction or a log index there is no ordinal, so bytes decide.
        case("no transaction index", "e:10", "e:2", (None, Some(5))),
        case("no log index", "e:10", "e:2", (Some(0), None)),
        case("synthesised", "activation:10", "activation:9", (None, None)),
    ]
}

async fn install(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        "CREATE TABLE event_position (
             label text NOT NULL, later boolean NOT NULL, block_number bigint NOT NULL,
             transaction_index bigint, log_index bigint, event_identity text NOT NULL,
             position jsonb NOT NULL)",
    )
    .execute(pool)
    .await?;
    // Three events at one log: bytes would order them e:10, e:2, zzz; the ordinal orders them
    // zzz (none), e:2, e:10.
    sqlx::raw_sql(
        "CREATE TABLE event_triple AS
         SELECT 'triple'::text AS label, 1::bigint AS block_number,
                0::bigint AS transaction_index, 5::bigint AS log_index, identity AS event_identity,
                jsonb_build_object('block_number', 1, 'transaction_index', 0, 'log_index', 5,
                                   'event_identity', identity) AS position
         FROM unnest(ARRAY['e:10', 'e:2', 'zzz']) identity",
    )
    .execute(pool)
    .await?;
    for (label, earlier, later, transaction, log) in cases() {
        for (is_later, identity) in [(false, earlier), (true, later)] {
            sqlx::query(
                "INSERT INTO event_position
                 VALUES ($1, $2, 1, $3, $4, $5, jsonb_build_object('block_number', 1,
                     'transaction_index', $3, 'log_index', $4, 'event_identity', $5::text))",
            )
            .bind(label)
            .bind(is_later)
            .bind(transaction)
            .bind(log)
            .bind(identity)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

/// Every case's pair compares later-is-greater one way and not the other, and a descending
/// selection picks the later event.
async fn check(pool: &PgPool, a: &str, b: &str, e: &str) -> Result<()> {
    let wrong: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT a.label FROM event_position a JOIN event_position b ON b.label = a.label
         WHERE a.later AND NOT b.later AND ({a} > {b} AND NOT {b} > {a}) IS NOT TRUE
         ORDER BY a.label"
    ))
    .fetch_all(pool)
    .await?;
    let descending: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT ON (e.label) e.label || CASE WHEN e.later THEN '' ELSE '!' END
         FROM event_position e ORDER BY e.label, {e} DESC"
    ))
    .fetch_all(pool)
    .await?
    .into_iter()
    .filter(|label: &String| label.ends_with('!'))
    .collect();
    ensure!(
        wrong.is_empty() && descending.is_empty(),
        "greater-than failed for {wrong:?}; descending selection failed for {descending:?}"
    );
    let ascending: Option<String> = sqlx::query_scalar(&format!(
        "SELECT string_agg(e.event_identity, ',' ORDER BY {e}) FROM event_triple e"
    ))
    .fetch_one(pool)
    .await?;
    ensure!(
        ascending.as_deref() == Some("zzz,e:2,e:10"),
        "ascending triple {ascending:?}"
    );
    let latest: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT ON (e.label) e.event_identity FROM event_triple e
         ORDER BY e.label, {e} DESC"
    ))
    .fetch_all(pool)
    .await?;
    ensure!(latest == ["e:10"], "descending triple {latest:?}");
    Ok(())
}

async fn with_database<F, Fut>(name: &str, body: F) -> Result<()>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let result = async {
        install(&pool).await?;
        body(pool.clone()).await
    }
    .await;
    drop(pool);
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn row_position_orders_by_the_emission_ordinal() -> Result<()> {
    with_database("family_row_position", |pool| async move {
        check(
            &pool,
            &row_position("a"),
            &row_position("b"),
            &row_position("e"),
        )
        .await
    })
    .await
}

#[tokio::test]
async fn json_position_orders_by_the_emission_ordinal() -> Result<()> {
    with_database("family_json_position", |pool| async move {
        check(
            &pool,
            &json_position("a.position"),
            &json_position("b.position"),
            &json_position("e.position"),
        )
        .await
    })
    .await
}
