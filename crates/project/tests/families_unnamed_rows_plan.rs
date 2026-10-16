//! The read of a block's still-unnamed ENSv1 registrar lease events
//! (`project:families.decode.unnamed_rows` in crates/project/src/families/decode.rs) probes the
//! primary key of `project_lifecycle_event` once per lease, with `state_key` in the index
//! condition, on a table seeded so that PostgreSQL's estimate of the unnamed lease events is far
//! below their real count, as on the mainnet replay. A partial index over exactly those events
//! made PostgreSQL read every one of them for the chain on each call and filter by lease.
//!
//! sqlx prepares the statement, and the phase runner's pool keeps `plan_cache_mode = auto`
//! (apps/phase-runner/tests/bootstrap.rs), so after five executions the runner may get the
//! generic plan. Each case runs the prepared statement under `auto` (six executions, then the
//! plan of the seventh) and under the two forced modes. On this seed every mode probes the
//! primary key. On the mainnet replay, with 11.3M rows, a custom plan with 424 literal leases
//! walked `project_lifecycle_event_decoded_name_idx` instead, a choice the runner's prepared
//! path does not make there. Forcing custom plans on the runner's pool would bring that walk
//! back for this statement.
mod families_support;

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use families_support::{Fixture, uuid};
use sqlx::{PgConnection, Postgres, Row, pool::PoolConnection, raw_sql};

/// The statement as decode.rs sends it, byte for byte.
const UNNAMED_ROWS: &str =
    "/* project:families.decode.unnamed_rows */ SELECT state_key, event_identity
         FROM project_lifecycle_event
         WHERE chain_id = $1 AND state_kind = 'resource' AND state_key = ANY($2)
           AND source_family = 'ens_v1_registrar_l1'
           AND original_logical_name_id IS NULL AND decoded_logical_name_id IS NULL";

const MAINNET: &str = "ethereum-mainnet";
const SEPOLIA: &str = "ethereum-sepolia";
/// A chain whose lease events are all named.
const NAMED_ONLY: &str = "base-mainnet";
const MODES: [&str; 3] = ["auto", "force_generic_plan", "force_custom_plan"];

/// Mainnet: 200,000 events. Every 50th is an unnamed registrar lease event (4,000 over leases
/// 1-3,000, leases 1-1,000 with two), the next a named one (leases 1,501-5,500), the rest named
/// registry triple events. Each predicate column alone matches about 2-5% of the table, so
/// PostgreSQL multiplies them to about one row. Sepolia: 20,000 events, 2,000 unnamed on leases
/// 1-2,000 shared with mainnet. base-mainnet: 2,000 named lease events on leases 1-2,000.
const SEED: &str = r#"
INSERT INTO project_lifecycle_event (
    chain_id, state_kind, state_key, block_number, transaction_index, log_index, event_identity,
    event_kind, original_logical_name_id, decoded_logical_name_id, resource_id, source_family)
SELECT chain_id, CASE WHEN lease IS NULL THEN 'triple' ELSE 'resource' END,
       COALESCE('00000000-0000-0000-0000-' || lpad(to_hex(lease), 12, '0'), '["n' || event || '"]'),
       1000000 + i, i % 200, i % 300, event, 'RegistrationGranted',
       CASE WHEN NOT unnamed THEN 'name-' || md5(event) END,
       CASE WHEN NOT unnamed THEN 'name-' || md5(event) END,
       CASE WHEN lease IS NOT NULL THEN md5(event)::uuid END,
       CASE WHEN lease IS NULL THEN 'ens_v1_registry_l1' ELSE 'ens_v1_registrar_l1' END
FROM (
    SELECT 'ethereum-mainnet' AS chain_id, i, 'a-' || i AS event, i % 50 = 0 AS unnamed,
           CASE i % 50 WHEN 0 THEN CASE WHEN i / 50 < 3000 THEN i / 50 + 1 ELSE i / 50 - 2999 END
                       WHEN 1 THEN i / 50 + 1501 END AS lease
    FROM generate_series(0, 199999) AS i
    UNION ALL
    SELECT 'ethereum-sepolia', i, 'b-' || i, i % 10 = 0,
           CASE WHEN i % 10 = 0 THEN i / 10 + 1 WHEN i % 50 = 1 THEN 100000 + i END
    FROM generate_series(0, 19999) AS i
    UNION ALL
    SELECT 'base-mainnet', i, 'c-' || i, false, i + 1
    FROM generate_series(0, 1999) AS i
) AS seed;
ANALYZE project_lifecycle_event;
"#;

/// The seed's unnamed rows of these leases on this chain.
fn expected(chain: &str, leases: &[u32]) -> BTreeSet<(String, String)> {
    let mut rows = BTreeSet::new();
    for &lease in leases {
        match chain {
            MAINNET if (1..=3000).contains(&lease) => {
                rows.insert((uuid(lease), format!("a-{}", 50 * (lease - 1))));
                if lease <= 1000 {
                    rows.insert((uuid(lease), format!("a-{}", 50 * (lease + 2999))));
                }
            }
            SEPOLIA if (1..=2000).contains(&lease) => {
                rows.insert((uuid(lease), format!("b-{}", 10 * (lease - 1))));
            }
            _ => {}
        }
    }
    rows
}

const MISS: u32 = 900_000;

fn misses(count: u32) -> Vec<u32> {
    (0..count).map(|n| MISS + n).collect()
}

/// `hits` leases with unnamed rows spread over mainnet's 1-3,000, then `missing` with none.
fn mixed(hits: u32, missing: u32) -> Vec<u32> {
    let step = 3000 / hits;
    (0..hits)
        .map(|n| 1 + n * step)
        .chain(misses(missing))
        .collect()
}

/// A seeded database and its one connection, where the prepared statement lives.
async fn seeded(prefix: &str) -> Result<(Fixture, PoolConnection<Postgres>)> {
    let fixture = Fixture::new(prefix, 0).await?;
    raw_sql(SEED).execute(&fixture.pool).await?;
    let connection = fixture.pool.acquire().await?;
    Ok((fixture, connection))
}

/// Drops the database whether the case passed or not: each one holds a seeded table.
async fn finish(
    fixture: Fixture,
    connection: PoolConnection<Postgres>,
    result: Result<()>,
) -> Result<()> {
    drop(connection);
    fixture.cleanup().await?;
    result
}

/// The plan and the rows of the prepared statement in `mode`, with the compose files' planner
/// settings. Under `auto` it runs six times first, so the plan is the one the seventh execution
/// gets.
async fn run(
    connection: &mut PgConnection,
    mode: &str,
    chain: &str,
    leases: &[u32],
) -> Result<(Vec<String>, BTreeSet<(String, String)>)> {
    let keys: Vec<String> = leases.iter().map(|&lease| uuid(lease)).collect();
    let execute = format!("EXECUTE unnamed_rows ('{chain}', '{{{}}}')", keys.join(","));
    let setting = if mode == "auto" {
        "RESET plan_cache_mode".to_owned()
    } else {
        format!("SET plan_cache_mode = {mode}")
    };
    raw_sql(&format!(
        "SET random_page_cost = 1.1; SET jit = off; {setting}; DEALLOCATE ALL;
         PREPARE unnamed_rows (text, text[]) AS {UNNAMED_ROWS}"
    ))
    .execute(&mut *connection)
    .await?;
    if mode == "auto" {
        let current: String = raw_sql("SHOW plan_cache_mode")
            .fetch_one(&mut *connection)
            .await?
            .try_get(0)?;
        ensure!(current == "auto", "plan_cache_mode is {current}");
        for _ in 0..6 {
            raw_sql(&execute).execute(&mut *connection).await?;
        }
    }
    let plan = raw_sql(&format!("EXPLAIN (COSTS OFF) {execute}"))
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|row| row.try_get(0))
        .collect::<Result<Vec<String>, _>>()?;
    let rows = raw_sql(&execute)
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect::<Result<BTreeSet<_>>>()?;
    Ok((plan, rows))
}

/// The scan of the table is a primary key probe with `state_key` in its index condition, and no
/// filter checks `state_key`.
fn probes_primary_key(label: &str, plan: &[String]) -> Result<()> {
    let probe = plan.windows(2).any(|lines| {
        lines[0].contains("using project_lifecycle_event_pkey on project_lifecycle_event")
            && lines[1].contains("Index Cond:")
            && [
                "(chain_id = ",
                "(state_kind = 'resource'::text)",
                "(state_key = ANY ",
            ]
            .iter()
            .all(|condition| lines[1].contains(condition))
    });
    let filtered = plan
        .iter()
        .any(|line| line.contains("Filter:") && line.contains("state_key"));
    ensure!(
        probe && !filtered,
        "{label}: the read does not probe the primary key by lease\n{}",
        plan.join("\n")
    );
    Ok(())
}

/// The rows equal the seed's in every mode, then the plan probes the primary key in every mode.
async fn assert_case(
    connection: &mut PgConnection,
    label: &str,
    chain: &str,
    leases: &[u32],
    expected_rows: &BTreeSet<(String, String)>,
    check_plan: bool,
) -> Result<()> {
    let mut plans = Vec::new();
    for mode in MODES {
        let (plan, rows) = run(connection, mode, chain, leases).await?;
        ensure!(
            &rows == expected_rows,
            "{label} {mode}: rows differ from the seed's unnamed rows\nexpected {expected_rows:?}\nobserved {rows:?}"
        );
        plans.push((mode, plan));
    }
    if check_plan {
        let failures: Vec<String> = plans
            .iter()
            .filter_map(|(mode, plan)| probes_primary_key(&format!("{label} {mode}"), plan).err())
            .map(|error| error.to_string())
            .collect();
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
    }
    Ok(())
}

#[test]
fn the_statement_is_the_one_decode_sends() {
    assert!(
        include_str!("../src/families/decode.rs").contains(&format!("\"{UNNAMED_ROWS}\"")),
        "decode.rs no longer sends the statement this test plans"
    );
}

#[tokio::test]
async fn one_lease_probes_the_primary_key() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_one").await?;
    let leases = [1];
    let rows = expected(MAINNET, &leases);
    let result = assert_case(&mut connection, "1 lease", MAINNET, &leases, &rows, true).await;
    finish(fixture, connection, result).await
}

#[tokio::test]
async fn twenty_five_leases_probe_the_primary_key() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_25").await?;
    let leases = mixed(15, 10);
    let rows = expected(MAINNET, &leases);
    let result = assert_case(&mut connection, "25 leases", MAINNET, &leases, &rows, true).await;
    finish(fixture, connection, result).await
}

/// The custom plan here is the measured case of the module comment: a primary key probe on this
/// seed.
#[tokio::test]
async fn four_hundred_sixty_leases_probe_the_primary_key() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_460").await?;
    let leases = mixed(400, 60);
    let rows = expected(MAINNET, &leases);
    let result = assert_case(&mut connection, "460 leases", MAINNET, &leases, &rows, true).await;
    finish(fixture, connection, result).await
}

#[tokio::test]
async fn rows_are_the_unnamed_lease_events_of_the_leases() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_rows").await?;
    let result = async {
        for leases in [vec![1], mixed(15, 10), mixed(400, 60)] {
            let label = format!("{} leases", leases.len());
            let rows = expected(MAINNET, &leases);
            ensure!(!rows.is_empty());
            assert_case(&mut connection, &label, MAINNET, &leases, &rows, false).await?;
        }
        Ok(())
    }
    .await;
    finish(fixture, connection, result).await
}

#[tokio::test]
async fn a_chain_with_only_named_lease_events_returns_nothing() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_named_chain").await?;
    let leases: Vec<u32> = (1..=25).collect();
    let result = assert_case(
        &mut connection,
        "named-only chain",
        NAMED_ONLY,
        &leases,
        &BTreeSet::new(),
        true,
    )
    .await;
    finish(fixture, connection, result).await
}

#[tokio::test]
async fn each_chain_reads_only_its_own_unnamed_events() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_two_chains").await?;
    let leases: Vec<u32> = (1..=25).collect();
    let result = async {
        for chain in [MAINNET, SEPOLIA] {
            let rows = expected(chain, &leases);
            ensure!(!rows.is_empty());
            assert_case(&mut connection, chain, chain, &leases, &rows, true).await?;
        }
        Ok(())
    }
    .await;
    finish(fixture, connection, result).await
}

/// decode.rs passes the block's candidate leases without removing repeats.
#[tokio::test]
async fn repeated_leases_return_each_row_once() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_repeats").await?;
    let distinct = mixed(15, 10);
    let mut leases = distinct.clone();
    leases.extend_from_slice(&distinct[..10]);
    let rows = expected(MAINNET, &distinct);
    let result = assert_case(
        &mut connection,
        "repeated leases",
        MAINNET,
        &leases,
        &rows,
        true,
    )
    .await;
    finish(fixture, connection, result).await
}

#[tokio::test]
async fn leases_without_rows_probe_the_primary_key() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_misses").await?;
    let leases = misses(25);
    let result = assert_case(
        &mut connection,
        "25 misses",
        MAINNET,
        &leases,
        &BTreeSet::new(),
        true,
    )
    .await;
    finish(fixture, connection, result).await
}

/// Leases 1,501-1,525 have one unnamed and one named event each, leases 3,001-3,025 only named.
#[tokio::test]
async fn named_lease_events_are_not_returned() -> Result<()> {
    let (fixture, mut connection) = seeded("unnamed_rows_named").await?;
    let leases: Vec<u32> = (1501..=1525).chain(3001..=3025).collect();
    let rows = expected(MAINNET, &leases);
    let result = async {
        ensure!(rows.len() == 25);
        assert_case(
            &mut connection,
            "named leases",
            MAINNET,
            &leases,
            &rows,
            false,
        )
        .await
    }
    .await;
    finish(fixture, connection, result).await
}
