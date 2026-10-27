//! Row 14: the departure probe's cardinality axes. The probe reads one proxy's canonical
//! `Upgraded` rows through `normalized_events_registry_departure_idx`, either side of the
//! origin's implementation, so it visits no same-code upgrade and stops at the first departure.
//! Proxies {1, many}, upgrades per proxy {0, 1, many same-code, many departures}, plan mode
//! {custom, generic}, before and after ANALYZE.
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};

use super::super::DEPARTURES;
use crate::families::id_index_plan_tests::{PLAN_MODES, Probe, missing_probes, with_database};

const CHAIN: &str = "ethereum-mainnet";
const INDEX: &str = "normalized_events_registry_departure_idx";
const CODE: &str = "0x00000000000000000000000000000000000000c1";
const OTHER: &str = "0x00000000000000000000000000000000000000c2";
const UPGRADES: i64 = 1_000;

/// Proxies 1 to 20 each carry a thousand same-code upgrades, so "many" proxies and "many"
/// same-code rows hold together. Proxy 21 has none, proxy 22 one departure among a thousand
/// same-code upgrades, proxy 23 a thousand departures, and proxy 24 one upgrade with no
/// implementation.
async fn install(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         SELECT '{CHAIN}', 'block-' || n, n, to_timestamp(n), 'canonical'
         FROM generate_series(1, {UPGRADES}) n;

         INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state)
         SELECT 'upgrade:' || proxy || ':' || n, 'ens', 'Upgraded', 'ens_v2_registry_l1', 1,
                '{CHAIN}', n, 'block-' || n, 'proxy_upgrade', 'canonical',
                jsonb_build_object('proxy_address', '0x' || lpad(to_hex(proxy), 40, '0'),
                    'implementation', CASE
                        WHEN proxy = 23 OR (proxy = 22 AND n = 500) THEN '{OTHER}'
                        ELSE '{CODE}' END)
         FROM generate_series(1, 23) proxy CROSS JOIN generate_series(1, {UPGRADES}) n
         WHERE proxy <> 21;

         INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state)
         VALUES ('upgrade:24', 'ens', 'Upgraded', 'ens_v2_registry_l1', 1, '{CHAIN}', 7,
             'block-7', 'proxy_upgrade', 'canonical',
             jsonb_build_object('proxy_address', '0x' || lpad(to_hex(24), 40, '0')));

         PREPARE departure (text, text, bigint, bigint, text) AS {DEPARTURES}"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

fn nodes(plan: &Value) -> Vec<&Value> {
    let mut out = Vec::new();
    let mut pending = vec![&plan[0]["Plan"]];
    while let Some(node) = pending.pop() {
        out.push(node);
        if let Some(children) = node["Plans"].as_array() {
            pending.extend(children);
        }
    }
    out
}

/// The `normalized_events` rows the plan's scans returned or filtered out, over every loop.
fn visited(plan: &Value) -> i64 {
    nodes(plan)
        .into_iter()
        .filter(|node| node["Relation Name"] == "normalized_events")
        .map(|node| {
            let loops = node["Actual Loops"].as_i64().unwrap_or(1);
            let rows = node["Actual Rows"].as_i64().unwrap_or_default();
            let removed = node["Rows Removed by Filter"].as_i64().unwrap_or_default();
            (rows + removed) * loops
        })
        .sum()
}

struct Measured {
    departed: bool,
    visited: i64,
    text: Vec<String>,
}

async fn measure(connection: &mut PgConnection, mode: &str, proxy: i64) -> Result<Measured> {
    let values = format!("'{CHAIN}', '0x{proxy:040x}', 1, {UPGRADES}, '{CODE}'");
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(&mut *connection)
        .await?;
    let text = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (COSTS OFF) EXECUTE departure ({values})"
    ))
    .fetch_all(&mut *connection)
    .await?;
    let plan = sqlx::query_scalar::<_, sqlx::types::Json<Value>>(&format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE departure ({values})"
    ))
    .fetch_one(&mut *connection)
    .await?
    .0;
    let departed = sqlx::query_scalar(&format!("EXECUTE departure ({values})"))
        .fetch_one(&mut *connection)
        .await?;
    Ok(Measured {
        departed,
        visited: visited(&plan),
        text,
    })
}

/// (proxy, departed, the rows the probe may visit)
const CELLS: [(i64, bool, i64); 5] = [
    (1, false, 0),
    (21, false, 0),
    (22, true, 1),
    (23, true, 1),
    (24, true, 1),
];

async fn check(connection: &mut PgConnection, stats: &str) -> Result<Vec<String>> {
    let probes = [Probe {
        index: INDEX,
        conditions: &["chain_id = ", "proxy_address", "implementation"],
    }];
    let mut failures = Vec::new();
    for mode in PLAN_MODES {
        for (proxy, departed, bound) in CELLS {
            let label = format!("{stats} {mode} proxy {proxy}");
            let measured = measure(connection, mode, proxy).await?;
            if measured.departed != departed {
                failures.push(format!("{label}: departed {}", measured.departed));
            }
            if measured.visited > bound {
                failures.push(format!(
                    "{label}: visited {} rows, bound {bound}\n{}",
                    measured.visited,
                    measured.text.join("\n")
                ));
            }
            failures.extend(missing_probes(&label, &measured.text, &probes));
            // Every arm reads the index.
            let scans = measured
                .text
                .iter()
                .filter(|line| line.contains(&format!("using {INDEX} on")))
                .count();
            if scans != 3 {
                failures.push(format!(
                    "{label}: {scans} scans of {INDEX}\n{}",
                    measured.text.join("\n")
                ));
            }
        }
    }
    Ok(failures)
}

#[tokio::test]
async fn the_departure_probe_visits_no_same_code_upgrade() -> Result<()> {
    with_database("registry_support_departure_plan", async |connection| {
        install(connection).await?;
        let mut failures = check(connection, "before ANALYZE").await?;
        raw_sql("ANALYZE normalized_events")
            .execute(&mut *connection)
            .await?;
        failures.extend(check(connection, "after ANALYZE").await?);
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
        // Without the index the arms read the proxy's upgrades through another index and
        // visit every same-code upgrade, so the bound above can fail.
        raw_sql(&format!("DROP INDEX {INDEX}"))
            .execute(&mut *connection)
            .await?;
        let control = measure(connection, "force_generic_plan", 1).await?;
        ensure!(
            control.visited >= UPGRADES,
            "the control visited {}\n{}",
            control.visited,
            control.text.join("\n")
        );
        Ok(())
    })
    .await
}
