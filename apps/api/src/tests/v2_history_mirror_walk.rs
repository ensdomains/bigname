//! History for a name migrated to ENSv2 whose ENSv2 resolver is the declared ENSv1 mirror, read
//! through the real routes. The served records are the ENSv1 resolver's that the mirror's
//! registry walk selects (`history/attribution/mirror.rs`). Here the name's ENSv1 registry pointer
//! is old and the chain has since carried 250,000 unrelated ENSv1 pointers, the shape in which the
//! mirror's registry pointer lookup, planned with the bound's block as a literal, can walk the
//! chain's rows newest first instead of probing the pointer index. Each route is timed on fresh
//! connections, the first-execution custom plans, and every executed mirror statement's plan is
//! read back from `auto_explain`.

use super::*;
use sqlx::postgres::PgPoolOptions;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

const MIRROR: &str = "0x1010101010101010101010101010101010101010";
const POINTER_INDEX: &str = "normalized_events_project_v1_pointer_addressed_node_idx";
const MIRROR_STATEMENT: &str = "WITH walk AS (SELECT * FROM unnest(";
const TARGET: Duration = Duration::from_millis(500);
const RUNS: usize = 3;

/// The fixture's ENSv1 pointer moved to an old block, then 250,000 unrelated ENSv1 pointers over
/// 2,000 other nodes in the 30,000 blocks after it, all below the published block. Statistics
/// sample the whole table (300 x 1100 rows), so the plans are the same every run, and autovacuum
/// is off so no later analyze replaces them.
const POINTER_HEAVY_CHAIN: &str = r#"
ALTER TABLE bigname_phase.normalized_events SET (autovacuum_enabled = false);
ALTER TABLE bigname_phase.chain_lineage SET (autovacuum_enabled = false);
ALTER TABLE bigname_phase.name_surfaces SET (autovacuum_enabled = false);
INSERT INTO bigname_phase.chain_lineage(chain_id, block_hash, block_number, block_timestamp,
    canonicality_state)
SELECT 'ethereum-sepolia', 'heavy-' || b, b, to_timestamp(1775000000 + b - 20899000),
       'canonical'::canonicality_state
FROM generate_series(20899000, 20929999) b
ON CONFLICT DO NOTHING;
UPDATE bigname_phase.normalized_events
SET block_number = 20899000, block_hash = 'heavy-20899000'
WHERE event_identity = 'mirror-v1-pointer';
INSERT INTO bigname_phase.normalized_events(event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, block_hash, block_number, transaction_hash, transaction_index,
    log_index, derivation_kind, canonicality_state, after_state)
SELECT 'heavy:pointer:' || n, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1, 'ethereum-sepolia',
       'heavy-' || (20900000 + n % 30000), 20900000 + n % 30000, 'tx-heavy-' || n, 1, n % 1000,
       'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('node', '0x' || lpad(to_hex(200000 + n % 2000), 64, '0'),
                          'resolver', '0x' || lpad(to_hex(9), 40, '0'))
FROM generate_series(1, 250000) n;
SET default_statistics_target = 1100;
ANALYZE bigname_phase.normalized_events, bigname_phase.name_surfaces, bigname_phase.chain_lineage;
RESET default_statistics_target;
"#;

/// Every statement's executed plan, sent to the client as a notice.
const AUTO_EXPLAIN: &str = "LOAD 'auto_explain';
    SET auto_explain.log_min_duration = 0;
    SET auto_explain.log_analyze = on;
    SET auto_explain.log_buffers = on;
    SET auto_explain.log_format = 'json';
    SET auto_explain.log_level = 'notice';";

#[derive(Clone, Default)]
struct Notices(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Notices {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "sqlx::postgres::notice" {
            let mut message = NoticeMessage(None);
            event.record(&mut message);
            if let Some(message) = message.0 {
                self.0.lock().unwrap().push(message);
            }
        }
    }
}

struct NoticeMessage(Option<String>);

impl tracing::field::Visit for NoticeMessage {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

struct Explained {
    duration_ms: f64,
    text: String,
    plan: Value,
}

/// An `auto_explain` notice: `duration: <ms> ms  plan:` and the JSON object.
fn explained(message: &str) -> Option<Explained> {
    let (duration, rest) = message.strip_prefix("duration: ")?.split_once(" ms")?;
    let value: Value = serde_json::from_str(&rest[rest.find('{')?..]).ok()?;
    Some(Explained {
        duration_ms: duration.trim().parse().ok()?,
        text: value["Query Text"].as_str()?.to_owned(),
        plan: value["Plan"].clone(),
    })
}

/// The registry pointer reads: `normalized_events` scans the planner aliases `registry…`.
fn registry_reads(node: &Value) -> Vec<Value> {
    let mut found = Vec::new();
    if node["Relation Name"] == "normalized_events"
        && node["Alias"]
            .as_str()
            .is_some_and(|alias| alias.starts_with("registry"))
    {
        let mut indexes = Vec::new();
        index_names(node, &mut indexes);
        found.push(
            json!({"type":node["Node Type"],"alias":node["Alias"],"indexes":indexes,
            "rows":node["Actual Rows"],"removed":node["Rows Removed by Filter"],
            "loops":node["Actual Loops"],"shared_hit":node["Shared Hit Blocks"],
            "shared_read":node["Shared Read Blocks"]}),
        );
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        found.extend(registry_reads(child));
    }
    found
}

fn index_names(node: &Value, indexes: &mut Vec<String>) {
    if let Some(index) = node["Index Name"].as_str() {
        indexes.push(index.to_owned());
    }
    if matches!(
        node["Node Type"].as_str(),
        Some("Bitmap Heap Scan" | "BitmapOr" | "BitmapAnd")
    ) {
        for child in node["Plans"].as_array().into_iter().flatten() {
            index_names(child, indexes);
        }
    }
}

async fn route_pool(database: &TestDatabase, explain: bool) -> Result<PgPool> {
    let mut options = PgPoolOptions::new().max_connections(6);
    if explain {
        options = options.after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::raw_sql(AUTO_EXPLAIN)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        });
    }
    let pool = options
        .connect_with(database.pool.connect_options().as_ref().clone())
        .await?;
    sqlx::query("SELECT 1").execute(&pool).await?;
    Ok(pool)
}

/// One request on a new pool, so every statement runs on a connection that has not prepared it.
async fn request(
    database: &TestDatabase,
    uri: &str,
    public_namespaces: bool,
    notices: Option<Notices>,
) -> Result<(Duration, Value)> {
    let pool = route_pool(database, notices.is_some()).await?;
    let mut state =
        AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default());
    if public_namespaces {
        state = state.with_public_namespaces_for_test(["ens", "basenames"]);
    }
    let started = Instant::now();
    let response = async {
        let response = app_router(state)
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        Ok::<_, anyhow::Error>((status, body))
    };
    let (status, body) = match notices {
        Some(notices) => {
            response
                .with_subscriber(tracing_subscriber::registry().with(notices))
                .await?
        }
        None => response.await?,
    };
    let elapsed = started.elapsed();
    pool.close().await;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status} {body}");
    Ok((elapsed, body))
}

#[tokio::test]
async fn name_history_under_the_ensv1_mirror_reads_the_pointer_index_within_the_target()
-> Result<()> {
    let database =
        v2_mirror_records_database("alice.eth", MIRROR, MirrorFixtureSource::Exact, "resolver")
            .await?;
    let result = async {
        let started = Instant::now();
        sqlx::raw_sql(POINTER_HEAVY_CHAIN)
            .execute(&database.pool)
            .await?;
        println!("pointer-heavy chain installed in {:?}", started.elapsed());
        let address = format!(
            "/v1/addresses/{V2_ADDRESS}/history?relation=owner&scope=registration&kind=RecordChanged&page_size=200&include=total_count"
        );
        let mut failures = Vec::new();
        // The name route attributes with every write of the resource; the address route checks
        // its candidate pairs (`matching_attribution_pairs`). Both run the mirror statement.
        for (label, uri, public_namespaces) in [
            (
                "name history",
                "/v1/names/alice.eth/history?include=data,raw",
                true,
            ),
            ("address history", address.as_str(), false),
        ] {
            for run in 1..=RUNS {
                let (elapsed, body) = request(&database, uri, public_namespaces, None).await?;
                let rows = body["data"].as_array().cloned().unwrap_or_default();
                let mirrored = if label == "name history" {
                    rows.iter()
                        .filter(|row| row["kind"] == "RecordChanged")
                        .count()
                } else {
                    rows.len()
                };
                println!(
                    "{label} run {run}: {:.1} ms, {} rows, {mirrored} mirrored record writes",
                    elapsed.as_secs_f64() * 1000.0,
                    rows.len()
                );
                if mirrored != 3 {
                    failures.push(format!(
                        "{label} run {run}: {mirrored} mirrored record writes, expected 3: {body}"
                    ));
                }
                if elapsed >= TARGET {
                    failures.push(format!(
                        "{label} run {run}: {elapsed:?}, the target is under {TARGET:?}"
                    ));
                }
            }

            let notices = Notices::default();
            request(&database, uri, public_namespaces, Some(notices.clone())).await?;
            let statements = notices
                .0
                .lock()
                .unwrap()
                .iter()
                .filter_map(|message| explained(message.as_str()))
                .collect::<Vec<_>>();
            for statement in statements.iter().filter(|s| s.duration_ms >= 5.0) {
                println!(
                    "{label}, explained run, statement {:.1} ms: {}",
                    statement.duration_ms,
                    statement
                        .text
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .chars()
                        .take(120)
                        .collect::<String>()
                );
            }
            let mirror = statements
                .iter()
                .filter(|s| s.text.trim_start().starts_with(MIRROR_STATEMENT))
                .collect::<Vec<_>>();
            if mirror.is_empty() {
                failures.push(format!(
                    "{label}: no mirror statement among {} explained statements",
                    statements.len()
                ));
            }
            for statement in mirror {
                let reads = registry_reads(&statement.plan);
                println!(
                    "{label}, explained run, mirror statement {:.1} ms, {} rows, registry reads {}",
                    statement.duration_ms,
                    statement.plan["Actual Rows"],
                    json!(reads)
                );
                if reads.is_empty()
                    || reads.iter().any(|read| {
                        read["indexes"].as_array().is_none_or(|indexes| {
                            indexes.is_empty() || indexes.iter().any(|index| index != POINTER_INDEX)
                        })
                    })
                {
                    failures.push(format!(
                        "{label}: the mirror's registry pointer read is not on {POINTER_INDEX} \
                         alone: {}\nplan: {}",
                        json!(reads),
                        statement.plan
                    ));
                }
            }
        }
        anyhow::ensure!(
            failures.is_empty(),
            "name history under the ENSv1 mirror:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
