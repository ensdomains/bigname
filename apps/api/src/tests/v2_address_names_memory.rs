// Manual measurement, not run by default: peak resident memory, latency and SQL statement count
// of address-names requests for one address holding `BIGNAME_BULK_NAMES` names (default 20,000).
//   scripts/test-db -- cargo test -p bigname-api --features bigname-storage/test-support \
//     -- --ignored --nocapture v2_address_names_large_address_memory
// One router serves every request, after a discarded warm-up request.
// `BIGNAME_BULK_RESOLVES_TO=1` also points every name's resolver at the address on coin 60 and
// binds up to `BIGNAME_BULK_ALIASES` names (default 2,000) to the first name's registration,
// and adds the `relation=resolves_to` queries. `BIGNAME_BULK_QUERIES` replaces the query list.
// `BIGNAME_BULK_BLOCK_NAMES` spreads the names over blocks of that many, and
// `BIGNAME_BULK_SEED_LIMIT_SECS` fails a seed that runs longer.
// `BIGNAME_BULK_DATABASE=<name>` keeps the seeded database under that name and measures an
// existing one without seeding, so two builds can measure one seed, with each statement bounded
// by `BIGNAME_BULK_STATEMENT_TIMEOUT_MS`. `BIGNAME_BULK_LOG_SLOW_MS` sets the server's
// `log_min_duration_statement` for the seed and the requests, and its absence resets it.

fn resident_kib() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps must run");
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or_default()
}

/// The SQL statements a request runs, the time and summary of each that took a second or more,
/// from the `sqlx::query` events, and the request's warnings and errors.
#[derive(Clone, Default)]
struct MeasuredStatements(std::sync::Arc<std::sync::Mutex<RequestEvents>>);

#[derive(Clone, Default)]
struct RequestEvents {
    statements: usize,
    slow: Vec<SlowStatement>,
    warnings: Vec<String>,
}

/// A statement's time in seconds and its summary.
type SlowStatement = (f64, String);

#[derive(Default)]
struct StatementFields {
    elapsed: f64,
    summary: String,
    text: String,
}

impl tracing::field::Visit for StatementFields {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == "elapsed_secs" {
            self.elapsed = value;
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "summary" {
            self.summary = value.to_owned();
        }
        self.text.push_str(&format!(" {}={value}", field.name()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.text.push_str(&format!(" {}={value:?}", field.name()));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MeasuredStatements {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = StatementFields::default();
        event.record(&mut fields);
        let mut measured = self.0.lock().unwrap();
        let metadata = event.metadata();
        if metadata.target() == "sqlx::query" {
            measured.statements += 1;
            if fields.elapsed >= 1.0 {
                measured.slow.push((fields.elapsed, fields.summary));
            }
        } else if *metadata.level() <= tracing::Level::WARN {
            let warning = format!("{} {}:{}", metadata.level(), metadata.target(), fields.text);
            measured.warnings.push(warning);
        }
    }
}

struct Measured {
    status: StatusCode,
    elapsed: std::time::Duration,
    peak_kib: u64,
    statements: usize,
    slow: Vec<SlowStatement>,
    warnings: Vec<String>,
    payload: Value,
}

/// The request's status and latency, the process's peak resident memory above its level before
/// it, and its statements.
async fn measure(router: &axum::Router, uri: &str) -> Result<Measured> {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    let before = resident_kib();
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(before));
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (peak, done) = (peak.clone(), done.clone());
        std::thread::spawn(move || {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                peak.fetch_max(resident_kib(), std::sync::atomic::Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        })
    };
    let statements = MeasuredStatements::default();
    let started = std::time::Instant::now();
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request must build");
    let response = router
        .clone()
        .oneshot(request)
        .with_subscriber(tracing_subscriber::registry().with(statements.clone()))
        .await?;
    let status = response.status();
    let payload = read_json::<Value>(response).await?;
    let elapsed = started.elapsed();
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    sampler.join().expect("sampler must finish");
    let peak = peak.load(std::sync::atomic::Ordering::Relaxed);
    let events = statements.0.lock().unwrap().clone();
    Ok(Measured {
        status,
        elapsed,
        peak_kib: peak.saturating_sub(before),
        statements: events.statements,
        slow: events.slow,
        warnings: events.warnings,
        payload,
    })
}

/// Seeds `count` bulk names, with resolver records and a registration of up to
/// `BIGNAME_BULK_ALIASES` (default 2,000) names when `resolves_to`, within
/// `BIGNAME_BULK_SEED_LIMIT_SECS` when set.
async fn seed_bulk_measurement(
    database: &TestDatabase,
    count: usize,
    resolves_to: bool,
) -> Result<()> {
    let started = std::time::Instant::now();
    let limit = std::env::var("BIGNAME_BULK_SEED_LIMIT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map_or(std::time::Duration::MAX, std::time::Duration::from_secs);
    tokio::time::timeout(limit, seed_bulk_names_for(database, count, resolves_to))
        .await
        .map_err(|_| anyhow::anyhow!("the seed of {count} names passed {limit:?}"))??;
    println!("seeded {count} names in {:?}", started.elapsed());
    Ok(())
}

async fn seed_bulk_names_for(
    database: &TestDatabase,
    count: usize,
    resolves_to: bool,
) -> Result<()> {
    if resolves_to {
        let aliases: usize = std::env::var("BIGNAME_BULK_ALIASES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2_000);
        let aliases: Vec<(usize, usize)> = (1..count.min(aliases + 1))
            .map(|index| (0, index))
            .collect();
        seed_bulk_names_with_records(
            database,
            count,
            |_| BULK_ADDRESS,
            &aliases,
            |_| BulkRecords {
                writes: vec![("addr:60".to_owned(), BULK_ADDRESS.to_owned())],
                cleared: false,
            },
        )
        .await?;
    } else {
        seed_bulk_address_names(database, count).await?;
    }
    Ok(())
}

/// Refreshes the planner's statistics, failing rather than waiting a minute on a lock.
async fn analyze_bulk_database(pool: &PgPool) -> Result<()> {
    let started = std::time::Instant::now();
    sqlx::raw_sql("BEGIN; SET LOCAL lock_timeout = '60s'; ANALYZE; COMMIT")
        .execute(pool)
        .await?;
    println!("analyzed in {:?}", started.elapsed());
    Ok(())
}

fn bulk_connect_options() -> Result<sqlx::postgres::PgConnectOptions> {
    Ok(std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))?
        .parse()?)
}

/// Sets the server's `log_min_duration_statement` to `BIGNAME_BULK_LOG_SLOW_MS`, or resets it,
/// for the seed and the requests that follow.
async fn log_slow_bulk_statements() -> Result<()> {
    let admin = PgPool::connect_with(bulk_connect_options()?).await?;
    let slow = match std::env::var("BIGNAME_BULK_LOG_SLOW_MS") {
        Ok(ms) => format!("SET log_min_duration_statement = {}", ms.parse::<u64>()?),
        Err(_) => "RESET log_min_duration_statement".to_owned(),
    };
    sqlx::raw_sql(&format!("ALTER SYSTEM {slow}"))
        .execute(&admin)
        .await?;
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&admin)
        .await?;
    admin.close().await;
    Ok(())
}

/// A pool on the kept database `name`, seeding it first under that name when it does not
/// exist, with the statement timeout the environment asks for.
async fn kept_bulk_database(name: &str, count: usize, resolves_to: bool) -> Result<PgPool> {
    let options = bulk_connect_options()?;
    let admin = PgPool::connect_with(options.clone()).await?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(&admin)
            .await?;
    if exists {
        println!("measuring the kept database {name}");
    } else {
        let database = TestDatabase::new_migrated().await?;
        seed_bulk_measurement(&database, count, resolves_to).await?;
        database.pool.close().await;
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&database.database_name)
        .execute(&admin)
        .await?;
        sqlx::raw_sql(&format!(
            "ALTER DATABASE \"{}\" RENAME TO \"{name}\"",
            database.database_name
        ))
        .execute(&admin)
        .await?;
        println!("kept the seed as {name}");
        let kept = PgPool::connect_with(options.clone().database(name)).await?;
        analyze_bulk_database(&kept).await?;
        kept.close().await;
    }
    admin.close().await;
    let timeout =
        std::env::var("BIGNAME_BULK_STATEMENT_TIMEOUT_MS").unwrap_or_else(|_| "0".to_owned());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .connect_with(options.database(name).options([
            ("search_path", "bigname_phase"),
            ("statement_timeout", timeout.as_str()),
        ]))
        .await?;
    Ok(pool)
}

#[tokio::test]
#[ignore = "manual memory measurement"]
async fn v2_address_names_large_address_memory() -> Result<()> {
    let count: usize = std::env::var("BIGNAME_BULK_NAMES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let resolves_to = std::env::var("BIGNAME_BULK_RESOLVES_TO").is_ok_and(|value| value == "1");
    let kept = std::env::var("BIGNAME_BULK_DATABASE").ok();
    log_slow_bulk_statements().await?;
    let (pool, database) = match &kept {
        Some(name) => (kept_bulk_database(name, count, resolves_to).await?, None),
        None => {
            let database = TestDatabase::new_migrated().await?;
            seed_bulk_measurement(&database, count, resolves_to).await?;
            analyze_bulk_database(&database.pool).await?;
            (database.pool.clone(), Some(database))
        }
    };
    // Freed memory stays resident, so the reads that compose every name run last.
    let mut defaults = vec![
        "page_size=50",
        "page_size=50&sort=expires_at",
        "page_size=50&dedupe=registration&order=desc",
    ];
    if resolves_to {
        defaults.extend([
            "relation=resolves_to&page_size=5",
            "relation=resolves_to&coin_type=evm&page_size=5",
            "relation=resolves_to&dedupe=registration&page_size=5",
            "relation=resolves_to&sort=expires_at&page_size=5",
            "relation=resolves_to&page_size=200",
            "relation=resolves_to&q=zzz&page_size=5",
            "relation=resolves_to&sort=created_at&page_size=5",
        ]);
    }
    defaults.extend([
        "page_size=200&sort=created_at",
        "page_size=50&q=zzz",
        "page_size=1&relation=former_owner",
        "page_size=50&include=total_count",
    ]);
    let queries = std::env::var("BIGNAME_BULK_QUERIES").unwrap_or_else(|_| defaults.join(" "));
    let router = app_router(
        AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default())
            .with_public_namespaces_for_test(["ens", "basenames"]),
    );
    if !queries.trim().is_empty() {
        // One discarded request first, so no measured request pays the process's first use.
        let warm_uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=1");
        let warm = measure(&router, &warm_uri).await?;
        println!("warm-up: {} {:?}", warm.status.as_u16(), warm.elapsed);
    }
    for query in queries.split_whitespace() {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?{query}");
        let measured = measure(&router, &uri).await?;
        println!(
            "{query}: {} {:?}, peak +{} MiB, statements {}, rows {}, total_count {}",
            measured.status.as_u16(),
            measured.elapsed,
            measured.peak_kib / 1024,
            measured.statements,
            measured.payload["data"].as_array().map_or(0, Vec::len),
            measured.payload["page"]["total_count"]
        );
        for (elapsed, summary) in &measured.slow {
            println!("  slow statement {elapsed:.3} s: {summary}");
        }
        if !measured.status.is_success() {
            println!("  error body: {}", measured.payload["error"]);
            for warning in &measured.warnings {
                println!("  {warning}");
            }
        }
    }
    pool.close().await;
    match database {
        Some(database) => database.cleanup().await,
        None => Ok(()),
    }
}
