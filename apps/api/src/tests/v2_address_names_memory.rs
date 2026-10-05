// Manual measurement, not run by default: peak resident memory and latency of address-names
// requests for one address holding `BIGNAME_BULK_NAMES` names (default 20,000).
//   scripts/test-db -- cargo test -p bigname-api --features bigname-storage/test-support \
//     -- --ignored --nocapture v2_address_names_large_address_memory

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

/// The request's latency and the process's peak resident memory above its level before it.
async fn measure(database: &TestDatabase, uri: &str) -> Result<(std::time::Duration, u64, Value)> {
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
    let started = std::time::Instant::now();
    let response = v2_address_names_response_for_database(database, uri).await?;
    let status = response.status();
    let payload = read_json::<Value>(response).await?;
    let elapsed = started.elapsed();
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    sampler.join().expect("sampler must finish");
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {payload}");
    let peak = peak.load(std::sync::atomic::Ordering::Relaxed);
    Ok((elapsed, peak.saturating_sub(before), payload))
}

#[tokio::test]
#[ignore = "manual memory measurement"]
async fn v2_address_names_large_address_memory() -> Result<()> {
    let count: usize = std::env::var("BIGNAME_BULK_NAMES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    let database = TestDatabase::new_migrated().await?;
    let started = std::time::Instant::now();
    seed_bulk_address_names(&database, count).await?;
    println!("seeded {count} names in {:?}", started.elapsed());
    // Freed memory stays resident, so the reads that compose every name run last.
    let queries = std::env::var("BIGNAME_BULK_QUERIES").unwrap_or_else(|_| {
        [
            "page_size=50",
            "page_size=50&sort=expires_at",
            "page_size=50&dedupe=registration&order=desc",
            "page_size=200&sort=created_at",
            "page_size=50&q=zzz",
            "page_size=1&relation=former_owner",
            "page_size=50&include=total_count",
        ]
        .join(" ")
    });
    for query in queries.split_whitespace() {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?{query}");
        let (elapsed, peak_kib, payload) = measure(&database, &uri).await?;
        println!(
            "{query}: {elapsed:?}, peak +{} MiB, rows {}, total_count {}",
            peak_kib / 1024,
            payload["data"].as_array().map_or(0, Vec::len),
            payload["page"]["total_count"]
        );
    }
    database.cleanup().await
}
