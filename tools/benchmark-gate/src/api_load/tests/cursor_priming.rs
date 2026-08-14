use super::*;

#[tokio::test]
async fn cursor_priming_continues_past_the_fixed_prefix() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for index in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let body = if index < 2 {
                r#"{"data":[{"name":"one-page.eth"}]}"#
            } else {
                r#"{"data":[{"name":"paginated.eth"}],"page":{"next_cursor":"later"}}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let base = normalized_base_url(&format!("http://{address}")).unwrap();
    let requests = (0..3)
        .map(|index| get(&base, &["v2", "events", &index.to_string()], &[]).unwrap())
        .collect::<Vec<_>>();

    let primed = prime_cursor_variants(&Client::new(), "events", requests, 2, 10)
        .await
        .unwrap();
    let probe = primed.probe;
    let requests = primed.requests;
    server.abort();

    assert!(probe.populated);
    assert_eq!(probe.cursor_variants, 1);
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .any(|request| request.url.query() == Some("cursor=later"))
    );
}

#[tokio::test]
async fn cursor_priming_exhausts_the_corpus_without_inventing_a_cursor() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server_served = Arc::clone(&served);
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            server_served.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let body = r#"{"data":[{"name":"one-page.eth"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let base = normalized_base_url(&format!("http://{address}")).unwrap();
    let requests = (0..3)
        .map(|index| get(&base, &["v2", "events", &index.to_string()], &[]).unwrap())
        .collect::<Vec<_>>();

    let primed = prime_cursor_variants(&Client::new(), "events", requests, 2, 10)
        .await
        .unwrap();
    let probe = primed.probe;
    let requests = primed.requests;
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("cursor-exhaustion mock did not receive the bounded corpus within two seconds")
        .unwrap();

    assert!(probe.populated);
    assert_eq!(served.load(std::sync::atomic::Ordering::Relaxed), 3);
    assert_eq!(probe.cursor_variants, 0);
    assert_eq!(requests.len(), 3);
    let budgets = BudgetsFile::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
    )
    .unwrap();
    assert!(
        require_seed_probe(budgets.profile(BudgetProfile::Production), "events", probe).is_err()
    );
}

#[tokio::test]
async fn cursor_priming_deduplicates_identical_resumed_requests() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let body = r#"{"data":[{"name":"same.eth"}],"page":{"next_cursor":"same"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let base = normalized_base_url(&format!("http://{address}")).unwrap();
    let seed = get(&base, &["v2", "events"], &[]).unwrap();

    let primed = prime_cursor_variants(&Client::new(), "events", vec![seed.clone(), seed], 2, 10)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(primed.probe.cursor_variants, 1);
    assert_eq!(primed.unique_cursor_variants, 1);
}
