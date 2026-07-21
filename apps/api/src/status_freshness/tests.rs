use tokio::net::TcpListener;

use super::*;

#[tokio::test]
async fn comparison_distinguishes_probe_states_and_applies_both_lag_thresholds() -> Result<()> {
    let urls = ChainRpcUrls::from_entries(&["ethereum-mainnet=http://rpc.test".to_owned()])?;
    let freshness = StatusFreshness::new(StatusFreshnessConfig::new(50, 5, 30, 5, 60)?);
    assert_eq!(
        freshness
            .compare(&urls, "ethereum-mainnet", Some(100), None)
            .await
            .status,
        NetworkHeadStatus::Pending
    );

    freshness.seed_unavailable("ethereum-mainnet").await;
    assert_eq!(
        freshness
            .compare(&urls, "ethereum-mainnet", Some(100), None)
            .await
            .status,
        NetworkHeadStatus::Unavailable
    );

    let observed_at = OffsetDateTime::now_utc();
    freshness
        .seed_success("ethereum-mainnet", 106, observed_at)
        .await;
    let block_lag = freshness
        .compare(&urls, "ethereum-mainnet", Some(100), Some(observed_at))
        .await;
    assert_eq!(block_lag.status, NetworkHeadStatus::Fresh);
    assert_eq!(block_lag.ingestion_lag_blocks, Some(6));
    assert_eq!(block_lag.ingestion_lag_seconds, Some(0));
    assert!(block_lag.data_is_stale);
    assert_eq!(
        status_readiness(Some(100), Some(100), Some(0), &block_lag),
        StatusReadiness::Stale
    );

    freshness
        .seed_success("ethereum-mainnet", 101, observed_at)
        .await;
    let time_lag = freshness
        .compare(
            &urls,
            "ethereum-mainnet",
            Some(100),
            Some(observed_at - Duration::from_secs(61)),
        )
        .await;
    assert_eq!(time_lag.ingestion_lag_blocks, Some(1));
    assert_eq!(time_lag.ingestion_lag_seconds, Some(61));
    assert!(time_lag.data_is_stale);

    freshness.seed_unavailable("ethereum-mainnet").await;
    let unavailable = freshness
        .compare(&urls, "ethereum-mainnet", Some(100), Some(observed_at))
        .await;
    assert_eq!(
        status_readiness(Some(100), Some(100), Some(0), &unavailable),
        StatusReadiness::Degraded
    );

    assert_eq!(
        freshness
            .compare(
                &ChainRpcUrls::default(),
                "ethereum-mainnet",
                Some(100),
                None
            )
            .await
            .status,
        NetworkHeadStatus::Unconfigured
    );
    Ok(())
}

#[tokio::test]
async fn slow_provider_refresh_is_bounded_and_never_blocks_cached_comparison() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("slow probe must connect");
        std::future::pending::<()>().await;
    });
    let urls = ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={endpoint}")])?;
    let freshness = StatusFreshness::new(StatusFreshnessConfig::new(500, 5, 30, 5, 60)?);
    let refresh = {
        let freshness = freshness.clone();
        let urls = urls.clone();
        tokio::spawn(async move { freshness.refresh_once(&urls).await })
    };
    tokio::task::yield_now().await;

    let comparison = tokio::time::timeout(
        Duration::from_millis(100),
        freshness.compare(&urls, "ethereum-mainnet", Some(100), None),
    )
    .await
    .expect("cached status comparison must not wait for the provider probe");
    assert_eq!(comparison.status, NetworkHeadStatus::Pending);

    tokio::time::timeout(Duration::from_millis(750), refresh)
        .await
        .expect("provider timeout must bound the asynchronous refresh")
        .expect("refresh task must not panic");
    assert_eq!(
        freshness
            .compare(&urls, "ethereum-mainnet", Some(100), None)
            .await
            .status,
        NetworkHeadStatus::Unavailable
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn successful_refresh_caches_eth_block_number_and_cache_age_is_explicit() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("probe must connect");
        let mut request = vec![0_u8; 4096];
        let count = stream.read(&mut request).await.expect("request must read");
        let request = String::from_utf8_lossy(&request[..count]);
        assert!(request.contains("eth_blockNumber"));
        let body = r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("response must write");
    });
    let urls = ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={endpoint}")])?;
    let freshness = StatusFreshness::new(StatusFreshnessConfig::new(200, 5, 30, 5, 60)?);

    freshness.refresh_once(&urls).await;
    server.await.expect("provider task must not panic");
    let comparison = freshness
        .compare(&urls, "ethereum-mainnet", Some(42), None)
        .await;
    assert_eq!(comparison.status, NetworkHeadStatus::Fresh);
    assert_eq!(comparison.block, Some(42));
    assert_eq!(comparison.ingestion_lag_blocks, Some(0));
    assert!(comparison.observed_at.is_some());
    assert!(comparison.age_seconds.is_some_and(|age| age <= 1));

    freshness
        .seed_success(
            "ethereum-mainnet",
            42,
            OffsetDateTime::now_utc() - Duration::from_secs(31),
        )
        .await;
    let stale_cache = freshness
        .compare(&urls, "ethereum-mainnet", Some(42), None)
        .await;
    assert_eq!(stale_cache.status, NetworkHeadStatus::Stale);
    assert_eq!(
        status_readiness(Some(42), Some(42), Some(0), &stale_cache),
        StatusReadiness::Degraded
    );
    Ok(())
}

#[tokio::test]
async fn failed_refresh_degrades_but_retains_the_last_successful_head_as_evidence() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("failed probe must connect");
        drop(stream);
    });
    let urls = ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={endpoint}")])?;
    let freshness = StatusFreshness::new(StatusFreshnessConfig::new(200, 5, 30, 5, 60)?);
    freshness
        .seed_success("ethereum-mainnet", 42, OffsetDateTime::now_utc())
        .await;

    freshness.refresh_once(&urls).await;
    server.await.expect("failed provider task must not panic");
    let comparison = freshness
        .compare(&urls, "ethereum-mainnet", Some(42), None)
        .await;

    assert_eq!(comparison.status, NetworkHeadStatus::Unavailable);
    assert_eq!(comparison.block, Some(42));
    assert!(comparison.observed_at.is_some());
    assert_eq!(
        status_readiness(Some(42), Some(42), Some(0), &comparison),
        StatusReadiness::Degraded
    );
    Ok(())
}
