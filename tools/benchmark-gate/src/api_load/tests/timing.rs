use super::*;

#[tokio::test]
async fn response_timing_includes_the_complete_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        stream.write_all(b"done").await.unwrap();
    });
    let base = normalized_base_url(&format!("http://{address}")).unwrap();
    let request = get(&base, &["slow"], &[]).unwrap();
    let sample = sample_request(&Client::new(), &request, Instant::now()).await;
    server.await.unwrap();
    assert!(sample.success);
    assert!(sample.elapsed_micros >= 40_000);
}

#[tokio::test]
async fn response_timing_includes_pre_poll_queue_delay() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let base = normalized_base_url(&format!("http://{address}")).unwrap();
    let request = get(&base, &["queued"], &[]).unwrap();
    let scheduled_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sample = sample_request(&Client::new(), &request, scheduled_start).await;
    server.await.unwrap();

    assert!(
        sample.elapsed_micros >= 40_000,
        "latency sample omitted the pre-poll queue delay: {:?}",
        sample.elapsed_micros
    );
}

#[test]
fn execute_window_captures_dispatch_time_before_spawning_the_request_task() {
    let source = include_str!("../../api_load.rs");
    let execute_window = source
        .split_once("async fn execute_window")
        .unwrap()
        .1
        .split_once("fn build_endpoint_report")
        .unwrap()
        .0;
    let capture = execute_window
        .find("let scheduled_start = Instant::now()")
        .unwrap();
    let spawn = execute_window.find("tasks.spawn(async move").unwrap();

    assert!(
        capture < spawn,
        "dispatch time must be captured before the request task enters the executor queue"
    );
    assert!(execute_window.contains("sample_request(&client, &request, scheduled_start)"));
}
