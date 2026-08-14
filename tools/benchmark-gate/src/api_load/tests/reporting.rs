use super::*;
use serde_json::json;

#[test]
fn records_probe_requires_a_found_requested_record() {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let both = get(
        &base,
        &["v2", "names", "records.eth", "records"],
        &[("keys", "addr:60,text:avatar")],
    )
    .unwrap();
    let address_only = get(
        &base,
        &["v2", "names", "records.eth", "records"],
        &[("keys", "addr:60")],
    )
    .unwrap();
    assert!(!response_is_populated(
        "records",
        &json!({
            "data": {
                "addresses": {"60": "0x0000000000000000000000000000000000000001"},
                "text_records": {},
                "content_hash": null
            }
        })
    ));
    assert!(!requested_records_are_populated(
        &both,
        &json!({
            "data": {
                "records": {
                    "addr:60": {"status": "not_found"},
                    "text:avatar": {"status": "not_found"}
                }
            }
        })
    ));
    assert!(requested_records_are_populated(
        &both,
        &json!({
            "data": {
                "records": {
                    "addr:60": {"status": "not_found"},
                    "text:avatar": {"status": "ok", "value": "ipfs://avatar"}
                }
            }
        })
    ));
    assert!(!requested_records_are_populated(
        &address_only,
        &json!({
            "data": {
                "records": {
                    "text:unrequested": {"status": "ok", "value": "wrong evidence"}
                }
            }
        })
    ));
}

#[tokio::test]
async fn timed_keyless_record_aggregates_do_not_satisfy_the_requested_key_floor() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = stream.read(&mut request).await.unwrap();
        let body = r#"{"data":{"addresses":{"60":"0x0000000000000000000000000000000000000001"},"text_records":{},"content_hash":null}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    let base = normalized_base_url(&format!("http://{address}")).unwrap();
    let request = get(&base, &["v2", "names", "aggregate.eth", "records"], &[]).unwrap();

    let sample = sample_request(&Client::new(), &request, "records", Instant::now(), false).await;
    server.await.unwrap();

    assert!(sample.success);
    assert_eq!(sample.outcome, "200:records_aggregate_populated");
    assert!(!sample.outcome.ends_with(":records_populated"));
}

#[test]
fn preflight_failure_report_records_observed_totals_and_floors() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
    let budgets = BudgetsFile::load(&path).unwrap();
    let production = budgets.profile(BudgetProfile::Production);
    let report = preflight_failure_report(
        corpus::TableScale {
            name_current_rows: 50_000,
            address_names_current_rows: 75_000,
        },
        production,
        ApiTargetIdentity {
            build_sha: "release".to_owned(),
            interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            database_identity: "keccak256:database".to_owned(),
        },
        "keccak256:database".to_owned(),
        Some("release"),
        vec!["name_current is below its floor".to_owned()],
    );

    assert!(!report.green);
    assert_eq!(report.name_current_rows, 50_000);
    assert_eq!(report.min_name_current_rows, 3_000_000);
    assert_eq!(report.address_names_current_rows, 75_000);
    assert_eq!(report.min_address_names_current_rows, 3_000_000);
    assert!(report.endpoints.is_empty());
}

#[test]
fn reachable_health_without_database_identity_returns_a_red_report() {
    let (identity, failures) = health_identity(
        HealthResponse {
            identity: HealthIdentity {
                build_sha: "release".to_owned(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            },
            database: HealthDatabase { identity: None },
        },
        false,
    )
    .unwrap();
    assert_eq!(identity.database_identity, "<missing>");
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("omitted database.identity"));

    let budgets = BudgetsFile::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
    )
    .unwrap();
    let report = preflight_failure_report(
        corpus::TableScale {
            name_current_rows: 3_000_000,
            address_names_current_rows: 3_000_000,
        },
        budgets.profile(BudgetProfile::Production),
        identity,
        "corpus-identity".to_owned(),
        Some("release"),
        failures,
    );
    assert!(!report.green);
    assert!(report.failures[0].contains("database.identity"));
}

#[test]
fn exhausted_seed_probe_has_a_named_zero_request_red_report() {
    let budgets = BudgetsFile::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
    )
    .unwrap();
    let production = budgets.profile(BudgetProfile::Production);
    let endpoint = production
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == "search")
        .unwrap();
    let report = endpoint_failure_report(
        endpoint,
        production,
        "seed probing did not establish required evidence: bare search produced no populated real continuation cursor".to_owned(),
    );
    assert!(!report.green);
    assert_eq!(report.requests, 0);
    assert!(report.failures[0].contains("seed probing"));
    assert!(report.failures[0].contains("bare search"));
}

#[test]
fn failed_seed_probe_has_a_named_zero_request_red_report() {
    let budgets = BudgetsFile::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
    )
    .unwrap();
    let production = budgets.profile(BudgetProfile::Production);
    let endpoint = production
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == "events")
        .unwrap();
    let report = endpoint_failure_report(
        endpoint,
        production,
        "seed probing failed: API benchmark request failed: connection refused".to_owned(),
    );

    assert!(!report.green);
    assert_eq!(report.requests, 0);
    assert!(report.failures[0].contains("seed probing failed"));
    assert!(report.failures[0].contains("connection refused"));
}

#[test]
fn api_orchestrator_routes_preflight_corpus_and_seed_failures_into_reports() {
    let source = include_str!("../../api_load.rs");
    assert!(source.contains("let (identity, health_failures) = load_api_preflight_identity"));
    assert!(source.contains("let mut preflight_failures = health_failures"));
    assert!(source.contains("return Ok(corpus_failure_report("));
    assert!(source.contains("Err(error) => endpoint_failure_report("));
    assert!(source.contains("seed probing did not establish required evidence"));
}
