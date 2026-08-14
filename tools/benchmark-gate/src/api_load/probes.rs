use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use reqwest::{Client, StatusCode, Url};
use serde_json::Value;

use super::{
    SeedProbe, corpus::Corpus, endpoint_requires_cursor, send, workload::RequestSpec, workload::get,
};
use crate::budgets::GateBudgets;

const DEFAULT_PRIMARY_NAME_PROBE_LIMIT: usize = 10;

#[derive(Debug, Default)]
pub(super) struct DefaultPrimaryNameProbe {
    pub(super) requests_sent: usize,
    pub(super) outcomes: BTreeMap<String, usize>,
    pub(super) failures: Vec<String>,
}

fn requires_cursor_probe(budgets: &GateBudgets, endpoint: &str) -> bool {
    budgets.api_require_cursor_variants
        || (budgets.api_require_resolver_cursor_variant && endpoint == "resolver")
}

pub(super) fn require_seed_probe(
    budgets: &GateBudgets,
    endpoint: &str,
    probe: SeedProbe,
) -> Result<()> {
    ensure!(
        !budgets.api_require_populated_probes || probe.populated,
        "endpoint {endpoint:?} returned no populated seed response"
    );
    ensure!(
        !budgets.api_require_populated_probes
            || endpoint != "search"
            || probe.bare_search_populated,
        "bare search returned no populated seed response"
    );
    ensure!(
        !requires_cursor_probe(budgets, endpoint)
            || !endpoint_requires_cursor(endpoint)
            || probe.cursor_variants > 0,
        "endpoint {endpoint:?} produced no real continuation cursor"
    );
    Ok(())
}

pub(super) async fn probe_default_primary_name(
    client: &Client,
    base: &Url,
    corpus: &Corpus,
    budgets: &GateBudgets,
) -> Result<DefaultPrimaryNameProbe> {
    probe_default_primary_name_with_requirement(
        client,
        base,
        corpus,
        budgets.api_require_populated_probes,
    )
    .await
}

async fn probe_default_primary_name_with_requirement(
    client: &Client,
    base: &Url,
    corpus: &Corpus,
    require_nonempty: bool,
) -> Result<DefaultPrimaryNameProbe> {
    let probes = default_primary_name_requests(base, corpus)?;
    let mut report = DefaultPrimaryNameProbe {
        requests_sent: probes.len(),
        ..Default::default()
    };
    if probes.is_empty() && require_nonempty {
        report.failures.push(
            "untimed primary-name default-source probe found no ens/coin-60 successful claim tuple; restore a production-shaped ENS coin-type-60 primary-name corpus and rerun the gate"
                .to_owned(),
        );
    }

    for (namespace, address, coin_type, request) in &probes {
        match send(client, request).await {
            Ok(response) => {
                let status = response.status();
                let body = response
                    .bytes()
                    .await
                    .ok()
                    .and_then(|body| serde_json::from_slice::<Value>(&body).ok());
                let error_code = body
                    .as_ref()
                    .and_then(|body| body.pointer("/error/code"))
                    .and_then(Value::as_str);
                let outcome = if status.is_success() {
                    format!("http_{}", status.as_u16())
                } else {
                    format!(
                        "http_{}:{}",
                        status.as_u16(),
                        error_code.unwrap_or("missing_code")
                    )
                };
                *report.outcomes.entry(outcome).or_default() += 1;
                if let Some(failure) = default_primary_name_failure(
                    namespace,
                    address,
                    coin_type,
                    status,
                    body.as_ref(),
                ) {
                    report.failures.push(failure);
                }
            }
            Err(error) => {
                *report
                    .outcomes
                    .entry("transport_error".to_owned())
                    .or_default() += 1;
                report.failures.push(format!(
                    "untimed primary-name default-source probe for namespace {namespace:?}, address {address:?}, coin type {coin_type:?} failed to complete: {error:#}; verify the drained API's live RPC configuration and rerun the gate"
                ));
            }
        }
    }

    Ok(report)
}

fn default_primary_name_requests(
    base: &Url,
    corpus: &Corpus,
) -> Result<Vec<(String, String, String, RequestSpec)>> {
    corpus
        .primary_names
        .iter()
        .filter(|(_, coin_type, namespace)| namespace == "ens" && coin_type == "60")
        .take(DEFAULT_PRIMARY_NAME_PROBE_LIMIT)
        .map(|(address, coin_type, namespace)| {
            Ok((
                namespace.clone(),
                address.clone(),
                coin_type.clone(),
                get(
                    base,
                    &["v2", "addresses", address, "primary-name"],
                    &[("namespace", namespace), ("coin_type", coin_type)],
                )?,
            ))
        })
        .collect()
}

fn default_primary_name_failure(
    namespace: &str,
    address: &str,
    coin_type: &str,
    status: StatusCode,
    body: Option<&Value>,
) -> Option<String> {
    let success_is_valid = status.is_success() && body.is_some_and(valid_primary_name_success);
    let error_code = body
        .and_then(|body| body.pointer("/error/code"))
        .and_then(Value::as_str);
    let documented_stale = status == StatusCode::CONFLICT
        && error_code == Some("stale")
        && body.is_some_and(valid_error_envelope);
    if success_is_valid || documented_stale {
        return None;
    }

    let reason = if status.is_success() {
        format!(
            "returned HTTP {} without the documented indexed-then-verified answer pair",
            status.as_u16()
        )
    } else {
        format!(
            "returned HTTP {} with error code {:?}; only a well-formed 409 stale response is accepted",
            status.as_u16(),
            error_code.unwrap_or("missing_code")
        )
    };
    Some(format!(
        "untimed primary-name default-source probe for namespace {namespace:?}, address {address:?}, coin type {coin_type:?} {reason}; verify the drained API's live RPC configuration and rerun the gate"
    ))
}

fn valid_primary_name_success(body: &Value) -> bool {
    body.pointer("/data/answers")
        .and_then(Value::as_array)
        .is_some_and(|answers| {
            answers.len() == 2
                && valid_primary_name_answer(&answers[0], "indexed")
                && valid_primary_name_answer(&answers[1], "verified")
        })
}

fn valid_primary_name_answer(answer: &Value, source: &str) -> bool {
    if answer.get("source").and_then(Value::as_str) != Some(source) {
        return false;
    }
    let Some(status) = answer.get("status").and_then(Value::as_str) else {
        return false;
    };
    match source {
        "indexed" => matches!(status, "ok" | "not_found" | "invalid_name" | "unsupported"),
        "verified" => matches!(
            status,
            "ok" | "not_found" | "mismatch" | "unsupported" | "failed"
        ),
        _ => false,
    }
}

fn valid_error_envelope(body: &Value) -> bool {
    body.pointer("/error/code").is_some_and(Value::is_string)
        && body.pointer("/error/message").is_some_and(Value::is_string)
        && body.pointer("/error/details").is_some_and(Value::is_object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_source_probe_names_a_server_failure_and_tuple() {
        let body = json!({"error": {"code": "internal_error", "message": "failed"}});
        let failure = default_primary_name_failure(
            "ens",
            "0x0000000000000000000000000000000000000001",
            "60",
            StatusCode::SERVICE_UNAVAILABLE,
            Some(&body),
        )
        .expect("5xx must make the untimed default-source probe red");

        assert!(failure.contains("namespace \"ens\""));
        assert!(failure.contains("coin type \"60\""));
        assert!(failure.contains("0x0000000000000000000000000000000000000001"));
        assert!(failure.contains("returned HTTP 503"));
    }

    #[test]
    fn default_source_probe_accepts_a_handled_stale_error() {
        let body = json!({
            "error": {"code": "stale", "message": "RPC is not configured", "details": {}}
        });

        assert!(
            default_primary_name_failure(
                "ens",
                "0x0000000000000000000000000000000000000001",
                "60",
                StatusCode::CONFLICT,
                Some(&body),
            )
            .is_none(),
            "the probe accepts the documented whole-request stale response"
        );
    }

    #[test]
    fn default_source_probe_rejects_invalid_input_for_a_proven_tuple() {
        let body = json!({
            "error": {"code": "invalid_input", "message": "source is required", "details": {}}
        });

        let failure = default_primary_name_failure(
            "ens",
            "0x0000000000000000000000000000000000000001",
            "60",
            StatusCode::BAD_REQUEST,
            Some(&body),
        )
        .expect("a proven tuple must not accept invalid_input");

        assert!(failure.contains("HTTP 400"));
        assert!(failure.contains("invalid_input"));
        assert!(failure.contains("0x0000000000000000000000000000000000000001"));
    }

    #[test]
    fn default_source_probe_pins_each_answers_documented_status_vocabulary() {
        let indexed = [
            ("ok", true),
            ("not_found", true),
            ("invalid_name", true),
            ("unsupported", true),
            ("mismatch", false),
            ("stale", false),
            ("failed", false),
        ];
        let verified = [
            ("ok", true),
            ("not_found", true),
            ("invalid_name", false),
            ("unsupported", true),
            ("mismatch", true),
            ("stale", false),
            ("failed", true),
        ];
        for (source, statuses) in [
            ("indexed", indexed.as_slice()),
            ("verified", verified.as_slice()),
        ] {
            for (status, accepted) in statuses {
                let indexed_status = if source == "indexed" { status } else { &"ok" };
                let verified_status = if source == "verified" { status } else { &"ok" };
                let body = json!({
                    "data": {
                        "answers": [
                            {"source": "indexed", "status": indexed_status},
                            {"source": "verified", "status": verified_status}
                        ]
                    }
                });
                let failure = default_primary_name_failure(
                    "ens",
                    "0x0000000000000000000000000000000000000001",
                    "60",
                    StatusCode::OK,
                    Some(&body),
                );
                assert_eq!(
                    failure.is_none(),
                    *accepted,
                    "{source} status {status:?} acceptance drifted"
                );
            }
        }

        let canonical = json!({
            "data": {
                "answers": [
                    {"source": "indexed", "status": "ok"},
                    {"source": "verified", "status": "ok"}
                ]
            }
        });
        assert!(
            default_primary_name_failure(
                "ens",
                "0x0000000000000000000000000000000000000001",
                "60",
                StatusCode::OK,
                Some(&canonical),
            )
            .is_none()
        );
    }

    #[test]
    fn default_source_requests_select_only_ens_coin_60_tuples() {
        let base = super::super::workload::normalized_base_url("http://127.0.0.1:3000").unwrap();
        let corpus = Corpus {
            names: Vec::new(),
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: vec![
                (
                    "0x0000000000000000000000000000000000000001".to_owned(),
                    "60".to_owned(),
                    "ens".to_owned(),
                ),
                (
                    "0x0000000000000000000000000000000000000002".to_owned(),
                    "60".to_owned(),
                    "basenames".to_owned(),
                ),
                (
                    "0x0000000000000000000000000000000000000003".to_owned(),
                    "1".to_owned(),
                    "ens".to_owned(),
                ),
            ],
            resolvers: Vec::new(),
            namespaces: vec!["basenames".to_owned(), "ens".to_owned()],
            names_by_namespace: Default::default(),
            parents_by_namespace: Default::default(),
            resolver_manifest_coverage: Vec::new(),
        };

        let requests = default_primary_name_requests(&base, &corpus).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "ens");
        assert_eq!(requests[0].1, "0x0000000000000000000000000000000000000001");
        assert_eq!(requests[0].2, "60");
    }

    #[tokio::test]
    async fn default_source_probe_records_nonzero_http_and_error_code_outcomes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (status, body) in [
                (
                    "200 OK",
                    json!({
                        "data": {
                            "answers": [
                                {"source": "indexed", "status": "ok"},
                                {"source": "verified", "status": "ok"}
                            ]
                        }
                    }),
                ),
                (
                    "409 Conflict",
                    json!({
                        "error": {"code": "stale", "message": "provider is not ready", "details": {}}
                    }),
                ),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                let body = serde_json::to_vec(&body).unwrap();
                let response = format!(
                    "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    status,
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let base =
            super::super::workload::normalized_base_url(&format!("http://{address}")).unwrap();
        let corpus = Corpus {
            names: Vec::new(),
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: (1..=2)
                .map(|index| (format!("0x{index:040x}"), "60".to_owned(), "ens".to_owned()))
                .collect(),
            resolvers: Vec::new(),
            namespaces: vec!["ens".to_owned()],
            names_by_namespace: Default::default(),
            parents_by_namespace: Default::default(),
            resolver_manifest_coverage: Vec::new(),
        };

        let report =
            probe_default_primary_name_with_requirement(&Client::new(), &base, &corpus, true)
                .await
                .unwrap();
        server.await.unwrap();
        assert_eq!(report.requests_sent, 2);
        assert_eq!(report.outcomes.get("http_200"), Some(&1));
        assert_eq!(report.outcomes.get("http_409:stale"), Some(&1));
        assert!(report.failures.is_empty());
    }

    #[test]
    fn default_source_probe_rejects_an_incomplete_error_envelope() {
        let body = json!({"error": {"code": "stale", "message": "RPC is not configured"}});

        assert!(
            default_primary_name_failure(
                "ens",
                "0x0000000000000000000000000000000000000001",
                "60",
                StatusCode::CONFLICT,
                Some(&body),
            )
            .is_some()
        );
    }

    #[test]
    fn default_source_requests_are_bounded_and_omit_source() {
        let base = super::super::workload::normalized_base_url("http://127.0.0.1:3000").unwrap();
        let corpus = Corpus {
            names: Vec::new(),
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: (0..12)
                .map(|index| (format!("0x{index:040x}"), "60".to_owned(), "ens".to_owned()))
                .collect(),
            resolvers: Vec::new(),
            namespaces: vec!["ens".to_owned()],
            names_by_namespace: Default::default(),
            parents_by_namespace: Default::default(),
            resolver_manifest_coverage: Vec::new(),
        };

        let requests = default_primary_name_requests(&base, &corpus).unwrap();
        assert_eq!(requests.len(), DEFAULT_PRIMARY_NAME_PROBE_LIMIT);
        assert!(requests.iter().all(|(_, _, _, request)| {
            !request.url.query_pairs().any(|(key, _)| key == "source")
        }));
    }

    #[tokio::test]
    async fn production_default_source_probe_requires_an_ens_coin_60_tuple() {
        use crate::budgets::{BudgetProfile, BudgetsFile};
        use std::path::Path;

        let base = super::super::workload::normalized_base_url("http://127.0.0.1:1").unwrap();
        let corpus = Corpus {
            names: Vec::new(),
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: Vec::new(),
            resolvers: Vec::new(),
            namespaces: vec!["ens".to_owned()],
            names_by_namespace: Default::default(),
            parents_by_namespace: Default::default(),
            resolver_manifest_coverage: Vec::new(),
        };
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
        let budgets = BudgetsFile::load(&path).unwrap();

        let report = probe_default_primary_name_with_requirement(
            &Client::new(),
            &base,
            &corpus,
            budgets
                .profile(BudgetProfile::Production)
                .api_require_populated_probes,
        )
        .await
        .unwrap();

        assert!(
            budgets
                .profile(BudgetProfile::Production)
                .api_require_populated_probes
        );
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.contains("ens/coin-60")),
            "production must red when the default-source probe is vacuous"
        );
        assert_eq!(report.requests_sent, 0);
        assert!(report.outcomes.is_empty());
    }

    #[tokio::test]
    async fn smoke_allows_an_empty_default_source_probe_and_records_zero() {
        let base = super::super::workload::normalized_base_url("http://127.0.0.1:1").unwrap();
        let corpus = Corpus {
            names: Vec::new(),
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: Vec::new(),
            resolvers: Vec::new(),
            namespaces: Vec::new(),
            names_by_namespace: Default::default(),
            parents_by_namespace: Default::default(),
            resolver_manifest_coverage: Vec::new(),
        };

        let report =
            probe_default_primary_name_with_requirement(&Client::new(), &base, &corpus, false)
                .await
                .unwrap();
        assert_eq!(report.requests_sent, 0);
        assert!(report.outcomes.is_empty());
        assert!(report.failures.is_empty());
    }

    #[test]
    fn production_search_probe_requires_a_populated_bare_variant() {
        use super::super::SeedProbe;
        use crate::budgets::{BudgetProfile, BudgetsFile};
        use std::path::Path;

        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
        let budgets = BudgetsFile::load(&path).unwrap();
        let probe = SeedProbe {
            populated: true,
            bare_search_populated: false,
            cursor_variants: 1,
        };

        assert!(
            require_seed_probe(budgets.profile(BudgetProfile::Production), "search", probe)
                .is_err()
        );
    }
}

#[cfg(test)]
#[path = "probes/tests/mutation.rs"]
mod mutation_tests;
