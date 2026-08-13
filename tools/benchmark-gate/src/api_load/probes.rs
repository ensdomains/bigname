use anyhow::{Result, ensure};
use reqwest::{Client, StatusCode, Url};
use serde_json::Value;

use super::{
    SeedProbe, corpus::Corpus, endpoint_requires_cursor, send, workload::RequestSpec, workload::get,
};
use crate::budgets::GateBudgets;

const DEFAULT_PRIMARY_NAME_PROBE_LIMIT: usize = 10;

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
) -> Result<Vec<String>> {
    let probes = default_primary_name_requests(base, corpus)?;
    let mut failures = Vec::new();

    for (namespace, address, coin_type, request) in &probes {
        match send(client, request).await {
            Ok(response) => {
                let status = response.status();
                let body = response
                    .bytes()
                    .await
                    .ok()
                    .and_then(|body| serde_json::from_slice::<Value>(&body).ok());
                if let Some(failure) = default_primary_name_failure(
                    namespace,
                    address,
                    coin_type,
                    status,
                    body.as_ref(),
                ) {
                    failures.push(failure);
                }
            }
            Err(error) => failures.push(format!(
                "untimed primary-name default-source probe for namespace {namespace:?}, address {address:?}, coin type {coin_type:?} failed to complete: {error:#}; verify the drained API's live RPC configuration and rerun the gate"
            )),
        }
    }

    Ok(failures)
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
    let well_formed = body.is_some_and(|body| {
        if status.is_success() {
            body.pointer("/data/answers")
                .and_then(Value::as_array)
                .is_some_and(|answers| {
                    answers.len() == 2
                        && answers[0].get("source").and_then(Value::as_str) == Some("indexed")
                        && answers[1].get("source").and_then(Value::as_str) == Some("verified")
                        && answers.iter().all(|answer| {
                            answer
                                .get("status")
                                .and_then(Value::as_str)
                                .is_some_and(valid_primary_name_status)
                        })
                })
        } else {
            body.pointer("/error/code").is_some_and(Value::is_string)
                && body.pointer("/error/message").is_some_and(Value::is_string)
                && body.pointer("/error/details").is_some_and(Value::is_object)
        }
    });
    if !status.is_server_error() && well_formed {
        return None;
    }

    let reason = if status.is_server_error() {
        format!("returned HTTP {}", status.as_u16())
    } else {
        format!(
            "returned HTTP {} without a well-formed response envelope",
            status.as_u16()
        )
    };
    Some(format!(
        "untimed primary-name default-source probe for namespace {namespace:?}, address {address:?}, coin type {coin_type:?} {reason}; verify the drained API's live RPC configuration and rerun the gate"
    ))
}

fn valid_primary_name_status(status: &str) -> bool {
    matches!(
        status,
        "ok" | "not_found" | "invalid_name" | "mismatch" | "unsupported" | "stale" | "failed"
    )
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
    fn default_source_probe_rejects_an_indexed_only_success() {
        let body = json!({
            "data": {
                "answers": [{"source": "indexed", "status": "ok", "name": "example.eth"}]
            }
        });

        let failure = default_primary_name_failure(
            "ens",
            "0x0000000000000000000000000000000000000001",
            "60",
            StatusCode::OK,
            Some(&body),
        )
        .expect("omitted source must return indexed and verified answers");

        assert!(failure.contains("well-formed response envelope"));
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
            "the adjudicated probe contract accepts a well-formed non-5xx error"
        );
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
        };

        let requests = default_primary_name_requests(&base, &corpus).unwrap();
        assert_eq!(requests.len(), DEFAULT_PRIMARY_NAME_PROBE_LIMIT);
        assert!(requests.iter().all(|(_, _, _, request)| {
            !request.url.query_pairs().any(|(key, _)| key == "source")
        }));
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
