use anyhow::{Context, Result};
use reqwest::{Client, Method};
use serde_json::Value;

use super::{send, workload::RequestSpec};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SeedProbe {
    pub(super) populated: bool,
    pub(super) bare_search_populated: bool,
    pub(super) lookup_name_populated: bool,
    pub(super) lookup_address_populated: bool,
    pub(super) permission_audit_required: bool,
    pub(super) permission_audit_populated: bool,
    pub(super) cursor_variants: usize,
    pub(super) bare_search_cursor_variants: usize,
}

#[derive(Debug)]
pub(super) struct PrimedRequests {
    pub(super) requests: Vec<RequestSpec>,
    pub(super) probe: SeedProbe,
    pub(super) base_variants: usize,
    pub(super) unique_cursor_variants: usize,
    pub(super) weighted_cursor_requests: usize,
}

pub(super) async fn prime_cursor_variants(
    client: &Client,
    endpoint: &str,
    requests: Vec<RequestSpec>,
    limit: usize,
    cursor_weight_percent: usize,
) -> Result<PrimedRequests> {
    let base_variants = requests.len();
    let seeds = requests.clone();
    let mut cursors = Vec::new();
    let mut probe = SeedProbe {
        permission_audit_required: requests
            .iter()
            .any(|request| request.required_permission_audit_evidence),
        ..SeedProbe::default()
    };
    for (index, seed) in seeds.into_iter().enumerate() {
        let prefix_complete = index >= limit;
        let cursor_requirement_met = !endpoint_requires_cursor(endpoint)
            || (!cursors.is_empty()
                && (endpoint != "search" || probe.bare_search_cursor_variants > 0));
        let population_requirement_met = probe.populated
            && (endpoint != "search" || probe.bare_search_populated)
            && (endpoint != "lookup"
                || (probe.lookup_name_populated && probe.lookup_address_populated))
            && (endpoint != "permissions"
                || !probe.permission_audit_required
                || probe.permission_audit_populated);
        if prefix_complete && population_requirement_met && cursor_requirement_met {
            break;
        }
        let response = send(client, &seed).await?;
        if !response.status().is_success() {
            continue;
        }
        let body: Value = response
            .json()
            .await
            .context("failed to parse cursor-seed response")?;
        update_seed_evidence(&mut probe, endpoint, &seed, &body);
        let variants = cursor_variants(&seed, &body);
        if endpoint == "search" && is_bare_search(&seed) && response_is_populated(endpoint, &body) {
            probe.bare_search_cursor_variants += usize::from(!variants.is_empty());
        }
        for variant in variants {
            if !cursors
                .iter()
                .any(|existing| same_request(existing, &variant))
            {
                cursors.push(variant);
            }
        }
    }
    probe.cursor_variants = cursors.len();
    let unique_cursor_variants = cursors.len();
    let (requests, weighted_cursor_requests) =
        weight_cursor_requests(requests, &cursors, cursor_weight_percent);
    Ok(PrimedRequests {
        requests,
        probe,
        base_variants,
        unique_cursor_variants,
        weighted_cursor_requests,
    })
}

fn update_seed_evidence(probe: &mut SeedProbe, endpoint: &str, seed: &RequestSpec, body: &Value) {
    let response_populated = if endpoint == "records" {
        requested_records_are_populated(seed, body)
    } else if endpoint == "permissions" && seed.required_permission_audit_evidence {
        retained_registration_response_matches(seed, body)
    } else {
        response_is_populated(endpoint, body)
    };
    probe.populated |= response_populated;
    if endpoint == "search" && is_bare_search(seed) {
        probe.bare_search_populated |= response_populated;
    }
    if endpoint == "lookup" {
        probe.lookup_name_populated |= lookup_kind_populated(body, "name");
        probe.lookup_address_populated |= lookup_kind_populated(body, "address");
        probe.populated = probe.lookup_name_populated || probe.lookup_address_populated;
    }
    if endpoint == "permissions" && seed.required_permission_audit_evidence {
        probe.permission_audit_populated |= response_populated;
    }
}

fn retained_registration_response_matches(request: &RequestSpec, body: &Value) -> bool {
    let Some(requested_id) = request
        .url
        .query_pairs()
        .find(|(key, _)| key == "registration_id")
        .map(|(_, value)| value.into_owned())
    else {
        return false;
    };
    body.get("data")
        .and_then(Value::as_array)
        .is_some_and(|rows| {
            !rows.is_empty()
                && rows.iter().all(|row| {
                    row.get("registration_id").and_then(Value::as_str)
                        == Some(requested_id.as_str())
                })
        })
}

fn is_bare_search(request: &RequestSpec) -> bool {
    !request
        .url
        .query_pairs()
        .any(|(key, _)| key == "namespace" || key == "match" || key == "page_size")
}

pub(super) fn endpoint_requires_cursor(endpoint: &str) -> bool {
    matches!(
        endpoint,
        "lookup"
            | "subnames"
            | "name_history"
            | "permissions"
            | "address_names"
            | "address_history"
            | "search"
            | "events"
            | "resolver"
    )
}

pub(super) fn response_is_populated(endpoint: &str, body: &Value) -> bool {
    match endpoint {
        "lookup" => lookup_kind_populated(body, "address"),
        "subnames" | "name_history" | "permissions" | "address_names" | "address_history"
        | "search" | "events" => body
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|rows| !rows.is_empty()),
        "primary_name" => body
            .pointer("/data/answers")
            .and_then(Value::as_array)
            .is_some_and(|answers| {
                answers.iter().any(|answer| {
                    answer.get("source").and_then(Value::as_str) == Some("indexed")
                        && answer.get("status").and_then(Value::as_str) == Some("ok")
                })
            }),
        "resolver" => body
            .pointer("/data/address")
            .and_then(Value::as_str)
            .is_some(),
        "records" => false,
        "status" => status_is_ready(body),
        "name" => body.pointer("/data/status").and_then(Value::as_str) == Some("ok"),
        "namespace" => body.get("data").is_some(),
        _ => false,
    }
}

pub(super) fn requested_records_are_populated(request: &RequestSpec, body: &Value) -> bool {
    let Some(requested_keys) = request
        .url
        .query_pairs()
        .find(|(key, _)| key == "keys")
        .map(|(_, value)| value.into_owned())
    else {
        return false;
    };
    let Some(records) = body.pointer("/data/records").and_then(Value::as_object) else {
        return false;
    };
    requested_keys.split(',').any(|key| {
        records
            .get(key)
            .is_some_and(|record| record.get("status").and_then(Value::as_str) == Some("ok"))
    })
}

pub(super) fn aggregate_records_are_populated(body: &Value) -> bool {
    body.get("data")
        .and_then(Value::as_object)
        .is_some_and(|data| {
            data.get("addresses")
                .and_then(Value::as_object)
                .is_some_and(|rows| !rows.is_empty())
                || data
                    .get("text_records")
                    .and_then(Value::as_object)
                    .is_some_and(|rows| !rows.is_empty())
                || data
                    .get("content_hash")
                    .is_some_and(|value| !value.is_null())
        })
}

fn same_request(left: &RequestSpec, right: &RequestSpec) -> bool {
    left.method == right.method
        && left.url == right.url
        && left.body == right.body
        && left.known_good_evidence == right.known_good_evidence
        && left.required_permission_audit_evidence == right.required_permission_audit_evidence
}

fn lookup_kind_populated(body: &Value, kind: &str) -> bool {
    body.get("data")
        .and_then(Value::as_array)
        .is_some_and(|results| {
            results.iter().any(|result| {
                if result.get("kind").and_then(Value::as_str) != Some(kind)
                    || result.get("status").and_then(Value::as_str) != Some("ok")
                {
                    return false;
                }
                match kind {
                    "name" => {
                        result.pointer("/record/status").and_then(Value::as_str) == Some("ok")
                    }
                    "address" => result
                        .get("records")
                        .and_then(Value::as_array)
                        .is_some_and(|records| !records.is_empty()),
                    _ => false,
                }
            })
        })
}

fn status_is_ready(body: &Value) -> bool {
    body.pointer("/data/status").and_then(Value::as_str) == Some("ready")
        && body
            .pointer("/data/chains")
            .and_then(Value::as_object)
            .is_some_and(|chains| {
                !chains.is_empty()
                    && chains
                        .values()
                        .all(|chain| chain.get("status").and_then(Value::as_str) == Some("ready"))
            })
}

fn weight_cursor_requests(
    base: Vec<RequestSpec>,
    cursors: &[RequestSpec],
    cursor_weight_percent: usize,
) -> (Vec<RequestSpec>, usize) {
    if base.is_empty() || cursors.is_empty() || cursor_weight_percent == 0 {
        return (base, 0);
    }
    let denominator = 100usize.saturating_sub(cursor_weight_percent).max(1);
    let cursor_copies = base
        .len()
        .saturating_mul(cursor_weight_percent)
        .div_ceil(denominator)
        .max(1);
    let mut weighted = Vec::with_capacity(base.len().saturating_add(cursor_copies));
    let base_len = base.len();
    let mut inserted = 0usize;
    for (index, request) in base.into_iter().enumerate() {
        weighted.push(request);
        let expected = (index + 1).saturating_mul(cursor_copies).div_ceil(base_len);
        while inserted < expected {
            weighted.push(cursors[inserted % cursors.len()].clone());
            inserted += 1;
        }
    }
    (weighted, inserted)
}

pub(super) fn cursor_variants(seed: &RequestSpec, body: &Value) -> Vec<RequestSpec> {
    if seed.method == Method::POST && seed.url.path().ends_with("/v2/lookup") {
        let Some(results) = body.get("data").and_then(Value::as_array) else {
            return Vec::new();
        };
        return results
            .iter()
            .enumerate()
            .find_map(|(index, result)| {
                let cursor = result.pointer("/page/next_cursor")?.as_str()?;
                let mut resumed = seed.clone();
                resumed
                    .body
                    .as_mut()?
                    .pointer_mut(&format!("/inputs/{index}"))?
                    .as_object_mut()?
                    .insert("cursor".to_owned(), Value::String(cursor.to_owned()));
                Some(vec![resumed])
            })
            .unwrap_or_default();
    }

    let cursor = body
        .pointer("/page/next_cursor")
        .or_else(|| body.pointer("/data/bound_names/page/next_cursor"))
        .and_then(Value::as_str);
    let Some(cursor) = cursor else {
        return Vec::new();
    };
    let mut resumed = seed.clone();
    resumed.url.query_pairs_mut().append_pair("cursor", cursor);
    vec![resumed]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_load::workload::{get, normalized_base_url};
    use serde_json::json;

    #[test]
    fn cursor_weight_is_even_and_reaches_the_configured_share() {
        let base_url = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let base = (0..90)
            .map(|index| {
                get(
                    &base_url,
                    &["v2", "events"],
                    &[("seed", &index.to_string())],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let cursor = get(&base_url, &["v2", "events"], &[("cursor", "next")]).unwrap();
        let (weighted, cursor_count) = weight_cursor_requests(base, &[cursor], 10);

        assert_eq!(cursor_count, 10);
        assert_eq!(weighted.len(), 100);
        assert!(weighted.chunks(10).all(|chunk| {
            chunk
                .iter()
                .any(|request| request.url.query_pairs().any(|(key, _)| key == "cursor"))
        }));
    }

    #[test]
    fn retained_registration_seed_requires_its_own_populated_response() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let mut request = get(
            &base,
            &["v2", "permissions"],
            &[("registration_id", "00000000-0000-0000-0000-000000000043")],
        )
        .unwrap();
        request.required_permission_audit_evidence = true;
        let mut probe = SeedProbe {
            populated: true,
            permission_audit_required: true,
            ..SeedProbe::default()
        };

        update_seed_evidence(&mut probe, "permissions", &request, &json!({"data": []}));
        assert!(!probe.permission_audit_populated);

        update_seed_evidence(
            &mut probe,
            "permissions",
            &request,
            &json!({"data": [{"registration_id": "00000000-0000-0000-0000-000000000041"}]}),
        );
        assert!(
            !probe.permission_audit_populated,
            "a populated current-registration response must not satisfy retained-registration evidence"
        );

        update_seed_evidence(
            &mut probe,
            "permissions",
            &request,
            &json!({"data": [
                {"registration_id": "00000000-0000-0000-0000-000000000043"},
                {"registration_id": "00000000-0000-0000-0000-000000000041"}
            ]}),
        );
        assert!(
            !probe.permission_audit_populated,
            "a response that ignores the registration filter must not satisfy audit evidence"
        );

        update_seed_evidence(
            &mut probe,
            "permissions",
            &request,
            &json!({"data": [{"registration_id": "00000000-0000-0000-0000-000000000043"}]}),
        );
        assert!(probe.permission_audit_populated);
    }
}
