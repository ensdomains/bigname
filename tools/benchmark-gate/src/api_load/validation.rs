use serde_json::Value;

use super::workload::RequestSpec;

pub(super) fn endpoint_is_sampled(endpoint: &str) -> bool {
    matches!(endpoint, "name" | "primary_name" | "lookup")
}

pub(super) fn validate_timed_response(
    endpoint: &str,
    request: &RequestSpec,
    body: &[u8],
) -> Option<String> {
    if !endpoint_is_sampled(endpoint) {
        return None;
    }
    let body: Value = match serde_json::from_slice(body) {
        Ok(body) => body,
        Err(error) => {
            return Some(format!(
                "sampled {endpoint} response was not valid JSON: {error}"
            ));
        }
    };
    match endpoint {
        "name" => (body.pointer("/data/status").and_then(Value::as_str) != Some("ok"))
            .then(|| "sampled supported name response did not return data.status=ok".to_owned()),
        "primary_name" => (!indexed_primary_name_is_ok(&body)).then(|| {
            "sampled successful primary-name tuple did not return an indexed ok answer".to_owned()
        }),
        "lookup" => lookup_failure(request, &body),
        _ => None,
    }
}

fn indexed_primary_name_is_ok(body: &Value) -> bool {
    body.pointer("/data/answers")
        .and_then(Value::as_array)
        .is_some_and(|answers| {
            answers.iter().any(|answer| {
                answer.get("source").and_then(Value::as_str) == Some("indexed")
                    && answer.get("status").and_then(Value::as_str) == Some("ok")
            })
        })
}

fn lookup_failure(request: &RequestSpec, body: &Value) -> Option<String> {
    let Some(inputs) = request
        .body
        .as_ref()
        .and_then(|body| body.get("inputs"))
        .and_then(Value::as_array)
    else {
        return Some("sampled lookup request did not contain an inputs array".to_owned());
    };
    let Some(results) = body.get("data").and_then(Value::as_array) else {
        return Some("sampled lookup response did not contain a data array".to_owned());
    };
    for input in inputs {
        let id = input
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let expected_kind = if input.get("name").is_some() {
            "name"
        } else if input.get("address").is_some() {
            "address"
        } else {
            return Some(format!("sampled lookup input {id:?} has no supported kind"));
        };
        let Some(result) = results
            .iter()
            .find(|result| result.pointer("/input/id").and_then(Value::as_str) == Some(id))
        else {
            return Some(format!("sampled lookup response omitted input {id:?}"));
        };
        if result.get("kind").and_then(Value::as_str) != Some(expected_kind)
            || result.get("status").and_then(Value::as_str) != Some("ok")
        {
            return Some(format!(
                "sampled lookup input {id:?} did not return {expected_kind}-kind status ok"
            ));
        }
        let populated = match expected_kind {
            "name" => result.pointer("/record/status").and_then(Value::as_str) == Some("ok"),
            "address" => result
                .get("records")
                .and_then(Value::as_array)
                .is_some_and(|records| !records.is_empty()),
            _ => false,
        };
        if !populated {
            return Some(format!(
                "sampled lookup input {id:?} returned no indexed {expected_kind} evidence"
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_load::workload::{normalized_base_url, post};
    use serde_json::json;

    fn lookup_request() -> RequestSpec {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        post(
            &base,
            &["v2", "lookup"],
            json!({"inputs": [
                {"id": "forward", "name": "known.eth"},
                {"id": "reverse", "address": "0x0000000000000000000000000000000000000001"}
            ]}),
        )
        .unwrap()
    }

    #[test]
    fn sampled_in_band_failures_are_rejected_after_timing() {
        let request = lookup_request();
        let lookup = json!({"data": [
            {"input": {"id": "forward"}, "kind": "name", "status": "not_found"},
            {"input": {"id": "reverse"}, "kind": "address", "status": "unsupported", "records": []}
        ]});
        assert!(
            validate_timed_response("lookup", &request, lookup.to_string().as_bytes())
                .unwrap()
                .contains("forward")
        );

        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let name = super::super::workload::get(&base, &["v2", "names", "known.eth"], &[]).unwrap();
        assert!(
            validate_timed_response("name", &name, br#"{"data":{"status":"unsupported"}}"#,)
                .is_some()
        );
        assert!(
            validate_timed_response(
                "primary_name",
                &name,
                br#"{"data":{"answers":[{"source":"indexed","status":"not_found"}]}}"#,
            )
            .is_some()
        );
    }

    #[test]
    fn sampled_known_good_inputs_require_populated_ok_evidence() {
        let request = lookup_request();
        let lookup = json!({"data": [
            {"input": {"id": "forward"}, "kind": "name", "status": "ok", "record": {"status": "ok"}},
            {"input": {"id": "reverse"}, "kind": "address", "status": "ok", "records": [{"name": "known.eth"}]}
        ]});
        assert!(
            validate_timed_response("lookup", &request, lookup.to_string().as_bytes()).is_none()
        );
    }
}
