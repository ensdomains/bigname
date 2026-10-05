//! Validate real responses at the common test router boundary, including optional variants
//! exercised by the existing feature fixtures. No schema work enters the production router.

use std::{collections::BTreeMap, sync::OnceLock};

use axum::{
    body::{Body, to_bytes},
    extract::Request,
    middleware::Next,
    response::Response,
};
use jsonschema::{Draft, Validator};
use serde_json::{Value, json};

pub(crate) fn document() -> &'static Value {
    static DOCUMENT: OnceLock<Value> = OnceLock::new();
    DOCUMENT.get_or_init(|| serde_json::from_str(crate::openapi::TEMPLATE).unwrap())
}

fn validator(schema: &Value) -> Result<Validator, jsonschema::error::ValidationError<'static>> {
    let mut root = schema.clone();
    root["components"] = document()["components"].clone();
    jsonschema::options()
        .with_draft(Draft::Draft202012)
        .offline()
        .build(&root)
}

fn validators() -> &'static BTreeMap<String, Validator> {
    static VALIDATORS: OnceLock<BTreeMap<String, Validator>> = OnceLock::new();
    VALIDATORS.get_or_init(|| {
        let mut validators = BTreeMap::new();
        for (path, methods) in document()["paths"].as_object().unwrap() {
            for (method, operation) in methods.as_object().unwrap() {
                for (status, response) in operation["responses"].as_object().unwrap() {
                    if let Some(schema) = response.pointer("/content/application~1json/schema") {
                        let key = format!("{} {path} {status}", method.to_uppercase());
                        let compiled =
                            validator(schema).unwrap_or_else(|error| panic!("{key}: {error}"));
                        validators.insert(key, compiled);
                    }
                }
            }
        }
        validators
    })
}

pub(crate) fn operation_path(method: &str, request_path: &str) -> Option<&'static str> {
    let request_parts = request_path.split('/').collect::<Vec<_>>();
    document()["paths"]
        .as_object()
        .unwrap()
        .iter()
        .find_map(|(path, methods)| {
            let parts = path.split('/').collect::<Vec<_>>();
            (methods.get(method.to_lowercase()).is_some()
                && parts.len() == request_parts.len()
                && parts.iter().zip(&request_parts).all(|(pattern, actual)| {
                    (pattern.starts_with('{') && !actual.is_empty()) || pattern == actual
                }))
            .then_some(path.as_str())
        })
}

pub(crate) fn assert_payload(method: &str, path: &str, status: u16, payload: &Value) {
    let key = format!("{method} {path} {status}");
    let validator = validators()
        .get(&key)
        .unwrap_or_else(|| panic!("undocumented JSON response: {key}"));
    let errors = validator
        .iter_errors(payload)
        .take(12)
        .map(|error| {
            format!(
                "instance {} schema {}: {error}",
                error.instance_path(),
                error.schema_path()
            )
        })
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "{key} response schema violation:\n{}\npayload: {payload}",
        errors.join("\n")
    );
}

pub(crate) async fn validate_response(request: Request, next: Next) -> Response {
    let method = request.method().as_str().to_owned();
    let path = operation_path(&method, request.uri().path());
    let response = next.run(request).await;
    let Some(path) = path else { return response };
    let status = response.status().as_u16();
    let operation = &document()["paths"][path][method.to_lowercase()];
    let declaration = &operation["responses"][status.to_string()];
    assert!(
        !declaration.is_null(),
        "undocumented response: {method} {path} {status}"
    );
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, usize::MAX)
        .await
        .expect("response body must read");
    if declaration.get("content").is_some() {
        let payload = serde_json::from_slice(&body)
            .unwrap_or_else(|error| panic!("{method} {path} {status} is not JSON: {error}"));
        assert_payload(&method, path, status, &payload);
    } else {
        assert!(body.is_empty(), "{method} {path} {status} must be bodyless");
    }
    Response::from_parts(parts, Body::from(body))
}

fn validate_inline_schemas(value: &Value, path: &str) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let path = format!("{path}/{key}");
                if key == "schema" {
                    jsonschema::draft202012::meta::validate(value)
                        .unwrap_or_else(|error| panic!("{path}: {error}"));
                    validator(value).unwrap_or_else(|error| panic!("{path}: {error}"));
                } else {
                    validate_inline_schemas(value, &path);
                }
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_inline_schemas(value, &format!("{path}/{index}"));
            }
        }
        _ => {}
    }
}

#[test]
fn openapi_document_and_every_payload_schema_validate_offline() {
    let standard: Value = serde_json::from_str(include_str!(
        "../../../../scripts/openapi/schema/openapi-3.1-2025-09-15.json"
    ))
    .unwrap();
    let standard = jsonschema::options()
        .with_draft(Draft::Draft202012)
        .offline()
        .build(&standard)
        .unwrap();
    let errors = standard
        .iter_errors(document())
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    assert!(errors.is_empty(), "OpenAPI 3.1 document: {errors:#?}");
    assert_eq!(
        document()["jsonSchemaDialect"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    for (name, schema) in document()["components"]["schemas"].as_object().unwrap() {
        jsonschema::draft202012::meta::validate(schema)
            .unwrap_or_else(|error| panic!("component {name}: {error}"));
        validator(&json!({"$ref": format!("#/components/schemas/{name}")}))
            .unwrap_or_else(|error| panic!("component {name}: {error}"));
    }
    validate_inline_schemas(document(), "#");
    assert!(!validators().is_empty());
    let mut malformed_document = document().clone();
    malformed_document["openapi"] = json!("2.0");
    assert!(!standard.is_valid(&malformed_document));
    assert!(jsonschema::draft202012::meta::validate(&json!({"type": "no-such-type"})).is_err());
    assert!(validator(&json!({"$ref":"#/components/schemas/NoSuchSchema"})).is_err());
    assert!(
        jsonschema::options()
            .offline()
            .build(&json!({"$ref":"https://example.invalid/external-schema"}))
            .is_err()
    );
    let error = validator(&json!({"$ref":"#/components/schemas/ErrorEnvelope"})).unwrap();
    assert!(!error.is_valid(&json!({"error":{"code":"invalid_input","message":"bad"}})));
    assert!(!error.is_valid(&json!({"error":{"code":"invented","message":"bad","details":{}}})));
    assert!(
        error.is_valid(&json!({"error":{"code":"invalid_input","message":"bad","details":{}}}))
    );
}

#[test]
fn openapi_lookup_request_closes_both_input_alternatives() {
    let schema = &document()["paths"]["/v1/lookup"]["post"]["requestBody"]["content"]["application/json"]
        ["schema"];
    let validator = validator(schema).unwrap();
    for valid in [
        json!({"inputs":[]}),
        json!({"profile":"detail","inputs":[{"id":"name","name":"alice.eth"}]}),
        json!({"profile":"feed","inputs":[{"address":"0x0000000000000000000000000000000000000001","coin_type":60}]}),
    ] {
        assert!(validator.is_valid(&valid), "{valid}");
    }
    for invalid in [
        json!({}),
        json!({"inputs":[],"unknown":true}),
        json!({"inputs":[{"name":"alice.eth","address":"0x1"}]}),
        json!({"inputs":[{"name":"alice.eth","unknown":true}]}),
        json!({"include":["inventory"],"inputs":[]}),
        json!({"profile":"detail","include":"inventory","inputs":[]}),
    ] {
        assert!(!validator.is_valid(&invalid), "{invalid}");
    }
}

#[test]
fn openapi_request_page_sizes_enforce_the_documented_inclusive_bounds() {
    let mut query_parameters = 0;
    for (path, methods) in document()["paths"].as_object().unwrap() {
        for (method, operation) in methods.as_object().unwrap() {
            for parameter in operation["parameters"].as_array().unwrap() {
                if parameter["name"] != "page_size" {
                    continue;
                }
                query_parameters += 1;
                let schema = &parameter["schema"];
                assert_eq!(schema["minimum"], 1, "{method} {path} page_size");
                assert_eq!(schema["maximum"], 200, "{method} {path} page_size");
                assert_eq!(schema["default"], 50, "{method} {path} page_size");
                let validator = validator(schema).unwrap();
                for (value, valid) in [(0, false), (1, true), (200, true), (201, false)] {
                    assert_eq!(
                        validator.is_valid(&json!(value)),
                        valid,
                        "{method} {path} page_size={value}"
                    );
                }
            }
        }
    }
    assert_eq!(query_parameters, 13);

    let schema = &document()["paths"]["/v1/lookup"]["post"]["requestBody"]["content"]["application/json"]
        ["schema"];
    let validator = validator(schema).unwrap();
    for (value, valid) in [(0, false), (1, true), (200, true), (201, false)] {
        let body = json!({"inputs":[{"address":"0x0000000000000000000000000000000000000abc","relation":"owner","page_size":value}]});
        assert_eq!(
            validator.is_valid(&body),
            valid,
            "POST /v1/lookup inputs[0].page_size={value}"
        );
    }
}

#[test]
fn openapi_record_keys_enforce_the_fixed_array_limit() {
    let parameters = document()["paths"]["/v1/names/{name}/records"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let keys = parameters.iter().find(|p| p["name"] == "keys").unwrap();
    assert_eq!(keys["style"], "form");
    assert_eq!(keys["explode"], false);
    assert_eq!(keys["required"], false);
    assert_eq!(keys["schema"]["minItems"], 0);
    assert_eq!(keys["schema"]["maxItems"], 200);
    let validator = validator(&keys["schema"]).unwrap();
    for (count, valid) in [(0, true), (1, true), (200, true), (201, false)] {
        let values = (0..count)
            .map(|i| format!("text:key{i}"))
            .collect::<Vec<_>>();
        assert_eq!(
            validator.is_valid(&json!(values)),
            valid,
            "GET /v1/names/{{name}}/records keys count={count}"
        );
    }
    // This limit is deployment-configurable, so its default is not a universal bound.
    assert!(
        document()["components"]["schemas"]["LookupRequest"]["properties"]["inputs"]
            .get("maxItems")
            .is_none()
    );
}

#[test]
fn openapi_expiry_windows_repeat_with_bounded_membership() {
    let parameters = document()["paths"]["/v1/names"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let windows = parameters
        .iter()
        .find(|p| p["name"] == "expires_window")
        .unwrap();
    assert_eq!(windows["style"], "form");
    assert_eq!(windows["explode"], true);
    assert_eq!(windows["required"], false);
    let bounds = validator(&windows["schema"]).unwrap();
    for (count, valid) in [(0, false), (1, true), (32, true), (33, false)] {
        assert_eq!(bounds.is_valid(&json!(vec!["1..2"; count])), valid);
    }
    let schema = &document()["components"]["schemas"]["SearchName"];
    assert!(
        !schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("expires_window_index"))
    );
    let index = validator(&schema["properties"]["expires_window_index"]).unwrap();
    for (value, valid) in [(-1, false), (0, true), (31, true), (32, false)] {
        assert_eq!(index.is_valid(&json!(value)), valid);
    }
}

#[test]
fn openapi_error_responses_constrain_codes_for_the_operation_and_status() {
    for (method, path, status, allowed) in [
        ("post", "/v1/lookup", "400", &["invalid_input"][..]),
        ("post", "/v1/lookup", "409", &["conflict", "stale"][..]),
        ("get", "/v1/search", "409", &["conflict", "stale"][..]),
        ("get", "/v1/names", "409", &["stale"][..]),
        ("get", "/v1/names/{name}", "500", &["internal_error"][..]),
    ] {
        let schema = &document()["paths"][path][method]["responses"][status]["content"]["application/json"]
            ["schema"];
        let validator = validator(schema).unwrap();
        for code in document()["components"]["schemas"]["ErrorCode"]["enum"]
            .as_array()
            .unwrap()
        {
            let code = code.as_str().unwrap();
            let payload = json!({"error":{"code":code,"message":"failed","details":{}}});
            assert_eq!(
                validator.is_valid(&payload),
                allowed.contains(&code),
                "{method} {path} {status} error.code={code}"
            );
        }
        let code = allowed[0];
        for invalid in [
            json!({"error":{"code":code,"message":"failed","details":{},"extra":true}}),
            json!({"error":{"code":code,"message":"failed","details":{}},"extra":true}),
            json!({"error":{"message":"failed","details":{}}}),
        ] {
            assert!(
                !validator.is_valid(&invalid),
                "{method} {path} {status}: {invalid}"
            );
        }
    }
}
