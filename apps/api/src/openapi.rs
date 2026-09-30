//! The checked-in contract artifact is generated offline; serving it needs no database or RPC.

use std::sync::OnceLock;

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};

pub(crate) const TEMPLATE: &str = include_str!("../openapi.json");
const CACHE_CONTROL: &str = "public, max-age=300";

struct Document {
    body: String,
    etag: HeaderValue,
}

fn document() -> &'static Document {
    static DOCUMENT: OnceLock<Document> = OnceLock::new();
    DOCUMENT.get_or_init(|| {
        // JSON-escape build metadata too: an unusual build label must not corrupt the document.
        let mut value: serde_json::Value =
            serde_json::from_str(TEMPLATE).expect("checked-in OpenAPI JSON must parse");
        value["info"]["version"] = crate::SOFTWARE_VERSION.into();
        value["info"]["x-build-sha"] = crate::BUILD_SHA.into();
        let body =
            serde_json::to_string_pretty(&value).expect("OpenAPI JSON must serialize") + "\n";
        let etag = HeaderValue::from_str(&format!(
            "W/\"{}\"",
            alloy_primitives::keccak256(body.as_bytes())
        ))
        .expect("hex digest is a valid ETag");
        Document { body, etag }
    })
}

pub(crate) async fn get_openapi(headers: HeaderMap) -> Response {
    let document = document();
    let tag = document.etag.to_str().expect("ETag is ASCII");
    // If-None-Match uses weak comparison for GET and HEAD, including lists and '*'.
    let not_modified = headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.split(',').map(str::trim).any(|candidate| {
                candidate == "*"
                    || candidate.strip_prefix("W/").unwrap_or(candidate)
                        == tag.strip_prefix("W/").unwrap_or(tag)
            })
        })
    });
    let mut response = if not_modified {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response
    } else {
        let mut response = Response::new(Body::from(document.body.as_str()));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        response
    };
    response
        .headers_mut()
        .insert(header::ETAG, document.etag.clone());
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL),
    );
    response
}

#[cfg(test)]
#[path = "tests/openapi_http.rs"]
mod tests;
