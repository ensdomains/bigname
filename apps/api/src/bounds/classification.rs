use std::borrow::Cow;

use axum::http::{Method, Uri};

pub(super) fn is_verified_execution_request(method: &Method, uri: &Uri) -> bool {
    if !matches!(method, &Method::GET | &Method::HEAD) {
        return false;
    }
    let segments = uri.path().trim_matches('/').split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        ["v1", "primary-names", _] => true,
        ["v1", "profiles", "names", _] => {
            query_absent_or_matches(uri, "mode", &["verified", "both"])
        }
        ["v1", "names", _, _, "records"] => {
            query_matches(uri, "mode", &["verified", "both"])
                || (query_matches(uri, "mode", &["auto"]) && v1_auto_records_can_execute(uri))
        }
        ["v2", "addresses", _, "primary-name"] => true,
        ["v2", "names", _] => query_matches(uri, "source", &["verified"]),
        ["v2", "names", _, "records"] => {
            query_matches(uri, "source", &["verified"])
                || (query_matches(uri, "source", &["auto"]) && query_has_csv_item(uri, "keys"))
        }
        ["v2", "diagnostics", "names", _, "records"] => true,
        _ => false,
    }
}

fn v1_auto_records_can_execute(uri: &Uri) -> bool {
    let explicit_section = [
        "include",
        "texts",
        "known_text_keys",
        "avatar",
        "content_hash",
        "coin_types",
    ]
    .into_iter()
    .any(|key| query_has_nonblank_value(uri, key));
    if !explicit_section {
        return true;
    }

    query_has_csv_item(uri, "texts")
        || query_has_csv_item(uri, "coin_types")
        || query_matches(uri, "avatar", &["true"])
        || query_matches(uri, "content_hash", &["true"])
        || query_csv_matches(uri, "include", &["avatar", "content_hash", "coins"])
}

fn query_matches(uri: &Uri, key: &str, expected: &[&str]) -> bool {
    query_values(uri, key).any(|value| expected.contains(&value.trim()))
}

fn query_absent_or_matches(uri: &Uri, key: &str, expected: &[&str]) -> bool {
    let mut found = false;
    for value in query_values(uri, key) {
        found = true;
        if expected.contains(&value.trim()) {
            return true;
        }
    }
    !found
}

fn query_has_nonblank_value(uri: &Uri, key: &str) -> bool {
    query_values(uri, key).any(|value| !value.trim().is_empty())
}

fn query_has_csv_item(uri: &Uri, key: &str) -> bool {
    query_values(uri, key).any(|value| value.split(',').any(|item| !item.trim().is_empty()))
}

fn query_csv_matches(uri: &Uri, key: &str, expected: &[&str]) -> bool {
    query_values(uri, key).any(|value| {
        value
            .split(',')
            .map(str::trim)
            .any(|item| expected.contains(&item))
    })
}

fn query_values<'a>(uri: &'a Uri, key: &'a str) -> impl Iterator<Item = Cow<'a, str>> + 'a {
    uri.query()
        .into_iter()
        .flat_map(|query| form_urlencoded::parse(query.as_bytes()))
        .filter_map(move |(name, value)| (name == key).then_some(value))
}
