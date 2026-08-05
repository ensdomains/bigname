use std::borrow::Cow;

use axum::http::{Method, Uri};

pub(super) fn is_verified_execution_request(method: &Method, uri: &Uri) -> bool {
    if !matches!(method, &Method::GET | &Method::HEAD) {
        return false;
    }
    let segments = uri.path().trim_matches('/').split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        ["v2", "addresses", _, "primary-name"] => {
            query_absent_blank_or_matches(uri, "source", &["verified"])
        }
        ["v2", "names", _] => query_matches(uri, "source", &["verified"]),
        ["v2", "names", _, "records"] => {
            query_matches(uri, "source", &["verified"])
                || (query_matches(uri, "source", &["auto"]) && query_has_csv_item(uri, "keys"))
        }
        ["v2", "diagnostics", "names", _, "records"] => true,
        _ => false,
    }
}

fn query_matches(uri: &Uri, key: &str, expected: &[&str]) -> bool {
    query_values(uri, key).any(|value| expected.contains(&value.trim()))
}

fn query_absent_blank_or_matches(uri: &Uri, key: &str, expected: &[&str]) -> bool {
    let mut found = false;
    for value in query_values(uri, key) {
        found = true;
        let value = value.trim();
        if value.is_empty() || expected.contains(&value) {
            return true;
        }
    }
    !found
}

fn query_has_csv_item(uri: &Uri, key: &str) -> bool {
    query_values(uri, key).any(|value| value.split(',').any(|item| !item.trim().is_empty()))
}

fn query_values<'a>(uri: &'a Uri, key: &'a str) -> impl Iterator<Item = Cow<'a, str>> + 'a {
    uri.query()
        .into_iter()
        .flat_map(|query| form_urlencoded::parse(query.as_bytes()))
        .filter_map(move |(name, value)| (name == key).then_some(value))
}
