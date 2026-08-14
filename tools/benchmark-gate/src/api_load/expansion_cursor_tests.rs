use super::*;
use crate::api_load::workload::{get, post};
use serde_json::json;

#[test]
fn cursor_variants_preserve_expansion_queries() {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let lookup = post(
        &base,
        &["v2", "lookup"],
        json!({"inputs": [{"address": "0x01"}]}),
    )
    .unwrap();
    let resumed = cursor_variants(
        &lookup,
        &json!({"data": [{"page": {"next_cursor": "lookup-next"}}]}),
    );
    assert_eq!(
        resumed[0].body.as_ref().unwrap()["inputs"][0]["cursor"],
        "lookup-next"
    );

    let resolver = get(&base, &["v2", "resolvers", "1", "0x01"], &[]).unwrap();
    let resumed = cursor_variants(
        &resolver,
        &json!({"data": {"bound_names": {"page": {"next_cursor": "resolver-next"}}}}),
    );
    assert_eq!(resumed[0].url.query(), Some("cursor=resolver-next"));

    let permissions = get(
        &base,
        &["v2", "permissions"],
        &[("address", "0x01"), ("include", "lineage")],
    )
    .unwrap();
    let resumed = cursor_variants(
        &permissions,
        &json!({"page": {"next_cursor": "permissions-next"}}),
    );
    let query = resumed[0].url.query_pairs().collect::<BTreeMap<_, _>>();
    assert_eq!(
        query.get("include").map(|value| value.as_ref()),
        Some("lineage")
    );
    assert_eq!(
        query.get("cursor").map(|value| value.as_ref()),
        Some("permissions-next")
    );
}
