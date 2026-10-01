use std::collections::{BTreeMap, BTreeSet};

use super::openapi_contract::document;

fn literals(source: &str) -> BTreeSet<&str> {
    source.split('"').skip(1).step_by(2).collect()
}

fn allowed<'a>(source: &'a str, start: &str) -> BTreeSet<&'a str> {
    let rest = source
        .split_once(start)
        .unwrap_or_else(|| panic!("missing allowlist {start}"))
        .1;
    let rest = if start.starts_with("const ") {
        rest.split_once('=').expect("allowlist initializer").1
    } else {
        rest
    };
    let rest = rest
        .split_once("&[")
        .expect("allowlist starts with an array")
        .1;
    literals(rest.split_once(']').expect("allowlist closes").0)
}

#[test]
fn openapi_operations_equal_the_product_router_in_both_directions() {
    let registered = include_str!("../v2/router.rs")
        .split(".route(")
        .skip(1)
        .map(|rest| {
            let rest = rest
                .trim_start()
                .strip_prefix('"')
                .expect("literal route path");
            let (path, method) = rest.split_once('"').expect("closed route path");
            let method = method
                .trim_start()
                .strip_prefix(',')
                .expect("route method")
                .trim_start();
            let method = method.split_once('(').expect("method router").0;
            assert!(
                matches!(method, "get" | "post"),
                "new method expression requires explicit contract checking: {method}"
            );
            (path, method)
        })
        .filter(|(path, _)| !path.starts_with("/v1/diagnostics/"))
        .collect::<BTreeSet<_>>();
    let documented = document()["paths"]
        .as_object()
        .unwrap()
        .iter()
        .flat_map(|(path, methods)| {
            methods
                .as_object()
                .unwrap()
                .keys()
                .map(move |method| (path.as_str(), method.as_str()))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(registered, documented, "explicit product method/path pairs");
    assert_eq!(
        documented.len(),
        19,
        "update the selected-surface coverage deliberately"
    );
    assert!(
        !documented
            .iter()
            .any(|(path, _)| path.contains("diagnostics"))
    );
}

#[test]
fn openapi_query_names_equal_every_route_specific_extractor() {
    macro_rules! list {
        ($path:literal) => {
            allowed(include_str!($path), "const ALLOWED:")
        };
    }
    let source = BTreeMap::from([
        ("/v1/names", list!("../v2/names.rs")),
        ("/v1/names/{name}", list!("../v2/name_record.rs")),
        (
            "/v1/names/{name}/records",
            list!("../v2/name_records/mod.rs"),
        ),
        ("/v1/names/{name}/subnames", list!("../v2/subnames.rs")),
        ("/v1/names/{name}/history", list!("../v2/history.rs")),
        ("/v1/permissions", list!("../v2/permissions.rs")),
        (
            "/v1/addresses/{address}/names",
            list!("../v2/address_names.rs"),
        ),
        (
            "/v1/addresses/{address}/history",
            list!("../v2/address_history.rs"),
        ),
        (
            "/v1/addresses/{address}/primary-name",
            allowed(
                include_str!("../v2/primary_name.rs"),
                "parse_raw_query_params_with_allowlist::<",
            ),
        ),
        ("/v1/events", list!("../v2/events.rs")),
        (
            "/v1/search",
            allowed(
                include_str!("../v2/search/mod.rs"),
                "const SEARCH_QUERY_PARAMS:",
            ),
        ),
        (
            "/v1/resolvers/{chain_id}/{address}",
            list!("../v2/resolvers.rs"),
        ),
        (
            "/v1/resolvers/{chain_id}/{address}/links",
            list!("../v2/resolvers/collections.rs"),
        ),
        (
            "/v1/resolvers/{chain_id}/{address}/roles",
            list!("../v2/resolvers/collections.rs"),
        ),
        (
            "/v1/registries/{chain_id}/{address}",
            list!("../v2/registries.rs"),
        ),
        (
            "/v1/registries/{chain_id}/{address}/labels",
            list!("../v2/registries/labels.rs"),
        ),
        ("/v1/status", BTreeSet::new()),
        ("/v1/namespaces/{namespace}", BTreeSet::new()),
        ("/v1/lookup", BTreeSet::new()),
    ]);
    for (file, source) in [
        ("status", include_str!("../v2/status.rs")),
        ("namespace", include_str!("../v2/namespaces.rs")),
        ("lookup", include_str!("../v2/lookup/mod.rs")),
    ] {
        assert!(
            source.contains("NoQueryParams"),
            "{file} query-free extractor changed"
        );
    }
    let paths = document()["paths"].as_object().unwrap();
    assert_eq!(
        source.keys().copied().collect::<BTreeSet<_>>(),
        paths.keys().map(String::as_str).collect()
    );
    for (path, methods) in paths {
        for (method, operation) in methods.as_object().unwrap() {
            let parameters = operation["parameters"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let queries = parameters
                .iter()
                .filter(|parameter| parameter["in"] == "query")
                .map(|parameter| parameter["name"].as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                &queries,
                &source[path.as_str()],
                "{method} {path} query allowlist"
            );
            for parameter in parameters {
                if parameter["in"] == "query" && parameter["schema"]["type"] == "array" {
                    assert_eq!(parameter["style"], "form", "{path} {parameter}");
                    assert_eq!(
                        parameter["explode"], false,
                        "comma-separated query {path} {parameter}"
                    );
                }
            }
        }
    }
}
