use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

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
        20,
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
            "/v1/resolvers/{chain_id}/{address}/records",
            list!("../v2/resolvers/records.rs"),
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
                    let repeated = path == "/v1/names"
                        && matches!(
                            parameter["name"].as_str(),
                            Some("expires_window" | "grace_ends_window")
                        );
                    assert_eq!(
                        parameter["explode"], repeated,
                        "query array serialization {path} {parameter}"
                    );
                }
            }
        }
    }
}

/// Each operation's Fern SDK group and method name. SDK generators that group by these
/// extensions, such as Forge, silently leave out an operation that lacks them.
fn fern_sdk_names(document: &Value) -> Result<BTreeSet<(String, String)>, String> {
    let mut names = BTreeSet::new();
    for (path, methods) in document["paths"].as_object().ok_or("no paths")? {
        for (method, operation) in methods.as_object().ok_or(format!("{path}: no methods"))? {
            let at = format!("{method} {path}");
            let Some([Value::String(tag)]) = operation["tags"].as_array().map(Vec::as_slice) else {
                return Err(format!("{at}: needs exactly one tag"));
            };
            let prefix = format!("{method}_v1_");
            let sdk_method = operation["operationId"]
                .as_str()
                .and_then(|id| id.strip_prefix(&prefix))
                .ok_or(format!("{at}: operationId lacks the {prefix} prefix"))?;
            if operation["x-fern-sdk-group-name"] != tag.as_str() {
                return Err(format!("{at}: x-fern-sdk-group-name is not the tag {tag}"));
            }
            if operation["x-fern-sdk-method-name"] != sdk_method {
                return Err(format!("{at}: x-fern-sdk-method-name is not {sdk_method}"));
            }
            if !names.insert((tag.clone(), sdk_method.to_owned())) {
                return Err(format!(
                    "{at}: SDK group and method {tag}.{sdk_method} repeat"
                ));
            }
        }
    }
    Ok(names)
}

#[test]
fn every_operation_has_a_distinct_fern_sdk_group_and_method() {
    let names = fern_sdk_names(document()).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(names.len(), 20, "one SDK method per operation");
    assert!(names.contains(&("lookup".into(), "lookup".into())));

    let spec = |operations: &[(&str, &str, Value)]| {
        let mut paths = json!({});
        for (path, method, operation) in operations {
            paths[*path][*method] = operation.clone();
        }
        json!({ "paths": paths })
    };
    let operation = |id: &str, tags: Value, group: &str, sdk_method: &str| {
        json!({
            "operationId": id,
            "tags": tags,
            "x-fern-sdk-group-name": group,
            "x-fern-sdk-method-name": sdk_method,
        })
    };
    let name = operation("get_v1_names_name", json!(["names"]), "names", "names_name");
    let pairs = |names: &[(&str, &str)]| {
        names
            .iter()
            .map(|(group, sdk_method)| (group.to_string(), sdk_method.to_string()))
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        fern_sdk_names(&spec(&[("/v1/names/{name}", "get", name.clone())])),
        Ok(pairs(&[("names", "names_name")]))
    );
    let mut untagged = name.clone();
    untagged.as_object_mut().unwrap().remove("tags");
    for tags in [None, Some(json!([])), Some(json!(["names", "extra"]))] {
        let mut operation = untagged.clone();
        if let Some(tags) = tags {
            operation["tags"] = tags;
        }
        let error = fern_sdk_names(&spec(&[("/v1/names/{name}", "get", operation)])).unwrap_err();
        assert!(error.contains("exactly one tag"), "{error}");
    }
    let subnames = operation(
        "get_v1_names_name_subnames",
        json!(["names"]),
        "names",
        "names_name_subnames",
    );
    assert_eq!(
        fern_sdk_names(&spec(&[
            ("/v1/names/{name}", "get", name.clone()),
            ("/v1/names/{name}/subnames", "get", subnames),
        ])),
        Ok(pairs(&[
            ("names", "names_name"),
            ("names", "names_name_subnames")
        ]))
    );
    let lookup = operation("post_v1_lookup", json!(["lookup"]), "lookup", "lookup");
    assert_eq!(
        fern_sdk_names(&spec(&[("/v1/lookup", "post", lookup.clone())])),
        Ok(pairs(&[("lookup", "lookup")]))
    );
    let mut unstripped = lookup;
    unstripped["x-fern-sdk-method-name"] = json!("post_v1_lookup");
    let error = fern_sdk_names(&spec(&[("/v1/lookup", "post", unstripped)])).unwrap_err();
    assert!(error.contains("is not lookup"), "{error}");
    let post_name = operation(
        "post_v1_names_name",
        json!(["names"]),
        "names",
        "names_name",
    );
    let error = fern_sdk_names(&spec(&[
        ("/v1/names/{name}", "get", name),
        ("/v1/names/{name}", "post", post_name),
    ]))
    .unwrap_err();
    assert!(error.contains("names.names_name repeat"), "{error}");
}
