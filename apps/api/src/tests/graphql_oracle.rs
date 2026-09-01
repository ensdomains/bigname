use std::{
    collections::{BTreeMap as OracleMap, BTreeSet as OracleSet},
    fs as oracle_fs,
    path::{Path as OraclePath, PathBuf as OraclePathBuf},
    process::Command as OracleCommand,
};

const ORACLE_INTROSPECTION: &str = r#"query OracleIntrospection {
  __schema { queryType { name } mutationType { name } subscriptionType { name }
  types { kind name interfaces { name } possibleTypes { name } fields(includeDeprecated: true) {
    name isDeprecated type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } }
    args(includeDeprecated: true) { name defaultValue isDeprecated type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
  } inputFields(includeDeprecated: true) { name defaultValue isDeprecated type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
  enumValues(includeDeprecated: true) { name isDeprecated } } }
}"#;

fn oracle_root() -> OraclePathBuf {
    OraclePath::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/fixtures/graphql-oracle/v1")
}

fn read_oracle_json(path: impl AsRef<OraclePath>) -> Result<Value> {
    Ok(serde_json::from_slice(&oracle_fs::read(path)?)?)
}

fn verify_oracle_integrity() -> Result<Value> {
    let workspace = OraclePath::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = OracleCommand::new(workspace.join("scripts/graphql-compat-oracle"))
        .args(["verify-fixtures", "--offline", "--fixtures"])
        .arg(oracle_root())
        .current_dir(&workspace)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "fixture integrity failed before query execution: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = read_oracle_json(oracle_root().join("manifest.json"))?;
    anyhow::ensure!(
        manifest["fixture_format_version"] == json!(1),
        "unsupported fixture format; upgrade the oracle runner"
    );
    Ok(manifest)
}

#[test]
fn graphql_oracle_rejects_provisional_fixture_without_local_escape() -> Result<()> {
    let workspace = OraclePath::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = OracleCommand::new(workspace.join("scripts/graphql-compat-oracle"))
        .args(["verify-fixtures", "--offline", "--fixtures"])
        .arg(oracle_root())
        .env_remove("BIGNAME_ALLOW_PROVISIONAL_GRAPHQL_ORACLE")
        .current_dir(&workspace)
        .output()?;
    anyhow::ensure!(!output.status.success(), "provisional fixture was accepted");
    anyhow::ensure!(
        String::from_utf8_lossy(&output.stderr).contains("docs/development.md"),
        "rejection did not name the operator refresh procedure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn oracle_type_ref(value: &Value) -> Result<String> {
    match value["kind"].as_str().context("type reference kind")? {
        "NON_NULL" => Ok(format!("{}!", oracle_type_ref(&value["ofType"])?)),
        "LIST" => Ok(format!("[{}]", oracle_type_ref(&value["ofType"])?)),
        _ => value["name"]
            .as_str()
            .map(str::to_owned)
            .context("named type reference"),
    }
}

fn oracle_schema_surface(payload: &Value) -> Result<OracleMap<String, Value>> {
    let mut surface = OracleMap::new();
    let schema = payload.pointer("/data/__schema").context("introspection schema")?;
    for kind in ["query", "mutation", "subscription"] {
        if let Some(name) = schema[format!("{kind}Type")]["name"].as_str() {
            surface.insert(format!("root:{kind}"), json!({"type": name}));
        }
    }
    for item in schema["types"].as_array().context("introspection types")? {
        let Some(name) = item["name"].as_str().filter(|name| !name.starts_with("__")) else {
            continue;
        };
        surface.insert(format!("type:{name}"), json!({"kind": item["kind"]}));
        for interface in item["interfaces"].as_array().into_iter().flatten() {
            surface.insert(format!("implements:{name}.{}", interface["name"].as_str().context("interface name")?), json!({"present":true}));
        }
        for member in item["possibleTypes"].as_array().into_iter().flatten() {
            surface.insert(format!("member:{name}.{}", member["name"].as_str().context("member name")?), json!({"present":true}));
        }
        for field in item["fields"].as_array().into_iter().flatten() {
            let field_name = field["name"].as_str().context("field name")?;
            surface.insert(format!("field:{name}.{field_name}"), json!({"deprecated": field["isDeprecated"].as_bool().unwrap_or(false), "type": oracle_type_ref(&field["type"])?}));
            for arg in field["args"].as_array().into_iter().flatten() {
                surface.insert(format!("arg:{name}.{field_name}({})", arg["name"].as_str().context("argument name")?), json!({"default": arg["defaultValue"], "deprecated": arg["isDeprecated"].as_bool().unwrap_or(false), "type": oracle_type_ref(&arg["type"])?}));
            }
        }
        for field in item["inputFields"].as_array().into_iter().flatten() {
            surface.insert(format!("input:{name}.{}", field["name"].as_str().context("input field name")?), json!({"default": field["defaultValue"], "deprecated": field["isDeprecated"].as_bool().unwrap_or(false), "type": oracle_type_ref(&field["type"])?}));
        }
        for value in item["enumValues"].as_array().into_iter().flatten() {
            surface.insert(
                format!(
                    "enum:{name}.{}",
                    value["name"].as_str().context("enum value name")?
                ),
                json!({"deprecated": value["isDeprecated"].as_bool().unwrap_or(false)}),
            );
        }
    }
    Ok(surface)
}

fn scope_matches(scope: &str, path: &str) -> bool {
    if let Some(name) = scope.strip_prefix("type:") {
        return path == scope
            || ["field:", "arg:", "input:", "enum:", "implements:", "member:"]
                .iter()
                .any(|prefix| path.starts_with(&format!("{prefix}{name}.")));
    }
    scope.strip_prefix("root:").is_some_and(|root| {
        path == format!("field:{root}") || path.starts_with(&format!("arg:{root}("))
    })
}

fn apply_oracle_coverage(
    upstream: &OracleMap<String, Value>,
    local: &OracleMap<String, Value>,
    coverage: &Value,
) -> Result<(usize, usize, usize)> {
    let upstream_only: OracleSet<_> = upstream
        .keys()
        .filter(|path| !local.contains_key(*path))
        .cloned()
        .collect();
    let local_only: OracleSet<_> = local
        .keys()
        .filter(|path| !upstream.contains_key(*path))
        .cloned()
        .collect();
    let changed: OracleSet<_> = upstream
        .keys()
        .filter(|path| local.get(*path) != Some(&upstream[*path]))
        .filter(|path| local.contains_key(*path))
        .cloned()
        .collect();
    let mut failures = Vec::new();
    for path in coverage["claimed_paths"]
        .as_array()
        .context("claimed_paths")?
        .iter()
        .filter_map(Value::as_str)
    {
        if !upstream.contains_key(path) || local.get(path) != upstream.get(path) {
            failures.push(format!("claimed path changed: {path}"));
        }
    }
    let dispositions = coverage["schema_signature_differences"]
        .as_array()
        .context("schema_signature_differences")?;
    let mut seen = OracleSet::new();
    for disposition in dispositions {
        let path = disposition["path"]
            .as_str()
            .context("signature disposition path")?;
        if !seen.insert(path) {
            failures.push(format!("duplicate conflicting disposition: {path}"));
        }
        if !path.contains('*')
            && changed.contains(path)
            && disposition["upstream"] == upstream[path]
            && disposition["local"] == local[path]
        {
            continue;
        }
        failures.push(format!("stale or invalid signature disposition: {path}"));
    }
    for path in &changed {
        if !seen.contains(path.as_str()) {
            failures.push(format!("un-dispositioned shared change: {path}"));
        }
    }
    let deferred = coverage["upstream_only"]
        .as_array()
        .context("upstream_only")?;
    let mut scopes = OracleSet::new();
    for entry in deferred {
        let scope = entry["scope"].as_str().context("upstream scope")?;
        if !scopes.insert(scope) {
            failures.push(format!("duplicate conflicting disposition: {scope}"));
        }
        if scope.contains('*') || !scope.starts_with("type:") && !scope.starts_with("root:") {
            failures.push(format!("overbroad disposition: {scope}"));
            continue;
        }
        if !upstream_only.iter().any(|path| scope_matches(scope, path)) {
            failures.push(format!("stale upstream disposition: {scope}"));
        }
    }
    for path in &upstream_only {
        if !deferred.iter().any(|entry| {
            entry["scope"]
                .as_str()
                .is_some_and(|scope| scope_matches(scope, path))
        }) {
            failures.push(format!("unowned upstream-only path: {path}"));
        }
    }
    let extensions = coverage["local_extensions"]
        .as_array()
        .context("local_extensions")?;
    let mut extension_paths = OracleSet::new();
    for entry in extensions {
        let path = entry["path"].as_str().context("extension path")?;
        if path.contains('*') || !extension_paths.insert(path) {
            failures.push(format!("overbroad or duplicate local extension: {path}"));
        }
        if !local_only.contains(path) {
            failures.push(format!("stale local extension: {path}"));
        }
    }
    for path in &local_only {
        if !extensions.iter().any(|entry| entry["path"] == json!(path)) {
            failures.push(format!("undocumented local extension: {path}"));
        }
    }
    let known = coverage["known_upstream_types"]
        .as_object()
        .context("known_upstream_types")?;
    for (name, entry) in known {
        if !entry["owner"].as_str().is_some_and(|owner| owner.starts_with('#'))
            || entry["docs"].as_str().is_none_or(str::is_empty)
            || !upstream.contains_key(&format!("type:{name}"))
        {
            failures.push(format!("upstream type census lacks owner or docs: {name}"));
        }
    }
    for path in upstream_only
        .iter()
        .filter(|path| path.starts_with("type:"))
    {
        if !known.contains_key(path.trim_start_matches("type:")) {
            failures.push(format!("unknown upstream entity/type: {path}"));
        }
    }
    for collection in [dispositions, deferred, extensions] {
        for entry in collection {
            if !entry["owner"]
                .as_str()
                .is_some_and(|owner| owner.starts_with('#'))
                || entry["docs"].as_str().is_none_or(str::is_empty)
                || !matches!(
                    entry["status"].as_str(),
                    Some("deferred" | "intentional-extension")
                )
            {
                failures.push(format!(
                    "disposition lacks valid status, issue owner, or docs: {entry}"
                ));
            }
        }
    }
    anyhow::ensure!(
        failures.is_empty(),
        "schema compatibility failures:\n{}",
        failures.join("\n")
    );
    Ok((upstream_only.len(), local_only.len(), changed.len()))
}

fn first_json_difference(
    path: &str,
    expected: &Value,
    actual: &Value,
) -> Option<(String, Value, Value)> {
    match (expected, actual) {
        (Value::Object(left), Value::Object(right)) => {
            let keys: OracleSet<_> = left.keys().chain(right.keys()).collect();
            keys.into_iter()
                .find_map(|key| match (left.get(key), right.get(key)) {
                    (Some(left), Some(right)) => {
                        first_json_difference(&format!("{path}/{key}"), left, right)
                    }
                    (left, right) => Some((
                        format!("{path}/{key}"),
                        left.cloned().unwrap_or_else(|| json!({"oracle":"omitted"})),
                        right
                            .cloned()
                            .unwrap_or_else(|| json!({"oracle":"omitted"})),
                    )),
                })
        }
        (Value::Array(left), Value::Array(right)) => {
            (0..left.len().max(right.len())).find_map(|index| {
                match (left.get(index), right.get(index)) {
                    (Some(left), Some(right)) => {
                        first_json_difference(&format!("{path}/{index}"), left, right)
                    }
                    (left, right) => Some((
                        format!("{path}/{index}"),
                        left.cloned().unwrap_or_else(|| json!({"oracle":"omitted"})),
                        right
                            .cloned()
                            .unwrap_or_else(|| json!({"oracle":"omitted"})),
                    )),
                }
            })
        }
        _ if expected != actual => Some((path.to_owned(), expected.clone(), actual.clone())),
        _ => None,
    }
}

async fn run_oracle_cases(kind: &str) -> Result<()> {
    let manifest = verify_oracle_integrity()?;
    let root = oracle_root();
    let coverage = read_oracle_json(root.join("coverage.json"))?;
    let upstream: OracleMap<String, Value> =
        serde_json::from_value(read_oracle_json(root.join("upstream/schema-surface.json"))?)?;
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let introspection = post_graphql(database.app_state(), ORACLE_INTROSPECTION, json!({})).await?;
    let local = oracle_schema_surface(&introspection)?;
    let summary = apply_oracle_coverage(&upstream, &local, &coverage)?;
    println!(
        "GraphQL schema diff: {} upstream-only, {} local-only, {} signature differences; all dispositioned",
        summary.0, summary.1, summary.2
    );
    let mut executed = 0;
    for case in manifest["cases"]
        .as_array()
        .context("manifest cases")?
        .iter()
        .filter(|case| case["kind"] == kind)
    {
        executed += 1;
        let case_id = case["id"].as_str().context("oracle case ID")?;
        for path in case["required_schema_paths"].as_array().context("required schema paths")?.iter().filter_map(Value::as_str) {
            anyhow::ensure!(local.contains_key(path), "{case_id}: required schema path missing: {path}");
        }
        let query = oracle_fs::read_to_string(root.join(case["query"].as_str().context("query path")?))?;
        let variables = read_oracle_json(root.join(case["variables"].as_str().context("variables path")?))?;
        let fixture_block = variables.pointer("/block/number").and_then(Value::as_i64).context("fixture block")?;
        let actual = post_graphql_allow_errors(database.app_state(), &query, variables).await?;
        anyhow::ensure!(actual.get("errors").is_none(), "{case_id}: GraphQL errors at fixture block {fixture_block}: {}", actual["errors"]);
        let expected = read_oracle_json(root.join(case["response"].as_str().context("response path")?))?;
        if let Some((path, expected, actual)) = first_json_difference("$", &expected, &actual) {
            anyhow::bail!("{case_id}: response mismatch at {path}; expected {expected}, actual {actual}; fixture block {fixture_block}");
        }
    }
    anyhow::ensure!(executed > 0, "manifest has no {kind} oracle case");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_oracle_domain_entity_by_id_matches_pinned_upstream() -> Result<()> {
    run_oracle_cases("entity").await
}

#[tokio::test]
async fn graphql_oracle_domain_filter_name_eq_matches_pinned_upstream() -> Result<()> {
    run_oracle_cases("filter").await
}

#[test]
#[rustfmt::skip]
fn graphql_oracle_schema_comparator_rejects_semantic_drift() {
    let coverage = json!({"claimed_paths":["field:Query.value"],"schema_signature_differences":[],"upstream_only":[],"local_extensions":[],"known_upstream_types":{}});
    for (upstream, local) in [("ID!", "String!"), ("String!", "String"), ("[String!]!", "String")] {
        let left = OracleMap::from([("field:Query.value".into(), json!({"type":upstream}))]);
        let right = OracleMap::from([("field:Query.value".into(), json!({"type":local}))]);
        assert!(apply_oracle_coverage(&left, &right, &coverage).is_err());
    }
    let field = ("field:Query.value".into(), json!({"type":"String"}));
    let arg = ("arg:Query.value(limit)".into(), json!({"type":"Int","default":"1","deprecated":false}));
    assert!(apply_oracle_coverage(&OracleMap::from([field.clone(), arg.clone()]), &OracleMap::from([field.clone()]), &coverage).is_err());
    assert!(apply_oracle_coverage(&OracleMap::from([field.clone(), arg.clone()]), &OracleMap::from([field.clone(), (arg.0, json!({"type":"Int","default":"2","deprecated":false}))]), &coverage).is_err());
    assert!(apply_oracle_coverage(&OracleMap::from([field.clone(), ("enum:Mode.new".into(), json!({"deprecated":false}))]), &OracleMap::from([field.clone()]), &coverage).is_err());
    assert!(apply_oracle_coverage(&OracleMap::from([field.clone()]), &OracleMap::from([field.clone(), ("field:Query.extra".into(), json!({"type":"String"}))]), &coverage).is_err());
    assert!(apply_oracle_coverage(&OracleMap::from([field]), &OracleMap::new(), &coverage).is_err());
    assert!(apply_oracle_coverage(&OracleMap::new(), &OracleMap::new(), &coverage).is_err());
}

#[test]
#[rustfmt::skip]
fn graphql_oracle_dispositions_reject_unknown_stale_duplicate_and_wildcard_entries() {
    let upstream = OracleMap::from([("type:Future".into(), json!({"kind":"OBJECT"}))]);
    let local = OracleMap::new();
    for coverage in [
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[],"local_extensions":[],"known_upstream_types":{}}),
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[{"scope":"*","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}}),
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[{"scope":"type:Missing","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}}),
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[{"scope":"type:Future","status":"deferred","owner":"#1","docs":"x"},{"scope":"type:Future","status":"deferred","owner":"#2","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}}),
        json!({"claimed_paths":[],"schema_signature_differences":[{"path":"field:Query.stale","upstream":{},"local":{},"status":"deferred","owner":"#1","docs":"x"}],"upstream_only":[{"scope":"type:Future","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}}),
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[{"scope":"type:Future","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[{"path":"field:Query.stale","status":"intentional-extension","owner":"#1","docs":"x"}],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}}),
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[{"scope":"type:Future","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{}}}),
        json!({"claimed_paths":[],"schema_signature_differences":[],"upstream_only":[{"scope":"type:Future","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"},"Ghost":{"owner":"#1","docs":"x"}}}),
    ] {
        assert!(apply_oracle_coverage(&upstream, &local, &coverage).is_err());
    }
}

#[test]
#[rustfmt::skip]
fn graphql_oracle_normalization_ignores_only_nonsemantic_order_and_prose() -> Result<()> {
    let field = |name: &str, description: &str| json!({"name":name,"description":description,"deprecationReason":"changed prose","isDeprecated":true,"type":{"kind":"SCALAR","name":"String"},"args":[]});
    let payload = |fields: Vec<Value>| json!({"data":{"__schema":{"types":[{"kind":"OBJECT","name":"Query","fields":fields,"inputFields":null,"enumValues":null}]}}});
    assert_eq!(oracle_schema_surface(&payload(vec![field("b", "old"), field("a", "old")]))?, oracle_schema_surface(&payload(vec![field("a", "new"), field("b", "new")]))?);
    assert!(first_json_difference("$", &json!({"a":1,"b":2}), &json!({"b":2,"a":1})).is_none());
    assert!(first_json_difference("$", &json!([1, 2]), &json!([2, 1])).is_some());
    assert!(first_json_difference("$", &json!({"value":null}), &json!({})).is_some());
    let membership = json!({"data":{"__schema":{"queryType":{"name":"Query"},"mutationType":null,"subscriptionType":null,"types":[{"kind":"OBJECT","name":"Query","fields":[],"interfaces":[]},{"kind":"INTERFACE","name":"DomainEvent","fields":[],"interfaces":[],"possibleTypes":[{"name":"Transfer"}]},{"kind":"OBJECT","name":"Transfer","fields":[],"interfaces":[{"name":"DomainEvent"}]}]}}});
    let surface = oracle_schema_surface(&membership)?;
    assert_eq!(surface["root:query"], json!({"type":"Query"})); assert!(surface.contains_key("implements:Transfer.DomainEvent")); assert!(surface.contains_key("member:DomainEvent.Transfer"));
    Ok(())
}
