use std::{
    collections::{BTreeMap as OracleMap, BTreeSet as OracleSet},
    fs as oracle_fs,
    path::{Path as OraclePath, PathBuf as OraclePathBuf},
    process::Command as OracleCommand,
};
use sha2::{Digest, Sha256};

const ORACLE_INTROSPECTION: &str = r#"query OracleIntrospection {
  __schema { queryType { name } mutationType { name } subscriptionType { name }
  types { kind name interfaces { name } possibleTypes { name } fields(includeDeprecated: true) {
    name isDeprecated type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } }
    args(includeDeprecated: true) { name defaultValue isDeprecated type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
  } inputFields(includeDeprecated: true) { name defaultValue isDeprecated type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
  enumValues(includeDeprecated: true) { name isDeprecated } } }
}"#;

#[test]
fn graphql_oracle_local_introspection_uses_comparable_surface() {
    assert!(ORACLE_INTROSPECTION.contains("inputFields(includeDeprecated: true)"));
    assert!(!ORACLE_INTROSPECTION.contains("isRepeatable"));
}

fn oracle_root() -> OraclePathBuf {
    OraclePath::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/fixtures/graphql-oracle/v1")
}

fn read_oracle_json(path: impl AsRef<OraclePath>) -> Result<Value> {
    Ok(serde_json::from_slice(&oracle_fs::read(path)?)?)
}

fn oracle_sha256(input: &[u8]) -> String {
    hex::encode(Sha256::digest(input))
}

#[test]
fn graphql_oracle_sha256_matches_known_vector() {
    assert_eq!(
        oracle_sha256(b"{}\n"),
        "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"
    );
}

fn verify_coverage_sha256(root: &OraclePath, manifest: &Value) -> Result<()> {
    let actual = oracle_sha256(&oracle_fs::read(root.join("coverage.json"))?);
    let expected = manifest["coverage_sha256"]
        .as_str()
        .context("manifest coverage_sha256")?;
    anyhow::ensure!(
        actual == expected,
        "coverage_sha256 mismatch: manifest has {expected}, coverage.json has {actual}"
    );
    Ok(())
}

fn verify_oracle_integrity() -> Result<Value> {
    let workspace = OraclePath::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = read_oracle_json(oracle_root().join("manifest.json"))?;
    anyhow::ensure!(
        manifest["fixture_format_version"] == json!(1),
        "unsupported fixture format; upgrade the oracle runner"
    );
    verify_coverage_sha256(&oracle_root(), &manifest)?;
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
    Ok(manifest)
}

#[test]
fn graphql_oracle_rejects_provisional_fixture_without_local_escape() -> Result<()> {
    let workspace = OraclePath::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixtures = std::env::temp_dir().join(format!(
        "bigname-graphql-oracle-{}-{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let generated = OracleCommand::new(workspace.join("scripts/test-graphql-compat-oracle"))
        .args(["--generate", fixtures.to_str().context("temporary fixture path")?])
        .current_dir(&workspace)
        .output()?;
    anyhow::ensure!(
        generated.status.success(),
        "failed to generate provisional fixture: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let output = OracleCommand::new(workspace.join("scripts/graphql-compat-oracle"))
        .args(["verify-fixtures", "--offline", "--fixtures"])
        .arg(&fixtures)
        .env_remove("BIGNAME_ALLOW_PROVISIONAL_GRAPHQL_ORACLE")
        .current_dir(&workspace)
        .output()?;
    oracle_fs::remove_dir_all(fixtures)?;
    anyhow::ensure!(!output.status.success(), "provisional fixture was accepted");
    anyhow::ensure!(
        String::from_utf8_lossy(&output.stderr).contains("docs/development.md"),
        "rejection did not name the operator refresh procedure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn graphql_oracle_rejects_mismatched_manifest_coverage_digest() -> Result<()> {
    let fixtures = std::env::temp_dir().join(format!(
        "bigname-graphql-oracle-coverage-digest-{}-{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    ));
    oracle_fs::create_dir_all(&fixtures)?;
    oracle_fs::write(fixtures.join("coverage.json"), b"{}\n")?;
    let manifest = json!({"coverage_sha256": "0000000000000000000000000000000000000000000000000000000000000000"});
    let error = verify_coverage_sha256(&fixtures, &manifest)
        .expect_err("mismatched coverage_sha256 was accepted")
        .to_string();
    oracle_fs::remove_dir_all(fixtures)?;
    assert!(error.contains("coverage_sha256 mismatch"));
    Ok(())
}

struct OracleDomainSeed {
    id: String,
    name: String,
    created_at: i64,
    owner: String,
}

struct OracleSeed {
    block: i64,
    domain: OracleDomainSeed,
    distractor: OracleDomainSeed,
}

fn oracle_domain_seed(value: &Value, derive_id: bool) -> Result<OracleDomainSeed> {
    let name = value["name"].as_str().context("oracle seed name")?.to_owned();
    let id = if derive_id {
        bigname_lookup::ens_namehash_hex(&name)?
    } else {
        value["id"].as_str().context("oracle seed id")?.to_owned()
    };
    anyhow::ensure!(
        id == bigname_lookup::ens_namehash_hex(&name)?,
        "oracle Domain id is not the namehash of {name}"
    );
    Ok(OracleDomainSeed {
        id,
        name,
        created_at: value["createdAt"]
            .as_str()
            .context("oracle seed createdAt")?
            .parse()?,
        owner: value["owner"]["id"]
            .as_str()
            .context("oracle seed owner")?
            .to_owned(),
    })
}

fn oracle_seed_from_values(response: &Value, provenance: &Value, descriptor: &Value) -> Result<OracleSeed> {
    Ok(OracleSeed {
        block: provenance["block_number"].as_i64().context("oracle seed block")?,
        domain: oracle_domain_seed(&response["data"]["domain"], false)?,
        distractor: oracle_domain_seed(&descriptor["distractor"], true)?,
    })
}

fn load_oracle_seed(root: &OraclePath, manifest: &Value) -> Result<OracleSeed> {
    let point = manifest["cases"].as_array().context("manifest cases")?.iter().find(|case| case["id"] == "domain.entity-by-id").context("point case")?;
    oracle_seed_from_values(
        &read_oracle_json(root.join(point["response"].as_str().context("point response path")?))?,
        &read_oracle_json(root.join("upstream/provenance.json"))?,
        &read_oracle_json(root.join(manifest["seed"].as_str().context("seed descriptor path")?))?,
    )
}

async fn seed_oracle_fixture(database: &TestDatabase, seed: &OracleSeed) -> Result<()> {
    for (index, domain) in [&seed.domain, &seed.distractor].into_iter().enumerate() {
        let token_lineage_id = Uuid::from_u128(0x670_1001 + index as u128 * 3);
        let resource_id = Uuid::from_u128(0x670_1002 + index as u128 * 3);
        let surface_binding_id = Uuid::from_u128(0x670_1003 + index as u128 * 3);
        let logical_name_id = format!("ens:{}", domain.name);
        upsert_test_token_lineages(&database.pool, &[address_name_token_lineage(token_lineage_id, &format!("0xoracle-tl-{index}"), seed.block)]).await?;
        upsert_test_resources(&database.pool, &[address_name_resource(resource_id, Some(token_lineage_id), &format!("0xoracle-res-{index}"), seed.block)]).await?;
        upsert_test_name_surfaces(&database.pool, &[collection_name_surface(&logical_name_id, &domain.name, &domain.id, seed.block)]).await?;
        upsert_test_surface_bindings(&database.pool, &[address_name_surface_binding(surface_binding_id, &logical_name_id, resource_id, &format!("0xoracle-bind-{index}"), seed.block, 1_700_000_000 + index as i64)]).await?;
        database.insert_name_current_row(address_name_name_current_row(&logical_name_id, &domain.name, &domain.name, &domain.id, surface_binding_id, resource_id, Some(token_lineage_id), seed.block, json!({
            "registration": {"status": "active", "authority_kind": "registrar", "created_at": domain.created_at},
            "control": {"registry_owner": domain.owner},
        }))).await?;
    }
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
    if scope.starts_with("field:") || scope.starts_with("input:") {
        return path == scope;
    }
    if exact_enum_scope(scope).is_some() {
        return path == scope;
    }
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

fn exact_enum_scope(scope: &str) -> Option<(&str, &str)> {
    let (enum_type, value) = scope.strip_prefix("enum:")?.split_once('.')?;
    (!enum_type.is_empty() && !value.is_empty() && !value.contains('.'))
        .then_some((enum_type, value))
}

fn oracle_named_type(type_ref: &str) -> &str {
    type_ref.trim_matches(['[', ']', '!'])
}

fn oracle_parent_field(path: &str) -> Option<String> {
    path.strip_prefix("arg:")
        .and_then(|path| path.split_once('('))
        .map(|(field, _)| format!("field:{field}"))
}

fn oracle_field_is_deferred(
    path: &str,
    upstream: &OracleMap<String, Value>,
    upstream_only: &OracleSet<String>,
    deferred: &[Value],
    known: &serde_json::Map<String, Value>,
    claimed: &OracleSet<String>,
) -> bool {
    if deferred.iter().any(|entry| {
        entry["scope"]
            .as_str()
            .is_some_and(|scope| scope_matches(scope, path))
    }) {
        return true;
    }
    let Some(field) = path.strip_prefix("field:") else {
        return false;
    };
    let Some((parent_type, _)) = field.split_once('.') else {
        return false;
    };
    if upstream_only.contains(&format!("type:{parent_type}")) {
        return true;
    }
    if parent_type != "Query" {
        return false;
    }
    if !upstream_only.contains(path) {
        return false;
    }
    let Some(return_type) = upstream[path]["type"].as_str().map(oracle_named_type) else {
        return false;
    };
    let return_type_path = format!("type:{return_type}");
    known.contains_key(return_type)
        && !claimed.contains(&return_type_path)
        && upstream_only.contains(&return_type_path)
        && upstream[&return_type_path]["kind"] == json!("OBJECT")
        && upstream.contains_key(&format!("field:{return_type}.id"))
}

fn oracle_upstream_path_is_deferred(
    path: &str,
    upstream: &OracleMap<String, Value>,
    upstream_only: &OracleSet<String>,
    deferred: &[Value],
    known: &serde_json::Map<String, Value>,
    claimed: &OracleSet<String>,
) -> bool {
    if path.starts_with("field:") {
        return oracle_field_is_deferred(path, upstream, upstream_only, deferred, known, claimed);
    }
    if let Some(parent) = oracle_parent_field(path) {
        return oracle_field_is_deferred(
            &parent,
            upstream,
            upstream_only,
            deferred,
            known,
            claimed,
        );
    }
    deferred.iter().any(|entry| {
        entry["scope"]
            .as_str()
            .is_some_and(|scope| scope_matches(scope, path))
    }) || known.keys().any(|name| {
        upstream_only.contains(&format!("type:{name}"))
            && scope_matches(&format!("type:{name}"), path)
    })
}

fn apply_oracle_coverage(
    upstream: &OracleMap<String, Value>,
    local: &OracleMap<String, Value>,
    coverage: &Value,
) -> Result<(usize, usize, usize, usize)> {
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
    let known = coverage["known_upstream_types"]
        .as_object()
        .context("known_upstream_types")?;
    let mut claimed = OracleSet::new();
    for value in coverage["claimed_paths"]
        .as_array()
        .context("claimed_paths")?
    {
        let Some(path) = value.as_str().filter(|path| !path.is_empty()) else {
            failures.push(format!("invalid claimed path: {value}"));
            continue;
        };
        if !claimed.insert(path.to_owned()) {
            failures.push(format!("duplicate claimed path: {path}"));
            continue;
        }
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
        if scope.contains('*')
            || scope.starts_with("enum:") && exact_enum_scope(scope).is_none()
            || !scope.starts_with("type:")
                && !scope.starts_with("root:")
                && !scope.starts_with("field:")
                && !scope.starts_with("input:")
                && !scope.starts_with("enum:")
        {
            failures.push(format!("overbroad disposition: {scope}"));
            continue;
        }
        if scope.starts_with("type:") && !upstream_only.contains(scope) {
            failures.push(format!("overbroad disposition: {scope}"));
            continue;
        }
        if !upstream_only.iter().any(|path| scope_matches(scope, path)) {
            failures.push(format!("stale upstream disposition: {scope}"));
        }
    }
    let mut unowned = 0;
    for path in &upstream_only {
        if !oracle_upstream_path_is_deferred(
            path,
            upstream,
            &upstream_only,
            deferred,
            known,
            &claimed,
        ) {
            unowned += 1;
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
    for (name, entry) in known {
        if !entry["owner"].as_str().is_some_and(|owner| owner.starts_with('#'))
            || entry["docs"].as_str().is_none_or(str::is_empty)
            || !upstream.contains_key(&format!("type:{name}"))
        {
            failures.push(format!("upstream type census lacks owner or docs: {name}"));
        }
    }
    let unknown_types = upstream
        .keys()
        .filter_map(|path| path.strip_prefix("type:"))
        .filter(|name| !known.contains_key(*name))
        .collect::<Vec<_>>();
    if !unknown_types.is_empty() {
        failures.push(format!(
            "unknown upstream types: {}",
            unknown_types.join(", ")
        ));
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
    Ok((upstream_only.len(), local_only.len(), changed.len(), unowned))
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
    assert_eq!(
        coverage["known_upstream_types"]
            .as_object()
            .context("known_upstream_types")?
            .len(),
        113,
        "live fixture coverage census changed"
    );
    let upstream: OracleMap<String, Value> =
        serde_json::from_value(read_oracle_json(root.join("upstream/schema-surface.json"))?)?;
    let database = TestDatabase::new_migrated().await?;
    seed_oracle_fixture(&database, &load_oracle_seed(&root, &manifest)?).await?;
    let introspection = post_graphql(database.app_state(), ORACLE_INTROSPECTION, json!({})).await?;
    let local = oracle_schema_surface(&introspection)?;
    let summary = apply_oracle_coverage(&upstream, &local, &coverage)?;
    assert_eq!(summary.3, 0, "live fixture surface has unowned paths");
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

#[tokio::test]
async fn graphql_oracle_seed_accepts_a_refreshed_fixture_without_test_edits() -> Result<()> {
    let fixture = read_oracle_json(
        oracle_root()
            .parent()
            .context("oracle fixture parent")?
            .join("alternate-seed.json"),
    )?;
    let database = TestDatabase::new_migrated().await?;
    let seed = oracle_seed_from_values(
        &fixture["point_response"],
        &fixture["provenance"],
        &fixture["seed"],
    )?;
    seed_oracle_fixture(&database, &seed).await?;
    let domain = &fixture["point_response"]["data"]["domain"];
    let query = oracle_fs::read_to_string(
        oracle_root().join("entities/domain/entity-by-id/query.graphql"),
    )?;
    let actual = post_graphql_allow_errors(
        database.app_state(),
        &query,
        json!({"id": domain["id"], "block": {"number": fixture["provenance"]["block_number"]}}),
    )
    .await?;
    assert_eq!(actual, fixture["point_response"]);
    let filter_query = oracle_fs::read_to_string(
        oracle_root().join("entities/domain/filters/name-eq/query.graphql"),
    )?;
    let filtered = post_graphql_allow_errors(
        database.app_state(),
        &filter_query,
        json!({"name": domain["name"], "block": {"number": seed.block}}),
    )
    .await?;
    assert_eq!(
        filtered,
        json!({"data": {"_meta": fixture["point_response"]["data"]["_meta"], "domains": [domain]}})
    );
    database.cleanup().await
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
        json!({"claimed_paths":[null],"schema_signature_differences":[],"upstream_only":[{"scope":"type:Future","status":"deferred","owner":"#1","docs":"x"}],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}}),
    ] {
        assert!(apply_oracle_coverage(&upstream, &local, &coverage).is_err());
    }
    let duplicate_claims = json!({"claimed_paths":["type:Future","type:Future"],"schema_signature_differences":[],"upstream_only":[],"local_extensions":[],"known_upstream_types":{"Future":{"owner":"#1","docs":"x"}}});
    assert!(apply_oracle_coverage(&upstream, &upstream, &duplicate_claims).is_err());
}

#[test]
fn graphql_oracle_census_owns_wholly_deferred_type_surfaces() {
    let upstream = OracleMap::from([
        ("type:Future".into(), json!({"kind":"OBJECT"})),
        ("field:Future.id".into(), json!({"type":"ID!"})),
        ("type:Later".into(), json!({"kind":"INPUT_OBJECT"})),
        ("input:Later.id".into(), json!({"type":"ID"})),
    ]);
    let coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [],
        "local_extensions": [],
        "known_upstream_types": {
            "Future": {"owner":"#1", "docs":"x"},
            "Later": {"owner":"#2", "docs":"x"}
        }
    });
    assert!(apply_oracle_coverage(&upstream, &OracleMap::new(), &coverage).is_ok());
    assert_oracle_field_ownership_rules();
    assert_oracle_argument_ownership_rules();
    assert_oracle_enum_value_ownership_rules();
    assert_oracle_exact_input_ownership_rules();
}

fn assert_oracle_field_ownership_rules() {
    let query = ("type:Query".into(), json!({"kind":"OBJECT"}));
    let account = ("type:Account".into(), json!({"kind":"OBJECT"}));
    let future = ("type:Future".into(), json!({"kind":"OBJECT"}));
    let future_id = ("field:Future.id".into(), json!({"type":"ID!"}));
    let account_domains = (
        "field:Account.domains".into(),
        json!({"type":"[Future!]!"}),
    );
    let query_future = (
        "field:Query.future".into(),
        json!({"type":"Future"}),
    );
    let coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [{"scope":"field:Account.domains", "status":"deferred", "owner":"#1", "docs":"x"}],
        "local_extensions": [],
        "known_upstream_types": {
            "Account": {"owner":"#1", "docs":"x"},
            "Future": {"owner":"#2", "docs":"x"},
            "Query": {"owner":"#3", "docs":"x"}
        }
    });
    let upstream = OracleMap::from([
        query.clone(),
        account.clone(),
        future,
        future_id,
        account_domains,
        query_future,
    ]);
    let local = OracleMap::from([query, account]);
    assert!(apply_oracle_coverage(&upstream, &local, &coverage).is_ok());

    let claimed_return_upstream = OracleMap::from([
        ("type:Query".into(), json!({"kind":"OBJECT"})),
        ("type:Domain".into(), json!({"kind":"OBJECT"})),
        ("field:Domain.id".into(), json!({"type":"ID!"})),
        (
            "field:Query.futureDomains".into(),
            json!({"type":"[Domain!]!"}),
        ),
    ]);
    let claimed_return_local = OracleMap::from([
        ("type:Query".into(), json!({"kind":"OBJECT"})),
        ("type:Domain".into(), json!({"kind":"OBJECT"})),
        ("field:Domain.id".into(), json!({"type":"ID!"})),
    ]);
    let claimed_return_coverage = json!({
        "claimed_paths": ["type:Domain"],
        "schema_signature_differences": [],
        "upstream_only": [],
        "local_extensions": [],
        "known_upstream_types": {
            "Domain": {"owner":"#1", "docs":"x"},
            "Query": {"owner":"#2", "docs":"x"}
        }
    });
    let claimed_return_error = apply_oracle_coverage(
        &claimed_return_upstream,
        &claimed_return_local,
        &claimed_return_coverage,
    )
    .expect_err("claimed return type auto-owned an upstream-only Query field")
    .to_string();
    assert!(claimed_return_error
        .contains("unowned upstream-only path: field:Query.futureDomains"));

    let locally_served_return_coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [],
        "local_extensions": [],
        "known_upstream_types": {
            "Domain": {"owner":"#1", "docs":"x"},
            "Query": {"owner":"#2", "docs":"x"}
        }
    });
    let locally_served_return_error = apply_oracle_coverage(
        &claimed_return_upstream,
        &claimed_return_local,
        &locally_served_return_coverage,
    )
    .expect_err("locally served return type auto-owned an upstream-only Query field")
    .to_string();
    assert!(locally_served_return_error
        .contains("unowned upstream-only path: field:Query.futureDomains"));

    let claimed_domain = OracleMap::from([
        ("type:Domain".into(), json!({"kind":"OBJECT"})),
        ("field:Domain.future".into(), json!({"type":"String"})),
    ]);
    let claimed_local = OracleMap::from([("type:Domain".into(), json!({"kind":"OBJECT"}))]);
    let claimed_coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [],
        "local_extensions": [],
        "known_upstream_types": {"Domain": {"owner":"#1", "docs":"x"}}
    });
    assert!(apply_oracle_coverage(&claimed_domain, &claimed_local, &claimed_coverage).is_err());
    let type_wide_coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [{"scope":"type:Domain", "status":"deferred", "owner":"#1", "docs":"x"}],
        "local_extensions": [],
        "known_upstream_types": {"Domain": {"owner":"#1", "docs":"x"}}
    });
    assert!(
        apply_oracle_coverage(&claimed_domain, &claimed_local, &type_wide_coverage).is_err()
    );
}

#[tokio::test]
async fn graphql_oracle_claims_deny_and_matches_the_default_policy() -> Result<()> {
    let root = oracle_root();
    let coverage = read_oracle_json(root.join("coverage.json"))?;
    assert!(
        coverage["claimed_paths"]
            .as_array()
            .context("claimed_paths")?
            .iter()
            .any(|path| path == "enum:_SubgraphErrorPolicy_.deny"),
        "deny is not an exact claimed path"
    );
    let manifest = verify_oracle_integrity()?;
    let database = TestDatabase::new_migrated().await?;
    seed_oracle_fixture(&database, &load_oracle_seed(&root, &manifest)?).await?;
    let variables = read_oracle_json(
        root.join("entities/domain/entity-by-id/variables.json"),
    )?;
    let default_policy = post_graphql(
        database.app_state(),
        r#"query OracleDefaultPolicy($id: ID!, $block: Block_height!) {
            domain(id: $id, block: $block) { id }
        }"#,
        variables.clone(),
    )
    .await?;
    let deny_policy = post_graphql(
        database.app_state(),
        r#"query OracleDenyPolicy($id: ID!, $block: Block_height!) {
            domain(id: $id, block: $block, subgraphError: deny) { id }
        }"#,
        variables,
    )
    .await?;
    assert_eq!(deny_policy, default_policy);
    database.cleanup().await
}

fn assert_oracle_argument_ownership_rules() {
    let query = ("type:Query".into(), json!({"kind":"OBJECT"}));
    let future = ("type:Future".into(), json!({"kind":"OBJECT"}));
    let future_id = ("field:Future.id".into(), json!({"type":"ID!"}));
    let field = ("field:Query.future".into(), json!({"type":"[Future!]!"}));
    let argument = (
        "arg:Query.future(first)".into(),
        json!({"type":"Int", "default":null, "deprecated":false}),
    );
    let coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [],
        "local_extensions": [],
        "known_upstream_types": {
            "Future": {"owner":"#1", "docs":"x"},
            "Query": {"owner":"#2", "docs":"x"}
        }
    });
    let upstream = OracleMap::from([query.clone(), future, future_id, field, argument]);
    let local = OracleMap::from([query]);
    assert!(apply_oracle_coverage(&upstream, &local, &coverage).is_ok());

    for (parent, argument) in [
        ("field:Query.domain", "arg:Query.domain(extra)"),
        ("field:Domain.name", "arg:Domain.name(extra)"),
    ] {
        let parent_type = parent
            .strip_prefix("field:")
            .and_then(|path| path.split_once('.'))
            .map(|(name, _)| name)
            .unwrap();
        let upstream = OracleMap::from([
            (format!("type:{parent_type}"), json!({"kind":"OBJECT"})),
            (parent.into(), json!({"type":"String"})),
            (
                argument.into(),
                json!({"type":"String", "default":null, "deprecated":false}),
            ),
        ]);
        let local = OracleMap::from([
            (format!("type:{parent_type}"), json!({"kind":"OBJECT"})),
            (parent.into(), json!({"type":"String"})),
        ]);
        let coverage = json!({
            "claimed_paths": [parent],
            "schema_signature_differences": [],
            "upstream_only": [],
            "local_extensions": [],
            "known_upstream_types": {parent_type: {"owner":"#1", "docs":"x"}}
        });
        assert!(apply_oracle_coverage(&upstream, &local, &coverage).is_err());
    }
}

fn assert_oracle_enum_value_ownership_rules() {
    let enum_type = ("type:Domain_orderBy".into(), json!({"kind":"ENUM"}));
    let new_value = (
        "enum:Domain_orderBy.future".into(),
        json!({"deprecated":false}),
    );
    let coverage = json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": [],
        "local_extensions": [],
        "known_upstream_types": {"Domain_orderBy": {"owner":"#670/T3", "docs":"x"}}
    });
    assert!(apply_oracle_coverage(
        &OracleMap::from([enum_type.clone(), new_value]),
        &OracleMap::from([enum_type]),
        &coverage,
    ).is_err());

    let uncensused = apply_oracle_coverage(
        &OracleMap::from([
            ("type:Other_orderBy".into(), json!({"kind":"ENUM"})),
            (
                "enum:Other_orderBy.future".into(),
                json!({"deprecated":false}),
            ),
        ]),
        &OracleMap::from([(
            "type:Other_orderBy".into(),
            json!({"kind":"ENUM"}),
        )]),
        &json!({
            "claimed_paths": [],
            "schema_signature_differences": [],
            "upstream_only": [],
            "local_extensions": [],
            "known_upstream_types": {}
        }),
    )
    .unwrap_err()
    .to_string();
    assert!(uncensused.contains("unowned upstream-only path: enum:Other_orderBy.future"));
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
