fn partial_domain_filter_surfaces() -> (OracleMap<String, Value>, OracleMap<String, Value>) {
    let upstream = OracleMap::from([
        ("type:Domain_filter".into(), json!({"kind":"INPUT_OBJECT"})),
        ("input:Domain_filter.id".into(), json!({"type":"ID"})),
        ("input:Domain_filter.id_not".into(), json!({"type":"ID"})),
        ("input:Domain_filter.owner".into(), json!({"type":"String"})),
    ]);
    let local = OracleMap::from([
        ("type:Domain_filter".into(), json!({"kind":"INPUT_OBJECT"})),
        ("input:Domain_filter.id".into(), json!({"type":"ID"})),
    ]);
    (upstream, local)
}

fn exact_input_coverage(scopes: &[&str]) -> Value {
    json!({
        "claimed_paths": [],
        "schema_signature_differences": [],
        "upstream_only": scopes.iter().map(|scope| json!({
            "scope": scope,
            "status": "deferred",
            "owner": "#670/T3",
            "docs": "docs/consumer-capabilities.md#graphql-compatibility"
        })).collect::<Vec<_>>(),
        "local_extensions": [],
        "known_upstream_types": {
            "Domain_filter": {
                "owner": "#670/T3",
                "docs": "docs/consumer-capabilities.md#graphql-compatibility"
            }
        }
    })
}

#[test]
fn graphql_oracle_exact_input_scope_owns_one_member() {
    let (upstream, mut local) = partial_domain_filter_surfaces();
    local.insert(
        "input:Domain_filter.owner".into(),
        json!({"type":"String"}),
    );
    let coverage = exact_input_coverage(&["input:Domain_filter.id_not"]);
    apply_oracle_coverage(&upstream, &local, &coverage)
        .expect("exact input scope must own one upstream-only member");
}

#[test]
fn graphql_oracle_input_scope_does_not_own_siblings() {
    let (upstream, local) = partial_domain_filter_surfaces();
    let error = apply_oracle_coverage(
        &upstream,
        &local,
        &exact_input_coverage(&["input:Domain_filter.id_not"]),
    )
    .expect_err("an exact input scope unexpectedly owned its sibling")
    .to_string();
    assert!(error.contains("unowned upstream-only path: input:Domain_filter.owner"));
}

#[test]
fn graphql_oracle_rejects_stale_duplicate_unknown_and_wildcard_input_scopes() {
    let (upstream, local) = partial_domain_filter_surfaces();
    for (scopes, expected) in [
        (
            vec![
                "input:Domain_filter.id_not",
                "input:Domain_filter.owner",
                "input:Domain_filter.id",
            ],
            "stale upstream disposition: input:Domain_filter.id",
        ),
        (
            vec![
                "input:Domain_filter.id_not",
                "input:Domain_filter.owner",
                "input:Domain_filter.id_not",
            ],
            "duplicate conflicting disposition: input:Domain_filter.id_not",
        ),
        (
            vec![
                "input:Domain_filter.id_not",
                "input:Domain_filter.owner",
                "input:Domain_filter.missing",
            ],
            "stale upstream disposition: input:Domain_filter.missing",
        ),
        (
            vec![
                "input:Domain_filter.id_not",
                "input:Domain_filter.owner",
                "input:Domain_filter.*",
            ],
            "overbroad disposition: input:Domain_filter.*",
        ),
    ] {
        let error = apply_oracle_coverage(&upstream, &local, &exact_input_coverage(&scopes))
            .expect_err("invalid exact-input scope unexpectedly passed")
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn graphql_oracle_rejects_type_scope_for_a_partial_input_object() {
    let (upstream, local) = partial_domain_filter_surfaces();
    let error = apply_oracle_coverage(
        &upstream,
        &local,
        &exact_input_coverage(&["type:Domain_filter"]),
    )
    .expect_err("a partial input object accepted whole-type ownership")
    .to_string();
    assert!(error.contains("overbroad disposition: type:Domain_filter"));
}

fn assert_oracle_exact_input_ownership_rules() {
    graphql_oracle_exact_input_scope_owns_one_member();
    graphql_oracle_input_scope_does_not_own_siblings();
    graphql_oracle_rejects_stale_duplicate_unknown_and_wildcard_input_scopes();
    graphql_oracle_rejects_type_scope_for_a_partial_input_object();
}
