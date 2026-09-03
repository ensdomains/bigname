fn partial_domain_order_surfaces() -> (OracleMap<String, Value>, OracleMap<String, Value>) {
    let ty = ("type:Domain_orderBy".into(), json!({"kind":"ENUM"}));
    let id = ("enum:Domain_orderBy.id".into(), json!({"deprecated":false}));
    let label = (
        "enum:Domain_orderBy.labelName".into(),
        json!({"deprecated":false}),
    );
    (
        OracleMap::from([ty.clone(), id.clone(), label]),
        OracleMap::from([ty, id]),
    )
}

fn exact_enum_coverage(scopes: &[&str]) -> Value {
    json!({
        "claimed_paths": ["type:Domain_orderBy", "enum:Domain_orderBy.id"],
        "schema_signature_differences": [],
        "upstream_only": scopes.iter().map(|scope| json!({
            "scope": scope,
            "status": "deferred",
            "owner": "#670/T3",
            "docs": "docs/consumer-capabilities.md#graphql-compatibility"
        })).collect::<Vec<_>>(),
        "local_extensions": [],
        "known_upstream_types": {
            "Domain_orderBy": {
                "owner": "#670/T3",
                "docs": "docs/consumer-capabilities.md#graphql-compatibility"
            }
        }
    })
}

#[test]
fn graphql_oracle_exact_enum_scope_owns_only_one_value() {
    let (upstream, local) = partial_domain_order_surfaces();
    apply_oracle_coverage(
        &upstream,
        &local,
        &exact_enum_coverage(&["enum:Domain_orderBy.labelName"]),
    )
    .expect("an exact enum scope must own its upstream value");
}

#[test]
fn graphql_oracle_partial_enum_requires_exact_value_ownership() {
    let (upstream, local) = partial_domain_order_surfaces();
    let error = apply_oracle_coverage(&upstream, &local, &exact_enum_coverage(&[]))
        .expect_err("partial enum type inheritance unexpectedly owned a missing value")
        .to_string();
    assert!(error.contains("unowned upstream-only path: enum:Domain_orderBy.labelName"));
}

#[test]
fn graphql_oracle_rejects_malformed_wildcard_and_stale_enum_scopes() {
    let (upstream, local) = partial_domain_order_surfaces();
    for (scope, expected) in [
        ("enum:Domain_orderBy.*", "overbroad disposition"),
        ("enum:Domain_orderBy", "overbroad disposition"),
        ("enum:Domain_orderBy.missing", "stale upstream disposition"),
        ("enum:.labelName", "overbroad disposition"),
        ("enum:Domain_orderBy.labelName.extra", "overbroad disposition"),
    ] {
        let error = apply_oracle_coverage(&upstream, &local, &exact_enum_coverage(&[scope]))
            .expect_err("invalid enum scope unexpectedly passed")
            .to_string();
        assert!(error.contains(expected), "{scope}: {error}");
    }
}
