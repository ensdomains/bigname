use super::*;

fn corpus_with_address_names() -> Corpus {
    Corpus {
        names: (0..24)
            .map(|index| ("ens".to_owned(), format!("name-{index}.eth")))
            .collect(),
        address_names: (0..24)
            .map(|index| {
                (
                    format!("0x{index:040x}"),
                    format!("name-{index}.eth"),
                    "ens".to_owned(),
                    "token_holder".to_owned(),
                )
            })
            .collect(),
        parents: (0..24)
            .map(|index| ("ens".to_owned(), format!("parent-{index}.eth")))
            .collect(),
        permission_subjects: (0..4)
            .map(|index| super::super::corpus::PermissionTarget {
                address: format!("0x{index:040x}"),
                registration_id: format!("00000000-0000-0000-0000-{index:012}"),
                retained_registration: index % 2 == 0,
                namespace: "ens".to_owned(),
                name: format!("name-{index}.eth"),
            })
            .collect(),
        primary_names: Vec::new(),
        resolvers: vec![ResolverTarget {
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            resolver_address: "0x00000000000000000000000000000000000000ff".to_owned(),
        }],
        namespaces: vec!["ens".to_owned()],
        names_by_namespace: [("ens".to_owned(), 24)].into_iter().collect(),
        parents_by_namespace: [("ens".to_owned(), 24)].into_iter().collect(),
        resolver_manifest_coverage: Vec::new(),
    }
}

fn assert_query_values(endpoint: &str, parameter: &str, expected: &[&str]) {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let corpus = corpus_with_address_names();
    let requests = request_variants(&base, &corpus, endpoint).unwrap();
    let actual = requests
        .iter()
        .flat_map(|request| request.url.query_pairs())
        .filter(|(key, _)| key == parameter)
        .map(|(_, value)| value.into_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        actual,
        expected.iter().map(|value| (*value).to_owned()).collect(),
        "{endpoint} must rotate every documented {parameter} value"
    );
}

#[test]
fn records_rotate_inventory_expansion() {
    assert_query_values("records", "include", &["inventory"]);
}

#[test]
fn subnames_rotate_counts_expansion() {
    assert_query_values("subnames", "include", &["counts"]);
}

#[test]
fn permissions_rotate_lineage_expansion() {
    assert_query_values("permissions", "include", &["lineage"]);
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let requests = request_variants(&base, &corpus_with_address_names(), "permissions").unwrap();
    assert!(requests.iter().any(|request| {
        request
            .url
            .query_pairs()
            .any(|(key, value)| key == "include" && value == "lineage")
    }));
}

#[test]
fn address_names_rotate_documented_expansion_values() {
    assert_query_values("address_names", "include", &["role_summary"]);
    assert_query_values("address_names", "dedupe", &["name", "registration"]);
    assert_query_values(
        "address_names",
        "sort",
        &["name", "expires_at", "registered_at"],
    );
    assert_query_values("address_names", "order", &["asc", "desc"]);
}

#[test]
fn resolvers_rotate_every_overview_section() {
    assert_query_values(
        "resolver",
        "include",
        &["nodes", "aliases", "roles", "events"],
    );
}

#[test]
fn address_name_variants_cover_role_summary_and_registration_dedupe() {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let requests = request_variants(&base, &corpus_with_address_names(), "address_names").unwrap();
    let combinations = requests
        .iter()
        .map(|request| {
            let pairs = request.url.query_pairs().collect::<Vec<_>>();
            (
                pairs
                    .iter()
                    .any(|(key, value)| key == "include" && value == "role_summary"),
                pairs
                    .iter()
                    .any(|(key, value)| key == "dedupe" && value == "registration"),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        combinations,
        [(false, false), (false, true), (true, false), (true, true)]
            .into_iter()
            .collect(),
        "address-name traffic must retain the base query and cover both enrichments"
    );

    let variant_sort_pairs = requests
        .iter()
        .filter(|request| request.url.query_pairs().any(|(key, _)| key == "dedupe"))
        .map(|request| {
            let pairs = request.url.query_pairs().collect::<Vec<_>>();
            let include = pairs
                .iter()
                .any(|(key, value)| key == "include" && value == "role_summary");
            let dedupe = pairs
                .iter()
                .find(|(key, _)| key == "dedupe")
                .expect("every address-name request must select a documented dedupe mode")
                .1
                .to_string();
            let sort = pairs
                .iter()
                .find(|(key, _)| key == "sort")
                .unwrap()
                .1
                .to_string();
            ((include, dedupe), sort)
        })
        .collect::<std::collections::BTreeSet<_>>();
    let expected = [
        (false, "name".to_owned()),
        (true, "name".to_owned()),
        (false, "registration".to_owned()),
        (true, "registration".to_owned()),
    ]
    .into_iter()
    .flat_map(|variant| {
        [
            "name".to_owned(),
            "expires_at".to_owned(),
            "registered_at".to_owned(),
        ]
        .into_iter()
        .map(move |sort| (variant.clone(), sort))
    })
    .collect();
    assert_eq!(
        variant_sort_pairs, expected,
        "every address-name enrichment variant must cover every documented sort path"
    );
}

#[test]
fn every_documented_default_form_is_present() {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let corpus = corpus_with_address_names();

    for endpoint in [
        "status",
        "name",
        "records",
        "subnames",
        "name_history",
        "address_names",
        "address_history",
        "events",
        "resolver",
        "namespace",
    ] {
        let requests = request_variants(&base, &corpus, endpoint).unwrap();
        assert!(
            requests.iter().any(|request| request.url.query().is_none()),
            "{endpoint} must include its documented default request form"
        );
    }

    let permissions = request_variants(&base, &corpus, "permissions").unwrap();
    assert!(
        permissions.iter().any(|request| {
            let query = request.url.query_pairs().collect::<Vec<_>>();
            query.len() == 1
                && matches!(query[0].0.as_ref(), "address" | "name" | "registration_id")
        }),
        "permissions must include a dimension-only default request"
    );

    let search = request_variants(&base, &corpus, "search").unwrap();
    assert!(
        search.iter().any(|request| {
            let query = request.url.query_pairs().collect::<Vec<_>>();
            query.len() == 1 && query[0].0 == "q"
        }),
        "search must include its q-only default request"
    );

    let lookup = request_variants(&base, &corpus, "lookup").unwrap();
    assert!(
        lookup.iter().any(|request| {
            request.body.as_ref().is_some_and(|body| {
                body.get("profile").is_none()
                    && body["inputs"].as_array().is_some_and(|inputs| {
                        inputs.iter().any(|input| {
                            input.get("address").is_some()
                                && input.get("coin_type").is_none()
                                && input.get("relation").is_none()
                                && input.get("page_size").is_none()
                        })
                    })
            })
        }),
        "lookup must include its default body and reverse input without profile, coin_type, relation, or page_size"
    );
    assert!(
        lookup.iter().any(|request| {
            request.body.as_ref().is_some_and(|body| {
                body.get("namespace").is_none()
                    && body["inputs"].as_array().is_some_and(|inputs| {
                        inputs.iter().any(|input| input.get("name").is_some())
                    })
            })
        }),
        "lookup must include default forward inputs without namespace"
    );
}

#[test]
fn collection_pages_cover_omitted_default_and_documented_maximum() {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let corpus = corpus_with_address_names();
    for endpoint in [
        "subnames",
        "name_history",
        "permissions",
        "address_names",
        "address_history",
        "search",
        "events",
        "resolver",
    ] {
        let requests = request_variants(&base, &corpus, endpoint).unwrap();
        assert!(
            requests
                .iter()
                .any(|request| !request.url.query_pairs().any(|(key, _)| key == "page_size")),
            "{endpoint} must exercise the omitted server-default page size"
        );
        assert!(
            requests.iter().any(|request| request
                .url
                .query_pairs()
                .any(|(key, value)| key == "page_size" && value == "200")),
            "{endpoint} must exercise the documented 200-row maximum page"
        );
    }
}

#[test]
fn permissions_cover_every_documented_read_dimension() {
    let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
    let requests = request_variants(&base, &corpus_with_address_names(), "permissions").unwrap();
    let keys = requests
        .iter()
        .flat_map(|request| request.url.query_pairs().map(|(key, _)| key.into_owned()))
        .collect::<std::collections::BTreeSet<_>>();

    for required in ["address", "name", "registration_id"] {
        assert!(
            keys.contains(required),
            "permissions must rotate {required} reads"
        );
    }
    assert!(requests.iter().any(|request| {
        request.required_permission_audit_evidence
            && request
                .url
                .query_pairs()
                .any(|(key, _)| key == "registration_id")
    }));
    assert!(requests.iter().any(|request| {
        !request.required_permission_audit_evidence
            && request
                .url
                .query_pairs()
                .any(|(key, _)| key == "registration_id")
    }));
}

#[test]
fn reported_api_base_url_omits_userinfo() {
    let reported = report_base_url("https://operator:secret@example.test/api").unwrap();

    assert_eq!(reported, "https://example.test/api/");
    assert!(!reported.contains("operator"));
    assert!(!reported.contains("secret"));
}

async fn rendered_request_failure(value: &str) -> String {
    match normalized_base_url(value) {
        Err(error) => format!("{error:#}"),
        Ok(base) => {
            let request = get(&base, &["unreachable"], &[]).unwrap();
            let error = crate::api_load::send(&reqwest::Client::new(), &request)
                .await
                .expect_err("the test endpoint must refuse the connection");
            format!("{error:#}")
        }
    }
}

#[tokio::test]
async fn invalid_utf8_api_userinfo_is_refused_before_request_construction() {
    for (url, secret) in [
        ("http://%FF:SECRET@127.0.0.1:1", "SECRET"),
        ("http://operator:%FF@127.0.0.1:1", "%FF"),
    ] {
        let rendered = rendered_request_failure(url).await;

        assert!(
            rendered.contains("userinfo must percent-decode to valid UTF-8"),
            "invalid userinfo was not refused by the URL validator: {rendered}"
        );
        assert!(
            !rendered.contains(secret),
            "invalid userinfo reached a reportable transport error: {rendered}"
        );
    }
}

#[tokio::test]
async fn ordinary_api_userinfo_is_stripped_from_transport_errors() {
    let rendered = rendered_request_failure("http://operator:ordinary-secret@127.0.0.1:1").await;

    assert!(rendered.contains("API benchmark request failed"));
    assert!(!rendered.contains("operator"));
    assert!(!rendered.contains("ordinary-secret"));
}
