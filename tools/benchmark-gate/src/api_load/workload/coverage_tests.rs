use super::*;

fn corpus_with_address_names() -> Corpus {
    Corpus {
        names: vec![("ens".to_owned(), "one.eth".to_owned())],
        address_names: (0..4)
            .map(|index| {
                (
                    format!("0x{index:040x}"),
                    format!("name-{index}.eth"),
                    "ens".to_owned(),
                    "token_holder".to_owned(),
                )
            })
            .collect(),
        parents: Vec::new(),
        permission_subjects: Vec::new(),
        primary_names: Vec::new(),
        resolvers: Vec::new(),
        namespaces: vec!["ens".to_owned()],
        names_by_namespace: [("ens".to_owned(), 1)].into_iter().collect(),
        parents_by_namespace: Default::default(),
        resolver_manifest_coverage: Vec::new(),
    }
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
}

#[test]
fn reported_api_base_url_omits_userinfo() {
    let reported = report_base_url("https://operator:secret@example.test/api").unwrap();

    assert_eq!(reported, "https://example.test/api/");
    assert!(!reported.contains("operator"));
    assert!(!reported.contains("secret"));
}
