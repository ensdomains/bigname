use anyhow::{Result, bail, ensure};
use reqwest::{Method, Url};
use serde_json::Value;

use super::corpus::Corpus;

mod base_url;
mod defaults;
mod resolver;
pub(in crate::api_load) use base_url::normalized_base_url;
pub(crate) use base_url::report_base_url;
pub(in crate::api_load) use resolver::{ResolverTarget, endpoint_requests};

#[derive(Clone, Debug)]
pub(super) struct RequestSpec {
    pub(super) method: Method,
    pub(super) url: Url,
    pub(super) body: Option<Value>,
    pub(super) known_good_evidence: bool,
    pub(super) required_permission_audit_evidence: bool,
}

const RESOLVER_INCLUDE_VARIANTS: [Option<&str>; 5] = [
    None,
    Some("nodes"),
    Some("aliases"),
    Some("roles"),
    Some("events"),
];

pub(super) fn request_variants(
    base: &Url,
    corpus: &Corpus,
    endpoint: &str,
) -> Result<Vec<RequestSpec>> {
    let mut requests = Vec::new();
    match endpoint {
        "lookup" => defaults::lookup_requests(base, corpus, &mut requests)?,
        "status" => requests.push(get(base, &["v2", "status"], &[])?),
        "name" => defaults::exact_name_requests(base, corpus, &mut requests)?,
        "records" => defaults::record_requests(base, corpus, &mut requests)?,
        "subnames" => defaults::subname_requests(base, corpus, &mut requests)?,
        "name_history" => defaults::name_history_requests(base, corpus, &mut requests)?,
        "permissions" => defaults::permission_requests(base, corpus, &mut requests)?,
        "address_names" => defaults::address_name_requests(base, corpus, &mut requests)?,
        "primary_name" => primary_name_requests(base, corpus, &mut requests)?,
        "address_history" => defaults::address_history_requests(base, corpus, &mut requests)?,
        "search" => defaults::search_requests(base, corpus, &mut requests)?,
        "events" => defaults::event_requests(base, corpus, &mut requests)?,
        "resolver" => {
            ensure!(
                !corpus.resolvers.is_empty(),
                "resolver endpoint has no real resolver corpus"
            );
            for (index, target) in corpus.resolvers.iter().enumerate() {
                for (variant, include) in RESOLVER_INCLUDE_VARIANTS.into_iter().enumerate() {
                    let request_index = index * RESOLVER_INCLUDE_VARIANTS.len() + variant;
                    let mut query = Vec::new();
                    if let Some(include) = include {
                        query.push(("include", include));
                        query.push((
                            "page_size",
                            defaults::parameterized_page_size(request_index),
                        ));
                    }
                    requests.push(get(
                        base,
                        &[
                            "v2",
                            "resolvers",
                            numeric_chain_id(&target.chain_id)?,
                            &target.resolver_address,
                        ],
                        &query,
                    )?);
                }
            }
        }
        "namespace" => {
            for namespace in &corpus.namespaces {
                requests.push(get(base, &["v2", "namespaces", namespace], &[])?);
            }
        }
        unknown => bail!("unknown endpoint budget {unknown:?}"),
    }
    Ok(requests)
}

fn primary_name_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    let primary_names = if corpus.primary_names.is_empty() {
        corpus
            .address_names
            .iter()
            .map(|sample| (sample.0.as_str(), "60", sample.2.as_str()))
            .collect::<Vec<_>>()
    } else {
        corpus
            .primary_names
            .iter()
            .map(|sample| (sample.0.as_str(), sample.1.as_str(), sample.2.as_str()))
            .collect::<Vec<_>>()
    };
    let proven = !corpus.primary_names.is_empty();
    for (address, coin_type, namespace) in primary_names {
        let mut request = get(
            base,
            &["v2", "addresses", address, "primary-name"],
            &[
                ("source", "indexed"),
                ("namespace", namespace),
                ("coin_type", coin_type),
            ],
        )?;
        request.known_good_evidence = proven;
        requests.push(request);
    }
    Ok(())
}

pub(super) fn get(base: &Url, segments: &[&str], query: &[(&str, &str)]) -> Result<RequestSpec> {
    let mut url = with_path(base, segments)?;
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query.iter().copied());
    }
    Ok(RequestSpec {
        method: Method::GET,
        url,
        body: None,
        known_good_evidence: true,
        required_permission_audit_evidence: false,
    })
}

pub(super) fn post(base: &Url, segments: &[&str], body: Value) -> Result<RequestSpec> {
    Ok(RequestSpec {
        method: Method::POST,
        url: with_path(base, segments)?,
        body: Some(body),
        known_good_evidence: true,
        required_permission_audit_evidence: false,
    })
}

fn with_path(base: &Url, segments: &[&str]) -> Result<Url> {
    let mut url = base.clone();
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("API base URL cannot hold path segments"))?
        .pop_if_empty()
        .extend(segments);
    Ok(url)
}

fn lookup_batch_size(bucket: usize, corpus_len: usize) -> usize {
    let requested = match bucket {
        0..=34 => 1,
        35..=44 => 10,
        45..=47 => 100,
        48 => 250,
        _ => 1_000,
    };
    requested.min(corpus_len)
}

fn public_relation(stored: &str) -> Result<&'static str> {
    match stored {
        "token_holder" => Ok("owner"),
        "effective_controller" => Ok("manager"),
        "registrant" => Ok("registrant"),
        other => bail!("address corpus contains unsupported stored relation {other:?}"),
    }
}

fn history_scope(index: usize) -> &'static str {
    ["name", "registration", "both"][index % 3]
}

fn search_term(name: &str) -> String {
    let label = name.split('.').next().unwrap_or(name);
    label.chars().take(3).collect::<String>().to_lowercase()
}

pub(super) fn numeric_chain_id(chain: &str) -> Result<&'static str> {
    match chain {
        "ethereum-mainnet" => Ok("1"),
        "ethereum-sepolia" => Ok("11155111"),
        "base-mainnet" => Ok("8453"),
        "base-sepolia" => Ok("84532"),
        _ => bail!("resolver corpus contains unsupported chain slug {chain:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus_with_ens_base_eth_name() -> Corpus {
        Corpus {
            names: vec![("ens".to_owned(), "ordinary.base.eth".to_owned())],
            address_names: vec![(
                "0x0000000000000000000000000000000000000001".to_owned(),
                "ordinary.base.eth".to_owned(),
                "ens".to_owned(),
                "token_holder".to_owned(),
            )],
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
    fn path_parameters_are_encoded_as_segments() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let request = get(&base, &["v2", "names", "name with space.eth"], &[]).unwrap();
        assert_eq!(
            request.url.as_str(),
            "http://127.0.0.1:3000/v2/names/name%20with%20space.eth"
        );
    }

    #[test]
    fn lookup_batches_cover_small_medium_and_ceiling_sizes() {
        let sizes = (0..50)
            .map(|bucket| lookup_batch_size(bucket, 10_000))
            .collect::<Vec<_>>();
        assert_eq!(sizes.iter().filter(|size| **size == 1).count(), 35);
        assert!(sizes.contains(&10));
        assert!(sizes.contains(&100));
        assert!(sizes.contains(&250));
        assert!(sizes.contains(&1_000));
    }

    #[test]
    fn stored_relations_map_to_public_filters() {
        assert_eq!(public_relation("token_holder").unwrap(), "owner");
        assert_eq!(public_relation("effective_controller").unwrap(), "manager");
        assert_eq!(public_relation("registrant").unwrap(), "registrant");
        assert!(public_relation("invented").is_err());
    }

    #[test]
    fn name_shaped_requests_bind_the_sampled_ens_namespace() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let corpus = corpus_with_ens_base_eth_name();

        for endpoint in ["name", "records", "subnames", "name_history"] {
            let requests = request_variants(&base, &corpus, endpoint).unwrap();
            assert_eq!(
                requests[0]
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "namespace")
                    .map(|(_, value)| value.into_owned()),
                Some("ens".to_owned()),
                "{endpoint} must not infer Basenames from an ENS x.base.eth sample"
            );
        }
        let mut lookup_corpus = corpus.clone();
        lookup_corpus
            .names
            .push(("ens".to_owned(), "inferable.eth".to_owned()));
        let lookup = request_variants(&base, &lookup_corpus, "lookup").unwrap();
        assert_eq!(lookup[0].body.as_ref().unwrap()["namespace"], "ens");
    }

    #[test]
    fn name_lookup_variants_cover_every_active_namespace() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let mut names = (0..50)
            .map(|index| ("basenames".to_owned(), format!("base-{index}.base.eth")))
            .collect::<Vec<_>>();
        names.extend((0..50).map(|index| ("ens".to_owned(), format!("ens-{index}.eth"))));
        let corpus = Corpus {
            names,
            address_names: vec![(
                "0x0000000000000000000000000000000000000001".to_owned(),
                "one.eth".to_owned(),
                "ens".to_owned(),
                "token_holder".to_owned(),
            )],
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: Vec::new(),
            resolvers: Vec::new(),
            namespaces: vec!["basenames".to_owned(), "ens".to_owned()],
            names_by_namespace: [("basenames".to_owned(), 50), ("ens".to_owned(), 50)]
                .into_iter()
                .collect(),
            parents_by_namespace: Default::default(),
            resolver_manifest_coverage: Vec::new(),
        };

        let namespaces = request_variants(&base, &corpus, "lookup")
            .unwrap()
            .into_iter()
            .filter_map(|request| request.body?.get("namespace")?.as_str().map(str::to_owned))
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            namespaces,
            ["basenames".to_owned(), "ens".to_owned()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn search_variants_include_bare_and_explicit_namespace_requests() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let mut corpus = corpus_with_ens_base_eth_name();
        for name in ["second.eth", "third.eth", "fourth.eth"] {
            corpus.names.push(("ens".to_owned(), name.to_owned()));
        }

        let requests = request_variants(&base, &corpus, "search").unwrap();
        let combinations = requests
            .iter()
            .filter_map(|request| {
                let pairs = request.url.query_pairs().collect::<Vec<_>>();
                let match_mode = pairs
                    .iter()
                    .find(|(key, _)| key == "match")
                    .map(|(_, value)| value.to_string())?;
                let explicit_namespace = pairs.iter().any(|(key, _)| key == "namespace");
                Some((match_mode, explicit_namespace))
            })
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            combinations,
            [
                ("contains".to_owned(), false),
                ("contains".to_owned(), true),
                ("prefix".to_owned(), false),
                ("prefix".to_owned(), true),
            ]
            .into_iter()
            .collect(),
            "bare and explicit search must each cover prefix and contains"
        );
    }

    #[test]
    fn timed_primary_name_requests_are_indexed_only() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let mut corpus = corpus_with_ens_base_eth_name();
        corpus.primary_names.push((
            "0x0000000000000000000000000000000000000001".to_owned(),
            "60".to_owned(),
            "ens".to_owned(),
        ));

        let requests = request_variants(&base, &corpus, "primary_name").unwrap();
        assert!(!requests.is_empty());
        assert!(requests.iter().all(|request| {
            request
                .url
                .query_pairs()
                .any(|(key, value)| key == "source" && value == "indexed")
        }));
        assert!(requests.iter().all(|request| request.known_good_evidence));

        corpus.primary_names.clear();
        let fallback = request_variants(&base, &corpus, "primary_name").unwrap();
        assert!(fallback.iter().all(|request| !request.known_good_evidence));
    }
}

#[cfg(test)]
#[path = "workload/coverage_tests.rs"]
mod coverage_tests;
