use anyhow::{Context, Result, bail, ensure};
use reqwest::{Method, Url};
use serde_json::{Value, json};

use super::corpus::Corpus;

#[derive(Clone, Debug)]
pub(super) struct RequestSpec {
    pub(super) method: Method,
    pub(super) url: Url,
    pub(super) body: Option<Value>,
}

pub(super) fn request_variants(
    base: &Url,
    corpus: &Corpus,
    endpoint: &str,
) -> Result<Vec<RequestSpec>> {
    let mut requests = Vec::new();
    match endpoint {
        "lookup" => lookup_requests(base, corpus, &mut requests)?,
        "status" => requests.push(get(base, &["v2", "status"], &[])?),
        "name" => {
            for (namespace, name) in &corpus.names {
                requests.push(get(
                    base,
                    &["v2", "names", name],
                    &[("source", "indexed"), ("namespace", namespace)],
                )?);
            }
        }
        "records" => {
            for (index, (namespace, name)) in corpus.names.iter().enumerate() {
                let keys = if index % 2 == 0 {
                    "addr:60"
                } else {
                    "text:avatar,text:description"
                };
                requests.push(get(
                    base,
                    &["v2", "names", name, "records"],
                    &[
                        ("source", "indexed"),
                        ("keys", keys),
                        ("namespace", namespace),
                    ],
                )?);
            }
        }
        "subnames" => {
            let parents = if corpus.parents.is_empty() {
                &corpus.names
            } else {
                &corpus.parents
            };
            for (index, (namespace, parent)) in parents.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "names", parent, "subnames"],
                    &[("page_size", page_size(index)), ("namespace", namespace)],
                )?);
            }
        }
        "name_history" => {
            for (index, (namespace, name)) in corpus.names.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "names", name, "history"],
                    &[
                        ("scope", history_scope(index)),
                        ("page_size", page_size(index)),
                        ("namespace", namespace),
                    ],
                )?);
            }
        }
        "permissions" => permission_requests(base, corpus, &mut requests)?,
        "address_names" => address_name_requests(base, corpus, &mut requests)?,
        "primary_name" => primary_name_requests(base, corpus, &mut requests)?,
        "address_history" => address_history_requests(base, corpus, &mut requests)?,
        "search" => {
            for (index, (namespace, name)) in corpus.names.iter().enumerate() {
                let query = search_term(name);
                let match_mode = if index % 2 == 0 { "prefix" } else { "contains" };
                requests.push(get(
                    base,
                    &["v2", "search"],
                    &[
                        ("q", query.as_str()),
                        ("match", match_mode),
                        ("namespace", namespace),
                        ("page_size", page_size(index)),
                    ],
                )?);
                if index % 2 == 0 {
                    let bare_match = if (index / 2) % 2 == 0 {
                        "prefix"
                    } else {
                        "contains"
                    };
                    requests.push(get(
                        base,
                        &["v2", "search"],
                        &[
                            ("q", query.as_str()),
                            ("match", bare_match),
                            ("page_size", page_size(index)),
                        ],
                    )?);
                }
            }
        }
        "events" => {
            for (index, (namespace, name)) in corpus.names.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "events"],
                    &[
                        ("name", name),
                        ("namespace", namespace),
                        ("page_size", page_size(index)),
                    ],
                )?);
            }
        }
        "resolver" => {
            ensure!(
                !corpus.resolvers.is_empty(),
                "resolver endpoint has no real resolver corpus"
            );
            for (index, (chain, resolver)) in corpus.resolvers.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "resolvers", numeric_chain_id(chain)?, resolver],
                    &[("page_size", page_size(index))],
                )?);
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

fn lookup_requests(base: &Url, corpus: &Corpus, requests: &mut Vec<RequestSpec>) -> Result<()> {
    for variant in 0..100 {
        let bucket = variant / 2;
        let name_samples = if variant % 2 == 0 {
            let namespace = &corpus.namespaces[bucket % corpus.namespaces.len()];
            corpus
                .names
                .iter()
                .filter(|(sample_namespace, _)| sample_namespace == namespace)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let corpus_len = if variant % 2 == 0 {
            name_samples.len()
        } else {
            corpus.address_names.len()
        };
        let batch_size = lookup_batch_size(bucket, corpus_len);
        let inputs = (0..batch_size)
            .map(|offset| -> Result<Value> {
                let index = (bucket * 97 + offset) % corpus_len;
                Ok(if variant % 2 == 0 {
                    json!({
                        "id": format!("name-{variant}-{offset}"),
                        "name": name_samples[index].1,
                    })
                } else {
                    json!({
                        "id": format!("address-{variant}-{offset}"),
                        "address": corpus.address_names[index].0,
                        "coin_type": 60,
                        "relation": public_relation(&corpus.address_names[index].3)?,
                        "page_size": 1 + index % 3,
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let profile = if batch_size <= 10 && bucket % 3 == 0 {
            "detail"
        } else {
            "feed"
        };
        let mut body = json!({"profile": profile, "inputs": inputs});
        if variant % 2 == 0 {
            body["namespace"] = Value::String(name_samples[0].0.clone());
        }
        requests.push(post(base, &["v2", "lookup"], body)?);
    }
    Ok(())
}

fn permission_requests(base: &Url, corpus: &Corpus, requests: &mut Vec<RequestSpec>) -> Result<()> {
    let subjects = if corpus.permission_subjects.is_empty() {
        corpus
            .address_names
            .iter()
            .map(|sample| sample.0.as_str())
            .collect::<Vec<_>>()
    } else {
        corpus
            .permission_subjects
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    };
    for (index, address) in subjects.into_iter().enumerate() {
        requests.push(get(
            base,
            &["v2", "permissions"],
            &[("address", address), ("page_size", page_size(index))],
        )?);
    }
    Ok(())
}

fn address_name_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (address, name, namespace, relation)) in corpus.address_names.iter().enumerate() {
        let query = search_term(name);
        requests.push(get(
            base,
            &["v2", "addresses", address, "names"],
            &[
                ("namespace", namespace),
                ("relation", public_relation(relation)?),
                ("q", &query),
                (
                    "sort",
                    if index % 2 == 0 {
                        "name"
                    } else {
                        "registered_at"
                    },
                ),
                ("order", if index % 3 == 0 { "desc" } else { "asc" }),
                ("page_size", page_size(index)),
            ],
        )?);
    }
    Ok(())
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
    for (address, coin_type, namespace) in primary_names {
        requests.push(get(
            base,
            &["v2", "addresses", address, "primary-name"],
            &[
                ("source", "indexed"),
                ("namespace", namespace),
                ("coin_type", coin_type),
            ],
        )?);
    }
    Ok(())
}

fn address_history_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (address, _, namespace, relation)) in corpus.address_names.iter().enumerate() {
        requests.push(get(
            base,
            &["v2", "addresses", address, "history"],
            &[
                ("namespace", namespace),
                ("relation", public_relation(relation)?),
                ("scope", history_scope(index)),
                ("page_size", page_size(index)),
            ],
        )?);
    }
    Ok(())
}

pub(super) fn normalized_base_url(value: &str) -> Result<Url> {
    let mut url = Url::parse(value).context("failed to parse API base URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "API base URL must use HTTP or HTTPS"
    );
    url.set_query(None);
    url.set_fragment(None);
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
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
    })
}

pub(super) fn post(base: &Url, segments: &[&str], body: Value) -> Result<RequestSpec> {
    Ok(RequestSpec {
        method: Method::POST,
        url: with_path(base, segments)?,
        body: Some(body),
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

fn page_size(index: usize) -> &'static str {
    ["1", "5", "20"][index % 3]
}

fn search_term(name: &str) -> String {
    let label = name.split('.').next().unwrap_or(name);
    label.chars().take(3).collect::<String>().to_lowercase()
}

fn numeric_chain_id(chain: &str) -> Result<&'static str> {
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
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: Vec::new(),
            resolvers: Vec::new(),
            namespaces: vec!["ens".to_owned()],
            names_by_namespace: [("ens".to_owned(), 1)].into_iter().collect(),
            parents_by_namespace: Default::default(),
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
        let lookup = request_variants(&base, &corpus, "lookup").unwrap();
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
            .map(|request| {
                let pairs = request.url.query_pairs().collect::<Vec<_>>();
                let match_mode = pairs
                    .iter()
                    .find(|(key, _)| key == "match")
                    .unwrap()
                    .1
                    .to_string();
                let explicit_namespace = pairs.iter().any(|(key, _)| key == "namespace");
                (match_mode, explicit_namespace)
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
    }
}
