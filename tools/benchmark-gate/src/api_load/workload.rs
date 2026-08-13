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
            for name in &corpus.names {
                requests.push(get(base, &["v2", "names", name], &[("source", "indexed")])?);
            }
        }
        "records" => {
            for (index, name) in corpus.names.iter().enumerate() {
                let keys = if index % 2 == 0 {
                    "addr:60"
                } else {
                    "text:avatar,text:description"
                };
                requests.push(get(
                    base,
                    &["v2", "names", name, "records"],
                    &[("source", "indexed"), ("keys", keys)],
                )?);
            }
        }
        "subnames" => {
            let parents = if corpus.parents.is_empty() {
                &corpus.names
            } else {
                &corpus.parents
            };
            for (index, parent) in parents.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "names", parent, "subnames"],
                    &[("page_size", page_size(index))],
                )?);
            }
        }
        "name_history" => {
            for (index, name) in corpus.names.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "names", name, "history"],
                    &[
                        ("scope", history_scope(index)),
                        ("page_size", page_size(index)),
                    ],
                )?);
            }
        }
        "permissions" => permission_requests(base, corpus, &mut requests)?,
        "address_names" => address_name_requests(base, corpus, &mut requests)?,
        "primary_name" => primary_name_requests(base, corpus, &mut requests)?,
        "address_history" => address_history_requests(base, corpus, &mut requests)?,
        "search" => {
            for (index, name) in corpus.names.iter().enumerate() {
                let query = search_term(name);
                requests.push(get(
                    base,
                    &["v2", "search"],
                    &[
                        ("q", &query),
                        ("match", if index % 2 == 0 { "prefix" } else { "contains" }),
                        ("namespace", namespace_for_name(name)),
                        ("page_size", page_size(index)),
                    ],
                )?);
            }
        }
        "events" => {
            for (index, name) in corpus.names.iter().enumerate() {
                requests.push(get(
                    base,
                    &["v2", "events"],
                    &[
                        ("name", name),
                        ("namespace", namespace_for_name(name)),
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
        let corpus_len = if variant % 2 == 0 {
            corpus.names.len()
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
                        "name": corpus.names[index],
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
        requests.push(post(
            base,
            &["v2", "lookup"],
            json!({"profile": profile, "inputs": inputs}),
        )?);
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

fn namespace_for_name(name: &str) -> &'static str {
    if name.ends_with(".base.eth") {
        "basenames"
    } else {
        "ens"
    }
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
}
