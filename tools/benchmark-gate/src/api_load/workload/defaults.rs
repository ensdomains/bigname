use anyhow::{Result, bail};
use reqwest::Url;
use serde_json::{Value, json};

use super::{RequestSpec, get, lookup_batch_size, post, public_relation, search_term};
use crate::api_load::corpus::{Corpus, PermissionTarget};

const PAGE_SIZES: [&str; 5] = ["1", "5", "20", "50", "200"];

pub(super) fn parameterized_page_size(index: usize) -> &'static str {
    PAGE_SIZES[index % PAGE_SIZES.len()]
}

fn namespace_inference_matches(namespace: &str, name: &str) -> bool {
    match namespace {
        "basenames" => name.ends_with(".base.eth"),
        "ens" => !name.ends_with(".base.eth"),
        _ => false,
    }
}

pub(super) fn exact_name_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (namespace, name) in &corpus.names {
        if namespace_inference_matches(namespace, name) {
            requests.push(get(base, &["v2", "names", name], &[])?);
        }
        requests.push(get(
            base,
            &["v2", "names", name],
            &[("source", "indexed"), ("namespace", namespace)],
        )?);
    }
    Ok(())
}

pub(super) fn record_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (namespace, name)) in corpus.names.iter().enumerate() {
        if namespace_inference_matches(namespace, name) {
            requests.push(get(base, &["v2", "names", name, "records"], &[])?);
        }
        let keys = if index % 2 == 0 {
            "addr:60"
        } else {
            "text:avatar,text:description"
        };
        let mut query = vec![
            ("source", "indexed"),
            ("keys", keys),
            ("namespace", namespace.as_str()),
        ];
        if index % 2 == 0 {
            query.push(("include", "inventory"));
        }
        requests.push(get(base, &["v2", "names", name, "records"], &query)?);
    }
    Ok(())
}

pub(super) fn subname_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    let parents = if corpus.parents.is_empty() {
        &corpus.names
    } else {
        &corpus.parents
    };
    for (index, (namespace, parent)) in parents.iter().enumerate() {
        if namespace_inference_matches(namespace, parent) {
            requests.push(get(base, &["v2", "names", parent, "subnames"], &[])?);
        }
        let mut query = vec![
            ("page_size", parameterized_page_size(index)),
            ("namespace", namespace.as_str()),
        ];
        if index % 2 == 0 {
            query.push(("include", "counts"));
        }
        requests.push(get(base, &["v2", "names", parent, "subnames"], &query)?);
    }
    Ok(())
}

pub(super) fn name_history_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (namespace, name)) in corpus.names.iter().enumerate() {
        if namespace_inference_matches(namespace, name) {
            requests.push(get(base, &["v2", "names", name, "history"], &[])?);
        }
        requests.push(get(
            base,
            &["v2", "names", name, "history"],
            &[
                ("scope", super::history_scope(index)),
                ("page_size", parameterized_page_size(index)),
                ("namespace", namespace),
            ],
        )?);
    }
    Ok(())
}

pub(super) fn lookup_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    let inferred_names = corpus
        .names
        .iter()
        .filter(|(namespace, name)| namespace_inference_matches(namespace, name))
        .collect::<Vec<_>>();
    for variant in 0..100 {
        let bucket = variant / 2;
        let mode = variant % 4;
        let name_mode = mode % 2 == 0;
        let default_mode = mode >= 2;
        let name_samples = if name_mode && default_mode {
            inferred_names.clone()
        } else if name_mode {
            let namespace = &corpus.namespaces[(variant / 4) % corpus.namespaces.len()];
            corpus
                .names
                .iter()
                .filter(|(sample_namespace, _)| sample_namespace == namespace)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let corpus_len = if name_mode {
            name_samples.len()
        } else {
            corpus.address_names.len()
        };
        if corpus_len == 0 {
            bail!("lookup default-form corpus is empty for variant {variant}");
        }
        let batch_size = lookup_batch_size(bucket, corpus_len);
        let inputs = (0..batch_size)
            .map(|offset| -> Result<Value> {
                let index = (bucket * 97 + offset) % corpus_len;
                if name_mode {
                    return Ok(json!({
                        "id": format!("name-{variant}-{offset}"),
                        "name": name_samples[index].1,
                    }));
                }
                let mut input = json!({
                    "id": format!("address-{variant}-{offset}"),
                    "address": corpus.address_names[index].0,
                });
                if !default_mode {
                    input["coin_type"] = Value::from(60);
                    input["relation"] =
                        Value::String(public_relation(&corpus.address_names[index].3)?.to_owned());
                    input["page_size"] =
                        Value::from(parameterized_page_size(index).parse::<u64>().unwrap_or(50));
                }
                Ok(input)
            })
            .collect::<Result<Vec<_>>>()?;
        let profile = if batch_size <= 10 && bucket % 3 == 0 {
            "detail"
        } else {
            "feed"
        };
        let mut body = json!({"inputs": inputs});
        if !default_mode {
            body["profile"] = Value::String(profile.to_owned());
        }
        if name_mode && !default_mode {
            body["namespace"] = Value::String(name_samples[0].0.clone());
        }
        requests.push(post(base, &["v2", "lookup"], body)?);
    }
    Ok(())
}

pub(super) fn permission_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, target) in corpus.permission_subjects.iter().enumerate() {
        permission_dimension_requests(base, target, "address", &target.address, index, requests)?;
        permission_dimension_requests(base, target, "name", &target.name, index + 1, requests)?;
        permission_dimension_requests(
            base,
            target,
            "registration_id",
            &target.registration_id,
            index + 2,
            requests,
        )?;
    }
    Ok(())
}

fn permission_dimension_requests(
    base: &Url,
    target: &PermissionTarget,
    key: &'static str,
    value: &str,
    index: usize,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    let mut default = vec![(key, value)];
    if key == "name" && !namespace_inference_matches(&target.namespace, &target.name) {
        default.push(("namespace", target.namespace.as_str()));
    }
    let mut default_request = get(base, &["v2", "permissions"], &default)?;
    default_request.required_permission_audit_evidence =
        key == "registration_id" && target.retained_registration;
    requests.push(default_request);
    let mut parameterized = default;
    parameterized.push(("include", "lineage"));
    parameterized.push(("page_size", parameterized_page_size(index)));
    let mut parameterized_request = get(base, &["v2", "permissions"], &parameterized)?;
    parameterized_request.required_permission_audit_evidence =
        key == "registration_id" && target.retained_registration;
    requests.push(parameterized_request);
    Ok(())
}

pub(super) fn address_name_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (address, name, namespace, relation)) in corpus.address_names.iter().enumerate() {
        requests.push(get(base, &["v2", "addresses", address, "names"], &[])?);
        let query = search_term(name);
        let mut pairs = vec![
            ("namespace", namespace.as_str()),
            ("relation", public_relation(relation)?),
            ("q", query.as_str()),
            ("sort", ["name", "expires_at", "registered_at"][index % 3]),
            ("order", if (index / 3) % 2 == 0 { "asc" } else { "desc" }),
            ("page_size", parameterized_page_size(index)),
        ];
        match (index / 6) % 4 {
            0 => pairs.push(("dedupe", "name")),
            1 => {
                pairs.push(("dedupe", "name"));
                pairs.push(("include", "role_summary"));
            }
            2 => pairs.push(("dedupe", "registration")),
            3 => {
                pairs.push(("include", "role_summary"));
                pairs.push(("dedupe", "registration"));
            }
            _ => {}
        }
        requests.push(get(base, &["v2", "addresses", address, "names"], &pairs)?);
    }
    Ok(())
}

pub(super) fn address_history_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (address, _, namespace, relation)) in corpus.address_names.iter().enumerate() {
        requests.push(get(base, &["v2", "addresses", address, "history"], &[])?);
        requests.push(get(
            base,
            &["v2", "addresses", address, "history"],
            &[
                ("namespace", namespace),
                ("relation", public_relation(relation)?),
                ("scope", super::history_scope(index)),
                ("page_size", parameterized_page_size(index)),
            ],
        )?);
    }
    Ok(())
}

pub(super) fn search_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (namespace, name)) in corpus.names.iter().enumerate() {
        let query = search_term(name);
        requests.push(get(base, &["v2", "search"], &[("q", query.as_str())])?);
        requests.push(get(
            base,
            &["v2", "search"],
            &[
                ("q", query.as_str()),
                (
                    "match",
                    if (index / 2) % 2 == 0 {
                        "prefix"
                    } else {
                        "contains"
                    },
                ),
                ("page_size", parameterized_page_size(index + 1)),
            ],
        )?);
        requests.push(get(
            base,
            &["v2", "search"],
            &[
                ("q", query.as_str()),
                ("match", if index % 2 == 0 { "prefix" } else { "contains" }),
                ("namespace", namespace),
                ("page_size", parameterized_page_size(index)),
            ],
        )?);
    }
    Ok(())
}

pub(super) fn event_requests(
    base: &Url,
    corpus: &Corpus,
    requests: &mut Vec<RequestSpec>,
) -> Result<()> {
    for (index, (namespace, name)) in corpus.names.iter().enumerate() {
        requests.push(get(base, &["v2", "events"], &[])?);
        requests.push(get(
            base,
            &["v2", "events"],
            &[
                ("name", name),
                ("namespace", namespace),
                ("page_size", parameterized_page_size(index)),
            ],
        )?);
    }
    for (index, target) in corpus.permission_subjects.iter().enumerate() {
        let filter = if index % 2 == 0 {
            ("address", target.address.as_str())
        } else {
            ("registration_id", target.registration_id.as_str())
        };
        requests.push(get(
            base,
            &["v2", "events"],
            &[filter, ("page_size", parameterized_page_size(index))],
        )?);
    }
    Ok(())
}
