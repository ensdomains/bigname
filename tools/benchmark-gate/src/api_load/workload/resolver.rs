use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use reqwest::Url;

use super::{RequestSpec, get, numeric_chain_id, page_size, request_variants};
use crate::api_load::{ResolverManifestCoverage, corpus::Corpus};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::api_load) struct ResolverTarget {
    pub(in crate::api_load) chain_id: String,
    pub(in crate::api_load) source_family: String,
    pub(in crate::api_load) resolver_address: String,
}

pub(in crate::api_load) fn endpoint_requests(
    base: &Url,
    corpus: &mut Corpus,
    endpoint: &str,
) -> Result<Vec<RequestSpec>> {
    let requests = request_variants(base, corpus, endpoint)?;
    if endpoint == "resolver" {
        record_resolver_request_construction(
            base,
            &corpus.resolvers,
            &requests,
            &mut corpus.resolver_manifest_coverage,
        )?;
    }
    Ok(requests)
}

fn record_resolver_request_construction(
    base: &Url,
    targets: &[ResolverTarget],
    requests: &[RequestSpec],
    coverage: &mut [ResolverManifestCoverage],
) -> Result<()> {
    let mut constructed = BTreeMap::<(&str, &str), usize>::new();
    for (index, target) in targets.iter().enumerate() {
        let expected = get(
            base,
            &[
                "v2",
                "resolvers",
                numeric_chain_id(&target.chain_id)?,
                &target.resolver_address,
            ],
            &[("page_size", page_size(index))],
        )?;
        ensure!(
            requests.iter().any(|request| request.url == expected.url),
            "resolver request construction omitted manifest address {} on chain {:?} in family {:?}",
            target.resolver_address,
            target.chain_id,
            target.source_family
        );
        *constructed
            .entry((&target.chain_id, &target.source_family))
            .or_default() += 1;
    }
    ensure!(
        requests.len() == targets.len(),
        "resolver request construction built {} variants for {} manifest addresses",
        requests.len(),
        targets.len()
    );
    for count in coverage {
        let actual = constructed
            .remove(&(count.chain_id.as_str(), count.source_family.as_str()))
            .unwrap_or_default();
        ensure!(
            actual == count.applicable_addresses,
            "resolver request construction covered {actual} of {} currently applicable declared addresses on chain {:?} in family {:?}",
            count.applicable_addresses,
            count.chain_id,
            count.source_family
        );
        count.exercised_addresses = actual;
    }
    ensure!(
        constructed.is_empty(),
        "resolver request construction contains a chain/family absent from manifest coverage"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_load::workload::normalized_base_url;

    #[test]
    fn every_resolver_corpus_entry_builds_a_request_variant() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let mut corpus = Corpus {
            names: Vec::new(),
            address_names: Vec::new(),
            parents: Vec::new(),
            permission_subjects: Vec::new(),
            primary_names: Vec::new(),
            resolvers: vec![
                ResolverTarget {
                    chain_id: "ethereum-mainnet".to_owned(),
                    source_family: "ens_v1_resolver_l1".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000001".to_owned(),
                },
                ResolverTarget {
                    chain_id: "base-mainnet".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000002".to_owned(),
                },
            ],
            namespaces: Vec::new(),
            names_by_namespace: BTreeMap::new(),
            parents_by_namespace: BTreeMap::new(),
            resolver_manifest_coverage: vec![
                ResolverManifestCoverage {
                    chain_id: "ethereum-mainnet".to_owned(),
                    source_family: "ens_v1_resolver_l1".to_owned(),
                    declared_addresses: 1,
                    applicable_addresses: 1,
                    exercised_addresses: 0,
                },
                ResolverManifestCoverage {
                    chain_id: "base-mainnet".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    declared_addresses: 1,
                    applicable_addresses: 1,
                    exercised_addresses: 0,
                },
            ],
        };

        let requests = endpoint_requests(&base, &mut corpus, "resolver").unwrap();

        assert_eq!(requests.len(), corpus.resolvers.len());
        assert!(
            requests
                .iter()
                .any(|request| request.url.path().ends_with('1'))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.url.path().ends_with('2'))
        );
        assert!(
            corpus
                .resolver_manifest_coverage
                .iter()
                .all(|count| count.exercised_addresses == 1)
        );
    }
}
