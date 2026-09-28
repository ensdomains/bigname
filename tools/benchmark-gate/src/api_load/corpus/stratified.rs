use std::collections::BTreeMap;

use anyhow::{Result, ensure};
pub(super) fn address_namespace_counts(
    rows: &[(String, String, String, String)],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (_, _, namespace, _) in rows {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

pub(super) fn primary_namespace_counts(
    rows: &[(String, String, String)],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (_, _, namespace) in rows {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

pub(super) fn require_active_namespace_coverage(
    namespaces: &[String],
    counts_by_namespace: &BTreeMap<String, usize>,
    seed_kind: &str,
) -> Result<()> {
    ensure!(
        !namespaces.is_empty(),
        "benchmark database has no active public namespace"
    );
    for namespace in namespaces {
        ensure!(
            counts_by_namespace
                .get(namespace)
                .copied()
                .unwrap_or_default()
                > 0,
            "active namespace {namespace:?} contributed no {seed_kind} to the benchmark corpus"
        );
    }
    Ok(())
}
