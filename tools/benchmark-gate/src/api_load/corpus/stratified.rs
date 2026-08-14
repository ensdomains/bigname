use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use bigname_storage::{
    DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS, DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    DEFAULT_PRIMARY_NAME_CURRENT_READ_FILTER,
};

pub(super) fn address_corpus_sql() -> String {
    format!(
        r#"WITH active_namespaces AS (
    SELECT namespace,
           row_number() OVER (ORDER BY namespace) AS quota_rank,
           count(*) OVER () AS namespace_count
    FROM (SELECT DISTINCT namespace FROM bigname_phase.manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames')) active
), candidates AS (
    SELECT anc.address, min(anc.raw_name) AS raw_name, anc.namespace, anc.relation
    FROM bigname_phase.address_names_current anc
    {DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS}
    JOIN active_namespaces active ON active.namespace = anc.namespace
    WHERE anc.support_status = 'supported'
      {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
    GROUP BY anc.address, anc.namespace, anc.relation
), ranked AS (
    SELECT candidate.*,
           row_number() OVER (PARTITION BY candidate.namespace ORDER BY candidate.address, candidate.relation) AS sample_rank,
           active.quota_rank, active.namespace_count
    FROM candidates candidate
    JOIN active_namespaces active ON active.namespace = candidate.namespace
)
SELECT address, raw_name, namespace, relation
FROM ranked
WHERE sample_rank <= ($1 / namespace_count)
    + CASE WHEN quota_rank <= ($1 % namespace_count) THEN 1 ELSE 0 END
ORDER BY namespace, address, relation"#
    )
}

pub(super) fn primary_name_corpus_sql() -> String {
    format!(
        r#"WITH active_namespaces AS (
    SELECT namespace,
           row_number() OVER (ORDER BY namespace) AS quota_rank,
           count(*) OVER () AS namespace_count
    FROM (SELECT DISTINCT namespace FROM bigname_phase.manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames')) active
), ranked AS (
    SELECT pnc.address, pnc.coin_type, pnc.namespace,
           row_number() OVER (PARTITION BY pnc.namespace ORDER BY pnc.address, pnc.coin_type) AS sample_rank,
           active.quota_rank, active.namespace_count
    FROM bigname_phase.primary_names_current pnc
    JOIN active_namespaces active ON active.namespace = pnc.namespace
    WHERE pnc.claim_status = 'success'
      {DEFAULT_PRIMARY_NAME_CURRENT_READ_FILTER}
)
SELECT address, coin_type, namespace
FROM ranked
WHERE sample_rank <= ($1 / namespace_count)
    + CASE WHEN quota_rank <= ($1 % namespace_count) THEN 1 ELSE 0 END
ORDER BY namespace, address, coin_type"#
    )
}

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
