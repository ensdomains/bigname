use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::budgets::GateBudgets;

mod permissions;
mod readers;
mod resolver_coverage;
mod scale;
mod stratified;
mod verdict;
pub(super) use permissions::PermissionTarget;
use resolver_coverage::load as load_resolver_coverage;
#[cfg(test)]
use scale::table_scale_failures;
pub(super) use scale::{TableScale, load_table_scale};
use stratified::{
    address_namespace_counts, primary_namespace_counts, require_active_namespace_coverage,
};
pub(super) use verdict::require_stratified_size as require_stratified_corpus_size;
use verdict::{
    collect_failure as collect_corpus_failure, require_minimum_size as require_minimum_corpus_size,
};

const ACTIVE_NAMESPACES_SQL: &str = "SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames') ORDER BY namespace";
#[derive(Clone, Debug)]
pub(super) struct Corpus {
    pub(super) names: Vec<(String, String)>,
    pub(super) address_names: Vec<(String, String, String, String)>,
    pub(super) parents: Vec<(String, String)>,
    pub(super) permission_subjects: Vec<PermissionTarget>,
    pub(super) primary_names: Vec<(String, String, String)>,
    pub(super) resolvers: Vec<super::workload::ResolverTarget>,
    pub(super) namespaces: Vec<String>,
    pub(super) names_by_namespace: BTreeMap<String, usize>,
    pub(super) parents_by_namespace: BTreeMap<String, usize>,
    pub(super) resolver_manifest_coverage: Vec<super::ResolverManifestCoverage>,
}

impl Corpus {
    pub(super) async fn load(pool: &PgPool, budgets: &GateBudgets) -> Result<(Self, Vec<String>)> {
        let limit = i64::try_from(budgets.api_corpus_size)
            .context("API corpus size exceeds PostgreSQL limit")?;
        let namespaces: Vec<String> = sqlx::query_scalar(ACTIVE_NAMESPACES_SQL)
            .fetch_all(pool)
            .await
            .context("failed to load namespace benchmark corpus")?;
        let names = readers::names(pool, budgets.api_corpus_size, false).await?;
        let (_, address_names) = readers::addresses(pool, budgets.api_corpus_size).await?;
        let parents = readers::names(pool, budgets.api_corpus_size, true).await?;
        let permission_subjects = permissions::load(pool, limit).await?;
        let primary_names = readers::primary_names(pool, budgets.api_corpus_size).await?;
        let resolver_coverage = load_resolver_coverage(pool).await?;
        let names_by_namespace = namespace_counts(&names);
        let parents_by_namespace = namespace_counts(&parents);
        let addresses_by_namespace = address_namespace_counts(&address_names);
        let primary_names_by_namespace = primary_namespace_counts(&primary_names);

        let mut failures = resolver_coverage.failures;
        if budgets.api_require_populated_probes
            && !permission_subjects
                .iter()
                .any(|target| target.retained_registration)
        {
            failures.push(
                "permission corpus contains no canonical retained registration absent from current composed names; restore production-shaped superseded-registration permission history and rerun the gate"
                    .to_owned(),
            );
        }
        collect_corpus_failure(
            &mut failures,
            require_active_namespace_coverage(&namespaces, &names_by_namespace, "supported names"),
        );
        collect_corpus_failure(
            &mut failures,
            require_active_namespace_coverage(
                &namespaces,
                &parents_by_namespace,
                "supported parents",
            ),
        );
        collect_corpus_failure(
            &mut failures,
            require_active_namespace_coverage(
                &namespaces,
                &addresses_by_namespace,
                "supported address/name relations",
            ),
        );
        if budgets.api_min_specialized_corpus_size > 0 {
            collect_corpus_failure(
                &mut failures,
                require_active_namespace_coverage(
                    &namespaces,
                    &primary_names_by_namespace,
                    "successful primary names",
                ),
            );
        }
        collect_corpus_failure(
            &mut failures,
            require_stratified_corpus_size(
                "name",
                names.len(),
                budgets.api_corpus_size,
                &names_by_namespace,
            ),
        );
        collect_corpus_failure(
            &mut failures,
            require_stratified_corpus_size(
                "address",
                address_names.len(),
                budgets.api_corpus_size,
                &addresses_by_namespace,
            ),
        );
        for (label, actual) in [
            ("subname parent", parents.len()),
            ("permission subject", permission_subjects.len()),
        ] {
            collect_corpus_failure(
                &mut failures,
                require_minimum_corpus_size(label, actual, budgets.api_min_specialized_corpus_size),
            );
        }
        collect_corpus_failure(
            &mut failures,
            require_stratified_corpus_size(
                "successful primary-name",
                primary_names.len(),
                budgets.api_min_specialized_corpus_size,
                &primary_names_by_namespace,
            ),
        );

        Ok((
            Self {
                names,
                address_names,
                parents,
                permission_subjects,
                primary_names,
                resolvers: resolver_coverage.resolvers,
                namespaces,
                names_by_namespace,
                parents_by_namespace,
                resolver_manifest_coverage: resolver_coverage.counts,
            },
            failures,
        ))
    }
}

fn namespace_counts(rows: &[(String, String)]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (namespace, _) in rows {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
#[path = "corpus/tests/load.rs"]
mod load_tests;

#[cfg(test)]
#[path = "corpus/tests/support.rs"]
mod tests;

#[cfg(test)]
#[path = "corpus/tests/families.rs"]
mod family_tests;
