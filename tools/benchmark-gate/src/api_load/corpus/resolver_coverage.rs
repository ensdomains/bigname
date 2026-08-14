use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::DEFAULT_RESOLVER_CURRENT_READ_FILTER;
use sqlx::PgPool;

use crate::api_load::ResolverManifestCoverage;

#[derive(Clone, Debug)]
pub(super) struct ResolverCoverage {
    pub(super) resolvers: Vec<(String, String)>,
    pub(super) counts: Vec<ResolverManifestCoverage>,
    pub(super) failures: Vec<String>,
}

#[derive(sqlx::FromRow)]
struct ResolverCoverageRow {
    chain_id: String,
    source_family: String,
    resolver_address: Option<String>,
    support_status: Option<String>,
    api_visible: bool,
}

fn resolver_manifest_coverage_sql() -> String {
    format!(
        r#"
WITH expected AS (
    SELECT DISTINCT manifest.chain_id,
           manifest.source_family,
           lower(declaration ->> 'address') AS resolver_address
    FROM bigname_phase.manifest_versions manifest
    LEFT JOIN LATERAL jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'contracts', '[]'::jsonb
    )) declaration ON true
    WHERE manifest.rollout_status = 'active'
      AND manifest.source_family IN (
          'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
          'basenames_base_resolver'
      )
      AND (
          declaration IS NULL
          OR (
              declaration ->> 'address' IS NOT NULL
              AND btrim(declaration ->> 'address') <> ''
          )
      )
)
SELECT expected.chain_id,
       expected.source_family,
       expected.resolver_address,
       candidate.support_status,
       resolver.resolver_address IS NOT NULL AS api_visible
FROM expected
LEFT JOIN bigname_phase.resolver_current candidate
  ON candidate.chain_id = expected.chain_id
 AND lower(candidate.resolver_address) = expected.resolver_address
LEFT JOIN bigname_phase.resolver_current resolver
  ON resolver.chain_id = expected.chain_id
 AND lower(resolver.resolver_address) = expected.resolver_address
 {DEFAULT_RESOLVER_CURRENT_READ_FILTER}
ORDER BY expected.chain_id, expected.source_family, expected.resolver_address"#
    )
}

pub(super) async fn load(pool: &PgPool) -> Result<ResolverCoverage> {
    let rows: Vec<ResolverCoverageRow> = sqlx::query_as(&resolver_manifest_coverage_sql())
        .fetch_all(pool)
        .await
        .context("failed to compare the resolver corpus with active stored manifests")?;
    let mut failures = Vec::new();
    let mut resolvers = Vec::with_capacity(rows.len());
    let mut counts = BTreeMap::<(String, String), (usize, usize)>::new();

    for row in rows {
        let ResolverCoverageRow {
            chain_id,
            source_family,
            resolver_address,
            support_status,
            api_visible,
        } = row;
        let count = counts
            .entry((chain_id.clone(), source_family.clone()))
            .or_insert((0, 0));
        let Some(resolver_address) = resolver_address else {
            continue;
        };
        count.0 += 1;
        match support_status.as_deref() {
            None => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} is missing from resolver_current; rebuild Project from the stored active manifests and rerun the gate"
            )),
            Some(status) if status != "supported" => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} is {status:?}, not supported, in resolver_current; rebuild Project from the stored active manifests and rerun the gate"
            )),
            Some(_) if !api_visible => failures.push(format!(
                "active resolver manifest address {resolver_address} on chain {chain_id:?} in family {source_family:?} is not API-visible through canonical projection lineage; repair or rebuild Project and rerun the gate"
            )),
            Some(_) => {
                count.1 += 1;
                resolvers.push((chain_id, resolver_address));
            }
        }
    }

    let counts = counts
        .into_iter()
        .map(
            |((chain_id, source_family), (declared_addresses, exercised_addresses))| {
                ResolverManifestCoverage {
                    chain_id,
                    source_family,
                    declared_addresses,
                    exercised_addresses,
                }
            },
        )
        .collect();
    if resolvers.is_empty() {
        failures.push(
            "active stored resolver-family manifests contribute zero supported, API-visible concrete resolver addresses; restore and project a manifest set with a concrete resolver contract, then rerun the gate"
                .to_owned(),
        );
    }
    Ok(ResolverCoverage {
        resolvers,
        counts,
        failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_query_reads_the_same_stored_contract_declarations_as_project() {
        let query = resolver_manifest_coverage_sql();
        assert!(query.contains("manifest.manifest_payload -> 'contracts'"));
        assert!(query.contains("manifest.rollout_status = 'active'"));
        assert!(query.contains("'ens_v1_resolver_l1', 'ens_v2_resolver_l1'"));
        assert!(query.contains("'basenames_base_resolver'"));
        assert!(query.contains(DEFAULT_RESOLVER_CURRENT_READ_FILTER.trim()));
    }
}
