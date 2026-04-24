use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use super::admission::DiscoveryAdmissionState;
use super::provenance::TRANSITIVE_DISCOVERY_EDGE_KIND;
use super::types::{StoredActiveContract, StoredActiveRoot, StoredDiscoveryRule};
use crate::{PROPAGATED_ROLE_PROVENANCE_FIELD, REACHABLE_FROM_ROOT_ADMISSION, normalize_address};

pub async fn load_discovery_admission_state(pool: &PgPool) -> Result<DiscoveryAdmissionState> {
    load_discovery_admission_state_with_excluded_source(pool, None).await
}

pub(super) async fn load_discovery_admission_state_with_excluded_source(
    pool: &PgPool,
    excluded_discovery_source: Option<&str>,
) -> Result<DiscoveryAdmissionState> {
    let active_manifest_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE rollout_status = 'active'",
    )
    .fetch_one(pool)
    .await
    .context("failed to count active manifest versions")? as usize;

    let active_root_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mci.contract_instance_id, cia.address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'root'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest roots")?;

    let active_contract_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mci.role, mci.contract_instance_id, cia.address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest contracts")?;

    let active_discovered_parent_rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            de.provenance ->> 'propagated_role' AS role,
            de.to_contract_instance_id AS contract_instance_id,
            cia.address AS address
        FROM discovery_edges de
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND de.deactivated_at IS NULL
          AND de.edge_kind = $4
          AND de.admission = $1
          AND de.provenance ? $2
          AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
        "#,
    )
    .bind(REACHABLE_FROM_ROOT_ADMISSION)
    .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
    .bind(excluded_discovery_source)
    .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
    .fetch_all(pool)
    .await
    .context("failed to load active transitive discovery parents")?;

    let active_rule_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mdr.edge_kind, mdr.from_role, mdr.admission
        FROM manifest_versions mv
        JOIN manifest_discovery_rules mdr ON mdr.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active discovery rules")?;

    let known_address_rows = sqlx::query(
        r#"
        SELECT chain_id, address, contract_instance_id
        FROM contract_instance_addresses
        ORDER BY chain_id, address, (deactivated_at IS NULL) DESC, admitted_at DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load known contract-instance addresses")?;

    let active_roots = active_root_rows
        .into_iter()
        .map(|row| {
            Ok(StoredActiveRoot {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read active root manifest_id")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read active root chain")?,
                _contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active root contract_instance_id")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read active root address")?,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let active_root_manifest_ids = active_roots.iter().map(|root| root.manifest_id).collect();

    let active_contracts = active_contract_rows
        .into_iter()
        .chain(active_discovered_parent_rows)
        .map(|row| {
            Ok(StoredActiveContract {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read active contract manifest_id")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read active contract chain")?,
                role: row
                    .try_get("role")
                    .context("failed to read active contract role")?,
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active contract contract_instance_id")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read active contract address")?,
                ),
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let mut rules_by_manifest_id: HashMap<i64, Vec<StoredDiscoveryRule>> = HashMap::new();
    for row in active_rule_rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read active rule manifest_id")?;
        let rule = StoredDiscoveryRule {
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read active rule edge_kind")?,
            from_role: row
                .try_get("from_role")
                .context("failed to read active rule from_role")?,
            admission: row
                .try_get("admission")
                .context("failed to read active rule admission")?,
        };
        rules_by_manifest_id
            .entry(manifest_id)
            .or_default()
            .push(rule);
    }

    let mut known_contract_instances_by_address = HashMap::new();
    for row in known_address_rows {
        let chain = row
            .try_get::<String, _>("chain_id")
            .context("failed to read known address chain_id")?;
        let address = normalize_address(
            &row.try_get::<String, _>("address")
                .context("failed to read known address")?,
        );
        known_contract_instances_by_address
            .entry((chain, address))
            .or_insert(
                row.try_get("contract_instance_id")
                    .context("failed to read known address contract_instance_id")?,
            );
    }

    let active_rule_count = rules_by_manifest_id.values().map(Vec::len).sum();

    Ok(DiscoveryAdmissionState {
        active_manifest_count,
        active_root_count: active_roots.len(),
        active_contract_count: active_contracts.len(),
        active_rule_count,
        active_roots,
        active_root_manifest_ids,
        active_contracts,
        known_contract_instances_by_address,
        rules_by_manifest_id,
    })
}
