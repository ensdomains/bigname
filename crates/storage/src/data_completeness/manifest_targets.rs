use anyhow::{Context, Result};

use super::{DiscoveryTargetMissingAddress, ManifestDeclaredTarget};

const ACTIVE_MANIFEST_TARGETS_CTE: &str = r#"
WITH active_manifest_entries AS (
    SELECT
        manifest.manifest_id,
        manifest.chain,
        manifest.source_family,
        'root'::TEXT AS declaration_kind,
        entry ->> 'name' AS declaration_name,
        lower(entry ->> 'address') AS declared_address,
        (entry ->> 'start_block')::BIGINT AS start_block,
        entry
    FROM manifest_versions manifest
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(manifest.manifest_payload -> 'roots', '[]'::JSONB)
    ) entry
    WHERE manifest.rollout_status = 'active'

    UNION ALL

    SELECT
        manifest.manifest_id,
        manifest.chain,
        manifest.source_family,
        'contract'::TEXT AS declaration_kind,
        entry ->> 'role' AS declaration_name,
        lower(entry ->> 'address') AS declared_address,
        (entry ->> 'start_block')::BIGINT AS start_block,
        entry
    FROM manifest_versions manifest
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(manifest.manifest_payload -> 'contracts', '[]'::JSONB)
    ) entry
    WHERE manifest.rollout_status = 'active'
),
manifest_targets AS (
    SELECT
        manifest_id,
        chain,
        source_family,
        declaration_kind,
        declaration_name,
        declared_address,
        'declaration'::TEXT AS target_kind,
        declared_address AS address,
        start_block
    FROM active_manifest_entries

    UNION ALL

    SELECT
        manifest_id,
        chain,
        source_family,
        declaration_kind,
        declaration_name,
        declared_address,
        'implementation'::TEXT AS target_kind,
        lower(entry ->> 'implementation') AS address,
        start_block
    FROM active_manifest_entries
    WHERE declaration_kind = 'contract'
      AND entry ->> 'implementation' IS NOT NULL
)
"#;

pub(super) async fn load_active_deployment_profile(pool: &sqlx::PgPool) -> Result<Option<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT chain, deployment_epoch
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY chain, deployment_epoch
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest corpus for deployment-profile inference")?;

    if rows.is_empty() {
        return Ok(None);
    }

    let rows = rows
        .into_iter()
        .map(|row| {
            Ok((
                crate::sql_row::get::<String>(&row, "chain")?,
                crate::sql_row::get::<String>(&row, "deployment_epoch")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if rows.iter().all(|(chain, _)| chain.ends_with("-mainnet")) {
        return Ok(Some("mainnet".to_owned()));
    }
    if rows.iter().all(|(chain, deployment_epoch)| {
        chain.ends_with("-sepolia") && deployment_epoch.ends_with("_sepolia_dev")
    }) {
        return Ok(Some("sepolia-dev".to_owned()));
    }

    Ok(None)
}

pub(super) async fn load_manifest_declared_targets(
    pool: &sqlx::PgPool,
) -> Result<Vec<ManifestDeclaredTarget>> {
    let query = format!(
        "{ACTIVE_MANIFEST_TARGETS_CTE}
        SELECT DISTINCT
            chain,
            source_family,
            address,
            start_block AS active_from_block_number
        FROM manifest_targets
        ORDER BY chain, source_family, address, active_from_block_number"
    );
    load_manifest_targets(
        pool,
        &query,
        "failed to load active manifest-declared targets",
    )
    .await
}

pub(super) async fn load_manifest_declared_targets_missing_address(
    pool: &sqlx::PgPool,
) -> Result<Vec<ManifestDeclaredTarget>> {
    let query = format!(
        "{ACTIVE_MANIFEST_TARGETS_CTE}
        SELECT DISTINCT
            target.chain,
            target.source_family,
            target.address,
            target.start_block AS active_from_block_number
        FROM manifest_targets target
        WHERE NOT EXISTS (
            SELECT 1
            FROM manifest_contract_instances declaration
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = CASE target.target_kind
                  WHEN 'declaration' THEN declaration.contract_instance_id
                  ELSE declaration.implementation_contract_instance_id
              END
            WHERE declaration.manifest_id = target.manifest_id
                AND declaration.declaration_kind = target.declaration_kind
                AND declaration.declaration_name = target.declaration_name
                AND lower(declaration.declared_address) = target.declared_address
                AND address.deactivated_at IS NULL
                AND address.chain_id = target.chain
                AND lower(address.address) = target.address
                AND (
                    target.target_kind = 'declaration'
                    OR lower(declaration.declared_implementation_address) = target.address
                )
        )
        ORDER BY target.chain, target.source_family, target.address, active_from_block_number"
    );
    load_manifest_targets(
        pool,
        &query,
        "failed to load manifest-declared targets missing a matching live address row",
    )
    .await
}

pub(super) async fn load_manifest_proxy_implementations_missing_edge(
    pool: &sqlx::PgPool,
) -> Result<Vec<ManifestDeclaredTarget>> {
    let query = format!(
        "{ACTIVE_MANIFEST_TARGETS_CTE}
        SELECT DISTINCT
            target.chain,
            target.source_family,
            target.address,
            target.start_block AS active_from_block_number
        FROM manifest_targets target
        JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = target.manifest_id
         AND declaration.declaration_kind = target.declaration_kind
         AND declaration.declaration_name = target.declaration_name
         AND lower(declaration.declared_address) = target.declared_address
         AND lower(declaration.declared_implementation_address) = target.address
        WHERE target.target_kind = 'implementation'
          AND declaration.implementation_contract_instance_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM discovery_edges edge
              WHERE edge.chain_id = target.chain
                AND edge.edge_kind = 'proxy_implementation'
                AND edge.from_contract_instance_id = declaration.contract_instance_id
                AND edge.to_contract_instance_id =
                    declaration.implementation_contract_instance_id
                AND edge.discovery_source = 'manifest_declared_proxy'
                AND edge.source_manifest_id = target.manifest_id
                AND edge.admission = 'manifest_declared'
                AND edge.deactivated_at IS NULL
          )
        ORDER BY target.chain, target.source_family, target.address, active_from_block_number"
    );
    load_manifest_targets(
        pool,
        &query,
        "failed to load manifest proxy implementations missing their managed edge",
    )
    .await
}

pub(super) async fn load_discovery_targets_missing_address(
    pool: &sqlx::PgPool,
) -> Result<Vec<DiscoveryTargetMissingAddress>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            edge.chain_id AS chain,
            COALESCE(target_manifest.source_family, source_manifest.source_family)
                AS source_family,
            edge.to_contract_instance_id AS contract_instance_id
        FROM discovery_edges edge
        JOIN manifest_versions source_manifest
          ON source_manifest.manifest_id = edge.source_manifest_id
        LEFT JOIN manifest_versions target_manifest
          ON target_manifest.rollout_status = 'active'
         AND target_manifest.namespace = source_manifest.namespace
         AND target_manifest.chain = edge.chain_id
         AND target_manifest.deployment_epoch = source_manifest.deployment_epoch
         AND target_manifest.source_family = CASE
             WHEN edge.edge_kind = 'resolver'
                  AND source_manifest.source_family = 'ens_v1_registry_l1'
                 THEN 'ens_v1_resolver_l1'
             WHEN edge.edge_kind = 'resolver'
                  AND source_manifest.source_family = 'ens_v2_registry_l1'
                 THEN 'ens_v2_resolver_l1'
             WHEN edge.edge_kind = 'resolver'
                  AND source_manifest.source_family = 'basenames_base_registry'
                 THEN 'basenames_base_resolver'
             ELSE NULL
         END
        WHERE source_manifest.rollout_status = 'active'
          AND edge.deactivated_at IS NULL
          AND edge.edge_kind <> 'migration'
          AND (
              edge.edge_kind <> 'resolver'
              OR source_manifest.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
              OR target_manifest.manifest_id IS NOT NULL
          )
          AND NOT EXISTS (
              SELECT 1
              FROM contract_instance_addresses address
              WHERE address.contract_instance_id = edge.to_contract_instance_id
                AND address.chain_id = edge.chain_id
                AND address.deactivated_at IS NULL
          )
        ORDER BY chain, source_family, contract_instance_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active discovery targets missing live address rows")?;

    rows.into_iter()
        .map(|row| {
            Ok(DiscoveryTargetMissingAddress {
                chain: crate::sql_row::get(&row, "chain")?,
                source_family: crate::sql_row::get(&row, "source_family")?,
                contract_instance_id: crate::sql_row::get(&row, "contract_instance_id")?,
            })
        })
        .collect()
}

async fn load_manifest_targets(
    pool: &sqlx::PgPool,
    query: &str,
    context: &'static str,
) -> Result<Vec<ManifestDeclaredTarget>> {
    let rows = sqlx::query(query).fetch_all(pool).await.context(context)?;
    rows.into_iter()
        .map(|row| {
            Ok(ManifestDeclaredTarget {
                chain: crate::sql_row::get(&row, "chain")?,
                source_family: crate::sql_row::get(&row, "source_family")?,
                address: crate::sql_row::get(&row, "address")?,
                active_from_block_number: crate::sql_row::get(&row, "active_from_block_number")?,
            })
        })
        .collect()
}
