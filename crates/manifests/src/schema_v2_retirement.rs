use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::ManifestAddressDeclaration;

#[derive(sqlx::FromRow)]
pub(super) struct RetiredManifestAddress {
    pub(super) row_id: i64,
    pub(super) instance_id: Uuid,
    pub(super) chain_id: String,
    pub(super) address: String,
    pub(super) active_from: Option<i64>,
    pub(super) active_to: Option<i64>,
    pub(super) active_to_hash: Option<String>,
    pub(super) head_number: Option<i64>,
    pub(super) head_hash: Option<String>,
}

pub(super) async fn active_manifest_address_declarations(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<ManifestAddressDeclaration>> {
    let declarations: Vec<(String, Uuid, String, i64, Value)> = sqlx::query_as(
        "SELECT declaration.chain_id, declaration.contract_instance_id,
                lower(declaration.declared_address), declaration.manifest_id,
                jsonb_build_object(
                    'source', 'manifest_declaration',
                    'manifest_id', declaration.manifest_id,
                    'declaration_kind', declaration.declaration_kind,
                    'declaration_name', declaration.declaration_name,
                    'declared_address', lower(declaration.declared_address)
                )
         FROM manifest_contract_instances declaration
         JOIN manifest_versions manifest USING (manifest_id, chain_id)
         WHERE manifest.rollout_status = 'active'
         UNION ALL
         SELECT declaration.chain_id,
                declaration.implementation_contract_instance_id,
                lower(declaration.declared_implementation_address),
                declaration.manifest_id,
                jsonb_build_object(
                    'source', 'manifest_proxy_implementation',
                    'manifest_id', declaration.manifest_id,
                    'proxy_role', declaration.role,
                    'proxy_address', lower(declaration.declared_address),
                    'declared_address', lower(declaration.declared_implementation_address)
                )
         FROM manifest_contract_instances declaration
         JOIN manifest_versions manifest USING (manifest_id, chain_id)
         WHERE manifest.rollout_status = 'active'
           AND declaration.implementation_contract_instance_id IS NOT NULL
           AND declaration.declared_implementation_address IS NOT NULL
         ORDER BY 1, 2, 3, 4",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to snapshot active manifest address declarations")?;
    let mut unique = BTreeMap::new();
    for (chain_id, instance_id, address, manifest_id, provenance) in declarations {
        unique
            .entry((chain_id.clone(), instance_id, address.clone()))
            .or_insert(ManifestAddressDeclaration {
                chain_id,
                instance_id,
                address,
                manifest_id,
                provenance,
            });
    }
    Ok(unique.into_values().collect())
}
