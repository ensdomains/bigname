use sqlx::{FromRow, Postgres, Transaction};

use crate::{LookupError, Result, error::database};

pub(super) struct EntrypointQuery<'a> {
    pub namespace: &'a str,
    pub source_family: &'a str,
    pub chain_id: &'a str,
    pub role: &'a str,
    pub allow_shadow: bool,
    pub execution_block_number: i64,
    pub required_manifest_version: Option<i64>,
    pub require_resolution_capability: bool,
}

#[derive(FromRow)]
struct ManifestEntry {
    declared_address: String,
}

pub(super) async fn load_entrypoint(
    transaction: &mut Transaction<'_, Postgres>,
    query: EntrypointQuery<'_>,
) -> Result<String> {
    sqlx::query_as::<_, ManifestEntry>(
        r#"
        WITH authoritative_manifest AS (
            SELECT manifest_id, chain_id, manifest_payload
            FROM manifest_versions
            WHERE namespace = $1
              AND source_family = $2
              AND chain_id = $3
              AND (
                  rollout_status = 'active'
                  OR ($5 AND rollout_status = 'shadow')
              )
              AND ($7::bigint IS NULL OR manifest_version = $7)
            ORDER BY (rollout_status = 'active') DESC,
                     manifest_version DESC, manifest_id DESC
            LIMIT 1
        )
        SELECT declaration.declared_address
        FROM authoritative_manifest manifest
        JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
         AND declaration.chain_id = manifest.chain_id
        WHERE declaration.role = $4
          AND (
              declaration.start_block_number IS NULL
              OR declaration.start_block_number <= $6
          )
          AND (
              NOT $8
              OR manifest.manifest_payload -> 'capability_flags'
                  -> 'verified_resolution' ->> 'status' = 'supported'
              OR ($5 AND manifest.manifest_payload -> 'capability_flags'
                  -> 'verified_resolution' ->> 'status' = 'shadow')
          )
        ORDER BY declaration.start_block_number DESC NULLS LAST,
                 declaration.declaration_name ASC,
                 declaration.manifest_contract_instance_id ASC
        LIMIT 1
        "#,
    )
    .bind(query.namespace)
    .bind(query.source_family)
    .bind(query.chain_id)
    .bind(query.role)
    .bind(query.allow_shadow)
    .bind(query.execution_block_number)
    .bind(query.required_manifest_version)
    .bind(query.require_resolution_capability)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load lookup entrypoint manifest"))?
    .map(|row| row.declared_address)
    .ok_or_else(|| {
        LookupError::unsupported(format!(
            "no declared {}/{} lookup entrypoint is available",
            query.source_family, query.role
        ))
    })
}
