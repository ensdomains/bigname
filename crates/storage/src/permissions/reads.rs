use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::{
    decode::decode_permissions_current_row,
    types::{PermissionScope, PermissionsCurrentRow},
};

pub(super) const DEFAULT_PERMISSIONS_CURRENT_READ_FILTER: &str = r#"
  AND pc.canonicality_summary ->> 'status' IN ('canonical', 'safe', 'finalized')
"#;

/// Load resource-centric permission rows with optional exact subject and scope filters.
pub async fn load_permissions_current(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
) -> Result<Vec<PermissionsCurrentRow>> {
    let scope_storage_key = scope.map(PermissionScope::storage_key);
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            pc.resource_id,
            pc.subject,
            pc.scope,
            pc.scope_kind,
            pc.scope_detail,
            pc.effective_powers,
            pc.grant_source,
            pc.revocation_source,
            pc.inheritance_path,
            pc.transfer_behavior,
            pc.provenance,
            pc.coverage,
            pc.chain_positions,
            pc.canonicality_summary,
            pc.manifest_version,
            pc.last_recomputed_at
        FROM permissions_current pc
        WHERE "#,
    );
    push_permissions_current_filters(
        &mut builder,
        resource_id,
        subject,
        scope_storage_key.as_deref(),
    );

    builder.push(" ORDER BY pc.subject ASC, pc.scope ASC");

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!("failed to load permissions_current rows for resource_id {resource_id}")
    })?;

    rows.into_iter()
        .map(decode_permissions_current_row)
        .collect()
}

/// Load current permission rows for many resources, grouped by resource_id.
pub async fn load_permissions_current_by_resource_ids(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, Vec<PermissionsCurrentRow>>> {
    let mut grouped = resource_ids
        .iter()
        .copied()
        .map(|resource_id| (resource_id, Vec::new()))
        .collect::<BTreeMap<_, _>>();

    if grouped.is_empty() {
        return Ok(grouped);
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            pc.resource_id,
            pc.subject,
            pc.scope,
            pc.scope_kind,
            pc.scope_detail,
            pc.effective_powers,
            pc.grant_source,
            pc.revocation_source,
            pc.inheritance_path,
            pc.transfer_behavior,
            pc.provenance,
            pc.coverage,
            pc.chain_positions,
            pc.canonicality_summary,
            pc.manifest_version,
            pc.last_recomputed_at
        FROM permissions_current pc
        WHERE pc.resource_id IN ("#,
    );
    {
        let mut separated = builder.separated(", ");
        for resource_id in grouped.keys() {
            separated.push_bind(*resource_id);
        }
        separated.push_unseparated(")");
    }
    builder.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
    builder.push(" ORDER BY pc.resource_id ASC, pc.subject ASC, pc.scope ASC");

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!(
            "failed to load permissions_current rows for {} resource_ids",
            grouped.len()
        )
    })?;

    for row in rows {
        let row = decode_permissions_current_row(row)?;
        grouped.entry(row.resource_id).or_default().push(row);
    }

    Ok(grouped)
}

/// Load persisted resolver-scoped permission rows across all resources.
pub async fn load_permissions_current_for_resolver_scope(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Vec<PermissionsCurrentRow>> {
    let scope = PermissionScope::Resolver {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_ascii_lowercase(),
    }
    .storage_key();
    let rows = sqlx::query(
        r#"
        SELECT
            pc.resource_id,
            pc.subject,
            pc.scope,
            pc.scope_kind,
            pc.scope_detail,
            pc.effective_powers,
            pc.grant_source,
            pc.revocation_source,
            pc.inheritance_path,
            pc.transfer_behavior,
            pc.provenance,
            pc.coverage,
            pc.chain_positions,
            pc.canonicality_summary,
            pc.manifest_version,
            pc.last_recomputed_at
        FROM permissions_current pc
        WHERE pc.scope = $1
          AND pc.scope_kind = 'resolver'
          AND pc.canonicality_summary ->> 'status' IN ('canonical', 'safe', 'finalized')
        ORDER BY pc.subject ASC, pc.resource_id ASC, pc.manifest_version ASC
        "#,
    )
    .bind(scope)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load resolver-scoped permissions_current rows for chain {chain_id} resolver {resolver_address}"
        )
    })?;

    rows.into_iter()
        .map(decode_permissions_current_row)
        .collect()
}

/// Discover resolver targets represented by persisted resolver-scoped permission rows.
pub async fn load_permissions_current_resolver_targets(
    pool: &PgPool,
) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            pc.scope_detail->>'chain_id' AS chain_id,
            LOWER(pc.scope_detail->>'resolver_address') AS resolver_address
        FROM permissions_current pc
        WHERE pc.scope_kind = 'resolver'
          AND pc.scope_detail->>'chain_id' IS NOT NULL
          AND pc.scope_detail->>'chain_id' <> ''
          AND pc.scope_detail->>'resolver_address' IS NOT NULL
          AND pc.scope_detail->>'resolver_address' <> ''
          AND pc.canonicality_summary ->> 'status' IN ('canonical', 'safe', 'finalized')
        ORDER BY chain_id, resolver_address
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load resolver targets from permissions_current")?;

    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("chain_id")?,
                row.try_get::<String, _>("resolver_address")?
                    .to_ascii_lowercase(),
            ))
        })
        .collect()
}

pub(super) fn push_permissions_current_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_id: Uuid,
    subject: Option<&'a str>,
    scope_storage_key: Option<&'a str>,
) {
    builder.push("pc.resource_id = ");
    builder.push_bind(resource_id);

    if let Some(subject) = subject {
        builder.push(" AND pc.subject = ");
        builder.push_bind(subject);
    }

    if let Some(scope_storage_key) = scope_storage_key {
        builder.push(" AND pc.scope = ");
        builder.push_bind(scope_storage_key);
    }

    builder.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
}
