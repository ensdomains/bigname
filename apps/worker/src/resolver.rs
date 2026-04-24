use anyhow::{Context, Result, bail};
use bigname_storage::{
    ResolverCurrentRow, clear_resolver_current, delete_resolver_current,
    upsert_resolver_current_rows,
};
use sqlx::PgPool;

mod profile;
mod state_helpers;
mod summary_json;
mod target_loading;

use profile::ResolverProfileGate;
use summary_json::build_resolver_current_row;
use target_loading::{ResolverTarget, load_target_resolvers, normalize_resolver_address};

#[cfg(test)]
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::{Row, types::time::OffsetDateTime};
#[cfg(test)]
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
#[cfg(test)]
const BASENAMES_NAMESPACE: &str = "basenames";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const RESOLVER_CURRENT_DERIVATION_KIND: &str = "resolver_current_rebuild";
const RESOLVER_CURRENT_ENUMERATION_BASIS: &str = "resolver_overview";
const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
const RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION: &str = "resolver_authorization";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD: &str = "resolver_record";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION: &str = "resolver_record_version";
const RESOLVER_FAMILY_PENDING_REASON: &str = "resolver_family_pending";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolverCurrentRebuildSummary {
    pub requested_resolver_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_resolver_current(
    pool: &PgPool,
    chain_id: Option<&str>,
    resolver_address: Option<&str>,
) -> Result<ResolverCurrentRebuildSummary> {
    match (chain_id, resolver_address) {
        (Some(chain_id), Some(resolver_address)) => {
            rebuild_one_resolver(pool, chain_id, resolver_address).await
        }
        (None, None) => rebuild_all_resolvers(pool).await,
        _ => bail!(
            "resolver_current rebuild requires both chain_id and resolver_address when targeting one resolver"
        ),
    }
}

async fn rebuild_all_resolvers(pool: &PgPool) -> Result<ResolverCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let targets = load_target_resolvers(pool).await?;

    let mut rows = Vec::with_capacity(targets.len());
    for target in &targets {
        if let Some(row) = build_resolver_current_row(pool, &profile_gate, target).await? {
            rows.push(row);
        }
    }

    let upserted_row_count = upsert_resolver_current_rows(pool, &rows).await?.len();
    let deleted_row_count = delete_stale_resolver_current_rows(pool, &rows).await?;
    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count: targets.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_resolver(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<ResolverCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let target = ResolverTarget {
        chain_id: chain_id.to_owned(),
        resolver_address: normalize_resolver_address(resolver_address),
    };
    let Some(row) = build_resolver_current_row(pool, &profile_gate, &target).await? else {
        let deleted_row_count =
            delete_resolver_current(pool, &target.chain_id, &target.resolver_address).await?;
        return Ok(ResolverCurrentRebuildSummary {
            requested_resolver_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_resolver_current_rows(pool, &[row]).await?.len();
    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count: 1,
        upserted_row_count,
        deleted_row_count: 0,
    })
}

async fn delete_stale_resolver_current_rows(
    pool: &PgPool,
    rows: &[ResolverCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_resolver_current(pool).await;
    }

    let chain_ids = rows
        .iter()
        .map(|row| row.chain_id.clone())
        .collect::<Vec<_>>();
    let resolver_addresses = rows
        .iter()
        .map(|row| row.resolver_address.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM resolver_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS replacement(chain_id, resolver_address)
            WHERE replacement.chain_id = current.chain_id
              AND replacement.resolver_address = current.resolver_address
        )
        "#,
    )
    .bind(&chain_ids)
    .bind(&resolver_addresses)
    .execute(pool)
    .await
    .context("failed to delete stale resolver_current rows after rebuild")
    .map(|result| result.rows_affected())
}

#[cfg(test)]
mod tests;
