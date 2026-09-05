use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::{
    canonicality::{
        CURRENT_PERMISSION_SUMMARY_READ_FILTER, DEFAULT_PERMISSIONS_CURRENT_READ_FILTER,
    },
    decode::{decode_effective_permission_row, decode_permissions_current_full_filter_summary},
    types::{
        EffectivePermissionRow, EffectivePermissionsAccountResourcePage,
        PermissionsCurrentAccountResourceCursor, PermissionsCurrentFullFilterSummary,
    },
};
use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page,
};

const ACCOUNT_READ_FILTER: &str = r#"
 AND aps.canonicality_summary->>'state' IN ('canonical','safe','finalized')
 AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage account_lineage
   WHERE account_lineage.chain_id=aps.chain_id
     AND account_lineage.block_hash=aps.chain_positions->>'target_block_hash'
     AND account_lineage.canonicality_state IN ('canonical','safe','finalized'))
 AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage binding_lineage
   WHERE binding_lineage.chain_id=summary.registry_binding_provenance->>'chain_id'
     AND binding_lineage.block_hash=summary.registry_binding_chain_positions->>'block_hash'
     AND binding_lineage.canonicality_state IN ('canonical','safe','finalized'))
"#;

const DIRECT_COLUMNS: &str = r#"pc.resource_id,pc.subject,pc.scope AS scope_storage_key,
 pc.scope_kind,pc.scope_detail,NULL::text AS grant_relation,pc.effective_powers,
 pc.grant_source,pc.revocation_source,pc.inheritance_path,pc.transfer_behavior,
 pc.provenance,jsonb_build_object('status','projected','exhaustiveness','not_asserted') AS coverage,
 pc.chain_positions,pc.canonicality_summary,pc.manifest_version,pc.last_recomputed_at"#;
const OPERATOR_COLUMNS: &str = r#"summary.resource_id,aps.subject,
 'account:'||aps.chain_id||':'||aps.authority_kind||':'||aps.authority_contract||':'||aps.owner AS scope_storage_key,
 'account'::text AS scope_kind,jsonb_build_object('chain_id',aps.chain_id,'authority_kind',aps.authority_kind,
 'authority_contract',aps.authority_contract,'owner',aps.owner) AS scope_detail,
 'operator'::text AS grant_relation,aps.effective_powers,aps.grant_source,NULL::jsonb AS revocation_source,
 aps.inheritance_path,aps.transfer_behavior,
 jsonb_build_object('account',aps.provenance,'registry_binding',summary.registry_binding_provenance) AS provenance,
 jsonb_build_object('status','projected','exhaustiveness','not_asserted') AS coverage,
 jsonb_build_object('account',aps.chain_positions,'registry_binding',summary.registry_binding_chain_positions) AS chain_positions,
 jsonb_build_object('account',aps.canonicality_summary,'registry_binding',summary.canonicality_summary) AS canonicality_summary,
 GREATEST(aps.manifest_version,summary.manifest_version) AS manifest_version,
 GREATEST(aps.last_recomputed_at,summary.last_recomputed_at) AS last_recomputed_at"#;

fn push_union_start(builder: &mut QueryBuilder<'_, Postgres>) {
    builder
        .push("WITH direct_candidates AS (SELECT ")
        .push(DIRECT_COLUMNS)
        .push(" FROM bigname_phase.permissions_current pc WHERE TRUE");
}

fn push_operator_start(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push("), operator_candidates AS (SELECT ").push(OPERATOR_COLUMNS).push(
        " FROM bigname_phase.account_permission_state_current aps \
         JOIN bigname_phase.permissions_current_resource_summary summary \
           ON summary.registry_binding_provenance->>'chain_id'=aps.chain_id \
          AND summary.registry_contract=aps.authority_contract \
          AND summary.registry_owner=aps.owner \
         WHERE aps.approved AND aps.authority_kind='registry' AND aps.relation_kind='operator' AND ",
    ).push(CURRENT_PERMISSION_SUMMARY_READ_FILTER).push(ACCOUNT_READ_FILTER);
}

fn push_union_end(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(") , effective_permissions AS (SELECT * FROM direct_candidates UNION ALL SELECT * FROM operator_candidates) ");
}

pub(super) fn push_effective_permission_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    alias: &str,
    subject: Option<&'a str>,
    resource_id: Option<Uuid>,
) {
    if let Some(subject) = subject {
        builder
            .push(" AND ")
            .push(alias)
            .push(".subject=")
            .push_bind(subject);
    }
    if let Some(resource_id) = resource_id {
        builder
            .push(" AND ")
            .push(alias)
            .push(".resource_id=")
            .push_bind(resource_id);
    }
}

fn push_namespace_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource: &str,
    namespace: Option<&'a str>,
) {
    if let Some(namespace) = namespace {
        builder.push(" AND EXISTS (SELECT 1 FROM bigname_phase.surface_bindings sb JOIN bigname_phase.name_surfaces ns ON ns.logical_name_id=sb.logical_name_id JOIN bigname_phase.chain_lineage ns_binding_lineage ON ns_binding_lineage.chain_id=sb.chain_id AND ns_binding_lineage.block_hash=sb.block_hash JOIN bigname_phase.chain_lineage ns_surface_lineage ON ns_surface_lineage.chain_id=ns.chain_id AND ns_surface_lineage.block_hash=ns.block_hash WHERE sb.resource_id=")
            .push(resource).push(" AND ns.namespace=").push_bind(namespace)
            .push(" AND sb.canonicality_state IN ('canonical','safe','finalized') AND ns.canonicality_state IN ('canonical','safe','finalized') AND ns_binding_lineage.canonicality_state IN ('canonical','safe','finalized') AND ns_surface_lineage.canonicality_state IN ('canonical','safe','finalized'))");
    }
}

pub(super) fn push_effective_permission_cursor<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    alias: &str,
    resource: &str,
    scope: &str,
    cursor: Option<&'a PermissionsCurrentAccountResourceCursor>,
) {
    if let Some(cursor) = cursor {
        builder
            .push(" AND (")
            .push(alias)
            .push(r#".subject COLLATE "C","#)
            .push(resource)
            .push(",")
            .push(scope)
            .push(r#" COLLATE "C")>("#)
            .push_bind(&cursor.subject)
            .push(r#" COLLATE "C","#)
            .push_bind(cursor.resource_id)
            .push(",")
            .push_bind(&cursor.scope)
            .push(r#" COLLATE "C")"#);
    }
}

fn build_page<'a>(
    prefix: &str,
    subject: Option<&'a str>,
    resource_id: Option<Uuid>,
    namespace: Option<&'a str>,
    cursor: Option<&'a PermissionsCurrentAccountResourceCursor>,
    limit: i64,
) -> QueryBuilder<'a, Postgres> {
    let mut b = QueryBuilder::new(prefix);
    push_union_start(&mut b);
    push_effective_permission_filters(&mut b, "pc", subject, resource_id);
    push_namespace_filter(&mut b, "pc.resource_id", namespace);
    push_effective_permission_cursor(&mut b, "pc", "pc.resource_id", "pc.scope", cursor);
    b.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER)
        .push(r#" ORDER BY pc.subject COLLATE "C",pc.resource_id,pc.scope COLLATE "C" LIMIT "#)
        .push_bind(limit);
    push_operator_start(&mut b);
    push_effective_permission_filters(&mut b, "aps", subject, None);
    if let Some(resource_id) = resource_id {
        b.push(" AND summary.resource_id=").push_bind(resource_id);
    }
    push_namespace_filter(&mut b, "summary.resource_id", namespace);
    let account_scope = "('account:'||aps.chain_id||':'||aps.authority_kind||':'||aps.authority_contract||':'||aps.owner)";
    push_effective_permission_cursor(&mut b, "aps", "summary.resource_id", account_scope, cursor);
    b.push(r#" ORDER BY aps.subject COLLATE "C",summary.resource_id,"#)
        .push(account_scope)
        .push(r#" COLLATE "C" LIMIT "#)
        .push_bind(limit);
    push_union_end(&mut b);
    b.push(r#"SELECT * FROM effective_permissions ORDER BY subject COLLATE "C",resource_id,scope_storage_key COLLATE "C" LIMIT "#).push_bind(limit);
    b
}

pub(super) fn build_effective_permissions_account_resource_page_query<'a>(
    subject: Option<&'a str>,
    resource_id: Option<Uuid>,
    namespace: Option<&'a str>,
    cursor: Option<&'a PermissionsCurrentAccountResourceCursor>,
    limit: i64,
) -> QueryBuilder<'a, Postgres> {
    build_page("", subject, resource_id, namespace, cursor, limit)
}

fn build_summary<'a>(
    prefix: &str,
    subject: Option<&'a str>,
    resource_id: Option<Uuid>,
    namespace: Option<&'a str>,
    full: bool,
) -> QueryBuilder<'a, Postgres> {
    let mut b = QueryBuilder::new(prefix);
    push_union_start(&mut b);
    push_effective_permission_filters(&mut b, "pc", subject, resource_id);
    push_namespace_filter(&mut b, "pc.resource_id", namespace);
    b.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
    push_operator_start(&mut b);
    push_effective_permission_filters(&mut b, "aps", subject, None);
    if let Some(resource_id) = resource_id {
        b.push(" AND summary.resource_id=").push_bind(resource_id);
    }
    push_namespace_filter(&mut b, "summary.resource_id", namespace);
    push_union_end(&mut b);
    if full {
        b.push(r#"SELECT COUNT(*)::bigint AS row_count,
        COALESCE(jsonb_agg(provenance ORDER BY subject,resource_id,scope_storage_key),'[]') AS provenance,
        (jsonb_agg(coverage ORDER BY subject,resource_id,scope_storage_key)->0) AS coverage,
        COALESCE(jsonb_agg(chain_positions ORDER BY subject,resource_id,scope_storage_key),'[]') AS chain_positions,
        COALESCE(jsonb_agg(canonicality_summary ORDER BY subject,resource_id,scope_storage_key),'[]') AS canonicality_summaries,
        MAX(last_recomputed_at) AS last_recomputed_at FROM effective_permissions"#);
    } else {
        b.push("SELECT COUNT(*)::bigint AS row_count,'[]'::jsonb AS provenance,NULL::jsonb AS coverage,'[]'::jsonb AS chain_positions,'[]'::jsonb AS canonicality_summaries,NULL::timestamptz AS last_recomputed_at FROM effective_permissions");
    }
    b
}
pub(super) fn build_effective_permissions_account_resource_summary_query<'a>(
    subject: Option<&'a str>,
    resource_id: Option<Uuid>,
    namespace: Option<&'a str>,
) -> QueryBuilder<'a, Postgres> {
    build_summary("", subject, resource_id, namespace, true)
}

fn build_batch<'a>(
    prefix: &'static str,
    resource_ids: &'a [Uuid],
    namespace: Option<&'a str>,
) -> QueryBuilder<'a, Postgres> {
    let mut b = QueryBuilder::new(prefix);
    push_union_start(&mut b);
    push_ids(&mut b, "pc.resource_id", resource_ids);
    push_namespace_filter(&mut b, "pc.resource_id", namespace);
    b.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
    push_operator_start(&mut b);
    push_ids(&mut b, "summary.resource_id", resource_ids);
    push_namespace_filter(&mut b, "summary.resource_id", namespace);
    push_union_end(&mut b);
    b.push(r#"SELECT * FROM effective_permissions ORDER BY resource_id,subject COLLATE "C",scope_storage_key COLLATE "C""#);
    b
}
pub(super) fn build_effective_permissions_by_resource_ids_query<'a>(
    resource_ids: &'a [Uuid],
    namespace: Option<&'a str>,
) -> QueryBuilder<'a, Postgres> {
    build_batch("", resource_ids, namespace)
}

fn push_ids(builder: &mut QueryBuilder<'_, Postgres>, column: &str, ids: &[Uuid]) {
    builder.push(" AND ").push(column).push(" IN (");
    let mut separated = builder.separated(",");
    for id in ids {
        separated.push_bind(*id);
    }
    separated.push_unseparated(")");
}

pub async fn load_effective_permissions_account_resource_page(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<EffectivePermissionsAccountResourcePage> {
    load_page(
        pool,
        subject,
        resource_id,
        namespace,
        cursor,
        page_size,
        false,
    )
    .await
}
pub async fn load_effective_permissions_account_resource_page_count_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<EffectivePermissionsAccountResourcePage> {
    load_page(
        pool,
        subject,
        resource_id,
        namespace,
        cursor,
        page_size,
        true,
    )
    .await
}
async fn load_page(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
    count: bool,
) -> Result<EffectivePermissionsAccountResourcePage> {
    if subject.is_none() && resource_id.is_none() {
        bail!("effective permissions require subject or resource_id")
    }
    let limit = checked_page_limit_i64(
        page_size,
        "effective permissions page_size must be positive",
        "effective permissions page_size is too large",
    )?;
    let size = checked_page_size_usize(
        page_size,
        "effective permissions page_size must be positive",
        "effective permissions page_size must fit usize",
    )?;
    let rows = build_effective_permissions_account_resource_page_query(
        subject,
        resource_id,
        namespace,
        cursor,
        limit,
    )
    .build()
    .fetch_all(pool)
    .await
    .context("load effective permissions page")?
    .into_iter()
    .map(decode_effective_permission_row)
    .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, size, |row| {
        PermissionsCurrentAccountResourceCursor::from(row)
    });
    let summary = if count {
        Some(load_summary(pool, subject, resource_id, namespace, false).await?)
    } else {
        None
    };
    Ok(EffectivePermissionsAccountResourcePage {
        rows,
        next_cursor,
        summary,
    })
}
async fn load_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    full: bool,
) -> Result<PermissionsCurrentFullFilterSummary> {
    let mut builder = if full {
        build_effective_permissions_account_resource_summary_query(subject, resource_id, namespace)
    } else {
        build_summary("", subject, resource_id, namespace, false)
    };
    let row = builder.build().fetch_one(pool).await?;
    decode_permissions_current_full_filter_summary(row)
}
pub async fn load_effective_permissions_by_resource_ids(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
) -> Result<Vec<EffectivePermissionRow>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    build_effective_permissions_by_resource_ids_query(ids, namespace)
        .build()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(decode_effective_permission_row)
        .collect()
}

async fn explain(pool: &PgPool, mut builder: QueryBuilder<'_, Postgres>) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut *tx)
        .await?;
    let row = builder.build().fetch_one(&mut *tx).await?;
    Ok(row.try_get(0)?)
}
pub async fn explain_effective_permissions_account_resource_page(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<Value> {
    let limit = checked_page_limit_i64(page_size, "positive", "large")?;
    explain(
        pool,
        build_page(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ",
            subject,
            resource_id,
            namespace,
            cursor,
            limit,
        ),
    )
    .await
}
pub async fn explain_effective_permissions_account_resource_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
) -> Result<Value> {
    explain(
        pool,
        build_summary(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ",
            subject,
            resource_id,
            None,
            true,
        ),
    )
    .await
}
pub async fn explain_effective_permissions_by_resource_ids(
    pool: &PgPool,
    ids: &[Uuid],
) -> Result<Value> {
    explain(
        pool,
        build_batch("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ", ids, None),
    )
    .await
}
