//! Reads of the published Project families for scenario assertions. Each helper calls the
//! storage reader the API serves the same data from (`bigname_storage::families`), so a scenario
//! checks what Project published without reproducing its composition in SQL. The readers require
//! a servable family marker, which a completed fixture replay leaves behind.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort, ChildrenCurrentPageFilter, EffectivePermissionRow,
    EffectivePermissionScope, NameCurrentRow, PermissionScope, PrimaryNameCurrentSnapshot,
    RecordInventoryCurrentRow, ResolverCurrentRow,
    families::{
        control::permissions::{ServedApproval, load_shadow_approvals},
        name::load_family_name,
        records::{
            load_family_address_names_page, load_family_primary_name_snapshot,
            load_family_record_inventory,
        },
        topology::{FamilyChildRow, load_children_shadow_page, load_family_resolver_current},
    },
};
use serde_json::Value;
use sqlx::{PgPool, types::Uuid};

const PAGE: u64 = 500;

/// The logical name id of the surface `raw_name` in `namespace`, when one exists.
pub async fn logical_name_id(
    pool: &PgPool,
    namespace: &str,
    raw_name: &str,
) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces
         WHERE namespace = $1 AND raw_name = $2",
    )
    .bind(namespace)
    .bind(raw_name)
    .fetch_optional(pool)
    .await?)
}

/// The composed name row the name routes serve, or none when the name has no readable surface.
pub async fn name(pool: &PgPool, logical_name_id: &str) -> Result<Option<NameCurrentRow>> {
    load_family_name(pool, logical_name_id).await
}

/// [`name`], required to exist.
pub async fn required_name(pool: &PgPool, logical_name_id: &str) -> Result<NameCurrentRow> {
    name(pool, logical_name_id)
        .await?
        .with_context(|| format!("no published name row for {logical_name_id}"))
}

/// [`name`] by namespace and raw name.
pub async fn name_by_raw(
    pool: &PgPool,
    namespace: &str,
    raw_name: &str,
) -> Result<Option<NameCurrentRow>> {
    match logical_name_id(pool, namespace, raw_name).await? {
        Some(id) => name(pool, &id).await,
        None => Ok(None),
    }
}

/// [`name_by_raw`], required to exist.
pub async fn required_name_by_raw(
    pool: &PgPool,
    namespace: &str,
    raw_name: &str,
) -> Result<NameCurrentRow> {
    name_by_raw(pool, namespace, raw_name)
        .await?
        .with_context(|| format!("no published name row for {namespace}:{raw_name}"))
}

/// The coarse support of a composed row, as the exact-name route reports it.
pub fn support_status(row: &NameCurrentRow) -> &'static str {
    if row.coverage.get("status").and_then(Value::as_str) == Some("unsupported") {
        "unsupported"
    } else {
        "supported"
    }
}

/// The published record inventory of `resource_id`, on the chain the resource belongs to.
pub async fn record_inventory(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let chain_id: Option<String> =
        sqlx::query_scalar("SELECT chain_id FROM bigname_phase.resources WHERE resource_id = $1")
            .bind(resource_id)
            .fetch_optional(pool)
            .await?;
    let Some(chain_id) = chain_id else {
        return Ok(None);
    };
    load_family_record_inventory(pool, &chain_id, resource_id).await
}

/// The record inventory the records route serves for a composed name row.
pub async fn name_record_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> Result<Option<RecordInventoryCurrentRow>> {
    match row.record_serving_resource_id() {
        Some(resource_id) => record_inventory(pool, resource_id).await,
        None => Ok(None),
    }
}

/// Every served child of `parent_logical_name_id`, in name order.
pub async fn children(pool: &PgPool, parent_logical_name_id: &str) -> Result<Vec<FamilyChildRow>> {
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let page = load_children_shadow_page(
            pool,
            parent_logical_name_id,
            &ChildrenCurrentPageFilter::default(),
            cursor.as_ref(),
            PAGE,
        )
        .await?;
        rows.extend(page.rows);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(rows),
        }
    }
}

/// How many served child rows name the node `child_namehash`, under any parent a published
/// child edge gives it.
pub async fn served_child_rows(pool: &PgPool, child_namehash: &str) -> Result<usize> {
    let parents: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT namespace || ':' || parent_node
         FROM bigname_phase.project_child_edge_candidate
         WHERE lower(child_node) = lower($1)",
    )
    .bind(child_namehash)
    .fetch_all(pool)
    .await?;
    let mut served = 0;
    for parent in parents {
        served += children(pool, &parent)
            .await?
            .iter()
            .filter(|child| child.namehash.eq_ignore_ascii_case(child_namehash))
            .count();
    }
    Ok(served)
}

async fn permission_rows(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
) -> Result<Vec<EffectivePermissionRow>> {
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let page = bigname_storage::load_serving_effective_permissions_page(
            pool,
            subject,
            resource_id,
            None,
            cursor.as_ref(),
            PAGE,
        )
        .await?;
        rows.extend(page.rows);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(rows),
        }
    }
}

/// Every effective permission row of `resource_id`.
pub async fn resource_permissions(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<EffectivePermissionRow>> {
    permission_rows(pool, None, Some(resource_id)).await
}

/// Every effective permission row of `subject`.
pub async fn subject_permissions(
    pool: &PgPool,
    subject: &str,
) -> Result<Vec<EffectivePermissionRow>> {
    permission_rows(pool, Some(subject), None).await
}

/// Whether a permission row has the resource scope.
pub fn is_resource_scope(row: &EffectivePermissionRow) -> bool {
    row.scope == EffectivePermissionScope::Direct(PermissionScope::Resource)
}

/// The effective powers of a permission row, sorted.
pub fn powers(row: &EffectivePermissionRow) -> Vec<String> {
    let mut powers = row
        .effective_powers
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    powers.sort();
    powers
}

/// The resource-scope rows of `resource_id` as (subject, sorted powers), ordered by subject.
pub async fn resource_scope_powers(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<(String, Vec<String>)>> {
    let mut rows = resource_permissions(pool, resource_id)
        .await?
        .iter()
        .filter(|row| is_resource_scope(row))
        .map(|row| (row.subject.clone(), powers(row)))
        .collect::<Vec<_>>();
    rows.sort();
    Ok(rows)
}

/// The names the address routes list for `address`, one entry per name and resource with all
/// of its relations.
pub async fn address_names(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
) -> Result<Vec<AddressNameCurrentEntry>> {
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = load_family_address_names_page(
            pool,
            &address.to_ascii_lowercase(),
            namespace,
            None,
            AddressNamesCurrentDedupe::Resource,
            None,
            None,
            None,
            AddressNamesCurrentSort::Name,
            AddressNamesCurrentOrder::Asc,
            cursor.as_ref(),
            PAGE,
        )
        .await?;
        entries.extend(page.entries);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(entries),
        }
    }
}

/// The addresses Project indexes under `logical_name_id` for `relation`: every address the
/// relation can take, a superset of the addresses the address routes serve it for.
pub async fn indexed_addresses(
    pool: &PgPool,
    logical_name_id: &str,
    relation: &str,
) -> Result<BTreeSet<String>> {
    Ok(sqlx::query_scalar(
        "SELECT address FROM bigname_phase.project_address_name_index
         WHERE logical_name_id = $1 AND relation = $2",
    )
    .bind(logical_name_id)
    .bind(relation)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

/// The declared primary-name claim of one reverse tuple.
pub async fn primary_name(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentSnapshot>> {
    load_family_primary_name_snapshot(pool, address, namespace, coin_type).await
}

/// The resolver overview row of `resolver_address` on `chain_id`.
pub async fn resolver(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    load_family_resolver_current(pool, chain_id, resolver_address).await
}

/// Every registry account approval Project holds for `chain_id`, approved or revoked.
pub async fn approvals(pool: &PgPool, chain_id: &str) -> Result<Vec<ServedApproval>> {
    load_shadow_approvals(pool, chain_id).await
}

/// Every row of every family table, each table's rows as sorted JSON text, for comparing the
/// publications of two databases. The families are the `project_*` tables other than the
/// marker, undo journal and repair record, plus the retained child registration history, whose
/// two wall-clock audit columns are left out. Contract instance ids, which each database mints
/// independently, are replaced by their chain and address.
pub async fn family_rows(pool: &PgPool) -> Result<BTreeMap<String, Vec<String>>> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename::text FROM pg_tables
         WHERE schemaname = 'bigname_phase'
           AND ((tablename LIKE 'project\\_%'
                 AND tablename NOT IN ('project_family_marker', 'project_family_undo',
                                       'project_repair_record'))
                OR tablename = 'child_registration_events')
         ORDER BY tablename",
    )
    .fetch_all(pool)
    .await?;
    ensure!(
        tables.iter().any(|table| table == "project_name_summary"),
        "no family tables found: {tables:?}"
    );
    let instances = super::perturb::contract_instance_stable_keys(pool).await?;
    let mut out = BTreeMap::new();
    for table in tables {
        let rows: Vec<Value> =
            sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM bigname_phase.{table} t"))
                .fetch_all(pool)
                .await?;
        let mut rows = rows
            .into_iter()
            .map(|mut row| {
                super::perturb::normalize_contract_instance_ids(&mut row, &instances);
                serde_json::to_string(&row)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.sort();
        out.insert(table, rows);
    }
    Ok(out)
}

/// Fails with the first differing table when two family snapshots differ.
pub fn assert_family_rows_equal(
    expected: &BTreeMap<String, Vec<String>>,
    actual: &BTreeMap<String, Vec<String>>,
    context: &str,
) -> Result<()> {
    let expected_tables = expected.keys().collect::<Vec<_>>();
    let actual_tables = actual.keys().collect::<Vec<_>>();
    ensure!(
        expected_tables == actual_tables,
        "{context}: family table sets differ: {expected_tables:?} vs {actual_tables:?}"
    );
    for (table, rows) in expected {
        let other = &actual[table];
        if rows != other {
            let missing = rows.iter().filter(|row| !other.contains(row)).take(3);
            let extra = other.iter().filter(|row| !rows.contains(row)).take(3);
            anyhow::bail!(
                "{context}: family table {table} differs ({} vs {} rows)\nonly expected: {:?}\nonly actual: {:?}",
                rows.len(),
                other.len(),
                missing.collect::<Vec<_>>(),
                extra.collect::<Vec<_>>()
            );
        }
    }
    Ok(())
}
