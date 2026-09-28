use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page, take_json_array,
};

use super::{
    DECLARED_SURFACE_CLASS, DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS,
    DEFAULT_CHILDREN_CURRENT_READ_FILTER,
    types::{
        ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentPageFilter,
        ChildrenCurrentRow, ChildrenCurrentSortValue, ChildrenCurrentSummary, RegistryChildrenPage,
    },
};

/// `NewOwner` carries the labelhash, not the label
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L45 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f),
/// so a registry event proves a child node and its labelhash but not the label itself. Project
/// writes the name columns null until a preimage arrives, and stores raw bytes with no decoded
/// form for a preimage that does not decode. A preimage that decodes but fails normalization keeps
/// its raw bytes in `label_preimages` yet gets both name columns null too: its text must not
/// serve as a name, and the escaped form would reproduce the same misleading text. The arms
/// answer in that order: the decoded name, the escape-encoded raw bytes, and the documented
/// `[<labelhash-without-0x>].<parent-name>` placeholder. The escape arm is therefore reached
/// only by bytes that do not decode; the placeholder covers both a label never observed and an
/// observed label whose text is not name-safe. The placeholder is total, which is what keeps a
/// null out of a mandatory field. The escape arm re-encodes the
/// whole stored string, so a non-ASCII parent portion is octal-escaped too, while the placeholder
/// carries the parent's spelling as-is — and reads it here rather than inheriting the copy Project
/// composed into the name columns. That spelling is empty only for the zero-label
/// root surface, since a parent whose own labels do not decode is written
/// `visibility_state = 'shadow'` and both children projection arms admit active parents only. No
/// route can address the empty name, so the trailing-dot string that root would produce never
/// reaches a served page.
pub(super) const CHILD_DISPLAY_NAME_EXPR: &str = r#"COALESCE(
    cc.decoded_name,
    encode(cc.raw_name, 'escape'),
    '[' || substring(lower(cc.labelhash) FROM 3) || '].' || display_parent.raw_name
)"#;

/// Joined here rather than inside the expression so the parent row is read once per child instead
/// of once per evaluation, and so the audit path — which omits the canonicality identity joins —
/// still resolves the parent portion. `logical_name_id` is the primary key, so this cannot
/// multiply rows.
pub(super) const CHILD_DISPLAY_PARENT_JOIN: &str = r#"
  LEFT JOIN bigname_phase.name_surfaces display_parent
    ON display_parent.logical_name_id = cc.parent_logical_name_id
"#;

/// ENSv2 child rows keep the registration event's raw-log reference first in their provenance;
/// its emitter is the registry contract that holds the label.
const REGISTRY_CHILD_FILTER: &str =
    " AND lower(cc.provenance #>> '{raw_fact_refs,0,registration,emitting_address}') = ";

fn child_select() -> String {
    format!(
        r#"
    SELECT cc.parent_logical_name_id, cc.child_logical_name_id, cc.surface_class,
           cc.namespace,
           {CHILD_DISPLAY_NAME_EXPR} AS canonical_display_name,
           {CHILD_DISPLAY_NAME_EXPR} AS normalized_name,
           cc.namehash, cc.labelhash, cc.owner, cc.registrant, cc.provenance,
           cc.chain_positions, cc.canonicality_summary, cc.manifest_version,
           cc.last_recomputed_at
    FROM bigname_phase.children_current cc
    {CHILD_DISPLAY_PARENT_JOIN}
"#
    )
}

pub async fn load_children_current(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, false).await
}

pub async fn load_children_current_including_noncanonical(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, true).await
}

pub async fn load_children_current_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    super::page::load_children_current_page_filtered(
        pool,
        parent_logical_name_id,
        &ChildrenCurrentPageFilter::default(),
        cursor,
        page_size,
    )
    .await
}

/// A page of the declared children of `parent_logical_name_id` whose ENSv2 registration was
/// emitted by `registry_address`: the labels one registry contract currently holds under the
/// name it serves. `label_count` counts every such child, not just the page.
pub async fn load_registry_children_current_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    registry_address: &str,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<RegistryChildrenPage> {
    super::families::registry_page(
        pool,
        parent_logical_name_id,
        registry_address,
        cursor,
        page_size,
    )
    .await
}

/// Exact count of the declared children of `parent_logical_name_id` whose ENSv2 registration
/// was emitted by `registry_address`.
pub async fn count_registry_children_current(
    pool: &PgPool,
    parent_logical_name_id: &str,
    registry_address: &str,
) -> Result<i64> {
    super::families::registry_count(pool, parent_logical_name_id, registry_address).await
}

pub async fn load_children_current_summaries(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<ChildrenCurrentSummary>> {
    if parent_logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }

    super::families::summaries(pool, parent_logical_name_ids).await
}

pub(super) async fn load_children_current_summary(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<ChildrenCurrentSummary> {
    load_children_current_summaries(pool, &[parent_logical_name_id.to_owned()])
        .await?
        .into_iter()
        .next()
        .context("phase children summary must preserve its requested key")
}

async fn load_children_current_internal(
    pool: &PgPool,
    parent_logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Vec<ChildrenCurrentRow>> {
    let mut query = child_select();
    if !include_noncanonical {
        query.push_str(DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS);
    }
    query.push_str(" WHERE cc.parent_logical_name_id = $1 AND cc.surface_class = $2");
    if !include_noncanonical {
        query.push_str(DEFAULT_CHILDREN_CURRENT_READ_FILTER);
    }
    query.push_str(&format!(
        " ORDER BY {CHILD_DISPLAY_NAME_EXPR}, cc.child_logical_name_id"
    ));
    let rows = sqlx::query(&query)
        .bind(parent_logical_name_id)
        .bind(DECLARED_SURFACE_CLASS)
        .fetch_all(pool)
        .await
        .context("failed to load phase children_current rows")?;
    rows.into_iter().map(decode_children_current_row).collect()
}

pub(super) fn decode_children_current_row(row: PgRow) -> Result<ChildrenCurrentRow> {
    let surface_class: String = crate::sql_row::get(&row, "surface_class")?;
    if surface_class != DECLARED_SURFACE_CLASS {
        bail!("children_current row has unsupported surface_class {surface_class}");
    }
    Ok(ChildrenCurrentRow {
        parent_logical_name_id: crate::sql_row::get(&row, "parent_logical_name_id")?,
        child_logical_name_id: crate::sql_row::get(&row, "child_logical_name_id")?,
        surface_class,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        labelhash: crate::sql_row::get(&row, "labelhash")?,
        owner: crate::sql_row::get(&row, "owner")?,
        registrant: crate::sql_row::get(&row, "registrant")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn decode_children_current_summary(row: PgRow) -> Result<ChildrenCurrentSummary> {
    Ok(ChildrenCurrentSummary {
        parent_logical_name_id: row.try_get("parent_logical_name_id")?,
        child_count: row.try_get("child_count")?,
        provenance_inputs: take_json_array(row.try_get("provenance_inputs")?, || {
            "children summary provenance_inputs must be a JSON array".to_owned()
        })?,
        chain_positions: take_json_array(row.try_get("chain_positions")?, || {
            "children summary chain_positions must be a JSON array".to_owned()
        })?,
        canonicality_summaries: take_json_array(row.try_get("canonicality_summaries")?, || {
            "children summary canonicality_summaries must be a JSON array".to_owned()
        })?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}
