//! Exact name and subname values returned by permanent family readers.
use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use bigname_storage::{
    ChildrenCurrentPageFilter, ChildrenCurrentRow, NameCurrentRow,
    load_children_current_page_filtered, load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use sqlx::PgPool;

const NAME_CHUNK: usize = 500;

/// What the name and subname readers serve for every key in the database.
pub struct Served {
    pub names: BTreeMap<String, Value>,
    pub children: BTreeMap<(String, String), Value>,
    pub pages_read: usize,
    /// Parents with at least one stored subname row.
    pub parents: usize,
}

impl Served {
    /// `children_page` is the subname page size; a small one makes every parent span several
    /// pages.
    pub async fn read(pool: &PgPool, children_page: u64) -> Result<Self> {
        let keys: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT logical_name_id FROM name_surfaces ORDER BY 1")
                .fetch_all(pool)
                .await?;
        let mut names = BTreeMap::new();
        for chunk in keys.chunks(NAME_CHUNK) {
            let rows = load_name_current_by_logical_name_ids(pool, chunk).await?;
            for (key, row) in rows {
                names.insert(key, name_json(&row));
            }
        }
        let parents: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT logical_name_id FROM name_surfaces ORDER BY 1")
                .fetch_all(pool)
                .await?;
        let mut children = BTreeMap::new();
        let mut pages_read = 0;
        let mut parent_count = 0;
        for parent in parents {
            let mut cursor = None;
            let mut served = 0_u64;
            let mut admitted = None;
            let mut pages = 0;
            loop {
                pages += 1;
                let page = load_children_current_page_filtered(
                    pool,
                    &parent,
                    &ChildrenCurrentPageFilter::default(),
                    cursor.as_ref(),
                    children_page,
                )
                .await?;
                admitted.get_or_insert(page.total_count);
                for row in page.rows {
                    served += 1;
                    let key = (parent.clone(), row.child_logical_name_id.clone());
                    ensure!(
                        children.insert(key, child_json(&row)).is_none(),
                        "the subname reader served a child of {parent} twice"
                    );
                }
                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            if served > 0 {
                parent_count += 1;
                pages_read += pages;
            }
            ensure!(
                admitted == Some(served),
                "the subname pages of {parent} served {served} of {admitted:?} children"
            );
        }
        Ok(Self {
            names,
            children,
            pages_read,
            parents: parent_count,
        })
    }

    pub fn subname_rows(&self) -> usize {
        self.children.len()
    }
}

fn name_json(row: &NameCurrentRow) -> Value {
    json!({
        "logical_name_id": row.logical_name_id,
        "namespace": row.namespace,
        "canonical_display_name": row.canonical_display_name,
        "normalized_name": row.normalized_name,
        "namehash": row.namehash,
        "surface_binding_id": row.surface_binding_id.map(|id| id.to_string()),
        "resource_id": row.resource_id.map(|id| id.to_string()),
        "serving_resource_id": row.serving_resource_id.map(|id| id.to_string()),
        "token_lineage_id": row.token_lineage_id.map(|id| id.to_string()),
        "binding_kind": row.binding_kind.map(|kind| format!("{kind:?}")),
        "declared_summary": row.declared_summary,
        "provenance": row.provenance,
        "coverage": row.coverage,
        "chain_positions": row.chain_positions,
        "canonicality_summary": row.canonicality_summary,
        "manifest_version": row.manifest_version,
    })
}

fn child_json(row: &ChildrenCurrentRow) -> Value {
    json!({
        "parent_logical_name_id": row.parent_logical_name_id,
        "child_logical_name_id": row.child_logical_name_id,
        "surface_class": row.surface_class,
        "namespace": row.namespace,
        "canonical_display_name": row.canonical_display_name,
        "normalized_name": row.normalized_name,
        "namehash": row.namehash,
        "labelhash": row.labelhash,
        "owner": row.owner,
        "registrant": row.registrant,
        "provenance": row.provenance,
        "chain_positions": row.chain_positions,
        "canonicality_summary": row.canonicality_summary,
        "manifest_version": row.manifest_version,
    })
}
