//! Reader-level comparison between a committed batch and a committed rebuild at the same block.
//!
//! Every name in either state is read through the name reader, and every subname page of every
//! parent through the subname reader, so the keys compared do not depend on what the batch chose
//! to rewrite.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{
    ChildrenCurrentPageFilter, ChildrenCurrentRow, NameCurrentRow,
    load_children_current_page_filtered, load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::CHAIN;

const NAME_CHUNK: usize = 500;

/// What the name and subname readers serve for every key in the database.
pub struct Served {
    pub names: BTreeMap<String, Value>,
    pub children: BTreeMap<(String, String), Value>,
    pub pages_read: usize,
}

impl Served {
    /// `children_page` is the subname page size; a small one makes every parent span several
    /// pages.
    pub async fn read(pool: &PgPool, children_page: u64) -> Result<Self> {
        let keys: Vec<String> =
            sqlx::query_scalar("SELECT logical_name_id FROM name_current ORDER BY 1")
                .fetch_all(pool)
                .await?;
        let mut names = BTreeMap::new();
        for chunk in keys.chunks(NAME_CHUNK) {
            let rows = load_name_current_by_logical_name_ids(pool, chunk).await?;
            for (key, row) in rows {
                names.insert(key, name_json(&row));
            }
        }
        let parents: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT parent_logical_name_id FROM children_current ORDER BY 1",
        )
        .fetch_all(pool)
        .await?;
        let mut children = BTreeMap::new();
        let mut pages_read = 0;
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
            pages_read += pages;
            ensure!(
                admitted == Some(served),
                "the subname pages of {parent} served {served} of {admitted:?} children"
            );
        }
        Ok(Self {
            names,
            children,
            pages_read,
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

/// The block a rebuild stamps on every row, and its timestamp for name rows.
pub struct Target {
    pub number: i64,
    pub hash: String,
    pub timestamp: Value,
}

impl Target {
    pub async fn load(pool: &PgPool, number: i64, hash: &str) -> Result<Self> {
        let timestamp = sqlx::query_scalar(
            "SELECT to_jsonb(block_timestamp) FROM chain_lineage
             WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3",
        )
        .bind(CHAIN)
        .bind(number)
        .bind(hash)
        .fetch_one(pool)
        .await
        .context("target timestamp")?;
        Ok(Self {
            number,
            hash: hash.to_owned(),
            timestamp,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Name,
    Child,
}

/// The row as a rebuild at `target` would stamp it: the one difference a row the batch kept may
/// show against the rebuild.
pub fn refresh(kind: Kind, row: &Value, target: &Target) -> Result<Value> {
    fn replace(row: &mut Value, path: &str, value: Value) -> Result<()> {
        let slot = row
            .pointer_mut(path)
            .with_context(|| format!("unsupported target metadata shape at {path}"))?;
        ensure!(!slot.is_null(), "null target metadata at {path}");
        *slot = value;
        Ok(())
    }
    let mut refreshed = row.clone();
    match kind {
        Kind::Name => {
            let prefix = format!("/chain_positions/{CHAIN}");
            replace(
                &mut refreshed,
                &format!("{prefix}/block_number"),
                json!(target.number),
            )?;
            replace(
                &mut refreshed,
                &format!("{prefix}/block_hash"),
                json!(target.hash),
            )?;
            replace(
                &mut refreshed,
                &format!("{prefix}/timestamp"),
                target.timestamp.clone(),
            )?;
        }
        Kind::Child => {
            replace(
                &mut refreshed,
                "/chain_positions/target_block_number",
                json!(target.number),
            )?;
            replace(
                &mut refreshed,
                "/chain_positions/target_block_hash",
                json!(target.hash),
            )?;
        }
    }
    replace(
        &mut refreshed,
        "/canonicality_summary/target_block_number",
        json!(target.number),
    )?;
    replace(
        &mut refreshed,
        "/canonicality_summary/target_block_hash",
        json!(target.hash),
    )?;
    Ok(refreshed)
}

#[derive(Debug, Default, PartialEq)]
pub struct Outcome {
    pub exact: usize,
    pub retained: usize,
}

/// Every key must be served in both states, and every row must match the rebuild exactly or
/// after the target refresh.
pub fn compare(candidate: &Served, rebuilt: &Served, target: &Target) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    compare_family(
        Kind::Name,
        &candidate.names,
        &rebuilt.names,
        target,
        &mut outcome,
    )?;
    compare_family(
        Kind::Child,
        &candidate.children,
        &rebuilt.children,
        target,
        &mut outcome,
    )?;
    Ok(outcome)
}

fn compare_family<K: Ord + std::fmt::Debug>(
    kind: Kind,
    candidate: &BTreeMap<K, Value>,
    rebuilt: &BTreeMap<K, Value>,
    target: &Target,
    outcome: &mut Outcome,
) -> Result<()> {
    let candidate_keys = candidate.keys().collect::<BTreeSet<_>>();
    let rebuilt_keys = rebuilt.keys().collect::<BTreeSet<_>>();
    if candidate_keys != rebuilt_keys {
        let missing = rebuilt_keys.difference(&candidate_keys).next();
        let extra = candidate_keys.difference(&rebuilt_keys).next();
        bail!("{kind:?} keys differ from the rebuild: missing {missing:?}, extra {extra:?}");
    }
    for (key, kept) in candidate {
        let expected = &rebuilt[key];
        if kept == expected {
            outcome.exact += 1;
            continue;
        }
        ensure!(
            &refresh(kind, kept, target)? == expected,
            "{kind:?} row {key:?} differs from the rebuild: {kept} != {expected}"
        );
        outcome.retained += 1;
    }
    Ok(())
}
