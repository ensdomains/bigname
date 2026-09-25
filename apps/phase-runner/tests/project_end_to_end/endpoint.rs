//! Reader-value comparison between a committed batch and a full rebuild committed at the same
//! block.
//!
//! Every name in either state is read through the name reader, and every subname page of every
//! parent through the subname reader, so the keys compared do not depend on what the batch chose
//! to rewrite. The comparison is about the values the readers serve: a row equal to the rebuild
//! passes whatever the batch did to it, and a row that differs passes only under one explicit
//! retained-row rule (see [`Retention`]). A key the baseline served that neither later state
//! serves is counted, as removed when the batch had to rewrite it and as dropped otherwise.
//!
//! This is not the rollback benchmark's contract oracle and is not equivalent to it. The oracle's
//! reference is the legacy algorithm run over the same window, so it can require byte-equal rows,
//! tell mandatory from legacy-only ownership, and reject a refresh the batch did not need. Here
//! the reference is a full rebuild, which stamps its target block on every row, including correct
//! rows outside both incremental scopes, so those rules do not carry over. The oracle stays the
//! full-scope, byte-level contract; this comparison checks what a client reads after a real
//! commit.
//!
//! Out of scope here: columns the readers do not serve, such as `inserted_at` and
//! `last_recomputed_at`, so a rewrite that changes only those timestamps is invisible to this
//! comparison and is left to the oracle; and rows the storage readers filter out in both states.
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
    /// Parents with at least one stored subname row.
    pub parents: usize,
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
        let parent_count = parents.len();
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
    /// Rows equal to the rebuild.
    pub exact: usize,
    /// Rows that differ from the rebuild only in the target block, under the retained-row rule.
    pub retained: usize,
    /// Baseline keys neither later state serves, where the batch had to rewrite the key.
    pub removed: usize,
    /// Baseline keys neither later state serves, with no changed event in the window naming them.
    /// The rebuild agrees they are gone, so this is not a served-value difference, but the
    /// harness cannot tell whether the batch's full scope covered them; the fixture requires
    /// none.
    pub dropped: usize,
}

/// The retained-row rule, the one way a served row may differ from the rebuild: it is the row
/// the baseline served, the batch was not required to rewrite it, the block it names is readable
/// history at or before the previous publication (looked up by number and hash together, so a
/// real hash under the wrong number is refused), both target stamps name that block, and the
/// rebuild differs from it only in the target block it stamps.
///
/// Which keys the batch had to rewrite comes, in the rollback oracle, from an independent scope
/// audit that is test-only code inside `bigname-project`. The harness cannot run it, so
/// `required_names` holds the part of that scope it can compute itself: every name with a
/// changed event in the batch window, selected as the scope selects them. A kept row inside the
/// full scope but outside that part is not caught here; the rollback oracle applies the full
/// scope.
pub struct Retention {
    pub baseline: Served,
    pub required_names: BTreeSet<String>,
    pub previous: i64,
    /// Timestamps of the readable blocks rows name, by number and hash.
    pub history: BTreeMap<(i64, String), Value>,
}

impl Retention {
    pub async fn load(
        pool: &PgPool,
        baseline: Served,
        window: (i64, i64),
        previous: i64,
    ) -> Result<Self> {
        // The changed-event predicate of `stage_changed_events` in
        // crates/project/src/scope.rs (lines 144 to 155), whose names seed the scope
        // (`seed_direct_scope`): activated, readable events on readable blocks in the window.
        let required_names = sqlx::query_scalar(
            "SELECT DISTINCT event.logical_name_id
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_number = event.block_number
              AND lineage.block_hash = event.block_hash
             WHERE event.chain_id = $1
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND event.block_number BETWEEN $2 AND $3
               AND event.logical_name_id IS NOT NULL",
        )
        .bind(CHAIN)
        .bind(window.0)
        .bind(window.1)
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect();
        let mut blocks = BTreeSet::new();
        for (kind, row) in baseline
            .names
            .values()
            .map(|row| (Kind::Name, row))
            .chain(baseline.children.values().map(|row| (Kind::Child, row)))
        {
            if let Ok((number, hash)) = named_block(kind, row) {
                blocks.insert((number, hash.to_owned()));
            }
        }
        let (numbers, hashes): (Vec<i64>, Vec<String>) = blocks.into_iter().unzip();
        let history = sqlx::query_as::<_, (i64, String, Value)>(
            "SELECT lineage.block_number, lineage.block_hash, to_jsonb(lineage.block_timestamp)
             FROM unnest($2::bigint[], $3::text[]) wanted(block_number, block_hash)
             JOIN chain_lineage lineage
               ON lineage.chain_id = $1
              AND lineage.block_number = wanted.block_number
              AND lineage.block_hash = wanted.block_hash
             WHERE lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
        )
        .bind(CHAIN)
        .bind(numbers)
        .bind(hashes)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(number, hash, timestamp)| ((number, hash), timestamp))
        .collect();
        Ok(Self {
            baseline,
            required_names,
            previous,
            history,
        })
    }
}

/// The block a row says it was built at.
fn named_block(kind: Kind, row: &Value) -> Result<(i64, &str)> {
    let (context, number, hash) = match kind {
        Kind::Name => (&row["chain_positions"][CHAIN], "block_number", "block_hash"),
        Kind::Child => (
            &row["chain_positions"],
            "target_block_number",
            "target_block_hash",
        ),
    };
    Ok((
        context[number]
            .as_i64()
            .context("row names no target block number")?,
        context[hash]
            .as_str()
            .context("row names no target block hash")?,
    ))
}

/// Every key must be served in both later states or in neither, and every served row must match
/// the rebuild exactly or satisfy the retained-row rule. Baseline keys served in neither are
/// counted as removed or dropped.
pub fn compare(
    candidate: &Served,
    rebuilt: &Served,
    target: &Target,
    retention: &Retention,
) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    compare_family(
        Kind::Name,
        &candidate.names,
        &rebuilt.names,
        &retention.baseline.names,
        |key| retention.required_names.contains(key),
        target,
        retention,
        &mut outcome,
    )?;
    compare_family(
        Kind::Child,
        &candidate.children,
        &rebuilt.children,
        &retention.baseline.children,
        |(parent, child)| {
            retention.required_names.contains(parent) || retention.required_names.contains(child)
        },
        target,
        retention,
        &mut outcome,
    )?;
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn compare_family<K: Ord + std::fmt::Debug>(
    kind: Kind,
    candidate: &BTreeMap<K, Value>,
    rebuilt: &BTreeMap<K, Value>,
    baseline: &BTreeMap<K, Value>,
    required: impl Fn(&K) -> bool,
    target: &Target,
    retention: &Retention,
    outcome: &mut Outcome,
) -> Result<()> {
    let candidate_keys = candidate.keys().collect::<BTreeSet<_>>();
    let rebuilt_keys = rebuilt.keys().collect::<BTreeSet<_>>();
    if candidate_keys != rebuilt_keys {
        let missing = rebuilt_keys.difference(&candidate_keys).next();
        let extra = candidate_keys.difference(&rebuilt_keys).next();
        bail!("{kind:?} keys differ from the rebuild: missing {missing:?}, extra {extra:?}");
    }
    // The key sets are equal here, so a baseline key the rebuild lacks is in neither later state.
    for key in baseline.keys().filter(|key| !rebuilt.contains_key(key)) {
        if required(key) {
            outcome.removed += 1;
        } else {
            outcome.dropped += 1;
        }
    }
    for (key, kept) in candidate {
        let expected = &rebuilt[key];
        if kept == expected {
            outcome.exact += 1;
            continue;
        }
        ensure!(
            !required(key),
            "{kind:?} row {key:?} had an event in the batch window but differs from the rebuild"
        );
        ensure!(
            baseline.get(key) == Some(kept),
            "{kind:?} row {key:?} differs from the rebuild and is not the row served before the \
             batch"
        );
        let (number, hash) = named_block(kind, kept)?;
        ensure!(
            number <= retention.previous,
            "{kind:?} row {key:?} names block {number}, after the previous publication"
        );
        let timestamp = retention
            .history
            .get(&(number, hash.to_owned()))
            .with_context(|| {
                format!("{kind:?} row {key:?} names block {number} {hash}, not readable history")
            })?;
        ensure!(
            kept["canonicality_summary"]["target_block_number"] == json!(number)
                && kept["canonicality_summary"]["target_block_hash"] == json!(hash),
            "{kind:?} row {key:?} names two different target blocks"
        );
        if kind == Kind::Name {
            ensure!(
                &kept["chain_positions"][CHAIN]["timestamp"] == timestamp,
                "name row {key:?} carries a timestamp its block does not have"
            );
        }
        ensure!(
            &refresh(kind, kept, target)? == expected,
            "{kind:?} row {key:?} differs from the rebuild beyond its target block: \
             {kept} != {expected}"
        );
        outcome.retained += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD: &str = "0x05";
    const NEW: &str = "0x0a";

    fn child(number: i64, hash: &str) -> Value {
        json!({
            "parent_logical_name_id": "ens:parent",
            "child_logical_name_id": "ens:child",
            "owner": "0xa1",
            "chain_positions": {"target_block_number": number, "target_block_hash": hash},
            "canonicality_summary": {"target_block_number": number, "target_block_hash": hash},
        })
    }

    fn served(row: Option<Value>) -> Served {
        Served {
            names: BTreeMap::new(),
            children: row
                .map(|row| (("ens:parent".into(), "ens:child".into()), row))
                .into_iter()
                .collect(),
            pages_read: 1,
            parents: 1,
        }
    }

    /// Compares with the child served in every state; `None` means the key is not served.
    fn check_served(
        baseline: Option<Value>,
        candidate: Option<Value>,
        rebuilt: Option<Value>,
        required: &[&str],
    ) -> Result<Outcome> {
        let retention = Retention {
            baseline: served(baseline),
            required_names: required.iter().map(|name| (*name).to_owned()).collect(),
            previous: 7,
            history: BTreeMap::from([
                ((5, OLD.to_owned()), json!("2026-01-01T00:00:00")),
                ((6, "0x06".to_owned()), json!("2026-01-01T00:00:12")),
            ]),
        };
        let target = Target {
            number: 10,
            hash: NEW.into(),
            timestamp: json!("2026-01-01T00:01:00"),
        };
        compare(&served(candidate), &served(rebuilt), &target, &retention)
    }

    fn check(baseline: Value, candidate: Value, required: &[&str]) -> Result<Outcome> {
        check_served(
            Some(baseline),
            Some(candidate),
            Some(child(10, NEW)),
            required,
        )
    }

    #[test]
    fn a_kept_row_from_readable_history_outside_the_window_is_retained() {
        let outcome = check(child(5, OLD), child(5, OLD), &[]).unwrap();
        assert_eq!(
            outcome,
            Outcome {
                retained: 1,
                ..Outcome::default()
            }
        );
    }

    #[test]
    fn a_real_hash_under_the_wrong_block_number_is_rejected() {
        // 0x05 is the hash of block 5, and block 6 is readable history with another hash.
        let error = check(child(6, OLD), child(6, OLD), &[]).unwrap_err();
        assert!(
            error.to_string().contains("not readable history"),
            "{error}"
        );
    }

    #[test]
    fn a_baseline_key_neither_later_state_serves_is_counted() {
        let outcome = check_served(Some(child(5, OLD)), None, None, &[]).unwrap();
        assert_eq!(
            outcome,
            Outcome {
                dropped: 1,
                ..Outcome::default()
            }
        );
        let outcome = check_served(Some(child(5, OLD)), None, None, &["ens:parent"]).unwrap();
        assert_eq!(
            outcome,
            Outcome {
                removed: 1,
                ..Outcome::default()
            }
        );
        // Served by the rebuild only, it is a missing key, not a removal.
        let error = check_served(Some(child(5, OLD)), None, Some(child(10, NEW)), &[]).unwrap_err();
        assert!(error.to_string().contains("keys differ"), "{error}");
    }

    #[test]
    fn a_kept_row_with_a_corrupted_target_hash_is_rejected() {
        let corrupted = || {
            let mut row = child(5, OLD);
            row["chain_positions"]["target_block_hash"] = json!("0xbad");
            row
        };
        // The batch corrupted a row it kept.
        let error = check(child(5, OLD), corrupted(), &[]).unwrap_err();
        assert!(
            error.to_string().contains("not the row served before"),
            "{error}"
        );
        // The row was already corrupt and the batch kept it.
        let error = check(corrupted(), corrupted(), &[]).unwrap_err();
        assert!(
            error.to_string().contains("not readable history"),
            "{error}"
        );
    }

    #[test]
    fn a_row_the_batch_had_to_rebuild_is_not_retained() {
        let error = check(child(5, OLD), child(5, OLD), &["ens:child"]).unwrap_err();
        assert!(
            error.to_string().contains("event in the batch window"),
            "{error}"
        );
    }

    #[test]
    fn a_kept_row_that_differs_beyond_its_target_is_rejected() {
        let mut stale = child(5, OLD);
        stale["owner"] = json!("0xa2");
        let error = check(stale.clone(), stale, &[]).unwrap_err();
        assert!(
            error.to_string().contains("beyond its target block"),
            "{error}"
        );
    }
}
