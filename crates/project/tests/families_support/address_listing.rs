//! The address listing oracle every publication a family test makes is checked against: the
//! stored rows (`project_address_listing`) equal what the address-names route composes at read
//! for every address of the chain (`assert_address_listing_matches_the_route`).
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, ensure};
use bigname_storage::families::records::assert_address_listing_matches_the_route;
use serde_json::Value;

use super::Fixture;

impl Fixture {
    /// The address listing of `chain` equals the route's rows at its family marker's block. A
    /// chain with no marker block stores no rows. A publication the readers refuse has no route
    /// rows to compare with: one written under this build's projection content hash is counted
    /// as skipped, and one written under another hash, which no reader of this build serves, is
    /// not checked.
    pub async fn assert_address_listing(&self, chain: &str) -> Result<()> {
        let marker: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
            "SELECT current_block_number, input_content_hash FROM project_family_marker
             WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_optional(&self.pool)
        .await?;
        let (block, content_hash) = marker.unwrap_or_default();
        if block.is_none() {
            let stored: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM project_address_listing WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(&self.pool)
            .await?;
            ensure!(
                stored == 0,
                "{chain} has no marker block but stores {stored} address listing rows"
            );
            return Ok(());
        }
        let counter = if assert_address_listing_matches_the_route(&self.pool, chain).await? {
            &self.address_listing_checks.compared
        } else if content_hash.as_deref() == Some(super::CONTENT_HASH) {
            &self.address_listing_checks.skipped
        } else {
            return Ok(());
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// The publications whose address listing was compared with the route so far, and those
    /// the readers refused. A rebuild publishes `bootstrap_pending` until its composition pass
    /// ends, which the readers refuse, so a test of rebuilds alone compares none.
    pub fn address_listing_checks(&self) -> (usize, usize) {
        (
            self.address_listing_checks.compared(),
            self.address_listing_checks.skipped(),
        )
    }
}

/// The key `project_address_listing_former_owner_idx` orders a former owner's names by after
/// expiry and namespace: `normalized_name` cut as `display_key` is cut.
pub const FORMER_NAME_KEY: &str = "CASE WHEN octet_length(normalized_name) <= 2000 THEN \
    normalized_name ELSE left(normalized_name, 500) END";

/// `value` as an SQL string literal.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The read of the names `address` lists on `chain` whose normalized name starts with `q`, as a
/// reader keys `project_address_listing_q_idx`: the first 100 characters of `q` as a range over
/// the index's cut key, then the whole `q` checked against the full name. The values are
/// literals, so a plan of it is the plan of a read with those values.
pub fn q_read(chain: &str, address: &str, q: &str) -> String {
    let cut: String = q.chars().take(100).collect();
    format!(
        "SELECT logical_name_id FROM project_address_listing
         WHERE address = {address} AND listed AND chain_id = {chain}
           AND left(normalized_name, 100) LIKE {range} AND starts_with(normalized_name, {q})",
        address = literal(address),
        chain = literal(chain),
        range = literal(&format!("{cut}%")),
        q = literal(q),
    )
}

/// The read of the names `address` lists on `chain` under `parent`, as a reader keys
/// `project_address_listing_parent_idx`: the hash of the parent's normalized name, then the name
/// itself.
pub fn parent_read(chain: &str, address: &str, parent: &str) -> String {
    format!(
        "SELECT logical_name_id FROM project_address_listing
         WHERE address = {address} AND listed AND chain_id = {chain}
           AND md5(parent_normalized_name) = md5({parent})
           AND parent_normalized_name = {parent}",
        address = literal(address),
        chain = literal(chain),
        parent = literal(parent),
    )
}

/// The former-owner page of `address` on `chain` in the order of
/// `project_address_listing_former_owner_idx`: expiry, namespace, the cut name, namehash.
pub fn former_read(chain: &str, address: &str) -> String {
    format!(
        "SELECT logical_name_id FROM project_address_listing
         WHERE address = {address} AND former_owner AND chain_id = {chain}
         ORDER BY address, former_expires_key, namespace, ({FORMER_NAME_KEY}), namehash",
        address = literal(address),
        chain = literal(chain),
    )
}

impl Fixture {
    /// The name ids `read` yields, in its order.
    pub async fn listing_read(&self, read: &str) -> Result<Vec<String>> {
        sqlx::query_scalar(read)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("read {read}"))
    }

    /// `read` is served by an index scan of `index` whose index condition names `condition`,
    /// with no sort, when sequential scans, bitmap scans and sorts are off: the shape of the index answers
    /// the read, whatever the planner would pick for this small fixture.
    pub async fn ensure_index_serves(
        &self,
        read: &str,
        index: &str,
        condition: &str,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::raw_sql(
            "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;
             SET LOCAL enable_sort = off",
        )
        .execute(&mut *transaction)
        .await?;
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON, COSTS OFF) {read}"))
            .fetch_one(&mut *transaction)
            .await
            .with_context(|| format!("explain {read}"))?;
        transaction.rollback().await?;
        let mut nodes = Vec::new();
        collect_nodes(&plan, &mut nodes);
        ensure!(
            nodes.iter().all(|node| node["Node Type"] != "Sort"),
            "{index} leaves a sort: {plan}"
        );
        ensure!(
            nodes.iter().any(|node| node["Index Name"] == index
                && node["Index Cond"]
                    .as_str()
                    .is_some_and(|cond| cond.contains(condition))),
            "{index} does not serve the read with an index condition on {condition}: {plan}"
        );
        Ok(())
    }
}

/// Every plan node under `plan`.
fn collect_nodes<'a>(plan: &'a Value, nodes: &mut Vec<&'a Value>) {
    match plan {
        Value::Array(items) => items.iter().for_each(|item| collect_nodes(item, nodes)),
        Value::Object(map) => {
            if map.contains_key("Node Type") {
                nodes.push(plan);
            }
            map.values().for_each(|value| collect_nodes(value, nodes));
        }
        _ => {}
    }
}

/// The address listing checks of one fixture: publications compared, and publications the
/// readers refused.
#[derive(Debug, Default)]
pub(super) struct Checks {
    compared: AtomicUsize,
    skipped: AtomicUsize,
}

impl Checks {
    /// The publications compared so far.
    pub(super) fn compared(&self) -> usize {
        self.compared.load(Ordering::Relaxed)
    }

    /// The publications the readers refused so far.
    pub(super) fn skipped(&self) -> usize {
        self.skipped.load(Ordering::Relaxed)
    }
}
