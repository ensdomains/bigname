//! The child listing oracle every publication a family test makes is checked against: the
//! stored listing (`project_child_listing`) equals the rows the unnarrowed children relation
//! composes at the published block, and the stored counts (`project_child_listing_count`) equal
//! the listing's own counts per parent and per (parent, label registry).
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Result, ensure};
use bigname_storage::families::topology::{ListingBlock, listing_rows};

use super::Fixture;

/// Every parent of the chain that can list a child: an ENSv1 or Basenames edge's parent, a name
/// with an ENSv2 subregistry pointer, and a parent with a stored row.
const PARENTS: &str = "SELECT DISTINCT parent FROM (
        SELECT edge.namespace || ':' || edge.parent_node AS parent
        FROM project_child_edge_candidate edge WHERE edge.chain_id = $1
        UNION SELECT pointer.logical_name_id FROM project_parent_subregistry pointer
        WHERE pointer.chain_id = $1
        UNION SELECT listing.parent_logical_name_id FROM project_child_listing listing
        WHERE listing.chain_id = $1
    ) parents ORDER BY parent";

const STORED: &str = "SELECT coalesce(jsonb_agg(to_jsonb(listing)
                                 ORDER BY listing.child_logical_name_id), '[]')::text
    FROM project_child_listing listing WHERE listing.chain_id = $1";

const COUNTS: &str = "SELECT coalesce(jsonb_agg(jsonb_build_array(counter.parent_logical_name_id,
                                                  counter.registry_address,
                                                  counter.child_count)
                                 ORDER BY counter.parent_logical_name_id,
                                          counter.registry_address), '[]')::text
    FROM project_child_listing_count counter WHERE counter.chain_id = $1";

const COUNTED: &str = "SELECT coalesce(jsonb_agg(jsonb_build_array(counted.parent,
                                                  counted.registry, counted.child_count)
                                 ORDER BY counted.parent, counted.registry), '[]')::text
    FROM (SELECT listing.parent_logical_name_id AS parent, '' AS registry,
                 count(*) AS child_count
          FROM project_child_listing listing WHERE listing.chain_id = $1
          GROUP BY listing.parent_logical_name_id
          UNION ALL
          SELECT listing.parent_logical_name_id, listing.registry_address, count(*)
          FROM project_child_listing listing
          WHERE listing.chain_id = $1 AND listing.registry_address IS NOT NULL
          GROUP BY listing.parent_logical_name_id, listing.registry_address) counted";

impl Fixture {
    /// The listing of `chain` equals the relation at its family marker's block, and its counts
    /// equal the listing's. A chain with no marker block has neither rows nor counts.
    pub async fn assert_child_listing(&self, chain: &str) -> Result<()> {
        let marker: Option<(Option<i64>, Option<i64>, String)> = sqlx::query_as(
            "SELECT current_block_number, extract(epoch FROM block_timestamp)::bigint, state
             FROM project_family_marker WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_optional(&self.pool)
        .await?;
        let stored: String = sqlx::query_scalar(STORED)
            .bind(chain)
            .fetch_one(&self.pool)
            .await?;
        let counts: String = sqlx::query_scalar(COUNTS)
            .bind(chain)
            .fetch_one(&self.pool)
            .await?;
        let Some((Some(number), Some(timestamp_seconds), state)) = marker else {
            ensure!(
                stored == "[]" && counts == "[]",
                "{chain} has no marker block but stores listing rows {stored} and counts {counts}"
            );
            return Ok(());
        };
        let parents: Vec<String> = sqlx::query_scalar(PARENTS)
            .bind(chain)
            .fetch_all(&self.pool)
            .await?;
        let block = ListingBlock {
            chain_id: chain,
            number,
            timestamp_seconds,
        };
        let mut oracle = listing_rows(
            "SELECT coalesce(jsonb_agg(to_jsonb(composed)
                                       ORDER BY composed.child_logical_name_id), '[]')::text
             FROM (",
            block,
            &parents,
            None,
        );
        oracle.push(") composed");
        let composed: String = oracle
            .build_query_scalar::<String>()
            .fetch_one(&self.pool)
            .await?;
        if stored != composed && self.before_children_stream(chain, &state).await? {
            // A rebuild whose composition pass has not finished the children stream publishes
            // nothing yet (`bootstrap_pending`), and the stream has not reached every parent.
            // That one difference is allowed, counted, and predicted by the test
            // (`expect_listing_skips`).
            self.listing_checks.skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        ensure!(
            stored == composed,
            "the child listing of {chain} at {number} is {stored}, the relation composes {composed}"
        );
        let counted: String = sqlx::query_scalar(COUNTED)
            .bind(chain)
            .fetch_one(&self.pool)
            .await?;
        ensure!(
            counts == counted,
            "the child counts of {chain} at {number} are {counts}, the listing counts {counted}"
        );
        self.listing_checks.compared.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Whether `chain`'s marker (in `state`) belongs to a rebuild whose composition pass has not
    /// finished the children stream: the marker is `bootstrap_pending` and the repair record is
    /// still applying ranges before its pass (`rebuilding` with no composed block) or composing
    /// the names stream (0) or the children stream (4), which run first and second
    /// (`families/compose.rs`, `Stream`).
    async fn before_children_stream(&self, chain: &str, state: &str) -> Result<bool> {
        if state != "bootstrap_pending" {
            return Ok(false);
        }
        let record: Option<(String, Option<i16>, Option<i64>)> = sqlx::query_as(
            "SELECT state, compose_stream, composed_block FROM project_repair_record
             WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match record {
            Some((record, _, composed)) if record == "rebuilding" => composed.is_none(),
            Some((record, stream, _)) if record == "composing" => {
                matches!(stream, None | Some(0) | Some(4))
            }
            _ => false,
        })
    }

    /// The test predicts `skips` publications before the children stream finished whose listing
    /// differs from the relation (none unless it says so).
    pub fn expect_listing_skips(&self, skips: usize) {
        self.listing_checks.expected.store(skips, Ordering::Relaxed);
    }
}

/// The child listing checks of one fixture: publications compared in full, publications skipped
/// before the children stream finished, and the skips the test predicts.
#[derive(Debug, Default)]
pub(super) struct Checks {
    compared: AtomicUsize,
    skipped: AtomicUsize,
    expected: AtomicUsize,
}

impl Checks {
    /// A fixture skipped exactly the predicted number, and one that checked any publication
    /// compared at least one in full.
    pub(super) fn require(&self) -> Result<()> {
        let compared = self.compared.load(Ordering::Relaxed);
        let skipped = self.skipped.load(Ordering::Relaxed);
        let expected = self.expected.load(Ordering::Relaxed);
        ensure!(
            skipped == expected,
            "{skipped} publications were skipped before the children stream finished, the test \
             predicts {expected}"
        );
        ensure!(
            skipped == 0 || compared > 0,
            "{skipped} publications were skipped and none was compared in full"
        );
        Ok(())
    }
}
