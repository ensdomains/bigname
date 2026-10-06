//! Hydration at the head: Project's RPC read of the selectors `reverse.sql` and `text.sql`
//! select (`docs/projections.md`, follow-only hydration). A block
//! hydrates only when it is published as an ordinary follow block and is the highest readable
//! block the run captured when it started, by number and hash (`Plan::head`). A block applied
//! while catching up, a block a run stops on because its budget is spent, a replayed or rebuilt
//! block and a caller's target below the readable head never call RPC; their selectors wait for
//! the next head block.
//!
//! A hydrating block previews its owned selectors in a short transaction, releases it before
//! RPC, then applies the reads to the real post-reducer working set. The publication transaction
//! checks the same predecessor, revision and block hash again.
mod admission;
pub(crate) mod batch;
pub(crate) mod outcome;
mod reverse;
mod schedule;
mod text;
pub(crate) mod work;

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{PgPool, Postgres, Transaction};

use self::outcome::{HydrationOutcome, Writes};
use super::{FamilyOptions, block, input, keys, reduce, resolver, store::RowSet};
use crate::{Marker, ProjectError, Result};

pub(crate) const ETHEREUM: &str = "ethereum-mainnet";

pub(crate) struct Prepared {
    block: input::BlockHeader,
    reverse: reverse::Prepared,
    text: text::Prepared,
}

/// The highest readable block of a chain a run may hydrate on, read once when the run starts;
/// `None` for a run that cannot hydrate. Project reads it itself: the caller's target says how
/// far to publish, not which block is the head.
pub(crate) async fn head(
    pool: &PgPool,
    chain_id: &str,
    options: &FamilyOptions,
) -> Result<Option<Marker>> {
    if options.hydration_rpc_urls.is_none() || chain_id != ETHEREUM {
        return Ok(None);
    }
    input::readable_head(pool, chain_id).await
}

/// Selector changes invalidate retry scheduling independently of whether this block hydrates.
/// The owned rows journal these resets with the event changes, preserving undo and replay.
pub(crate) async fn reset_schedule(
    transaction: &mut Transaction<'_, Postgres>,
    context: &reduce::Context<'_>,
    rows: &mut RowSet,
) -> Result<()> {
    if context.chain_id == ETHEREUM {
        reverse::reset_schedule(transaction, context, rows).await?;
        text::reset_schedule(transaction, context, rows).await?;
    }
    Ok(())
}

pub(crate) async fn prepare(
    pool: &PgPool,
    chain_id: &str,
    number: i64,
    plan: &block::Plan<'_>,
    options: &FamilyOptions,
    stats: &mut HydrationOutcome,
) -> Result<Option<Prepared>> {
    let Some(rpc_urls) = options.hydration_rpc_urls.as_ref() else {
        return Ok(None);
    };
    if chain_id != ETHEREUM || plan.role != block::Role::Follow {
        return Ok(None);
    }
    let Some(head) = plan.head.filter(|head| head.number == number) else {
        return Ok(None);
    };
    // Use the same fences and reducers, with no publication. New tuples and resolver changes
    // in N must be considered at N; selecting only the stored N-1 rows misses those claims.
    let mut opened = block::open(pool, chain_id, number, plan).await?;
    if opened.block.hash != head.hash {
        // Another block took the head's height since the run started; it is published without
        // hydration, as a replayed block is.
        close(opened.transaction).await?;
        return Ok(None);
    }
    let (events, _) = input::block_events(&mut opened.transaction, chain_id, &opened.block).await?;
    let keys = keys::derive(&events);
    let mut rows = RowSet::default();
    let context = reduce::Context {
        chain_id,
        block: &opened.block,
        keys: &keys,
        manifests: &opened.manifests,
        manifests_changed: opened.prior.admission_manifests.as_deref()
            != Some(opened.manifests.key.as_str()),
        prefetched: None,
    };
    resolver::registry_pointers(&mut opened.transaction, &context, &events, &mut rows).await?;
    resolver::resource_pointers(&mut opened.transaction, &context, &events, &mut rows).await?;
    super::classification::apply(&mut opened.transaction, &context, &events, &mut rows).await?;
    super::records::apply(&mut opened.transaction, &context, &events, &mut rows).await?;
    super::reverse::apply(&mut opened.transaction, &context, &events, &mut rows).await?;
    reset_schedule(&mut opened.transaction, &context, &mut rows).await?;
    let reverse = reverse::select(&mut opened.transaction, &context, &rows).await?;
    let text = text::select(&mut opened.transaction, &context, &rows).await?;
    close(opened.transaction).await?;
    stats.passes += 1;
    stats.head_age_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|now| {
            now.as_secs()
                .saturating_sub(u64::try_from(opened.block.timestamp_seconds).unwrap_or(0))
        });
    let mut session = batch::Session::new(
        chain_id,
        rpc_urls,
        &opened.block,
        options.hydration_time_limits,
        stats,
    );
    let reads = async {
        // Reverse names are read first, within half of the time while text selectors wait.
        session.share(if text::waiting(&text) { 2 } else { 1 });
        let reverse = reverse::execute(reverse, &mut session).await?;
        session.share(1);
        Ok::<_, ProjectError>((reverse, text::execute(text, &mut session).await?))
    }
    .await;
    session.finish();
    let (reverse, text) = reads?;
    Ok(Some(Prepared {
        block: opened.block,
        reverse,
        text,
    }))
}

async fn close(transaction: Transaction<'static, Postgres>) -> Result<()> {
    transaction.rollback().await.map_err(|error| {
        ProjectError::database("failed to close family hydration preparation", error)
    })
}

impl Prepared {
    pub(crate) fn require_block(&self, block: &input::BlockHeader) -> Result<()> {
        if self.block.number != block.number || self.block.hash != block.hash {
            return Err(ProjectError::transient(
                "family hydration's prepared block changed",
            ));
        }
        Ok(())
    }

    pub(crate) async fn apply(
        self,
        transaction: &mut Transaction<'_, Postgres>,
        context: &reduce::Context<'_>,
        rows: &mut RowSet,
        ordinal: i64,
    ) -> Result<(Writes, Writes)> {
        let reverse = self
            .reverse
            .apply(transaction, context, rows, ordinal)
            .await?;
        let text = self.text.apply(transaction, context, rows, ordinal).await?;
        Ok((reverse, text))
    }
}
