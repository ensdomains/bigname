//! Follow-block hydration: preview owned selectors in a short transaction, release it before
//! RPC, then apply results to the real post-reducer working set. The publication transaction
//! checks the same predecessor, revision and block hash again. Replay/rebuild never hydrate
//! (TYR-36 step7a packet E3); their restored or empty overlays refresh on later follow blocks.
mod reverse;
mod text;

use sqlx::{PgPool, Postgres, Transaction};

use super::{FamilyOptions, block, input, keys, reduce, resolver, store::RowSet};
use crate::{ProjectError, Result};

pub(crate) const ETHEREUM: &str = "ethereum-mainnet";

pub(crate) struct Prepared {
    block: input::BlockHeader,
    reverse: reverse::Prepared,
    text: text::Prepared,
}

pub(crate) async fn prepare(
    pool: &PgPool,
    chain_id: &str,
    number: i64,
    plan: &block::Plan<'_>,
    options: &FamilyOptions,
) -> Result<Option<Prepared>> {
    let Some(rpc_urls) = options.hydration_rpc_urls.as_ref() else {
        return Ok(None);
    };
    if chain_id != ETHEREUM || plan.role != block::Role::Follow {
        return Ok(None);
    }
    // Use the same fences and reducers, with no publication. New tuples and resolver changes
    // in N must be considered at N; selecting only the stored N-1 rows misses those claims.
    let mut opened = block::open(pool, chain_id, number, plan).await?;
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
    let reverse = reverse::select(&mut opened.transaction, &context, &rows).await?;
    let text = text::select(&mut opened.transaction, &context, &rows).await?;
    opened.transaction.rollback().await.map_err(|error| {
        ProjectError::database("failed to close family hydration preparation", error)
    })?;
    let reverse = reverse::execute(reverse, rpc_urls, &opened.block).await?;
    let text = text::execute(text, rpc_urls, &opened.block).await?;
    Ok(Some(Prepared {
        block: opened.block,
        reverse,
        text,
    }))
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
    ) -> Result<()> {
        self.reverse
            .apply(transaction, context, rows, ordinal)
            .await?;
        self.text.apply(transaction, context, rows, ordinal).await
    }
}
