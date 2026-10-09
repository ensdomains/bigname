//! Project's current lookup state shares the existing publication, journal and undo horizon.
//! Work is captured before summary deadlines advance; only changed components are written.
mod inventories;
mod names;
mod replace;
mod work;
pub(super) use work::prepare;

use super::{block::BlockStats, input::BlockHeader};
use crate::Result;
use sqlx::{Postgres, Transaction};

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    manifests: &str,
    stats: &mut BlockStats,
) -> Result<()> {
    let publication =
        super::marker::publication(transaction, chain, block, Some(manifests)).await?;
    names::refresh(transaction, &publication, block, stats).await?;
    inventories::refresh(transaction, &publication, block, stats).await
}
