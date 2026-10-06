//! Project's current lookup state shares the existing publication, journal and undo horizon.
//! Work is captured before summary deadlines advance; only changed components are written.
mod inventories;
mod names;
mod replace;
mod work;
pub(super) use work::prepare;

use super::{block::BlockStats, input::BlockHeader};
use crate::{ProjectError, Result};
use bigname_storage::families::name::FamilyPublication;
use sqlx::{Postgres, Transaction, types::time::OffsetDateTime};

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    stats: &mut BlockStats,
) -> Result<()> {
    let publication = FamilyPublication {
        chain_id: chain.into(),
        block_number: block.number,
        block_hash: block.hash.clone(),
        block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp_seconds)
            .map_err(|e| ProjectError::data_integrity(format!("lookup publication time: {e}")))?,
        block_timestamp_json: block.timestamp.clone(),
    };
    names::refresh(transaction, &publication, block, stats).await?;
    inventories::refresh(transaction, &publication, block, stats).await
}
