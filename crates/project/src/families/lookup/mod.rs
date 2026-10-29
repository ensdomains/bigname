//! Project's current lookup state shares the existing publication, journal and undo horizon.
//! Work is captured before summary deadlines advance; only changed components are written.
mod inventories;
mod names;
mod replace;
mod work;
pub(super) use work::{prepare, prepare_names, prepare_resources};

use super::{block::BlockStats, input::BlockHeader};
use crate::Result;
use sqlx::{Postgres, Transaction};

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    composition: &super::marker::Composition,
    stats: &mut BlockStats,
) -> Result<()> {
    let publication = super::marker::publication(chain, block, composition)?;
    names::refresh(transaction, &publication, block, true, stats).await?;
    inventories::refresh(transaction, &publication, block, stats).await
}

/// The lookup names of a composition pass's names chunk, their serving resources left to its
/// inventories stream (`refresh_inventories`).
#[allow(dead_code)] // The red stub removes its caller.
pub(super) async fn refresh_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    composition: &super::marker::Composition,
    stats: &mut BlockStats,
) -> Result<()> {
    let publication = super::marker::publication(chain, block, composition)?;
    names::refresh(transaction, &publication, block, false, stats).await
}

/// A chunk of a composition pass's inventories stream: each name of `served` gets the serving
/// resource its core records, then the inventory work `prepare_resources` wrote is composed.
pub(super) async fn refresh_inventories(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    composition: &super::marker::Composition,
    served: &[(String, uuid::Uuid)],
    stats: &mut BlockStats,
) -> Result<()> {
    let publication = super::marker::publication(chain, block, composition)?;
    names::serve(transaction, chain, served).await?;
    inventories::refresh(transaction, &publication, block, stats).await
}
