//! The reducers of one block, in dependency order: each family loads the rows its keys name,
//! then applies the block's events to them in the canonical event order.
use sqlx::{Postgres, Transaction};

use super::{input::BlockEvent, input::BlockHeader, keys::BlockKeys, store::RowSet};
use crate::{ProjectError, Result};

pub(crate) struct Context<'a> {
    pub(crate) chain_id: &'a str,
    pub(crate) block: &'a BlockHeader,
    pub(crate) keys: &'a BlockKeys,
}

/// Name the family in a reducer error, so a skipped block says which family failed.
pub(crate) fn in_family(family: &str) -> impl Fn(ProjectError) -> ProjectError + '_ {
    move |error| {
        let kind = error.kind();
        let message = format!("family {family}: {error}");
        match kind {
            crate::ErrorKind::Transient => ProjectError::transient(message),
            crate::ErrorKind::DataIntegrity => ProjectError::data_integrity(message),
            crate::ErrorKind::Configuration => ProjectError::configuration(message),
        }
    }
}

pub(crate) async fn apply(
    _transaction: &mut Transaction<'_, Postgres>,
    _context: &Context<'_>,
    _events: &[BlockEvent],
    _rows: &mut RowSet,
) -> Result<()> {
    Ok(())
}
