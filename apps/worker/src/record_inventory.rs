mod chain_position;
mod constants;
mod json;
mod loading;
mod profile;
mod projection;
mod types;

use anyhow::Result;
use sqlx::PgPool;

pub use types::RecordInventoryCurrentRebuildSummary;

pub async fn rebuild_record_inventory_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    projection::rebuild_record_inventory_current(pool, resource_id).await
}

#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
use bigname_storage::{
    CanonicalityState, RecordInventoryCurrentRow, upsert_record_inventory_current_rows,
};
#[cfg(test)]
use chain_position::format_timestamp;
#[cfg(test)]
use constants::*;
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::{Row, types::time::OffsetDateTime};
#[cfg(test)]
use uuid::Uuid;

#[cfg(test)]
mod tests;
