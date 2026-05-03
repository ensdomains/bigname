mod chain_position;
mod constants;
mod hydration;
mod json;
mod loading;
mod profile;
mod projection;
mod types;

use anyhow::Result;
use sqlx::PgPool;

pub use hydration::RecordInventoryTextHydrationConfig;
pub use types::{RecordInventoryCurrentRebuildSummary, RecordInventoryTextHydrationSummary};

pub async fn rebuild_record_inventory_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    projection::rebuild_record_inventory_current(pool, resource_id).await
}

pub async fn hydrate_record_inventory_text_values(
    pool: &PgPool,
    resource_id: Option<&str>,
    config: RecordInventoryTextHydrationConfig,
) -> Result<RecordInventoryTextHydrationSummary> {
    hydration::hydrate_record_inventory_text_values(pool, resource_id, config).await
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
