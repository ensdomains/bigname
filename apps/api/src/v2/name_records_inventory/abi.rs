//! The inventory container's ABI content types (docs/api-v1-routes.md, records route). Both
//! routes resolve every container they serve through one batched storage read.

use bigname_storage::{
    AbiContentTypes, AbiContentTypesInput, AbiContentTypesUnavailable, IdentityRecordInventoryRow,
    RecordInventoryCurrentRow, record_version_boundary_storage_key,
};
use sqlx::PgPool;
use tracing::error;

use super::RecordInventory;
use crate::v2::{V2Error, V2Result, support::serving_record_inventory};

impl RecordInventory {
    pub(crate) fn set_abi_content_types(&mut self, answer: AbiContentTypes) {
        (self.abi_content_types, self.abi_unsupported_reason) = match answer {
            AbiContentTypes::Observed(content_types) => (Some(content_types), None),
            AbiContentTypes::Unavailable(reason) => (None, Some(reason.as_str().to_owned())),
        };
    }
}

/// The records route's row; authority follows the same serving decision as its other keys.
/// `boundary_key` is the row's stored key, which decoding the row already checked.
pub(crate) fn abi_input_for_row<'a>(
    row: &'a RecordInventoryCurrentRow,
    boundary_key: &'a str,
) -> AbiContentTypesInput<'a> {
    AbiContentTypesInput {
        authoritative: serving_record_inventory(Some(row)).is_some(),
        resource_id: row.resource_id,
        record_version_boundary_key: boundary_key,
        provenance: &row.provenance,
        chain_positions: &row.chain_positions,
        last_recomputed_at: row.last_recomputed_at,
    }
}

/// The lookup route's identity-facade row; authority follows `InventorySections`.
pub(crate) fn abi_input_for_identity_row(
    row: &IdentityRecordInventoryRow,
) -> AbiContentTypesInput<'_> {
    AbiContentTypesInput {
        authoritative: row.support_status == "supported",
        resource_id: row.resource_id,
        record_version_boundary_key: &row.record_version_boundary_key,
        provenance: &row.provenance,
        chain_positions: &row.chain_positions,
        last_recomputed_at: row.last_recomputed_at,
    }
}

/// Fill the records route's container, when it is served, from the row its keys came from. A
/// container served without a row keeps the route's `inventory_not_available` meaning.
pub(crate) async fn fill_records_route_abi_content_types(
    pool: &PgPool,
    inventory: Option<&mut RecordInventory>,
    row: Option<&RecordInventoryCurrentRow>,
) -> V2Result<()> {
    let Some(inventory) = inventory else {
        return Ok(());
    };
    let answer = match row {
        Some(row) => {
            let boundary_key = record_version_boundary_storage_key(
                &row.record_version_boundary,
                row.resource_id,
            )
            .map_err(|key_error| {
                error!(service = "api", error = ?key_error, "record inventory boundary key");
                V2Error::internal_error("failed to load record inventory ABI content types")
            })?;
            load_abi_content_types(pool, &[abi_input_for_row(row, &boundary_key)])
                .await?
                .pop()
                .expect("one ABI answer per inventory row")
        }
        None => AbiContentTypes::Unavailable(AbiContentTypesUnavailable::InventoryNotAvailable),
    };
    inventory.set_abi_content_types(answer);
    Ok(())
}

/// One read for every container in the request, answered in input order.
pub(crate) async fn load_abi_content_types(
    pool: &PgPool,
    inputs: &[AbiContentTypesInput<'_>],
) -> V2Result<Vec<AbiContentTypes>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    #[cfg(test)]
    abi_content_types_test_hooks::record(pool, inputs.len()).await;
    bigname_storage::load_record_inventory_abi_content_types(pool, inputs)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = inputs.len(),
                error = ?load_error,
                "failed to load record inventory ABI content types"
            );
            V2Error::internal_error("failed to load record inventory ABI content types")
        })
}

/// Records the input count of each batched read so a test can assert one read per request, and
/// can run one statement after the route loaded its inventory rows and before the ABI read, so a
/// test can publish a newer projection in that window.
#[cfg(test)]
pub(crate) mod abi_content_types_test_hooks {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;

    type Calls = Arc<Mutex<Vec<usize>>>;

    static HOOKS: ScopedTestHookRegistry<String, Calls> = ScopedTestHookRegistry::new();
    static INTERLEAVED: ScopedTestHookRegistry<String, String> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(ScopedTestHookGuard<String, Calls>, Calls)> {
        let calls = Calls::default();
        let guard = HOOKS.install(current_test_database(pool).await?, Arc::clone(&calls));
        Ok((guard, calls))
    }

    /// Run `statement` once, just before the next ABI read on this test database.
    pub(crate) async fn interleave(
        pool: &PgPool,
        statement: &str,
    ) -> Result<ScopedTestHookGuard<String, String>> {
        Ok(INTERLEAVED.install(current_test_database(pool).await?, statement.to_owned()))
    }

    pub(super) async fn record(pool: &PgPool, inputs: usize) {
        let Ok(database) = current_test_database(pool).await else {
            return;
        };
        if let Some(calls) = HOOKS.get_cloned(&database) {
            calls.lock().expect("ABI read calls").push(inputs);
        }
        if let Some(statement) = INTERLEAVED.take(&database) {
            sqlx::raw_sql(&statement)
                .execute(pool)
                .await
                .expect("interleaved ABI test statement");
        }
    }
}
