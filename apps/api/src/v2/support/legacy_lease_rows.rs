//! The composed name rows the read-time lease fill (`legacy_lease`) heals: name detail's own
//! row, and lookup's identity rows. Collection pages heal their rows at their publication's
//! per-chain bounds ([`serve_legacy_leases_at_bounds`]).
use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::{IdentityNameCurrentRow, NameCurrentRow};
use serde_json::Value;
use sqlx::PgConnection;
use tracing::error;

use super::legacy_lease::{LeaseRead, fill_lease, lease_read};
use super::{LegacyLeaseShape, legacy_lease_shape};

#[cfg(test)]
tokio::task_local! {
    /// The lease savepoints one request took, for the tests that bound the fill's cost. Every
    /// statement the fill issues runs inside one.
    pub(crate) static LEASE_SAVEPOINTS: std::cell::Cell<usize>;
}

/// The fields of a composed name row the fill reads, and the summary it fills.
pub(crate) trait LeaseRow {
    fn logical_name_id(&self) -> &str;
    fn namespace(&self) -> &str;
    fn normalized_name(&self) -> &str;
    fn provenance(&self) -> &Value;
    fn chain_positions(&self) -> &Value;
    fn declared_summary(&self) -> &Value;
    fn declared_summary_mut(&mut self) -> &mut Value;
    /// The summary and the provenance together, for the fill of an ownerless row's `authority`.
    fn summary_and_provenance_mut(&mut self) -> (&mut Value, &mut Value);
}

macro_rules! lease_row {
    ($row:ty) => {
        impl LeaseRow for $row {
            fn logical_name_id(&self) -> &str {
                &self.logical_name_id
            }
            fn namespace(&self) -> &str {
                &self.namespace
            }
            fn normalized_name(&self) -> &str {
                &self.normalized_name
            }
            fn provenance(&self) -> &Value {
                &self.provenance
            }
            fn chain_positions(&self) -> &Value {
                &self.chain_positions
            }
            fn declared_summary(&self) -> &Value {
                &self.declared_summary
            }
            fn declared_summary_mut(&mut self) -> &mut Value {
                &mut self.declared_summary
            }
            fn summary_and_provenance_mut(&mut self) -> (&mut Value, &mut Value) {
                (&mut self.declared_summary, &mut self.provenance)
            }
        }
    };
}

lease_row!(NameCurrentRow);
lease_row!(IdentityNameCurrentRow);

/// The shape the fill heals `row` from, if any. It reads only the row, so a caller can skip a
/// row of no shape without a statement.
pub(crate) fn lease_row_shape<R: LeaseRow>(row: &R) -> Option<LegacyLeaseShape> {
    legacy_lease_shape(
        row.namespace(),
        row.normalized_name(),
        row.provenance(),
        row.declared_summary(),
    )
}

/// Fills each of `rows` the fill matches, each in its own savepoint of `conn`'s read transaction,
/// at `bounds`, the snapshot's block on each chain id. A row of no shape issues no statement. A
/// failed read leaves its row as composed and the others are still filled. A failed savepoint is
/// returned, since the read transaction may be unusable after it.
pub(crate) async fn serve_legacy_leases_at_bounds<'a, R: LeaseRow + 'a>(
    conn: &mut PgConnection,
    rows: impl IntoIterator<Item = &'a mut R>,
    bounds: &BTreeMap<String, i64>,
) -> Result<()> {
    for row in rows {
        let read = lease_read(&*row, |chain_id| bounds.get(chain_id).copied());
        serve_read_in_savepoint(&mut *conn, row, read).await?;
    }
    Ok(())
}

/// [`fill_lease`] of `row` in a savepoint of `conn`, as `serve_legacy_lease_in_savepoint`
/// describes. A row whose fill fails is restored as composed, summary and provenance both.
pub(super) async fn serve_read_in_savepoint<R: LeaseRow>(
    conn: &mut PgConnection,
    row: &mut R,
    read: Option<LeaseRead>,
) -> Result<bool> {
    let Some(read) = read else {
        return Ok(false);
    };
    #[cfg(test)]
    let _ = LEASE_SAVEPOINTS.try_with(|count| count.set(count.get() + 1));
    let composed = (row.declared_summary().clone(), row.provenance().clone());
    let mut savepoint = sqlx::Connection::begin(&mut *conn).await?;
    match fill_lease(&mut savepoint, &mut *row, read).await {
        Ok(filled) => {
            if let Err(error) = savepoint.commit().await {
                restore(row, composed);
                return Err(error.into());
            }
            Ok(filled)
        }
        Err(read_error) => {
            restore(row, composed);
            if let Err(rollback_error) = savepoint.rollback().await {
                error!(
                    service = "api",
                    name = %row.normalized_name(),
                    error = %read_error,
                    "legacy lease read failed and its savepoint did not roll back"
                );
                return Err(rollback_error.into());
            }
            error!(
                service = "api",
                name = %row.normalized_name(),
                error = %read_error,
                "legacy lease read failed, serving the composed registration"
            );
            Ok(false)
        }
    }
}

fn restore<R: LeaseRow>(row: &mut R, (summary, provenance): (Value, Value)) {
    let (filled_summary, filled_provenance) = row.summary_and_provenance_mut();
    *filled_summary = summary;
    *filled_provenance = provenance;
}
