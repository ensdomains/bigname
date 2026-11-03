//! Checked entry points into the schema-v2 adapter for in-process tests, and the snapshot the
//! Interpret engine takes for database-backed tests.

use bigname_adapters::schema_v2::{
    AdapterSession, BatchInput, BatchOutput, InterpreterStateRequest, InterpreterStateValue,
    ManifestInput, PreparedAdapterBatch, StateCacheCapacity,
};
use sqlx::PgPool;

use super::{Batch, allow, assert_selectable, unmatched};

crate::fixture_log_views!(pub(super) BatchInput, BatchOutput);

/// `interpret_schema_v2_batch`, then [`assert_selectable`] on its output.
pub fn interpret_checked(input: BatchInput) -> anyhow::Result<BatchOutput> {
    let batch = fixture_batch(&input);
    let output = bigname_adapters::interpret_schema_v2_batch(input)?;
    assert_selectable(&batch, &fixture_outcome(&output));
    Ok(output)
}

/// `prepare_schema_v2_batch_incremental`, checked when the prepared batch finishes.
pub fn prepare_checked(
    input: BatchInput,
    session: Option<AdapterSession>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<CheckedPreparedBatch> {
    let batch = fixture_batch(&input);
    Ok(CheckedPreparedBatch {
        prepared: bigname_adapters::prepare_schema_v2_batch_incremental(
            input,
            session,
            cache_capacity,
        )?,
        batch,
    })
}

/// `prepare_schema_v2_batch_incremental_with_provenance`, checked when the prepared batch
/// finishes.
pub fn prepare_checked_with_provenance(
    input: BatchInput,
    provenance_manifests: Vec<ManifestInput>,
    session: Option<AdapterSession>,
    cache_capacity: StateCacheCapacity,
) -> anyhow::Result<CheckedPreparedBatch> {
    let batch = fixture_batch(&input);
    Ok(CheckedPreparedBatch {
        prepared: bigname_adapters::prepare_schema_v2_batch_incremental_with_provenance(
            input,
            provenance_manifests,
            session,
            cache_capacity,
        )?,
        batch,
    })
}

/// A prepared adapter batch whose [`finish`](Self::finish) runs [`assert_selectable`].
pub struct CheckedPreparedBatch {
    prepared: PreparedAdapterBatch,
    batch: Batch,
}

impl CheckedPreparedBatch {
    pub fn state_value_requests(&self) -> &[InterpreterStateRequest] {
        self.prepared.state_value_requests()
    }

    pub fn finish(
        self,
        loaded: Vec<InterpreterStateValue>,
    ) -> anyhow::Result<(BatchOutput, AdapterSession)> {
        let (output, session) = self.prepared.finish(loaded)?;
        assert_selectable(&self.batch, &fixture_outcome(&output));
        Ok((output, session))
    }
}

/// What the Interpret engine keeps of a loaded batch input until the batch output exists. Only
/// test builds take it: `bigname-interpret` compiles the call under `cfg(test)` or its
/// `test-support` feature, which only dev-dependencies enable.
pub struct Snapshot(Batch);

impl Snapshot {
    pub fn of(input: &BatchInput) -> Self {
        Self(fixture_batch(input))
    }

    /// Panics unless every log of the batch was selected, recorded as a decode skip, or accepted
    /// by an allowance installed for this test database or on this thread.
    pub async fn check(self, pool: &PgPool, output: &BatchOutput) {
        let unmatched = unmatched(&self.0, &fixture_outcome(output));
        if unmatched.is_empty() {
            return;
        }
        let database = allow::database_key(pool).await;
        let mut scopes = allow::thread_scopes();
        scopes.extend(allow::database_scope(&database));
        allow::fail_unless_allowed(&unmatched, &scopes, Some(database.as_str()));
    }
}
