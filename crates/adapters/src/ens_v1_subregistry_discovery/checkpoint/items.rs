use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};

use super::{ADAPTER, ITEM_KIND_LATEST_ASSIGNMENT, SubregistryReplayCheckpoint};

const CHECKPOINT_ITEM_INSERT_BATCH_SIZE: usize = 500;

/// Upsert checkpoint items, returning how many `latest_assignment` items were
/// newly inserted (as opposed to updating an already-staged key). The caller
/// adds this to the checkpoint's running `staged_item_count`.
///
/// The insert-vs-update distinction reads `(xmax = 0)` from the upsert's
/// RETURNING clause: a freshly inserted row has no updater xid, while a row
/// taken through `ON CONFLICT DO UPDATE` carries this transaction's xid as
/// `xmax`. That is only sound because `item_rows` holds each
/// `(item_kind, item_key)` at most once per call — `save_progress` builds
/// the rows from a page-local `BTreeMap`/`BTreeSet` — and because this
/// transaction performs no other write to the same rows (a duplicate key in
/// one statement would error under `ON CONFLICT DO UPDATE`, and a row both
/// inserted and updated in the same transaction would report `xmax != 0`
/// and undercount).
pub(super) async fn insert_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &SubregistryReplayCheckpoint,
    item_rows: &[(&'static str, String, Value)],
) -> Result<usize> {
    let mut newly_inserted_assignment_count = 0;
    for chunk in item_rows.chunks(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
        if chunk.is_empty() {
            continue;
        }
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO normalized_replay_adapter_checkpoint_items (
                deployment_profile,
                chain_id,
                cursor_kind,
                adapter,
                checkpoint_scope,
                item_kind,
                item_key,
                item_payload
            )
            "#,
        );
        builder.push_values(
            chunk.iter(),
            |mut row, (item_kind, item_key, item_payload)| {
                row.push_bind(&checkpoint.context.deployment_profile)
                    .push_bind(&checkpoint.chain)
                    .push_bind(&checkpoint.context.cursor_kind)
                    .push_bind(ADAPTER)
                    .push_bind(checkpoint.context.checkpoint_scope)
                    .push_bind(*item_kind)
                    .push_bind(item_key)
                    .push_bind(item_payload);
            },
        );
        builder.push(
            r#"
            ON CONFLICT (
                deployment_profile,
                chain_id,
                cursor_kind,
                adapter,
                checkpoint_scope,
                item_kind,
                item_key
            ) DO UPDATE
            SET item_payload = EXCLUDED.item_payload,
                updated_at = now()
            RETURNING item_kind, (xmax = 0) AS newly_inserted
            "#,
        );
        let upserted = builder
            .build_query_as::<(String, bool)>()
            .fetch_all(transaction.as_mut())
            .await
            .context("failed to upsert replay adapter checkpoint items")?;
        newly_inserted_assignment_count += upserted
            .iter()
            .filter(|(item_kind, newly_inserted)| {
                *newly_inserted && item_kind == ITEM_KIND_LATEST_ASSIGNMENT
            })
            .count();
    }
    Ok(newly_inserted_assignment_count)
}
