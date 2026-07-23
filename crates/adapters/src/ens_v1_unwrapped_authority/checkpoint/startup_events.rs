use super::*;
use crate::checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress};
use crate::ens_v1_unwrapped_authority::event_persistence::pin_existing_event_manifest_provenance;

pub(super) const ITEM_KIND_STARTUP_PENDING_EVENT: &str = "startup_pending_normalized_event";
const STARTUP_EVENT_PUBLICATION_PAGE_LIMIT: i64 = 20_000;

impl UnwrappedAuthorityReplayCheckpoint {
    pub(crate) fn is_startup(&self) -> bool {
        self.context.is_startup()
    }

    pub(crate) async fn stage_startup_events(
        &self,
        pool: &PgPool,
        events: &[NormalizedEvent],
        staged_events: &mut UnwrappedAuthorityReplayFlushedEvents,
        startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<()> {
        ensure!(
            self.is_startup(),
            "only startup authority checkpoints may stage unpublished events"
        );
        if events.is_empty() {
            return Ok(());
        }

        let mut item_rows = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            item_rows.push((
                ITEM_KIND_STARTUP_PENDING_EVENT,
                event.event_identity.clone(),
                encode_item(event)?,
            ));
            if index + 1 == events.len()
                || (index + 1).is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE)
            {
                record_startup_adapter_progress(pool, startup_progress).await?;
            }
        }
        let mut transaction = pool
            .begin()
            .await
            .context("failed to start startup authority event-staging transaction")?;
        insert_checkpoint_items_with_progress(
            pool,
            &mut transaction,
            self,
            &item_rows,
            startup_progress,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit startup authority event staging")?;

        staged_events.total_count += events.len();
        for (index, event) in events.iter().enumerate() {
            *staged_events
                .by_kind
                .entry(event.event_kind.clone())
                .or_insert(0) += 1;
            if index + 1 == events.len()
                || (index + 1).is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE)
            {
                record_startup_adapter_progress(pool, startup_progress).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn publish_startup_events(
        &mut self,
        pool: &PgPool,
        staged_events: &mut UnwrappedAuthorityReplayFlushedEvents,
        startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<usize> {
        ensure!(
            self.is_startup(),
            "only startup authority checkpoints may publish staged events"
        );
        let mut published_count = 0usize;
        loop {
            let rows = sqlx::query(
                r#"
                SELECT item_key, item_payload
                FROM normalized_replay_adapter_checkpoint_items
                WHERE deployment_profile = $1
                  AND chain_id = $2
                  AND cursor_kind = $3
                  AND adapter = $4
                  AND checkpoint_scope = $5
                  AND item_kind = $6
                ORDER BY item_key
                LIMIT $7
                "#,
            )
            .bind(&self.context.deployment_profile)
            .bind(&self.chain)
            .bind(&self.context.cursor_kind)
            .bind(ADAPTER)
            .bind(self.context.checkpoint_scope)
            .bind(ITEM_KIND_STARTUP_PENDING_EVENT)
            .bind(STARTUP_EVENT_PUBLICATION_PAGE_LIMIT)
            .fetch_all(pool)
            .await
            .context("failed to load a startup authority event publication page")?;
            if rows.is_empty() {
                break;
            }

            let mut item_keys = Vec::with_capacity(rows.len());
            let mut events = Vec::with_capacity(rows.len());
            for row in rows {
                let item_key: String = row.try_get("item_key")?;
                let event = decode_item::<NormalizedEvent>(
                    row.try_get("item_payload")?,
                    ITEM_KIND_STARTUP_PENDING_EVENT,
                )?;
                ensure!(
                    event.event_identity == item_key,
                    "startup authority staged event key does not match its event identity"
                );
                item_keys.push(item_key);
                events.push(event);
            }

            let mut transaction = pool
                .begin()
                .await
                .context("failed to start startup authority event publication transaction")?;
            pin_existing_event_manifest_provenance(&mut transaction, &mut events).await?;
            let inserted_count =
                bigname_storage::upsert_normalized_events_count_only_in_transaction(
                    &mut transaction,
                    &events,
                )
                .await?;
            delete_checkpoint_items_with_progress(
                pool,
                &mut transaction,
                self,
                ITEM_KIND_STARTUP_PENDING_EVENT,
                &item_keys,
                startup_progress,
            )
            .await?;
            let next_inserted_count = staged_events
                .inserted_count
                .checked_add(inserted_count)
                .context("startup authority inserted-event count overflowed")?;
            let checkpoint_update = sqlx::query(
                r#"
                UPDATE normalized_replay_adapter_checkpoints
                SET state_payload = jsonb_set(
                        state_payload,
                        '{flushed_normalized_event_inserted_count}',
                        to_jsonb($6::BIGINT),
                        true
                    ),
                    updated_at = now()
                WHERE deployment_profile = $1
                  AND chain_id = $2
                  AND cursor_kind = $3
                  AND adapter = $4
                  AND checkpoint_scope = $5
                "#,
            )
            .bind(&self.context.deployment_profile)
            .bind(&self.chain)
            .bind(&self.context.cursor_kind)
            .bind(ADAPTER)
            .bind(self.context.checkpoint_scope)
            .bind(
                i64::try_from(next_inserted_count)
                    .context("startup authority inserted-event count overflowed i64")?,
            )
            .execute(transaction.as_mut())
            .await
            .context("failed to advance startup authority event publication")?;
            ensure!(
                checkpoint_update.rows_affected() == 1,
                "startup authority checkpoint disappeared during event publication"
            );
            transaction
                .commit()
                .await
                .context("failed to commit startup authority event publication page")?;

            staged_events.inserted_count = next_inserted_count;
            published_count += events.len();
            record_startup_adapter_progress(pool, startup_progress).await?;
        }
        self.flushed_events = staged_events.clone();
        Ok(published_count)
    }
}
