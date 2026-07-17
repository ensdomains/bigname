use super::*;

/// Re-derivation keeps the manifest provenance originally attached to an
/// existing event identity. Every other identity field remains subject to the
/// storage-owned strict equality check.
pub(super) async fn upsert_events_preserving_manifest_provenance(
    pool: &PgPool,
    events: &mut [NormalizedEvent],
) -> Result<usize> {
    if events.is_empty() {
        return Ok(0);
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin ENSv1 normalized-event publication")?;
    pin_existing_event_manifest_provenance(&mut transaction, events).await?;
    let inserted_count = bigname_storage::upsert_normalized_events_count_only_in_transaction(
        &mut transaction,
        events,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit ENSv1 normalized-event publication")?;
    Ok(inserted_count)
}

pub(super) async fn pin_existing_event_manifest_provenance(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &mut [NormalizedEvent],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let existing = sqlx::query_as::<_, (String, i64, Option<i64>)>(
        r#"
        SELECT event_identity, manifest_version, source_manifest_id
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        FOR UPDATE
        "#,
    )
    .bind(&event_identities)
    .fetch_all(transaction.as_mut())
    .await
    .context("failed to lock existing ENSv1 normalized-event manifest provenance")?
    .into_iter()
    .map(|(event_identity, manifest_version, source_manifest_id)| {
        (event_identity, (manifest_version, source_manifest_id))
    })
    .collect::<HashMap<_, _>>();

    for event in events {
        let Some((manifest_version, source_manifest_id)) = existing.get(&event.event_identity)
        else {
            continue;
        };
        event.manifest_version = *manifest_version;
        event.source_manifest_id = *source_manifest_id;
    }
    Ok(())
}
