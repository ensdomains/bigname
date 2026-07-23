use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Transaction};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct EntryMutationSummary {
    pub(super) upserted_entry_count: i64,
    pub(super) deleted_entry_count: i64,
    pub(super) statement_count: usize,
    pub(super) max_batch_size: usize,
}

pub(super) async fn apply_entry_diff(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    batch_size: usize,
    progress: &mut Option<&mut dyn super::ResolverProfileAuthorityJournalProgress>,
) -> Result<EntryMutationSummary> {
    ensure!(batch_size > 0, "journal entry batch size must be positive");
    let mut summary = EntryMutationSummary::default();
    let mut after_key = None::<String>;
    loop {
        let (count, last_key) =
            upsert_entry_batch(transaction, journal_key, after_key.as_deref(), batch_size).await?;
        let Some(last_key) = last_key else {
            break;
        };
        after_key = Some(last_key);
        record_batch(&mut summary, count)?;
        summary.upserted_entry_count += count;
        super::record_journal_progress(progress).await?;
    }

    let mut after_key = None::<String>;
    loop {
        let (count, last_key) =
            delete_entry_batch(transaction, journal_key, after_key.as_deref(), batch_size).await?;
        let Some(last_key) = last_key else {
            break;
        };
        after_key = Some(last_key);
        record_batch(&mut summary, count)?;
        summary.deleted_entry_count += count;
        super::record_journal_progress(progress).await?;
    }
    Ok(summary)
}

fn record_batch(summary: &mut EntryMutationSummary, count: i64) -> Result<()> {
    let count = usize::try_from(count)?;
    summary.statement_count += 1;
    summary.max_batch_size = summary.max_batch_size.max(count);
    Ok(())
}

async fn upsert_entry_batch(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    after_key: Option<&str>,
    batch_size: usize,
) -> Result<(i64, Option<String>)> {
    sqlx::query_as::<_, (i64, Option<String>)>(
        r#"
        WITH candidates AS (
            SELECT after.entry_key, after.entry_payload
            FROM pg_temp.resolver_profile_authority_after_entries after
            LEFT JOIN resolver_profile_authority_journal_entries before
              ON before.journal_key = $1
             AND before.entry_key = after.entry_key
            WHERE ($2::TEXT IS NULL OR after.entry_key > $2)
              AND before.entry_payload IS DISTINCT FROM after.entry_payload
            ORDER BY after.entry_key
            LIMIT $3
        ),
        upserted AS (
            INSERT INTO resolver_profile_authority_journal_entries (
                journal_key,
                entry_key,
                entry_payload
            )
            SELECT $1, entry_key, entry_payload
            FROM candidates
            ON CONFLICT (journal_key, entry_key) DO UPDATE
            SET entry_payload = EXCLUDED.entry_payload
            RETURNING entry_key
        )
        SELECT COUNT(*)::BIGINT, MAX(entry_key)::TEXT
        FROM upserted
        "#,
    )
    .bind(journal_key)
    .bind(after_key)
    .bind(i64::try_from(batch_size)?)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to upsert a resolver-profile authority entry batch")
}

async fn delete_entry_batch(
    transaction: &mut Transaction<'_, Postgres>,
    journal_key: &str,
    after_key: Option<&str>,
    batch_size: usize,
) -> Result<(i64, Option<String>)> {
    sqlx::query_as::<_, (i64, Option<String>)>(
        r#"
        WITH candidates AS (
            SELECT before.entry_key
            FROM resolver_profile_authority_journal_entries before
            LEFT JOIN pg_temp.resolver_profile_authority_after_entries after
              ON after.entry_key = before.entry_key
            WHERE before.journal_key = $1
              AND ($2::TEXT IS NULL OR before.entry_key > $2)
              AND after.entry_key IS NULL
            ORDER BY before.entry_key
            LIMIT $3
        ),
        deleted AS (
            DELETE FROM resolver_profile_authority_journal_entries before
            USING candidates
            WHERE before.journal_key = $1
              AND before.entry_key = candidates.entry_key
            RETURNING before.entry_key
        )
        SELECT COUNT(*)::BIGINT, MAX(entry_key)::TEXT
        FROM deleted
        "#,
    )
    .bind(journal_key)
    .bind(after_key)
    .bind(i64::try_from(batch_size)?)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to delete a resolver-profile authority entry batch")
}
