use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder, Transaction};

use crate::resolver_profile_input_changes::enqueue_resolver_profile_reconciliations_with_executor;

use super::{
    RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE, RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY,
    ResolverProfileAuthorityJournalEntry, validate_journal_header,
};

#[path = "advance/diff.rs"]
mod diff;
#[path = "advance/mutations.rs"]
mod mutations;

const RESOLVER_PROFILE_AUTHORITY_TARGET_BATCH_SIZE: usize = 1_000;

#[derive(Clone, Copy)]
pub(super) struct ResolverProfileAuthorityJournalBatchSizes {
    pub(super) entry_mutation: usize,
    pub(super) target_enqueue: usize,
}

impl Default for ResolverProfileAuthorityJournalBatchSizes {
    fn default() -> Self {
        Self {
            entry_mutation: RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE,
            target_enqueue: RESOLVER_PROFILE_AUTHORITY_TARGET_BATCH_SIZE,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResolverProfileAuthorityJournalAdvanceSummary {
    pub staged_entry_count: usize,
    pub staging_statement_count: usize,
    pub max_staged_entry_batch_size: usize,
    pub changed_entry_count: i64,
    pub enqueued_target_count: i64,
    pub target_enqueue_statement_count: usize,
    pub max_target_enqueue_batch_size: usize,
    pub upserted_entry_count: i64,
    pub deleted_entry_count: i64,
    pub entry_mutation_statement_count: usize,
    pub max_entry_mutation_batch_size: usize,
}

/// Transaction-scoped staging and compare-and-set publication for one
/// [resolver-profile](../../../../docs/glossary.md) authority capture.
pub struct ResolverProfileAuthorityJournalAdvance {
    transaction: Transaction<'static, Postgres>,
    expected_revision: i64,
    batch_sizes: ResolverProfileAuthorityJournalBatchSizes,
    summary: ResolverProfileAuthorityJournalAdvanceSummary,
    changed_entry_count: Option<i64>,
}

impl ResolverProfileAuthorityJournalAdvance {
    pub(super) async fn begin(pool: &PgPool, expected_revision: i64) -> Result<Self> {
        Self::begin_with_batch_sizes(
            pool,
            expected_revision,
            ResolverProfileAuthorityJournalBatchSizes::default(),
        )
        .await
    }

    pub(super) async fn begin_with_batch_sizes(
        pool: &PgPool,
        expected_revision: i64,
        batch_sizes: ResolverProfileAuthorityJournalBatchSizes,
    ) -> Result<Self> {
        validate_journal_header(expected_revision, &serde_json::json!({}))?;
        ensure!(
            batch_sizes.entry_mutation > 0,
            "resolver-profile authority journal entry batch size must be positive"
        );
        ensure!(
            batch_sizes.target_enqueue > 0,
            "resolver-profile authority journal target batch size must be positive"
        );

        let mut transaction = pool
            .begin()
            .await
            .context("failed to begin resolver-profile authority journal handoff")?;
        diff::create_after_entries_table(&mut transaction).await?;
        Ok(Self {
            transaction,
            expected_revision,
            batch_sizes,
            summary: ResolverProfileAuthorityJournalAdvanceSummary::default(),
            changed_entry_count: None,
        })
    }

    pub async fn stage_entries(
        &mut self,
        entries: &[ResolverProfileAuthorityJournalEntry],
    ) -> Result<()> {
        ensure!(
            self.changed_entry_count.is_none(),
            "cannot stage resolver-profile authority entries after preparing the diff"
        );
        for chunk in entries.chunks(self.batch_sizes.entry_mutation) {
            stage_entry_chunk(&mut self.transaction, chunk).await?;
            self.summary.staged_entry_count += chunk.len();
            self.summary.staging_statement_count += 1;
            self.summary.max_staged_entry_batch_size =
                self.summary.max_staged_entry_batch_size.max(chunk.len());
        }
        Ok(())
    }

    /// Borrow the journal transaction while constructing the bounded AFTER
    /// set. Callers may stream authority inputs through this connection before
    /// asking the advance to materialize its diff.
    pub fn connection_mut(&mut self) -> Result<&mut PgConnection> {
        ensure!(
            self.changed_entry_count.is_none(),
            "cannot read resolver-profile authority inputs after preparing the diff"
        );
        Ok(self.transaction.as_mut())
    }

    pub async fn staged_entry_diff_count(&mut self) -> Result<i64> {
        if let Some(count) = self.changed_entry_count {
            return Ok(count);
        }
        let count = diff::materialize_changed_entry_keys(
            &mut self.transaction,
            RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY,
        )
        .await?;
        self.changed_entry_count = Some(count);
        self.summary.changed_entry_count = count;
        Ok(count)
    }

    /// Queue the staged diff, apply exact entry mutations in bounded
    /// statements, and finally compare-and-set the journal header.
    pub async fn publish(
        mut self,
        discovery_epoch_snapshot: &Value,
    ) -> Result<Option<ResolverProfileAuthorityJournalAdvanceSummary>> {
        validate_journal_header(self.expected_revision, discovery_epoch_snapshot)?;
        let changed_entry_count = self.staged_entry_diff_count().await?;
        if self.expected_revision > 0 && changed_entry_count > 0 {
            diff::materialize_reconciliation_targets(
                &mut self.transaction,
                RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY,
            )
            .await?;
            self.enqueue_reconciliation_targets().await?;
        }

        let mutation_summary = mutations::apply_entry_diff(
            &mut self.transaction,
            RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY,
            self.batch_sizes.entry_mutation,
        )
        .await?;
        self.summary.upserted_entry_count = mutation_summary.upserted_entry_count;
        self.summary.deleted_entry_count = mutation_summary.deleted_entry_count;
        self.summary.entry_mutation_statement_count = mutation_summary.statement_count;
        self.summary.max_entry_mutation_batch_size = mutation_summary.max_batch_size;

        let updated_revision = sqlx::query_scalar::<_, i64>(
            r#"
            UPDATE resolver_profile_authority_journal
            SET
                revision = revision + 1,
                discovery_epoch_snapshot = $2,
                updated_at = now()
            WHERE journal_key = $1
              AND revision = $3
            RETURNING revision
            "#,
        )
        .bind(RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY)
        .bind(discovery_epoch_snapshot)
        .bind(self.expected_revision)
        .fetch_optional(&mut *self.transaction)
        .await
        .context("failed to compare-and-set resolver-profile authority journal")?;

        if updated_revision.is_none() {
            self.transaction
                .rollback()
                .await
                .context("failed to roll back stale resolver-profile authority handoff")?;
            return Ok(None);
        }

        self.transaction
            .commit()
            .await
            .context("failed to commit resolver-profile authority journal handoff")?;
        Ok(Some(self.summary))
    }

    pub async fn abort(self) -> Result<()> {
        self.transaction
            .rollback()
            .await
            .context("failed to roll back resolver-profile authority journal capture")
    }

    async fn enqueue_reconciliation_targets(&mut self) -> Result<()> {
        let mut after = None::<(String, String)>;
        loop {
            let page = diff::load_reconciliation_target_page(
                &mut self.transaction,
                after.as_ref(),
                self.batch_sizes.target_enqueue,
            )
            .await?;
            let Some(last) = page.last() else {
                return Ok(());
            };
            after = Some((last.chain_id.clone(), last.contract_address.clone()));
            let recorded = enqueue_resolver_profile_reconciliations_with_executor(
                &mut *self.transaction,
                &page,
            )
            .await?;
            self.summary.enqueued_target_count += recorded;
            self.summary.target_enqueue_statement_count += 1;
            self.summary.max_target_enqueue_batch_size =
                self.summary.max_target_enqueue_batch_size.max(page.len());
        }
    }
}

async fn stage_entry_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    entries: &[ResolverProfileAuthorityJournalEntry],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO pg_temp.resolver_profile_authority_after_entries (
            entry_key,
            entry_payload
        )
        "#,
    );
    builder.push_values(entries, |mut row, entry| {
        row.push_bind(&entry.entry_key)
            .push_bind(&entry.entry_payload);
    });
    builder.push(" ON CONFLICT (entry_key) DO NOTHING");
    let inserted = builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to stage resolver-profile authority entry batch")?
        .rows_affected();
    ensure!(
        inserted == u64::try_from(entries.len())?,
        "resolver-profile authority page repeated an entry key across bounded batches"
    );
    Ok(())
}
