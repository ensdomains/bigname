use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use bigname_manifests::ResolverProfileAuthorityTargetPages;
use bigname_storage::{
    ResolverProfileAuthorityJournalAdvance, ResolverProfileAuthorityJournalEntry,
    begin_resolver_profile_authority_journal_advance, load_resolver_profile_authority_journal,
};
use tracing::info;

#[cfg(test)]
use super::ResolverProfileAuthoritySnapshot;
use super::capture_resolver_profile_authority_for_targets;
#[cfg(test)]
use bigname_storage::ResolverProfileAuthorityJournal;

const MAX_AUTHORITY_JOURNAL_ATTEMPTS: usize = 32;
const AUTHORITY_TARGET_PAGE_SIZE: usize = 250;
const MIN_AUTHORITY_JOURNAL_POOL_CONNECTIONS: u32 = 3;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolverProfileAuthorityJournalSummary {
    pub(crate) epoch_guard_count: usize,
    pub(crate) authority_scan_count: usize,
    pub(crate) enqueued_target_count: u64,
    pub(crate) unstable_epoch_count: usize,
    pub(crate) cas_conflict_count: usize,
    pub(crate) journal_advanced: bool,
}

/// Compare current manifest/discovery authority to the last entry set whose
/// forced work was durably queued. Revision zero is the migration baseline: it
/// records current authority without claiming historical replay completeness.
/// Later queue rows and journal entries commit atomically; a stale revision
/// rolls both changes back.
pub(crate) async fn journal_resolver_profile_authority(
    pool: &sqlx::PgPool,
) -> Result<ResolverProfileAuthorityJournalSummary> {
    ensure!(
        pool.options().get_max_connections() >= MIN_AUTHORITY_JOURNAL_POOL_CONNECTIONS,
        "resolver-profile authority journal requires at least \
         {MIN_AUTHORITY_JOURNAL_POOL_CONNECTIONS} database connections (runtime writer guard, \
         journal transaction, and one available for bounded authority admission reads), but the \
         pool allows only {}",
        pool.options().get_max_connections()
    );
    let mut summary = ResolverProfileAuthorityJournalSummary::default();

    for _ in 0..MAX_AUTHORITY_JOURNAL_ATTEMPTS {
        let persisted = load_resolver_profile_authority_journal(pool).await?;
        let persisted_epochs = serde_json::from_value::<BTreeMap<String, i64>>(
            persisted.discovery_epoch_snapshot.clone(),
        )
        .context("failed to decode persisted resolver-profile discovery-epoch snapshot")?;
        let epochs_before = load_discovery_admission_epochs(pool).await?;
        let mut advance =
            begin_resolver_profile_authority_journal_advance(pool, persisted.revision).await?;
        stage_current_authority(pool, &mut advance).await?;
        summary.authority_scan_count += 1;
        let epochs_after = load_discovery_admission_epochs(pool).await?;
        if epochs_before != epochs_after {
            advance.abort().await?;
            summary.unstable_epoch_count += 1;
            continue;
        }
        let changed_entry_count = advance.staged_entry_diff_count().await?;
        if persisted.revision > 0 && changed_entry_count == 0 && persisted_epochs == epochs_after {
            advance.abort().await?;
            return Ok(summary);
        }

        let serialized_epochs = serde_json::to_value(&epochs_after)
            .context("failed to encode current resolver-profile discovery-epoch snapshot")?;
        let advanced = advance.publish(&serialized_epochs).await?;
        if let Some(advanced) = advanced {
            summary.enqueued_target_count += u64::try_from(advanced.enqueued_target_count)?;
            summary.journal_advanced = true;
            info!(
                service = "indexer",
                command = "resolver-profile-authority-journal",
                authority_scan_count = summary.authority_scan_count,
                staged_entry_count = advanced.staged_entry_count,
                changed_entry_count = advanced.changed_entry_count,
                enqueued_target_count = summary.enqueued_target_count,
                unstable_epoch_count = summary.unstable_epoch_count,
                cas_conflict_count = summary.cas_conflict_count,
                previous_revision = persisted.revision,
                next_revision = persisted.revision + 1,
                "resolver-profile authority diff durably journaled"
            );
            return Ok(summary);
        }
        summary.cas_conflict_count += 1;
    }

    bail!(
        "resolver-profile authority journal exceeded {MAX_AUTHORITY_JOURNAL_ATTEMPTS} revision conflicts"
    )
}

async fn stage_current_authority(
    pool: &sqlx::PgPool,
    advance: &mut ResolverProfileAuthorityJournalAdvance,
) -> Result<()> {
    let mut targets = ResolverProfileAuthorityTargetPages::begin(advance.connection_mut()?).await?;
    loop {
        let page = targets
            .next_page(advance.connection_mut()?, AUTHORITY_TARGET_PAGE_SIZE)
            .await?;
        if page.is_empty() {
            break;
        }
        let entries = capture_resolver_profile_authority_for_targets(pool, &page).await?;
        let entries = entries
            .into_iter()
            .map(|entry| {
                let payload = serde_json::to_value(entry)
                    .context("failed to encode resolver-profile authority entry")?;
                ResolverProfileAuthorityJournalEntry::from_payload(payload)
            })
            .collect::<Result<Vec<_>>>()?;
        advance.stage_entries(&entries).await?;
    }
    targets.finish(advance.connection_mut()?).await
}

/// Cheap ordinary-live guard. A chain epoch match performs no resolver-profile
/// authority scan; drift falls through to the full revision-fenced journal.
pub(crate) async fn journal_resolver_profile_authority_if_epoch_changed(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<ResolverProfileAuthorityJournalSummary> {
    let (revision, persisted_epoch) = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT
            revision,
            COALESCE((discovery_epoch_snapshot ->> $2)::BIGINT, 0)
        FROM resolver_profile_authority_journal
        WHERE journal_key = $1
        "#,
    )
    .bind("active_resolver_profiles")
    .bind(chain)
    .fetch_one(pool)
    .await
    .context("failed to load resolver-profile authority epoch guard")?;
    ensure!(
        revision >= 0,
        "resolver-profile authority journal revision must not be negative"
    );
    let current_epoch = bigname_manifests::load_discovery_admission_epoch(pool, chain).await?;
    if persisted_epoch == current_epoch {
        return Ok(ResolverProfileAuthorityJournalSummary {
            epoch_guard_count: 1,
            ..ResolverProfileAuthorityJournalSummary::default()
        });
    }

    let mut summary = journal_resolver_profile_authority(pool).await?;
    summary.epoch_guard_count += 1;
    Ok(summary)
}

async fn load_discovery_admission_epochs(pool: &sqlx::PgPool) -> Result<BTreeMap<String, i64>> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT chain_id, epoch FROM discovery_admission_epochs ORDER BY chain_id",
    )
    .fetch_all(pool)
    .await
    .context("failed to load resolver-profile discovery-admission epoch snapshot")
    .map(|rows| rows.into_iter().collect())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ResolverProfileAuthorityJournalAttempt {
    pub(super) enqueued_target_count: u64,
    pub(super) journal_advanced: bool,
}

#[cfg(test)]
pub(super) async fn journal_resolver_profile_authority_attempt(
    pool: &sqlx::PgPool,
    persisted: &ResolverProfileAuthorityJournal,
    _before: &ResolverProfileAuthoritySnapshot,
    after: &ResolverProfileAuthoritySnapshot,
    discovery_epochs: &BTreeMap<String, i64>,
) -> Result<ResolverProfileAuthorityJournalAttempt> {
    let mut advance =
        begin_resolver_profile_authority_journal_advance(pool, persisted.revision).await?;
    let entries = after
        .entries
        .iter()
        .map(|entry| {
            let payload = serde_json::to_value(entry)
                .context("failed to encode test resolver-profile authority entry")?;
            ResolverProfileAuthorityJournalEntry::from_payload(payload)
        })
        .collect::<Result<Vec<_>>>()?;
    advance.stage_entries(&entries).await?;
    let serialized_epochs = serde_json::to_value(discovery_epochs)
        .context("failed to encode test resolver-profile discovery epochs")?;
    let advanced = advance.publish(&serialized_epochs).await?;
    Ok(ResolverProfileAuthorityJournalAttempt {
        enqueued_target_count: advanced
            .map(|summary| u64::try_from(summary.enqueued_target_count))
            .transpose()?
            .unwrap_or_default(),
        journal_advanced: advanced.is_some(),
    })
}
