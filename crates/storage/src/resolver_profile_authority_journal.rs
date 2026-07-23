use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Row};

#[path = "resolver_profile_authority_journal/advance.rs"]
mod advance;
#[path = "resolver_profile_authority_journal/reads.rs"]
mod reads;

pub use advance::{
    ResolverProfileAuthorityJournalAdvance, ResolverProfileAuthorityJournalAdvanceSummary,
    ResolverProfileAuthorityJournalProgress, ResolverProfileAuthorityJournalProgressFuture,
};
pub use reads::{
    load_resolver_profile_authority_entries_for_targets,
    load_resolver_profile_authority_family_target_page,
};

pub const RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE: usize = 1_000;
pub(crate) const RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY: &str = "active_resolver_profiles";

const AUTHORITY_IDENTITY_FIELDS: [&str; 8] = [
    "chain",
    "source_family",
    "address",
    "contract_instance_id",
    "source",
    "source_manifest_id",
    "active_from_block_number",
    "active_to_block_number",
];

/// Revision and discovery epochs for the last authority entry set whose diff
/// was durably queued.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileAuthorityJournal {
    pub revision: i64,
    pub discovery_epoch_snapshot: Value,
}

/// One normalized entry in the
/// [resolver-profile](../../../docs/glossary.md) authority journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileAuthorityJournalEntry {
    pub entry_key: String,
    pub entry_payload: Value,
}

impl ResolverProfileAuthorityJournalEntry {
    pub fn from_payload(entry_payload: Value) -> Result<Self> {
        validate_authority_entry_payload(&entry_payload)?;
        let entry_key = resolver_profile_authority_entry_key(&entry_payload)?;
        Ok(Self {
            entry_key,
            entry_payload,
        })
    }
}

pub async fn load_resolver_profile_authority_journal(
    pool: &PgPool,
) -> Result<ResolverProfileAuthorityJournal> {
    let row = sqlx::query(
        r#"
        SELECT revision, discovery_epoch_snapshot
        FROM resolver_profile_authority_journal
        WHERE journal_key = $1
        "#,
    )
    .bind(RESOLVER_PROFILE_AUTHORITY_JOURNAL_KEY)
    .fetch_one(pool)
    .await
    .context("failed to load resolver-profile authority journal header")?;

    let revision = row.try_get("revision")?;
    let discovery_epoch_snapshot = row.try_get("discovery_epoch_snapshot")?;
    validate_journal_header(revision, &discovery_epoch_snapshot)?;
    Ok(ResolverProfileAuthorityJournal {
        revision,
        discovery_epoch_snapshot,
    })
}

/// Begin one transaction-scoped authority-journal replacement.
///
/// The caller stages the current authority in bounded pages. Publishing
/// force-enqueues the staged/before diff, mutates only changed entry rows, and
/// advances the header with a revision compare-and-set in that order. A stale
/// revision rolls every queue and entry mutation back.
pub async fn begin_resolver_profile_authority_journal_advance(
    pool: &PgPool,
    expected_revision: i64,
) -> Result<ResolverProfileAuthorityJournalAdvance> {
    ResolverProfileAuthorityJournalAdvance::begin(pool, expected_revision).await
}

pub fn resolver_profile_authority_entry_key(entry_payload: &Value) -> Result<String> {
    validate_authority_entry_payload(entry_payload)?;
    let components = AUTHORITY_IDENTITY_FIELDS
        .iter()
        .map(|field| {
            entry_payload
                .get(*field)
                .cloned()
                .unwrap_or(Value::Null)
                .to_string()
        })
        .collect::<Vec<_>>();
    Ok(format!("[{}]", components.join(", ")))
}

pub(crate) fn validate_journal_header(
    revision: i64,
    discovery_epoch_snapshot: &Value,
) -> Result<()> {
    ensure!(
        revision >= 0,
        "resolver-profile authority journal revision must not be negative"
    );
    ensure!(
        discovery_epoch_snapshot.is_object(),
        "resolver-profile discovery-epoch snapshot must be an object"
    );
    Ok(())
}

fn validate_authority_entry_payload(entry_payload: &Value) -> Result<()> {
    let object = entry_payload
        .as_object()
        .context("resolver-profile authority journal entry payload must be an object")?;
    for field in [
        "chain",
        "source_family",
        "address",
        "contract_instance_id",
        "source",
    ] {
        ensure!(
            object.get(field).is_some_and(Value::is_string),
            "resolver-profile authority journal entry {field} must be a string"
        );
    }
    for field in [
        "source_manifest_id",
        "active_from_block_number",
        "active_to_block_number",
    ] {
        ensure!(
            object
                .get(field)
                .is_some_and(|value| value.is_i64() || value.is_null()),
            "resolver-profile authority journal entry {field} must be an integer or null"
        );
    }
    ensure!(
        object.get("is_seed").is_some_and(Value::is_boolean),
        "resolver-profile authority journal entry is_seed must be a boolean"
    );
    ensure!(
        object
            .get("admission_semantics")
            .is_some_and(Value::is_array),
        "resolver-profile authority journal entry admission_semantics must be an array"
    );
    Ok(())
}

#[cfg(test)]
#[path = "resolver_profile_authority_journal/tests.rs"]
mod tests;
