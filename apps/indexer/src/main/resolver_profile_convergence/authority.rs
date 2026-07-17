use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use bigname_manifests::{
    ResolverProfileAdmission, WatchedContractSource, load_basenames_l2_resolver_profile_admissions,
    load_ens_v1_public_resolver_profile_admissions,
};
use bigname_storage::{
    ResolverProfileAuthorityJournal, ResolverProfileReconciliationTarget,
    advance_resolver_profile_authority_journal, load_resolver_profile_authority_journal,
};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

const MAX_AUTHORITY_JOURNAL_ATTEMPTS: usize = 32;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ResolverProfileAuthorityEntry {
    pub(crate) chain: String,
    pub(crate) source_family: String,
    pub(crate) address: String,
    pub(crate) contract_instance_id: Uuid,
    pub(crate) source: String,
    pub(crate) source_manifest_id: Option<i64>,
    pub(crate) active_from_block_number: Option<i64>,
    pub(crate) active_to_block_number: Option<i64>,
    pub(crate) is_seed: bool,
    pub(crate) admission_semantics: BTreeSet<ResolverProfileAdmissionSemantics>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ResolverProfileAdmissionSemantics {
    pub(crate) profile: String,
    pub(crate) fact_family: String,
    pub(crate) status: String,
    pub(crate) admission_basis: String,
    pub(crate) matched_code_hash: Option<String>,
    pub(crate) matched_contract_instance_id: Option<Uuid>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ResolverProfileAuthoritySnapshot {
    pub(crate) entries: BTreeSet<ResolverProfileAuthorityEntry>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolverProfileAuthorityJournalSummary {
    pub(crate) epoch_guard_count: usize,
    pub(crate) authority_scan_count: usize,
    pub(crate) enqueued_target_count: u64,
    pub(crate) unstable_epoch_count: usize,
    pub(crate) cas_conflict_count: usize,
    pub(crate) journal_advanced: bool,
}

pub(crate) async fn capture_resolver_profile_authority(
    pool: &sqlx::PgPool,
) -> Result<ResolverProfileAuthoritySnapshot> {
    let (ens_v1, basenames) = tokio::try_join!(
        load_ens_v1_public_resolver_profile_admissions(pool),
        load_basenames_l2_resolver_profile_admissions(pool),
    )?;

    let mut grouped = BTreeMap::<AuthorityIdentity, GroupedAuthority>::new();
    for admission in ens_v1.into_iter().chain(basenames) {
        let is_seed = is_seed_admission_basis(&admission.admission_basis);
        let admission_semantics = ResolverProfileAdmissionSemantics::from(&admission);
        let entry = grouped
            .entry(AuthorityIdentity::from(&admission))
            .or_default();
        entry.is_seed |= is_seed;
        entry.admission_semantics.insert(admission_semantics);
    }

    Ok(ResolverProfileAuthoritySnapshot {
        entries: grouped
            .into_iter()
            .map(|(identity, grouped)| identity.into_entry(grouped))
            .collect(),
    })
}

/// Compare current manifest/discovery authority to the last snapshot whose
/// forced work was durably queued. Revision zero is the migration baseline: it
/// records current authority without claiming historical replay completeness.
/// Later queue rows and journal snapshots commit atomically; a stale revision
/// rolls both changes back.
pub(crate) async fn journal_resolver_profile_authority(
    pool: &sqlx::PgPool,
) -> Result<ResolverProfileAuthorityJournalSummary> {
    let mut summary = ResolverProfileAuthorityJournalSummary::default();

    for _ in 0..MAX_AUTHORITY_JOURNAL_ATTEMPTS {
        let persisted = load_resolver_profile_authority_journal(pool).await?;
        let before = serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
            persisted.authority_snapshot.clone(),
        )
        .context("failed to decode persisted resolver-profile authority snapshot")?;
        let persisted_epochs = serde_json::from_value::<BTreeMap<String, i64>>(
            persisted.discovery_epoch_snapshot.clone(),
        )
        .context("failed to decode persisted resolver-profile discovery-epoch snapshot")?;
        let epochs_before = load_discovery_admission_epochs(pool).await?;
        let after = capture_resolver_profile_authority(pool).await?;
        summary.authority_scan_count += 1;
        let epochs_after = load_discovery_admission_epochs(pool).await?;
        if epochs_before != epochs_after {
            summary.unstable_epoch_count += 1;
            continue;
        }
        if persisted.revision > 0 && before == after && persisted_epochs == epochs_after {
            return Ok(summary);
        }

        let attempt = journal_resolver_profile_authority_attempt(
            pool,
            &persisted,
            &before,
            &after,
            &epochs_after,
        )
        .await?;
        summary.enqueued_target_count += attempt.enqueued_target_count;
        if attempt.journal_advanced {
            summary.journal_advanced = true;
            info!(
                service = "indexer",
                command = "resolver-profile-authority-journal",
                authority_scan_count = summary.authority_scan_count,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ResolverProfileAuthorityJournalAttempt {
    enqueued_target_count: u64,
    journal_advanced: bool,
}

async fn journal_resolver_profile_authority_attempt(
    pool: &sqlx::PgPool,
    persisted: &ResolverProfileAuthorityJournal,
    before: &ResolverProfileAuthoritySnapshot,
    after: &ResolverProfileAuthoritySnapshot,
    discovery_epochs: &BTreeMap<String, i64>,
) -> Result<ResolverProfileAuthorityJournalAttempt> {
    let targets = if persisted.revision == 0 {
        Vec::new()
    } else {
        authority_change_targets(before, after)
    };
    let serialized = serde_json::to_value(after)
        .context("failed to encode current resolver-profile authority snapshot")?;
    let serialized_epochs = serde_json::to_value(discovery_epochs)
        .context("failed to encode current resolver-profile discovery-epoch snapshot")?;
    let enqueued_target_count = advance_resolver_profile_authority_journal(
        pool,
        persisted.revision,
        &serialized,
        &serialized_epochs,
        &targets,
    )
    .await?;
    Ok(ResolverProfileAuthorityJournalAttempt {
        enqueued_target_count: enqueued_target_count
            .map(u64::try_from)
            .transpose()?
            .unwrap_or_default(),
        journal_advanced: enqueued_target_count.is_some(),
    })
}

fn authority_change_targets(
    before: &ResolverProfileAuthoritySnapshot,
    after: &ResolverProfileAuthoritySnapshot,
) -> Vec<ResolverProfileReconciliationTarget> {
    let changed_entries = before
        .entries
        .symmetric_difference(&after.entries)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut target_keys = changed_entries
        .iter()
        .map(|entry| (entry.chain.clone(), entry.address.clone()))
        .collect::<BTreeSet<_>>();

    let changed_seed_families = changed_entries
        .iter()
        .filter(|entry| entry.is_seed)
        .map(|entry| (entry.chain.clone(), entry.source_family.clone()))
        .collect::<BTreeSet<_>>();
    for entry in before.entries.iter().chain(&after.entries) {
        if changed_seed_families.contains(&(entry.chain.clone(), entry.source_family.clone())) {
            target_keys.insert((entry.chain.clone(), entry.address.clone()));
        }
    }

    target_keys
        .into_iter()
        .map(
            |(chain_id, contract_address)| ResolverProfileReconciliationTarget {
                chain_id,
                contract_address,
            },
        )
        .collect()
}

fn is_seed_admission_basis(admission_basis: &str) -> bool {
    matches!(
        admission_basis,
        "manifest_public_resolver_seed"
            | "first_party_known_resolver_admission"
            | "manifest_l2_resolver_seed"
    )
}

#[derive(Default)]
struct GroupedAuthority {
    is_seed: bool,
    admission_semantics: BTreeSet<ResolverProfileAdmissionSemantics>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AuthorityIdentity {
    chain: String,
    source_family: String,
    address: String,
    contract_instance_id: Uuid,
    source: String,
    source_manifest_id: Option<i64>,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
}

impl From<&ResolverProfileAdmission> for AuthorityIdentity {
    fn from(admission: &ResolverProfileAdmission) -> Self {
        Self {
            chain: admission.chain.clone(),
            source_family: admission.source_family.clone(),
            address: admission.address.clone(),
            contract_instance_id: admission.contract_instance_id,
            source: watched_contract_source_key(admission.source).to_owned(),
            source_manifest_id: admission.source_manifest_id,
            active_from_block_number: admission.active_from_block_number,
            active_to_block_number: admission.active_to_block_number,
        }
    }
}

const fn watched_contract_source_key(source: WatchedContractSource) -> &'static str {
    match source {
        WatchedContractSource::ManifestRoot => "manifest_root",
        WatchedContractSource::ManifestContract => "manifest_contract",
        WatchedContractSource::DiscoveryEdge => "discovery_edge",
    }
}

impl From<&ResolverProfileAdmission> for ResolverProfileAdmissionSemantics {
    fn from(admission: &ResolverProfileAdmission) -> Self {
        Self {
            profile: admission.profile.clone(),
            fact_family: admission.fact_family.clone(),
            status: admission.status.clone(),
            admission_basis: admission.admission_basis.clone(),
            matched_code_hash: admission.matched_code_hash.clone(),
            matched_contract_instance_id: admission.matched_contract_instance_id,
        }
    }
}

impl AuthorityIdentity {
    fn into_entry(self, grouped: GroupedAuthority) -> ResolverProfileAuthorityEntry {
        ResolverProfileAuthorityEntry {
            chain: self.chain,
            source_family: self.source_family,
            address: self.address,
            contract_instance_id: self.contract_instance_id,
            source: self.source,
            source_manifest_id: self.source_manifest_id,
            active_from_block_number: self.active_from_block_number,
            active_to_block_number: self.active_to_block_number,
            is_seed: grouped.is_seed,
            admission_semantics: grouped.admission_semantics,
        }
    }
}

#[cfg(test)]
#[path = "authority/tests.rs"]
mod tests;
