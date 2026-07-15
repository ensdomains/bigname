use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
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
    let persisted = load_resolver_profile_authority_journal(pool).await?;
    let persisted_epochs =
        serde_json::from_value::<BTreeMap<String, i64>>(persisted.discovery_epoch_snapshot)
            .context("failed to decode persisted resolver-profile discovery-epoch snapshot")?;
    let current_epoch = bigname_manifests::load_discovery_admission_epoch(pool, chain).await?;
    if persisted_epochs.get(chain).copied().unwrap_or_default() == current_epoch {
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
mod tests {
    use anyhow::Result;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    fn admission_semantics(
        status: &str,
        admission_basis: &str,
    ) -> ResolverProfileAdmissionSemantics {
        ResolverProfileAdmissionSemantics {
            profile: "ens_v1_public_resolver_compatible".to_owned(),
            fact_family: "ens_v1_resolver_records".to_owned(),
            status: status.to_owned(),
            admission_basis: admission_basis.to_owned(),
            matched_code_hash: Some("0x01".to_owned()),
            matched_contract_instance_id: Some(Uuid::from_u128(1)),
        }
    }

    fn entry(address: &str, is_seed: bool) -> ResolverProfileAuthorityEntry {
        ResolverProfileAuthorityEntry {
            chain: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            address: address.to_owned(),
            contract_instance_id: Uuid::new_v4(),
            source: "discovery_edge".to_owned(),
            source_manifest_id: Some(1),
            active_from_block_number: Some(1),
            active_to_block_number: None,
            is_seed,
            admission_semantics: BTreeSet::from([admission_semantics(
                "admitted",
                if is_seed {
                    "manifest_public_resolver_seed"
                } else {
                    "matching_seed_code_hash"
                },
            )]),
        }
    }

    #[test]
    fn candidate_authority_change_targets_only_that_address() {
        let before = ResolverProfileAuthoritySnapshot::default();
        let candidate = entry("0x0000000000000000000000000000000000000002", false);
        let after = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([candidate.clone()]),
        };

        assert_eq!(
            authority_change_targets(&before, &after),
            vec![ResolverProfileReconciliationTarget {
                chain_id: candidate.chain,
                contract_address: candidate.address,
            }]
        );
    }

    #[test]
    fn seed_authority_change_targets_every_family_candidate() {
        let seed = entry("0x0000000000000000000000000000000000000001", true);
        let candidate = entry("0x0000000000000000000000000000000000000002", false);
        let after = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([seed, candidate]),
        };

        let targets =
            authority_change_targets(&ResolverProfileAuthoritySnapshot::default(), &after);
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn admission_semantics_change_targets_the_unchanged_candidate_identity() {
        let before_entry = entry("0x0000000000000000000000000000000000000002", false);
        let mut after_entry = before_entry.clone();
        after_entry.admission_semantics = BTreeSet::from([admission_semantics(
            "pending_code_hash",
            "matching_seed_code_hash",
        )]);
        let before = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([before_entry]),
        };
        let after = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([after_entry.clone()]),
        };

        assert_eq!(
            authority_change_targets(&before, &after),
            vec![ResolverProfileReconciliationTarget {
                chain_id: after_entry.chain,
                contract_address: after_entry.address,
            }]
        );
    }

    #[test]
    fn seed_admission_semantics_change_ripples_to_unchanged_candidates() {
        let before_seed = entry("0x0000000000000000000000000000000000000001", true);
        let candidate = entry("0x0000000000000000000000000000000000000002", false);
        let mut after_seed = before_seed.clone();
        after_seed.admission_semantics = BTreeSet::from([admission_semantics(
            "admitted",
            "first_party_known_resolver_admission",
        )]);
        let before = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([before_seed, candidate.clone()]),
        };
        let after = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([after_seed, candidate]),
        };

        let targets = authority_change_targets(&before, &after);
        assert_eq!(targets.len(), 2);
        assert_eq!(
            targets[0].contract_address,
            "0x0000000000000000000000000000000000000001"
        );
        assert_eq!(
            targets[1].contract_address,
            "0x0000000000000000000000000000000000000002"
        );
    }

    #[tokio::test]
    async fn empty_initial_capture_establishes_baseline_before_later_addition() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("indexer_resolver_profile_empty_authority_baseline"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for empty resolver-profile authority baseline test",
        )
        .await?;

        let first = journal_resolver_profile_authority(database.pool()).await?;
        assert!(first.journal_advanced);
        assert_eq!(first.enqueued_target_count, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM resolver_profile_input_changes"
            )
            .fetch_one(database.pool())
            .await?,
            0,
            "an empty first capture is a baseline, not repair work"
        );

        let baseline = load_resolver_profile_authority_journal(database.pool()).await?;
        assert_eq!(baseline.revision, 1);
        let before = serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
            baseline.authority_snapshot.clone(),
        )?;
        assert_eq!(before, ResolverProfileAuthoritySnapshot::default());

        let address = "0x0000000000000000000000000000000000000002";
        let added = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([entry(address, false)]),
        };
        let second = journal_resolver_profile_authority_attempt(
            database.pool(),
            &baseline,
            &before,
            &added,
            &BTreeMap::new(),
        )
        .await?;
        assert!(second.journal_advanced);
        assert_eq!(second.enqueued_target_count, 1);
        assert_eq!(
            sqlx::query_as::<_, (bool, bool)>(
                r#"
                SELECT
                    processed_generation < generation AS pending,
                    force_reconciliation
                FROM resolver_profile_input_changes
                WHERE chain_id = 'ethereum-mainnet'
                  AND contract_address = $1
                "#,
            )
            .bind(address)
            .fetch_one(database.pool())
            .await?,
            (true, true),
            "authority added after the baseline must become forced repair work"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn journal_baselines_initial_authority_then_queues_later_removals() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("indexer_resolver_profile_authority_journal"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for resolver-profile authority journal test",
        )
        .await?;
        let address = "0x0000000000000000000000000000000000000002";
        sqlx::query(
            r#"
            INSERT INTO raw_log_staging_input_revisions (
                chain_id,
                revision,
                retention_generation,
                retained_history_complete,
                incomplete_since
            ) VALUES ('ethereum-mainnet', 0, 1, false, now())
            "#,
        )
        .execute(database.pool())
        .await?;
        let added = ResolverProfileAuthoritySnapshot {
            entries: BTreeSet::from([entry(address, false)]),
        };
        let initial = load_resolver_profile_authority_journal(database.pool()).await?;
        let first = journal_resolver_profile_authority_attempt(
            database.pool(),
            &initial,
            &ResolverProfileAuthoritySnapshot::default(),
            &added,
            &BTreeMap::new(),
        )
        .await?;
        assert_eq!(first.enqueued_target_count, 0);
        assert!(first.journal_advanced);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM resolver_profile_input_changes"
            )
            .fetch_one(database.pool())
            .await?,
            0,
            "the first journal snapshot is a baseline, not an unproven historical repair request"
        );

        sqlx::query(
            r#"
            CREATE FUNCTION require_profile_authority_enqueue_before_journal()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM resolver_profile_input_changes
                    WHERE chain_id = 'ethereum-mainnet'
                      AND contract_address = '0x0000000000000000000000000000000000000002'
                      AND processed_generation < generation
                      AND force_reconciliation
                ) THEN
                    RAISE EXCEPTION 'resolver-profile target was not queued before journal CAS';
                END IF;
                RETURN NEW;
            END;
            $$;
            "#,
        )
        .execute(database.pool())
        .await?;
        sqlx::query(
            r#"
            CREATE TRIGGER require_profile_authority_enqueue_before_journal
            BEFORE UPDATE ON resolver_profile_authority_journal
            FOR EACH ROW
            EXECUTE FUNCTION require_profile_authority_enqueue_before_journal();
            "#,
        )
        .execute(database.pool())
        .await?;

        let persisted = load_resolver_profile_authority_journal(database.pool()).await?;
        let before = serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
            persisted.authority_snapshot.clone(),
        )?;
        assert_eq!(before, added);
        let removed = ResolverProfileAuthoritySnapshot::default();
        let second = journal_resolver_profile_authority_attempt(
            database.pool(),
            &persisted,
            &before,
            &removed,
            &BTreeMap::new(),
        )
        .await?;
        assert_eq!(second.enqueued_target_count, 1);
        assert!(second.journal_advanced);
        assert_eq!(
            sqlx::query_as::<_, (bool, bool)>(
                r#"
                SELECT
                    processed_generation < generation AS pending,
                    force_reconciliation
                FROM resolver_profile_input_changes
                WHERE chain_id = 'ethereum-mainnet'
                  AND contract_address = $1
                "#,
            )
            .bind(address)
            .fetch_one(database.pool())
            .await?,
            (true, true),
            "the persisted before-snapshot must retain a removed target for absence cleanup"
        );
        let final_journal = load_resolver_profile_authority_journal(database.pool()).await?;
        assert_eq!(final_journal.revision, 2);
        assert_eq!(
            serde_json::from_value::<ResolverProfileAuthoritySnapshot>(
                final_journal.authority_snapshot
            )?,
            removed
        );
        let error = super::super::drain_resolver_profile_input_changes(database.pool())
            .await
            .expect_err("a later real change on unknown legacy history must fail closed");
        assert!(
            format!("{error:#}").contains("fully rebootstrap the database"),
            "unexpected resolver-profile retention error: {error:#}"
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>(
                r#"
                SELECT generation, processed_generation
                FROM resolver_profile_input_changes
                WHERE chain_id = 'ethereum-mainnet'
                  AND contract_address = $1
                "#,
            )
            .bind(address)
            .fetch_one(database.pool())
            .await?,
            (1, 0),
            "failed-closed work must remain pending for operator recovery"
        );

        database.cleanup().await
    }
}
