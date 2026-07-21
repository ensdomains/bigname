use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_manifests::{
    ResolverProfileAdmission, WatchedContractSource,
    load_basenames_l2_resolver_profile_admissions_for_targets,
    load_ens_v1_public_resolver_profile_admissions_for_targets,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[path = "authority/journal.rs"]
mod journal;

#[cfg(test)]
use journal::journal_resolver_profile_authority_attempt;
pub(crate) use journal::{
    journal_resolver_profile_authority, journal_resolver_profile_authority_if_epoch_changed,
};

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

#[cfg(test)]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ResolverProfileAuthoritySnapshot {
    pub(crate) entries: BTreeSet<ResolverProfileAuthorityEntry>,
}

pub(super) async fn capture_resolver_profile_authority_for_targets(
    pool: &sqlx::PgPool,
    targets: &[(String, String)],
) -> Result<Vec<ResolverProfileAuthorityEntry>> {
    let (ens_v1, basenames) = tokio::try_join!(
        load_ens_v1_public_resolver_profile_admissions_for_targets(pool, targets),
        load_basenames_l2_resolver_profile_admissions_for_targets(pool, targets),
    )?;
    Ok(
        authority_entries_from_admissions(ens_v1.into_iter().chain(basenames))
            .into_iter()
            .collect(),
    )
}

fn authority_entries_from_admissions(
    admissions: impl IntoIterator<Item = ResolverProfileAdmission>,
) -> BTreeSet<ResolverProfileAuthorityEntry> {
    let mut grouped = BTreeMap::<AuthorityIdentity, GroupedAuthority>::new();
    for admission in admissions {
        let is_seed = is_seed_admission_basis(&admission.admission_basis);
        let admission_semantics = ResolverProfileAdmissionSemantics::from(&admission);
        let entry = grouped
            .entry(AuthorityIdentity::from(&admission))
            .or_default();
        entry.is_seed |= is_seed;
        entry.admission_semantics.insert(admission_semantics);
    }
    grouped
        .into_iter()
        .map(|(identity, grouped)| identity.into_entry(grouped))
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
