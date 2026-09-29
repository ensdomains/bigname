use serde::{Deserialize, Serialize};

use super::{V2Error, V2Result};

#[path = "vocab/wrapper_fuses.rs"]
mod wrapper_fuses;
pub(crate) use wrapper_fuses::WrapperFuses;
#[path = "vocab/wrapper_state.rs"]
mod wrapper_state;
pub(crate) use wrapper_state::WrapperState;

#[path = "vocab/authority.rs"]
mod authority;
pub(crate) use authority::{Authority, AuthoritySet};
#[path = "vocab/history_event_type.rs"]
mod history_event_type;
pub(crate) use history_event_type::{HistoryEventType, HistoryEventTypeSet};
#[path = "vocab/sort.rs"]
mod sort;
pub(crate) use sort::AddressNamesSort;

#[path = "vocab/unsupported_reason.rs"]
mod unsupported_reason;
pub(crate) use unsupported_reason::{
    MISSING_UNSUPPORTED_REASON, PARTIAL_SERVE_UNSUPPORTED_REASON, downgrades_unsupported_name,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    NotFound,
    InvalidName,
    Mismatch,
    Unsupported,
    Stale,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OpsStatus {
    Ready,
    Degraded,
    Stale,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Completeness {
    Full,
    Partial,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Indexed,
    Verified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Finality {
    Latest,
    Safe,
    Finalized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryScope {
    Name,
    Registration,
    Both,
}

impl HistoryScope {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Registration => "registration",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegistrationStatus {
    Active,
    Wrapped,
    Registered,
    Released,
    Unregistered,
}

/// How a permission row may be read. `CurrentForName` is claimed only when a
/// `name` filter selected the row's current registration for that name;
/// everything else is a resource-keyed read that makes no current-name claim.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthorityContext {
    CurrentForName,
    ResourceAudit,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Relation {
    Owner,
    Manager,
    Registrant,
    /// The address holds an ENSv2 registry role on the name; not exclusive, and not the manager.
    RoleHolder,
    /// The address is the value of the name's current `addr:<coin_type>` resolver record. A
    /// resolver-record relation, not an authority relation: it is coin-type scoped, never part
    /// of `any`, and never combined with the authority relations in one set.
    ResolvesTo,
    /// The address was the last registrant of the name's registration when it ended: an ENSv1
    /// lease that lapsed past grace, or an ENSv2 registration that expired or was unregistered
    /// (`lapsed_registration.registrant`). Never current authority, never part of `any`, and never
    /// combined with another relation in one set.
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L341-L362 @ ens_v2_sepolia_20260916@366de741)
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
    FormerRegistrant,
}

impl Relation {
    /// The authority relations `any` expands to. `resolves_to` is deliberately outside this set.
    pub(crate) const ALL: [Self; 4] = [
        Self::Owner,
        Self::Manager,
        Self::Registrant,
        Self::RoleHolder,
    ];

    /// The relations reverse lookup serves, and what `any` expands to there.
    pub(crate) const LOOKUP: [Self; 3] = [Self::Owner, Self::Manager, Self::Registrant];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Manager => "manager",
            Self::Registrant => "registrant",
            Self::RoleHolder => "role_holder",
            Self::ResolvesTo => "resolves_to",
            Self::FormerRegistrant => "former_registrant",
        }
    }

    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "manager" => Some(Self::Manager),
            "registrant" => Some(Self::Registrant),
            "role_holder" => Some(Self::RoleHolder),
            "resolves_to" => Some(Self::ResolvesTo),
            "former_registrant" => Some(Self::FormerRegistrant),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelationSet {
    relations: Vec<Relation>,
}

impl RelationSet {
    pub(crate) fn all() -> Self {
        Self {
            relations: Relation::ALL.to_vec(),
        }
    }

    /// Canonicalizes a requested set. `resolves_to` is only valid on its own: a set that mixes it
    /// with an authority relation has no canonical form and returns `None`.
    pub(crate) fn from_relations(relations: impl IntoIterator<Item = Relation>) -> Option<Self> {
        let requested = relations.into_iter().collect::<Vec<_>>();
        for exclusive in [Relation::ResolvesTo, Relation::FormerRegistrant] {
            if requested.contains(&exclusive) {
                return requested
                    .iter()
                    .all(|relation| *relation == exclusive)
                    .then(|| Self {
                        relations: vec![exclusive],
                    });
            }
        }
        let mut normalized = Vec::new();
        for candidate in Relation::ALL {
            if requested.contains(&candidate) && !normalized.contains(&candidate) {
                normalized.push(candidate);
            }
        }
        (!normalized.is_empty()).then_some(Self {
            relations: normalized,
        })
    }

    /// The reverse lookup form of `any`: the three relations reverse lookup serves.
    pub(crate) fn lookup_all() -> Self {
        Self {
            relations: Relation::LOOKUP.to_vec(),
        }
    }

    pub(crate) fn as_slice(&self) -> &[Relation] {
        &self.relations
    }

    pub(crate) fn canonical_value(&self) -> String {
        self.relations
            .iter()
            .map(|relation| relation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn is_all(&self) -> bool {
        self.relations == Relation::ALL
    }

    pub(crate) fn is_exact_manager(&self) -> bool {
        self.relations == [Relation::Manager]
    }

    pub(crate) fn is_resolves_to(&self) -> bool {
        self.relations == [Relation::ResolvesTo]
    }

    pub(crate) fn is_former_registrant(&self) -> bool {
        self.relations == [Relation::FormerRegistrant]
    }

    pub(crate) fn is_exact_owner_and_registrant(&self) -> bool {
        self.relations == [Relation::Owner, Relation::Registrant]
    }
}

impl From<Relation> for RelationSet {
    fn from(value: Relation) -> Self {
        Self {
            relations: vec![value],
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AddressNamesDedupe {
    Name,
    Registration,
}

impl AddressNamesDedupe {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Registration => "registration",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Resolver {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
}

pub(crate) const PRODUCT_PIPELINE_TERMS: &[&str] = &[
    "projection",
    "sidecar",
    "manifest_version",
    "manifest",
    "normalized_event",
    "normalized event",
    "permission_row",
    "raw_log",
    "raw_fact",
    "raw fact",
    "coverage",
    "resource_authority",
    "resource_rebound",
    "derivation_kind",
    "exhaustiveness",
    "enumeration_basis",
    "source_classes_considered",
    "address_names_current",
    "address_records_current",
    "chain_header_audit",
    "chain_lineage",
    "children_current",
    "contract_instance_addresses",
    "contract_instances",
    "discovery_edges",
    "label_preimages",
    "manifest_contract_instances",
    "manifest_discovery_rules",
    "manifest_versions",
    "name_current",
    "name_surfaces",
    "normalized_events",
    "permission_current",
    "permissions_current",
    "primary_names_current",
    "raw_logs",
    "raw_receipts",
    "raw_transactions",
    "record_inventory_current",
    "resolver_current",
    "resources",
    "surface_bindings",
    "token_lineages",
];

pub(crate) fn contains_boundary_vocabulary(candidate: &str, terms: &[&str]) -> bool {
    !matched_boundary_vocabulary_terms(candidate, terms).is_empty()
}

const SHARED_PRODUCT_REASON_MAP: &[(&str, &str)] = &[
    ("projection_read_failed", "read_failed"),
    (
        "mixed_ensv1_ensv2_exact_name_corpus",
        "mixed_exact_name_corpus",
    ),
];

pub(crate) fn shared_product_reason(
    reason: &str,
    pipeline_rejection_log: &'static str,
    pipeline_rejection_error: &'static str,
) -> V2Result<String> {
    if let Some((_, product_reason)) = SHARED_PRODUCT_REASON_MAP
        .iter()
        .find(|(storage_reason, _)| *storage_reason == reason)
    {
        return Ok((*product_reason).to_owned());
    }

    if contains_boundary_vocabulary(reason, PRODUCT_PIPELINE_TERMS) {
        tracing::error!(%reason, "{}", pipeline_rejection_log);
        return Err(V2Error::internal_error(pipeline_rejection_error));
    }

    Ok(reason.to_owned())
}

pub(crate) fn projected_row_product_reason(
    reason: &str,
    pipeline_rejection_log: &'static str,
    pipeline_rejection_error: &'static str,
) -> String {
    shared_product_reason(reason, pipeline_rejection_log, pipeline_rejection_error)
        .unwrap_or_else(|_| "unsupported_reason_unrecognized".to_owned())
}

pub(crate) fn matched_boundary_vocabulary_terms<'a>(
    candidate: &str,
    terms: &'a [&'a str],
) -> Vec<&'a str> {
    let normalized_candidate = normalize_pipeline_candidate(candidate);
    terms
        .iter()
        .copied()
        .filter(|term| pipeline_term_matches(&normalized_candidate, term))
        .collect()
}

fn pipeline_term_matches(normalized_candidate: &str, term: &str) -> bool {
    let normalized_term = normalize_pipeline_candidate(term);
    pipeline_term_variants(&normalized_term)
        .iter()
        .any(|variant| candidate_has_underscore_boundary_term(normalized_candidate, variant))
}

fn normalize_pipeline_candidate(candidate: &str) -> String {
    candidate
        .chars()
        .map(|ch| match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            '-' | ' ' => '_',
            _ => ch,
        })
        .collect()
}

fn pipeline_term_variants(term: &str) -> Vec<String> {
    let mut variants = vec![term.to_owned(), format!("{term}s"), format!("{term}es")];
    if let Some(singular) = term.strip_suffix('s') {
        variants.push(singular.to_owned());
    }
    variants.sort_unstable();
    variants.dedup();
    variants
}

fn candidate_has_underscore_boundary_term(candidate: &str, term: &str) -> bool {
    candidate
        .match_indices(term)
        .any(|(start, _)| term_match_has_underscore_boundaries(candidate, term, start))
}

fn term_match_has_underscore_boundaries(candidate: &str, term: &str, start: usize) -> bool {
    let before_is_boundary = start == 0 || candidate.as_bytes()[start - 1] == b'_';
    if !before_is_boundary {
        return false;
    }

    let end = start + term.len();
    if end == candidate.len() || candidate.as_bytes()[end] == b'_' {
        return true;
    }

    candidate.as_bytes()[end] == b's'
        && (end + 1 == candidate.len() || candidate.as_bytes()[end + 1] == b'_')
}

#[cfg(test)]
mod tests;
