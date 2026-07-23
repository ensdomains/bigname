use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sqlx::Row;

use super::ReplayRawLogSelection;
use crate::reconciliation::types::{
    RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
};

#[path = "classification/closure_boundary.rs"]
mod closure_boundary;
#[path = "classification/contracts.rs"]
mod contracts;

pub(crate) use closure_boundary::LegacyRegistryNewlyRequiredCoverage;
use closure_boundary::{
    earliest_required_raw_fact_block, ensure_full_closure_retention_authority,
    ensure_legacy_registry_closure_retention_authority,
};
pub(crate) use contracts::NORMALIZED_EVENT_REPLAY_CONTRACTS;
#[cfg(test)]
pub(crate) use contracts::SOURCE_FAMILY_ENS_V2_REGISTRY_L1;
use contracts::{
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplayDependencyModel {
    StatelessRawFact,
    StatefulClosureRequired,
    ContextualDependencyRequired,
}

impl ReplayDependencyModel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StatelessRawFact => "stateless_raw_fact",
            Self::StatefulClosureRequired => "stateful_closure_required",
            Self::ContextualDependencyRequired => "contextual_dependency_required",
        }
    }

    const fn restricted_replay_supported(self) -> bool {
        matches!(self, Self::StatelessRawFact)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum NormalizedEventReplayAdapter {
    BlockDerivedNormalizedEvents,
    EnsV1ReverseClaim,
    EnsV1SubregistryDiscovery,
    EnsV1UnwrappedAuthority,
    EnsV2RegistryResourceSurface,
    EnsV2Registrar,
    EnsV2Resolver,
    EnsV2Permissions,
    ManifestNormalizedEvents,
}

impl NormalizedEventReplayAdapter {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BlockDerivedNormalizedEvents => "block_derived_normalized_events",
            Self::EnsV1ReverseClaim => "ens_v1_reverse_claim",
            Self::EnsV1SubregistryDiscovery => "ens_v1_subregistry_discovery",
            Self::EnsV1UnwrappedAuthority => "ens_v1_unwrapped_authority",
            Self::EnsV2RegistryResourceSurface => "ens_v2_registry_resource_surface",
            Self::EnsV2Registrar => "ens_v2_registrar",
            Self::EnsV2Resolver => "ens_v2_resolver",
            Self::EnsV2Permissions => "ens_v2_permissions",
            Self::ManifestNormalizedEvents => "manifest_normalized_events",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatelessReplayLane {
    WholeAdapter,
    NormalizedEventsOnly,
    Unsupported,
}

impl StatelessReplayLane {
    const fn supported(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdapterReplayContract {
    pub(crate) adapter: NormalizedEventReplayAdapter,
    pub(crate) model: ReplayDependencyModel,
    pub(crate) stateless_replay_lane: StatelessReplayLane,
    pub(crate) raw_fact_replay_participant: bool,
    pub(crate) source_families: &'static [&'static str],
    pub(crate) closure_source_families: &'static [&'static str],
    pub(crate) dependency_adapters: &'static [NormalizedEventReplayAdapter],
    pub(crate) producer_paths: &'static [&'static str],
    pub(crate) stateless_replay_proof_tests: &'static [&'static str],
    pub(crate) closure_replay_supported: bool,
    pub(crate) replay_note: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawFactReplayContractMode {
    StatelessRestricted,
    StatelessOnlyAuthoritative,
    FullClosure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawFactReplayContractPlan(RawFactReplayContractMode);

impl RawFactReplayContractPlan {
    pub(crate) const fn stateless_restricted() -> Self {
        Self(RawFactReplayContractMode::StatelessRestricted)
    }

    pub(crate) const fn full_closure() -> Self {
        Self(RawFactReplayContractMode::FullClosure)
    }

    pub(crate) const fn stateless_only_authoritative() -> Self {
        Self(RawFactReplayContractMode::StatelessOnlyAuthoritative)
    }

    pub(crate) fn permits_nonstateless_adapters(self) -> bool {
        matches!(self.0, RawFactReplayContractMode::FullClosure)
    }

    pub(crate) fn uses_stateless_replay_authority(self) -> bool {
        matches!(
            self.0,
            RawFactReplayContractMode::StatelessOnlyAuthoritative
        )
    }

    pub(crate) fn uses_restricted_sync_for(self, adapter: NormalizedEventReplayAdapter) -> bool {
        match self.0 {
            RawFactReplayContractMode::StatelessRestricted => true,
            RawFactReplayContractMode::StatelessOnlyAuthoritative => {
                replay_contract(adapter).stateless_replay_lane.supported()
            }
            RawFactReplayContractMode::FullClosure => !full_closure_reemits_adapter(adapter),
        }
    }

    pub(crate) fn ensure_adapter_allowed(
        self,
        adapter: NormalizedEventReplayAdapter,
    ) -> Result<()> {
        let contract = replay_contract(adapter);
        if !contract.raw_fact_replay_participant {
            bail!(
                "normalized-event replay adapter {} is not part of raw-fact replay",
                adapter.as_str()
            );
        }
        if self.uses_stateless_replay_authority() {
            if contract.stateless_replay_lane.supported() {
                return Ok(());
            }
            bail!(
                "normalized-event replay adapter {} has no centrally classified stateless replay lane",
                adapter.as_str()
            );
        }
        if contract.model.restricted_replay_supported() {
            return Ok(());
        }
        if self.permits_nonstateless_adapters() && contract.closure_replay_supported {
            return Ok(());
        }
        bail!(
            "normalized-event replay adapter {} is classified as {}; raw-fact restricted replay is disabled until a documented closure/dependency replay session is implemented",
            adapter.as_str(),
            contract.model.as_str()
        )
    }
}

pub(crate) async fn classify_raw_fact_replay_contract(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    raw_log_selection: &ReplayRawLogSelection,
    source_scope: &[(String, String, i64, i64)],
    closure_validation_source_scope: &[(String, String, i64, i64)],
) -> Result<RawFactReplayContractPlan> {
    let selected_contracts = selected_raw_fact_contracts(source_scope);
    let nonstateless_contracts = selected_contracts
        .iter()
        .copied()
        .map(replay_contract)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .collect::<Vec<_>>();
    if nonstateless_contracts.is_empty() {
        return Ok(RawFactReplayContractPlan::stateless_restricted());
    }

    let adapter_list = adapter_list(&nonstateless_contracts);
    let RawFactNormalizedEventReplaySelection::BlockRange {
        from_block,
        to_block,
    } = &request.selection
    else {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {adapter_list}; block-hash and source-scoped replay are disabled for these adapters"
        );
    };
    let from_block = *from_block;
    let Some((selected_from_block, _)) = raw_log_selection.range else {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {adapter_list} without a replay range"
        );
    };
    if selected_from_block != from_block {
        bail!(
            "normalized-event replay selected range {selected_from_block} but request starts at {from_block} for closure/context-dependent adapter(s) {adapter_list}"
        );
    }

    let nonstateless_adapters = nonstateless_contracts
        .iter()
        .map(|contract| contract.adapter)
        .collect::<Vec<_>>();
    ensure_full_closure_retention_authority_for_adapters(
        pool,
        &request.chain,
        &nonstateless_adapters,
        *to_block,
    )
    .await?;
    let closure_source_families = closure_source_families_for_contracts(&nonstateless_contracts);
    let closure_start_block = earliest_required_raw_fact_block(
        pool,
        &request.chain,
        closure_validation_source_scope,
        &closure_source_families,
    )
    .await?;
    if let Some(closure_start_block) = closure_start_block {
        if from_block > closure_start_block {
            bail!(
                "normalized-event replay for closure/context-dependent adapter(s) {adapter_list} must start at closure boundary block {closure_start_block}; requested block {from_block}"
            );
        }
    } else {
        let input_version =
            bigname_storage::load_raw_log_staging_input_version(pool, &request.chain).await?;
        if from_block != 0 || input_version.retention_generation != 0 {
            bail!(
                "normalized-event replay for closure/context-dependent adapter(s) {adapter_list} has no retained canonical raw fact boundary; explicit historical backfill/refetch or log-audit retention is required"
            );
        }
    }

    let unsupported_adapters = nonstateless_contracts
        .iter()
        .filter(|contract| !contract.closure_replay_supported)
        .map(|contract| contract.adapter.as_str())
        .collect::<Vec<_>>();
    if !unsupported_adapters.is_empty() {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {}; full closure replay is not implemented for these adapters",
            unsupported_adapters.join(", ")
        );
    }

    Ok(RawFactReplayContractPlan::full_closure())
}

pub(crate) async fn chain_has_closure_or_dependency_replay_adapter(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM manifest_versions
            WHERE chain = $1
              AND rollout_status = 'active'::manifest_rollout_status
              AND source_family = ANY($2::TEXT[])
        )
        "#,
    )
    .bind(chain)
    .bind(closure_or_dependency_source_families())
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to classify normalized-event replay adapters for chain {chain}")
    })
}

pub(crate) async fn active_closure_or_dependency_replay_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<Vec<NormalizedEventReplayAdapter>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT source_family
        FROM manifest_versions
        WHERE chain = $1
          AND rollout_status = 'active'::manifest_rollout_status
          AND source_family = ANY($2::TEXT[])
        ORDER BY source_family
        "#,
    )
    .bind(chain)
    .bind(full_closure_reemitted_source_families())
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to list active closure/dependency replay adapters for chain {chain}")
    })?;
    let active_source_families = rows
        .into_iter()
        .map(|row| row.get::<String, _>("source_family"))
        .collect::<BTreeSet<_>>();

    Ok(NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| full_closure_reemits_adapter(contract.adapter))
        .filter(|contract| {
            active_source_families
                .iter()
                .any(|source_family| source_family_in(source_family, contract.source_families))
        })
        .map(|contract| contract.adapter)
        .collect())
}

pub(crate) fn unsupported_closure_replay_adapters(
    adapters: &[NormalizedEventReplayAdapter],
) -> Vec<&'static str> {
    adapters
        .iter()
        .copied()
        .map(replay_contract)
        .filter(|contract| !contract.closure_replay_supported)
        .map(|contract| contract.adapter.as_str())
        .collect()
}

pub(crate) async fn ensure_full_closure_retention_authority_for_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    adapters: &[NormalizedEventReplayAdapter],
    through_block: i64,
) -> Result<()> {
    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface) {
        bigname_adapters::ensure_ens_v2_retained_history_proof_through(pool, chain, through_block)
            .await?;
    }
    let contracts = adapters
        .iter()
        .copied()
        .map(replay_contract)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .collect::<Vec<_>>();
    let closure_source_families = closure_source_families_for_contracts(&contracts);
    ensure_full_closure_retention_authority(pool, chain, &closure_source_families, through_block)
        .await
}

pub(crate) async fn ensure_legacy_registry_closure_retention_authority_for_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    adapters: &[NormalizedEventReplayAdapter],
    through_block: i64,
) -> Result<i64> {
    let contracts = adapters
        .iter()
        .copied()
        .map(replay_contract)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .collect::<Vec<_>>();
    let closure_source_families = closure_source_families_for_contracts(&contracts);
    ensure_legacy_registry_closure_retention_authority(
        pool,
        chain,
        &closure_source_families,
        through_block,
    )
    .await
}

pub(crate) fn replay_contract(
    adapter: NormalizedEventReplayAdapter,
) -> &'static AdapterReplayContract {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .find(|contract| contract.adapter == adapter)
        .expect("normalized-event replay adapter is missing a central replay contract")
}

pub(crate) fn source_scope_includes_adapter(
    source_scope: &[(String, String, i64, i64)],
    adapter: NormalizedEventReplayAdapter,
) -> bool {
    let contract = replay_contract(adapter);
    source_scope
        .iter()
        .any(|(source_family, _, _, _)| source_family_in(source_family, contract.source_families))
}

fn selected_raw_fact_contracts(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<NormalizedEventReplayAdapter> {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| {
            source_scope.iter().any(|(source_family, _, _, _)| {
                source_family_in(source_family, contract.source_families)
            })
        })
        .map(|contract| contract.adapter)
        .collect()
}

fn closure_source_families_for_contracts(
    contracts: &[&'static AdapterReplayContract],
) -> Vec<&'static str> {
    contracts
        .iter()
        .flat_map(|contract| contract.closure_source_families.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn closure_or_dependency_source_families() -> Vec<String> {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .flat_map(|contract| contract.source_families.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn full_closure_reemitted_source_families() -> Vec<String> {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| full_closure_reemits_adapter(contract.adapter))
        .flat_map(|contract| contract.source_families.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn full_closure_reemits_adapter(adapter: NormalizedEventReplayAdapter) -> bool {
    adapter == NormalizedEventReplayAdapter::EnsV1ReverseClaim
        || !replay_contract(adapter).model.restricted_replay_supported()
}

fn adapter_list(contracts: &[&'static AdapterReplayContract]) -> String {
    contracts
        .iter()
        .map(|contract| contract.adapter.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn source_family_in(source_family: &str, candidates: &[&str]) -> bool {
    candidates.contains(&source_family)
}

#[cfg(test)]
#[path = "classification/tests.rs"]
mod tests;
