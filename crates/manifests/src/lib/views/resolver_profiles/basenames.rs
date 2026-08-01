use std::collections::BTreeSet;

use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{ResolverProfileAdmission, WatchedContract, normalize_address};

use super::super::{
    drift::{
        load_manifest_code_hash_observations,
        load_manifest_code_hash_observations_for_watched_contracts,
    },
    types::ManifestCodeHashObservation,
    watched::load_watched_contracts_by_source_family,
};
use super::{
    ResolverProfileAdmissionConfig, address_only, derive_code_hash_resolver_profile_admissions,
    load_resolver_profile_seed_ids, load_resolver_profile_seed_watched_contracts,
    load_resolver_profile_target_watched_contracts, sort_resolver_profile_admissions,
};

const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const BASENAMES_L2_RESOLVER_ROLE: &str = "resolver";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const BASENAMES_L2_RESOLVER_PROFILE_FACT_FAMILIES: [&str; 2] =
    ["resolver_record", "resolver_authorization"];
const RESOLVER_PROFILE_BASIS_BASENAMES_L2_RESOLVER_SEED: &str = "manifest_l2_resolver_seed";

pub async fn load_basenames_l2_resolver_profile_admissions(
    pool: &PgPool,
) -> Result<Vec<ResolverProfileAdmission>> {
    let l2_resolver_seed_ids = load_resolver_profile_seed_ids(
        pool,
        "basenames",
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        BASENAMES_L2_RESOLVER_ROLE,
        "Basenames L2Resolver",
    )
    .await?;
    let watched_contracts =
        load_watched_contracts_by_source_family(pool, BASENAMES_BASE_RESOLVER_SOURCE_FAMILY)
            .await?;
    let code_hash_observations = load_manifest_code_hash_observations(pool).await?;

    let mut admissions = derive_basenames_l2_resolver_profile_admissions(
        &watched_contracts,
        &code_hash_observations,
        &l2_resolver_seed_ids,
    );
    let watched_targets = watched_contracts
        .iter()
        .map(|contract| (contract.chain.clone(), normalize_address(&contract.address)))
        .collect::<BTreeSet<_>>();
    let address_only_targets =
        address_only::load_resolver_pointer_targets(pool, "basenames_base_registry")
            .await?
            .into_iter()
            .filter(|target| !watched_targets.contains(target))
            .collect::<Vec<_>>();
    address_only::append_code_hash_profile_admissions(
        pool,
        &address_only_targets,
        &code_hash_observations,
        &l2_resolver_seed_ids,
        admission_config(),
        &mut admissions,
    )
    .await?;
    sort_resolver_profile_admissions(&mut admissions);
    Ok(admissions)
}

pub async fn load_basenames_l2_resolver_profile_admissions_for_targets(
    pool: &PgPool,
    targets: &[(String, String)],
) -> Result<Vec<ResolverProfileAdmission>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let l2_resolver_seed_contracts = load_resolver_profile_seed_watched_contracts(
        pool,
        "basenames",
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        BASENAMES_L2_RESOLVER_ROLE,
        "Basenames L2Resolver",
    )
    .await?;
    let l2_resolver_seed_ids = l2_resolver_seed_contracts
        .iter()
        .map(|contract| contract.contract_instance_id)
        .collect::<Vec<_>>();
    let target_contracts = load_resolver_profile_target_watched_contracts(
        pool,
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        targets,
    )
    .await?;
    let mut code_hash_targets = l2_resolver_seed_contracts.clone();
    code_hash_targets.extend(target_contracts.clone());
    let code_hash_observations =
        load_manifest_code_hash_observations_for_watched_contracts(pool, &code_hash_targets)
            .await?;

    let mut admissions = derive_code_hash_resolver_profile_admissions(
        &target_contracts,
        &code_hash_observations,
        &l2_resolver_seed_ids,
        admission_config(),
    );
    let watched_targets = target_contracts
        .iter()
        .map(|contract| (contract.chain.clone(), normalize_address(&contract.address)))
        .collect::<BTreeSet<_>>();
    let address_only_targets = targets
        .iter()
        .map(|(chain, address)| (chain.clone(), normalize_address(address)))
        .filter(|target| {
            target.1 != "0x0000000000000000000000000000000000000000"
                && !watched_targets.contains(target)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    address_only::append_code_hash_profile_admissions(
        pool,
        &address_only_targets,
        &code_hash_observations,
        &l2_resolver_seed_ids,
        admission_config(),
        &mut admissions,
    )
    .await?;
    sort_resolver_profile_admissions(&mut admissions);
    Ok(admissions)
}

pub fn derive_basenames_l2_resolver_profile_admissions(
    watched_contracts: &[WatchedContract],
    code_hash_observations: &[ManifestCodeHashObservation],
    l2_resolver_seed_ids: &[Uuid],
) -> Vec<ResolverProfileAdmission> {
    derive_code_hash_resolver_profile_admissions(
        watched_contracts,
        code_hash_observations,
        l2_resolver_seed_ids,
        admission_config(),
    )
}

fn admission_config() -> ResolverProfileAdmissionConfig {
    ResolverProfileAdmissionConfig {
        source_family: BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        profile: BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE,
        fact_families: &BASENAMES_L2_RESOLVER_PROFILE_FACT_FAMILIES,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_BASENAMES_L2_RESOLVER_SEED,
    }
}
