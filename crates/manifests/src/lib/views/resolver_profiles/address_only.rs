use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{ResolverProfileAdmission, normalize_address};

use super::super::types::ManifestCodeHashObservation;
use super::{
    ResolverProfileAdmissionConfig, classify_resolver_profile_match,
    latest_resolver_code_hashes_by_contract_id,
};

pub(super) async fn load_resolver_pointer_targets(
    pool: &PgPool,
    registry_source_family: &str,
) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            chain_id AS chain,
            LOWER(after_state->>'resolver') AS address
        FROM normalized_events
        WHERE event_kind = 'ResolverChanged'
          AND source_family = $1
          AND chain_id IS NOT NULL
          AND canonicality_state <> 'orphaned'
          AND LOWER(after_state->>'resolver') ~ '^0x[0-9a-f]{40}$'
          AND LOWER(after_state->>'resolver') <>
              '0x0000000000000000000000000000000000000000'
        ORDER BY chain, address
        "#,
    )
    .bind(registry_source_family)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load resolver-pointer profile targets for {registry_source_family}")
    })?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("address")
                .context("failed to read resolver-pointer address")?;
            Ok((
                row.try_get("chain")
                    .context("failed to read resolver-pointer chain")?,
                normalize_address(&address),
            ))
        })
        .collect()
}

pub(super) async fn load_latest_code_hashes_for_addresses(
    pool: &PgPool,
    targets: &[(String, String)],
) -> Result<BTreeMap<(String, String), String>> {
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }

    let targets = targets
        .iter()
        .map(|(chain, address)| (chain.clone(), normalize_address(address)))
        .collect::<BTreeSet<_>>();
    let chains = targets
        .iter()
        .map(|(chain, _)| chain.clone())
        .collect::<Vec<_>>();
    let addresses = targets
        .iter()
        .map(|(_, address)| address.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        WITH target_addresses AS (
            SELECT DISTINCT chain, address
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS target(chain, address)
        )
        SELECT
            target.chain,
            target.address,
            latest.code_hash
        FROM target_addresses target
        JOIN LATERAL (
            SELECT code_hash
            FROM raw_code_hashes
            WHERE chain_id = target.chain
              AND contract_address = target.address
              AND canonicality_state <> 'orphaned'
            ORDER BY
                block_number DESC,
                CASE canonicality_state
                    WHEN 'finalized' THEN 4
                    WHEN 'safe' THEN 3
                    WHEN 'canonical' THEN 2
                    WHEN 'observed' THEN 1
                    ELSE 0
                END DESC,
                raw_code_hash_id DESC
            LIMIT 1
        ) latest ON TRUE
        ORDER BY target.chain, target.address
        "#,
    )
    .bind(&chains)
    .bind(&addresses)
    .fetch_all(pool)
    .await
    .context("failed to load address-scoped resolver code hashes")?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("address")
                .context("failed to read address-scoped resolver address")?;
            Ok((
                (
                    row.try_get("chain")
                        .context("failed to read address-scoped resolver chain")?,
                    normalize_address(&address),
                ),
                row.try_get("code_hash")
                    .context("failed to read address-scoped resolver code hash")?,
            ))
        })
        .collect()
}

pub(super) async fn append_code_hash_profile_admissions(
    pool: &PgPool,
    targets: &[(String, String)],
    code_hash_observations: &[ManifestCodeHashObservation],
    resolver_seed_ids: &[Uuid],
    config: ResolverProfileAdmissionConfig,
    admissions: &mut Vec<ResolverProfileAdmission>,
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }

    let resolver_seed_ids = resolver_seed_ids.iter().copied().collect::<BTreeSet<_>>();
    let observed_code_hashes =
        latest_resolver_code_hashes_by_contract_id(code_hash_observations, config.source_family);
    let seed_code_hashes = resolver_seed_ids
        .iter()
        .filter_map(|contract_instance_id| {
            observed_code_hashes
                .get(contract_instance_id)
                .map(|code_hash| (*contract_instance_id, code_hash.clone()))
        })
        .collect::<Vec<_>>();
    let target_code_hashes = load_latest_code_hashes_for_addresses(pool, targets).await?;

    for (chain, address) in targets {
        let address = normalize_address(address);
        let profile_match = classify_resolver_profile_match(
            None,
            &resolver_seed_ids,
            &seed_code_hashes,
            target_code_hashes.get(&(chain.clone(), address.clone())),
            config.manifest_seed_basis,
        );
        for fact_family in config.fact_families {
            admissions.push(ResolverProfileAdmission {
                chain: chain.clone(),
                source_family: config.source_family.to_owned(),
                contract_instance_id: None,
                address: address.clone(),
                source: None,
                source_manifest_id: None,
                active_from_block_number: None,
                active_to_block_number: None,
                profile: config.profile.to_owned(),
                fact_family: (*fact_family).to_owned(),
                status: profile_match.status.clone(),
                admission_basis: profile_match.admission_basis.clone(),
                observed_code_hash: profile_match.observed_code_hash.clone(),
                matched_code_hash: profile_match.matched_code_hash.clone(),
                matched_contract_instance_id: profile_match.matched_contract_instance_id,
            });
        }
    }

    Ok(())
}
