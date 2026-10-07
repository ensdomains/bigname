//! Reverse lookup pages/counts over exact stored current relations. Primary claims are
//! composed in bounded request-local batches on the same admitted family snapshot.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{PgConnection, PgPool};

use super::primary::load_family_primary_name_snapshots_on;
use crate::{
    IdentityPrimaryNameSnapshot, ReverseIdentityGroup, ReverseIdentityStorageInput,
    families::name::servable_publication,
};

/// Readable primary claims for the requested address and coin type, keyed by namespace.
pub async fn load_family_reverse_primary_snapshots(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    namespaces: &[String],
    chains: Option<&[String]>,
) -> Result<BTreeMap<String, IdentityPrimaryNameSnapshot>> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    ensure_publications(&mut snapshot, chains).await?;
    let out = primary_on(&mut snapshot, address, coin_type, namespaces, chains).await?;
    snapshot.commit().await?;
    Ok(out)
}

async fn primary_on(
    conn: &mut PgConnection,
    address: &str,
    coin_type: &str,
    namespaces: &[String],
    chains: Option<&[String]>,
) -> Result<BTreeMap<String, IdentityPrimaryNameSnapshot>> {
    let keys = namespaces
        .iter()
        .map(|ns| (ns.clone(), coin_type.to_owned()))
        .collect::<Vec<_>>();
    let claims = load_family_primary_name_snapshots_on(conn, address, &keys, chains).await?;
    let mut out = BTreeMap::new();
    for ((namespace, _), claim) in claims {
        let chain = claim.row.claim_provenance["chain_id"]
            .as_str()
            .context("family primary claim has no chain")?;
        let publication = servable_publication(conn, chain).await?;
        let slot = match chain {
            "ethereum-mainnet" => "ethereum",
            "base-mainnet" => "base",
            chain => chain,
        };
        out.insert(
            namespace.clone(),
            IdentityPrimaryNameSnapshot {
                address: claim.row.address,
                namespace,
                coin_type: claim.row.coin_type,
                claim_status: claim.row.claim_status,
                normalized_claim_name: claim.normalized_claim_name,
                chain_positions: Some(json!({slot: {
                    "chain_id": chain,
                    "block_number": publication.block_number,
                    "block_hash": publication.block_hash,
                    "timestamp": crate::time::format_timestamp(publication.block_timestamp),
                }})),
            },
        );
    }
    Ok(out)
}

/// The API reverse page/count contract. The count is independent of the cursor; page-only
/// callers stop after the first page and sentinel. Inventories are read only for returned rows.
pub async fn load_family_reverse_identity_groups(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    namespaces: &[String],
    chains: Option<&[String]>,
    include_count: bool,
    include_inventory: bool,
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    // Check before the walk: a reset can empty every candidate table.
    ensure_publications(&mut snapshot, chains).await?;
    let mut out = Vec::with_capacity(inputs.len());
    // Maps are retained only for a bounded input chunk. Duplicate tuples share family reads;
    // primary claims remain request-local and are never attached to persisted name state.
    for inputs in inputs.chunks(32) {
        let primary =
            super::primary::batch::load(&mut snapshot, inputs, namespaces, chains).await?;
        let mut counts = BTreeMap::new();
        for (input, claims) in inputs.iter().zip(primary) {
            let mut primary = BTreeMap::new();
            for ((namespace, _), claim) in claims {
                let chain = claim.row.claim_provenance["chain_id"]
                    .as_str()
                    .context("family primary claim has no chain")?;
                let publication = servable_publication(&mut snapshot, chain).await?;
                primary.insert(
                    namespace.clone(),
                    IdentityPrimaryNameSnapshot {
                        address: claim.row.address,
                        namespace,
                        coin_type: claim.row.coin_type,
                        claim_status: claim.row.claim_status,
                        normalized_claim_name: claim.normalized_claim_name,
                        chain_positions: Some(primary_positions(&publication)),
                    },
                );
            }
            let count_key = (input.address.to_ascii_lowercase(), input.roles);
            let read_count = include_count && !counts.contains_key(&count_key);
            let mut group = crate::families::lookup::reverse::group_on(
                &mut snapshot,
                input,
                namespaces,
                &primary,
                chains,
                read_count,
                include_inventory,
            )
            .await?;
            if let Some(count) = group.total_count {
                counts.insert(count_key.clone(), count);
            } else if include_count {
                group.total_count = counts.get(&count_key).copied();
            }
            out.push(group);
        }
    }
    snapshot.commit().await?;
    Ok(out)
}

fn primary_positions(publication: &crate::families::name::FamilyPublication) -> serde_json::Value {
    let chain = publication.chain_id.as_str();
    let slot = match chain {
        "ethereum-mainnet" => "ethereum",
        "base-mainnet" => "base",
        chain => chain,
    };
    json!({slot:{"chain_id":chain,"block_number":publication.block_number,
        "block_hash":publication.block_hash,"timestamp":crate::time::format_timestamp(publication.block_timestamp)}})
}

async fn ensure_publications(conn: &mut PgConnection, chains: Option<&[String]>) -> Result<()> {
    crate::families::lookup::ensure_publications(conn, chains).await?;
    Ok(())
}

#[cfg(test)]
#[path = "reverse_page_reference.rs"]
mod reference;
