//! Reverse lookup pages over the address index, with primary claims, role masks and counts
//! evaluated in one family snapshot. Candidate keys are sought in page order in batches of 64.
//! Exact counts visit the whole candidate set, retaining only a page and its overflow row.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use super::primary::load_family_primary_name_snapshots_on;
use crate::{
    AddressNameRelation, IdentityPrimaryNameSnapshot, ReverseIdentityCursor, ReverseIdentityGroup,
    ReverseIdentityRecordRow, ReverseIdentityRoles, ReverseIdentityStorageInput,
    families::name::{all_servable_publications, servable_publication},
    phase_projection_reads::family_identity,
};

const BATCH: i64 = 64;
type NameKey = (String, String, String);
type Candidate = (String, String, String, String);

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
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    // Check before the walk: a reset can empty every candidate table.
    ensure_publications(&mut snapshot, chains).await?;
    let mut out = Vec::with_capacity(inputs.len());
    let mut counts = BTreeMap::new();
    for input in inputs {
        let primary = primary_on(
            &mut snapshot,
            &input.address,
            &input.coin_type,
            namespaces,
            chains,
        )
        .await?;
        let count_key = (input.address.to_ascii_lowercase(), input.roles);
        let read_count = include_count && !counts.contains_key(&count_key);
        let mut group = group_on(
            &mut snapshot,
            input,
            namespaces,
            &primary,
            chains,
            read_count,
        )
        .await?;
        if let Some(count) = group.total_count {
            counts.insert(count_key.clone(), count);
        } else if include_count {
            group.total_count = counts.get(&count_key).copied();
        }
        let ids = group
            .entries
            .iter()
            .map(|entry| entry.name_record.row.logical_name_id.clone())
            .collect::<Vec<_>>();
        let mut detailed = family_identity::load_on(&mut snapshot, &ids, true)
            .await?
            .into_iter()
            .map(|record| (record.row.logical_name_id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        for entry in &mut group.entries {
            if let Some(record) = detailed.remove(&entry.name_record.row.logical_name_id) {
                entry.name_record = record;
            }
        }
        out.push(group);
    }
    snapshot.commit().await?;
    Ok(out)
}

async fn group_on(
    conn: &mut PgConnection,
    input: &ReverseIdentityStorageInput,
    namespaces: &[String],
    primary: &BTreeMap<String, IdentityPrimaryNameSnapshot>,
    chains: Option<&[String]>,
    include_count: bool,
) -> Result<ReverseIdentityGroup> {
    let limit = usize::try_from(input.page_size.max(0)).unwrap_or(usize::MAX - 1);
    let mut entries = Vec::new();
    let mut total = 0_u64;
    let primary_names: Value = primary
        .iter()
        .filter_map(|(namespace, claim)| {
            claim
                .normalized_claim_name
                .as_ref()
                .map(|name| (namespace.clone(), json!(name)))
        })
        .collect();
    // The two independent priorities precede the lexical name cursor. A candidate index can
    // contain superseded or masked relations; only the composed relation decides its bucket.
    for is_primary in [true, false] {
        for rank in [0_i16, 1] {
            if (input.roles == ReverseIdentityRoles::Owned && rank != 0)
                || (input.roles == ReverseIdentityRoles::Managed && rank != 1)
            {
                continue;
            }
            let bucket = (!is_primary, rank);
            if !include_count
                && input
                    .cursor
                    .as_ref()
                    .is_some_and(|c| bucket < (!c.is_primary, c.role_rank))
            {
                continue;
            }
            let mut after = input
                .cursor
                .as_ref()
                .filter(|c| !include_count && bucket == (!c.is_primary, c.role_rank))
                .map(|c| {
                    (
                        c.normalized_name.clone(),
                        c.namespace.clone(),
                        c.namehash.clone(),
                    )
                });
            loop {
                let batch = if include_count {
                    BATCH
                } else {
                    i64::try_from(limit.saturating_add(1).saturating_sub(entries.len()))
                        .unwrap_or(BATCH)
                        .min(BATCH)
                };
                let candidates = candidates_on(
                    conn,
                    input,
                    namespaces,
                    &primary_names,
                    is_primary,
                    rank,
                    after.as_ref(),
                    chains,
                    batch,
                )
                .await?;
                if candidates.is_empty() {
                    break;
                }
                let exhausted = candidates.len() < batch as usize;
                after = candidates
                    .last()
                    .map(|(_, name, ns, hash)| (name.clone(), ns.clone(), hash.clone()));
                let ids = candidates
                    .iter()
                    .map(|(id, ..)| id.clone())
                    .collect::<Vec<_>>();
                let mut records = family_identity::load_on(conn, &ids, false)
                    .await?
                    .into_iter()
                    .map(|record| (record.row.logical_name_id.clone(), record))
                    .collect::<BTreeMap<_, _>>();
                for (id, ..) in candidates {
                    let Some(record) = records.remove(&id) else {
                        continue;
                    };
                    if record.row.coverage["status"] == "unsupported" {
                        continue;
                    }
                    let mut facets = record
                        .relations
                        .iter()
                        .filter(|relation| {
                            relation.address.eq_ignore_ascii_case(&input.address)
                                && input.roles.includes(relation.relation)
                        })
                        .map(|relation| relation.relation)
                        .collect::<Vec<_>>();
                    facets.sort();
                    facets.dedup();
                    if facets.is_empty() {
                        continue;
                    }
                    let actual_rank = if facets.iter().any(|relation| {
                        matches!(
                            relation,
                            AddressNameRelation::Registrant | AddressNameRelation::TokenHolder
                        )
                    }) {
                        0
                    } else {
                        1
                    };
                    if actual_rank != rank {
                        continue;
                    }
                    total += 1;
                    let key = ReverseIdentityCursor {
                        is_primary,
                        role_rank: rank,
                        normalized_name: record.row.normalized_name.clone(),
                        namespace: record.row.namespace.clone(),
                        namehash: record.row.namehash.clone(),
                    };
                    if input
                        .cursor
                        .as_ref()
                        .is_some_and(|cursor| key_tuple(&key) <= key_tuple(cursor))
                        || entries.len() > limit
                    {
                        continue;
                    }
                    let claim = primary.get(&record.row.namespace).cloned();
                    entries.push(ReverseIdentityRecordRow {
                        name_record: record,
                        relation_facets: facets,
                        primary_chain_positions: claim
                            .as_ref()
                            .and_then(|c| c.chain_positions.clone()),
                        primary_name: claim,
                        requested_coin_type: input.coin_type.clone(),
                    });
                }
                if !include_count && entries.len() > limit {
                    break;
                }
                if exhausted {
                    break;
                }
            }
            if !include_count && entries.len() > limit {
                break;
            }
        }
        if !include_count && entries.len() > limit {
            break;
        }
    }
    let has_more = entries.len() > limit;
    entries.truncate(limit);
    Ok(ReverseIdentityGroup {
        input: input.clone(),
        entries,
        total_count: include_count.then_some(total),
        has_more,
    })
}

fn key_tuple(cursor: &ReverseIdentityCursor) -> (bool, i16, &str, &str, &str) {
    (
        !cursor.is_primary,
        cursor.role_rank,
        &cursor.normalized_name,
        &cursor.namespace,
        &cursor.namehash,
    )
}

#[allow(clippy::too_many_arguments)]
async fn candidates_on(
    conn: &mut PgConnection,
    input: &ReverseIdentityStorageInput,
    namespaces: &[String],
    primary: &Value,
    is_primary: bool,
    rank: i16,
    after: Option<&NameKey>,
    chains: Option<&[String]>,
    limit: i64,
) -> Result<Vec<Candidate>> {
    let relations = if rank == 0 {
        vec!["registrant", "token_holder"]
    } else {
        vec!["effective_controller"]
    };
    sqlx::query_as(
        "/* storage:families.records.reverse_candidates */
         SELECT DISTINCT surface.logical_name_id, surface.raw_name, surface.namespace, surface.namehash
         FROM bigname_phase.name_surfaces surface
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
         WHERE surface.visibility_state = 'active' AND surface.raw_name <> ''
           AND surface.block_number <= marker.current_block_number
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND surface.namespace = ANY($2)
           AND ($10::text[] IS NULL OR surface.chain_id = ANY($10))
           AND COALESCE(surface.raw_name = $3::jsonb ->> surface.namespace, false) = $4
           AND (surface.chain_id, surface.logical_name_id) IN (
               SELECT indexed.chain_id, indexed.logical_name_id
               FROM bigname_phase.project_address_name_index indexed
               WHERE indexed.address = lower($1) AND indexed.relation = ANY($5)
               UNION ALL
               -- A name the address manages through an ENSv2 registry role (address_roles.rs).
               SELECT candidate.chain_id, candidate.logical_name_id
               FROM bigname_phase.project_grant grant_row
               JOIN bigname_phase.project_binding_candidate candidate
                 ON candidate.chain_id = grant_row.chain_id
                AND candidate.resource_id = grant_row.resource_id
               WHERE $11::text[] IS NOT NULL
                 AND grant_row.subject = lower($1) AND grant_row.scope_kind = 'registry'
                 AND grant_row.effective_powers ?| $11::text[]
           )
           AND ($6::text IS NULL OR (surface.raw_name, surface.namespace, surface.namehash) > ($6, $7, $8))
         ORDER BY surface.raw_name, surface.namespace, surface.namehash, surface.logical_name_id
         LIMIT $9",
    )
    .bind(&input.address).bind(namespaces).bind(primary).bind(is_primary).bind(relations)
    .bind(after.map(|key| &key.0)).bind(after.map(|key| &key.1)).bind(after.map(|key| &key.2))
    .bind(limit).bind(chains)
    .bind((rank == 1).then_some(super::address_roles::MANAGEMENT_POWERS.as_slice()))
    .persistent(false).fetch_all(conn).await
    .context("failed to seek family reverse lookup candidates")
}

async fn ensure_publications(conn: &mut PgConnection, chains: Option<&[String]>) -> Result<()> {
    if let Some(chains) = chains {
        for chain in chains {
            servable_publication(conn, chain).await?;
        }
    } else {
        all_servable_publications(conn).await?;
    }
    Ok(())
}
