//! Primary-name claims over the families under the publication switch (TYR-36 step 7b, E7):
//! the reverse claim of each (address, namespace, coin type) tuple from F12 (`reverse.rs`) at
//! the family publication, overlaid with the tuple's hydration from the F12 hydration columns
//! (`project_reverse_tuple.hydrated_name`, `attempt_block`, `attempt_hash`), as the served read
//! overlays `primary_names_current` with its `canonical_head_multicall_hydration`
//! (primary_name/reads.rs): a hydration whose attempt block is no longer on canonical lineage is
//! not read, and the pre-hydration claim is served.
//!
//! The hydration columns hold the hydrated name only. A hydration that found no name and one that
//! failed both leave it null, and the served read tells them apart (a found-nothing hydration
//! serves `not_found`, a failed one the baseline), so a null hydrated name serves the baseline.
//! Nothing writes the columns yet (TYR-36 step 7a-2), so today every claim is the pre-hydration
//! claim.
//!
//! The served tuple has no chain; the family tuple has one. A namespace's tuples live on one
//! chain, which the read takes from the family rows.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Row};

use super::reverse::load_family_reverse_claim_on;
use crate::{
    PrimaryNameClaimStatus, PrimaryNameCurrentSnapshot,
    families::name::{FamilyPublication, all_servable_publications, servable_publication},
};

const HYDRATION: &str = "canonical_head_multicall_hydration";

/// `load_primary_name_current_snapshot` over the families.
pub async fn load_family_primary_name_snapshot(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentSnapshot>> {
    let keys = [(namespace.to_owned(), coin_type.to_owned())];
    Ok(load_family_primary_name_snapshots(pool, address, &keys)
        .await?
        .remove(&keys[0]))
}

/// `load_primary_name_current_snapshots` over the families, read in one snapshot.
pub async fn load_family_primary_name_snapshots(
    pool: &PgPool,
    address: &str,
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), PrimaryNameCurrentSnapshot>> {
    let mut out = BTreeMap::new();
    if keys.is_empty() {
        return Ok(out);
    }
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let address = address.to_ascii_lowercase();
    let mut publications: BTreeMap<String, FamilyPublication> = BTreeMap::new();
    let mut checked_all = false;
    for (namespace, coin_type) in keys {
        let chains: Vec<String> = sqlx::query_scalar(
            "/* storage:families.records.primary_tuple_chains */
             SELECT chain_id FROM bigname_phase.project_reverse_tuple
             WHERE address = $1 AND namespace = $2 AND coin_type = $3
               AND reverse_position IS NOT NULL
             ORDER BY chain_id",
        )
        .bind(&address)
        .bind(namespace)
        .bind(coin_type)
        .fetch_all(&mut *snapshot)
        .await
        .context("failed to find the chain of a reverse tuple")?;
        let Some(chain_id) = chains.first() else {
            // No tuple: an answer only when every chain's families are published.
            if !checked_all {
                all_servable_publications(&mut snapshot).await?;
                checked_all = true;
            }
            continue;
        };
        if !publications.contains_key(chain_id) {
            let publication = servable_publication(&mut snapshot, chain_id).await?;
            publications.insert(chain_id.clone(), publication);
        }
        let publication = &publications[chain_id];
        let Some(claim) =
            load_family_reverse_claim_on(&mut snapshot, chain_id, &address, namespace, coin_type)
                .await?
        else {
            continue;
        };
        let mut claim = claim.snapshot;
        stamp(&mut claim, publication);
        hydrate(&mut snapshot, chain_id, &mut claim).await?;
        out.insert((namespace.clone(), coin_type.clone()), claim);
    }
    snapshot.commit().await?;
    Ok(out)
}

/// The publication target the served claim provenance carries (and its read filter checks).
fn stamp(claim: &mut PrimaryNameCurrentSnapshot, publication: &FamilyPublication) {
    if let Value::Object(provenance) = &mut claim.row.claim_provenance {
        provenance.insert(
            "target_block_number".into(),
            json!(publication.block_number),
        );
        provenance.insert("target_block_hash".into(), json!(publication.block_hash));
    }
}

/// Overlay the tuple's hydrated name when its attempt block is on canonical lineage.
async fn hydrate(
    conn: &mut PgConnection,
    chain_id: &str,
    claim: &mut PrimaryNameCurrentSnapshot,
) -> Result<()> {
    let row = sqlx::query(
        "/* storage:families.records.primary_hydration */
         SELECT tuple.hydrated_name, tuple.attempt_block, tuple.attempt_hash
         FROM bigname_phase.project_reverse_tuple tuple
         WHERE tuple.address = $1 AND tuple.namespace = $2 AND tuple.coin_type = $3
           AND tuple.chain_id = $4 AND tuple.hydrated_name IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM bigname_phase.chain_lineage lineage
               WHERE lineage.chain_id = tuple.chain_id
                 AND lineage.block_number = tuple.attempt_block
                 AND lineage.block_hash = tuple.attempt_hash
                 AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           )",
    )
    .bind(&claim.row.address)
    .bind(&claim.row.namespace)
    .bind(&claim.row.coin_type)
    .bind(chain_id)
    .fetch_optional(&mut *conn)
    .await
    .context("failed to load the reverse tuple hydration")?;
    let Some(row) = row else {
        return Ok(());
    };
    let name: String = row.try_get("hydrated_name")?;
    let block: i64 = row.try_get("attempt_block")?;
    let hash: String = row.try_get("attempt_hash")?;
    let baseline = json!({
        "claim_status": claim.row.claim_status.as_str(),
        "raw_claim_name": claim.row.raw_claim_name,
        "claim_name_is_normalized": claim.claim_name_is_normalized,
    });
    // The served classification of a hydrated name (crates/project/src/hydration/reverse.rs,
    // `classify_result`).
    let (status, raw, normalized) = if name.trim().is_empty() {
        (PrimaryNameClaimStatus::NotFound, None, false)
    } else {
        match bigname_domain::normalization::normalize_name(&name) {
            Ok(normalized) => {
                let exact = normalized.normalized_name.as_bytes() == name.as_bytes();
                (PrimaryNameClaimStatus::Success, Some(name), exact)
            }
            Err(_) => (PrimaryNameClaimStatus::InvalidName, Some(name), false),
        }
    };
    claim.row.claim_status = status;
    claim.row.raw_claim_name = raw;
    claim.claim_name_is_normalized = normalized;
    claim.normalized_claim_name =
        crate::normalized_claim_name(status, normalized, claim.row.raw_claim_name.as_deref());
    if let Value::Object(provenance) = &mut claim.row.claim_provenance {
        provenance.insert(
            HYDRATION.into(),
            json!({
                "chain_id": chain_id,
                "block_number": block,
                "block_hash": hash,
                "baseline": baseline,
            }),
        );
    }
    Ok(())
}
