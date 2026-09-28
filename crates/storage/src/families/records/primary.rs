//! Primary-name claims over the families under the publication switch (TYR-36 step 7b):
//! the reverse claim of each (address, namespace, coin type) tuple from F12 (`reverse.rs`) at
//! the family publication, overlaid with the tuple's hydration from the F12 hydration columns
//! (`project_reverse_tuple.hydrated_name`, `attempt_block`, `attempt_hash`), as the served read
//! overlays `primary_names_current` with its `canonical_head_multicall_hydration`
//! (primary_name/reads.rs): a hydration whose attempt block is no longer on canonical lineage is
//! not read, and the pre-hydration claim is served.
//!
//! Hydration is applied only while its prepared reverse node and resolver still match the
//! current claim and its attempt block remains canonical. Replay can preserve hydration
//! columns while changing a pointer, so attempt lineage alone does not establish that match.
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
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let out = load_family_primary_name_snapshots_on(&mut snapshot, address, keys, None).await?;
    snapshot.commit().await?;
    Ok(out)
}

/// The same tuple read on a caller's family snapshot, shared by reverse pagination.
pub(crate) async fn load_family_primary_name_snapshots_on(
    conn: &mut PgConnection,
    address: &str,
    keys: &[(String, String)],
    selected_chains: Option<&[String]>,
) -> Result<BTreeMap<(String, String), PrimaryNameCurrentSnapshot>> {
    let mut out = BTreeMap::new();
    if keys.is_empty() {
        return Ok(out);
    }
    let address = address.to_ascii_lowercase();
    let mut publications: BTreeMap<String, FamilyPublication> = BTreeMap::new();
    let mut checked_all = false;
    for (namespace, coin_type) in keys {
        let chains: Vec<String> = sqlx::query_scalar(
            "/* storage:families.records.primary_tuple_chains */
             SELECT chain_id FROM bigname_phase.project_reverse_tuple
             WHERE address = $1 AND namespace = $2 AND coin_type = $3
               AND reverse_position IS NOT NULL
               AND ($4::text[] IS NULL OR chain_id = ANY($4))
             ORDER BY chain_id",
        )
        .bind(&address)
        .bind(namespace)
        .bind(coin_type)
        .bind(selected_chains)
        .fetch_all(&mut *conn)
        .await
        .context("failed to find the chain of a reverse tuple")?;
        let Some(chain_id) = chains.first() else {
            // No tuple: an answer only when every chain's families are published.
            if selected_chains.is_none() && !checked_all {
                all_servable_publications(&mut *conn).await?;
                checked_all = true;
            }
            continue;
        };
        if !publications.contains_key(chain_id) {
            let publication = servable_publication(&mut *conn, chain_id).await?;
            publications.insert(chain_id.clone(), publication);
        }
        let publication = &publications[chain_id];
        let Some(claim) =
            load_family_reverse_claim_on(&mut *conn, chain_id, &address, namespace, coin_type)
                .await?
        else {
            continue;
        };
        let mut claim = claim.snapshot;
        stamp(&mut claim, publication);
        hydrate(&mut *conn, chain_id, &mut claim).await?;
        out.insert((namespace.clone(), coin_type.clone()), claim);
    }
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
         SELECT tuple.hydrated_name, tuple.attempt_block, tuple.attempt_hash, tuple.baseline
         FROM bigname_phase.project_reverse_tuple tuple
         WHERE tuple.address = $1 AND tuple.namespace = $2 AND tuple.coin_type = $3
           AND tuple.chain_id = $4 AND tuple.hydrated_name IS NOT NULL
           AND tuple.baseline IS NOT NULL
           AND (tuple.baseline ->> 'reverse_node') IS NOT DISTINCT FROM $5::text
           AND (tuple.baseline ->> 'resolver_address') IS NOT DISTINCT FROM $6::text
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
    .bind(
        claim
            .row
            .claim_provenance
            .get("reverse_node")
            .and_then(Value::as_str),
    )
    .bind(
        claim
            .row
            .claim_provenance
            .get("resolver_address")
            .and_then(Value::as_str),
    )
    .fetch_optional(&mut *conn)
    .await
    .context("failed to load the reverse tuple hydration")?;
    let Some(row) = row else {
        return Ok(());
    };
    let name: String = row.try_get("hydrated_name")?;
    let block: i64 = row.try_get("attempt_block")?;
    let hash: String = row.try_get("attempt_hash")?;
    let prepared_baseline: Value = row.try_get("baseline")?;
    let baseline = json!({
        "claim_status": claim.row.claim_status.as_str(),
        "raw_claim_name": claim.row.raw_claim_name,
        "claim_name_is_normalized": claim.claim_name_is_normalized,
        "unsupported_reason": prepared_baseline.get("unsupported_reason").cloned().unwrap_or(Value::Null),
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
                "source": "multicall_at_canonical_head",
                "chain_id": chain_id,
                "block_number": block,
                "block_hash": hash,
                "resolver_address": prepared_baseline.get("resolver_address"),
                "reverse_node": prepared_baseline.get("reverse_node"),
                "baseline": baseline,
            }),
        );
    }
    Ok(())
}
