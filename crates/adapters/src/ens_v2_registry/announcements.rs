use anyhow::{Context, Result};
use bigname_manifests::{
    DiscoveryObservation, WatchedContract, WatchedContractSource,
    load_manifest_declared_watched_contracts,
};
use bigname_storage::sql_row;
use serde_json::json;
use sqlx::PgPool;

use crate::adapter_manifest::{
    load_latest_active_manifest_metadata_for_source_family,
    load_required_active_manifest_event_topic0s_by_signature,
};

use super::{
    constants::{
        ABI_EVENT_REGISTRY_CREATED_SIGNATURE, GENERIC_SOURCE_SCOPE_ADDRESS,
        REGISTRY_ANNOUNCEMENT_EDGE_KIND, SOURCE_FAMILY_ENS_V2_REGISTRY_L1, ZERO_ADDRESS,
    },
    discovery::ens_v2_registry_announcement_discovery_source,
    load::RawLogCanonicalityFilter,
    types::RegistryRawLogSourceScopeTarget,
    util::normalize_address,
};

pub(super) async fn load_registry_announcement_observations(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
    canonicality_filter: RawLogCanonicalityFilter,
    max_block_number: Option<i64>,
) -> Result<Vec<DiscoveryObservation>> {
    let Some(manifest) = load_latest_active_manifest_metadata_for_source_family(
        pool,
        chain,
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        "ENSv2 RegistryCreated match-all manifest",
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let anchor = load_registry_announcement_anchor(pool, chain, manifest.manifest_id).await?;
    let ranges = announcement_ranges(source_scope, &anchor);
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &[manifest.manifest_id],
        &[ABI_EVENT_REGISTRY_CREATED_SIGNATURE],
        "ENSv2 RegistryCreated match-all",
    )
    .await?;
    let topic0 = event_topics.topic0(ABI_EVENT_REGISTRY_CREATED_SIGNATURE)?;
    let from_blocks = ranges.iter().map(|(from, _)| *from).collect::<Vec<_>>();
    let to_blocks = ranges.iter().map(|(_, to)| *to).collect::<Vec<_>>();
    let has_max_block_number = max_block_number.is_some();
    let max_block_number = max_block_number.unwrap_or(i64::MAX);
    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id,
            rl.block_hash,
            rl.block_number,
            rl.transaction_hash,
            rl.transaction_index,
            rl.log_index,
            rl.emitting_address,
            rl.topics,
            rl.data
        FROM raw_logs rl
        JOIN chain_lineage lineage
          ON lineage.chain_id = rl.chain_id
         AND lineage.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND lower(rl.topics[1]) = $2
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
          AND ($5::BOOLEAN = FALSE OR rl.block_number <= $6::BIGINT)
          AND EXISTS (
              SELECT 1
              FROM unnest($7::BIGINT[], $8::BIGINT[]) AS active_range(
                  effective_from_block,
                  effective_to_block
              )
              WHERE rl.block_number BETWEEN active_range.effective_from_block
                  AND active_range.effective_to_block
          )
          AND (
              ($9::BOOLEAN AND rl.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              ))
              OR (NOT $9::BOOLEAN AND rl.canonicality_state <> 'orphaned'::canonicality_state)
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index, lower(rl.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(topic0)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .bind(has_max_block_number)
    .bind(max_block_number)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(canonicality_filter.canonical_only())
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load RegistryCreated match-all logs for {chain}"))?;

    let mut observations = Vec::new();
    for row in rows {
        let emitting_address =
            normalize_address(&sql_row::get::<String>(&row, "emitting_address")?);
        if emitting_address == anchor.address || emitting_address == ZERO_ADDRESS {
            continue;
        }
        let topics = sql_row::get::<Vec<String>>(&row, "topics")?;
        let data = sql_row::get::<Vec<u8>>(&row, "data")?;
        if topics.len() != 1 || !data.is_empty() {
            continue;
        }
        let block_hash = sql_row::get::<String>(&row, "block_hash")?;
        let block_number = sql_row::get::<i64>(&row, "block_number")?;
        let transaction_hash = sql_row::get::<String>(&row, "transaction_hash")?;
        let transaction_index = sql_row::get::<i64>(&row, "transaction_index")?;
        let log_index = sql_row::get::<i64>(&row, "log_index")?;
        observations.push(DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: anchor.address.clone(),
            to_address: emitting_address.clone(),
            edge_kind: REGISTRY_ANNOUNCEMENT_EDGE_KIND.to_owned(),
            discovery_source: ens_v2_registry_announcement_discovery_source(chain),
            active_from_block_number: Some(block_number),
            active_from_block_hash: Some(block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "source": "raw_log",
                "source_event": "RegistryCreated",
                "observation_key": format!("registry-announcement:{emitting_address}"),
                "from_address": anchor.address,
                "to_address": emitting_address,
                "chain_id": chain,
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": transaction_hash,
                "transaction_index": transaction_index,
                "log_index": log_index,
                "tombstone": false,
            }),
        });
    }
    Ok(observations)
}

async fn load_registry_announcement_anchor(
    pool: &PgPool,
    chain: &str,
    manifest_id: i64,
) -> Result<WatchedContract> {
    load_manifest_declared_watched_contracts(pool)
        .await
        .context("failed to load the ENSv2 RegistryCreated announcement anchor")?
        .into_iter()
        .filter(|contract| {
            contract.chain == chain
                && contract.source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
                && contract.source_manifest_id == Some(manifest_id)
                && contract.source == WatchedContractSource::ManifestContract
        })
        .min_by_key(|contract| contract.active_from_block_number)
        .with_context(|| {
            format!(
                "active ENSv2 registry manifest {manifest_id} on {chain} has no declared registry contract"
            )
        })
}

fn announcement_ranges(
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
    anchor: &WatchedContract,
) -> Vec<(i64, i64)> {
    let anchor_from = anchor.active_from_block_number.unwrap_or(0);
    let anchor_to = anchor.active_to_block_number.unwrap_or(i64::MAX);
    let Some(source_scope) = source_scope else {
        return vec![(anchor_from, anchor_to)];
    };
    source_scope
        .iter()
        .filter(|target| {
            target.source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
                && target.address == GENERIC_SOURCE_SCOPE_ADDRESS
        })
        .filter_map(|target| {
            let from = target.effective_from_block.max(anchor_from);
            let to = target.effective_to_block.min(anchor_to);
            (from <= to).then_some((from, to))
        })
        .collect()
}
