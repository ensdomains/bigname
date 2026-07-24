use anyhow::{Context, Result, bail};
use bigname_adapters::{EnsV1SubregistryDiscoverySyncSummary, EnsV1UnwrappedAuthoritySyncSummary};
use serde_json::json;
use sqlx::{Row, postgres::PgPoolOptions};
use std::collections::BTreeSet;

const SOURCE_FAMILIES: [&str; 4] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_resolver_l1",
    "ens_v1_wrapper_l1",
];

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let chain = args
        .next()
        .context("usage: sync_ens_v1_unwrapped_authority_block_hashes <chain> <block_hash|block_hash:transaction_hash>...")?;
    let selection = SelectedReplay::parse(args.collect::<Vec<_>>())?;
    if selection.block_hashes.is_empty() {
        bail!("at least one block hash is required");
    }

    let database_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("BIGNAME_DATABASE_URL"))
        .context("DATABASE_URL or BIGNAME_DATABASE_URL must be set")?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(bigname_storage::stamp_projection_replay_version(
            database_url.parse()?,
        ))
        .await
        .context("failed to connect to database")?;

    let source_scope = source_scope_for_selected_blocks(
        &pool,
        &chain,
        &selection.block_hashes,
        selection.transaction_hashes.as_deref(),
    )
    .await?;
    let subregistry_summary =
        EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
            &pool,
            &chain,
            &selection.block_hashes,
            &source_scope,
        )
        .await?;
    let authority_summary = if let Some(transaction_hashes) =
        selection.transaction_hashes.as_deref()
    {
        EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope_and_transactions(
            &pool,
            &chain,
            &selection.block_hashes,
            &source_scope,
            transaction_hashes,
        )
        .await?
    } else {
        EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope(
            &pool,
            &chain,
            &selection.block_hashes,
            &source_scope,
        )
        .await?
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "subregistry_discovery": {
                "scanned_log_count": subregistry_summary.scanned_log_count,
                "matched_log_count": subregistry_summary.matched_log_count,
                "active_observation_count": subregistry_summary.active_observation_count,
                "active_edge_count": subregistry_summary.active_edge_count,
                "admitted_edge_count": subregistry_summary.admitted_edge_count,
                "inserted_edge_count": subregistry_summary.inserted_edge_count,
                "deactivated_edge_count": subregistry_summary.deactivated_edge_count,
                "total_normalized_event_count": subregistry_summary.total_normalized_event_count,
                "total_normalized_event_inserted_count": subregistry_summary.total_normalized_event_inserted_count,
            },
            "unwrapped_authority": {
                "scanned_log_count": authority_summary.scanned_log_count,
                "matched_log_count": authority_summary.matched_log_count,
                "total_name_surface_count": authority_summary.total_name_surface_count,
                "total_resource_count": authority_summary.total_resource_count,
                "total_surface_binding_count": authority_summary.total_surface_binding_count,
                "total_normalized_event_count": authority_summary.total_normalized_event_count,
                "total_normalized_event_inserted_count": authority_summary.total_normalized_event_inserted_count,
                "by_kind": authority_summary.by_kind,
            },
            "block_hash_count": selection.block_hashes.len(),
            "transaction_hash_count": selection.transaction_hashes.as_ref().map_or(0, Vec::len),
            "source_scope_target_count": source_scope.len(),
        }))?
    );

    Ok(())
}

struct SelectedReplay {
    block_hashes: Vec<String>,
    transaction_hashes: Option<Vec<String>>,
}

impl SelectedReplay {
    fn parse(args: Vec<String>) -> Result<Self> {
        let has_transaction_pairs = args.iter().any(|arg| arg.contains(':'));
        if has_transaction_pairs && args.iter().any(|arg| !arg.contains(':')) {
            bail!(
                "use either block hashes only or block_hash:transaction_hash pairs only; mixed selections are ambiguous"
            );
        }

        if has_transaction_pairs {
            let mut block_hashes = BTreeSet::new();
            let mut transaction_hashes = BTreeSet::new();
            for arg in args {
                let (block_hash, transaction_hash) = arg.split_once(':').context(
                    "transaction-filtered selections must be block_hash:transaction_hash pairs",
                )?;
                block_hashes.insert(block_hash.to_owned());
                transaction_hashes.insert(transaction_hash.to_owned());
            }
            Ok(Self {
                block_hashes: block_hashes.into_iter().collect(),
                transaction_hashes: Some(transaction_hashes.into_iter().collect()),
            })
        } else {
            Ok(Self {
                block_hashes: args,
                transaction_hashes: None,
            })
        }
    }
}

async fn source_scope_for_selected_blocks(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    transaction_hashes: Option<&[String]>,
) -> Result<Vec<(String, String, i64, i64)>> {
    let restrict_to_transaction_hashes = transaction_hashes.is_some();
    let transaction_hashes = transaction_hashes.unwrap_or(&[]);
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT lower(emitting_address) AS address, block_number
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR transaction_hash = ANY($4::TEXT[]))
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number, address
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(restrict_to_transaction_hashes)
    .bind(transaction_hashes)
    .fetch_all(pool)
    .await
    .context("failed to load selected-block raw log emitters")?;

    let mut source_scope = Vec::new();
    for row in rows {
        let address: String = row.try_get("address")?;
        let block_number: i64 = row.try_get("block_number")?;
        for source_family in SOURCE_FAMILIES {
            source_scope.push((
                source_family.to_owned(),
                address.clone(),
                block_number,
                block_number,
            ));
        }
    }
    Ok(source_scope)
}
