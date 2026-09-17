//! Explicit same-node transport changes preserve the retained ingest extent.
use anyhow::{Result, ensure};
use bigname_ingest::{VerificationProvider, WatchFilter, load_persisted_watch_filter};
use serde_json::{Value, json};

use crate::{
    config::{SeedBasis, SourceConfig, SourceRole, normalized_source_kind},
    database::RunnerDatabase,
    phase::PhaseName,
};

/// The operator attests these endpoints expose the same node. This operation checks
/// retained canonical boundaries and the next block's watched logs; it does not grant
/// independent verification or replace the node's historical coverage.
pub async fn transition(
    database: &RunnerDatabase,
    old: &SourceConfig,
    new: &SourceConfig,
) -> Result<Value> {
    transition_with_readers(database, old, new, |source| {
        let kind = normalized_source_kind(&source.source_kind);
        Ok(VerificationProvider::new(
            &source.chain_id,
            &kind,
            source.endpoint(),
        )?)
    })
    .await
}

/// [`transition`] with the caller opening the reader for each descriptor. Tests use it to
/// stand an HTTP node double in for the direct database reader, which otherwise needs a real
/// Reth datadir; every check and the cursor update are the production ones.
#[doc(hidden)]
pub async fn transition_with_readers(
    database: &RunnerDatabase,
    old: &SourceConfig,
    new: &SourceConfig,
    open_reader: impl Fn(&SourceConfig) -> Result<VerificationProvider>,
) -> Result<Value> {
    validate_pair(old, new)?;
    let chain = &old.chain_id;
    let from_kind = normalized_source_kind(&old.source_kind);
    let to_kind = normalized_source_kind(&new.source_kind);
    let mut tx = database.pool().begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *tx)
        .await?;
    for phase in PhaseName::ALL {
        let locked: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0::bigint))",
        )
        .bind(crate::phase_lock::lock_name(chain, phase))
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            locked,
            "stop all phase writers before changing source transport"
        );
    }
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(c) FROM ingest_cursors c WHERE chain_id = $1 FOR UPDATE",
    )
    .bind(chain)
    .fetch_all(&mut *tx)
    .await?;
    ensure!(
        rows.len() == 1,
        "transport change requires exactly one retained intake cursor"
    );
    let cursor = &rows[0];
    ensure!(
        cursor["source_key"].as_str() == Some(&old.source_key),
        "source key differs"
    );
    ensure!(
        cursor["source_kind"].as_str() == Some(&from_kind),
        "stored source kind differs"
    );
    ensure!(
        cursor["seed_basis"] == "ethereum_head",
        "stored seed basis differs"
    );
    ensure!(
        cursor["start_block_number"].as_i64() == Some(old.start_block_number),
        "stored start differs"
    );
    let phase: Value = sqlx::query_scalar(
        "SELECT to_jsonb(p) FROM chain_phase_state p WHERE chain_id = $1 AND phase_name = 'ingest' FOR UPDATE",
    )
    .bind(chain)
    .fetch_one(&mut *tx)
    .await?;
    let old_provider = open_reader(old)?;
    let new_provider = open_reader(new)?;
    let mut checked = Vec::new();
    for (row, number, hash) in [
        (
            cursor,
            "last_processed_block_number",
            "last_processed_block_hash",
        ),
        (&phase, "current_block_number", "current_block_hash"),
        (
            &phase,
            "redo_current_block_number",
            "redo_current_block_hash",
        ),
    ] {
        if let (Some(number), Some(hash)) = (row[number].as_i64(), row[hash].as_str()) {
            let left = old_provider
                .fetch(WatchFilter::default(), number, number)
                .await?;
            let right = new_provider
                .fetch(WatchFilter::default(), number, number)
                .await?;
            ensure!(
                left.end == right.end && right.end.hash == hash,
                "retained boundary differs at block {number}"
            );
            checked.push(json!({"block":number,"hash":hash}));
        }
    }
    ensure!(
        !checked.is_empty(),
        "transport change requires a retained boundary"
    );
    let next = if phase["redo_in_progress"] == true {
        phase["redo_current_block_number"]
            .as_i64()
            .map(|n| n + 1)
            .or(phase["redo_from_block_number"].as_i64())
    } else {
        cursor["next_block_number"].as_i64()
    }
    .ok_or_else(|| anyhow::anyhow!("missing next ingest block"))?;
    let filter = load_persisted_watch_filter(database.pool(), chain, next, next).await?;
    let left = old_provider.fetch(filter.clone(), next, next).await?;
    let right = new_provider.fetch(filter, next, next).await?;
    ensure!(
        left.end == right.end && left.logs == right.logs,
        "next block data differs"
    );
    let affected = sqlx::query(
        "UPDATE ingest_cursors SET source_kind = $3 WHERE chain_id = $1 AND source_key = $2 AND source_kind = $4",
    )
    .bind(chain).bind(&old.source_key).bind(&to_kind).bind(&from_kind)
    .execute(&mut *tx).await?.rows_affected();
    ensure!(affected == 1, "source cursor changed during transition");
    // Source changes cannot upgrade or clear any verification/redo state.
    tx.commit().await?;
    Ok(
        json!({"chain":chain,"source_key":old.source_key,"from_kind":from_kind,
        "to_kind":to_kind,"previous_cursor":cursor,"ingest_phase":phase,
        "checked_boundaries":checked,"next_block":next,"next_block_hash":right.end.hash,
        "next_block_log_count":right.logs.len(),"same_node_attested":true}),
    )
}

fn validate_pair(old: &SourceConfig, new: &SourceConfig) -> Result<()> {
    ensure!(
        old.chain_id == "ethereum-sepolia" && new.chain_id == old.chain_id,
        "transport change supports Sepolia only"
    );
    ensure!(
        old.source_key == new.source_key
            && old.seed_basis == new.seed_basis
            && old.seed_basis == SeedBasis::EthereumHead
            && old.start_block_number == new.start_block_number
            && old.start_block_number == 0
            && old.role == SourceRole::Intake
            && new.role == SourceRole::Intake,
        "transport change must preserve key, seed, start, and intake-only role"
    );
    let kinds = (
        normalized_source_kind(&old.source_kind),
        normalized_source_kind(&new.source_kind),
    );
    ensure!(
        matches!(
            (kinds.0.as_str(), kinds.1.as_str()),
            ("drpc", "reth_db") | ("reth_db", "drpc")
        ),
        "only same-node drpc/reth_db transport changes are supported"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn source(kind: &str) -> SourceConfig {
        SourceConfig::new_with_role(
            "ethereum-sepolia",
            "node",
            kind,
            SeedBasis::EthereumHead,
            0,
            SourceRole::Intake,
            "/fixture",
        )
        .unwrap()
    }
    #[test]
    fn same_node_transport_preserves_identity_and_supports_rollback() {
        let old = source("drpc");
        let new = source("reth_db");
        assert!(validate_pair(&old, &new).is_ok());
        assert!(validate_pair(&new, &old).is_ok());
        assert!(validate_pair(&old, &old).is_err());
        let mut changed = new.clone();
        changed.source_key = "replacement".into();
        assert!(validate_pair(&old, &changed).is_err());
        let mut changed = new.clone();
        changed.start_block_number = 1;
        assert!(validate_pair(&old, &changed).is_err());
        let mut changed = new.clone();
        changed.role = SourceRole::Both;
        assert!(validate_pair(&old, &changed).is_err());
        let mut changed = new;
        changed.chain_id = "ethereum-mainnet".into();
        assert!(validate_pair(&old, &changed).is_err());
    }
}
