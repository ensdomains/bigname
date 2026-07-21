use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use super::{
    PrimaryNameLegacyReverseHydrationConfig, PrimaryNameLegacyReverseHydrationTrigger,
    normalize_resolver_addresses,
};

pub(crate) async fn load_legacy_reverse_resolver_call_triggers(
    pool: &PgPool,
    config: &PrimaryNameLegacyReverseHydrationConfig,
) -> Result<Vec<PrimaryNameLegacyReverseHydrationTrigger>> {
    let resolver_addresses = normalize_resolver_addresses(&config.resolver_addresses);
    if resolver_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        WITH chain_positions AS (
            SELECT
                chain_id,
                canonical_block_number AS hydration_block_number,
                canonical_block_hash AS hydration_block_hash
            FROM chain_checkpoints
            WHERE chain_id = $2
              AND canonical_block_number IS NOT NULL
              AND canonical_block_hash IS NOT NULL
        )
        SELECT DISTINCT ON (LOWER(esc.resolver_address))
            LOWER(esc.resolver_address) AS resolver_address,
            esc.block_number,
            esc.block_hash,
            esc.transaction_hash,
            esc.transaction_index
        FROM event_silent_resolver_call_observations esc
        JOIN chain_positions
          ON chain_positions.chain_id = esc.chain_id
         AND esc.block_number <= chain_positions.hydration_block_number
        WHERE esc.chain_id = $2
          AND LOWER(esc.resolver_address) = ANY($1::TEXT[])
          AND esc.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            LOWER(esc.resolver_address) ASC,
            esc.block_number DESC,
            esc.transaction_index DESC,
            esc.transaction_hash DESC
        "#,
    )
    .bind(&resolver_addresses)
    .bind(bigname_storage::ETHEREUM_MAINNET_CHAIN_ID)
    .fetch_all(pool)
    .await
    .context("failed to load latest legacy reverse-resolver direct-call triggers")?;

    rows.into_iter()
        .map(|row| {
            Ok(PrimaryNameLegacyReverseHydrationTrigger {
                resolver_address: row
                    .try_get("resolver_address")
                    .context("missing legacy reverse hydration trigger resolver_address")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing legacy reverse hydration trigger block_number")?,
                block_hash: row
                    .try_get("block_hash")
                    .context("missing legacy reverse hydration trigger block_hash")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("missing legacy reverse hydration trigger transaction_hash")?,
                transaction_index: row
                    .try_get("transaction_index")
                    .context("missing legacy reverse hydration trigger transaction_index")?,
            })
        })
        .collect()
}
