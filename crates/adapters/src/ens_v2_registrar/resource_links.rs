use anyhow::{Context, Result};
use bigname_storage::sql_row;
use sqlx::{PgPool, types::Uuid};

use super::REGISTRY_DERIVATION_KIND;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ResourceLink {
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
}

pub(super) async fn load_registry_resource_link(
    pool: &PgPool,
    chain: &str,
    namespace: &str,
    logical_name_id: &str,
    token_id: &str,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
) -> Result<ResourceLink> {
    let row = sqlx::query(
        r#"
        SELECT logical_name_id, resource_id
        FROM normalized_events
        WHERE namespace = $1
          AND derivation_kind = $2
          AND chain_id = $3
          AND logical_name_id = $4
          AND event_kind IN ('TokenResourceLinked', 'TokenRegenerated')
          AND (
              after_state ->> 'token_id' = $5
              OR after_state ->> 'old_token_id' = $5
              OR after_state ->> 'new_token_id' = $5
          )
          AND (
              block_number,
              (raw_fact_ref ->> 'transaction_index')::BIGINT,
              log_index
          ) < ($6::BIGINT, $7::BIGINT, $8::BIGINT)
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            block_number DESC,
            (raw_fact_ref ->> 'transaction_index')::BIGINT DESC,
            log_index DESC,
            event_identity DESC
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(REGISTRY_DERIVATION_KIND)
    .bind(chain)
    .bind(logical_name_id)
    .bind(token_id)
    .bind(block_number)
    .bind(transaction_index)
    .bind(log_index)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load ENSv2 registry resource link for {logical_name_id} token {token_id}"
        )
    })?;

    Ok(match row {
        Some(row) => ResourceLink {
            logical_name_id: sql_row::get(&row, "logical_name_id")?,
            resource_id: sql_row::get(&row, "resource_id")?,
        },
        None => ResourceLink {
            logical_name_id: None,
            resource_id: None,
        },
    })
}
