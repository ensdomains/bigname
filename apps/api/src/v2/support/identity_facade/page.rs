use anyhow::{Context, Result};
use bigname_storage::ReverseIdentityStorageInput;
use serde_json::{Map, Value};
use sqlx::{PgPool, Row};

use super::{ReverseIdentityPageRow, roles_storage_value};

pub(super) async fn load_reverse_identity_page_rows(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityPageRow>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let input_indexes = (0..inputs.len() as i32).collect::<Vec<_>>();
    let addresses = inputs
        .iter()
        .map(|input| input.address.clone())
        .collect::<Vec<_>>();
    let coin_types = inputs
        .iter()
        .map(|input| input.coin_type.clone())
        .collect::<Vec<_>>();
    let roles = inputs
        .iter()
        .map(|input| roles_storage_value(input.roles).to_owned())
        .collect::<Vec<_>>();
    let primary_names = load_normalized_primary_names(pool, inputs).await?;
    let page_sizes = inputs
        .iter()
        .map(|input| input.page_size.max(0))
        .collect::<Vec<_>>();
    let cursor_present = inputs
        .iter()
        .map(|input| input.cursor.is_some())
        .collect::<Vec<_>>();
    let cursor_is_primary = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.is_primary))
        .collect::<Vec<_>>();
    let cursor_role_ranks = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.role_rank))
        .collect::<Vec<_>>();
    let cursor_normalized_names = inputs
        .iter()
        .map(|input| {
            input
                .cursor
                .as_ref()
                .map(|cursor| cursor.normalized_name.clone())
        })
        .collect::<Vec<_>>();
    let cursor_namespaces = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.namespace.clone()))
        .collect::<Vec<_>>();
    let cursor_namehashes = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.namehash.clone()))
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        WITH requested AS (
            SELECT * FROM UNNEST(
                $1::INT[], $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::JSONB[],
                $6::BIGINT[], $7::BOOLEAN[], $8::BOOLEAN[], $9::SMALLINT[],
                $10::TEXT[], $11::TEXT[], $12::TEXT[]
            ) AS requested(
                input_index, address, coin_type, roles, primary_names, page_size,
                cursor_present, cursor_is_primary, cursor_role_rank,
                cursor_normalized_name, cursor_namespace, cursor_namehash
            )
        )
        SELECT requested.input_index, candidate.logical_name_id,
               candidate.relation_chain_positions
        FROM requested
        JOIN LATERAL (
            SELECT grouped.*
            FROM (
                SELECT anc.logical_name_id,
                       bool_or(COALESCE(
                           requested.primary_names ->> anc.namespace = anc.raw_name,
                           false
                       )) AS is_primary,
                       min(CASE
                           WHEN anc.relation IN ('registrant', 'token_holder') THEN 0
                           ELSE 1
                       END)::SMALLINT AS role_rank,
                       anc.raw_name AS normalized_name,
                       anc.namespace,
                       anc.namehash,
                       array_agg(anc.chain_positions ORDER BY anc.relation)
                           AS relation_chain_positions
                FROM address_names_current anc
                JOIN name_current identity_nc
                  ON identity_nc.logical_name_id = anc.logical_name_id
                WHERE lower(anc.address) = lower(requested.address)
                  AND anc.support_status = 'supported'
                  AND identity_nc.support_status IN ('supported', 'unsupported')
                  AND (
                      requested.roles = 'both'
                      OR (requested.roles = 'owned'
                          AND anc.relation IN ('registrant', 'token_holder'))
                      OR (requested.roles = 'managed'
                          AND anc.relation = 'effective_controller')
                  )
                GROUP BY anc.logical_name_id, anc.raw_name, anc.namespace, anc.namehash
            ) grouped
            WHERE NOT requested.cursor_present
               OR (
                    NOT grouped.is_primary,
                    grouped.role_rank,
                    grouped.normalized_name,
                    grouped.namespace,
                    grouped.namehash
               ) > (
                    NOT requested.cursor_is_primary,
                    requested.cursor_role_rank,
                    requested.cursor_normalized_name,
                    requested.cursor_namespace,
                    requested.cursor_namehash
               )
            ORDER BY NOT grouped.is_primary, grouped.role_rank,
                     grouped.normalized_name, grouped.namespace, grouped.namehash
            LIMIT requested.page_size + 1
        ) candidate ON TRUE
        ORDER BY requested.input_index, NOT candidate.is_primary,
                 candidate.role_rank, candidate.normalized_name,
                 candidate.namespace, candidate.namehash
        "#,
    )
    .bind(&input_indexes)
    .bind(&addresses)
    .bind(&coin_types)
    .bind(&roles)
    .bind(&primary_names)
    .bind(&page_sizes)
    .bind(&cursor_present)
    .bind(&cursor_is_primary)
    .bind(&cursor_role_ranks)
    .bind(&cursor_normalized_names)
    .bind(&cursor_namespaces)
    .bind(&cursor_namehashes)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load phase reverse candidates for {} inputs",
            inputs.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(ReverseIdentityPageRow {
                input_index: row.try_get::<i32, _>("input_index")? as usize,
                logical_name_id: row.try_get("logical_name_id")?,
                relation_chain_positions: row.try_get("relation_chain_positions")?,
            })
        })
        .collect()
}

async fn load_normalized_primary_names(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<Value>> {
    let addresses = inputs
        .iter()
        .map(|input| input.address.clone())
        .collect::<Vec<_>>();
    let coin_types = inputs
        .iter()
        .map(|input| input.coin_type.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        WITH requested AS (
            SELECT * FROM UNNEST($1::INT[], $2::TEXT[], $3::TEXT[])
                AS requested(input_index, address, coin_type)
        )
        SELECT requested.input_index, primary_name.namespace,
               primary_name.raw_claim_name
        FROM requested
        JOIN primary_names_current primary_name
          ON lower(primary_name.address) = lower(requested.address)
         AND primary_name.coin_type = requested.coin_type
         AND primary_name.claim_status = 'success'
        ORDER BY requested.input_index, primary_name.namespace
        "#,
    )
    .bind((0..inputs.len() as i32).collect::<Vec<_>>())
    .bind(addresses)
    .bind(coin_types)
    .fetch_all(pool)
    .await
    .context("failed to load phase primary names for reverse pagination")?;

    let mut by_input = vec![Map::new(); inputs.len()];
    for row in rows {
        let input_index = row.try_get::<i32, _>("input_index")? as usize;
        let namespace: String = row.try_get("namespace")?;
        let raw_name: String = row.try_get("raw_claim_name")?;
        let normalized = bigname_domain::normalization::normalize_name(&raw_name)
            .with_context(|| format!("successful phase primary name {raw_name} is invalid"))?;
        by_input[input_index].insert(namespace, Value::String(normalized.normalized_name));
    }
    Ok(by_input.into_iter().map(Value::Object).collect())
}
