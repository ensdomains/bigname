use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::{
    IdentityPrimaryNameSnapshot, READABLE_REVERSE_IDENTITY_CTES, ReverseIdentityStorageInput,
};
use serde_json::{Map, Value};
use sqlx::{PgPool, Row};

use super::{ReverseIdentityPageRow, roles_storage_value};

#[derive(Clone)]
struct CandidateNameForms {
    normalized_name: String,
    canonical_display_name: String,
    labelhash: Option<String>,
    labelhash_count: Option<i32>,
}

pub(super) async fn load_reverse_identity_page_rows(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
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
    let primary_names = load_primary_names(pool, inputs, public_namespaces).await?;
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

    let query = format!(
        r#"
        WITH {READABLE_REVERSE_IDENTITY_CTES}, requested AS (
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
        SELECT requested.input_index, candidate.logical_name_id, candidate.raw_name,
               requested.primary_names -> candidate.namespace AS primary_name
        FROM requested
        JOIN LATERAL (
            SELECT grouped.*
            FROM (
                SELECT anc.logical_name_id,
                       bool_or(COALESCE(
                           requested.primary_names -> anc.namespace
                               ->> 'normalized_claim_name' = identity_nc.raw_name,
                           false
                       )) AS is_primary,
                       min(CASE
                           WHEN anc.relation IN ('registrant', 'token_holder') THEN 0
                           ELSE 1
                       END)::SMALLINT AS role_rank,
                       identity_nc.raw_name AS raw_name,
                       anc.namespace,
                       anc.namehash
                FROM readable_relations anc
                JOIN readable_names identity_nc
                  ON identity_nc.logical_name_id = anc.logical_name_id
                WHERE lower(anc.address) = lower(requested.address)
                  AND anc.namespace = ANY($13::TEXT[])
                  AND (
                      requested.roles = 'both'
                      OR (requested.roles = 'owned'
                          AND anc.relation IN ('registrant', 'token_holder'))
                      OR (requested.roles = 'managed'
                          AND anc.relation = 'effective_controller')
                  )
                GROUP BY anc.logical_name_id, identity_nc.raw_name,
                         anc.namespace, anc.namehash
            ) grouped
            WHERE NOT requested.cursor_present
               OR (
                    NOT grouped.is_primary,
                    grouped.role_rank,
                    grouped.raw_name,
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
                     grouped.raw_name, grouped.namespace, grouped.namehash
            LIMIT requested.page_size + 1
        ) candidate ON TRUE
        ORDER BY requested.input_index, NOT candidate.is_primary,
                 candidate.role_rank, candidate.raw_name,
                 candidate.namespace, candidate.namehash
        "#
    );
    let rows = sqlx::query(&query)
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
        .bind(public_namespaces)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load phase reverse candidates for {} inputs",
                inputs.len()
            )
        })?;

    #[cfg(test)]
    super::primary_coherence_test_hooks::candidate_read_complete(pool).await?;
    #[cfg(test)]
    super::primary_coherence_test_hooks::pause_after_candidate_read(pool).await?;

    // Coin-type variants can repeat the same candidate across request inputs.
    let mut candidate_name_forms = BTreeMap::<(String, String), CandidateNameForms>::new();
    rows.into_iter()
        .map(|row| {
            let logical_name_id = row.try_get::<String, _>("logical_name_id")?;
            let raw_name = row.try_get::<String, _>("raw_name")?;
            let candidate_key = (logical_name_id.clone(), raw_name.clone());
            let name_forms = if let Some(name_forms) = candidate_name_forms.get(&candidate_key) {
                name_forms.clone()
            } else {
                let normalized = bigname_domain::normalization::normalize_name(&raw_name)
                    .with_context(|| {
                        format!("reverse candidate {logical_name_id} has an unreadable name")
                    })?;
                let labelhash = normalized.normalized_labels.first().map(|label| {
                    format!(
                        "0x{}",
                        alloy_primitives::hex::encode(alloy_primitives::keccak256(
                            label.as_bytes(),
                        ))
                    )
                });
                let name_forms = CandidateNameForms {
                    normalized_name: normalized.normalized_name,
                    canonical_display_name: normalized.canonical_display_name,
                    labelhash,
                    labelhash_count: i32::try_from(normalized.normalized_labels.len()).ok(),
                };
                candidate_name_forms.insert(candidate_key, name_forms.clone());
                name_forms
            };
            let primary_name = row
                .try_get::<Option<Value>, _>("primary_name")?
                .map(decode_primary_name)
                .transpose()?;
            Ok(ReverseIdentityPageRow {
                input_index: row.try_get::<i32, _>("input_index")? as usize,
                logical_name_id,
                normalized_name: name_forms.normalized_name,
                canonical_display_name: name_forms.canonical_display_name,
                labelhash: name_forms.labelhash,
                labelhash_count: name_forms.labelhash_count,
                primary_name,
            })
        })
        .collect()
}

async fn load_primary_names(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
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
        SELECT requested.input_index, primary_name.address, primary_name.namespace,
               primary_name.coin_type,
               CASE WHEN hydration.readable THEN primary_name.claim_status
                    ELSE primary_name.claim_provenance
                        -> 'canonical_head_multicall_hydration'
                        -> 'baseline' ->> 'claim_status'
               END AS claim_status,
               CASE WHEN hydration.readable THEN primary_name.raw_claim_name
                    ELSE primary_name.claim_provenance
                        -> 'canonical_head_multicall_hydration'
                        -> 'baseline' ->> 'raw_claim_name'
               END AS raw_claim_name,
               CASE WHEN hydration.readable THEN primary_name.claim_name_is_normalized
                    ELSE (primary_name.claim_provenance
                        -> 'canonical_head_multicall_hydration'
                        -> 'baseline' ->> 'claim_name_is_normalized')::boolean
               END AS claim_name_is_normalized,
               CASE
                   WHEN lineage.block_hash IS NULL THEN NULL
                   ELSE jsonb_build_object(
                       CASE primary_name.claim_provenance ->> 'chain_id'
                           WHEN 'ethereum-mainnet' THEN 'ethereum'
                           WHEN 'ethereum-sepolia' THEN 'ethereum-sepolia'
                           WHEN 'base-mainnet' THEN 'base'
                           WHEN 'base-sepolia' THEN 'base-sepolia'
                           ELSE primary_name.claim_provenance ->> 'chain_id'
                       END,
                       jsonb_build_object(
                           'chain_id', lineage.chain_id,
                           'block_number', lineage.block_number,
                           'block_hash', lineage.block_hash,
                           'timestamp', to_char(
                               lineage.block_timestamp AT TIME ZONE 'UTC',
                               'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                           )
                       )
                   )
               END AS chain_positions
        FROM requested
        JOIN bigname_phase.primary_names_current primary_name
         ON primary_name.address = requested.address
         AND primary_name.coin_type = requested.coin_type
         AND primary_name.namespace = ANY($4::TEXT[])
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = primary_name.claim_provenance ->> 'chain_id'
         AND lineage.block_hash = primary_name.claim_provenance ->> 'target_block_hash'
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        CROSS JOIN LATERAL (
            SELECT NOT (
                primary_name.claim_provenance ? 'canonical_head_multicall_hydration'
            ) OR EXISTS (
                SELECT 1
                FROM bigname_phase.chain_lineage hydration_lineage
                WHERE hydration_lineage.chain_id = primary_name.claim_provenance
                    -> 'canonical_head_multicall_hydration' ->> 'chain_id'
                  AND hydration_lineage.block_number::text = primary_name.claim_provenance
                    -> 'canonical_head_multicall_hydration' ->> 'block_number'
                  AND hydration_lineage.block_hash = primary_name.claim_provenance
                    -> 'canonical_head_multicall_hydration' ->> 'block_hash'
                  AND hydration_lineage.canonicality_state IN (
                      'canonical'::bigname_phase.canonicality_state,
                      'safe'::bigname_phase.canonicality_state,
                      'finalized'::bigname_phase.canonicality_state
                  )
            ) AS readable
        ) hydration
        ORDER BY requested.input_index, primary_name.namespace
        "#,
    )
    .bind((0..inputs.len() as i32).collect::<Vec<_>>())
    .bind(addresses)
    .bind(coin_types)
    .bind(public_namespaces)
    .fetch_all(pool)
    .await
    .context("failed to load phase primary names for reverse pagination")?;

    let mut by_input = vec![Map::new(); inputs.len()];
    for row in rows {
        let input_index = row.try_get::<i32, _>("input_index")? as usize;
        let address = row.try_get::<String, _>("address")?.to_ascii_lowercase();
        let namespace: String = row.try_get("namespace")?;
        let coin_type: String = row.try_get("coin_type")?;
        let claim_status =
            super::parse_primary_name_claim_status(&row.try_get::<String, _>("claim_status")?)?;
        let raw_name: Option<String> = row.try_get("raw_claim_name")?;
        let normalized_claim_name = bigname_storage::normalized_claim_name(
            claim_status,
            row.try_get("claim_name_is_normalized")?,
            raw_name.as_deref(),
        );
        let chain_positions: Option<Value> = row.try_get("chain_positions")?;
        let mut metadata = Map::from_iter([
            ("address".to_owned(), Value::String(address)),
            ("namespace".to_owned(), Value::String(namespace.clone())),
            ("coin_type".to_owned(), Value::String(coin_type)),
            (
                "claim_status".to_owned(),
                Value::String(claim_status.as_str().to_owned()),
            ),
        ]);
        if let Some(normalized_claim_name) = normalized_claim_name {
            metadata.insert(
                "normalized_claim_name".to_owned(),
                Value::String(normalized_claim_name),
            );
        }
        if let Some(chain_positions) = chain_positions {
            metadata.insert("chain_positions".to_owned(), chain_positions);
        }
        by_input[input_index].insert(namespace, Value::Object(metadata));
    }
    Ok(by_input.into_iter().map(Value::Object).collect())
}

fn decode_primary_name(value: Value) -> Result<IdentityPrimaryNameSnapshot> {
    let object = value
        .as_object()
        .context("reverse candidate primary-name metadata must be an object")?;
    let string = |field| {
        object
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .with_context(|| format!("reverse candidate primary-name metadata needs {field}"))
    };
    Ok(IdentityPrimaryNameSnapshot {
        address: string("address")?,
        namespace: string("namespace")?,
        coin_type: string("coin_type")?,
        claim_status: super::parse_primary_name_claim_status(&string("claim_status")?)?,
        normalized_claim_name: object
            .get("normalized_claim_name")
            .and_then(Value::as_str)
            .map(str::to_owned),
        chain_positions: object.get("chain_positions").cloned(),
    })
}

#[cfg(test)]
#[test]
fn reverse_identity_primary_claim_is_loaded_by_one_sql_statement() {
    let read_anchor = ["bigname_phase", "primary_names_current primary_name"].join(".");
    let read_count = [include_str!("mod.rs"), include_str!("page.rs")]
        .into_iter()
        .map(|source| source.matches(&read_anchor).count())
        .sum::<usize>();

    assert_eq!(read_count, 1, "primary-name selection must have one read");
}
