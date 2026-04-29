use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::NameCurrentRow;
use serde_json::{Map, Value};
use sqlx::{Postgres, Transaction};

use crate::validation::{RequestedChainPosition, required_chain_positions};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ManifestVersionIdentity {
    source_manifest_id: Option<i64>,
    source_family: Option<String>,
    manifest_version: i64,
}

pub(super) fn normalize_requested_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let mut positions = required_chain_positions(value, context)?;
    positions.sort_by(|left, right| {
        left.chain_id
            .cmp(&right.chain_id)
            .then(left.block_number.cmp(&right.block_number))
            .then(left.block_hash.cmp(&right.block_hash))
    });
    Ok(positions)
}

pub(crate) fn build_requested_chain_positions_from_projection(
    chain_positions: &Value,
) -> Result<Vec<RequestedChainPosition>> {
    Ok(
        bigname_storage::resolution_requested_chain_positions_from_projection(chain_positions)?
            .into_iter()
            .map(|position| RequestedChainPosition {
                chain_id: position.chain_id,
                block_number: position.block_number,
                block_hash: position.block_hash,
            })
            .collect(),
    )
}

pub(super) async fn ensure_requested_positions_are_eligible_for_projection(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NameCurrentRow,
    requested_positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    let projected_positions = build_requested_chain_positions_from_projection(&row.chain_positions)
        .with_context(|| {
            format!(
                "{context} failed to normalize projected chain_positions for logical_name_id {}",
                row.logical_name_id
            )
        })?;
    if projected_positions == requested_positions {
        return Ok(());
    }

    let projected_by_chain_id = positions_by_chain_id(
        &projected_positions,
        &format!("{context} projected chain_positions"),
    )?;
    let requested_by_chain_id = positions_by_chain_id(
        requested_positions,
        &format!("{context} cache_key.requested_chain_positions"),
    )?;
    if projected_by_chain_id
        .keys()
        .ne(requested_by_chain_id.keys())
    {
        bail!(
            "{context} cache_key.requested_chain_positions must use the same chain set as projected chain_positions for logical_name_id {}",
            row.logical_name_id
        );
    }

    for (chain_id, projected_position) in &projected_by_chain_id {
        let requested_position = requested_by_chain_id
            .get(chain_id)
            .expect("requested map must have the same chain_id keys as projected map");
        if requested_position.block_number < projected_position.block_number {
            bail!(
                "{context} cache_key.requested_chain_positions is older than projected chain_positions for logical_name_id {} on chain {}",
                row.logical_name_id,
                chain_id
            );
        }
        if requested_position.block_number == projected_position.block_number {
            if requested_position.block_hash != projected_position.block_hash {
                bail!(
                    "{context} cache_key.requested_chain_positions does not match projected chain_positions for logical_name_id {} on chain {}",
                    row.logical_name_id,
                    chain_id
                );
            }
            continue;
        }

        if !position_is_canonical_lineage_member(transaction, chain_id, projected_position).await? {
            bail!(
                "{context} projected chain_positions block is no longer canonical for logical_name_id {} on chain {}",
                row.logical_name_id,
                chain_id
            );
        }
        if !position_is_canonical_lineage_member(transaction, chain_id, requested_position).await? {
            bail!(
                "{context} cache_key.requested_chain_positions block is not canonical for logical_name_id {} on chain {}",
                row.logical_name_id,
                chain_id
            );
        }
        if name_current_has_newer_projection_inputs(
            transaction,
            row,
            chain_id,
            projected_position.block_number,
            requested_position.block_number,
        )
        .await?
        {
            bail!(
                "{context} cache_key.requested_chain_positions crosses newer projection inputs for logical_name_id {} on chain {}",
                row.logical_name_id,
                chain_id
            );
        }
    }

    Ok(())
}

fn positions_by_chain_id<'a>(
    positions: &'a [RequestedChainPosition],
    context: &str,
) -> Result<BTreeMap<&'a str, &'a RequestedChainPosition>> {
    let mut by_chain_id = BTreeMap::new();
    for position in positions {
        if by_chain_id
            .insert(position.chain_id.as_str(), position)
            .is_some()
        {
            bail!("{context} repeats chain_id {}", position.chain_id);
        }
    }
    Ok(by_chain_id)
}

async fn position_is_canonical_lineage_member(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    position: &RequestedChainPosition,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(chain_id)
    .bind(&position.block_hash)
    .bind(position.block_number)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to check execution chain position block {} on chain {chain_id}",
            position.block_hash
        )
    })
}

async fn name_current_has_newer_projection_inputs(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NameCurrentRow,
    chain_id: &str,
    projected_block_number: i64,
    selected_block_number: i64,
) -> Result<bool> {
    let newer_event = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM normalized_events ne
            WHERE ne.chain_id = $1
              AND ne.block_number > $2
              AND ne.block_number <= $3
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND (
                  ne.logical_name_id = $4
                  OR ($5::UUID IS NOT NULL AND ne.resource_id = $5)
              )
            LIMIT 1
        )
        "#,
    )
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .bind(&row.logical_name_id)
    .bind(row.resource_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to check execution requested-position normalized-event invalidation for {}",
            row.logical_name_id
        )
    })?;
    if newer_event {
        return Ok(true);
    }

    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM surface_bindings sb
            WHERE sb.logical_name_id = $1
              AND sb.chain_id = $2
              AND sb.block_number > $3
              AND sb.block_number <= $4
              AND sb.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            LIMIT 1
        )
        "#,
    )
    .bind(&row.logical_name_id)
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to check execution requested-position surface-binding invalidation for {}",
            row.logical_name_id
        )
    })
}

pub(super) fn normalize_manifest_versions_for_revalidation(
    value: &Value,
    context: &str,
) -> Result<Value> {
    let items = value
        .as_array()
        .with_context(|| format!("{context} must be a JSON array"))?;
    if items.is_empty() {
        bail!("{context} must not be empty");
    }

    let mut versions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = item
            .as_object()
            .with_context(|| format!("{context}[{index}] must be a JSON object"))?;
        let source_manifest_id = match object.get("source_manifest_id") {
            None | Some(Value::Null) => None,
            Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
                format!("{context}[{index}].source_manifest_id must be null or a positive integer")
            })?),
        };
        let source_family = match object.get("source_family") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
            Some(_) => bail!("{context}[{index}].source_family must be null or a non-empty string"),
        };
        if source_manifest_id.is_none() && source_family.is_none() {
            bail!("{context}[{index}] must include source_manifest_id or source_family");
        }
        let manifest_version = object
            .get("manifest_version")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .with_context(|| {
                format!("{context}[{index}].manifest_version must be a positive integer")
            })?;
        versions.push(ManifestVersionIdentity {
            source_manifest_id,
            source_family,
            manifest_version,
        });
    }

    versions.sort();
    versions.dedup();

    Ok(Value::Array(
        versions
            .into_iter()
            .map(|version| {
                let mut object = Map::new();
                if let Some(source_manifest_id) = version.source_manifest_id {
                    object.insert(
                        "source_manifest_id".to_owned(),
                        Value::Number(source_manifest_id.into()),
                    );
                }
                if let Some(source_family) = version.source_family {
                    object.insert("source_family".to_owned(), Value::String(source_family));
                }
                object.insert(
                    "manifest_version".to_owned(),
                    Value::Number(version.manifest_version.into()),
                );
                Value::Object(object)
            })
            .collect(),
    ))
}
