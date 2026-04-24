use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

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
