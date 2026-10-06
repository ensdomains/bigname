//! The name a surface is served under when it stores no raw bytes.
//!
//! A name surface always carries its complete label-hash path, but the raw bytes of its labels
//! are optional (docs/storage.md, "Name identity and raw evidence"). A surface with bytes is
//! served under its stored name. A surface without them is served under a name built at read
//! time: each label is its text when `label_preimages` holds a decoded label that passed
//! normalization, else `[<64 lowercase hex digits of the labelhash>]`, joined with `.` over the
//! whole path.
//!
//! The rule has one SQL spelling ([`rendered_name_sql`]) and one Rust spelling
//! ([`placeholder_name`], the name when no label is known). The composition Project's summary
//! step reaches uses only the Rust one, so an imported preimage never changes a stored summary;
//! the serving readers apply the SQL one afterwards ([`enrich`]).
use std::collections::BTreeMap;

use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result};
use bigname_domain::normalization::{
    self, EnsNameNormalizationError, normalize_label_under_suffix, normalize_name,
};
use sqlx::PgConnection;

use crate::NameCurrentRow;

/// SQL for the served name of the `name_surfaces` row aliased `surface`, with the tables
/// qualified by `bigname_phase`. A row with bytes yields its `raw_name` without reading
/// `label_preimages`. The alias must not be `path` or `preimage`, which the expression uses.
pub fn rendered_name_sql(surface: &str) -> String {
    name_sql(surface, "bigname_phase.")
}

/// [`rendered_name_sql`] for statements that name tables through the search path.
pub fn rendered_name_sql_unqualified(surface: &str) -> String {
    name_sql(surface, "")
}

fn name_sql(surface: &str, schema: &str) -> String {
    format!(
        "COALESCE({surface}.raw_name, (
            SELECT string_agg(
                       CASE WHEN preimage.decoded_label IS NOT NULL
                                 AND preimage.normalized_under_version
                            THEN preimage.decoded_label
                            ELSE '[' || substring(lower(path.labelhash) FROM 3) || ']' END,
                       '.' ORDER BY path.position)
            FROM unnest({surface}.labelhashes) WITH ORDINALITY AS path(labelhash, position)
            LEFT JOIN {schema}label_preimages preimage
              ON preimage.labelhash = lower(path.labelhash)))"
    )
}

/// SQL for "the name compositor reads the `name_surfaces` row aliased `surface`": it is active
/// and is not the empty root name. A surface without raw bytes passes. Every reader that asks
/// whether a node has a composed name row uses this text, so that none lists a node the
/// compositor also serves. Canonicality and the publication bound are the caller's.
pub fn composed_surface_sql(surface: &str) -> String {
    format!("{surface}.visibility_state = 'active' AND {surface}.raw_name IS DISTINCT FROM ''")
}

/// `[<64 lowercase hex digits>]` for a `0x`-prefixed labelhash.
pub fn placeholder_label(labelhash: &str) -> Option<String> {
    let digits = labelhash.strip_prefix("0x")?;
    (digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| format!("[{}]", digits.to_ascii_lowercase()))
}

/// Every label of a label-hash path as its placeholder, joined with `.`: the name of a surface
/// none of whose labels is known.
pub fn placeholder_name(labelhashes: &[String]) -> Result<String> {
    anyhow::ensure!(!labelhashes.is_empty(), "a name has at least one label");
    let labels = labelhashes
        .iter()
        .map(|labelhash| {
            placeholder_label(labelhash)
                .with_context(|| format!("{labelhash} is not a 0x-prefixed 32-byte labelhash"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(labels.join("."))
}

/// The hex digits of a label spelled `[<64 hex digits>]`, either case. The normalizer rejects
/// `[` and `]`, so no normalized label takes this form.
pub fn bracketed_label(label: &str) -> Option<&str> {
    label
        .strip_prefix('[')?
        .strip_suffix(']')
        .filter(|digits| digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// The labelhash one label of a served name stands for: the hash a bracketed label spells, else
/// keccak256 of the label's text.
pub fn label_hash(label: &str) -> [u8; 32] {
    match bracketed_label(label) {
        Some(digits) => {
            let mut labelhash = [0_u8; 32];
            hex::decode_to_slice(digits, &mut labelhash).expect("64 hex digits decode to 32 bytes");
            labelhash
        }
        None => keccak256(label.as_bytes()).0,
    }
}

/// A served name split into its labels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedName {
    pub normalized_name: String,
    pub canonical_display_name: String,
    /// The labels of `normalized_name`, in name order.
    pub labels: Vec<String>,
    /// The labelhash of each label.
    pub labelhashes: Vec<[u8; 32]>,
}

/// Parse a served name. A name with no bracketed label is normalized whole, exactly as
/// [`normalize_name`] does. Otherwise each bracketed label must be lowercase and is kept, and
/// every other label is normalized by itself; the display name is then the normalized name.
pub fn parse(name: &str) -> normalization::Result<RenderedName> {
    if !name
        .split('.')
        .any(|label| bracketed_label(label).is_some())
    {
        let normalized = normalize_name(name)?;
        return Ok(RenderedName {
            labelhashes: normalized
                .normalized_labels
                .iter()
                .map(|label| keccak256(label.as_bytes()).0)
                .collect(),
            normalized_name: normalized.normalized_name,
            canonical_display_name: normalized.canonical_display_name,
            labels: normalized.normalized_labels,
        });
    }
    let mut labels = Vec::new();
    for label in name.split('.') {
        match bracketed_label(label) {
            Some(digits) if digits.bytes().any(|byte| byte.is_ascii_uppercase()) => {
                return Err(EnsNameNormalizationError::new(format!(
                    "bracketed labelhash {label} must be lowercase hex"
                )));
            }
            Some(_) => labels.push(label.to_owned()),
            None => labels.push(normalize_label_under_suffix(label, &[])?.normalized_name),
        }
    }
    let normalized_name = labels.join(".");
    Ok(RenderedName {
        labelhashes: labels.iter().map(|label| label_hash(label)).collect(),
        canonical_display_name: normalized_name.clone(),
        normalized_name,
        labels,
    })
}

/// Give the composed rows of surfaces without raw bytes the name the serving readers show:
/// placeholder labels become label text wherever a usable preimage exists. Rows of surfaces
/// with bytes carry no bracketed label, so a batch of them issues no statement.
pub(crate) async fn enrich(
    conn: &mut PgConnection,
    rows: &mut BTreeMap<String, NameCurrentRow>,
) -> Result<()> {
    let ids: Vec<String> = rows
        .values()
        .filter(|row| {
            row.normalized_name
                .split('.')
                .any(|label| bracketed_label(label).is_some())
        })
        .map(|row| row.logical_name_id.clone())
        .collect();
    if ids.is_empty() {
        return Ok(());
    }
    let names: Vec<(String, String)> = sqlx::query_as(&format!(
        "/* storage:families.name.rendered_names */
         SELECT surface.logical_name_id, {name}
         FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1::text[]) AND surface.raw_name IS NULL",
        name = rendered_name_sql("surface")
    ))
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to render the names of surfaces without raw bytes")?;
    for (id, name) in names {
        if let Some(row) = rows.get_mut(&id) {
            row.canonical_display_name.clone_from(&name);
            row.normalized_name = name;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "rendered_tests.rs"]
mod tests;
