use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use alloy_primitives::keccak256;
use anyhow::Result;
use serde_json::json;

use super::{
    manifests::address,
    raw::owner,
    recipe::{ByteObservation, Recipe, ResolverCohort},
};

/// Deterministic strata exercise depths, parent fanout, byte, resolver and
/// churn cohorts. The corpus remains the authority for exact population size.
pub(super) fn write(directory: &Path, recipe: &Recipe) -> Result<()> {
    let mut selected = (0..50_u32).collect::<BTreeSet<_>>();
    selected.extend([
        600, 601, 610, 620, 630, 640, 650, 651, 799, 800, 801, 899, 900, 989, 990, 991, 999,
    ]);
    let last = recipe.nodes.len() as u32 - 1_000;
    selected.extend([last, last + 601, last + 650, last + 991, last + 999]);
    let mut children = BTreeMap::<u32, u32>::new();
    let mut depths = selected
        .iter()
        .map(|index| recipe.nodes[*index as usize].depth_below_eth)
        .collect::<BTreeSet<_>>();
    for node in &recipe.nodes {
        if let Some(parent) = node.parent {
            *children.entry(parent).or_default() += 1;
        }
        if depths.insert(node.depth_below_eth) {
            selected.insert(node.ordinal);
        }
    }
    let samples = selected.iter().map(|ordinal| {
        let node = &recipe.nodes[*ordinal as usize];
        let labels = recipe.raw_labels(*ordinal);
        let input = labels.iter().map(|label| format!("[{:x}]",keccak256(label))).collect::<Vec<_>>().join(".");
        let raw_name = labels.iter().map(|label| std::str::from_utf8(label)).collect::<Result<Vec<_>,_>>().ok().map(|labels| labels.join("."));
        let resolver = match node.resolver_cohort() {
            ResolverCohort::None => None,
            ResolverCohort::Stable => Some(address("public_resolver")),
            ResolverCohort::Changing => Some(address("public_resolver_8948458")),
        };
        let root_lease = node.parent.is_none() && !matches!(node.byte_observation(),ByteObservation::None);

        json!({
            "ordinal": ordinal, "namehash": format!("{:#x}",node.namehash), "input":input,
            "raw_name":raw_name, "changed_resolver":resolver.map(|value|format!("{value:#x}")),
            "changed_text_url":resolver.map(|_|format!("https://current.example/{:#x}",node.namehash)),
            "bytes_epoch_lease_expiry":root_lease.then_some(2_000_000_000_u64+u64::from(node.ordinal%4)*86_400),
            "owner":format!("{:#x}",owner(*ordinal)), "depth":node.depth_below_eth,
            "direct_children":children.get(ordinal).copied().unwrap_or(0),
            "byte_observation":node.byte_observation(), "resolver_cohort":node.resolver_cohort(),
            "later_changes":node.later_changes(),
            "bytes_epoch_visible":!matches!(node.byte_observation(),ByteObservation::Invalid),
        })
    }).collect::<Vec<_>>();
    fs::write(
        directory.join("samples.json"),
        serde_json::to_vec_pretty(
            &json!({"namespace":"ens","chain_id":11155111,"samples":samples}),
        )?,
    )?;
    Ok(())
}
