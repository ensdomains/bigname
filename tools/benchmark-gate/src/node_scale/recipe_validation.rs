use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::Serialize;

use super::recipe::{ByteObservation, Recipe, ResolverCohort};

#[derive(Debug, Default, Serialize)]
pub(super) struct RecipeCounts {
    pub(super) names_below_eth: usize,
    pub(super) suffix_bootstrap_names: u32,
    pub(super) depths: [u32; 17],
    pub(super) maximum_depth: u8,
    pub(super) valid_byte_observations: u32,
    pub(super) invalid_byte_observations: u32,
    pub(super) no_byte_observation: u32,
    pub(super) unicode_label_candidates: u32,
    pub(super) resolver_never: u32,
    pub(super) resolver_stable: u32,
    pub(super) resolver_changing: u32,
    pub(super) ownership_unchanged: u32,
    pub(super) ownership_ordinary_churn: u32,
    pub(super) ownership_hot_churn: u32,
    pub(super) later_change_operations: u64,
    pub(super) hot_parent_fanouts: [u32; 11],
    pub(super) maximum_dns_wire_bytes: usize,
}

/// Validate the complete topology before emitting any raw fixture input. This
/// checks actual constructed hashes/paths, not only allocation-loop counters.
pub(super) fn validate(recipe: &Recipe) -> Result<RecipeCounts> {
    let names = recipe.nodes.len();
    let mut counts = RecipeCounts {
        names_below_eth: names,
        suffix_bootstrap_names: 1,
        ..RecipeCounts::default()
    };
    let mut hashes = HashSet::with_capacity(names);
    for node in &recipe.nodes {
        ensure!(
            hashes.insert(node.namehash),
            "duplicate node at {}",
            node.ordinal
        );
        ensure!(
            (1..=16).contains(&node.depth_below_eth),
            "invalid node depth"
        );
        counts.depths[node.depth_below_eth as usize] += 1;
        counts.maximum_depth = counts.maximum_depth.max(node.depth_below_eth);
        if let Some(parent) = node.parent {
            ensure!(parent < node.ordinal, "parent follows its child");
            let ancestor = &recipe.nodes[parent as usize];
            ensure!(
                ancestor.depth_below_eth + 1 == node.depth_below_eth,
                "depth mismatch"
            );
            if parent < 11 {
                counts.hot_parent_fanouts[parent as usize] += 1;
            }
        }
        let labels = recipe.raw_labels(node.ordinal);
        ensure!(
            labels.len() == node.depth_below_eth as usize + 1,
            "incomplete ancestry"
        );
        ensure!(
            labels
                .iter()
                .all(|label| !label.is_empty() && label.len() <= 63),
            "invalid label length"
        );
        let wire_bytes = 1 + labels.iter().map(|label| label.len() + 1).sum::<usize>();
        counts.maximum_dns_wire_bytes = counts.maximum_dns_wire_bytes.max(wire_bytes);
        ensure!(wire_bytes <= 255, "DNS name exceeds 255 bytes");
        match node.byte_observation() {
            ByteObservation::Valid => {
                ensure!(
                    labels
                        .iter()
                        .all(|label| std::str::from_utf8(label).is_ok()),
                    "valid witness has invalid ancestry"
                );
                let name = labels
                    .iter()
                    .map(|label| std::str::from_utf8(label).unwrap())
                    .collect::<Vec<_>>()
                    .join(".");
                let normalized = bigname_domain::normalization::normalize_name(&name)?;
                ensure!(
                    normalized.normalized_name == name,
                    "valid witness changes under ENS normalization"
                );
                counts.valid_byte_observations += 1;
            }
            ByteObservation::Invalid => {
                ensure!(
                    std::str::from_utf8(&node.raw_label).is_err(),
                    "invalid witness is valid UTF-8"
                );
                counts.invalid_byte_observations += 1;
            }
            ByteObservation::None => counts.no_byte_observation += 1,
        }
        if std::str::from_utf8(&node.raw_label).is_ok_and(|label| label.starts_with('é')) {
            counts.unicode_label_candidates += 1;
        }
        match node.resolver_cohort() {
            ResolverCohort::None => counts.resolver_never += 1,
            ResolverCohort::Stable => counts.resolver_stable += 1,
            ResolverCohort::Changing => counts.resolver_changing += 1,
        }
        let changes = node.later_changes();
        match changes {
            0 => counts.ownership_unchanged += 1,
            2..=4 => counts.ownership_ordinary_churn += 1,
            100 => counts.ownership_hot_churn += 1,
            _ => anyhow::bail!("unsupported history cohort"),
        }
        counts.later_change_operations += u64::from(changes);
    }
    let groups = names as u32 / 1_000;
    ensure!(
        counts.depths[1] == 650 * groups,
        "depth-one population drift"
    );
    ensure!(
        counts.depths[2..=4].iter().sum::<u32>() == 250 * groups,
        "middle-depth population drift"
    );
    ensure!(
        counts.depths[5..=8].iter().sum::<u32>() == 90 * groups,
        "deep population drift"
    );
    ensure!(
        counts.depths[9..=16].iter().sum::<u32>() == 10 * groups,
        "deepest population drift"
    );
    ensure!(
        counts.hot_parent_fanouts[0] == 100 * groups,
        "largest fanout drift"
    );
    ensure!(
        counts.hot_parent_fanouts[1..]
            .iter()
            .all(|count| *count == 10 * groups),
        "secondary fanout drift"
    );
    ensure!(
        counts.valid_byte_observations == 200 * groups
            && counts.invalid_byte_observations == 10 * groups
            && counts.no_byte_observation == 790 * groups,
        "byte cohort drift"
    );
    ensure!(
        counts.unicode_label_candidates == 100 * groups,
        "Unicode cohort drift"
    );
    ensure!(
        counts.resolver_never == 500 * groups
            && counts.resolver_stable == 300 * groups
            && counts.resolver_changing == 200 * groups,
        "resolver cohort drift"
    );
    ensure!(
        counts.ownership_unchanged == 800 * groups
            && counts.ownership_ordinary_churn == 199 * groups
            && counts.ownership_hot_churn == groups,
        "history cohort drift"
    );
    Ok(counts)
}
