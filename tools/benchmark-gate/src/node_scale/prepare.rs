use std::{fs, path::Path};

use anyhow::{Result, ensure};
use serde::Serialize;

use super::{
    events,
    manifests::{self, ManifestReceipt},
    raw::{RawCounts, Writer},
    recipe::{RECIPE_VERSION, Recipe},
    recipe_validation::{self, RecipeCounts},
};

#[derive(Serialize)]
pub(super) struct Corpus {
    pub(super) recipe_version: &'static str,
    pub(super) chain: &'static str,
    pub(super) source_head: String,
    pub(super) interpreter_content_hash: &'static str,
    pub(super) normalizer_version: &'static str,
    pub(super) topology: RecipeCounts,
    pub(super) manifests: Vec<ManifestReceipt>,
    pub(super) structural_head: i64,
    pub(super) changed_head: i64,
    pub(super) bytes_head: i64,
    pub(super) raw: RawCounts,
    pub(super) validation_executed: bool,
    pub(super) feature_gate_complete: bool,
}

pub(super) fn run(names: u32, directory: &Path) -> Result<Corpus> {
    ensure!(
        !directory.exists(),
        "corpus output directory already exists"
    );
    let recipe = Recipe::new(names)?;
    let topology = recipe_validation::validate(&recipe)?;
    fs::create_dir_all(directory)?;
    let (repository, manifests) = manifests::write(&directory.join("manifests"))?;
    let mut writer = Writer::new(&directory.join("raw-transactions.jsonl"), &repository)?;
    let structural_head = events::structural(&mut writer, &recipe)?;
    let changed_head = events::changes(&mut writer, &recipe, structural_head + 1)?;
    let bytes_head = events::bytes(&mut writer, &recipe, changed_head + 1)?;
    let raw = writer.finish()?;
    let changes: u64 = recipe
        .nodes
        .iter()
        .map(|node| u64::from(node.later_changes()))
        .sum();
    let groups = u64::from(names / 1_000);
    // Each root byte observation has numeric mint/grant followed by four wrap
    // logs; a deeper wrap has three. Controller/suffix setup and approvals add
    // 202. These expected counts are independent of the writer's counters.
    let expected_logs = u64::from(names) + 1 + changes + 1_600 * groups + 1_050 * groups + 202;
    let expected_transactions =
        u64::from(names) + 1 + changes + 1_600 * groups + 350 * groups + 202;
    ensure!(
        raw.logs == expected_logs && raw.transactions == expected_transactions,
        "raw-event population mismatch: expected {expected_logs} logs/{expected_transactions} transactions, generated {}/{}",
        raw.logs,
        raw.transactions
    );
    let corpus = Corpus {
        recipe_version: RECIPE_VERSION,
        chain: manifests::CHAIN,
        source_head: crate::git_head(),
        interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH,
        normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION,
        topology,
        manifests,
        structural_head,
        changed_head,
        bytes_head,
        raw,
        validation_executed: false,
        feature_gate_complete: false,
    };
    super::samples::write(directory, &recipe)?;
    fs::write(
        directory.join("corpus.json"),
        serde_json::to_vec_pretty(&corpus)?,
    )?;
    Ok(corpus)
}
