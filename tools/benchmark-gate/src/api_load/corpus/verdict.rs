use std::collections::BTreeMap;

use anyhow::{Result, ensure};

pub(super) fn collect_failure(failures: &mut Vec<String>, result: Result<()>) {
    if let Err(error) = result {
        failures.push(format!("{error:#}"));
    }
}

pub(super) fn require_minimum_size(label: &str, actual: usize, minimum: usize) -> Result<()> {
    ensure!(
        actual >= minimum,
        "{label} corpus has {actual} rows; release profile requires {minimum}"
    );
    Ok(())
}

pub(crate) fn require_stratified_size(
    label: &str,
    actual: usize,
    minimum: usize,
    counts_by_namespace: &BTreeMap<String, usize>,
) -> Result<()> {
    let contributions = counts_by_namespace
        .iter()
        .map(|(namespace, count)| format!("{namespace}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    ensure!(
        actual >= minimum,
        "{label} corpus has {actual} rows; release profile requires {minimum}; namespace contributions: {contributions}"
    );
    Ok(())
}
