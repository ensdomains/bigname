//! Shadow comparison of the owned key family readers (TYR-36 step 4) after a Project run: the
//! families are rebuilt to the served marker, then every record inventory row, the complete
//! sequence of names resolving to each address and every reverse claim is read through today's
//! readers and through the family readers (`bigname_storage::families::records`) and compared
//! field by field. A test that includes this file calls it after each Project run.
//!
//! A test states every difference it expects as an [`ExpectedDifference`]: the target, the
//! complete result key, every differing field with today's and the family's value, and how many
//! comparisons in the test must show it. A comparison fails on a difference no record expects, on
//! an expected result with another field or value, and on an expected difference that does not
//! show; [`Expectations::finish`] fails when one showed a different number of times. The index
//! misses and node claims at another resolver the report lists as diagnostics must equal the ones
//! the test states at that target, so they too cannot appear or vanish unnoticed.
#![allow(dead_code)]

use std::sync::Mutex;

use anyhow::{Result, bail, ensure};
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions},
};
use bigname_storage::families::records::{ShadowReport, compare_family_reads};
use serde_json::Value;
use sqlx::PgPool;

/// One difference a test expects.
#[derive(Clone, Debug)]
pub struct ExpectedDifference {
    /// The served block the comparison runs at.
    pub target: i64,
    /// The complete result key, as the report names it.
    pub key: String,
    /// Every field that differs, with today's value and the family's value.
    pub fields: Vec<(String, Value, Value)>,
    /// How many comparisons in the test must show it.
    pub times: usize,
}

/// What a test expects its comparisons to show beyond equality.
#[derive(Debug, Default)]
pub struct Expectations {
    pub differences: Vec<ExpectedDifference>,
    /// `(target, index miss)`: the served entries the address index alone would not find.
    pub index_misses: Vec<(i64, String)>,
    /// `(target, tuple key)`: reverse tuples whose node claim is only at another resolver.
    pub node_claims_at_other_resolver: Vec<(i64, String)>,
    /// `(target, resolver)`: resolvers classified from `resolver_current` because F3 has no row.
    pub classification_fallbacks: Vec<(i64, String)>,
    /// How often each expected difference showed so far; leave it at its default.
    pub seen: Mutex<Vec<usize>>,
}

impl Expectations {
    /// No difference and no diagnostic.
    pub fn none() -> Self {
        Self::default()
    }

    /// Check one comparison at `target` against the expectations.
    pub fn check(&self, target: i64, report: &ShadowReport) -> Result<()> {
        let mut matched = vec![false; self.differences.len()];
        for (key, differences) in &report.differences {
            let Some(index) = self
                .differences
                .iter()
                .position(|expected| expected.target == target && &expected.key == key)
            else {
                bail!("unexpected difference at {target} on {key}: {differences:#?}");
            };
            let mut actual: Vec<(String, Value, Value)> = differences
                .iter()
                .map(|difference| {
                    (
                        difference.field.clone(),
                        difference.today.clone(),
                        difference.family.clone(),
                    )
                })
                .collect();
            let mut expected = self.differences[index].fields.clone();
            actual.sort_by(|a, b| a.0.cmp(&b.0));
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            ensure!(
                actual == expected,
                "the difference at {target} on {key} is not the expected one: \
                 expected {expected:#?}, got {actual:#?}"
            );
            matched[index] = true;
        }
        for (index, expected) in self.differences.iter().enumerate() {
            ensure!(
                expected.target != target || matched[index],
                "the expected difference at {target} on {} did not show",
                expected.key
            );
        }
        // The note lists compare as multisets: their order carries no meaning.
        let stated = |list: &[(i64, String)]| -> Vec<String> {
            let mut keys: Vec<String> = list
                .iter()
                .filter(|(at, _)| *at == target)
                .map(|(_, key)| key.clone())
                .collect();
            keys.sort();
            keys
        };
        let sorted = |list: &[String]| -> Vec<String> {
            let mut keys = list.to_vec();
            keys.sort();
            keys
        };
        ensure!(
            sorted(&report.address_index_misses) == stated(&self.index_misses),
            "index misses at {target}: expected {:#?}, got {:#?}",
            stated(&self.index_misses),
            report.address_index_misses
        );
        ensure!(
            sorted(&report.node_claims_at_other_resolver)
                == stated(&self.node_claims_at_other_resolver),
            "node claims at another resolver at {target}: expected {:#?}, got {:#?}",
            stated(&self.node_claims_at_other_resolver),
            report.node_claims_at_other_resolver
        );
        ensure!(
            sorted(&report.classification_fallbacks) == stated(&self.classification_fallbacks),
            "classification fallbacks at {target}: expected {:#?}, got {:#?}",
            stated(&self.classification_fallbacks),
            report.classification_fallbacks
        );
        let mut seen = self.seen.lock().expect("expectation counts");
        seen.resize(self.differences.len(), 0);
        for (index, hit) in matched.into_iter().enumerate() {
            seen[index] += usize::from(hit);
        }
        Ok(())
    }

    /// Require every expected difference to have shown exactly its number of times.
    pub fn finish(&self) -> Result<()> {
        let mut seen = self.seen.lock().expect("expectation counts").clone();
        seen.resize(self.differences.len(), 0);
        for (expected, seen) in self.differences.iter().zip(seen) {
            ensure!(
                seen == expected.times,
                "the expected difference at {} on {} showed {seen} times, not {}",
                expected.target,
                expected.key,
                expected.times
            );
        }
        Ok(())
    }
}

/// Rebuild the families at `target` and compare, without judging the report.
pub async fn shadow_report_at(pool: &PgPool, target: &Marker) -> Result<ShadowReport> {
    let chain_id = rebuild_families_at(pool, target).await?;
    compare_family_reads_on(pool, &chain_id, target).await
}

/// Rebuild the families at `target` without comparing; returns the chain.
pub async fn rebuild_families_at(pool: &PgPool, target: &Marker) -> Result<String> {
    let chain_id: String = sqlx::query_scalar(
        "SELECT chain_id FROM bigname_phase.chain_lineage
         WHERE block_number = $1 AND block_hash = $2 LIMIT 1",
    )
    .bind(target.number)
    .bind(&target.hash)
    .fetch_one(pool)
    .await?;
    let token = families::input_token(pool, &chain_id).await?;
    let outcome = families::apply(
        pool,
        &chain_id,
        target,
        FamilyMode::Rebuild,
        &token,
        &FamilyOptions::new("family-shadow"),
    )
    .await;
    ensure!(
        outcome.skipped.is_none(),
        "the families stopped before the served marker: {:?}",
        outcome.skipped
    );
    Ok(chain_id)
}

async fn compare_family_reads_on(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
) -> Result<ShadowReport> {
    let report = compare_family_reads(
        pool,
        chain_id,
        Some((target.number, target.hash.clone())),
        1,
    )
    .await?;
    ensure!(
        report.current(),
        "the family marker is not the served marker: {report:?}"
    );
    eprintln!(
        "FAMILY_SHADOW target={} inventory_rows={} compatibility_pairs={} address_pages={} \
         address_entries={} primary_tuples={} differences={} node_claims_at_other_resolver={} \
         address_index_misses={} classification_fallbacks={}",
        target.number,
        report.inventory_rows,
        report.compatibility_pairs,
        report.address_pages,
        report.address_entries,
        report.primary_tuples,
        report.differences.len(),
        report.node_claims_at_other_resolver.len(),
        report.address_index_misses.len(),
        report.classification_fallbacks.len(),
    );
    Ok(report)
}

/// Rebuild the families at `target`, compare, and check the report against `expected`.
pub async fn compare_family_reads_at(
    pool: &PgPool,
    target: &Marker,
    expected: &Expectations,
) -> Result<ShadowReport> {
    let report = shadow_report_at(pool, target).await?;
    expected.check(target.number, &report)?;
    Ok(report)
}

/// [`compare_family_reads_at`] with no difference and no diagnostic expected.
pub async fn assert_family_reads_match(pool: &PgPool, target: &Marker) -> Result<ShadowReport> {
    compare_family_reads_at(pool, target, &Expectations::none()).await
}
