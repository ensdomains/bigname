//! Shadow comparison of the owned key family readers (TYR-36 step 4) after a Project run: the
//! families are rebuilt to the served marker, then every record inventory row, every page of names
//! resolving to an indexed address and every reverse claim is read through today's readers and
//! through the family readers (`bigname_storage::families::records`) and compared field by field.
//! A test that includes this file calls it after each Project run, so a served value the families
//! do not reproduce fails there.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions},
};
use bigname_storage::families::records::{ShadowReport, compare_family_reads};
use sqlx::PgPool;

/// Rebuild the families at `target`, compare, and require no difference outside `expected`, a
/// predicate over the difference key (for a disclosed canonical-order change or a known step 2
/// gap the test names).
pub async fn compare_family_reads_at(
    pool: &PgPool,
    target: &Marker,
    expected: impl Fn(&str) -> bool,
) -> Result<ShadowReport> {
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
    let report = compare_family_reads(
        pool,
        &chain_id,
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
         address_entries={} primary_tuples={} differences={} node_claim_findings={} \
         address_index_findings={}",
        target.number,
        report.inventory_rows,
        report.compatibility_pairs,
        report.address_pages,
        report.address_entries,
        report.primary_tuples,
        report.differences.len(),
        report.node_claim_findings.len(),
        report.address_index_findings.len()
    );
    let unexpected: Vec<_> = report
        .differences
        .iter()
        .filter(|(key, _)| !expected(key))
        .collect();
    ensure!(
        unexpected.is_empty(),
        "family reads differ from today's reads at {}: {unexpected:#?}",
        target.number
    );
    Ok(report)
}

/// [`compare_family_reads_at`] with no expected difference.
pub async fn assert_family_reads_match(pool: &PgPool, target: &Marker) -> Result<ShadowReport> {
    compare_family_reads_at(pool, target, |_| false).await
}
