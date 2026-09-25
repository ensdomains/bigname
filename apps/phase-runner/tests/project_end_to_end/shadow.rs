//! Shadow comparison of the owned key family readers (TYR-36 step 4) at a publication: once the
//! families stand at the served marker, every record inventory row, every page of names resolving
//! to an indexed address and every reverse claim is read through today's readers and through the
//! family readers (`bigname_storage::families::records`) and compared field by field. Production
//! still serves today's tables; this is the only place the family readers run against a copy.
use std::time::Instant;

use anyhow::Result;
use bigname_storage::families::records::{ShadowReport, compare_family_reads};
use phase_runner::heads::BlockMarker;
use sqlx::PgPool;

/// What one comparison saw, for the test to require.
pub struct Shadow {
    pub target: i64,
    pub stage: &'static str,
    pub report: ShadowReport,
}

/// Compare at `target` after the families followed the publication. `stage` names the state
/// compared (the incremental blocks or the rebuild) in the printed line.
pub async fn compare(
    pool: &PgPool,
    chain_id: &str,
    target: &BlockMarker,
    page_size: u64,
    stage: &'static str,
) -> Result<Shadow> {
    let started = Instant::now();
    let report = compare_family_reads(
        pool,
        chain_id,
        Some((target.number, target.hash.clone())),
        page_size,
    )
    .await?;
    eprintln!(
        "SEPOLIA_END_TO_END_SHADOW target={} stage={stage} current={} inventory_rows={} \
         compatibility_pairs={} address_pages={} address_entries={} primary_tuples={} \
         differences={} node_claim_findings={} address_index_misses={} elapsed_ms={}",
        target.number,
        report.current(),
        report.inventory_rows,
        report.compatibility_pairs,
        report.address_pages,
        report.address_entries,
        report.primary_tuples,
        report.differences.len(),
        report.node_claim_findings.len(),
        report.address_index_misses.len(),
        started.elapsed().as_millis()
    );
    for (key, differences) in report.differences.iter().take(5) {
        eprintln!("SEPOLIA_END_TO_END_SHADOW_DIFFERENCE {key}: {differences:?}");
    }
    Ok(Shadow {
        target: target.number,
        stage,
        report,
    })
}
