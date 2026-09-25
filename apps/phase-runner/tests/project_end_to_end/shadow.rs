//! Shadow comparison of the owned key family readers (TYR-36 step 4) at a publication: once the
//! families stand at the served marker, every record inventory row, every page of names resolving
//! to an indexed address and every reverse claim is read through today's readers and through the
//! family readers (`bigname_storage::families::records`) and compared field by field. Production
//! still serves today's tables; this is the only place the family readers run against a copy.
use std::time::Instant;

use anyhow::{Result, ensure};
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
         differences={} node_claims_at_other_resolver={} address_index_misses={} \
         classification_fallbacks={} elapsed_ms={}",
        target.number,
        report.current(),
        report.inventory_rows,
        report.compatibility_pairs,
        report.address_pages,
        report.address_entries,
        report.primary_tuples,
        report.differences.len(),
        report.node_claims_at_other_resolver.len(),
        report.address_index_misses.len(),
        report.classification_fallbacks.len(),
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

/// Require every comparison to have run and to show no difference and no diagnostic. The fixture
/// and the disposable-copy runs both call it, so a copy whose family reads diverge fails.
pub fn require_clean(shadows: &[Shadow]) -> Result<()> {
    ensure!(!shadows.is_empty(), "no shadow comparison ran");
    for shadow in shadows {
        let report = &shadow.report;
        ensure!(
            report.current() && report.inventory_rows > 0 && report.primary_tuples > 0,
            "the {} shadow comparison at {} compared nothing: {report:?}",
            shadow.stage,
            shadow.target
        );
        ensure!(
            report.differences.is_empty(),
            "the family reads differ from today's reads at {} ({}): {:#?}",
            shadow.target,
            shadow.stage,
            report.differences
        );
        ensure!(
            report.node_claims_at_other_resolver.is_empty()
                && report.address_index_misses.is_empty()
                && report.classification_fallbacks.is_empty(),
            "step 2 gaps at {} ({}): {:#?} {:#?} {:#?}",
            shadow.target,
            shadow.stage,
            report.node_claims_at_other_resolver,
            report.address_index_misses,
            report.classification_fallbacks
        );
    }
    Ok(())
}

#[test]
fn a_difference_or_a_diagnostic_fails_the_run() {
    let marker = Some((5, "0x05".to_owned()));
    let clean = || Shadow {
        target: 5,
        stage: "incremental",
        report: ShadowReport {
            family_marker: marker.clone(),
            served_marker: marker.clone(),
            inventory_rows: 1,
            primary_tuples: 1,
            ..ShadowReport::default()
        },
    };
    assert!(require_clean(&[clean()]).is_ok());
    assert!(require_clean(&[]).is_err());
    let mut differing = clean();
    differing.report.differences.push((
        "record_inventory x".to_owned(),
        vec![bigname_storage::families::records::Difference {
            field: "entries".to_owned(),
            today: None,
            family: None,
        }],
    ));
    assert!(require_clean(&[clean(), differing]).is_err());
    let mut lagging = clean();
    lagging.report.family_marker = Some((4, "0x04".to_owned()));
    assert!(require_clean(&[lagging]).is_err());
    let mut gap = clean();
    gap.report
        .classification_fallbacks
        .push("resolver 0x01".to_owned());
    assert!(require_clean(&[gap]).is_err());
}
