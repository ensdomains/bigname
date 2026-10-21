//! Enrichments read once per lookup batch across name results and reverse rows. With a served
//! head they read on the batch's read snapshot.
use std::collections::BTreeSet;

use bigname_storage::ReadDb;

use super::LookupResult;
use crate::v2::{Authority, V2Result};

/// Fills `migrated_at` on every rendered record whose current authority is the ENSv2 arm, in one
/// batch across name results and reverse rows.
pub(super) async fn apply_migrated_at(
    db: ReadDb<'_>,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let mut records = results
        .iter_mut()
        .flatten()
        .flat_map(|result| {
            result
                .record
                .iter_mut()
                .chain(result.records.iter_mut().flatten())
        })
        .filter(|record| record.authority == Some(Authority::EnsV2))
        .map(|record| {
            // The served name can spell a label as its bracketed hash.
            let logical_name_id =
                crate::v2::support::route_logical_name_id(&record.namespace, &record.name);
            (logical_name_id, record)
        })
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(());
    }
    let logical_name_ids = records
        .iter()
        .map(|(logical_name_id, _)| logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let migrated_at = crate::v2::name_record::load_migrated_at(db, &logical_name_ids).await?;
    for (logical_name_id, record) in &mut records {
        record.migrated_at = migrated_at.get(logical_name_id).cloned();
    }
    Ok(())
}

/// Fills the ABI key list on every rendered `records` group, in one batched read across name
/// results and reverse rows (`record_groups.rs`).
pub(super) async fn apply_record_abis(
    db: ReadDb<'_>,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let targets = results
        .iter_mut()
        .flatten()
        .flat_map(|result| {
            result
                .record
                .iter_mut()
                .chain(result.records.iter_mut().flatten())
        })
        .filter_map(|record| {
            let source = record.abi_source.as_ref();
            record.records.as_mut().map(|groups| (groups, source))
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Ok(());
    }
    crate::v2::record_groups::fill_abi_content_types(db, targets).await
}

/// Reads the wrapper expiry of every rendered `ens_v1` once per chain across name results and
/// reverse rows.
pub(super) async fn apply_wrapper_expiries(
    db: ReadDb<'_>,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let mut objects = Vec::new();
    for result in results.iter_mut().flatten() {
        for record in result
            .record
            .iter_mut()
            .chain(result.records.iter_mut().flatten())
        {
            if let Some(ens_v1) = record.ens_v1.as_mut() {
                objects.push(ens_v1);
            }
        }
    }
    crate::v2::name_record::fill_wrapper_expiries(db, objects).await
}
