use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{
    STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION, StoredLineageCoverageFrontierHeader,
};

pub(super) type Topic0sByFamily = BTreeMap<String, BTreeSet<String>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoveragePublicationPlan {
    pub(super) expected_snapshot_revision: Option<i64>,
    pub(super) verified_from_block: i64,
    pub(super) verified_through_block: i64,
    pub(super) topic_changed_source_families: Vec<String>,
    pub(super) reverify_all: bool,
}

pub(super) fn canonical_topic_sets(topics: &Topic0sByFamily) -> BTreeMap<String, Vec<String>> {
    topics
        .iter()
        .map(|(family, topics)| (family.clone(), topics.iter().cloned().collect()))
        .collect()
}

#[expect(
    clippy::too_many_arguments,
    reason = "publication planning keeps the persisted proof inputs explicit for auditability"
)]
pub(super) fn plan_publication(
    header: Option<&StoredLineageCoverageFrontierHeader>,
    persisted_requirements_valid: bool,
    current_topics: &BTreeMap<String, Vec<String>>,
    discovery_admission_epoch: i64,
    earliest_known_watched_block: Option<i64>,
    required_from_block: i64,
    required_through_block: i64,
    verify_ahead_through_block: i64,
    verification_chunk_blocks: i64,
) -> Result<Option<CoveragePublicationPlan>, String> {
    let verify_ahead_through_block = verify_ahead_through_block.max(required_through_block);
    if let Some(header) = header
        && header.proof_format_version != STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION
    {
        return Err(format!(
            "stored-lineage coverage frontier for chain {} uses unsupported proof format {}; this binary recognizes only {} and will not overwrite or downgrade the saved proof",
            header.chain_id,
            header.proof_format_version,
            STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION
        ));
    }

    let topic_changed_source_families = header
        .map(|header| changed_topic_families(&header.topic0s_by_family, current_topics))
        .unwrap_or_default();
    let deep_regression =
        header.is_some_and(|header| required_from_block < header.verified_from_block);
    let epoch_regression =
        header.is_some_and(|header| header.discovery_admission_epoch > discovery_admission_epoch);
    let cold_rebuild =
        header.is_none() || !persisted_requirements_valid || deep_regression || epoch_regression;

    if let Some(header) = header
        && !cold_rebuild
        && header.discovery_admission_epoch == discovery_admission_epoch
        && topic_changed_source_families.is_empty()
        && header.verified_from_block <= required_from_block
        && header.verified_through_block >= required_through_block
    {
        return Ok(None);
    }

    let verified_from_block = if cold_rebuild {
        earliest_known_watched_block.map_or(required_from_block, |earliest| {
            earliest.min(required_from_block)
        })
    } else {
        let header = header.expect("a non-cold publication must have a saved header");
        earliest_known_watched_block.map_or(header.verified_from_block, |earliest| {
            earliest.min(header.verified_from_block)
        })
    };
    let verified_through_block = match header {
        Some(header)
            if !cold_rebuild && required_through_block <= header.verified_through_block =>
        {
            header.verified_through_block
        }
        Some(header) if !cold_rebuild => header
            .verified_through_block
            .saturating_add(verification_chunk_blocks)
            .min(verify_ahead_through_block),
        _ => verified_from_block
            .saturating_add(verification_chunk_blocks.saturating_sub(1))
            .min(verify_ahead_through_block),
    };

    Ok(Some(CoveragePublicationPlan {
        expected_snapshot_revision: header.map(|header| header.snapshot_revision),
        verified_from_block,
        verified_through_block,
        topic_changed_source_families,
        reverify_all: cold_rebuild,
    }))
}

fn changed_topic_families(
    previous: &BTreeMap<String, Vec<String>>,
    current: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    previous
        .keys()
        .chain(current.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|family| previous.get(*family) != current.get(*family))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    fn header(from: i64, through: i64) -> StoredLineageCoverageFrontierHeader {
        StoredLineageCoverageFrontierHeader {
            chain_id: "test-chain".to_owned(),
            snapshot_revision: 4,
            proof_format_version: STORED_LINEAGE_COVERAGE_PROOF_FORMAT_VERSION.to_owned(),
            discovery_admission_epoch: 2,
            verified_from_block: from,
            verified_through_block: through,
            topic0s_by_family: BTreeMap::from([(
                "family".to_owned(),
                vec![format!("0x{:064x}", 1)],
            )]),
            requirement_row_count: 1,
            requirement_digest: "00000000000000000000000000000000".to_owned(),
            updated_at: OffsetDateTime::UNIX_EPOCH,
            is_well_formed: true,
        }
    }

    #[test]
    fn unchanged_eligible_header_is_reused() {
        let header = header(10, 100);
        assert_eq!(
            plan_publication(
                Some(&header),
                true,
                &header.topic0s_by_family,
                2,
                Some(10),
                50,
                60,
                100,
                32,
            )
            .expect("current proof must plan"),
            None
        );
    }

    #[test]
    fn deep_regression_forces_cold_full_candidate() {
        let header = header(50, 100);
        let plan = plan_publication(
            Some(&header),
            true,
            &header.topic0s_by_family,
            2,
            Some(10),
            20,
            30,
            100,
            32,
        )
        .expect("deep regression must plan")
        .expect("deep regression must publish");
        assert!(plan.reverify_all);
        assert_eq!(plan.verified_from_block, 10);
        assert_eq!(plan.expected_snapshot_revision, Some(4));
    }

    #[test]
    fn malformed_child_integrity_forces_cold_full_candidate() {
        let header = header(10, 100);
        let plan = plan_publication(
            Some(&header),
            false,
            &header.topic0s_by_family,
            2,
            Some(10),
            50,
            60,
            100,
            32,
        )
        .expect("malformed child integrity must plan")
        .expect("malformed child integrity must publish");
        assert!(plan.reverify_all);
        assert_eq!(plan.expected_snapshot_revision, Some(4));
    }

    #[test]
    fn unknown_proof_format_is_never_overwritten() {
        let mut header = header(10, 100);
        header.proof_format_version = "stored_lineage_coverage_v2".to_owned();
        let error = plan_publication(
            Some(&header),
            true,
            &header.topic0s_by_family,
            2,
            Some(10),
            20,
            30,
            100,
            32,
        )
        .expect_err("unknown format must hard-refuse");
        assert!(error.contains("will not overwrite or downgrade"));
    }
}
