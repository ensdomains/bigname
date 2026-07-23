use super::*;

pub(super) async fn record_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> std::result::Result<(), String> {
    if let Some(progress) = progress.as_deref_mut() {
        progress
            .record(pool)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(super) async fn verify_requirements(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
) -> std::result::Result<(), String> {
    if requirements.is_empty() {
        return Ok(());
    }
    super::super::topic_drift::ensure_required_topic_sets_undrifted_in_transaction(
        connection,
        chain,
        requirements,
    )
    .await?;
    let violations = find_uncovered_required_watched_tuples_in_transaction(
        connection,
        chain,
        requirements,
        MAX_REPORTED_UNCOVERED_TUPLES,
    )
    .await
    .map_err(|error| error.to_string())?;
    if violations.is_empty() {
        return Ok(());
    }
    let from_block = requirements
        .iter()
        .map(|requirement| requirement.required_from_block)
        .min()
        .expect("non-empty requirements must have a lower bound");
    let through_block = requirements
        .iter()
        .map(|requirement| requirement.required_to_block)
        .max()
        .expect("non-empty requirements must have an upper bound");
    Err(uncovered_tuples_refusal(
        from_block,
        through_block,
        &violations,
    ))
}

pub(super) fn uncovered_tuples_refusal(
    from_block: i64,
    through_block: i64,
    violations: &[UncoveredWatchedTuple],
) -> String {
    let listed = violations
        .iter()
        .map(|tuple| {
            format!(
                "(source_family {}, address {}, blocks {}..={})",
                tuple.source_family,
                tuple.address,
                tuple.required_from_block,
                tuple.required_to_block
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if violations.len() as i64 >= MAX_REPORTED_UNCOVERED_TUPLES {
        " (further violations elided)"
    } else {
        ""
    };
    format!(
        "watched tuples over blocks {from_block}..={through_block} do not form gap-free coverage from exact address- or family-scoped backfill_coverage_facts: {listed}{suffix}; run hash-pinned or Coinbase SQL backfill for the missing tuple intervals (or repair derive-backfill-coverage-facts for legacy full-payload jobs) and retry"
    )
}

/// Current manifest topic selectors per log-producing family. The frontier
/// stores them per family so a semantic change invalidates only that family's
/// tuple proofs.
pub(super) async fn load_current_topic0s_by_family(
    pool: &sqlx::PgPool,
    chain: &str,
    log_producing_source_families: &[String],
) -> std::result::Result<BTreeMap<String, BTreeSet<String>>, String> {
    if log_producing_source_families.is_empty() {
        return Ok(BTreeMap::new());
    }
    let events = load_active_manifest_abi_events_by_chain_and_source_families(
        pool,
        chain,
        log_producing_source_families,
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut current_topic0s_by_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        let Some(topic0) = event.topic0 else {
            continue;
        };
        current_topic0s_by_family
            .entry(event.source_family)
            .or_default()
            .insert(topic0.to_ascii_lowercase());
    }
    Ok(current_topic0s_by_family)
}

pub(super) fn path_start_number(path: &[ChainLineageBlock]) -> i64 {
    path.first()
        .expect("stored lineage path must not be empty")
        .block_number
}

pub(super) fn path_end_number(path: &[ChainLineageBlock]) -> i64 {
    path.last()
        .expect("stored lineage path must not be empty")
        .block_number
}
