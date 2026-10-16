//! The redo that discharges a [manifest-authority marker](../../../docs/glossary.md#manifest-authority-marker),
//! bounded to the manifest change's first effect where that is sound (docs/deployment.md §
//! Manifest-authority marker). The change is read as a diff: the manifest set the owned key
//! families were last published with (the family marker's set key) against the latest
//! SourceManifestUpdated event of every manifest now. The bounded redo applies only when the
//! families were published under this binary's interpreter content hash, and it never applies to
//! a change of an ENSv2 manifest. Otherwise the redo is the full range, as before.
use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    redo_first_effect::{FirstEffect, ManifestDiff, Side},
    redo_hash_adoption::{full_hash_range, require_full_hash_redo},
    redo_manifest_attestation::has_marker_prefix,
    redo_manifest_audit::ManifestAuthorityAttestationAudit,
    transitions::PhaseStateRow,
};

/// Families whose stored rows are composed from the publication's manifest set by event, below
/// any first effect (`crates/storage/src/families/control/permissions/registry_support.rs`), or
/// read from the live manifest tables (`crates/storage/src/families/control/lifecycle/policy.rs`).
/// A root change is also an admission change, whose sweep part 4 of TYR-356 makes resumable.
const WHOLE_REDO_FAMILIES: [&str; 3] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_migration_l1",
];

/// Payload fields Interpret classifies events against at every block, not from a declaration's
/// start: the Universal Resolver implementations an `Upgraded` log is classified against
/// (`crates/adapters/src/schema_v2/protocol/universal_resolver.rs`, `implementation_kind`) and
/// the correlation addresses adapters read (`crates/adapters/src/schema_v2/catalog.rs`,
/// `correlation_address`). The first event that reads them can lie below every declared start,
/// and only a pass over interpreted event payloads would find it.
const EVERY_BLOCK_FIELDS: [&str; 2] = [
    "universal_resolver_implementations",
    "correlation_addresses",
];

/// Whether the redo that discharges a marker can be bounded, and the change it bounds to.
pub(crate) enum Plan {
    Bounded {
        diff: ManifestDiff,
        /// The families were written under this binary's projection content hash, so the Project
        /// redo can start at the first effect too.
        project: bool,
    },
    Full(String),
}

type EventRow = (i64, i64, String, String, String, Option<Value>);

const MANIFEST_EVENT_COLUMNS: &str = "event.source_manifest_id, event.normalized_event_id,
        event.chain_id, event.namespace, event.source_family, event.after_state";

/// The latest readable SourceManifestUpdated event of every manifest the chain's families read,
/// the selection `crates/project/src/families/manifests.rs` (`History::read`, `History::at`)
/// makes for a block. Its set key equals the family marker's `admission_manifests` after a
/// publication at `$2` (`tests/bounded_adoption.rs`).
const CURRENT_SET: &str = "SELECT DISTINCT ON (event.source_manifest_id) {COLUMNS}
    FROM normalized_events event
    LEFT JOIN chain_lineage lineage
      ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
     AND lineage.block_number = event.block_number
    WHERE (event.chain_id = $1
           OR ($1 = 'base-mainnet' AND event.namespace = 'basenames'
               AND event.source_family = 'basenames_execution'
               AND event.chain_id = 'ethereum-mainnet'))
      AND event.event_kind = 'SourceManifestUpdated'
      AND event.source_manifest_id IS NOT NULL
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (event.block_hash IS NULL
           OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
      AND (event.block_number IS NULL OR event.block_number <= $2)
    ORDER BY event.source_manifest_id, event.normalized_event_id DESC";

/// Decide the redo of chain `chain_id`'s marker against the head `through`. Reads the family
/// marker and the manifest events only: the first-effect scan runs separately.
pub(crate) async fn plan(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    through: i64,
) -> RunnerResult<Plan> {
    let database =
        |error: sqlx::Error| RunnerError::database("failed to plan a bounded manifest redo", error);
    let marker: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT admission_manifests, interpret_input_content_hash, input_content_hash
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database)?;
    let Some((Some(published), interpreter, projection)) = marker else {
        return Ok(Plan::Full(
            "the family marker records no published manifest set".to_owned(),
        ));
    };
    if interpreter.as_deref() != Some(bigname_content_hash::INTERPRETER_CONTENT_HASH) {
        return Ok(Plan::Full(format!(
            "the owned key families were last published under interpreter content hash {}, not \
             this binary's",
            interpreter.as_deref().unwrap_or("<none>")
        )));
    }
    let current: Vec<EventRow> =
        sqlx::query_as(&CURRENT_SET.replace("{COLUMNS}", MANIFEST_EVENT_COLUMNS))
            .bind(chain_id)
            .bind(through)
            .fetch_all(&mut **transaction)
            .await
            .map_err(database)?;
    let key = current
        .iter()
        .map(|row| format!("{}:{}", row.0, row.1))
        .collect::<Vec<_>>()
        .join(",");
    if key == published {
        return Ok(Plan::Full(
            "the marker records no change of the published manifest set, as when manifest sync \
             repaired an address floor"
                .to_owned(),
        ));
    }
    let baseline_ids: Vec<i64> = published
        .split(',')
        .filter_map(|pair| pair.split_once(':')?.1.parse().ok())
        .collect();
    let baseline: Vec<EventRow> = sqlx::query_as(&format!(
        "SELECT {MANIFEST_EVENT_COLUMNS} FROM normalized_events event
         WHERE event.normalized_event_id = ANY($1)"
    ))
    .bind(&baseline_ids)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database)?;
    if baseline.len() != baseline_ids.len() {
        return Ok(Plan::Full(
            "an event of the published manifest set is missing".to_owned(),
        ));
    }
    let mut manifests: BTreeMap<i64, (Option<&EventRow>, Option<&EventRow>)> = BTreeMap::new();
    for row in &baseline {
        manifests.entry(row.0).or_default().0 = Some(row);
    }
    for row in &current {
        manifests.entry(row.0).or_default().1 = Some(row);
    }
    let mut diff = ManifestDiff::default();
    for (manifest_id, (before, after)) in manifests {
        let (old, new) = (side(before), side(after));
        if old.as_ref().map(|side| (side.rollout_status, side.payload))
            == new.as_ref().map(|side| (side.rollout_status, side.payload))
        {
            continue;
        }
        if let Some(row) = before
            .into_iter()
            .chain(after)
            .find(|row| WHOLE_REDO_FAMILIES.contains(&row.4.as_str()))
        {
            return Ok(Plan::Full(format!(
                "manifest {manifest_id} of family {} changed, and stored rows of every name read \
                 that family's declarations",
                row.4
            )));
        }
        if let (Some(before), Some(after)) = (before, after)
            && before.3 != after.3
        {
            return Ok(Plan::Full(format!(
                "manifest {manifest_id} changed its declaration namespace"
            )));
        }
        // A manifest with no state on one side reads as one without the field there.
        let field_of = |side: &Option<Side<'_>>, field: &str| {
            side.as_ref()
                .map_or(&Value::Null, |side| &side.payload[field])
                .clone()
        };
        if let Some(field) = EVERY_BLOCK_FIELDS
            .into_iter()
            .find(|field| field_of(&old, field) != field_of(&new, field))
        {
            return Ok(Plan::Full(format!(
                "manifest {manifest_id} changed `{field}`, which Interpret reads at every block"
            )));
        }
        // Base Project reads the Ethereum Mainnet `basenames_execution` manifest at every block
        // and has no Interpret redo of its own to bound (docs/manifests.md § Discovery
        // admission).
        if let Some(row) = before.or(after).filter(|row| row.2 != chain_id) {
            return Ok(Plan::Full(format!(
                "manifest {manifest_id} is on chain {}, whose change this chain's Project reads \
                 at every block",
                row.2
            )));
        }
        diff.add(old.as_ref(), new.as_ref());
    }
    Ok(Plan::Bounded {
        diff,
        project: projection.as_deref() == Some(bigname_content_hash::PROJECTION_CONTENT_HASH),
    })
}

/// One side of a manifest's change: its stored state, when the manifest has one there.
fn side(row: Option<&EventRow>) -> Option<Side<'_>> {
    row.and_then(|row| row.5.as_ref()).map(|state| Side {
        rollout_status: state["rollout_status"].as_str(),
        payload: &state["manifest_payload"],
    })
}

/// Where a bounded discharge starts, and for Interpret the first effect that put it there.
pub(crate) struct BoundedStart {
    pub(crate) from: i64,
    pub(crate) effect: Option<FirstEffect>,
}

/// Check the range of a redo that adopts this binary's content hash for `phase`. A redo that
/// discharges a marker the bounded redo covers may start at or below the first effect, and runs
/// from there. Every other redo must be the full range, as `require_full_hash_redo` says.
pub(crate) async fn require_adoption_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
    previous: &PhaseStateRow,
    adopts_new_hash: bool,
) -> RunnerResult<Option<BoundedStart>> {
    if !adopts_new_hash {
        return Ok(None);
    }
    let head = previous.current_block_number;
    let marker = previous
        .input_content_hash
        .as_deref()
        .filter(|hash| has_marker_prefix(hash));
    let (Some(marker), Some(head), Some(range)) = (marker, head, mode.range()) else {
        return full(transaction, chain_id, phase, mode, previous, None).await;
    };
    let plan = plan(transaction, chain_id, head).await?;
    let Some(full_range) = full_hash_range(
        transaction,
        chain_id,
        phase,
        Some(head),
        previous.redo_in_progress,
    )
    .await?
    else {
        return full(transaction, chain_id, phase, mode, previous, None).await;
    };
    let pending = previous
        .redo_in_progress
        .then(|| previous.requested_redo_range().map(|stamped| stamped.from))
        .flatten();
    let (from, effect) = match (phase, plan) {
        (PhaseName::Interpret, Plan::Bounded { diff, .. }) => {
            let effect = crate::redo_first_effect::scan(transaction, chain_id, &diff, head).await?;
            (effect.block().unwrap_or(head), Some(effect))
        }
        (PhaseName::Project, Plan::Bounded { project: true, .. }) => {
            let token = marker.rsplit_once(':').map_or("", |(_, token)| token);
            match crate::redo_manifest_audit::attested_range(transaction, chain_id, token).await? {
                Some(attested) => (attested.from.min(head), None),
                None => {
                    let reason = "no attested Interpret redo discharged this marker".to_owned();
                    return full(transaction, chain_id, phase, mode, previous, Some(reason)).await;
                }
            }
        }
        (_, Plan::Bounded { .. }) => {
            let reason = "the owned key families were written under another projection content \
                          hash"
                .to_owned();
            return full(transaction, chain_id, phase, mode, previous, Some(reason)).await;
        }
        (_, Plan::Full(reason)) => {
            return full(transaction, chain_id, phase, mode, previous, Some(reason)).await;
        }
    };
    let from = pending
        .map_or(from, |pending| pending.min(from))
        .max(full_range.from);
    if let Some(effect) = effect {
        tracing::info!(
            chain_id,
            phase = %phase,
            watched_log = effect.watched_log,
            emitted_log = effect.emitted_log,
            edge = effect.edge,
            declared_start = effect.declared_start,
            redo_from_block = from,
            "bounded the manifest-authority redo to the manifest change's first effect"
        );
    }
    if range.from > from {
        // Release tooling parses "redo the full range A..=B". The full range runs from `from`.
        return Err(RunnerError::new(
            ErrorKind::ContentHashMismatch,
            format!(
                "cannot adopt a new interpretation-input hash for chain {chain_id} phase {phase} \
                 with range {}..={}; redo the full range {}..={}, which runs from block {from}, \
                 the manifest change's first effect",
                range.from, range.to, full_range.from, full_range.to
            ),
        ));
    }
    Ok(Some(BoundedStart { from, effect }))
}

async fn full(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
    previous: &PhaseStateRow,
    reason: Option<String>,
) -> RunnerResult<Option<BoundedStart>> {
    if let Some(reason) = &reason {
        tracing::info!(chain_id, phase = %phase, reason, "the manifest-authority redo is the full range");
    }
    require_full_hash_redo(
        transaction,
        chain_id,
        phase,
        mode,
        previous.current_block_number,
        previous.redo_in_progress,
        reason.as_deref(),
    )
    .await
    .map(|()| None)
}

/// The start of the Project redo a completed bounded Interpret redo stamps: the Interpret redo's
/// attested start, or Project's head below it. `None` when the Project redo must be the full
/// range: its marker is not the one the Interpret redo discharged, or the plan is not bounded
/// for Project.
pub(crate) async fn project_stamp_from(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    project_hash: Option<&str>,
    project_head: i64,
    audit: Option<&ManifestAuthorityAttestationAudit>,
) -> RunnerResult<Option<i64>> {
    let (Some(marker), Some(audit)) = (project_hash.filter(|hash| has_marker_prefix(hash)), audit)
    else {
        return Ok(None);
    };
    if marker.rsplit_once(':').map(|(_, token)| token) != Some(audit.generation_token()) {
        return Ok(None);
    }
    Ok(match plan(transaction, chain_id, project_head).await? {
        Plan::Bounded { project: true, .. } => Some(audit.range().from.min(project_head)),
        _ => None,
    })
}

/// A Project redo bounded to `bounded` runs from its start, whatever start was requested.
pub(crate) fn project_range(
    phase: PhaseName,
    bounded: Option<&BoundedStart>,
    requested: BlockRange,
) -> RunnerResult<BlockRange> {
    match bounded {
        Some(start) if phase == PhaseName::Project => {
            BlockRange::new(start.from, requested.to.max(start.from))
        }
        _ => Ok(requested),
    }
}

/// The range an Interpret redo runs: from the bounded start, or from the audited start of the
/// attempt it resumes, or from the requested start, through the recorded interpreted head.
pub(crate) async fn interpret_execution_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    previous: &PhaseStateRow,
    requested: BlockRange,
    bounded: Option<&BoundedStart>,
    token: Option<&str>,
) -> RunnerResult<BlockRange> {
    let from = match bounded {
        Some(start) => start.from,
        None => resumed_interpret_from(transaction, chain_id, previous, requested, token)
            .await?
            .unwrap_or(requested.from),
    };
    crate::redo_presence::interpret_replay_range(
        previous,
        BlockRange::new(from, requested.to.max(from))?,
    )
}

/// The start of a resumed Interpret redo that an earlier attempt began with `token` and its
/// audited range: the redo resumes that range, not a recomputed one.
async fn resumed_interpret_from(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    previous: &PhaseStateRow,
    requested: BlockRange,
    token: Option<&str>,
) -> RunnerResult<Option<i64>> {
    let (Some(token), true) = (token, previous.redo_in_progress) else {
        return Ok(None);
    };
    if previous
        .input_content_hash
        .as_deref()
        .is_some_and(has_marker_prefix)
    {
        return Ok(None);
    }
    Ok(
        crate::redo_manifest_audit::pending_for_restart_locked(transaction, chain_id, token)
            .await?
            .map(|audit| audit.range().from)
            .filter(|from| requested.from <= *from),
    )
}
