use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_domain::resolver_read::ResolverReadFeature;
use bigname_storage::{
    BASENAMES_NAMESPACE, NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot,
    SnapshotSelectionErrorKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AppState;
use crate::v2::support::{
    ResolutionLookupError, ResolutionRecordKey, load_name_current_for_selected_snapshot,
    load_records_route_inventory, map_internal_api_error, normalize_inferred_route_name,
    snapshot_selection_api_error,
};

use super::support::{ResolutionLookupOutcome, execute_resolution_lookup};

use super::{
    AtSelector, Envelope, Finality, MAX_PAGE_SIZE, QueryParamAllowlist, RequestSource, Resolver,
    SnapshotReadResource, Source, Status, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2_for_resource, default_requested_records,
    name_records_inventory::RecordInventory, resolve_v2_snapshot_for, snapshot_meta,
    v2_exact_name_snapshot_scope_with_resolution_auxiliary,
};

mod build;
mod keys;
pub(crate) use build::{
    EXACT_NAME_AUTHORITY_NOT_VERIFIABLE, VERIFIED_NOT_SUPPORTED_REASON,
    build_authority_unsupported_name_records, build_auto_name_records, build_indexed_name_records,
    build_verified_name_records, ens_universal_resolver_discovery_candidate,
    indexed_records_requiring_verified_fallback,
};
pub(crate) use keys::{RecordSelection, parse_record_keys};

pub(crate) const MAX_RECORD_KEYS: usize = MAX_PAGE_SIZE as usize;
const VERIFIED_ANSWER_STALE_FOR_SNAPSHOT_REASON: &str = "verified_answer_stale_for_snapshot";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[cfg(test)]
pub(crate) mod auto_fallback_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use super::{V2Error, V2Result};

    #[derive(Clone)]
    pub(crate) struct AutoFallbackHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct AutoFallbackControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl AutoFallbackControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, AutoFallbackHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, AutoFallbackHook>,
        AutoFallbackControl,
    )> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            AutoFallbackHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, AutoFallbackControl { reached, resume }))
    }

    pub(super) async fn run(pool: &PgPool) -> V2Result<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run auto-fallback test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

pub(crate) struct NameRecordsQueryParams;

impl QueryParamAllowlist for NameRecordsQueryParams {
    const ALLOWED: &'static [&'static str] =
        &["namespace", "at", "finality", "source", "keys", "include"];
}

pub(crate) type NameRecordsQuery = StrictQueryParams<NameRecordsQueryParams>;

/// The records route's response data. `records` is its only value shape: one answer per
/// requested key, or per inventory-derived default key when `keys` is omitted, serialized in
/// record-key byte order. The flat value maps live on name detail and lookup only.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecords {
    pub(crate) namespace: String,
    pub(crate) resolver: Option<Resolver>,
    pub(crate) records: BTreeMap<String, RecordAnswer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) inventory: Option<RecordInventory>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordAnswer {
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) meta: Option<RecordAnswerMeta>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordAnswerMeta {
    pub(crate) basis: String,
    pub(crate) rule: ResolverReadFeature,
    pub(crate) source_record_key: String,
}

pub(crate) enum VerifiedRecordLookup {
    Found {
        response: Box<bigname_lookup::LookupResponse>,
    },
    Stale(String),
    NotSupported,
    /// The name's selected authority arm is not admitted by this profile's execution
    /// declaration; every requested key reports `exact_name_authority_not_verifiable`.
    AuthorityArmNotAdmitted,
}

pub(crate) async fn get_name_records(
    Path(input_name): Path<String>,
    params: NameRecordsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecords>>> {
    let params = params.into_inner();
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let explicit_records = parse_record_keys(params.keys.as_deref())?;
    let include_inventory = records_include_inventory(&params.include)?;

    let include_resolution_auxiliary =
        namespace == BASENAMES_NAMESPACE && params.source == RequestSource::Verified;
    let (mut selected_snapshot, mut row, mut record_inventory) = load_name_records_snapshot_state(
        &state,
        &namespace,
        &normalized.normalized_name,
        params.at.as_ref(),
        params.finality,
        include_resolution_auxiliary,
    )
    .await?;

    // Without `keys`, every source answers the inventory-derived default set. Inventory is loaded
    // only for a row the name may serve, so reservation and audit-only rows derive no keys. The
    // limit is checked before any lookup can run.
    let default_records;
    let selection = match explicit_records.as_deref() {
        Some(records) => RecordSelection::requested(records),
        None => {
            default_records = default_requested_records(record_inventory.as_ref());
            ensure_default_record_limit(&default_records)?;
            RecordSelection::inventory_default(&default_records)
        }
    };
    let requested_records = selection.records;

    let authority_unsupported = build_authority_unsupported_name_records(
        &row,
        record_inventory.as_ref(),
        selection,
        include_inventory,
    )?;
    let (route_source, data) = if let Some(data) = authority_unsupported {
        let source = match params.source {
            RequestSource::Verified => Source::Verified,
            RequestSource::Indexed | RequestSource::Auto => Source::Indexed,
        };
        (source, data)
    } else {
        match params.source {
            RequestSource::Indexed => (
                Source::Indexed,
                build_indexed_name_records(
                    &row,
                    record_inventory.as_ref(),
                    selection,
                    include_inventory,
                    false,
                )?,
            ),
            RequestSource::Verified => {
                let admit_null_resolver_discovery =
                    ens_universal_resolver_discovery_candidate(&row);
                #[cfg(test)]
                auto_fallback_test_hooks::run(&state.pool).await?;
                let verified_lookup = load_verified_record_lookup(
                    &state,
                    &row,
                    requested_records,
                    &mut selected_snapshot,
                )
                .await?;
                ensure_verified_route_matches_admission(
                    &verified_lookup,
                    admit_null_resolver_discovery,
                )?;
                (
                    Source::Verified,
                    build_verified_name_records(
                        &row,
                        record_inventory.as_ref(),
                        selection,
                        verified_lookup,
                        include_inventory,
                        false,
                    )?,
                )
            }
            RequestSource::Auto => {
                let records = requested_records;
                if !selection.explicit {
                    // An unkeyed auto read stays indexed over the default set: the default keys
                    // are not a caller selection and never enter verified fallback.
                    (
                        Source::Indexed,
                        build_indexed_name_records(
                            &row,
                            record_inventory.as_ref(),
                            selection,
                            include_inventory,
                            false,
                        )?,
                    )
                } else {
                    let admit_null_resolver_discovery =
                        ens_universal_resolver_discovery_candidate(&row);
                    let mut fallback_records = indexed_records_requiring_verified_fallback(
                        &row,
                        record_inventory.as_ref(),
                        records,
                        admit_null_resolver_discovery,
                    )?;
                    #[cfg(test)]
                    if !fallback_records.is_empty() {
                        auto_fallback_test_hooks::run(&state.pool).await?;
                    }
                    if namespace == BASENAMES_NAMESPACE && !fallback_records.is_empty() {
                        (selected_snapshot, row, record_inventory) =
                            load_name_records_snapshot_state(
                                &state,
                                &namespace,
                                &normalized.normalized_name,
                                params.at.as_ref(),
                                params.finality,
                                true,
                            )
                            .await?;
                        let refreshed_fallback_records =
                            indexed_records_requiring_verified_fallback(
                                &row,
                                record_inventory.as_ref(),
                                records,
                                false,
                            )?;
                        if refreshed_fallback_records.is_empty()
                            || build_authority_unsupported_name_records(
                                &row,
                                record_inventory.as_ref(),
                                selection,
                                include_inventory,
                            )?
                            .is_some()
                        {
                            return Err(V2Error::stale(
                                "name records changed while preparing verified fallback; retry the request",
                            ));
                        }
                        fallback_records = refreshed_fallback_records;
                    }
                    let verified_lookup = load_verified_record_lookup(
                        &state,
                        &row,
                        &fallback_records,
                        &mut selected_snapshot,
                    )
                    .await?;
                    ensure_verified_route_matches_admission(
                        &verified_lookup,
                        admit_null_resolver_discovery,
                    )?;
                    build_auto_name_records(
                        &row,
                        record_inventory.as_ref(),
                        records,
                        verified_lookup,
                        include_inventory,
                        admit_null_resolver_discovery,
                    )?
                }
            }
        }
    };

    let mut meta = snapshot_meta(&selected_snapshot)?;
    meta.source = Some(route_source);

    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

fn ensure_verified_route_matches_admission(
    verified_lookup: &Option<VerifiedRecordLookup>,
    admit_null_resolver_discovery: bool,
) -> V2Result<()> {
    if let Some(VerifiedRecordLookup::Found { response }) = verified_lookup {
        ensure_executed_route_matches_admission(
            &response.resolver_address,
            admit_null_resolver_discovery,
        )?;
    }
    Ok(())
}

fn ensure_executed_route_matches_admission(
    resolver_address: &str,
    admit_null_resolver_discovery: bool,
) -> V2Result<()> {
    let executed_null_resolver_discovery = resolver_address.eq_ignore_ascii_case(ZERO_ADDRESS);
    if admit_null_resolver_discovery != executed_null_resolver_discovery {
        return Err(V2Error::stale(
            "name records changed while preparing verified fallback; retry the request",
        ));
    }
    Ok(())
}

async fn load_name_records_snapshot_state(
    state: &AppState,
    namespace: &str,
    normalized_name: &str,
    at: Option<&AtSelector>,
    finality: Finality,
    include_resolution_auxiliary: bool,
) -> V2Result<(
    SelectedSnapshot,
    NameCurrentRow,
    Option<RecordInventoryCurrentRow>,
)> {
    let scope = v2_exact_name_snapshot_scope_with_resolution_auxiliary(
        state,
        namespace,
        at,
        include_resolution_auxiliary,
    )
    .await?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        at,
        finality,
        SnapshotReadResource::NameRecords,
    )
    .await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        namespace,
        normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        api_error_to_v2_for_resource(
            map_internal_api_error(
                error,
                format!(
                    "failed to load name records for {}/{}",
                    namespace, normalized_name
                ),
            ),
            SnapshotReadResource::NameRecords,
        )
    })?;

    let record_inventory = if super::name_record::row_has_current_registration(&row) {
        load_records_route_inventory(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(|error| {
                api_error_to_v2_for_resource(
                    snapshot_selection_api_error(error),
                    SnapshotReadResource::NameRecords,
                )
            })?
    } else {
        None
    };
    Ok((selected_snapshot, row, record_inventory))
}

/// Refuse an inventory-derived key set above the record-key limit. The set is never truncated;
/// callers narrow it with explicit `keys` (more than 200 explicit keys is a request error).
pub(crate) fn ensure_default_record_limit(records: &[ResolutionRecordKey]) -> V2Result<()> {
    if records.len() > MAX_RECORD_KEYS {
        return Err(V2Error::unsupported(format!(
            "inventory-derived record key sets support at most {MAX_RECORD_KEYS} record keys"
        )));
    }
    Ok(())
}

pub(crate) async fn load_verified_record_lookup(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
) -> V2Result<Option<VerifiedRecordLookup>> {
    load_verified_record_lookup_for_resource(
        state,
        row,
        records,
        selected_snapshot,
        SnapshotReadResource::NameRecords,
    )
    .await
}

pub(crate) async fn load_verified_record_lookup_for_resource(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
    resource: SnapshotReadResource,
) -> V2Result<Option<VerifiedRecordLookup>> {
    if !super::name_record::row_has_current_registration(row) {
        return Ok(Some(VerifiedRecordLookup::NotSupported));
    }
    execute_verified_record_lookup(state, row, records, selected_snapshot, resource).await
}

pub(crate) async fn load_ephemeral_verified_record_lookup(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
) -> V2Result<Option<VerifiedRecordLookup>> {
    execute_verified_record_lookup(
        state,
        row,
        records,
        selected_snapshot,
        SnapshotReadResource::NameRecords,
    )
    .await
}

async fn execute_verified_record_lookup(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
    resource: SnapshotReadResource,
) -> V2Result<Option<VerifiedRecordLookup>> {
    if records.is_empty() {
        return Ok(None);
    }

    match execute_resolution_lookup(state, row, records, selected_snapshot).await {
        Ok(ResolutionLookupOutcome::Executed(response)) => {
            Ok(Some(VerifiedRecordLookup::Found { response }))
        }
        Ok(ResolutionLookupOutcome::NotSupported) => Ok(Some(VerifiedRecordLookup::NotSupported)),
        Ok(ResolutionLookupOutcome::AuthorityArmNotAdmitted) => {
            Ok(Some(VerifiedRecordLookup::AuthorityArmNotAdmitted))
        }
        Err(ResolutionLookupError::Snapshot(error))
            if error.kind() == SnapshotSelectionErrorKind::Stale =>
        {
            Ok(Some(VerifiedRecordLookup::Stale(
                VERIFIED_ANSWER_STALE_FOR_SNAPSHOT_REASON.to_owned(),
            )))
        }
        Err(error) => Err(api_error_to_v2_for_resource(
            snapshot_selection_api_error(error.into_snapshot()),
            resource,
        )),
    }
}

fn records_include_inventory(include: &[String]) -> V2Result<bool> {
    let mut include_inventory = false;
    for value in include {
        match value.as_str() {
            "inventory" => include_inventory = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only inventory",
                ));
            }
        }
    }
    Ok(include_inventory)
}

#[cfg(test)]
mod tests {
    use super::{ZERO_ADDRESS, ensure_executed_route_matches_admission};

    #[test]
    fn verified_route_guard_rejects_disagreement_in_both_directions() {
        let direct_resolver = "0x1000000000000000000000000000000000000001";

        assert!(ensure_executed_route_matches_admission(direct_resolver, true).is_err());
        assert!(ensure_executed_route_matches_admission(ZERO_ADDRESS, false).is_err());
        assert!(ensure_executed_route_matches_admission(direct_resolver, false).is_ok());
        assert!(ensure_executed_route_matches_admission(ZERO_ADDRESS, true).is_ok());
    }
}
