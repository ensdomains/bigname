use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    BASENAMES_NAMESPACE, NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot,
    SnapshotSelectionErrorKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AppState;
use crate::v2::support::{
    ResolutionLookupError, ResolutionRecordKey, load_name_current_for_selected_snapshot,
    load_supported_record_inventory_current_for_snapshot, map_internal_api_error,
    normalize_inferred_route_name, parse_resolution_record_key, snapshot_selection_api_error,
};

use super::support::execute_resolution_lookup;

use super::{
    AtSelector, Envelope, Finality, MAX_PAGE_SIZE, QueryParamAllowlist, RequestSource, Resolver,
    SnapshotReadResource, Source, Status, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2_for_resource, default_requested_records,
    name_records_inventory::RecordInventory, resolve_v2_snapshot_for, snapshot_meta,
    v2_exact_name_snapshot_scope_with_resolution_auxiliary, validate_product_record,
};

mod build;
pub(crate) use build::{
    VERIFIED_NOT_SUPPORTED_REASON, build_auto_name_records, build_indexed_name_records,
    build_verified_name_records, indexed_records_requiring_verified_fallback,
};

pub(crate) const MAX_RECORD_KEYS: usize = MAX_PAGE_SIZE as usize;
const VERIFIED_ANSWER_STALE_FOR_SNAPSHOT_REASON: &str = "verified_answer_stale_for_snapshot";

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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecords {
    pub(crate) namespace: String,
    pub(crate) resolver: Option<Resolver>,
    pub(crate) addresses: BTreeMap<String, String>,
    pub(crate) text_records: BTreeMap<String, String>,
    pub(crate) content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) records: Option<BTreeMap<String, RecordAnswer>>,
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
}

pub(crate) enum VerifiedRecordLookup {
    Found {
        response: Box<bigname_lookup::LookupResponse>,
    },
    Stale(String),
    NotSupported,
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
    let requested_records = parse_record_keys(params.keys.as_deref())?;
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

    let default_records;
    let requested_records = match requested_records.as_deref() {
        Some(records) => Some(records),
        None if params.source == RequestSource::Verified => {
            default_records = default_requested_records(record_inventory.as_ref());
            ensure_verified_record_limit(&default_records)?;
            Some(default_records.as_slice())
        }
        None => None,
    };

    let (route_source, data) = match params.source {
        RequestSource::Indexed => (
            Source::Indexed,
            build_indexed_name_records(
                &row,
                record_inventory.as_ref(),
                requested_records,
                include_inventory,
            )?,
        ),
        RequestSource::Verified => {
            let verified_lookup = load_verified_record_lookup(
                &state,
                &row,
                record_inventory.as_ref(),
                requested_records.unwrap_or_default(),
                &mut selected_snapshot,
            )
            .await?;
            (
                Source::Verified,
                build_verified_name_records(
                    &row,
                    record_inventory.as_ref(),
                    requested_records,
                    verified_lookup,
                    include_inventory,
                )?,
            )
        }
        RequestSource::Auto => {
            let records = requested_records.unwrap_or_default();
            if records.is_empty() {
                (
                    Source::Indexed,
                    build_indexed_name_records(
                        &row,
                        record_inventory.as_ref(),
                        requested_records,
                        include_inventory,
                    )?,
                )
            } else {
                let mut fallback_records = indexed_records_requiring_verified_fallback(
                    &row,
                    record_inventory.as_ref(),
                    records,
                )?;
                if namespace == BASENAMES_NAMESPACE && !fallback_records.is_empty() {
                    #[cfg(test)]
                    auto_fallback_test_hooks::run(&state.pool).await?;
                    (selected_snapshot, row, record_inventory) = load_name_records_snapshot_state(
                        &state,
                        &namespace,
                        &normalized.normalized_name,
                        params.at.as_ref(),
                        params.finality,
                        true,
                    )
                    .await?;
                    let refreshed_fallback_records = indexed_records_requiring_verified_fallback(
                        &row,
                        record_inventory.as_ref(),
                        records,
                    )?;
                    if refreshed_fallback_records.is_empty() {
                        return Err(V2Error::stale(
                            "name records changed while preparing verified fallback; retry the request",
                        ));
                    }
                    fallback_records = refreshed_fallback_records;
                }
                let verified_lookup = load_verified_record_lookup(
                    &state,
                    &row,
                    record_inventory.as_ref(),
                    &fallback_records,
                    &mut selected_snapshot,
                )
                .await?;
                build_auto_name_records(
                    &row,
                    record_inventory.as_ref(),
                    records,
                    verified_lookup,
                    include_inventory,
                )?
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

    let record_inventory =
        load_supported_record_inventory_current_for_snapshot(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(|error| {
                api_error_to_v2_for_resource(
                    snapshot_selection_api_error(error),
                    SnapshotReadResource::NameRecords,
                )
            })?;
    Ok((selected_snapshot, row, record_inventory))
}

pub(crate) fn ensure_verified_record_limit(records: &[ResolutionRecordKey]) -> V2Result<()> {
    if records.len() > MAX_RECORD_KEYS {
        return Err(V2Error::unsupported(format!(
            "verified record reads support at most {MAX_RECORD_KEYS} record keys"
        )));
    }
    Ok(())
}

pub(crate) async fn load_verified_record_lookup(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
) -> V2Result<Option<VerifiedRecordLookup>> {
    load_verified_record_lookup_for_resource(
        state,
        row,
        record_inventory,
        records,
        selected_snapshot,
        SnapshotReadResource::NameRecords,
    )
    .await
}

pub(crate) async fn load_verified_record_lookup_for_resource(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
    resource: SnapshotReadResource,
) -> V2Result<Option<VerifiedRecordLookup>> {
    load_verified_record_lookup_with_persistence(
        state,
        row,
        record_inventory,
        records,
        selected_snapshot,
        resource,
    )
    .await
}

pub(crate) async fn load_ephemeral_verified_record_lookup(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
) -> V2Result<Option<VerifiedRecordLookup>> {
    load_verified_record_lookup_with_persistence(
        state,
        row,
        record_inventory,
        records,
        selected_snapshot,
        SnapshotReadResource::NameRecords,
    )
    .await
}

async fn load_verified_record_lookup_with_persistence(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    selected_snapshot: &mut SelectedSnapshot,
    resource: SnapshotReadResource,
) -> V2Result<Option<VerifiedRecordLookup>> {
    if records.is_empty() {
        return Ok(None);
    }

    let _ = record_inventory;
    match execute_resolution_lookup(state, row, records, selected_snapshot).await {
        Ok(Some(response)) => Ok(Some(VerifiedRecordLookup::Found {
            response: Box::new(response),
        })),
        Ok(None) => Ok(Some(VerifiedRecordLookup::NotSupported)),
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

pub(crate) fn parse_record_keys(keys: Option<&str>) -> V2Result<Option<Vec<ResolutionRecordKey>>> {
    let Some(keys) = keys.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let mut parsed = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for key in keys.split(',').map(str::trim) {
        if parsed.len() >= MAX_RECORD_KEYS {
            return Err(V2Error::invalid_input(format!(
                "keys must contain at most {MAX_RECORD_KEYS} record keys"
            )));
        }
        if key.is_empty() {
            return Err(V2Error::invalid_input(
                "keys must be a comma-separated record-key list",
            ));
        }
        let record = parse_resolution_record_key(key)
            .and_then(validate_product_record)
            .ok_or_else(|| {
                V2Error::invalid_input(
                    "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
                )
            })?;
        if !seen.insert(record.record_key.clone()) {
            return Err(V2Error::invalid_input(
                "keys must not contain duplicate record keys",
            ));
        }
        parsed.push(record);
    }

    Ok(Some(parsed))
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
