//! The record inventory a resolver holds for one name's node, read by the resolver rather than
//! through the name's current resolver pointer (docs/api-v1-routes.md,
//! `GET /v1/resolvers/{chain_id}/{address}/records`).
use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_domain::resolver_read::{ENSIP19_DEFAULT_RECORD_KEY, ensip19_default_fallback_target};
use bigname_storage::families::records::FamilyRecordInventory;

use super::{
    parse_numeric_chain_id, require_phase_target_snapshot, resolver_namespace,
    resolver_snapshot_scope, revalidate_project_generations,
};
use crate::{
    AppState,
    v2::{
        Envelope, QueryParamAllowlist, RecordAnswer, RecordSelection, RequestSource,
        SnapshotReadResource, Source, StrictQueryParams, V2Error, V2Result,
        api_error_to_v2_for_resource, default_requested_records,
        name_records::{
            NameRecords, ensure_default_record_limit, indexed_record_answer,
            records_include_inventory,
        },
        name_records_inventory::{fill_records_route_abi_content_types, inventory_summary},
        parse_record_keys, resolve_v2_snapshot_for, snapshot_meta,
        support::{
            ResolutionRecordKey, normalize_inferred_route_name, parse_evm_address,
            route_logical_name_id, snapshot_selection_api_error,
        },
        vocab::Resolver,
    },
};

pub(crate) struct ResolverRecordsQueryParams;

impl QueryParamAllowlist for ResolverRecordsQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "name",
        "namespace",
        "at",
        "finality",
        "source",
        "keys",
        "include",
    ];
}

type ResolverRecordsQuery = StrictQueryParams<ResolverRecordsQueryParams>;

pub(crate) async fn get_resolver_records(
    Path((chain_id, address)): Path<(String, String)>,
    params: ResolverRecordsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecords>>> {
    let params = params.into_inner();
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let address = parse_evm_address(&address, "address").map_err(crate::v2::api_error_to_v2)?;
    let name = params
        .name
        .as_deref()
        .ok_or_else(|| V2Error::invalid_input("name is required"))?;
    let normalized = normalize_inferred_route_name(name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let chain_namespace = resolver_namespace(chain_id_slug)?;
    if namespace != chain_namespace {
        return Err(V2Error::invalid_input(format!(
            "namespace must be {chain_namespace} for a resolver on chain {numeric_chain_id}"
        )));
    }
    if params.source != RequestSource::Indexed {
        return Err(V2Error::invalid_input("source must be indexed"));
    }
    let explicit_records = parse_record_keys(params.keys.as_deref())?;
    let include_inventory = records_include_inventory(&params.include)?;
    // The inventory container describes the whole row, so only a keyed read without it is
    // narrowed to its keys.
    let storage_keys = explicit_records
        .as_deref()
        .filter(|_| !include_inventory)
        .map(storage_record_keys);
    let logical_name_id = route_logical_name_id(&namespace, &normalized.normalized_name);
    let node = logical_name_id
        .split_once(':')
        .map(|(_, node)| node.to_owned())
        .ok_or_else(|| V2Error::internal_error("failed to derive the name's node"))?;

    let mut publication =
        crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
            &state,
            None,
            Some(chain_namespace),
        )
        .await?;
    let selected = resolve_v2_snapshot_for(
        &state.pool,
        &resolver_snapshot_scope(chain_id_slug)?,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::Resolver,
    )
    .await?;
    let generations =
        crate::v2::lookup::head::load_selected_project_generations(&state.pool, &selected).await?;
    revalidate_project_generations(
        &state,
        &mut publication,
        &selected,
        &generations,
        params.at.is_some(),
        "resolver records changed while reading; retry the request",
    )
    .await?;
    let row = bigname_storage::load_phase_resolver_current(
        publication.conn().await?,
        chain_id_slug,
        &address,
    )
    .await
    .map_err(crate::v2::name_rows_error(
        SnapshotReadResource::Resolver,
        |_| read_error(),
    ))?
    .ok_or_else(|| {
        V2Error::not_found(format!(
            "resolver {address} was not found on chain {numeric_chain_id}"
        ))
    })?;
    require_phase_target_snapshot(&row.chain_positions, chain_id_slug, &selected)?;
    let inventory = FamilyRecordInventory::load_resolver_node_for_snapshot(
        publication.conn().await?,
        chain_id_slug,
        &address,
        &namespace,
        &logical_name_id,
        &node,
        storage_keys.as_ref(),
        &selected.chain_positions,
    )
    .await
    .map_err(|error| {
        api_error_to_v2_for_resource(
            snapshot_selection_api_error(error),
            SnapshotReadResource::Resolver,
        )
    })?
    // The overview row read on this snapshot proves the classification the inventory needs.
    .ok_or_else(read_error)?;
    publication.finish(&state).await?;

    let default_records;
    let selection = match explicit_records.as_deref() {
        Some(records) => RecordSelection::requested(records),
        None => {
            default_records = default_requested_records(Some(&inventory));
            ensure_default_record_limit(&default_records)?;
            RecordSelection::inventory_default(&default_records)
        }
    };
    let records = selection
        .records
        .iter()
        .map(|record| {
            Ok((
                record.record_key.clone(),
                indexed_record_answer(Some(&inventory), record)?,
            ))
        })
        .collect::<V2Result<BTreeMap<String, RecordAnswer>>>()?;
    let mut data = NameRecords {
        namespace,
        canonical_name: None,
        resolver: Some(Resolver {
            chain_id: numeric_chain_id,
            address,
        }),
        records,
        inventory: include_inventory
            .then(|| inventory_summary(Some(&inventory), selection.inventory_request())),
    };
    fill_records_route_abi_content_types(&state.pool, data.inventory.as_mut(), Some(&inventory))
        .await?;
    let mut meta = snapshot_meta(&selected)?;
    meta.source = Some(Source::Indexed);
    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

/// The stored record keys the answers for `records` read: each key, and the source of the answer
/// the inventory can derive for it (`evaluate_indexed_record`).
fn storage_record_keys(records: &[ResolutionRecordKey]) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for record in records {
        keys.insert(record.record_key.clone());
        if record.record_key == "avatar" {
            keys.insert("text:avatar".to_owned());
        }
        let derivable = record.record_family == "addr"
            && record
                .selector_key
                .as_deref()
                .and_then(|coin_type| coin_type.parse::<u64>().ok())
                .is_some_and(ensip19_default_fallback_target);
        if derivable {
            keys.insert(ENSIP19_DEFAULT_RECORD_KEY.to_owned());
        }
    }
    keys
}

fn read_error() -> V2Error {
    V2Error::internal_error("failed to read resolver records")
}
