use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
};
use tracing::error;

use super::support::{load_reverse_identity_records_live, load_reverse_identity_records_page_live};
use crate::AppState;

use super::{Authority, Envelope, NoQueryParams, Page, Status, V2Error, V2Result, encode};

mod admission;
mod build;
mod cursor;
mod dto;
mod forward;
pub(crate) mod head;
#[cfg(test)]
pub(crate) use head::{
    served_head_initial_validation_test_hooks, served_head_revalidation_test_hooks,
};
mod page;
mod parse;
mod relation_filter;
mod resolves_to;
mod scope;

use admission::require_reverse_records_at_served_head;
pub(crate) use admission::{
    require_name_current_at_served_head, require_name_projection_at_served_head,
};
use build::{build_reverse_detail_record, build_reverse_feed_record, lookup_address_status};
use cursor::{
    LookupReverseCursorBinding, ReverseCursorKey, ReverseStorageKey, lookup_reverse_cursor_payload,
    reverse_identity_sort, reverse_identity_storage_cursor,
};
use dto::{LookupInput, LookupKind, LookupRecord, LookupRequest, LookupResult};
use forward::render_name_lookup_results;
use head::{load_served_head, revalidate_served_head};
use page::ReverseLookupPage;
use parse::{
    LookupProfile, ParsedAddressLookup, bind_address_cursor, ensure_lookup_batch_limit,
    parse_address_input, parse_lookup_include, parse_lookup_json_body, parse_lookup_namespace,
    parse_lookup_profile, parse_name_input,
};
use relation_filter::{
    requires_relation_post_filter, reverse_record_matches_relation, trim_reverse_record_relations,
};
use scope::{
    lookup_public_namespaces, lookup_request_scope_meta, lookup_snapshot_scope,
    revalidate_lookup_public_namespaces,
};

const EXACT_RELATION_SCAN_MULTIPLIER: u64 = 10;

pub(crate) async fn get_lookup(
    _query: NoQueryParams,
    State(state): State<AppState>,
    body: Result<Json<LookupRequest>, JsonRejection>,
) -> V2Result<Json<Envelope<Vec<LookupResult>>>> {
    let body = parse_lookup_json_body(body)?;
    ensure_lookup_batch_limit(body.inputs.len())?;
    let profile = parse_lookup_profile(body.profile.as_deref())?;
    let include = parse_lookup_include(body.include.as_deref(), profile)?;
    let namespace = parse_lookup_namespace(body.namespace.as_deref())?;
    let has_address_inputs = body
        .inputs
        .iter()
        .any(|input| matches!(input, LookupInput::Address(_)));
    if namespace.is_some() && has_address_inputs {
        return Err(V2Error::invalid_input(
            "namespace is not supported for address lookup inputs",
        ));
    }
    let mut name_inputs = Vec::new();
    let mut address_inputs = Vec::new();
    for (index, item) in body.inputs.iter().enumerate() {
        match item {
            LookupInput::Name(input) => {
                name_inputs.push(parse_name_input(index, input, namespace)?);
            }
            LookupInput::Address(input) => {
                address_inputs.push(parse_address_input(index, input)?);
            }
        }
    }
    let public_namespaces = if has_address_inputs {
        Some(lookup_public_namespaces(&state).await?)
    } else {
        None
    };
    if let Some(public_namespaces) = public_namespaces.as_ref() {
        if public_namespaces.is_empty() {
            return Err(V2Error::conflict(
                "the deployment does not serve a public namespace",
            ));
        }
        for input in &mut address_inputs {
            bind_address_cursor(input, public_namespaces.names())?;
        }
    }
    let snapshot_scope = lookup_snapshot_scope(
        &state,
        namespace,
        &name_inputs,
        !address_inputs.is_empty(),
        public_namespaces.as_ref(),
    )
    .await?;
    let served_head = match snapshot_scope.as_ref() {
        Some(scope) => load_served_head(&state.pool, scope).await?,
        None => None,
    };

    let mut results = vec![None; body.inputs.len()];
    let selected_snapshot = served_head.as_ref().map(head::ServedHead::selected);
    render_name_lookup_results(
        &state,
        profile,
        include,
        &name_inputs,
        selected_snapshot,
        &mut results,
    )
    .await?;
    render_reverse_lookup_results(
        &state,
        profile,
        &address_inputs,
        served_head.as_ref(),
        public_namespaces
            .as_ref()
            .map(|namespaces| namespaces.names())
            .unwrap_or_default(),
        &mut results,
    )
    .await?;
    apply_migrated_at(&state, &mut results).await?;
    #[cfg(test)]
    head::served_head_revalidation_test_hooks::run(&state.pool).await?;
    revalidate_lookup_public_namespaces(&state, public_namespaces.as_ref()).await?;
    if let Some(served_head) = served_head.as_ref() {
        revalidate_served_head(&state.pool, served_head).await?;
    }
    let data = results
        .into_iter()
        .map(|result| result.expect("every parsed lookup input must render a result"))
        .collect::<Vec<_>>();
    let meta = lookup_request_scope_meta(&served_head, &public_namespaces, &snapshot_scope)?;
    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

/// Fills `migrated_at` on every rendered record whose current authority is the ENSv2 arm, in one
/// batch across name results and reverse rows.
async fn apply_migrated_at(state: &AppState, results: &mut [Option<LookupResult>]) -> V2Result<()> {
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
            let logical_name_id =
                bigname_storage::logical_name_id_for_name(&record.namespace, &record.name);
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
    let migrated_at =
        crate::v2::name_record::load_migrated_at(&state.pool, &logical_name_ids).await?;
    for (logical_name_id, record) in &mut records {
        record.migrated_at = migrated_at.get(logical_name_id).cloned();
    }
    Ok(())
}

async fn render_reverse_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    inputs: &[ParsedAddressLookup],
    served_head: Option<&head::ServedHead>,
    public_namespaces: &[String],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    render_storage_exact_reverse_lookup_results(
        state,
        profile,
        inputs,
        served_head,
        public_namespaces,
        results,
    )
    .await?;
    resolves_to::render_resolves_to_lookup_results(
        state,
        profile,
        inputs,
        served_head,
        public_namespaces,
        results,
    )
    .await?;
    for input in inputs
        .iter()
        .filter(|input| requires_relation_post_filter(input.relation.as_ref()))
    {
        let page =
            load_exact_relation_reverse_page(state, input, served_head, public_namespaces).await?;
        render_reverse_input_result(profile, input, page, results)?;
    }
    Ok(())
}

async fn render_storage_exact_reverse_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    inputs: &[ParsedAddressLookup],
    served_head: Option<&head::ServedHead>,
    public_namespaces: &[String],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let selected_snapshot = served_head.map(head::ServedHead::selected);
    let storage_exact_inputs = inputs
        .iter()
        .filter(|input| {
            !requires_relation_post_filter(input.relation.as_ref())
                && !resolves_to::is_resolves_to_input(input)
        })
        .collect::<Vec<_>>();
    let storage_inputs = deduped_reverse_storage_inputs(storage_exact_inputs.iter().copied());
    let groups =
        load_reverse_identity_records_live(&state.pool, &storage_inputs, public_namespaces)
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                    input_count = inputs.len(),
                    error = ?load_error,
                    "failed to load v2 lookup reverse detail records"
                );
                V2Error::internal_error("failed to load lookup reverse detail records")
            })?
            .into_iter()
            .map(|group| (reverse_group_key(&group), group))
            .collect::<BTreeMap<_, _>>();

    for input in storage_exact_inputs {
        let key = ReverseStorageKey::from(input);
        let mut entries = groups
            .get(&key)
            .map(|group| group.entries.clone())
            .unwrap_or_default();
        if let Some(selected_snapshot) = selected_snapshot {
            require_reverse_records_at_served_head(&entries, selected_snapshot)?;
        }
        let total_count = groups
            .get(&key)
            .and_then(|group| group.total_count)
            .or(Some(0));
        let has_more = groups.get(&key).is_some_and(|group| group.has_more);
        entries.sort_by(reverse_identity_sort);
        let binding = LookupReverseCursorBinding {
            address: &input.address,
            coin_type: input.coin_type,
            relation: input.relation.as_ref(),
            public_namespaces,
        };
        let next_cursor = if has_more {
            entries
                .last()
                .map(|record| encode(&lookup_reverse_cursor_payload(record, &binding)))
        } else {
            None
        };
        let page = ReverseLookupPage {
            entries,
            next_cursor,
            total_count,
            has_more,
        };
        render_reverse_input_result(profile, input, page, results)?;
    }

    Ok(())
}

async fn load_exact_relation_reverse_page(
    state: &AppState,
    input: &ParsedAddressLookup,
    served_head: Option<&head::ServedHead>,
    public_namespaces: &[String],
) -> V2Result<ReverseLookupPage> {
    let selected_snapshot = served_head.map(head::ServedHead::selected);
    let target_len = input.page_size as usize;
    let scan_size = input.page_size.max(50);
    let scan_cap = scan_size.saturating_mul(EXACT_RELATION_SCAN_MULTIPLIER);
    let mut cursor = input.page_cursor.clone();
    let mut entries = Vec::with_capacity(target_len.saturating_add(1));
    let mut has_more = false;
    let mut hit_scan_cap = false;
    let mut rows_examined = 0_u64;
    let mut last_examined = None;

    loop {
        let storage_input = bigname_storage::ReverseIdentityStorageInput {
            address: input.address.clone(),
            coin_type: input.coin_type.to_string(),
            roles: input.roles,
            page_size: scan_size as i64,
            cursor: cursor.clone(),
        };
        let mut groups = load_reverse_identity_records_page_live(
            &state.pool,
            std::slice::from_ref(&storage_input),
            public_namespaces,
        )
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = 1,
                relation = ?input.relation,
                error = ?load_error,
                "failed to load v2 lookup reverse exact-relation records"
            );
            V2Error::internal_error("failed to load lookup reverse records")
        })?;
        let Some(mut group) = groups.pop() else {
            break;
        };

        group.entries.sort_by(reverse_identity_sort);
        let broad_has_more = group.has_more;
        if group.entries.is_empty() {
            break;
        }
        for entry in group.entries {
            if let Some(selected_snapshot) = selected_snapshot {
                require_reverse_records_at_served_head(
                    std::slice::from_ref(&entry),
                    selected_snapshot,
                )?;
            }
            let next_scan_cursor = reverse_identity_storage_cursor(&entry);
            rows_examined = rows_examined.saturating_add(1);
            last_examined = Some(entry.clone());
            if reverse_record_matches_relation(&entry, input.relation.as_ref()) {
                entries.push(trim_reverse_record_relations(
                    entry,
                    input.relation.as_ref(),
                ));
                if entries.len() > target_len {
                    has_more = true;
                    break;
                }
            }
            cursor = Some(next_scan_cursor);
            if rows_examined >= scan_cap && broad_has_more {
                hit_scan_cap = true;
                break;
            }
        }

        if has_more || hit_scan_cap || !broad_has_more {
            break;
        }
        if cursor.is_none() {
            break;
        }
        super::support::prepare_reverse_identity_additional_scan(&state.pool, served_head).await?;
    }

    let binding = LookupReverseCursorBinding {
        address: &input.address,
        coin_type: input.coin_type,
        relation: input.relation.as_ref(),
        public_namespaces,
    };
    let next_cursor_record = if has_more {
        entries.truncate(target_len);
        entries.last()
    } else if hit_scan_cap {
        has_more = true;
        last_examined.as_ref()
    } else {
        None
    };
    let next_cursor =
        next_cursor_record.map(|record| encode(&lookup_reverse_cursor_payload(record, &binding)));

    Ok(ReverseLookupPage {
        entries,
        next_cursor,
        total_count: None,
        has_more,
    })
}

fn render_reverse_input_result(
    profile: LookupProfile,
    input: &ParsedAddressLookup,
    page: ReverseLookupPage,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let records = page
        .entries
        .iter()
        .map(|record| match profile {
            LookupProfile::Feed => build_reverse_feed_record(record),
            LookupProfile::Detail => build_reverse_detail_record(record),
        })
        .collect::<V2Result<Vec<_>>>()?;
    let status = lookup_address_status(&records);
    results[input.index] = Some(address_lookup_result(
        input,
        records,
        page.next_cursor,
        page.total_count,
        page.has_more,
        status,
    ));
    Ok(())
}

fn address_lookup_result(
    input: &ParsedAddressLookup,
    records: Vec<LookupRecord>,
    next_cursor: Option<String>,
    total_count: Option<u64>,
    has_more: bool,
    status: Status,
) -> LookupResult {
    LookupResult {
        unsupported_reason: result_unsupported_reason(status, records.iter()),
        failure_reason: result_failure_reason(status, records.iter()),
        input: input.input.clone(),
        kind: LookupKind::Address,
        status,
        normalization: None,
        record: None,
        records: Some(records),
        page: Some(Page {
            cursor: input.page_cursor_token.clone(),
            next_cursor,
            page_size: input.page_size,
            total_count,
            has_more,
        }),
    }
}

fn result_unsupported_reason<'a>(
    status: Status,
    records: impl Iterator<Item = &'a LookupRecord>,
) -> Option<String> {
    (status == Status::Unsupported)
        .then(|| {
            records
                .filter_map(|record| record.unsupported_reason.clone())
                .next()
        })
        .flatten()
}

fn result_failure_reason<'a>(
    status: Status,
    records: impl Iterator<Item = &'a LookupRecord>,
) -> Option<String> {
    matches!(status, Status::Failed | Status::NotFound | Status::Mismatch)
        .then(|| {
            records
                .filter_map(|record| record.failure_reason.clone())
                .next()
        })
        .flatten()
}

fn deduped_reverse_storage_inputs<'a>(
    inputs: impl Iterator<Item = &'a ParsedAddressLookup>,
) -> Vec<bigname_storage::ReverseIdentityStorageInput> {
    inputs
        .map(ReverseStorageKey::from)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|key| bigname_storage::ReverseIdentityStorageInput {
            address: key.address,
            coin_type: key.coin_type.to_string(),
            roles: key.roles,
            page_size: key.page_size as i64,
            cursor: key.cursor.map(bigname_storage::ReverseIdentityCursor::from),
        })
        .collect()
}

fn reverse_group_key(group: &bigname_storage::ReverseIdentityGroup) -> ReverseStorageKey {
    ReverseStorageKey {
        address: group.input.address.clone(),
        coin_type: group.input.coin_type.parse::<u64>().unwrap_or_default(),
        roles: group.input.roles,
        page_size: group.input.page_size as u64,
        cursor: group.input.cursor.clone().map(ReverseCursorKey::from),
    }
}
