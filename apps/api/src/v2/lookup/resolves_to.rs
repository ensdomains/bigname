//! `relation=resolves_to` reverse inputs on `POST /v1/lookup`: the names whose current
//! `addr:<coin_type>` resolver record resolves to the input address.
//!
//! These inputs page `address_records_current` (name order) instead of the authority relations,
//! so they carry their own cursor binding. Every row still passes the reverse-lookup admission
//! rules: the name row, its inventory, its primary claim, and the reverse-index row itself must
//! be published at or before the served head, and the served generation is revalidated by the
//! caller after every input has rendered.

use std::collections::BTreeMap;

use bigname_storage::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    ReverseIdentityRecordRow,
};
use tracing::error;

use crate::AppState;
use crate::v2::{
    CursorPayload, Relation, RelationSet, V2Error, V2Result,
    address_names::{AddressNameResolution, address_name_resolution},
    decode, encode,
    support::load_reverse_identity_primary_snapshots,
};

use super::{
    address_lookup_result,
    admission::{require_name_projection_at_served_head, require_reverse_records_at_served_head},
    build::{build_reverse_detail_record, build_reverse_feed_record, lookup_address_status},
    cursor::{invalid_lookup_cursor, is_codeployed_public_namespace_set},
    dto::LookupResult,
    head::ServedHead,
    parse::{LookupProfile, ParsedAddressLookup},
};

const SORT: &str = "resolves_to_name_asc";
const ADDRESS_FILTER: &str = "address";
const COIN_TYPE_FILTER: &str = "coin_type";
const RELATION_FILTER: &str = "relation";
const PUBLIC_NAMESPACES_FILTER: &str = "public_namespaces";
const NAME_CURSOR: &str = "name";
const LOGICAL_NAME_ID_CURSOR: &str = "logical_name_id";
const RESOURCE_ID_CURSOR: &str = "resource_id";

pub(super) fn is_resolves_to_input(input: &ParsedAddressLookup) -> bool {
    input
        .relation
        .as_ref()
        .is_some_and(RelationSet::is_resolves_to)
}

pub(super) async fn render_resolves_to_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    inputs: &[ParsedAddressLookup],
    served_head: Option<&ServedHead>,
    public_namespaces: &[String],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let selected_snapshot = served_head.map(ServedHead::selected);
    for input in inputs.iter().filter(|input| is_resolves_to_input(input)) {
        let coin_type = input.coin_type.to_string();
        let page = bigname_storage::load_address_records_current_page(
            &state.pool,
            &input.address,
            &coin_type,
            Some(public_namespaces),
            AddressNamesCurrentDedupe::Surface,
            None,
            None,
            AddressNamesCurrentSort::Name,
            AddressNamesCurrentOrder::Asc,
            input.resolves_to_cursor.as_ref(),
            input.page_size,
        )
        .await
        .map_err(|load_error| {
            if input.resolves_to_cursor.is_some()
                && load_error
                    .to_string()
                    .contains("page cursor does not match a grouped entry")
            {
                return invalid_lookup_cursor();
            }
            error!(
                service = "api",
                address = %input.address,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load v2 lookup resolves_to records"
            );
            V2Error::internal_error("failed to load lookup resolves_to records")
        })?;

        let logical_name_ids = page
            .entries
            .iter()
            .map(|entry| entry.logical_name_id.clone())
            .collect::<Vec<_>>();
        let name_records =
            bigname_storage::load_phase_identity_records_by_ids(&state.pool, &logical_name_ids)
                .await
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %input.address,
                        error = ?load_error,
                        "failed to load v2 lookup resolves_to name records"
                    );
                    V2Error::internal_error("failed to load lookup resolves_to name records")
                })?
                .into_iter()
                .map(|record| (record.row.logical_name_id.clone(), record))
                .collect::<BTreeMap<_, _>>();
        let primary_names = load_reverse_identity_primary_snapshots(
            &state.pool,
            &input.address,
            &coin_type,
            public_namespaces,
        )
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %input.address,
                error = ?load_error,
                "failed to load v2 lookup resolves_to primary names"
            );
            V2Error::internal_error("failed to load lookup resolves_to primary names")
        })?;

        let mut entries: Vec<(ReverseIdentityRecordRow, AddressNameResolution)> = Vec::new();
        for entry in &page.entries {
            // A root pointer can serve records while authority remains unprojected. Keep that
            // same serving exception here; unrelated unsupported name rows stay excluded.
            let Some(name_record) = name_records.get(&entry.logical_name_id) else {
                continue;
            };
            if name_record
                .row
                .coverage
                .get("status")
                .and_then(|value| value.as_str())
                == Some("unsupported")
                && !(name_record
                    .row
                    .coverage
                    .get("unsupported_reason")
                    .and_then(|value| value.as_str())
                    == Some(crate::v2::vocab::PARTIAL_SERVE_UNSUPPORTED_REASON)
                    && bigname_storage::identity_name_current_has_event_linked_registry_serving(
                        &name_record.row,
                    ))
            {
                continue;
            }
            if let Some(selected_snapshot) = selected_snapshot {
                require_name_projection_at_served_head(
                    &entry.chain_positions,
                    &entry.namespace,
                    selected_snapshot,
                )?;
            }
            let primary_name = primary_names.get(&entry.namespace).cloned();
            entries.push((
                ReverseIdentityRecordRow {
                    name_record: name_record.clone(),
                    relation_facets: Vec::new(),
                    primary_chain_positions: primary_name
                        .as_ref()
                        .and_then(|primary| primary.chain_positions.clone()),
                    primary_name,
                    requested_coin_type: coin_type.clone(),
                },
                address_name_resolution(entry, input.coin_type),
            ));
        }
        if let Some(selected_snapshot) = selected_snapshot {
            let records = entries
                .iter()
                .map(|(record, _)| record.clone())
                .collect::<Vec<_>>();
            require_reverse_records_at_served_head(&records, selected_snapshot)?;
        }

        let records = entries
            .iter()
            .map(|(record, resolution)| {
                let mut built = match profile {
                    LookupProfile::Feed => build_reverse_feed_record(record),
                    LookupProfile::Detail => build_reverse_detail_record(record),
                }?;
                built.relations = vec![Relation::ResolvesTo];
                built.resolution = Some(resolution.clone());
                Ok(built)
            })
            .collect::<V2Result<Vec<_>>>()?;
        let status = lookup_address_status(&records);
        let binding = LookupResolvesToCursorBinding {
            address: &input.address,
            coin_type: input.coin_type,
            public_namespaces,
        };
        let has_more = page.next_cursor.is_some();
        let next_cursor = page
            .next_cursor
            .as_ref()
            .map(|cursor| encode(&resolves_to_cursor_payload(cursor, &binding)));
        results[input.index] = Some(address_lookup_result(
            input,
            records,
            next_cursor,
            None,
            has_more,
            status,
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct LookupResolvesToCursorBinding<'a> {
    address: &'a str,
    coin_type: u64,
    public_namespaces: &'a [String],
}

fn cursor_filters(binding: &LookupResolvesToCursorBinding<'_>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (ADDRESS_FILTER.to_owned(), binding.address.to_owned()),
        (COIN_TYPE_FILTER.to_owned(), binding.coin_type.to_string()),
        (
            RELATION_FILTER.to_owned(),
            Relation::ResolvesTo.as_str().to_owned(),
        ),
    ]);
    if !is_codeployed_public_namespace_set(binding.public_namespaces) {
        filters.insert(
            PUBLIC_NAMESPACES_FILTER.to_owned(),
            binding.public_namespaces.join(","),
        );
    }
    filters
}

fn resolves_to_cursor_payload(
    cursor: &AddressNamesCurrentSortedCursor,
    binding: &LookupResolvesToCursorBinding<'_>,
) -> CursorPayload {
    let AddressNamesCurrentSortedCursorValue::Name(name) = &cursor.sort_value else {
        unreachable!("resolves_to lookup pages are always name sorted");
    };
    CursorPayload::new(
        SORT,
        cursor_filters(binding),
        BTreeMap::from([
            (NAME_CURSOR.to_owned(), name.clone()),
            (
                LOGICAL_NAME_ID_CURSOR.to_owned(),
                cursor.logical_name_id.clone(),
            ),
            (
                RESOURCE_ID_CURSOR.to_owned(),
                cursor.resource_id.to_string(),
            ),
        ]),
        None,
    )
}

fn resolves_to_storage_cursor(
    payload: &CursorPayload,
    binding: &LookupResolvesToCursorBinding<'_>,
) -> V2Result<AddressNamesCurrentSortedCursor> {
    if payload.sort != SORT || payload.filters != cursor_filters(binding) {
        return Err(invalid_lookup_cursor());
    }
    if payload.last_item.len() != 3 {
        return Err(invalid_lookup_cursor());
    }
    let value = |key: &str| {
        payload
            .last_item
            .get(key)
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .ok_or_else(invalid_lookup_cursor)
    };
    Ok(AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name(value(NAME_CURSOR)?),
        logical_name_id: value(LOGICAL_NAME_ID_CURSOR)?,
        resource_id: sqlx::types::Uuid::parse_str(&value(RESOURCE_ID_CURSOR)?)
            .map_err(|_| invalid_lookup_cursor())?,
    })
}

pub(super) fn parse_resolves_to_cursor(
    cursor: Option<&str>,
    address: &str,
    coin_type: u64,
    public_namespaces: &[String],
) -> V2Result<(Option<AddressNamesCurrentSortedCursor>, Option<String>)> {
    let Some(cursor) = cursor.map(str::trim).filter(|cursor| !cursor.is_empty()) else {
        return Ok((None, None));
    };
    let binding = LookupResolvesToCursorBinding {
        address,
        coin_type,
        public_namespaces,
    };
    let payload = decode(cursor)?;
    let storage_cursor = resolves_to_storage_cursor(&payload, &binding)?;
    Ok((Some(storage_cursor), Some(encode(&payload))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::Uuid;

    #[test]
    fn resolves_to_lookup_cursor_binds_address_coin_type_and_relation() {
        let public_namespaces = vec!["basenames".to_owned(), "ens".to_owned()];
        let binding = LookupResolvesToCursorBinding {
            address: "0x0000000000000000000000000000000000000abc",
            coin_type: 60,
            public_namespaces: &public_namespaces,
        };
        let cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Name("alice.eth".to_owned()),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x5a0301),
        };
        let payload = resolves_to_cursor_payload(&cursor, &binding);
        assert_eq!(payload.filters["relation"], "resolves_to");
        assert!(payload.snapshot.is_none());
        assert_eq!(
            resolves_to_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );

        let other_coin = LookupResolvesToCursorBinding {
            coin_type: 2_147_483_658,
            ..binding.clone()
        };
        assert!(resolves_to_storage_cursor(&payload, &other_coin).is_err());

        let ens_only = vec!["ens".to_owned()];
        let ens_only_binding = LookupResolvesToCursorBinding {
            public_namespaces: &ens_only,
            ..binding
        };
        assert!(resolves_to_storage_cursor(&payload, &ens_only_binding).is_err());
    }
}
