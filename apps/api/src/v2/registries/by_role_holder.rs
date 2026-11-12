//! `GET /v1/registries?role_holder=`: the registries on which an address holds ENSv2 registry
//! root roles, one row per registry address, read by
//! `bigname_storage::families::topology::load_registries_by_role_holder` on the request's
//! collection snapshot.
use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{FromRequestParts, State},
    http::request::Parts,
};
use bigname_storage::families::topology::{RoleHolderRegistry, load_registries_by_role_holder};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    AppState,
    v2::{
        Envelope, Page, QueryParams, RawQueryParams, SnapshotReadResource, V2Error, V2Result,
        collection_snapshot::CollectionSnapshot,
        cursor::invalid_cursor_error,
        list_cursor::{ListCursor, ListPosition},
        name_rows_error, parse_numeric_chain_id, permission_powers_value,
        resolvers::resolver_namespace,
        slug_to_numeric,
        support::parse_evm_address,
    },
};

const ROLE_HOLDER_QUERY_PARAMS: &[&str] = &["role_holder", "chain_id", "cursor", "page_size"];
const POSITION: [&str; 2] = ["chain_id", "address"];

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RawRoleHolderQuery {
    role_holder: Option<String>,
    chain_id: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

/// The parsed query: the lower-cased holder, the chain filter as its numeric id and slug, and
/// the shared cursor and page size.
pub(crate) struct RoleHolderQuery {
    holder: String,
    chain: Option<(u64, &'static str)>,
    cursor: Option<String>,
    page_size: u64,
}

impl<S> FromRequestParts<S> for RoleHolderQuery
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let raw = crate::v2::parse_raw_query_params_with_allowlist::<RawRoleHolderQuery, S>(
            parts,
            state,
            ROLE_HOLDER_QUERY_PARAMS,
        )
        .await?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawRoleHolderQuery> for RoleHolderQuery {
    type Error = V2Error;

    fn try_from(raw: RawRoleHolderQuery) -> Result<Self, Self::Error> {
        let shared = QueryParams::try_from(RawQueryParams {
            cursor: raw.cursor,
            page_size: raw.page_size,
            ..RawQueryParams::default()
        })?;
        let holder = raw
            .role_holder
            .as_deref()
            .map(str::trim)
            .filter(|holder| !holder.is_empty())
            .ok_or_else(|| {
                V2Error::invalid_input(
                    "role_holder is required: this collection lists the registries of one holder",
                )
            })?;
        let holder = parse_evm_address(holder, "role_holder")
            .map_err(|error| V2Error::invalid_input(error.message))?
            .to_ascii_lowercase();
        let chain = raw
            .chain_id
            .as_deref()
            .map(str::trim)
            .filter(|chain| !chain.is_empty())
            .map(parse_numeric_chain_id)
            .transpose()?;
        Ok(Self {
            holder,
            chain,
            cursor: shared.cursor,
            page_size: shared.page_size,
        })
    }
}

pub(crate) async fn get_registries(
    params: RoleHolderQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Value>>>> {
    let mut filters = BTreeMap::from([("role_holder".to_owned(), params.holder.clone())]);
    if let Some((chain_id, _)) = params.chain {
        filters.insert("chain_id".to_owned(), chain_id.to_string());
    }
    let list = ListCursor::new("registry_asc", filters);
    let after = list
        .read(params.cursor.as_deref(), &POSITION)?
        .map(|position| cursor_position(&position))
        .transpose()?;
    let namespace = params
        .chain
        .map(|(_, slug)| resolver_namespace(slug))
        .transpose()?;
    let mut snapshot = CollectionSnapshot::capture_for_namespace(&state, None, namespace).await?;
    let served: Vec<String> = snapshot.block_bounds().into_keys().collect();
    let chains = match params.chain {
        Some((_, slug)) if !served.iter().any(|chain| chain == slug) => {
            return Err(snapshot
                .refuse(
                    &state,
                    V2Error::invalid_input("chain_id names a chain this deployment does not serve"),
                )
                .await);
        }
        Some((_, slug)) => vec![slug.to_owned()],
        None => served,
    };
    let limit = i64::try_from(params.page_size.saturating_add(1))
        .map_err(|_| V2Error::invalid_input("page_size is too large"))?;
    let mut rows = load_registries_by_role_holder(
        snapshot.conn().await?,
        &params.holder,
        &chains,
        after
            .as_ref()
            .map(|(slug, address)| (*slug, address.as_str())),
        limit,
    )
    .await
    .map_err(name_rows_error(SnapshotReadResource::Registry, |error| {
        tracing::error!(?error, "role holder registry read failed");
        V2Error::internal_error("failed to read the registries of the role holder")
    }))?;
    let has_more = rows.len() as u64 > params.page_size;
    rows.truncate(params.page_size as usize);
    let data = rows.iter().map(item).collect::<V2Result<Vec<_>>>()?;
    let next_cursor = if has_more {
        data.last().map(|row| {
            list.next(ListPosition::new([
                ("chain_id", row["chain_id"].to_string()),
                (
                    "address",
                    row["address"].as_str().unwrap_or_default().to_owned(),
                ),
            ]))
        })
    } else {
        None
    };
    let meta = snapshot.finish(&state).await?;
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor,
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta,
    }))
}

/// The registry a cursor continues after: a supported numeric chain id and an address, else
/// an invalid cursor.
fn cursor_position(position: &ListPosition) -> V2Result<(&'static str, String)> {
    let (_, slug) =
        parse_numeric_chain_id(position.get("chain_id")?).map_err(|_| invalid_cursor_error())?;
    let address = parse_evm_address(position.get("address")?, "cursor")
        .map_err(|_| invalid_cursor_error())?;
    Ok((slug, address.to_ascii_lowercase()))
}

fn item(row: &RoleHolderRegistry) -> V2Result<Value> {
    let chain_id = slug_to_numeric(&row.chain_id).ok_or_else(|| {
        V2Error::internal_error("a role holder registry is on an unsupported chain")
    })?;
    Ok(json!({
        "chain_id": chain_id,
        "address": row.registry_address,
        "grant_count": row.grant_count,
        "powers": permission_powers_value(&json!(row.powers))?,
    }))
}
