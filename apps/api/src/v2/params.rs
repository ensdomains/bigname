use serde::Deserialize;
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::support::parse_evm_address;
use super::{
    error::{V2Error, V2Result},
    vocab::{
        AddressNamesDedupe, AddressNamesSort, Authority, Finality, HistoryEventType,
        HistoryEventTypeSet, HistoryScope, Relation, RelationSet,
    },
};

pub(crate) const DEFAULT_PAGE_SIZE: u64 = 50;
pub(crate) const MAX_PAGE_SIZE: u64 = 200;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub(crate) struct RawQueryParams {
    pub(crate) at: Option<String>,
    pub(crate) finality: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) coin_type: Option<String>,
    pub(crate) keys: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) scope: Option<String>,
    #[serde(rename = "type")]
    pub(crate) event_type: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) registration_id: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) resolver: Option<String>,
    pub(crate) contract_address: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) authority: Option<String>,
    pub(crate) is_migrated: Option<String>,
    pub(crate) from_block: Option<String>,
    pub(crate) to_block: Option<String>,
    pub(crate) from_timestamp: Option<String>,
    pub(crate) to_timestamp: Option<String>,
    pub(crate) expires_after: Option<String>,
    pub(crate) expires_before: Option<String>,
    pub(crate) q: Option<String>,
    pub(crate) dedupe: Option<String>,
    pub(crate) sort: Option<String>,
    pub(crate) order: Option<String>,
    pub(crate) include_expired: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryParams {
    pub(crate) at: Option<AtSelector>,
    pub(crate) finality: Finality,
    pub(crate) source: RequestSource,
    pub(crate) coin_type: Option<String>,
    pub(crate) keys: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) include: Vec<String>,
    pub(crate) scope: HistoryScope,
    pub(crate) event_types: Option<HistoryEventTypeSet>,
    pub(crate) name: Option<String>,
    pub(crate) registration_id: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) resolver: Option<ResolverSelector>,
    pub(crate) contract_address: Option<String>,
    pub(crate) relation: Option<RelationSet>,
    pub(crate) authority: Option<Authority>,
    pub(crate) is_migrated: Option<bool>,
    pub(crate) from_block: Option<i64>,
    pub(crate) to_block: Option<i64>,
    pub(crate) from_timestamp: Option<TimestampBound>,
    pub(crate) to_timestamp: Option<TimestampBound>,
    pub(crate) expires_after: Option<OffsetDateTime>,
    pub(crate) expires_before: Option<OffsetDateTime>,
    pub(crate) q: Option<String>,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) sort: AddressNamesSort,
    /// The trimmed `sort` value as sent, for routes whose default sort is not `name` and that
    /// must still reject an explicit `sort=name`.
    pub(crate) sort_wire: Option<String>,
    /// `None` when the request omitted `order`; each route applies its own default.
    pub(crate) order: Option<SortOrder>,
    pub(crate) include_expired: Option<bool>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: u64,
}

/// A parsed RFC 3339 bound together with its canonical UTC wire form, which is
/// what cursors bind so equivalent spellings continue the same query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TimestampBound {
    pub(crate) value: OffsetDateTime,
    pub(crate) canonical: String,
}

/// A resolver contract named as `<numeric chain_id>:<address>`; the chain is
/// carried as the storage slug and the address in lowercase canonical form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolverSelector {
    pub(crate) chain_id: u64,
    pub(crate) chain_slug: &'static str,
    pub(crate) address: String,
}

impl ResolverSelector {
    /// Wire form cursors bind: `<numeric chain_id>:<lowercase address>`.
    pub(crate) fn canonical(&self) -> String {
        format!("{}:{}", self.chain_id, self.address)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AtSelector {
    Timestamp(String),
    SnapshotToken(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestSource {
    Indexed,
    Verified,
    Auto,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SortOrder {
    Asc,
    Desc,
}

impl TryFrom<RawQueryParams> for QueryParams {
    type Error = V2Error;

    fn try_from(raw: RawQueryParams) -> Result<Self, Self::Error> {
        let params = Self {
            at: raw.at.as_deref().map(parse_at).transpose()?,
            finality: parse_finality(raw.finality.as_deref())?,
            source: parse_source(raw.source.as_deref())?,
            coin_type: trim_to_option(raw.coin_type),
            keys: trim_to_option(raw.keys),
            namespace: trim_to_option(raw.namespace),
            include: parse_include(raw.include),
            scope: parse_scope(raw.scope.as_deref())?,
            event_types: parse_event_types(raw.event_type.as_deref())?,
            name: trim_to_option(raw.name),
            registration_id: parse_registration_id(raw.registration_id)?,
            address: parse_address(raw.address, "address")?,
            contract_address: parse_address(raw.contract_address, "contract_address")?,
            resolver: parse_resolver_selector(raw.resolver)?,
            relation: parse_relation_set_param(raw.relation.as_deref())?,
            authority: parse_authority(raw.authority.as_deref())?,
            is_migrated: parse_bool_flag(raw.is_migrated.as_deref(), "is_migrated")?,
            from_block: parse_block_bound(raw.from_block, "from_block")?,
            to_block: parse_block_bound(raw.to_block, "to_block")?,
            from_timestamp: parse_timestamp_bound(raw.from_timestamp, "from_timestamp")?,
            to_timestamp: parse_timestamp_bound(raw.to_timestamp, "to_timestamp")?,
            expires_after: parse_expiry_bound(raw.expires_after, "expires_after")?,
            expires_before: parse_expiry_bound(raw.expires_before, "expires_before")?,
            q: trim_to_option(raw.q),
            dedupe: parse_dedupe(raw.dedupe.as_deref())?,
            sort: parse_sort(raw.sort.as_deref())?,
            sort_wire: trim_to_option(raw.sort),
            order: parse_order(raw.order.as_deref())?,
            include_expired: parse_bool_flag(raw.include_expired.as_deref(), "include_expired")?,
            cursor: trim_to_option(raw.cursor),
            page_size: parse_page_size(raw.page_size)?,
        };
        if matches!(
            (params.from_timestamp.as_ref(), params.to_timestamp.as_ref()),
            (Some(from), Some(to)) if from.value > to.value
        ) {
            return Err(V2Error::invalid_input(
                "from_timestamp must be less than or equal to to_timestamp",
            ));
        }
        Ok(params)
    }
}

fn parse_at(value: &str) -> V2Result<AtSelector> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_parameter("at"));
    }

    if bigname_storage::parse_rfc3339_utc_timestamp(value).is_ok() {
        return Ok(AtSelector::Timestamp(value.to_owned()));
    }

    if is_url_safe_opaque_token(value) {
        return Ok(AtSelector::SnapshotToken(value.to_owned()));
    }

    Err(invalid_parameter("at"))
}

fn parse_finality(value: Option<&str>) -> V2Result<Finality> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("latest") => Ok(Finality::Latest),
        Some("safe") => Ok(Finality::Safe),
        Some("finalized") => Ok(Finality::Finalized),
        Some(_) => Err(invalid_parameter("finality")),
    }
}

fn parse_source(value: Option<&str>) -> V2Result<RequestSource> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("indexed") => Ok(RequestSource::Indexed),
        Some("verified") => Ok(RequestSource::Verified),
        Some("auto") => Ok(RequestSource::Auto),
        Some(_) => Err(invalid_parameter("source")),
    }
}

fn parse_order(value: Option<&str>) -> V2Result<Option<SortOrder>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("asc") => Ok(Some(SortOrder::Asc)),
        Some("desc") => Ok(Some(SortOrder::Desc)),
        Some(_) => Err(invalid_parameter("order")),
    }
}

impl SortOrder {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

fn parse_scope(value: Option<&str>) -> V2Result<HistoryScope> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("both") => Ok(HistoryScope::Both),
        Some("name") => Ok(HistoryScope::Name),
        Some("registration") => Ok(HistoryScope::Registration),
        Some(_) => Err(invalid_parameter("scope")),
    }
}

/// `type` accepts one product event type or a comma-separated set; the set is
/// canonicalized so cursors bind one wire value per distinct set.
fn parse_event_types(value: Option<&str>) -> V2Result<Option<HistoryEventTypeSet>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let mut event_types = Vec::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let Some(event_type) = HistoryEventType::from_wire(part) else {
            return Err(invalid_parameter("type"));
        };
        event_types.push(event_type);
    }

    HistoryEventTypeSet::from_event_types(event_types)
        .map(Some)
        .ok_or_else(|| invalid_parameter("type"))
}

fn parse_timestamp_bound(
    value: Option<String>,
    field_name: &'static str,
) -> V2Result<Option<TimestampBound>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };
    let parsed = bigname_storage::parse_rfc3339_utc_timestamp(&value).map_err(|_| {
        V2Error::invalid_input(format!("{field_name} must be an RFC 3339 timestamp"))
    })?;
    Ok(Some(TimestampBound {
        canonical: format_timestamp_bound(parsed),
        value: parsed,
    }))
}

/// Canonical RFC 3339 UTC form; fractional seconds are kept only when present so
/// whole-second inputs keep the same shape as row timestamps.
fn format_timestamp_bound(value: OffsetDateTime) -> String {
    let value = value.to_offset(sqlx::types::time::UtcOffset::UTC);
    let mut formatted = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    );
    if value.nanosecond() != 0 {
        let fraction = format!("{:09}", value.nanosecond());
        formatted.push('.');
        formatted.push_str(fraction.trim_end_matches('0'));
    }
    formatted.push('Z');
    formatted
}

pub(crate) fn parse_relation_set_param(value: Option<&str>) -> V2Result<Option<RelationSet>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let mut has_any = false;
    let mut relations = Vec::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if part == "any" {
            has_any = true;
            continue;
        }
        let Some(relation) = Relation::from_wire(part) else {
            return Err(invalid_parameter("relation"));
        };
        relations.push(relation);
    }

    let mixes_resolves_to = relations.contains(&Relation::ResolvesTo)
        && (has_any
            || relations
                .iter()
                .any(|relation| *relation != Relation::ResolvesTo));
    if mixes_resolves_to {
        return Err(V2Error::invalid_input(
            "relation=resolves_to cannot be combined with owner, manager, registrant, or any",
        ));
    }
    if has_any {
        return Ok(Some(RelationSet::all()));
    }

    RelationSet::from_relations(relations)
        .map(Some)
        .ok_or_else(|| invalid_parameter("relation"))
}

fn parse_authority(value: Option<&str>) -> V2Result<Option<Authority>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(value) => Authority::from_wire(value)
            .map(Some)
            .ok_or_else(|| invalid_parameter("authority")),
    }
}

fn parse_registration_id(value: Option<String>) -> V2Result<Option<String>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    Uuid::parse_str(&value)
        .map(|uuid| Some(uuid.to_string()))
        .map_err(|_| V2Error::invalid_input("registration_id must be a UUID"))
}

fn parse_address(value: Option<String>, field: &'static str) -> V2Result<Option<String>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    parse_evm_address(&value, field)
        .map(Some)
        .map_err(|error| V2Error::invalid_input(error.message))
}

fn parse_resolver_selector(value: Option<String>) -> V2Result<Option<ResolverSelector>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };
    let invalid = || {
        V2Error::invalid_input(
            "resolver must be <chain_id>:<address> with a supported numeric chain id",
        )
    };
    let (chain_id, address) = value.split_once(':').ok_or_else(invalid)?;
    let chain_id = chain_id.trim();
    if chain_id.is_empty() || !chain_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let chain_id = chain_id.parse::<u64>().map_err(|_| invalid())?;
    let chain_slug = super::chains::numeric_to_slug(chain_id).ok_or_else(invalid)?;
    let address = parse_evm_address(address, "resolver")
        .map_err(|error| V2Error::invalid_input(error.message))?;
    Ok(Some(ResolverSelector {
        chain_id,
        chain_slug,
        address,
    }))
}

fn parse_block_bound(value: Option<String>, field_name: &'static str) -> V2Result<Option<i64>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .map(Some)
        .ok_or_else(|| {
            V2Error::invalid_input(format!("{field_name} must be a non-negative integer"))
        })
}

fn parse_expiry_bound(
    value: Option<String>,
    field_name: &'static str,
) -> V2Result<Option<OffsetDateTime>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    bigname_storage::parse_rfc3339_utc_timestamp(&value)
        .map(Some)
        .map_err(|_| {
            V2Error::invalid_input(format!(
                "{field_name} must be an RFC 3339 UTC timestamp such as 2026-01-02T03:04:05Z"
            ))
        })
}

fn parse_dedupe(value: Option<&str>) -> V2Result<AddressNamesDedupe> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("name") => Ok(AddressNamesDedupe::Name),
        Some("registration") => Ok(AddressNamesDedupe::Registration),
        Some(_) => Err(invalid_parameter("dedupe")),
    }
}

fn parse_sort(value: Option<&str>) -> V2Result<AddressNamesSort> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("name") => Ok(AddressNamesSort::Name),
        Some("expires_at") => Ok(AddressNamesSort::ExpiresAt),
        Some("registered_at") => Ok(AddressNamesSort::RegisteredAt),
        Some(_) => Err(invalid_parameter("sort")),
    }
}

fn parse_bool_flag(value: Option<&str>, parameter: &'static str) -> V2Result<Option<bool>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(_) => Err(invalid_parameter(parameter)),
    }
}

fn parse_page_size(value: Option<u64>) -> V2Result<u64> {
    match value {
        None => Ok(DEFAULT_PAGE_SIZE),
        Some(value @ 1..=MAX_PAGE_SIZE) => Ok(value),
        Some(_) => Err(V2Error::invalid_input(format!(
            "page_size must be between 1 and {MAX_PAGE_SIZE}"
        ))),
    }
}

fn parse_include(value: Option<String>) -> Vec<String> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn trim_to_option(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn is_url_safe_opaque_token(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        )
    })
}

fn invalid_parameter(parameter: &'static str) -> V2Error {
    V2Error::invalid_input(format!("{parameter} is invalid"))
}

pub(crate) fn validate_latest_collection_selectors(
    at: Option<&AtSelector>,
    finality: Finality,
) -> V2Result<()> {
    if at.is_some() {
        return Err(V2Error::invalid_input(
            "at is not supported because collection routes read latest state",
        ));
    }
    if finality != Finality::Latest {
        return Err(V2Error::invalid_input(
            "finality must be latest because collection routes read latest state",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
