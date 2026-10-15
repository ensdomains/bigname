use crate::v2::support::{ResolutionRecordKey, parse_resolution_record_key};
use crate::v2::{RequestSource, V2Error, V2Result, validate_product_record};

use super::MAX_RECORD_KEYS;

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

pub(crate) fn records_include_inventory(include: &[String]) -> V2Result<bool> {
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

/// Whether a records read asks for the inventory alone (`values=false`). That mode answers no
/// value, so it takes no verified read and no `keys`, and both conflicts are refused before the
/// keys are parsed (docs/api-v1-routes.md § `GET /v1/names/{name}/records`).
pub(crate) fn inventory_only_request(
    values: Option<bool>,
    source: RequestSource,
    keys: Option<&str>,
) -> V2Result<bool> {
    if values != Some(false) {
        return Ok(false);
    }
    if source == RequestSource::Verified {
        return Err(V2Error::invalid_input(
            "source must be indexed or auto when values=false",
        ));
    }
    if keys.is_some() {
        return Err(V2Error::invalid_input(
            "keys must be omitted when values=false",
        ));
    }
    Ok(true)
}

/// The record keys one records-route read answers, and whether the caller chose them.
///
/// Without `keys` the route answers the inventory-derived default set, but that set is not a
/// caller selection: it must not make an unkeyed `source=auto` read eligible for verified
/// fallback, and the `include=inventory` container keeps listing only what the row itself
/// carries (`docs/api-v1-routes.md` § `GET /v1/names/{name}/records`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecordSelection<'a> {
    pub(crate) records: &'a [ResolutionRecordKey],
    pub(crate) explicit: bool,
}

impl<'a> RecordSelection<'a> {
    /// Keys the caller, or an internal caller such as name detail or diagnostics, asked for.
    pub(crate) fn requested(records: &'a [ResolutionRecordKey]) -> Self {
        Self {
            records,
            explicit: true,
        }
    }

    /// The inventory-derived default set answered when the caller supplied no keys.
    pub(crate) fn inventory_default(records: &'a [ResolutionRecordKey]) -> Self {
        Self {
            records,
            explicit: false,
        }
    }

    /// The keys the `include=inventory` container reports as requested.
    pub(crate) fn inventory_request(self) -> Option<&'a [ResolutionRecordKey]> {
        self.explicit.then_some(self.records)
    }
}
