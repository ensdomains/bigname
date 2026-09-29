//! Collection-changing history filters. Payload/count includes remain separate.

use std::collections::{BTreeMap, BTreeSet};

use super::params::parse_event_types;
use super::vocab::HistoryEventTypeSet;
use super::{QueryParams, V2Error, V2Result, product_history_event_kinds};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HistoryFilters {
    excluded_types: Option<HistoryEventTypeSet>,
    kinds: Option<Vec<String>>,
    pub(crate) record_key: Option<String>,
}

impl HistoryFilters {
    pub(crate) fn parse(
        exclude_type: Option<&str>,
        kind: Option<&str>,
        record_key: Option<String>,
    ) -> V2Result<Self> {
        let excluded_types = parse_event_types(exclude_type, "exclude_type")?;
        let kinds = kind
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                let supported = product_history_event_kinds();
                let mut kinds = BTreeSet::new();
                for part in value
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                {
                    if !supported.iter().any(|kind| kind == part) {
                        return Err(V2Error::invalid_input("kind is invalid"));
                    }
                    kinds.insert(part.to_owned());
                }
                if kinds.is_empty() {
                    return Err(V2Error::invalid_input("kind is invalid"));
                }
                Ok(kinds.into_iter().collect())
            })
            .transpose()?;
        // History keys are stored selectors, not profile lookup inputs. Preserve their exact
        // decoded spelling, including commas, whitespace, and keys outside lookup's grammar.
        if record_key.as_deref() == Some("") {
            return Err(V2Error::invalid_input("record_key must not be empty"));
        }
        Ok(Self {
            excluded_types,
            kinds,
            record_key,
        })
    }

    pub(crate) fn insert_cursor_keys(&self, filters: &mut BTreeMap<String, String>) {
        if let Some(types) = &self.excluded_types {
            filters.insert("exclude_type".to_owned(), types.canonical_value());
        }
        if let Some(kinds) = &self.kinds {
            filters.insert("kind".to_owned(), kinds.join(","));
        }
        if let Some(key) = &self.record_key {
            filters.insert("record_key".to_owned(), key.clone());
        }
    }

    pub(crate) fn is_explicit(&self) -> bool {
        self.excluded_types.is_some() || self.kinds.is_some() || self.record_key.is_some()
    }
}

/// Compile the public intersection once for all three product history routes. An empty result
/// must travel with `match_no_events=true`: storage's legacy empty vector means unrestricted.
pub(crate) fn event_kinds(params: &QueryParams) -> Vec<String> {
    let mut selected = params
        .event_types
        .as_ref()
        .map(HistoryEventTypeSet::storage_event_kinds)
        .unwrap_or_else(product_history_event_kinds);
    let extra = &params.history_filters;
    if let Some(excluded) = &extra.excluded_types {
        let excluded = excluded.storage_event_kinds();
        selected.retain(|kind| !excluded.contains(kind));
    }
    if let Some(kinds) = &extra.kinds {
        selected.retain(|kind| kinds.contains(kind));
    }
    if extra.record_key.is_some() {
        selected.retain(|kind| matches!(kind.as_str(), "RecordChanged" | "RecordVersionChanged"));
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::RawQueryParams;

    #[test]
    fn history_filter_intersection_distinguishes_empty_from_omitted() {
        let params = QueryParams::try_from(RawQueryParams {
            event_type: Some("record".into()),
            exclude_type: Some("record,record".into()),
            ..Default::default()
        })
        .unwrap();
        assert!(event_kinds(&params).is_empty());
        let params = QueryParams::try_from(RawQueryParams {
            kind: Some("RecordVersionChanged,RecordChanged,RecordChanged".into()),
            record_key: Some("text:Case, and space ".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            event_kinds(&params),
            ["RecordChanged", "RecordVersionChanged"]
        );
        let mut keys = BTreeMap::new();
        params.history_filters.insert_cursor_keys(&mut keys);
        assert_eq!(keys["kind"], "RecordChanged,RecordVersionChanged");
        assert_eq!(keys["record_key"], "text:Case, and space ");
    }
}
