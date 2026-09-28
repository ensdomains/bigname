//! The continuation cursor of a current-state list (TYR-36 D10).
//!
//! A cursor holds what the list is (its sort and filters), the position of the last row it
//! returned, and the request's `at` token when the request pinned `at`. It holds no publication,
//! generation or evaluation time. A continuation reads whatever is published when it runs and
//! returns the rows after the position. docs/api-v1.md "Current-state list cursors" states what
//! a client sees:
//!
//! - The row the cursor came from need not still exist or still sort there; the page starts
//!   after the position either way. A row whose sort key moved between pages can be returned
//!   again or not at all, and rows published since the first page can appear.
//! - A position after the last row answers an empty page with no `next_cursor`.
//! - A cursor that does not decode, belongs to another list, carries other filters, or carries
//!   anything this contract does not write (the publication token, evaluation time or
//!   generation that cursors issued before it carried) answers `400 invalid_input`.
//! - With `at`, the cursor binds that `at` token: the continuation must send the same `at`
//!   (else 400), and once a later block is published the route answers `409 stale` before
//!   reading rows (the route compares the captured publication's block with the pinned one).
//!   The pin is a chain position, not a publication generation: a same-block rebuild is not
//!   detected, since holding a generation is the binding this contract dropped.
//!
//! Only a publication that lands during one request's own read refuses that request
//! (`CollectionSnapshot::finish`, 409 asking for a retry); the same cursor then continues.
//!
//! Adopting it in a route, replacing `CollectionSnapshot::validate_cursor`/`bind_cursor` and
//! any publication or generation field the route wrote into its cursor:
//!
//! ```ignore
//! let list = ListCursor::new(SORT, filters);            // .pinned_at(Some(at_token)) with `at`
//! let position = list.read(params.cursor.as_deref(), &POSITION_KEYS)?;  // Option<ListPosition>
//! let storage_cursor = position.map(|p| storage_cursor_from(&p)).transpose()?;
//! // Capture the publication without the cursor, and do not mark the request as continuing:
//! // An expiry filter takes this request's `snapshot.evaluated_at()`, never a cursor's time.
//! let snapshot = CollectionSnapshot::capture_for_namespace(&state, None, namespace).await?;
//! // ... read the page after `storage_cursor` ...
//! let next_cursor = page.next_cursor.map(|c| list.next(position_of(&c)));
//! let meta = snapshot.finish(&state).await?;           // the same-request recheck stays
//! ```

use std::collections::BTreeMap;

use super::V2Result;
use super::cursor::{Payload, decode, encode, invalid_cursor_error};

/// What a continuation must match: the list's sort and filters, and the `at` token when the
/// request pinned `at`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListCursor {
    sort: String,
    filters: BTreeMap<String, String>,
    at: Option<String>,
    /// Filter keys whose value `check_shape` leaves to the full `read`, because the route knows
    /// it only after reading state (search's namespace anchor).
    deferred: Vec<String>,
}

/// The position of the last row a page returned: the list's keyset fields by name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListPosition(BTreeMap<String, String>);

impl ListCursor {
    pub(crate) fn new(sort: impl Into<String>, filters: BTreeMap<String, String>) -> Self {
        Self {
            sort: sort.into(),
            filters,
            at: None,
            deferred: Vec::new(),
        }
    }

    /// Leaves the value of filter `key` to the full `read`; `check_shape` still requires the key.
    pub(crate) fn deferring(mut self, key: &str) -> Self {
        self.deferred.push(key.to_owned());
        self
    }

    /// Binds the request's `at` token (`None` for a latest read, which binds nothing).
    pub(crate) fn pinned_at(mut self, at: Option<String>) -> Self {
        self.at = at;
        self
    }

    /// The position `cursor` continues from, or `None` without a cursor. The cursor must be one
    /// this list wrote: no top-level field the format does not define, the same sort, filters
    /// and `at`, exactly the `keys` as its position, each a non-blank string without a NUL
    /// (which PostgreSQL text cannot hold), and nothing else; otherwise `400 invalid_input`. A
    /// position no row holds is valid.
    pub(crate) fn read(
        &self,
        cursor: Option<&str>,
        keys: &[&str],
    ) -> V2Result<Option<ListPosition>> {
        let Some(payload) = self.read_shape(cursor, keys, true)? else {
            return Ok(None);
        };
        if payload.snapshot != self.at {
            return Err(invalid_cursor_error());
        }
        Ok(Some(ListPosition(payload.last_item)))
    }

    /// The checks of [`Self::read`] a route can run before it reads anything: everything but
    /// the `at` token's value and the deferred filter values. `pinned` says whether the request
    /// sent `at`, so a cursor with a pin on a request without one (or the reverse) is refused
    /// here too.
    pub(crate) fn check_shape(
        &self,
        cursor: Option<&str>,
        keys: &[&str],
        pinned: bool,
    ) -> V2Result<()> {
        match self.read_shape(cursor, keys, false)? {
            Some(payload) if payload.snapshot.is_some() != pinned => Err(invalid_cursor_error()),
            _ => Ok(()),
        }
    }

    /// The one decode-and-validate path: every check but the `at` pin, comparing deferred
    /// filter values only when `all_filters`.
    fn read_shape(
        &self,
        cursor: Option<&str>,
        keys: &[&str],
        all_filters: bool,
    ) -> V2Result<Option<Payload>> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let payload = decode(cursor)?;
        let filters_match = payload.filters.len() == self.filters.len()
            && self.filters.iter().all(|(key, value)| {
                payload.filters.get(key).is_some_and(|sent| {
                    sent == value || (!all_filters && self.deferred.contains(key))
                })
            });
        let matches = only_known_fields(cursor)
            && payload.sort == self.sort
            && filters_match
            && payload.evaluated_at.is_none()
            && payload.last_item.len() == keys.len()
            && keys.iter().all(|key| {
                payload
                    .last_item
                    .get(*key)
                    .is_some_and(|value| !value.trim().is_empty() && !value.contains('\0'))
            });
        if !matches {
            return Err(invalid_cursor_error());
        }
        Ok(Some(payload))
    }

    /// The continuation after `position`.
    pub(crate) fn next(&self, position: ListPosition) -> String {
        encode(&Payload::new(
            self.sort.clone(),
            self.filters.clone(),
            position.0,
            self.at.clone(),
        ))
    }
}

/// Whether the cursor's JSON object has only the fields a list cursor writes. The typed decode
/// ignores unknown fields and reads an explicit `"evaluated_at": null` as absent, and the encoder
/// omits `evaluated_at` for a list cursor, so its key is refused whatever its value.
fn only_known_fields(cursor: &str) -> bool {
    const FIELDS: [&str; 5] = ["version", "sort", "filters", "last_item", "snapshot"];
    hex::decode(cursor)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|value| {
            value
                .as_object()
                .map(|object| object.keys().all(|key| FIELDS.contains(&key.as_str())))
        })
        .unwrap_or(false)
}

impl ListPosition {
    pub(crate) fn new<K: Into<String>>(fields: impl IntoIterator<Item = (K, String)>) -> Self {
        Self(
            fields
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    /// One position field; a key the list did not read is an invalid cursor.
    pub(crate) fn get(&self, key: &str) -> V2Result<&str> {
        self.0
            .get(key)
            .map(String::as_str)
            .ok_or_else(invalid_cursor_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::error::ErrorCode;

    const KEYS: [&str; 2] = ["name", "namehash"];

    fn list() -> ListCursor {
        ListCursor::new(
            "name_asc",
            BTreeMap::from([("namespace".to_owned(), "ens".to_owned())]),
        )
    }

    fn position() -> ListPosition {
        ListPosition::new([
            ("name", "beta.eth".to_owned()),
            ("namehash", "0xbeta".to_owned()),
        ])
    }

    fn refused(list: &ListCursor, cursor: &str) {
        let error = list
            .read(Some(cursor), &KEYS)
            .expect_err("the cursor must be refused");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn a_cursor_round_trips_its_position_and_holds_nothing_else() {
        let cursor = list().next(position());
        assert_eq!(list().read(Some(&cursor), &KEYS).unwrap(), Some(position()));
        assert_eq!(list().read(None, &KEYS).unwrap(), None);
        let payload = decode(&cursor).unwrap();
        assert_eq!(payload.snapshot, None);
        assert_eq!(payload.evaluated_at, None);
        assert_eq!(position().get("name").unwrap(), "beta.eth");
        assert_eq!(
            position().get("other").unwrap_err().code(),
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn a_cursor_binds_sort_filters_keys_and_at() {
        let cursor = list().next(position());
        refused(&ListCursor::new("name_desc", list().filters), &cursor);
        refused(&ListCursor::new("name_asc", BTreeMap::new()), &cursor);
        refused(&list().pinned_at(Some("at-1".to_owned())), &cursor);
        let pinned = list().pinned_at(Some("at-1".to_owned()));
        let pinned_cursor = pinned.next(position());
        assert_eq!(
            pinned.read(Some(&pinned_cursor), &KEYS).unwrap(),
            Some(position())
        );
        refused(&list(), &pinned_cursor);
        refused(&list().pinned_at(Some("at-2".to_owned())), &pinned_cursor);
        assert!(list().read(Some(&cursor), &["name"]).is_err());
        assert!(
            list()
                .read(Some(&cursor), &["name", "namehash", "namespace"])
                .is_err()
        );
    }

    #[test]
    fn malformed_and_publication_bound_cursors_are_refused() {
        for malformed in ["", "zz", "00", "7b7d"] {
            refused(&list(), malformed);
        }
        let mut timed = decode(&list().next(position())).unwrap();
        timed.evaluated_at = Some("2026-09-28T00:00:00Z".to_owned());
        refused(&list(), &encode(&timed));
        let mut published = decode(&list().next(position())).unwrap();
        published.snapshot = Some("publication-0xab".to_owned());
        refused(&list(), &encode(&published));
        let mut generation = decode(&list().next(position())).unwrap();
        generation
            .last_item
            .insert("generation".to_owned(), "1".to_owned());
        refused(&list(), &encode(&generation));
        let mut raw: serde_json::Value =
            serde_json::from_slice(&hex::decode(list().next(position())).unwrap()).unwrap();
        raw["route"] = serde_json::json!("names");
        refused(&list(), &hex::encode(serde_json::to_vec(&raw).unwrap()));
        let mut raw: serde_json::Value =
            serde_json::from_slice(&hex::decode(list().next(position())).unwrap()).unwrap();
        raw["evaluated_at"] = serde_json::Value::Null;
        refused(&list(), &hex::encode(serde_json::to_vec(&raw).unwrap()));
        let nul = list().next(ListPosition::new([
            ("name", "beta\0.eth".to_owned()),
            ("namehash", "0xbeta".to_owned()),
        ]));
        refused(&list(), &nul);
        let empty = list().next(ListPosition::new([
            ("name", " ".to_owned()),
            ("namehash", "0xbeta".to_owned()),
        ]));
        refused(&list(), &empty);
    }
}
