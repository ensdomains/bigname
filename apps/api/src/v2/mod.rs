#![allow(dead_code, unused_imports)]

mod chains;
mod cursor;
mod envelope;
mod error;
mod name_record;
mod name_records;
mod name_records_inventory;
mod params;
mod router;
mod snapshots;
mod subnames;
mod vocab;

pub(crate) use chains::{numeric_to_slug, slug_to_numeric};
pub(crate) use cursor::{Payload as CursorPayload, V2_CURSOR_VERSION, decode, encode};
pub(crate) use envelope::{AsOf, Envelope, Meta, Page};
pub(crate) use error::{ErrorBody, ErrorCode, ErrorEnvelope, V2Error, V2Result};
pub(crate) use name_record::{NameRecord, build_name_record, classify_registration_status};
pub(crate) use name_records::{
    NameRecords, VerifiedRecordLookup, build_auto_name_records, build_indexed_name_records,
    build_verified_name_records, indexed_records_requiring_verified_fallback,
};
pub(crate) use name_records_inventory::{default_requested_records, validate_product_record};
pub(crate) use params::{
    AtSelector, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, QueryParams, RawQueryParams, RequestSource,
    SortOrder,
};
pub(crate) use snapshots::{
    as_of_meta, consistency_for_finality, decode_at_token, encode_at_token, resolve_v2_snapshot,
};
pub(crate) use subnames::{Subname, build_subname, subname_cursor_payload, subname_storage_cursor};
pub(crate) use vocab::{
    Completeness, Finality, RegistrationStatus, Relation, Resolver, Source, Status,
};

use axum::Router;

use crate::state::AppState;

pub(crate) fn router() -> Router<AppState> {
    router::router()
}
