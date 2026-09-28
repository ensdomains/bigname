//! How the permission page and summary reads fail: a read that reaches a chain whose owned key
//! families are not servable (a rebuild in flight) answers the
//! stale 409 a fence gives; any other failure is internal.
use super::super::{SnapshotReadResource, V2Error, name_rows_error};

pub(super) fn page_error() -> impl FnOnce(anyhow::Error) -> V2Error {
    name_rows_error(SnapshotReadResource::Resource, |_| {
        V2Error::internal_error("failed to load permissions")
    })
}

pub(super) fn support_error() -> impl FnOnce(anyhow::Error) -> V2Error {
    name_rows_error(SnapshotReadResource::Resource, |_| {
        V2Error::internal_error("failed to load permission support")
    })
}
