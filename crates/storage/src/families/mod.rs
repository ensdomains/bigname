//! Production readers over the owned key families (docs/projections.md, "Owned key families").
//! Names, control, records and topology share the selected family publication.
pub mod control;
pub mod name;
pub mod records;
pub mod topology;

#[cfg(test)]
mod id_index_plan_tests;

pub(crate) use name::read_snapshot;

/// The ids among `ids` that parse as uuids. The loaders bind these against a uuid id column
/// itself, never `column::text`, so the planner can probe the column's index. Every id they
/// are given was read from a uuid column, so none is dropped in practice; one that did not
/// parse could not have equalled a uuid's text either.
pub(crate) fn uuid_ids<'a>(ids: impl IntoIterator<Item = &'a String>) -> Vec<uuid::Uuid> {
    ids.into_iter()
        .filter_map(|id| uuid::Uuid::try_parse(id).ok())
        .collect()
}
