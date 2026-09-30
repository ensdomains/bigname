//! Production readers over the owned key families (docs/projections.md, "Owned key families").
//! Names, control, records and topology share the selected family publication.
pub mod control;
pub mod name;
pub mod position;
pub mod records;
pub mod topology;

#[cfg(test)]
mod id_index_plan_tests;

pub(crate) use name::read_snapshot;
