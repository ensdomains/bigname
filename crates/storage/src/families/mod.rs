//! Production readers over the owned key families (docs/projections.md, "Owned key families").
//! Names, control, records and topology share the selected family publication.
pub mod alias_path;
#[path = "name/basenames_context.rs"]
pub(crate) mod basenames_context;
pub mod control;
pub mod lookup;
pub mod name;
pub mod position;
pub mod records;
pub mod search_dictionary;
pub mod topology;

#[cfg(test)]
mod id_index_plan_tests;
#[cfg(test)]
mod name_order_plan_tests;
#[cfg(test)]
pub(crate) mod textless_tests;

pub(crate) use name::read_snapshot;
