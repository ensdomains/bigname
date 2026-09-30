//! Project's facade for the shared canonical family position and ordinal parser.
pub(crate) use bigname_storage::families::position::Position;
pub use bigname_storage::families::position::emission_ordinal;

#[cfg(test)]
#[path = "position_tests.rs"]
mod tests;
