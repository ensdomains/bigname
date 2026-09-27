//! Shadow readers over the owned key families of TYR-36 (docs/projections.md, "Owned key
//! families"), first group: who controls a name. They compute the registration and control
//! blocks of `name_current` (F2a with F1 facts), the NameWrapper masks (F2b), registry ownership
//! and the registry binding (F2c), and raw permissions with account approvals (F8, F9) from the
//! per-key family rows the Project phase fills after each publication. Nothing in the API calls
//! them: every served response still comes from today's tables, and the test harnesses run them
//! beside the production readers and compare the two (docs/glossary.md, "Shadow read").
pub mod compare;
pub mod lifecycle;
pub mod permissions;
pub mod position;
pub mod registry;
pub mod rows;
pub mod wrapper;
