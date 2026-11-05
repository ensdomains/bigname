//! The rows of the child listing family (`project_child_listing`, F17): one row per child a
//! parent lists at a publication, composed by the children relation ([`push_listed`]) for the
//! block Project publishes, so the subnames page, the registry labels page and the child counts
//! read stored rows through indexes instead of composing every child of a parent.
//!
//! The stored name is the chain-data spelling ([`Relation::Stored`]), keyed for the name sort by
//! itself, or by its first characters when it is too long for a btree entry; the timestamps are
//! the child's name summary's, a missing one stored as [`MISSING_TIMESTAMP`] so that every sort
//! key is a plain btree column. The numeric keys are stored with no trailing zeros
//! (`trim_scale`): whole seconds, as block times are, so a journalled before-image, which undo
//! reads as JSON numbers, restores the row byte for byte (`bigname_project` `families/store.rs`).
//! `EXTRACT(EPOCH ...)` alone gives six fractional zeros, which that round trip drops.
//!
//! Each row also keeps the label hashes its stored name still spells as `[labelhash]`
//! (`unhealed_labelhashes`, from the same expressions as the name): a preimage observation derives
//! again only the rows that hold one of the observed name's labels, on its own chain at its block
//! and on every other chain at that chain's next block (`bigname_project`
//! `families/derived/children/sources.rs`). A preimage written outside a block, such as the
//! rainbow import (TYR-373), heals the stored names it would spell only when their rows are
//! derived again for another reason.
use sqlx::{Execute, Postgres, QueryBuilder, postgres::PgArguments};

use super::children::{ListingBlock, Parents, Relation, push_listed};

/// The stored sort key of a missing timestamp: below every real one, so a child without one sorts
/// first ascending and last descending.
pub const MISSING_TIMESTAMP: i64 = -1;

/// A stored name longer than this many bytes is keyed by its first [`DISPLAY_KEY_CHARS`]
/// characters: a btree entry larger than about 2.7 KB fails the insert.
pub const DISPLAY_KEY_BYTES: i64 = 2000;
/// The characters a long name's key keeps, at most four bytes each.
pub const DISPLAY_KEY_CHARS: i64 = 500;

/// The listing family's columns, in table order.
pub const LISTING_COLUMNS: &str = "chain_id, parent_logical_name_id, child_logical_name_id, \
    namespace, display_key, display_name, namehash, labelhash, owner, registrant, \
    registry_authority, lifecycle_shadow, wrapper_held, released_lease, token_holder, \
    registry_address, summary_owner, registration_status, expires_key, registered_key, \
    recompose_at, unhealed_labelhashes";

/// `head` (an `INSERT INTO <table> (LISTING_COLUMNS)`, or the start of a statement that reads
/// the rows as a subquery) followed by the statement that selects the listing rows of `parents`
/// at `block`, each column named as in [`LISTING_COLUMNS`], narrowed to the child nodes
/// `children` (lowercase, no namespace) when given.
pub fn listing_rows<'a>(
    head: &str,
    block: ListingBlock<'a>,
    parents: &'a [String],
    children: Option<&'a [String]>,
) -> QueryBuilder<'a, Postgres> {
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "/* storage:families.topology.child_listing_rows */ {head} WITH "
    ));
    push_listed(
        &mut builder,
        Parents::Many(parents),
        None,
        children,
        None,
        Relation::Stored(block),
    );
    builder.push(format!(
        ") SELECT children.chain_id, children.parent_logical_name_id,
                  children.child_logical_name_id, children.namespace,
                  CASE WHEN octet_length(children.canonical_display_name) <= {DISPLAY_KEY_BYTES}
                       THEN children.canonical_display_name
                       ELSE left(children.canonical_display_name, {DISPLAY_KEY_CHARS})
                  END AS display_key,
                  children.canonical_display_name AS display_name, children.namehash,
                  children.labelhash, children.owner, children.registrant,
                  children.registry_authority, children.lifecycle_shadow, children.wrapper_held,
                  children.released_lease, children.token_holder,
                  children.label_registry AS registry_address, children.summary_owner,
                  children.registration_status,
                  trim_scale(COALESCE(children.expires_at, {MISSING_TIMESTAMP})) AS expires_key,
                  trim_scale(COALESCE(children.registered_at, {MISSING_TIMESTAMP}))
                      AS registered_key,
                  trim_scale(children.recompose_at) AS recompose_at,
                  children.unhealed_labelhashes
           FROM children"
    ));
    builder
}

/// [`listing_rows`] as its SQL and its bound arguments, both owned, for `sqlx::query_with`. A
/// caller whose future must be `Send` across generic lifetimes, such as the Project writer under
/// the phase runner, holds these across its await instead of a builder that borrows its binds:
/// the compiler cannot prove such a builder `Send` for every lifetime.
pub fn listing_statement(
    head: &str,
    block: ListingBlock<'_>,
    parents: &[String],
    children: Option<&[String]>,
) -> Result<(String, PgArguments), sqlx::Error> {
    let mut builder = listing_rows(head, block, parents, children);
    let arguments = builder
        .build()
        .take_arguments()
        .map_err(sqlx::Error::Encode)?
        .unwrap_or_default();
    Ok((builder.into_sql(), arguments))
}
