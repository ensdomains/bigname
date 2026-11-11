//! The address listing family F18 (`project_address_listing`): one row per (address, name) that
//! `GET /v1/addresses/{address}/names` lists, with the relations as flags and the sort keys, so
//! the route can read an address's rows through an index instead of composing every candidate
//! name. The rows come from the composition and relation fold the route reads with
//! (`bigname_storage::families::records::address_listing`), in the block's transaction; each
//! changed row is journalled with its before-image and written, and undo restores it from the
//! journal like any other family.
//!
//! A block derives again the rows of every name it can have moved: the names its summary refresh
//! composed, whose rows come from that same composition (`summary.rs`, `stage`), and the names the
//! lookup refresh composed besides, the address fold and controller candidate keys among them
//! (`lookup/work.rs`), and the ids the block's index refresh touched. A name's rows are derived
//! whole: every stored row of the name is replaced by its fresh rows. An id that composes no name
//! row can be a surface-less ENSv1 registry child, whose rows the children relation derives
//! (`topology::registry_child_listing_statement`), together with the children and the parents
//! the child listing's work list names (`children/work.rs`): a parent's own facts decide which of
//! its children it lists, so a parent there has its registry children derived whole.
//!
//! The name is the chain-data spelling. A preimage observation, on this chain or another
//! (`children/sources.rs`, read once for both families before the shared watermark moves), spells
//! again the stored rows that still hold one of its labels as `[labelhash]`
//! (`project_address_listing_unhealed_idx`): a name row from its surface, a registry child through
//! the children relation. An undo restores the rows its block healed to `[labelhash]` while
//! `label_preimages` keeps the label, so it spells them again at the restored block, journalling
//! nothing (`derive_heals`).
//!
//! `registration_lead` is decided last, from the stored rows of every (address, resource) group a
//! change touched, never from a chunk's partial view. A rebuild's composition pass writes the rows
//! with no lead and decides every group's lead in its leads stream (`compose_leads.rs`), by the
//! same rank (`lead_rank`).
use bigname_storage::families::{
    name::{FamilyPublication, compose_address_listing},
    records::{
        ADDRESS_LISTING_COLUMNS, AddressListingRows, address_listing_respell_sql,
        address_listing_statement,
    },
    topology::{ListingBlock, registry_child_listing_statement, registry_child_parents},
};
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, families::input};

#[cfg(test)]
mod plan_tests;
mod statements;
pub(crate) use statements::lead_rank;
use statements::{
    CHANGES, CLEAR_CHANGES, COMPOSED, DELETE, EXTRA, GROUPS, HEALS_ONLY, INSERT, JOURNAL,
    KEY_TARGET, LABEL_TARGET, LEADS, NAME, OBSERVED_LABELS, PREPARE, REGISTRY_CHILD,
    REGISTRY_HOLDERS, REGISTRY_PARENT, SCOPE, UNCOMPOSED,
};

/// Names composed, and registry children derived, per statement.
const CHUNK: usize = 1000;

/// What a listing step wrote: the rows written or removed and the undo rows journalled for them.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Refreshed {
    pub(crate) rows: u64,
    pub(crate) undo_rows: u64,
}

impl std::ops::AddAssign for Refreshed {
    fn add_assign(&mut self, other: Self) {
        self.rows += other.rows;
        self.undo_rows += other.undo_rows;
    }
}

/// What a block derives beyond the names its summary refresh staged.
pub(crate) struct Work<'a> {
    /// The family marker's block the block follows, -1 with none.
    pub(crate) after: i64,
    /// The name ids the block's index refresh touched (`derived::touched`).
    pub(crate) touched: &'a [String],
    /// The children and the parents of the child listing's work list (`children/work.rs`).
    pub(crate) children: &'a [String],
    pub(crate) parents: &'a [String],
    /// The labels other chains observed above the watermarks (`children/sources.rs`).
    pub(crate) labels: &'a [String],
}

/// Create the step's temporary tables, or empty them for a new step in the same transaction.
pub(crate) async fn prepare(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in PREPARE {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to prepare the address listing refresh", error)
            })?;
    }
    Ok(())
}

/// Stage the fresh rows of `names`, composed by the caller at the publication: each name's stored
/// rows are replaced by them when the step writes.
pub(crate) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    names: &[String],
    rows: &AddressListingRows,
) -> Result<()> {
    scope(transaction, NAME, names).await?;
    scope(transaction, COMPOSED, rows.composed()).await?;
    if rows.is_empty() {
        return Ok(());
    }
    let head = format!(
        "/* project:families.derived.address_listing_fresh */
         INSERT INTO pg_temp.bigname_address_listing_fresh ({ADDRESS_LISTING_COLUMNS})"
    );
    let (sql, arguments) = address_listing_statement(&head, chain_id, rows).map_err(|error| {
        ProjectError::database("failed to bind the address listing rows", error)
    })?;
    sqlx::query_with(&sql, arguments)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to derive the address listing rows", error)
        })?;
    Ok(())
}

/// Compose and stage `names` at `publication`, a chunk at a time.
async fn compose_and_stage(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &FamilyPublication,
    names: &[String],
) -> Result<()> {
    for chunk in names.chunks(CHUNK) {
        let rows = compose_address_listing(transaction, publication, chunk)
            .await
            .map_err(|error| {
                let message = format!(
                    "failed to compose the address listing of block {} of chain {}: {error:#}",
                    publication.block_number, publication.chain_id
                );
                if error
                    .downcast_ref::<bigname_storage::families::control::lifecycle::RequiredOwnerMissing>()
                    .is_some()
                {
                    ProjectError::data_integrity(message)
                } else {
                    ProjectError::transient(message)
                }
            })?;
        stage(transaction, &publication.chain_id, chunk, &rows).await?;
    }
    Ok(())
}

/// Derive again, journal and write the listing rows block `block` moved, at `publication`, after
/// the summary refresh staged the names it composed (`stage`).
pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    publication: &FamilyPublication,
    block: &input::BlockHeader,
    work: Work<'_>,
) -> Result<Refreshed> {
    let chain_id = publication.chain_id.as_str();
    let extra: Vec<String> = sqlx::query_scalar(EXTRA)
        .bind(work.touched)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the address listing's further names", error)
        })?;
    compose_and_stage(transaction, publication, &extra).await?;
    let mut labels: Vec<String> = sqlx::query_scalar(OBSERVED_LABELS)
        .bind(chain_id)
        .bind(block.number)
        .bind(work.after)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the labels a family block observed", error)
        })?;
    labels.extend(work.labels.iter().cloned());
    labels.sort();
    labels.dedup();
    let mut children = work.children.to_vec();
    if !labels.is_empty() {
        let holders: Vec<String> = sqlx::query_scalar(REGISTRY_HOLDERS)
            .bind(chain_id)
            .bind(&labels)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to read the registry children a label heals", error)
            })?;
        children.extend(holders);
    }
    children.sort();
    children.dedup();
    let mut refreshed =
        finish(transaction, chain_id, block, &children, work.parents, false).await?;
    if !labels.is_empty() {
        let respelled = format!(
            "/* project:families.derived.address_listing_respell_labels */
             INSERT INTO pg_temp.bigname_address_listing_change {}",
            address_listing_respell_sql(LABEL_TARGET)
        );
        let changed = sqlx::query(&respelled)
            .bind(chain_id)
            .bind(&labels)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to spell the healed address listing rows", error)
            })?
            .rows_affected();
        refreshed += apply(transaction, chain_id, block, changed, true).await?;
    }
    refreshed += leads(transaction, chain_id, block).await?;
    Ok(refreshed)
}

/// Derive the listing rows of one chunk of a rebuild's composition pass at the pass block
/// `block`, after the chunk's summaries staged the names they composed: `others` are the chunk's
/// ids the summaries do not compose, each of which can be a surface-less registry child. The
/// chunk decides no registration lead: a group's rows can lie in several workers' chunks, so the
/// pass's leads stream decides them once every chunk committed (`compose_leads.rs`).
pub(crate) async fn pass_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    others: &[String],
) -> Result<Refreshed> {
    scope(transaction, NAME, others).await?;
    finish(transaction, chain_id, block, &[], &[], false).await
}

/// `refresh` for an undo (`families/undo.rs`): at the restored block `block`, which journals
/// nothing, spell again the restored rows `keys` (address, name, registry child) that still spell
/// a label as `[labelhash]`, writing only the rows it spells further. The undo's other rows stay
/// as the journal restored them.
pub(crate) async fn derive_heals(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    keys: &[(String, String, bool)],
) -> Result<Refreshed> {
    if keys.is_empty() {
        return Ok(Refreshed::default());
    }
    prepare(transaction).await?;
    let (addresses, names): (Vec<String>, Vec<String>) = keys
        .iter()
        .filter(|(_, _, registry_child)| !registry_child)
        .map(|(address, name, _)| (address.clone(), name.clone()))
        .unzip();
    let mut children: Vec<String> = keys
        .iter()
        .filter(|(_, _, registry_child)| *registry_child)
        .map(|(_, name, _)| name.clone())
        .collect();
    children.sort();
    children.dedup();
    let mut refreshed = finish(transaction, chain_id, block, &children, &[], true).await?;
    if !names.is_empty() {
        let respelled = format!(
            "/* project:families.derived.address_listing_respell_keys */
             INSERT INTO pg_temp.bigname_address_listing_change {}",
            address_listing_respell_sql(KEY_TARGET)
        );
        let changed = sqlx::query(&respelled)
            .bind(chain_id)
            .bind(&addresses)
            .bind(&names)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to spell the restored address listing rows", error)
            })?
            .rows_affected();
        refreshed += apply(transaction, chain_id, block, changed, true).await?;
    }
    refreshed += leads(transaction, chain_id, block).await?;
    Ok(refreshed)
}

/// Derive the registry-child rows of the staged ids that composed no name row, of `children` and
/// of the `parents` derived whole, then diff the scope's stored rows against the fresh ones, and
/// journal (when the block journals) and write what changed. `heals_only` keeps only the rows it
/// spells further (`HEALS_ONLY`).
async fn finish(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    children: &[String],
    parents: &[String],
    heals_only: bool,
) -> Result<Refreshed> {
    scope(transaction, REGISTRY_CHILD, children).await?;
    let mut candidates: Vec<String> = sqlx::query_scalar(UNCOMPOSED)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the names that composed no row", error)
        })?;
    candidates.extend(children.iter().cloned());
    candidates.sort();
    candidates.dedup();
    let listing = ListingBlock {
        chain_id,
        number: block.number,
        timestamp_seconds: block.timestamp_seconds,
    };
    let head = format!(
        "/* project:families.derived.address_listing_registry_fresh */
         INSERT INTO pg_temp.bigname_address_listing_fresh ({ADDRESS_LISTING_COLUMNS})"
    );
    for chunk in candidates.chunks(CHUNK) {
        let found = registry_child_parents(transaction, chain_id, chunk)
            .await
            .map_err(|error| {
                ProjectError::transient(format!(
                    "failed to read the parents of registry children: {error:#}"
                ))
            })?;
        if found.is_empty() {
            continue;
        }
        let nodes: Vec<String> = chunk
            .iter()
            .filter_map(|id| {
                id.split_once(':')
                    .map(|(_, node)| node.to_ascii_lowercase())
            })
            .collect();
        registry_rows(transaction, (&head, listing), &found, Some((&nodes, chunk))).await?;
    }
    scope(transaction, REGISTRY_PARENT, parents).await?;
    for chunk in parents.chunks(CHUNK) {
        registry_rows(transaction, (&head, listing), chunk, None).await?;
    }
    let mut changed = execute(
        transaction,
        CHANGES,
        chain_id,
        "failed to diff the address listing",
    )
    .await?;
    if heals_only && changed > 0 {
        changed -= sqlx::query(HEALS_ONLY)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to keep the undo's heals", error))?
            .rows_affected();
    }
    apply(transaction, chain_id, block, changed, true).await
}

async fn registry_rows(
    transaction: &mut Transaction<'_, Postgres>,
    (head, listing): (&str, ListingBlock<'_>),
    parents: &[String],
    narrowed: Option<(&[String], &[String])>,
) -> Result<()> {
    let (sql, arguments) = registry_child_listing_statement(
        head,
        listing,
        parents,
        narrowed.map(|(nodes, _)| nodes),
        narrowed.map(|(_, candidates)| candidates),
    )
    .map_err(|error| ProjectError::database("failed to bind the registry child rows", error))?;
    sqlx::query_with(&sql, arguments)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to derive the registry child rows", error)
        })?;
    Ok(())
}

/// Journal (when the block journals) and write the `changed` rows of the change table, noting
/// their (address, resource) groups for the lead step when `groups`, and empty the table.
async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    changed: u64,
    groups: bool,
) -> Result<Refreshed> {
    let mut refreshed = Refreshed::default();
    if changed == 0 {
        return Ok(refreshed);
    }
    if block.journal {
        refreshed.undo_rows += sqlx::query(JOURNAL)
            .bind(chain_id)
            .bind(block.number)
            .bind(&block.hash)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to journal the address listing", error)
            })?
            .rows_affected();
    }
    if groups {
        statement(
            transaction,
            GROUPS,
            "failed to note the address listing groups",
        )
        .await?;
    }
    execute(
        transaction,
        DELETE,
        chain_id,
        "failed to remove address listing rows",
    )
    .await?;
    statement(transaction, INSERT, "failed to write address listing rows").await?;
    statement(
        transaction,
        CLEAR_CHANGES,
        "failed to clear the address listing changes",
    )
    .await?;
    refreshed.rows += changed;
    Ok(refreshed)
}

/// Decide `registration_lead` again for every (address, resource) group a write touched, from the
/// stored rows, and write the rows whose flag moves.
async fn leads(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
) -> Result<Refreshed> {
    let changed = execute(
        transaction,
        LEADS,
        chain_id,
        "failed to decide the registration leads",
    )
    .await?;
    apply(transaction, chain_id, block, changed, false).await
}

async fn scope(
    transaction: &mut Transaction<'_, Postgres>,
    kind: i16,
    ids: &[String],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(SCOPE)
        .bind(kind)
        .bind(ids)
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(|error| ProjectError::database("failed to scope the address listing", error))
}

async fn execute(
    transaction: &mut Transaction<'_, Postgres>,
    sql: &str,
    chain_id: &str,
    context: &str,
) -> Result<u64> {
    sqlx::query(sql)
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map(|result| result.rows_affected())
        .map_err(|error| ProjectError::database(context, error))
}

async fn statement(
    transaction: &mut Transaction<'_, Postgres>,
    sql: &str,
    context: &str,
) -> Result<()> {
    sqlx::query(sql)
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(|error| ProjectError::database(context, error))
}
