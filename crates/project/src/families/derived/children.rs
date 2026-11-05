//! The child listing family F17 (`project_child_listing`) and its counts
//! (`project_child_listing_count`). After a block's name summaries are composed, the listing
//! rows of every child the block can have moved are derived again by the children relation
//! (`bigname_storage::families::topology::listing_statement`) at the block, compared with the
//! stored rows, and every changed row is journalled with its before-image and written, in the
//! block's transaction. The counts move by the same diff. Undo restores both from the journal,
//! like any other family. It then derives the restored rows that still spell a label as
//! `[labelhash]` again at the restored block, by the same statement, and writes only those it
//! spells further (`derive_heals`, `families/undo.rs`). Interpret's redo never removes a label
//! from `label_preimages`, so a row the undone block healed stays healed, as a rebuild at the
//! restored block spells it.
//!
//! The work list (`work.rs`) has two arms. A child is derived again, under every parent that can
//! list it, when the block touched a fact of its own: its name summary, an ENSv1 or Basenames
//! edge naming it, an ENSv2 registration of it, its registry node, its NameWrapper row or a
//! registrar lifecycle row of its node, a preimage observation of its name, an observation of a
//! label its stored name still spells as `[labelhash]` (its own or its parent's, kept in
//! `unhealed_labelhashes` and found by that column's index), or the second at which the
//! migration gate stops keeping it (`recompose_at`). A parent's own facts decide every child it
//! lists: its migration state, its ENSv2 subregistry pointer, its surface, its raw name cleared
//! in place, and an address window of its subregistry. Such a parent is derived whole, by one
//! statement, and every row of it is journalled. An observation derives no parent whole.
//!
//! `label_preimages` is shared by every chain, so an observation on another chain also spells
//! this chain's stored names: each family step reads the other chains' observations above this
//! chain's watermark for each (`sources.rs`) and derives the rows that hold one of their labels,
//! as children. The limit: a preimage written outside a block, such as the rainbow import
//! (TYR-373), emits no observation, so a stored name it would spell heals only when its row is
//! derived again for another reason. The served name applies the preimage at read meanwhile.
//!
//! Inputs that change in place with no block, such as a lineage readability flip or a migration
//! registry association recorded after the parent's migration, are covered by the rebuild a hash
//! rotation forces, as for the name summaries (`summary.rs`).
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, families::input};

mod sources;
pub(crate) use sources::seed as seed_sources;
mod work;

/// Children derived per statement, to bound the working set.
const CHUNK: usize = 1000;

/// A parent arm above this many rows is logged: its rows and their before-images all land in one
/// block's transaction.
const LARGE_PARENT_ROWS: u64 = 100_000;

/// What a listing refresh wrote: the listing and count rows written or removed, the preimage
/// watermark rows written, and the undo rows journalled for them.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Refreshed {
    pub(crate) rows: u64,
    pub(crate) undo_rows: u64,
    /// The preimage watermark rows written (`sources.rs`).
    pub(crate) watermark_rows: u64,
}

/// Every parent that can list one of the children `$2` of chain `$1`: the parent of each of its
/// ENSv1 or Basenames edges, and each parent whose ENSv2 subregistry is an address of a registry
/// it has a registration in.
const CANDIDATE_PARENTS: &str = "/* project:families.derived.child_listing_parents */
    SELECT edge.namespace || ':' || edge.parent_node
    FROM unnest($2::text[]) child (id)
    JOIN project_child_edge_candidate edge
      ON edge.chain_id = $1 AND edge.namespace = split_part(child.id, ':', 1)
     AND edge.child_node = lower(split_part(child.id, ':', 2))
    UNION
    SELECT pointer.logical_name_id
    FROM project_child_registration_state registration
    JOIN contract_instance_addresses address
      ON address.chain_id = $1
     AND address.contract_instance_id::text = registration.registry_contract_instance_id
    JOIN project_parent_subregistry pointer
      ON pointer.chain_id = $1 AND pointer.subregistry_address = lower(address.address)
    WHERE registration.chain_id = $1 AND registration.logical_name_id = ANY($2)
    ORDER BY 1";

/// The refresh's temporary tables, emptied for this block, one statement each.
const PREPARE: [&str; 5] = [
    "/* project:families.derived.child_listing_prepare_fresh */
     CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_fresh
         (LIKE project_child_listing) ON COMMIT DROP",
    "/* project:families.derived.child_listing_prepare_scope */
     CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_scope
         (kind smallint NOT NULL, id text NOT NULL) ON COMMIT DROP",
    "/* project:families.derived.child_listing_prepare_change */
     CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_change
         (child_logical_name_id text NOT NULL, before_image jsonb, after_image jsonb)
         ON COMMIT DROP",
    "/* project:families.derived.child_listing_prepare_count_change */
     CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_count_change
         (parent_logical_name_id text NOT NULL, registry_address text NOT NULL,
          child_count bigint NOT NULL, before_image jsonb) ON COMMIT DROP",
    "/* project:families.derived.child_listing_prepare_truncate */
     TRUNCATE pg_temp.bigname_child_listing_fresh, pg_temp.bigname_child_listing_scope,
              pg_temp.bigname_child_listing_change, pg_temp.bigname_child_listing_count_change",
];

/// Scope kind of a parent derived whole.
const WHOLE_PARENT: i16 = 0;
/// Scope kind of a child derived under its candidate parents.
const CHILD: i16 = 1;

const SCOPE: &str = "/* project:families.derived.child_listing_scope */
    INSERT INTO pg_temp.bigname_child_listing_scope (kind, id)
    SELECT $1, id FROM unnest($2::text[]) scoped (id)";

/// The rows the scope's stored rows differ in from the fresh ones, keyed by child, with each
/// side's image (null for none). A child derived by both arms has one fresh row: both statements
/// run at the same block over the same rows.
const CHANGES: &str = "/* project:families.derived.child_listing_changes */
    INSERT INTO pg_temp.bigname_child_listing_change
    WITH fresh AS (
        SELECT DISTINCT ON (fresh.child_logical_name_id) fresh.*
        FROM pg_temp.bigname_child_listing_fresh fresh
        ORDER BY fresh.child_logical_name_id
    ), keys AS (
        SELECT listing.child_logical_name_id
        FROM pg_temp.bigname_child_listing_scope scope
        JOIN project_child_listing listing
          ON listing.chain_id = $1 AND listing.parent_logical_name_id = scope.id
        WHERE scope.kind = 0
        UNION SELECT scope.id FROM pg_temp.bigname_child_listing_scope scope WHERE scope.kind = 1
        UNION SELECT fresh.child_logical_name_id FROM fresh
    ), stored AS (
        SELECT listing.* FROM keys JOIN project_child_listing listing
          ON listing.chain_id = $1 AND listing.child_logical_name_id = keys.child_logical_name_id
    )
    SELECT keys.child_logical_name_id, to_jsonb(stored), to_jsonb(fresh)
    FROM keys
    LEFT JOIN stored ON stored.child_logical_name_id = keys.child_logical_name_id
    LEFT JOIN fresh ON fresh.child_logical_name_id = keys.child_logical_name_id
    WHERE to_jsonb(stored) IS DISTINCT FROM to_jsonb(fresh)";

/// An undo's derivation keeps only the rows it spells further: the same row with fewer labels left
/// as `[labelhash]` and nothing else changed but the name and its key (`derive_heals`).
const HEALS_ONLY: &str = "/* project:families.derived.child_listing_heals_only */
    DELETE FROM pg_temp.bigname_child_listing_change change
    WHERE NOT COALESCE(
        (change.before_image - '{display_key,display_name,unhealed_labelhashes}'::text[])
            = (change.after_image - '{display_key,display_name,unhealed_labelhashes}'::text[])
        AND (change.before_image -> 'unhealed_labelhashes')
            @> (change.after_image -> 'unhealed_labelhashes')
        AND (change.before_image -> 'unhealed_labelhashes')
            <> (change.after_image -> 'unhealed_labelhashes'),
        FALSE)";

/// The journal key text of a listing row, as `store::key_text` writes it.
const LISTING_JOURNAL: &str = "/* project:families.derived.child_listing_journal */
    INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key, before_image)
    SELECT $1, $2, $3, 'project_child_listing',
           '[' || to_json($1::text)::text || ',' || to_json(change.child_logical_name_id)::text
               || ']',
           change.before_image
    FROM pg_temp.bigname_child_listing_change change";

const LISTING_DELETE: &str = "/* project:families.derived.child_listing_delete */
    DELETE FROM project_child_listing listing
    USING pg_temp.bigname_child_listing_change change
    WHERE listing.chain_id = $1
      AND listing.child_logical_name_id = change.child_logical_name_id";

const LISTING_INSERT: &str = "/* project:families.derived.child_listing_insert */
    INSERT INTO project_child_listing
    SELECT DISTINCT ON (fresh.child_logical_name_id) fresh.*
    FROM pg_temp.bigname_child_listing_fresh fresh
    JOIN pg_temp.bigname_child_listing_change change
      ON change.child_logical_name_id = fresh.child_logical_name_id
     AND change.after_image IS NOT NULL
    ORDER BY fresh.child_logical_name_id";

/// The counts the diff moves. A parent derived whole is set to the size of its fresh rows; any
/// other parent moves by the diff: a removed row takes one from its parent's count and its label
/// registry's, an added row adds one. A key whose count stays has no row here.
const COUNT_CHANGES: &str = "/* project:families.derived.child_listing_count_changes */
    INSERT INTO pg_temp.bigname_child_listing_count_change
    WITH whole AS (
        SELECT scope.id AS parent FROM pg_temp.bigname_child_listing_scope scope
        WHERE scope.kind = 0
    ), delta AS (
        SELECT step.parent, step.registry, sum(step.change)::bigint AS change
        FROM pg_temp.bigname_child_listing_change change
        CROSS JOIN LATERAL (VALUES
            (change.before_image ->> 'parent_logical_name_id', '', -1),
            (change.before_image ->> 'parent_logical_name_id',
             change.before_image ->> 'registry_address', -1),
            (change.after_image ->> 'parent_logical_name_id', '', 1),
            (change.after_image ->> 'parent_logical_name_id',
             change.after_image ->> 'registry_address', 1)
        ) step (parent, registry, change)
        WHERE step.parent IS NOT NULL AND step.registry IS NOT NULL
          AND step.parent NOT IN (SELECT whole.parent FROM whole)
        GROUP BY step.parent, step.registry
    ), fresh AS (
        SELECT DISTINCT ON (fresh.child_logical_name_id)
               fresh.parent_logical_name_id, fresh.registry_address
        FROM pg_temp.bigname_child_listing_fresh fresh
        ORDER BY fresh.child_logical_name_id
    ), sized AS (
        SELECT step.parent, step.registry, count(*)::bigint AS child_count
        FROM fresh
        CROSS JOIN LATERAL (VALUES (fresh.parent_logical_name_id, ''),
                                   (fresh.parent_logical_name_id, fresh.registry_address))
            step (parent, registry)
        WHERE step.registry IS NOT NULL AND step.parent IN (SELECT whole.parent FROM whole)
        GROUP BY step.parent, step.registry
    ), targets AS (
        SELECT delta.parent, delta.registry,
               COALESCE(counter.child_count, 0) + delta.change AS child_count,
               to_jsonb(counter) AS before_image
        FROM delta
        LEFT JOIN project_child_listing_count counter
          ON counter.chain_id = $1 AND counter.parent_logical_name_id = delta.parent
         AND counter.registry_address = delta.registry
        UNION ALL
        SELECT keyed.parent, keyed.registry, COALESCE(sized.child_count, 0),
               to_jsonb(counter)
        FROM (SELECT sized.parent, sized.registry FROM sized
              UNION
              SELECT counter.parent_logical_name_id, counter.registry_address
              FROM whole JOIN project_child_listing_count counter
                ON counter.chain_id = $1 AND counter.parent_logical_name_id = whole.parent) keyed
        LEFT JOIN sized ON sized.parent = keyed.parent AND sized.registry = keyed.registry
        LEFT JOIN project_child_listing_count counter
          ON counter.chain_id = $1 AND counter.parent_logical_name_id = keyed.parent
         AND counter.registry_address = keyed.registry
    )
    SELECT targets.parent, targets.registry, targets.child_count, targets.before_image
    FROM targets
    WHERE targets.child_count <> COALESCE((targets.before_image ->> 'child_count')::bigint, 0)";

const NEGATIVE_COUNTS: &str = "/* project:families.derived.child_listing_negative_counts */
    SELECT count(*) FROM pg_temp.bigname_child_listing_count_change WHERE child_count < 0";

const COUNT_JOURNAL: &str = "/* project:families.derived.child_listing_count_journal */
    INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key, before_image)
    SELECT $1, $2, $3, 'project_child_listing_count',
           '[' || to_json($1::text)::text || ',' || to_json(change.parent_logical_name_id)::text
               || ',' || to_json(change.registry_address)::text || ']',
           change.before_image
    FROM pg_temp.bigname_child_listing_count_change change";

const COUNT_DELETE: &str = "/* project:families.derived.child_listing_count_delete */
    DELETE FROM project_child_listing_count counter
    USING pg_temp.bigname_child_listing_count_change change
    WHERE counter.chain_id = $1
      AND counter.parent_logical_name_id = change.parent_logical_name_id
      AND counter.registry_address = change.registry_address";

const COUNT_INSERT: &str = "/* project:families.derived.child_listing_count_insert */
    INSERT INTO project_child_listing_count
        (chain_id, parent_logical_name_id, registry_address, child_count)
    SELECT $1, change.parent_logical_name_id, change.registry_address, change.child_count
    FROM pg_temp.bigname_child_listing_count_change change
    WHERE change.child_count > 0";

/// Derive again, journal and write the listing rows and counts block `block` moved, after the
/// family marker's block `after` (-1 with none).
pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    after: i64,
    touched: (&[String], &[String]),
) -> Result<Refreshed> {
    let observed = sources::read(transaction, chain_id).await?;
    let (mut children, parents) = work::list(transaction, chain_id, block, after, touched).await?;
    if !observed.holders.is_empty() {
        children.extend(observed.holders.iter().cloned());
        children.sort();
        children.dedup();
    }
    let mut refreshed = derive(transaction, chain_id, block, &parents, &children).await?;
    let (rows, undo_rows) = sources::advance(transaction, chain_id, block, &observed).await?;
    refreshed.watermark_rows += rows;
    refreshed.undo_rows += undo_rows;
    Ok(refreshed)
}

/// Derive the listing rows of `parents`, each whole, and of `children` under every parent that
/// can list them, at `block`; then journal (when the block journals) and write what changed, and
/// move the counts by the same diff. The composition pass calls it with parents only.
pub(crate) async fn derive(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    parents: &[String],
    children: &[String],
) -> Result<Refreshed> {
    derive_with(transaction, chain_id, block, (parents, children), false).await
}

/// `derive` for an undo (`families/undo.rs`): the rows of `children` at the restored block
/// `block`, which journals nothing, writing only the rows it spells further (`HEALS_ONLY`). The
/// undo's other rows stay as the journal restored them, and no count moves.
pub(crate) async fn derive_heals(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    children: &[String],
) -> Result<Refreshed> {
    derive_with(transaction, chain_id, block, (&[], children), true).await
}

async fn derive_with(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    (parents, children): (&[String], &[String]),
    heals_only: bool,
) -> Result<Refreshed> {
    if parents.is_empty() && children.is_empty() {
        return Ok(Refreshed::default());
    }
    for statement in PREPARE {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to prepare the child listing refresh", error)
            })?;
    }
    let listing = bigname_storage::families::topology::ListingBlock {
        chain_id,
        number: block.number,
        timestamp_seconds: block.timestamp_seconds,
    };
    let head = format!(
        "/* project:families.derived.child_listing_fresh */
         INSERT INTO pg_temp.bigname_child_listing_fresh ({})",
        bigname_storage::families::topology::LISTING_COLUMNS
    );
    scope(transaction, WHOLE_PARENT, parents).await?;
    for parent in parents {
        let (sql, arguments) = statement(&head, listing, std::slice::from_ref(parent), None)?;
        let rows = sqlx::query_with(&sql, arguments)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database(
                    format!("failed to derive the child listing of {parent}"),
                    error,
                )
            })?
            .rows_affected();
        if rows > LARGE_PARENT_ROWS {
            tracing::warn!(
                target: "bigname_project::families",
                chain_id,
                block_number = block.number,
                parent = %parent,
                rows,
                "a parent's child listing was derived whole in one block transaction"
            );
        }
    }
    for chunk in children.chunks(CHUNK) {
        scope(transaction, CHILD, chunk).await?;
        let candidates: Vec<String> = sqlx::query_scalar(CANDIDATE_PARENTS)
            .bind(chain_id)
            .bind(chunk)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to read the parents of touched children", error)
            })?;
        if candidates.is_empty() {
            continue;
        }
        let nodes: Vec<String> = chunk
            .iter()
            .filter_map(|id| {
                id.split_once(':')
                    .map(|(_, node)| node.to_ascii_lowercase())
            })
            .collect();
        let (sql, arguments) = statement(&head, listing, &candidates, Some(nodes.as_slice()))?;
        sqlx::query_with(&sql, arguments)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database(
                    "failed to derive the listing rows of touched children",
                    error,
                )
            })?;
    }
    let mut changed = execute(
        transaction,
        CHANGES,
        chain_id,
        "failed to diff the child listing",
    )
    .await?;
    if heals_only && changed > 0 {
        changed -= sqlx::query(HEALS_ONLY)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to keep the undo's heals", error))?
            .rows_affected();
    }
    let mut refreshed = Refreshed::default();
    if changed > 0 {
        if block.journal {
            refreshed.undo_rows += journal(
                transaction,
                LISTING_JOURNAL,
                chain_id,
                block,
                "child listing",
            )
            .await?;
        }
        execute(
            transaction,
            LISTING_DELETE,
            chain_id,
            "failed to remove child listing rows",
        )
        .await?;
        execute(
            transaction,
            LISTING_INSERT,
            chain_id,
            "failed to write child listing rows",
        )
        .await?;
        refreshed.rows += changed;
    }
    let counts = execute(
        transaction,
        COUNT_CHANGES,
        chain_id,
        "failed to diff the child counts",
    )
    .await?;
    if counts == 0 {
        return Ok(refreshed);
    }
    let negative: i64 = sqlx::query_scalar(NEGATIVE_COUNTS)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to check the child counts", error))?;
    if negative > 0 {
        return Err(ProjectError::data_integrity(format!(
            "block {} of chain {chain_id} would leave {negative} child counts below zero: the \
             stored counts no longer match the stored child listing",
            block.number
        )));
    }
    if block.journal {
        refreshed.undo_rows +=
            journal(transaction, COUNT_JOURNAL, chain_id, block, "child counts").await?;
    }
    execute(
        transaction,
        COUNT_DELETE,
        chain_id,
        "failed to remove child counts",
    )
    .await?;
    execute(
        transaction,
        COUNT_INSERT,
        chain_id,
        "failed to write child counts",
    )
    .await?;
    refreshed.rows += counts;
    Ok(refreshed)
}

/// The listing statement of `parents` at `listing`, narrowed to `children` when given, as owned
/// SQL and arguments: the writer's future runs under the phase runner's `Send` bound, which a
/// builder borrowing its binds across the await fails (`listing_statement`).
fn statement(
    head: &str,
    listing: bigname_storage::families::topology::ListingBlock<'_>,
    parents: &[String],
    children: Option<&[String]>,
) -> Result<(String, sqlx::postgres::PgArguments)> {
    bigname_storage::families::topology::listing_statement(head, listing, parents, children)
        .map_err(|error| ProjectError::database("failed to bind the child listing rows", error))
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
        .map_err(|error| ProjectError::database("failed to scope the child listing", error))
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

async fn journal(
    transaction: &mut Transaction<'_, Postgres>,
    sql: &str,
    chain_id: &str,
    block: &input::BlockHeader,
    family: &str,
) -> Result<u64> {
    sqlx::query(sql)
        .bind(chain_id)
        .bind(block.number)
        .bind(&block.hash)
        .execute(&mut **transaction)
        .await
        .map(|result| result.rows_affected())
        .map_err(|error| ProjectError::database(format!("failed to journal the {family}"), error))
}
