//! The child listing family F17 (`project_child_listing`) and its counts
//! (`project_child_listing_count`). After a block's name summaries are composed, the listing
//! rows of every child the block can have moved are derived again by the children relation
//! (`bigname_storage::families::topology::listing_rows`) at the block, compared with the stored
//! rows, and every changed row is journalled with its before-image and written, in the block's
//! transaction. The counts move by the same diff. Undo restores both from the journal, like any
//! other family, so it derives nothing.
//!
//! The work list has two arms. A child is derived again, under every parent that can list it,
//! when the block touched a fact of its own: its name summary, an ENSv1 or Basenames edge naming
//! it, an ENSv2 registration of it, its registry node, its NameWrapper row or a registrar
//! lifecycle row of its node, a preimage observation of its name or of one of its labels, or the
//! second at which the migration gate stops keeping it (`recompose_at`). A parent's own facts
//! decide every child it lists: its migration state, its ENSv2 subregistry pointer, its surface,
//! its raw name cleared in place or spelled by an observation, and an address window of its
//! subregistry. Such a parent is derived whole, by one statement, and every row of it is
//! journalled.
//!
//! Inputs that change in place with no block, such as a lineage readability flip or a migration
//! registry association recorded after the parent's migration, are covered by the rebuild a hash
//! rotation forces, as for the name summaries (`summary.rs`).
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, families::input};

/// Children derived per statement, to bound the working set.
const CHUNK: usize = 1000;

/// A parent arm above this many rows is logged: its rows and their before-images all land in one
/// block's transaction.
const LARGE_PARENT_ROWS: u64 = 100_000;

/// What a listing refresh wrote: the listing and count rows written or removed, and the undo
/// rows journalled for them.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Refreshed {
    pub(crate) rows: u64,
    pub(crate) undo_rows: u64,
}

/// The names whose listing rows block `$2` (at time `$4`, in seconds) of chain `$1` can have
/// moved, after the family marker's block `$3`: `child` rows are children derived again under
/// every parent that can list them, `parent` rows are parents derived whole. `$5` holds the names
/// the block's summary refresh composed and `$6` the names whose raw name it swept.
const WORK_LIST: &str = r#"/* project:families.derived.child_listing_work */
    WITH journal AS MATERIALIZED (
        SELECT family, key::jsonb AS key, before_image
        FROM project_family_undo
        WHERE chain_id = $1 AND block_number = $2
          AND family IN ('project_name_summary', 'project_child_edge_candidate',
                         'project_child_registration_state', 'project_registry_node_state',
                         'project_wrapper_state', 'project_lifecycle_event', 'project_name_state',
                         'project_parent_subregistry')
    ),
    lifecycle AS (
        SELECT journal.before_image AS row FROM journal
        WHERE journal.family = 'project_lifecycle_event' AND journal.before_image IS NOT NULL
        UNION ALL
        SELECT to_jsonb(event) FROM journal JOIN project_lifecycle_event event
          ON journal.family = 'project_lifecycle_event'
         AND event.chain_id = $1 AND event.state_kind = journal.key ->> 1
         AND event.state_key = journal.key ->> 2 AND event.event_identity = journal.key ->> 3
    ),
    -- Each observation state is probed on its own, as the summary work list's `revealed` does,
    -- so `normalized_events_projection_idx` bounds the range to the blocks.
    revealed AS (
        SELECT revealed_event.logical_name_id
        FROM unnest('{canonical,safe,finalized}'::canonicality_state[]) AS state(canonicality_state)
        CROSS JOIN LATERAL (
            SELECT observed.logical_name_id
            FROM normalized_events observed
            WHERE observed.event_kind = 'PreimageObserved'
              AND observed.canonicality_state = state.canonicality_state
              AND observed.chain_id = $1
              AND observed.block_number > $3 AND observed.block_number <= $2
            OFFSET 0
        ) revealed_event
        WHERE revealed_event.logical_name_id IS NOT NULL
    ),
    -- The labels of the observed names: a label observed for one name spells every other name
    -- that carries it.
    observed AS MATERIALIZED (
        SELECT DISTINCT label.labelhash
        FROM revealed
        JOIN name_surfaces surface ON surface.logical_name_id = revealed.logical_name_id
        CROSS JOIN LATERAL unnest(surface.labelhashes) label (labelhash)
    ),
    -- The names with no raw bytes that carry an observed label, spelled from the label preimages
    -- (name_surfaces_search_labelhashes_idx): as a child its own name moves, as a parent the name
    -- of every child it lists.
    spelled AS (
        SELECT surface.logical_name_id
        FROM name_surfaces surface
        WHERE surface.raw_name IS NULL AND surface.chain_id = $1
          AND surface.labelhashes && (SELECT array_agg(observed.labelhash) FROM observed)
    ),
    children AS (
        SELECT name.id FROM unnest($5::text[]) name (id)
        UNION SELECT journal.key ->> 1 FROM journal WHERE journal.family = 'project_name_summary'
        UNION SELECT (journal.key ->> 1) || ':' || (journal.key ->> 3) FROM journal
        WHERE journal.family = 'project_child_edge_candidate'
        UNION SELECT journal.key ->> 1 FROM journal
        WHERE journal.family = 'project_child_registration_state'
        UNION SELECT (journal.key ->> 1) || ':' || lower(journal.key ->> 2) FROM journal
        WHERE journal.family = 'project_registry_node_state'
        UNION SELECT journal.before_image ->> 'logical_name_id' FROM journal
        WHERE journal.family = 'project_wrapper_state'
        UNION SELECT wrapper.logical_name_id FROM journal JOIN project_wrapper_state wrapper
          ON journal.family = 'project_wrapper_state'
         AND wrapper.chain_id = $1 AND wrapper.resource_id = (journal.key ->> 1)::uuid
        -- A registrar lease row names its node; the relation reads it by node
        -- (children_page/child_flags.rs), under the registrar's namespace.
        UNION SELECT CASE lifecycle.row ->> 'source_family'
                         WHEN 'ens_v1_registrar_l1' THEN 'ens'
                         WHEN 'basenames_base_registrar' THEN 'basenames' END
                     || ':' || lower(lifecycle.row ->> 'namehash')
        FROM lifecycle
        UNION SELECT revealed.logical_name_id FROM revealed
        UNION SELECT spelled.logical_name_id FROM spelled
        -- An observed label spells every ENSv1 or Basenames edge child that carries it, under
        -- any parent, with or without a surface (project_child_edge_candidate_labelhash_idx).
        UNION SELECT edge.namespace || ':' || edge.child_node
        FROM (SELECT DISTINCT lower(observed.labelhash) AS labelhash FROM observed) observed
        JOIN project_child_edge_candidate edge
          ON edge.chain_id = $1 AND edge.labelhash = observed.labelhash
        UNION SELECT listing.child_logical_name_id FROM project_child_listing listing
        WHERE listing.chain_id = $1 AND listing.recompose_at <= $4
    ),
    parents AS (
        -- The migration columns the gate reads (topology/children.rs, `parent_migration`).
        SELECT journal.key ->> 2 AS id
        FROM journal
        LEFT JOIN project_name_state state
          ON state.chain_id = $1 AND state.namespace = journal.key ->> 1
         AND state.logical_name_id = journal.key ->> 2
        WHERE journal.family = 'project_name_state'
          AND jsonb_build_array(
                  COALESCE(journal.before_image, '{}'::jsonb) ->> 'migration_path',
                  COALESCE(journal.before_image, '{}'::jsonb) -> 'migration_evidence',
                  COALESCE(journal.before_image, '{}'::jsonb) -> 'migration_position')
              IS DISTINCT FROM jsonb_build_array(
                  state.migration_path, state.migration_evidence, state.migration_position)
        UNION SELECT journal.key ->> 1 FROM journal
        WHERE journal.family = 'project_parent_subregistry'
        UNION SELECT name.id FROM unnest($6::text[]) name (id)
        -- An observation gives the observed name's surface its raw bytes in place, and a label
        -- observed for any name spells a parent with none: either moves the parent's spelling,
        -- which every child it lists ends with.
        UNION SELECT revealed.logical_name_id FROM revealed
        UNION SELECT spelled.logical_name_id FROM spelled
        UNION SELECT surface.logical_name_id FROM name_surfaces surface
        WHERE surface.chain_id = $1 AND surface.block_number > $3 AND surface.block_number <= $2
        UNION SELECT pointer.logical_name_id
        FROM contract_instance_addresses address
        JOIN project_parent_subregistry pointer
          ON pointer.chain_id = $1 AND pointer.subregistry_address = lower(address.address)
        WHERE address.chain_id = $1
          AND ((address.active_from_block_number > $3 AND address.active_from_block_number <= $2)
               OR (address.active_to_block_number > $3 AND address.active_to_block_number <= $2))
    )
    SELECT 'child', children.id FROM children
    WHERE children.id IS NOT NULL AND children.id LIKE '%:%'
    UNION
    SELECT 'parent', parents.id FROM parents
    WHERE parents.id IS NOT NULL
      AND (EXISTS (SELECT 1 FROM project_child_edge_candidate edge
                   WHERE edge.chain_id = $1 AND edge.namespace = split_part(parents.id, ':', 1)
                     AND edge.parent_node = lower(split_part(parents.id, ':', 2)))
           OR EXISTS (SELECT 1 FROM project_parent_subregistry pointer
                      WHERE pointer.chain_id = $1 AND pointer.logical_name_id = parents.id)
           OR EXISTS (SELECT 1 FROM project_child_listing listing
                      WHERE listing.chain_id = $1
                        AND listing.parent_logical_name_id = parents.id))
    ORDER BY 1, 2"#;

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

const PREPARE: &str = "/* project:families.derived.child_listing_prepare */
    CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_fresh
        (LIKE project_child_listing) ON COMMIT DROP;
    CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_scope
        (kind smallint NOT NULL, id text NOT NULL) ON COMMIT DROP;
    CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_change
        (child_logical_name_id text NOT NULL, before_image jsonb, after_image jsonb)
        ON COMMIT DROP;
    CREATE TEMP TABLE IF NOT EXISTS bigname_child_listing_count_change
        (parent_logical_name_id text NOT NULL, registry_address text NOT NULL,
         child_count bigint NOT NULL, before_image jsonb) ON COMMIT DROP;
    TRUNCATE pg_temp.bigname_child_listing_fresh, pg_temp.bigname_child_listing_scope,
             pg_temp.bigname_child_listing_change, pg_temp.bigname_child_listing_count_change";

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

/// The work list of block `block`, which follows the family marker's block `after` (-1 with
/// none): the children to derive again and the parents to derive whole. `names` are the names the
/// block's summary refresh composed and `swept` those whose raw name it swept.
async fn work_list(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    after: i64,
    (names, swept): (&[String], &[String]),
) -> Result<(Vec<String>, Vec<String>)> {
    let rows: Vec<(String, String)> = sqlx::query_as(WORK_LIST)
        .bind(chain_id)
        .bind(block.number)
        .bind(after)
        .bind(block.timestamp_seconds)
        .bind(names)
        .bind(swept)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the children a family block touched", error)
        })?;
    let mut children = Vec::new();
    let mut parents = Vec::new();
    for (kind, id) in rows {
        if kind == "parent" {
            parents.push(id);
        } else {
            children.push(id);
        }
    }
    Ok((children, parents))
}

/// Derive again, journal and write the listing rows and counts block `block` moved, after the
/// family marker's block `after` (-1 with none).
pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    after: i64,
    touched: (&[String], &[String]),
) -> Result<Refreshed> {
    let (children, parents) = work_list(transaction, chain_id, block, after, touched).await?;
    derive(transaction, chain_id, block, &parents, &children).await
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
    if parents.is_empty() && children.is_empty() {
        return Ok(Refreshed::default());
    }
    execute_raw(
        transaction,
        PREPARE,
        "failed to prepare the child listing refresh",
    )
    .await?;
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
    let changed = execute(
        transaction,
        CHANGES,
        chain_id,
        "failed to diff the child listing",
    )
    .await?;
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

async fn execute_raw(
    transaction: &mut Transaction<'_, Postgres>,
    sql: &str,
    context: &str,
) -> Result<()> {
    sqlx::raw_sql(sql)
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(|error| ProjectError::database(context, error))
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
