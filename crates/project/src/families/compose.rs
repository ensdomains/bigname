//! The composition pass of a rebuild in ranges: the work of its chunks, each run inside a
//! transaction the driver fenced (driver/compose.rs). The ranges before the pass wrote the family
//! rows only, so the pass composes, once, at the publication of the last range's block, what
//! those ranges' blocks would each have composed: every name the incremental path names and every
//! index key, then the address history catalogue, built once from set-based statements
//! (history_catalogue/build). The keys come from the base families, the surfaces and the
//! interpreted events, never from the journal, which the ranges did not write. A composition
//! reads base rows and Interpret rows, never another composed row, so composing a name once at
//! the pass block gives the row its last composition at or below that block would have given
//! (docs/projections.md, "Owned key families"). The one exception is a key source, not an input:
//! the lookup inventories stream takes its resources from the lookup names the finished names
//! stream composed.
//!
//! The pass journals nothing (`BlockHeader::journal`): its pass-end marker row ends the journal at
//! its block.
use std::collections::BTreeMap;

use bigname_storage::families::records::CurrentHistoryRelation;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    block::BlockStats,
    derived::{self, Touched},
    history_catalogue::build::{self, PersistedRelation},
    input::BlockHeader,
    lookup, marker,
    tables::NAME_SUMMARY,
};
use crate::{ProjectError, Result};

/// What the pass composes for one id of the names stream: the address name index only (0), the
/// lookup name row as well (1), or everything the summary work list's names get (2): the summary,
/// the catalogue name source and anchor, and the lookup name row.
pub(crate) type Reach = i16;
const LOOKUP: Reach = 1;
const COMPOSED: Reach = 2;

/// The pass's streams; `compose_stream` on the repair record. The pass runs them in the order of
/// `next` (0, 1, 2, 4, 3), not of their values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Stream {
    /// Every name id the incremental path composes or indexes. Workers claim the ids of the work
    /// list not yet composed, so the stream has no cursor.
    Names = 0,
    /// The (resolver, node) pairs of the record values, for the address record node index.
    Nodes = 1,
    /// The (resolver, record id) pairs of the record-id values, for the record-id index.
    RecordIds = 2,
    /// The address history catalogue, built in steps of partitions. The cursor names the last
    /// partition committed (history_catalogue/build).
    Catalogue = 3,
    /// The lookup inventories of the resources the composed lookup names serve, keyed by those
    /// names. The cursor is the last name committed.
    Inventories = 4,
}

impl Stream {
    pub(crate) fn of(value: Option<i16>) -> Self {
        match value {
            Some(1) => Self::Nodes,
            Some(2) => Self::RecordIds,
            Some(3) => Self::Catalogue,
            Some(4) => Self::Catalogue,
            _ => Self::Names,
        }
    }

    pub(crate) const fn index(self) -> i16 {
        self as i16
    }

    pub(crate) const fn next(self) -> Self {
        match self {
            Self::Names => Self::Nodes,
            Self::Nodes => Self::RecordIds,
            Self::RecordIds => Self::Inventories,
            Self::Inventories | Self::Catalogue => Self::Catalogue,
        }
    }
}

/// The pass's work list, written once when the pass begins (`prepare_names`): every name id the
/// pass composes or indexes, numbered from 1 in `COLLATE "C"` order, each with its reach
/// (`Reach`). The ranges below the pass are done by then and nothing else writes the families
/// until the pass ends, so the list a later run reads is the one a single enumeration at the
/// pass's start finds. A composed id is one the summary work list
/// (`derived::SUMMARY_WORK_LIST`) names for some block at or below the pass block `$2`: a
/// surface, a key of a family keyed by name or registry node, a name column of a family row, a
/// resource pointer's name, or a name an interpreted event carried, which covers every value a
/// family row held before a later event moved it (a pointer's node, a wrapper's or key state's
/// name, a record row's name). The events read are the activated ones, the input the fold applies
/// (input.rs, range/reads.rs), so an event no block applied names nothing. No base row of those families is deleted in a rebuild, so the
/// current rows hold every key the journal arms read. Each composed id gets its lookup name row
/// and its catalogue name source, and its summary when it has a surface.
///
/// The address fold and controller candidate keys reach the lookup names only, through the
/// lookup's path relation work (lookup/work.rs). The work list names a fold row's name from its
/// before-image alone, so it composes a fold's name when a later block rewrites the row. A fold
/// row with no surfaced name is the `<namespace>:<namehash>` row of unnamed registrar transfers
/// (addresses.rs, `fold_key`), so its name is composed when such transfers fall in two blocks or
/// more. Every other family's name column holds an event's name, which has a surface
/// (`normalized_events` references `name_surfaces`). The ENSv1 child-edge ids only index
/// addresses.
const PREPARE_NAMES: &str = r#"/* project:families.compose.prepare_names */
    WITH ids (logical_name_id, reach) AS (
        SELECT surface.logical_name_id, 2 FROM name_surfaces surface
        WHERE surface.chain_id = $1 AND surface.block_number <= $2
        UNION ALL
        SELECT state.logical_name_id, 2 FROM project_name_state state WHERE state.chain_id = $1
        UNION ALL
        SELECT name.value, 2 FROM project_lifecycle_event event
        CROSS JOIN LATERAL (VALUES (event.original_logical_name_id),
            (event.decoded_logical_name_id),
            (CASE WHEN event.state_kind = 'triple' THEN event.state_key::jsonb ->> 0 END)) name (value)
        WHERE event.chain_id = $1
        UNION ALL
        SELECT state.namespace || ':' || lower(state.node), 2 FROM project_registry_node_state state
        WHERE state.chain_id = $1
        UNION ALL
        SELECT name.value, 2 FROM project_registry_owner_event event
        CROSS JOIN LATERAL (VALUES (event.logical_name_id),
            (event.namespace || ':' || lower(event.node))) name (value)
        WHERE event.chain_id = $1
        UNION ALL
        SELECT pointer.namespace || ':' || lower(pointer.node), 2 FROM project_registry_pointer pointer
        WHERE pointer.chain_id = $1
        UNION ALL
        SELECT pointer.namespace || ':' || lower(pointer.namehash), 2
        FROM project_resource_pointer pointer WHERE pointer.chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_lifecycle_triple_summary WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_lifecycle_association WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_lifecycle_key_state WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_name_history WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_named_resource_pointer WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_binding_candidate WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_child_registration_state WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_node_record_partition WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_node_record_value WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_parent_subregistry WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_registry_binding_observation WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 2 FROM project_wrapper_state WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 1 FROM project_address_controller_candidate WHERE chain_id = $1
        UNION ALL SELECT logical_name_id, 1 FROM project_address_name_fold WHERE chain_id = $1
        UNION ALL
        SELECT transfer.namespace || ':' || lower(transfer.after_state ->> 'namehash'), 2
        FROM normalized_events transfer
        WHERE transfer.event_kind = 'TokenControlTransferred'
          AND transfer.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND transfer.chain_id = $1 AND transfer.block_number <= $2
          AND transfer.logical_name_id IS NULL
          AND transfer.consumer_visibility = 'activated'
          AND transfer.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
        GROUP BY 1 HAVING count(DISTINCT transfer.block_number) > 1
        UNION ALL
        SELECT name.value, 2 FROM normalized_events event
        CROSS JOIN LATERAL (VALUES (event.logical_name_id),
            (CASE WHEN event.event_kind IN ('ResolverChanged', 'RecordVersionChanged')
                       AND event.resource_id IS NOT NULL
                  THEN event.namespace || ':' || lower(event.after_state ->> 'node') END)) name (value)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND event.consumer_visibility = 'activated'
        UNION ALL
        SELECT edge.namespace || ':' || edge.child_node, 0 FROM project_child_edge_candidate edge
        WHERE edge.chain_id = $1 AND edge.authority_arm = 'ens_v1'
    )
    INSERT INTO project_compose_name_work (chain_id, serial, logical_name_id, reach)
    SELECT $1, row_number() OVER (ORDER BY logical_name_id COLLATE "C"), logical_name_id,
           max(reach)::smallint
    FROM ids
    WHERE logical_name_id IS NOT NULL AND logical_name_id <> ''
    GROUP BY logical_name_id"#;

/// A worker's claim on the names stream: the next ids of the work list no chunk has composed,
/// skipping those another worker's open chunk holds. The row locks last until the chunk commits
/// or rolls back.
const CLAIM: &str = "/* project:families.compose.claim */
    SELECT work.serial, work.logical_name_id, work.reach FROM project_compose_name_work work
    WHERE work.chain_id = $1 AND NOT work.composed
    ORDER BY work.serial
    LIMIT $2
    FOR UPDATE SKIP LOCKED";

/// The ids of the work list no committed chunk has composed.
const UNCOMPOSED: &str = "/* project:families.compose.uncomposed */
    SELECT count(*) FROM project_compose_name_work WHERE chain_id = $1 AND NOT composed";

const CLEAR_NAMES: &str = "/* project:families.compose.clear_names */
    DELETE FROM project_compose_name_work WHERE chain_id = $1";

const CLEAR_HISTORY: &str = "/* project:families.compose.clear_history */
    DELETE FROM project_compose_history_work WHERE chain_id = $1";

/// Mark the ids of a names chunk composed, each with the current relations the catalogue reads,
/// in the chunk's transaction.
const COMPOSED_NAMES: &str = "/* project:families.compose.composed */
    UPDATE project_compose_name_work work
    SET composed = true, relations = written.relations
    FROM unnest($2::bigint[], $3::jsonb[]) written (serial, relations)
    WHERE work.chain_id = $1 AND work.serial = written.serial";

const NODES: &str = r#"/* project:families.compose.nodes */
    SELECT DISTINCT value.resolver_address, value.node FROM project_node_record_value value
    WHERE value.chain_id = $1 AND value.node IS NOT NULL
      AND (value.resolver_address, value.node) > ($2, $3)
    ORDER BY value.resolver_address, value.node
    LIMIT $4"#;

const RECORD_IDS: &str = r#"/* project:families.compose.record_ids */
    SELECT DISTINCT value.resolver_address, value.record_id FROM project_record_id_value value
    WHERE value.chain_id = $1 AND (value.resolver_address, value.record_id) > ($2, $3)
    ORDER BY value.resolver_address, value.record_id
    LIMIT $4"#;

/// The composed lookup names with a serving resource, after the name `$2`, each with the
/// resource its core records. The names stream has ended and left the serving resource column
/// null (lookup/names.rs), so these are the inventories stream's keys: nothing else writes the
/// lookup names while the pass reads them.
const INVENTORIES: &str = "/* project:families.compose.inventories */
    SELECT name.logical_name_id, (name.core ->> 'record_serving_resource_id')::uuid
    FROM project_lookup_name name
    WHERE name.chain_id = $1 AND name.logical_name_id > $2
      AND name.core ->> 'record_serving_resource_id' IS NOT NULL
    ORDER BY name.logical_name_id
    LIMIT $3";

/// Write the pass's work list for the pass block `block`, in the transaction that begins the
/// pass, over whatever list an interrupted beginning left.
pub(crate) async fn prepare_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: i64,
) -> Result<u64> {
    clear_names(transaction, chain_id).await?;
    sqlx::query(PREPARE_NAMES)
        .bind(chain_id)
        .bind(block)
        .execute(&mut **transaction)
        .await
        .map(|result| result.rows_affected())
        .map_err(|error| ProjectError::database("failed to list the names to compose", error))
}

/// Drop the chain's work list and the catalogue build's work table, at the pass end.
pub(crate) async fn clear_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    for sql in [CLEAR_NAMES, CLEAR_HISTORY] {
        sqlx::query(sql)
            .bind(chain_id)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to clear the composition work lists", error)
            })?;
    }
    Ok(())
}

/// Claim at most `limit` ids of the work list no chunk has composed, each with its serial, in
/// the chunk's `transaction`.
pub(crate) async fn claim(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    limit: usize,
) -> Result<Vec<(i64, String, Reach)>> {
    sqlx::query_as(CLAIM)
        .bind(chain_id)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to claim the names to compose", error))
}

/// Refuse a work list with ids no committed chunk composed, inside `transaction`, before
/// `what`: the names stream's end and every partition of the catalogue build.
pub(crate) async fn require_composed(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    what: &str,
) -> Result<()> {
    let uncomposed: i64 = sqlx::query_scalar(UNCOMPOSED)
        .bind(chain_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to count the uncomposed names", error))?;
    if uncomposed != 0 {
        return Err(ProjectError::data_integrity(format!(
            "chain {chain_id}'s {what} found {uncomposed} names of the work list not composed"
        )));
    }
    Ok(())
}

/// The next names of the inventories stream after the name `cursor`, at most `limit`, each with
/// the resource it serves.
pub(crate) async fn inventories(
    pool: &PgPool,
    chain_id: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, Uuid)>> {
    sqlx::query_as(INVENTORIES)
        .bind(chain_id)
        .bind(cursor.unwrap_or_default())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await
        .map_err(|error| ProjectError::database("failed to read the inventories to compose", error))
}

/// The next pairs of the node or record-id stream after `cursor`, at most `limit`.
pub(crate) async fn pairs(
    pool: &PgPool,
    chain_id: &str,
    stream: Stream,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, String)>> {
    let (first, second) = match cursor {
        Some(cursor) => {
            let pair: (String, String) = serde_json::from_str(cursor).map_err(|error| {
                ProjectError::data_integrity(format!("composition cursor {cursor}: {error}"))
            })?;
            (Some(pair.0), Some(pair.1))
        }
        None => (None, None),
    };
    let sql = if stream == Stream::Nodes {
        NODES
    } else {
        RECORD_IDS
    };
    sqlx::query_as(sql)
        .bind(chain_id)
        .bind(first.unwrap_or_default())
        .bind(second.unwrap_or_default())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await
        .map_err(|error| ProjectError::database("failed to read the index keys to compose", error))
}

/// The cursor of a pair stream.
pub(crate) fn pair_cursor(pair: &(String, String)) -> String {
    serde_json::to_string(pair).expect("a pair of strings serializes")
}

/// Compose one chunk of the names stream at `block`, the pass block's publication with the
/// marker's composition: the address name index of every id, the summaries of the composed ones,
/// then the lookup names of those that reach the lookup. Every id of the chunk is marked
/// composed, a composed one with the current relations its summary gave, which the catalogue
/// build reads. The inventories the names serve are the inventories stream's, so no key a chunk
/// writes is another chunk's.
pub(crate) async fn names_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
    composition: &marker::Composition,
    ids: &[(i64, String, Reach)],
    stats: &mut BlockStats,
) -> Result<()> {
    let reaching = |least: Reach| {
        ids.iter()
            .filter(|(_, _, reach)| *reach >= least)
            .map(|(_, id, _)| id.clone())
            .collect::<Vec<_>>()
    };
    let (every, looked_up, composed) = (reaching(0), reaching(LOOKUP), reaching(COMPOSED));
    let keys = Touched::keys(every, Default::default(), Default::default());
    derived::refresh(transaction, chain_id, &keys, composition).await?;
    let publication = marker::publication(chain_id, block, composition)?;
    let mut current: Vec<CurrentHistoryRelation> = Vec::new();
    for chunk in composed.chunks(derived::SUMMARY_CHUNK) {
        let fresh = derived::compose_summaries(transaction, &publication, chunk).await?;
        derived::retire_null_resolver_divergences(
            transaction,
            chain_id,
            &fresh.null_resolver_names,
        )
        .await?;
        current.extend(fresh.current_history_relations);
        let (rows, _) =
            derived::replace_summary_chunk(transaction, chain_id, block, chunk, &fresh.rows)
                .await?;
        *stats.rows.entry(NAME_SUMMARY.name).or_default() += rows;
    }
    mark_composed(transaction, chain_id, ids, &current).await?;
    lookup::prepare_names(transaction, &looked_up).await?;
    lookup::refresh_names(transaction, chain_id, block, composition, stats).await
}

/// Compose one chunk of the inventories stream at `block`: each name of `served` gets its serving
/// resource, then every resource of the chunk its inventory. A resource several chunks' names
/// serve is composed in each of them, from the same base rows, as the names chunks of a pass
/// before the stream did.
pub(crate) async fn inventories_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
    composition: &marker::Composition,
    served: &[(String, Uuid)],
    stats: &mut BlockStats,
) -> Result<()> {
    let resources: Vec<Uuid> = served.iter().map(|(_, resource)| *resource).collect();
    lookup::prepare_resources(transaction, &resources).await?;
    lookup::refresh_inventories(transaction, chain_id, block, composition, served, stats).await
}

/// Derive the address record index rows of one chunk of the node or record-id stream.
pub(crate) async fn pairs_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    stream: Stream,
    composition: &marker::Composition,
    pairs: Vec<(String, String)>,
) -> Result<()> {
    let pairs = pairs.into_iter().unzip();
    let keys = if stream == Stream::Nodes {
        Touched::keys(Vec::new(), pairs, Default::default())
    } else {
        Touched::keys(Vec::new(), Default::default(), pairs)
    };
    derived::refresh(transaction, chain_id, &keys, composition)
        .await
        .map(|_| ())
}

async fn mark_composed(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    ids: &[(i64, String, Reach)],
    current: &[CurrentHistoryRelation],
) -> Result<()> {
    let mut relations: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for relation in current {
        relations
            .entry(relation.logical_name_id.as_str())
            .or_default()
            .push(PersistedRelation::of(relation).to_json());
    }
    let (serials, written): (Vec<i64>, Vec<Option<Value>>) = ids
        .iter()
        .map(|(serial, id, reach)| {
            let persisted = (*reach >= COMPOSED)
                .then(|| relations.remove(id.as_str()))
                .flatten()
                .map(Value::Array);
            (*serial, persisted)
        })
        .unzip();
    sqlx::query(COMPOSED_NAMES)
        .bind(chain_id)
        .bind(serials)
        .bind(written)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to mark the names composed", error))?;
    Ok(())
}

/// Run one partition of the catalogue build at the pass block `block`.
pub(crate) async fn catalogue_step(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
    cursor: &build::Cursor,
    work_mem: Option<&str>,
) -> Result<(&'static str, u64)> {
    build::run(transaction, chain_id, block.number, cursor, work_mem).await
}

#[cfg(test)]
#[path = "compose_tests.rs"]
mod compose_tests;
