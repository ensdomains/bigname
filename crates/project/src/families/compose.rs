//! The composition pass of a rebuild in ranges: the work of its chunks, each run inside a
//! transaction the driver fenced (driver/compose.rs). The ranges before the pass wrote the family
//! rows only, so the pass composes, once, at the publication of the last range's block, what
//! those ranges' blocks would each have composed: every name the incremental path names, every
//! index key, and the address history catalogue's event windows. The keys come from the base
//! families, the surfaces and the interpreted events, never from the journal, which the ranges
//! did not write. A composition reads base rows and Interpret rows, never another composed row,
//! so composing a name once at the pass block gives the row its last composition at or below that
//! block would have given (docs/projections.md, "Owned key families").
//!
//! The pass journals nothing (`BlockHeader::journal`): its pass-end marker row ends the journal at
//! its block.
use bigname_storage::families::records::CurrentHistoryRelation;
use sqlx::{PgPool, Postgres, Transaction};

use super::{
    block::BlockStats,
    derived::{self, Touched},
    history_catalogue,
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

/// The pass's streams in the order it runs them; `compose_stream` on the repair record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Stream {
    /// Every name id the incremental path composes or indexes, in `COLLATE "C"` order.
    Names = 0,
    /// The (resolver, node) pairs of the record values, for the address record node index.
    Nodes = 1,
    /// The (resolver, record id) pairs of the record-id values, for the record-id index.
    RecordIds = 2,
    /// The address history catalogue over consecutive block windows; the cursor is the last
    /// window's end, the pass block once the stream is done.
    Windows = 3,
}

impl Stream {
    pub(crate) fn of(value: Option<i16>) -> Self {
        match value {
            Some(1) => Self::Nodes,
            Some(2) => Self::RecordIds,
            Some(3) => Self::Windows,
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
            Self::RecordIds | Self::Windows => Self::Windows,
        }
    }
}

/// The next `limit` name ids after `cursor` the pass composes or indexes, in `COLLATE "C"`
/// order, each with its reach (`Reach`). A composed id is one the summary work list
/// (`derived::SUMMARY_WORK_LIST`) names for some block at or below the pass block `$2`: a
/// surface, a key of a family keyed by name or registry node, a name column of a family row, a
/// resource pointer's name, or a name an interpreted event carried, which covers every value a
/// family row held before a later event moved it (a pointer's node, a wrapper's or key state's
/// name, a record row's name). No base row of those families is deleted in a rebuild, so the
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
const NAMES: &str = r#"/* project:families.compose.names */
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
        UNION ALL
        SELECT edge.namespace || ':' || edge.child_node, 0 FROM project_child_edge_candidate edge
        WHERE edge.chain_id = $1 AND edge.authority_arm = 'ens_v1'
    )
    SELECT logical_name_id, max(reach)::smallint FROM ids
    WHERE logical_name_id IS NOT NULL AND logical_name_id <> ''
      AND logical_name_id COLLATE "C" > $3
    GROUP BY logical_name_id
    ORDER BY logical_name_id COLLATE "C"
    LIMIT $4"#;

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

/// The blocks after `$2` up to the pass block `$3` that hold interpreted events, with the running
/// count of their events, at most `$4` of them.
const WINDOW: &str = "/* project:families.compose.window */
    SELECT block.block_number, sum(block.events) OVER (ORDER BY block.block_number)::bigint
    FROM (
        SELECT event.block_number, count(*) AS events FROM normalized_events event
        WHERE event.chain_id = $1 AND event.block_number > $2 AND event.block_number <= $3
        GROUP BY event.block_number ORDER BY event.block_number LIMIT $4
    ) block
    ORDER BY block.block_number";

/// The next name ids of the names stream after `cursor`, at most `limit`.
pub(crate) async fn names(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, Reach)>> {
    sqlx::query_as(NAMES)
        .bind(chain_id)
        .bind(block)
        .bind(cursor.unwrap_or(""))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await
        .map_err(|error| ProjectError::database("failed to read the names to compose", error))
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

/// The end of the next catalogue window after `after`: the last block whose running event count
/// stays within `events`, at least one block, or the pass block `through` once the window reaches
/// every block with events, so the last window covers the blocks after them too.
pub(crate) async fn window_end(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    after: i64,
    through: i64,
    events: u64,
) -> Result<i64> {
    let limit = i64::try_from(events).unwrap_or(i64::MAX).max(1);
    let blocks: Vec<(i64, i64)> = sqlx::query_as(WINDOW)
        .bind(chain_id)
        .bind(after)
        .bind(through)
        .bind(limit)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to plan a catalogue window", error))?;
    let within = blocks
        .iter()
        .take_while(|(_, total)| *total <= limit)
        .count()
        .max(1);
    if within >= blocks.len() && i64::try_from(blocks.len()).unwrap_or(i64::MAX) < limit {
        return Ok(through);
    }
    Ok(blocks.get(within - 1).map_or(through, |(block, _)| *block))
}

/// Compose one chunk of the names stream at `block`, the pass block's publication with the
/// marker's composition: the address name index of every id, the summaries and the catalogue of
/// the composed ones, then the lookup names and inventories of those that reach the lookup.
pub(crate) async fn names_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
    composition: &marker::Composition,
    ids: &[(String, Reach)],
    stats: &mut BlockStats,
) -> Result<()> {
    let reaching = |least: Reach| {
        ids.iter()
            .filter(|(_, reach)| *reach >= least)
            .map(|(id, _)| id.clone())
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
    history_catalogue::refresh(
        transaction,
        chain_id,
        block,
        block.number,
        &composed,
        &current,
        stats,
    )
    .await?;
    lookup::prepare_names(transaction, &looked_up).await?;
    lookup::refresh(transaction, chain_id, block, composition, stats).await
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

/// Refresh the catalogue for the events of the window `(after, block]`, with no name of its own:
/// the names stream composed every name's own sources and current memberships, and this adds the
/// sources and resource memberships of the window's events, as each block of it did.
pub(crate) async fn window_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
    after: i64,
    stats: &mut BlockStats,
) -> Result<()> {
    history_catalogue::refresh(transaction, chain_id, block, after, &[], &[], stats).await
}
