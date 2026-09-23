//! The child arm of name history with `include=child_registrations`: the requested name's
//! direct child registration events, read through `child_registration_events` in history order.
//!
//! The arm is keyed by the parent and served by `child_registration_events_parent_history_idx`
//! (`parent_logical_name_id, chain_id, block_number, block_hash, transaction_order_key,
//! log_order_key, event_identity`). Within one parent and chain that key order is the history
//! order, so the keyset bound below is one row comparison on the index and a page reads at most
//! `page_size + 1` membership rows whatever else the table holds.

use std::cmp::Ordering;

use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{EventHistoryReadFilter, HistoryOrder, source::push_history_canonicality_filter};

/// Stored kinds of the friendly `registration` history type; every membership row has one.
const REGISTRATION_KINDS: [&str; 2] = ["RegistrationGranted", "LabelRegistered"];

/// Where the arm reads: one parent on the chain of its own name surface, inside the read's
/// block window for that chain.
#[derive(Clone, Debug)]
pub(super) struct ChildArm {
    pub(super) parent_logical_name_id: String,
    pub(super) chain_id: String,
    pub(super) from_block: Option<i64>,
    pub(super) to_block: Option<i64>,
}

impl ChildArm {
    /// The arm for `parent`, or `None` when it can hold no row of this read: the parent has no
    /// surface, the block window leaves out its chain, or an explicit type set excludes
    /// `registration`. Cursor validation without an explicit type passes a filter whose
    /// `event_kinds` it has cleared, so the type check never excludes the arm there.
    pub(super) async fn resolve(
        connection: &mut PgConnection,
        parent: &str,
        filter: &EventHistoryReadFilter,
    ) -> anyhow::Result<Option<Self>> {
        if !filter.event_kinds.is_empty()
            && !filter
                .event_kinds
                .iter()
                .any(|kind| REGISTRATION_KINDS.contains(&kind.as_str()))
        {
            return Ok(None);
        }
        let chain_id: Option<String> = sqlx::query_scalar(
            "SELECT chain_id FROM bigname_phase.name_surfaces WHERE logical_name_id = $1",
        )
        .bind(parent)
        .fetch_optional(&mut *connection)
        .await?;
        let Some(chain_id) = chain_id else {
            return Ok(None);
        };
        let (from_block, to_block) = match filter.block_window.as_ref() {
            None => (None, None),
            Some(window) => match window
                .ranges
                .iter()
                .find(|range| range.chain_id == chain_id)
            {
                Some(range) => (range.from_block, range.to_block),
                None => return Ok(None),
            },
        };
        Ok(Some(Self {
            parent_logical_name_id: parent.to_owned(),
            chain_id,
            from_block,
            to_block,
        }))
    }
}

/// Push `FROM … WHERE` for the arm's rows: membership rows joined to the exact normalized event
/// they cite, with the read's visibility, canonicality, window, and (when `kinds` is given) type
/// filters. The event must still sit at the membership's position under the same name, so the
/// arm's index order and the event's history order are the same order.
pub(super) fn push_child_arm_source<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    arm: &'a ChildArm,
    kinds: Option<&'a [String]>,
    canonical_only: bool,
) {
    builder.push(
        r#"
        FROM bigname_phase.child_registration_events m
        JOIN bigname_phase.normalized_events ne
          ON ne.event_identity = m.event_identity
        "#,
    );
    push_membership_matches_event(builder);
    // The child's own surface names the row, read in the same transaction as the page. Counts
    // and cursor checks join it too, so they cover exactly the rows a page can return.
    builder.push(
        " JOIN bigname_phase.name_surfaces child_surface
            ON child_surface.logical_name_id = ne.logical_name_id",
    );
    builder.push(
        r#"
        LEFT JOIN bigname_phase.chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.consumer_visibility = 'activated'
        "#,
    );
    push_arm_bounds(builder, arm);
    if let Some(kinds) = kinds.filter(|kinds| !kinds.is_empty()) {
        builder.push(" AND ne.event_kind = ANY(");
        builder.push_bind(kinds);
        builder.push("::text[])");
    }
    push_history_canonicality_filter(builder, canonical_only);
}

/// ` AND NOT EXISTS (…)` for a name-arm row: the row is not also a child-arm row. Name-arm rows
/// already pass the shared visibility, type, window, and canonicality filters, so membership at
/// the event's exact position decides the classification. A child classification wins.
pub(super) fn push_not_child_arm_row<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    arm: &'a ChildArm,
) {
    builder.push(
        r#"
        AND NOT EXISTS (
            SELECT 1 FROM bigname_phase.child_registration_events m
            WHERE m.event_identity = ne.event_identity
        "#,
    );
    push_arm_bounds(builder, arm);
    push_membership_matches_event(builder);
    builder.push(")");
}

fn push_arm_bounds<'a>(builder: &mut QueryBuilder<'a, Postgres>, arm: &'a ChildArm) {
    builder.push(" AND m.parent_logical_name_id = ");
    builder.push_bind(&arm.parent_logical_name_id);
    builder.push(" AND m.chain_id = ");
    builder.push_bind(&arm.chain_id);
    if let Some(from_block) = arm.from_block {
        builder.push(" AND m.block_number >= ");
        builder.push_bind(from_block);
    }
    if let Some(to_block) = arm.to_block {
        builder.push(" AND m.block_number <= ");
        builder.push_bind(to_block);
    }
}

fn push_membership_matches_event(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
         AND ne.chain_id = m.chain_id
         AND ne.block_number = m.block_number
         AND ne.block_hash = m.block_hash
         AND COALESCE(ne.transaction_hash, '') = m.transaction_order_key
         AND COALESCE(ne.log_index, -1) = m.log_order_key
         AND ne.logical_name_id = m.child_logical_name_id
        "#,
    );
}

/// `ORDER BY` for the arm in index order.
pub(super) fn push_child_arm_order(builder: &mut QueryBuilder<'_, Postgres>, order: HistoryOrder) {
    let direction = match order {
        HistoryOrder::Desc => "DESC",
        HistoryOrder::Asc => "ASC",
    };
    builder.push(format!(
        " ORDER BY m.block_number {direction}, m.block_hash {direction}, \
         m.transaction_order_key {direction}, m.log_order_key {direction}, \
         m.event_identity {direction}"
    ));
}

/// The history position of the cursor's event, as the history order reads it.
#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct CursorPosition {
    /// How the arm's chain compares with the cursor's chain under the database collation that
    /// orders history (-1, 0, or 1), or `None` when the cursor has no chain.
    pub(super) arm_chain_order: Option<i32>,
    pub(super) block_number: Option<i64>,
    pub(super) block_hash: Option<String>,
    pub(super) transaction_hash: Option<String>,
    pub(super) log_index: Option<i64>,
    pub(super) event_identity: String,
}

/// The arm's rows after the cursor, as an index bound. History orders `block_number DESC NULLS
/// LAST, chain_id ASC NULLS LAST, block_hash DESC NULLS LAST, transaction_hash DESC NULLS LAST,
/// log_index DESC NULLS LAST, event_identity DESC`, and `asc` is its exact reverse. The arm has
/// one chain, so the cursor's chain only decides whether the cursor's own block is included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ChildArmBound {
    Unbounded,
    Nothing,
    Block {
        operator: &'static str,
        block_number: i64,
    },
    Row {
        operator: &'static str,
        block_number: i64,
        block_hash: String,
        transaction_order_key: String,
        log_order_key: i64,
        event_identity: String,
    },
}

impl ChildArmBound {
    /// Load the cursor event's position, comparing chains in SQL so the comparison uses the
    /// collation history is ordered by.
    pub(super) async fn load_cursor(
        connection: &mut PgConnection,
        arm: &ChildArm,
        event_identity: &str,
    ) -> anyhow::Result<Option<CursorPosition>> {
        Ok(sqlx::query_as(
            "SELECT CASE WHEN chain_id IS NULL THEN NULL
                         WHEN $2 < chain_id THEN -1
                         WHEN $2 > chain_id THEN 1
                         ELSE 0 END AS arm_chain_order,
                    block_number, block_hash, transaction_hash, log_index, event_identity
             FROM bigname_phase.normalized_events WHERE event_identity = $1",
        )
        .bind(event_identity)
        .bind(&arm.chain_id)
        .fetch_optional(&mut *connection)
        .await?)
    }

    pub(super) fn after(cursor: &CursorPosition, order: HistoryOrder) -> Self {
        let descending = matches!(order, HistoryOrder::Desc);
        let Some(block_number) = cursor.block_number else {
            // A cursor without a block sorts after every positioned row descending, before
            // every positioned row ascending.
            return if descending {
                Self::Nothing
            } else {
                Self::Unbounded
            };
        };
        // Descending, chains order ascending with a missing chain last.
        let arm_chain_first = cursor
            .arm_chain_order
            .map_or(Ordering::Less, |order| order.cmp(&0));
        let block = |operator| Self::Block {
            operator,
            block_number,
        };
        match (arm_chain_first, cursor.block_hash.as_ref()) {
            // The arm's rows in the cursor's block come after it descending.
            (Ordering::Greater, _) => block(if descending { "<=" } else { ">" }),
            // They come before it descending: an earlier chain, or the same chain where the
            // cursor's missing block hash sorts last.
            (Ordering::Less, _) | (Ordering::Equal, None) => {
                block(if descending { "<" } else { ">=" })
            }
            (Ordering::Equal, Some(block_hash)) => Self::Row {
                operator: if descending { "<" } else { ">" },
                block_number,
                block_hash: block_hash.clone(),
                transaction_order_key: cursor.transaction_hash.clone().unwrap_or_default(),
                log_order_key: cursor.log_index.unwrap_or(-1),
                event_identity: cursor.event_identity.clone(),
            },
        }
    }

    pub(super) fn push<'a>(&'a self, builder: &mut QueryBuilder<'a, Postgres>) {
        match self {
            Self::Unbounded => {}
            Self::Nothing => {
                builder.push(" AND FALSE");
            }
            Self::Block {
                operator,
                block_number,
            } => {
                builder.push(format!(" AND m.block_number {operator} "));
                builder.push_bind(*block_number);
            }
            Self::Row {
                operator,
                block_number,
                block_hash,
                transaction_order_key,
                log_order_key,
                event_identity,
            } => {
                builder.push(format!(
                    " AND (m.block_number, m.block_hash, m.transaction_order_key, \
                     m.log_order_key, m.event_identity) {operator} ("
                ));
                builder.push_bind(*block_number);
                builder.push(", ");
                builder.push_bind(block_hash);
                builder.push(", ");
                builder.push_bind(transaction_order_key);
                builder.push(", ");
                builder.push_bind(*log_order_key);
                builder.push(", ");
                builder.push_bind(event_identity);
                builder.push(")");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(chain_order: Option<i32>, block: Option<i64>, hash: Option<&str>) -> CursorPosition {
        CursorPosition {
            arm_chain_order: chain_order,
            block_number: block,
            block_hash: hash.map(str::to_owned),
            transaction_hash: None,
            log_index: None,
            event_identity: "anchor".to_owned(),
        }
    }

    fn block(operator: &'static str) -> ChildArmBound {
        ChildArmBound::Block {
            operator,
            block_number: 7,
        }
    }

    #[test]
    fn the_cursor_chain_decides_whether_its_block_is_included() {
        use HistoryOrder::{Asc, Desc};
        // The arm's chain sorts after the cursor's chain.
        let arm_later = cursor(Some(1), Some(7), Some("0x07"));
        let arm_earlier = cursor(Some(-1), Some(7), Some("0x07"));
        let no_chain = cursor(None, Some(7), Some("0x07"));
        let no_hash = cursor(Some(0), Some(7), None);
        assert_eq!(ChildArmBound::after(&arm_later, Desc), block("<="));
        assert_eq!(ChildArmBound::after(&arm_later, Asc), block(">"));
        assert_eq!(ChildArmBound::after(&arm_earlier, Desc), block("<"));
        assert_eq!(ChildArmBound::after(&arm_earlier, Asc), block(">="));
        assert_eq!(ChildArmBound::after(&no_chain, Desc), block("<"));
        assert_eq!(ChildArmBound::after(&no_hash, Desc), block("<"));
        assert_eq!(ChildArmBound::after(&no_hash, Asc), block(">="));
        let unpositioned = cursor(Some(0), None, None);
        assert_eq!(
            ChildArmBound::after(&unpositioned, Desc),
            ChildArmBound::Nothing
        );
        assert_eq!(
            ChildArmBound::after(&unpositioned, Asc),
            ChildArmBound::Unbounded
        );
    }

    #[test]
    fn a_same_chain_cursor_bounds_the_whole_index_key() {
        let same = CursorPosition {
            transaction_hash: None,
            log_index: None,
            ..cursor(Some(0), Some(7), Some("0x07"))
        };
        let ChildArmBound::Row {
            operator,
            transaction_order_key,
            log_order_key,
            ..
        } = ChildArmBound::after(&same, HistoryOrder::Desc)
        else {
            panic!("a same-chain positioned cursor bounds the row key");
        };
        // A missing transaction hash or log index sorts last descending, like the keys stored
        // for it.
        assert_eq!(
            (operator, transaction_order_key.as_str(), log_order_key),
            ("<", "", -1)
        );
    }
}
