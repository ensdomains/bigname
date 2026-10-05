use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgConnection, Row, postgres::PgRow};

use super::chain_position::{
    ChainPosition, ChainPositions, SnapshotPositionRequirement, SnapshotSelectionScope,
};
use super::consistency::SnapshotConsistency;
use super::error::{SnapshotSelectionError, SnapshotSelectionResult};
use super::project::{
    PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS, validate_current_project_publications,
};
use crate::lineage::{CanonicalityState, load_chain_lineage_block_internal};
use crate::time::format_timestamp;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotAt {
    Timestamp(OffsetDateTime),
    ResolvedPositions(ChainPositions),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSelectorInput {
    pub at: Option<SnapshotAt>,
    pub chain_positions: Option<ChainPositions>,
    pub consistency: SnapshotConsistency,
    /// How many blocks the project publication may trail the stored head and still be served.
    pub publication_lag_tolerance_blocks: i64,
}

impl SnapshotSelectorInput {
    pub fn new(
        at: Option<SnapshotAt>,
        chain_positions: Option<ChainPositions>,
        consistency: SnapshotConsistency,
    ) -> SnapshotSelectionResult<Self> {
        if at.is_some() && chain_positions.is_some() {
            return Err(SnapshotSelectionError::invalid_input(
                "at and chain_positions are mutually exclusive snapshot selectors",
            ));
        }
        Ok(Self {
            at,
            chain_positions,
            consistency,
            publication_lag_tolerance_blocks: PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS,
        })
    }

    pub fn with_publication_lag_tolerance_blocks(mut self, blocks: i64) -> Self {
        self.publication_lag_tolerance_blocks = blocks;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedSnapshot {
    pub chain_positions: ChainPositions,
    pub consistency: SnapshotConsistency,
}

impl SelectedSnapshot {
    pub fn chain_positions_value(&self) -> Value {
        self.chain_positions.to_value()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotProjectionRead<T> {
    Found(T),
    NotFound,
}

/// Resolve and validate a selection on the caller's existing read transaction.
pub async fn resolve_exact_name_snapshot_selection_on(
    conn: &mut PgConnection,
    scope: &SnapshotSelectionScope,
    input: &SnapshotSelectorInput,
) -> SnapshotSelectionResult<SelectedSnapshot> {
    if input.at.is_some() && input.chain_positions.is_some() {
        return Err(SnapshotSelectionError::invalid_input(
            "at and chain_positions are mutually exclusive snapshot selectors",
        ));
    }

    let chain_positions = match (&input.at, &input.chain_positions) {
        (_, Some(chain_positions)) => {
            chain_positions.validate_scope(scope)?;
            validate_supplied_positions(&mut *conn, chain_positions, input.consistency).await?;
            chain_positions.clone()
        }
        (Some(SnapshotAt::ResolvedPositions(chain_positions)), None) => {
            chain_positions.validate_scope(scope)?;
            validate_supplied_positions(&mut *conn, chain_positions, input.consistency).await?;
            chain_positions.clone()
        }
        (Some(SnapshotAt::Timestamp(timestamp)), None) => {
            resolve_positions_at_timestamp(&mut *conn, scope, *timestamp, input.consistency).await?
        }
        (None, None) => resolve_latest_positions(&mut *conn, scope, input).await?,
    };

    validate_cross_chain_positions(scope, &chain_positions)?;
    validate_current_project_publications(
        &mut *conn,
        &chain_positions,
        input.publication_lag_tolerance_blocks,
    )
    .await?;
    Ok(SelectedSnapshot {
        chain_positions,
        consistency: input.consistency,
    })
}

pub fn ensure_projection_chain_positions_match(
    projection_family: &str,
    projection_chain_positions: &Value,
    selected_chain_positions: &ChainPositions,
) -> SnapshotSelectionResult<()> {
    let projected = ChainPositions::from_value(projection_chain_positions).map_err(|error| {
        SnapshotSelectionError::stale(format!(
            "{projection_family} projection has unusable chain_positions: {}",
            error.message()
        ))
    })?;

    if selected_chain_positions.equivalent_by_chain_id(&projected) {
        Ok(())
    } else {
        Err(SnapshotSelectionError::stale(format!(
            "{projection_family} projection does not match the selected snapshot"
        )))
    }
}

async fn validate_supplied_positions(
    conn: &mut PgConnection,
    chain_positions: &ChainPositions,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<()> {
    for position in chain_positions.as_map().values() {
        let block =
            load_chain_lineage_block_internal(&mut *conn, &position.chain_id, &position.block_hash)
                .await
                .map_err(|error| {
                    SnapshotSelectionError::internal(format!(
                        "failed to load lineage for supplied snapshot position {} {}: {error}",
                        position.chain_id, position.block_hash
                    ))
                })?
                .ok_or_else(|| {
                    SnapshotSelectionError::conflict(format!(
                        "snapshot position {} {} is not present in stored lineage",
                        position.chain_id, position.block_hash
                    ))
                })?;

        if block.block_number != position.block_number {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position {} {} has block_number {}, stored lineage has {}",
                position.chain_id, position.block_hash, position.block_number, block.block_number
            )));
        }
        if block.block_timestamp != position.timestamp {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position {} {} has timestamp {}, stored lineage has {}",
                position.chain_id,
                position.block_hash,
                format_timestamp(position.timestamp),
                format_timestamp(block.block_timestamp)
            )));
        }
        if !consistency.allows(block.canonicality_state) {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position {} {} does not satisfy consistency {}",
                position.chain_id,
                position.block_hash,
                consistency.as_str()
            )));
        }
    }

    Ok(())
}

async fn resolve_latest_positions(
    conn: &mut PgConnection,
    scope: &SnapshotSelectionScope,
    input: &SnapshotSelectorInput,
) -> SnapshotSelectionResult<ChainPositions> {
    let consistency = input.consistency;
    let mut positions = BTreeMap::new();

    if let Some(authoritative_slot) = scope.authoritative_slot() {
        let authoritative_requirement = scope.requirement_for_slot(authoritative_slot).ok_or_else(
            || {
                SnapshotSelectionError::invalid_input(format!(
                    "authoritative snapshot slot {authoritative_slot} is not required by the scope"
                ))
            },
        )?;
        let authoritative_position =
            load_phase_head_position(&mut *conn, authoritative_requirement, input).await?;
        let upper_bound = authoritative_position.timestamp;
        positions.insert(authoritative_position.slot.clone(), authoritative_position);

        for requirement in scope.required_positions() {
            if requirement.slot == authoritative_slot {
                continue;
            }
            let position = load_lineage_position_at_or_before(
                &mut *conn,
                requirement,
                upper_bound,
                consistency,
            )
            .await?;
            positions.insert(position.slot.clone(), position);
        }

        return Ok(ChainPositions::new(positions));
    }

    for requirement in scope.required_positions() {
        let position = load_phase_head_position(&mut *conn, requirement, input).await?;
        positions.insert(position.slot.clone(), position);
    }

    Ok(ChainPositions::new(positions))
}

async fn resolve_positions_at_timestamp(
    conn: &mut PgConnection,
    scope: &SnapshotSelectionScope,
    timestamp: OffsetDateTime,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPositions> {
    let mut positions = BTreeMap::new();

    if let Some(authoritative_slot) = scope.authoritative_slot() {
        let authoritative_requirement = scope.requirement_for_slot(authoritative_slot).ok_or_else(
            || {
                SnapshotSelectionError::invalid_input(format!(
                    "authoritative snapshot slot {authoritative_slot} is not required by the scope"
                ))
            },
        )?;
        let authoritative_position = load_lineage_position_at_or_before(
            &mut *conn,
            authoritative_requirement,
            timestamp,
            consistency,
        )
        .await?;
        let upper_bound = authoritative_position.timestamp;
        positions.insert(authoritative_position.slot.clone(), authoritative_position);

        for requirement in scope.required_positions() {
            if requirement.slot == authoritative_slot {
                continue;
            }
            let position = load_lineage_position_at_or_before(
                &mut *conn,
                requirement,
                upper_bound,
                consistency,
            )
            .await?;
            positions.insert(position.slot.clone(), position);
        }

        return Ok(ChainPositions::new(positions));
    }

    for requirement in scope.required_positions() {
        let position =
            load_lineage_position_at_or_before(&mut *conn, requirement, timestamp, consistency)
                .await?;
        positions.insert(position.slot.clone(), position);
    }

    Ok(ChainPositions::new(positions))
}

async fn load_phase_head_position(
    conn: &mut PgConnection,
    requirement: &SnapshotPositionRequirement,
    input: &SnapshotSelectorInput,
) -> SnapshotSelectionResult<ChainPosition> {
    let consistency = input.consistency;
    let row = sqlx::query(
        r#"
        SELECT
            latest_block_hash,
            latest_block_number,
            CASE $2
                WHEN 'head' THEN latest_block_hash
                WHEN 'safe' THEN safe_block_hash
                WHEN 'finalized' THEN finalized_block_hash
            END AS block_hash,
            CASE $2
                WHEN 'head' THEN latest_block_number
                WHEN 'safe' THEN safe_block_number
                WHEN 'finalized' THEN finalized_block_number
            END AS block_number
        FROM chain_heads
        WHERE chain_id = $1
        "#,
    )
    .bind(&requirement.chain_id)
    .bind(consistency.as_str())
    .fetch_optional(&mut *conn)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load schema-v2 head for chain {} at consistency {}: {error}",
            requirement.chain_id,
            consistency.as_str()
        ))
    })?
    .ok_or_else(|| {
        SnapshotSelectionError::conflict(format!(
            "chain {} has no stored schema-v2 head",
            requirement.chain_id
        ))
    })?;

    let latest_block_hash = row
        .try_get::<String, _>("latest_block_hash")
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode latest schema-v2 head hash for chain {}: {error}",
                requirement.chain_id
            ))
        })?;
    let latest_block_number = row
        .try_get::<i64, _>("latest_block_number")
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode latest schema-v2 head number for chain {}: {error}",
                requirement.chain_id
            ))
        })?;
    let block_hash = row
        .try_get::<Option<String>, _>("block_hash")
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode {} schema-v2 head hash for chain {}: {error}",
                consistency.as_str(),
                requirement.chain_id
            ))
        })?;
    let block_number = row
        .try_get::<Option<i64>, _>("block_number")
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode {} schema-v2 head number for chain {}: {error}",
                consistency.as_str(),
                requirement.chain_id
            ))
        })?;
    let (block_hash, block_number) = match (block_hash, block_number) {
        (Some(block_hash), Some(block_number)) => (block_hash, block_number),
        (None, None) => {
            return Err(SnapshotSelectionError::conflict(format!(
                "chain {} has no current {} schema-v2 position",
                requirement.chain_id,
                consistency.as_str()
            )));
        }
        _ => {
            return Err(SnapshotSelectionError::conflict(format!(
                "chain {} has a mismatched hash and number for its {} schema-v2 position",
                requirement.chain_id,
                consistency.as_str()
            )));
        }
    };

    // Serve at the family marker's publication. Live-follow moves the stored head the moment a
    // block arrives and Project publishes a few seconds later, so requiring the publication to sit
    // exactly at the stored head rejected most reads under real block cadence. A publication a
    // few blocks behind is still one consistent, canonical snapshot, served (as `as_of`) at its
    // own position when behind the requested one. A publication ahead of the head or further
    // behind than the tolerance ([`PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS`] by default) is stale.
    let publication =
        super::project::load_current_project_publication(&mut *conn, &requirement.chain_id)
            .await?
            .ok_or_else(|| {
                SnapshotSelectionError::stale(super::project::unpublished_message(
                    &requirement.chain_id,
                ))
            })?;
    let (block_hash, block_number) = if publication.block_number == latest_block_number
        && publication.block_hash == latest_block_hash
    {
        (block_hash, block_number)
    } else {
        if !(0..=input.publication_lag_tolerance_blocks)
            .contains(&(latest_block_number - publication.block_number))
        {
            return Err(SnapshotSelectionError::stale(format!(
                "{} (publication at {} is outside the lag tolerance of head {})",
                super::project::unpublished_message(&requirement.chain_id),
                publication.block_number,
                latest_block_number
            )));
        }
        if publication.block_number < block_number {
            (publication.block_hash, publication.block_number)
        } else {
            (block_hash, block_number)
        }
    };

    let block = load_chain_lineage_block_internal(&mut *conn, &requirement.chain_id, &block_hash)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load schema-v2 lineage for chain {} block {}: {error}",
                requirement.chain_id, block_hash
            ))
        })?
        .ok_or_else(|| {
            SnapshotSelectionError::conflict(format!(
                "schema-v2 position for chain {} references missing lineage block {}",
                requirement.chain_id, block_hash
            ))
        })?;
    if block.block_number != block_number {
        return Err(SnapshotSelectionError::conflict(format!(
            "schema-v2 position for chain {} block {} stores number {}, lineage stores {}",
            requirement.chain_id, block_hash, block_number, block.block_number
        )));
    }
    if !consistency.allows(block.canonicality_state) {
        return Err(SnapshotSelectionError::conflict(format!(
            "schema-v2 position for chain {} block {} does not satisfy consistency {}",
            requirement.chain_id,
            block_hash,
            consistency.as_str()
        )));
    }

    Ok(ChainPosition {
        slot: requirement.slot.clone(),
        chain_id: block.chain_id,
        block_number: block.block_number,
        block_hash: block.block_hash,
        timestamp: block.block_timestamp,
    })
}

async fn load_lineage_position_at_or_before(
    conn: &mut PgConnection,
    requirement: &SnapshotPositionRequirement,
    upper_bound: OffsetDateTime,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPosition> {
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND (
              ($3 = 'head' AND canonicality_state IN (
                  'canonical'::bigname_phase.canonicality_state,
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              ))
              OR ($3 = 'safe' AND canonicality_state IN (
                  'safe'::bigname_phase.canonicality_state,
                  'finalized'::bigname_phase.canonicality_state
              ))
              OR ($3 = 'finalized' AND canonicality_state = 'finalized'::bigname_phase.canonicality_state)
          )
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#,
    )
    .bind(&requirement.chain_id)
    .bind(upper_bound)
    .bind(consistency.as_str())
    .fetch_optional(&mut *conn)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load lineage position for chain {} at consistency {}: {error}",
            requirement.chain_id,
            consistency.as_str()
        ))
    })?;

    let row = row.ok_or_else(|| {
        SnapshotSelectionError::conflict(format!(
            "chain {} has no stored {} lineage position at or before {}",
            requirement.chain_id,
            consistency.as_str(),
            format_timestamp(upper_bound)
        ))
    })?;
    decode_lineage_position(requirement, row, consistency)
}

fn decode_lineage_position(
    requirement: &SnapshotPositionRequirement,
    row: PgRow,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPosition> {
    let canonicality_state = row
        .try_get::<String, _>("canonicality_state")
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage canonicality state for chain {}: {error}",
                requirement.chain_id
            ))
        })
        .and_then(|value| {
            CanonicalityState::parse(&value).map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to parse lineage canonicality state for chain {}: {error}",
                    requirement.chain_id
                ))
            })
        })?;
    if !consistency.allows(canonicality_state) {
        return Err(SnapshotSelectionError::conflict(format!(
            "lineage position for chain {} does not satisfy consistency {}",
            requirement.chain_id,
            consistency.as_str()
        )));
    }

    Ok(ChainPosition {
        slot: requirement.slot.clone(),
        chain_id: row.try_get("chain_id").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage chain_id for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
        block_hash: row.try_get("block_hash").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage block_hash for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
        block_number: row.try_get("block_number").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage block_number for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
        timestamp: row.try_get("block_timestamp").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage block_timestamp for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
    })
}

fn validate_cross_chain_positions(
    scope: &SnapshotSelectionScope,
    chain_positions: &ChainPositions,
) -> SnapshotSelectionResult<()> {
    let Some(authoritative_slot) = scope.authoritative_slot() else {
        return Ok(());
    };
    let authoritative = chain_positions.get(authoritative_slot).ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!(
            "missing authoritative snapshot position slot {authoritative_slot}"
        ))
    })?;

    for (slot, position) in chain_positions.as_map() {
        if slot == authoritative_slot {
            continue;
        }
        if position.timestamp > authoritative.timestamp {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position slot {slot} is newer than authoritative slot {authoritative_slot}"
            )));
        }
    }

    Ok(())
}
