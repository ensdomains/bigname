use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde_json::{Value, json};
use sqlx::types::time::{OffsetDateTime, UtcOffset};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::checkpoints::{ChainCheckpoint, CheckpointBlockRef, load_chain_checkpoint};
use crate::lineage::{CanonicalityState, load_chain_lineage_block};

/// API-facing failure class for exact-name snapshot selection and projection eligibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotSelectionErrorKind {
    InvalidInput,
    Conflict,
    Stale,
    InternalError,
}

impl SnapshotSelectionErrorKind {
    pub const fn api_error_code(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::Conflict => "conflict",
            Self::Stale => "stale",
            Self::InternalError => "internal_error",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSelectionError {
    kind: SnapshotSelectionErrorKind,
    message: String,
}

impl SnapshotSelectionError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::InvalidInput,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::Conflict,
            message: message.into(),
        }
    }

    pub fn stale(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::Stale,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotSelectionErrorKind::InternalError,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> SnapshotSelectionErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn api_error_code(&self) -> &'static str {
        self.kind.api_error_code()
    }
}

impl fmt::Display for SnapshotSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.kind.api_error_code(),
            self.message
        )
    }
}

impl std::error::Error for SnapshotSelectionError {}

pub type SnapshotSelectionResult<T> = std::result::Result<T, SnapshotSelectionError>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SnapshotConsistency {
    #[default]
    Head,
    Safe,
    Finalized,
}

impl SnapshotConsistency {
    pub fn parse(value: Option<&str>) -> SnapshotSelectionResult<Self> {
        match value.unwrap_or("head") {
            "head" => Ok(Self::Head),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            other => Err(SnapshotSelectionError::invalid_input(format!(
                "unsupported snapshot consistency {other}"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }

    fn allows(self, state: CanonicalityState) -> bool {
        match self {
            Self::Head => matches!(
                state,
                CanonicalityState::Canonical
                    | CanonicalityState::Safe
                    | CanonicalityState::Finalized
            ),
            Self::Safe => matches!(
                state,
                CanonicalityState::Safe | CanonicalityState::Finalized
            ),
            Self::Finalized => state == CanonicalityState::Finalized,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotPositionRequirement {
    pub slot: String,
    pub chain_id: String,
}

impl SnapshotPositionRequirement {
    pub fn new(slot: impl Into<String>, chain_id: impl Into<String>) -> Self {
        Self {
            slot: slot.into(),
            chain_id: chain_id.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSelectionScope {
    required_positions: Vec<SnapshotPositionRequirement>,
    authoritative_slot: Option<String>,
}

impl SnapshotSelectionScope {
    pub fn new(
        required_positions: Vec<SnapshotPositionRequirement>,
        authoritative_slot: Option<String>,
    ) -> SnapshotSelectionResult<Self> {
        if required_positions.is_empty() {
            return Err(SnapshotSelectionError::invalid_input(
                "snapshot selection scope must require at least one position slot",
            ));
        }

        let mut seen_slots = BTreeSet::new();
        let mut seen_chain_ids = BTreeSet::new();
        for requirement in &required_positions {
            if requirement.slot.trim().is_empty() {
                return Err(SnapshotSelectionError::invalid_input(
                    "snapshot selection scope contains an empty position slot",
                ));
            }
            if requirement.chain_id.trim().is_empty() {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot selection slot {} has an empty chain_id",
                    requirement.slot
                )));
            }
            if !seen_slots.insert(requirement.slot.clone()) {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot selection scope repeats position slot {}",
                    requirement.slot
                )));
            }
            if !seen_chain_ids.insert(requirement.chain_id.clone()) {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot selection scope repeats chain_id {}",
                    requirement.chain_id
                )));
            }
        }

        if let Some(authoritative_slot) = authoritative_slot.as_ref()
            && !seen_slots.contains(authoritative_slot)
        {
            return Err(SnapshotSelectionError::invalid_input(format!(
                "authoritative snapshot slot {authoritative_slot} is not required by the scope"
            )));
        }

        Ok(Self {
            required_positions,
            authoritative_slot,
        })
    }

    pub fn required_positions(&self) -> &[SnapshotPositionRequirement] {
        &self.required_positions
    }

    pub fn authoritative_slot(&self) -> Option<&str> {
        self.authoritative_slot.as_deref()
    }

    fn requirement_for_slot(&self, slot: &str) -> Option<&SnapshotPositionRequirement> {
        self.required_positions
            .iter()
            .find(|requirement| requirement.slot == slot)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainPosition {
    pub slot: String,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: OffsetDateTime,
}

impl ChainPosition {
    pub fn to_value(&self) -> Value {
        json!({
            "chain_id": self.chain_id,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "timestamp": format_timestamp(self.timestamp),
        })
    }

    fn identity(&self) -> ChainPositionIdentity {
        ChainPositionIdentity {
            block_number: self.block_number,
            block_hash: self.block_hash.clone(),
            timestamp: self.timestamp,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainPositions {
    positions: BTreeMap<String, ChainPosition>,
}

impl ChainPositions {
    pub fn new(positions: BTreeMap<String, ChainPosition>) -> Self {
        Self { positions }
    }

    pub fn from_value(value: &Value) -> SnapshotSelectionResult<Self> {
        decode_chain_positions_value(value, "chain_positions")
    }

    pub fn parse_explicit_json(
        raw: &str,
        scope: &SnapshotSelectionScope,
    ) -> SnapshotSelectionResult<Self> {
        let value = serde_json::from_str::<Value>(raw).map_err(|error| {
            SnapshotSelectionError::invalid_input(format!(
                "chain_positions must be one JSON object: {error}"
            ))
        })?;
        reject_duplicate_top_level_slots(raw)?;
        let positions = decode_chain_positions_value(&value, "chain_positions")?;
        positions.validate_scope(scope)?;
        Ok(positions)
    }

    pub fn as_map(&self) -> &BTreeMap<String, ChainPosition> {
        &self.positions
    }

    pub fn get(&self, slot: &str) -> Option<&ChainPosition> {
        self.positions.get(slot)
    }

    pub fn to_value(&self) -> Value {
        Value::Object(
            self.positions
                .iter()
                .map(|(slot, position)| (slot.clone(), position.to_value()))
                .collect(),
        )
    }

    pub fn equivalent_by_chain_id(&self, other: &Self) -> bool {
        let Some(left) = self.by_chain_id() else {
            return false;
        };
        let Some(right) = other.by_chain_id() else {
            return false;
        };
        left == right
    }

    fn validate_scope(&self, scope: &SnapshotSelectionScope) -> SnapshotSelectionResult<()> {
        for slot in self.positions.keys() {
            if scope.requirement_for_slot(slot).is_none() {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "unsupported snapshot position slot {slot}"
                )));
            }
        }

        for requirement in scope.required_positions() {
            let Some(position) = self.positions.get(&requirement.slot) else {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "missing required snapshot position slot {}",
                    requirement.slot
                )));
            };
            if position.chain_id != requirement.chain_id {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot position slot {} names chain_id {}, expected {}",
                    requirement.slot, position.chain_id, requirement.chain_id
                )));
            }
        }

        Ok(())
    }

    fn by_chain_id(&self) -> Option<BTreeMap<String, ChainPositionIdentity>> {
        let mut by_chain_id = BTreeMap::new();
        for position in self.positions.values() {
            if by_chain_id
                .insert(position.chain_id.clone(), position.identity())
                .is_some()
            {
                return None;
            }
        }
        Some(by_chain_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChainPositionIdentity {
    block_number: i64,
    block_hash: String,
    timestamp: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotAt {
    Timestamp(OffsetDateTime),
    ResolvedPositions(ChainPositions),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotSelectorInput {
    pub at: Option<SnapshotAt>,
    pub chain_positions: Option<ChainPositions>,
    pub consistency: SnapshotConsistency,
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
        })
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

pub async fn resolve_exact_name_snapshot_selection(
    pool: &PgPool,
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
            validate_supplied_positions(pool, chain_positions, input.consistency).await?;
            chain_positions.clone()
        }
        (Some(SnapshotAt::ResolvedPositions(chain_positions)), None) => {
            chain_positions.validate_scope(scope)?;
            validate_supplied_positions(pool, chain_positions, input.consistency).await?;
            chain_positions.clone()
        }
        (Some(SnapshotAt::Timestamp(timestamp)), None) => {
            resolve_positions_at_timestamp(pool, scope, *timestamp, input.consistency).await?
        }
        (None, None) => resolve_latest_positions(pool, scope, input.consistency).await?,
    };

    validate_cross_chain_positions(scope, &chain_positions)?;
    Ok(SelectedSnapshot {
        chain_positions,
        consistency: input.consistency,
    })
}

pub fn parse_rfc3339_utc_timestamp(value: &str) -> SnapshotSelectionResult<OffsetDateTime> {
    if value.len() != 20
        || !matches!(value.as_bytes().get(4), Some(b'-'))
        || !matches!(value.as_bytes().get(7), Some(b'-'))
        || !matches!(value.as_bytes().get(10), Some(b'T'))
        || !matches!(value.as_bytes().get(13), Some(b':'))
        || !matches!(value.as_bytes().get(16), Some(b':'))
        || !matches!(value.as_bytes().get(19), Some(b'Z'))
    {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} must use RFC 3339 UTC seconds format"
        )));
    }

    let year = parse_digits_i32(value, 0, 4, "year")?;
    let month = parse_digits_u8(value, 5, 7, "month")?;
    let day = parse_digits_u8(value, 8, 10, "day")?;
    let hour = parse_digits_u8(value, 11, 13, "hour")?;
    let minute = parse_digits_u8(value, 14, 16, "minute")?;
    let second = parse_digits_u8(value, 17, 19, "second")?;

    validate_date_parts(value, year, month, day)?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} has invalid time"
        )));
    } else {
        let days = days_from_civil(year, month, day);
        let seconds = days
            .checked_mul(86_400)
            .and_then(|value| value.checked_add(i64::from(hour) * 3_600))
            .and_then(|value| value.checked_add(i64::from(minute) * 60))
            .and_then(|value| value.checked_add(i64::from(second)))
            .ok_or_else(|| {
                SnapshotSelectionError::invalid_input(format!(
                    "timestamp {value} is outside the supported range"
                ))
            })?;
        OffsetDateTime::from_unix_timestamp(seconds).map_err(|_| {
            SnapshotSelectionError::invalid_input(format!(
                "timestamp {value} is outside the supported range"
            ))
        })
    }
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
    pool: &PgPool,
    chain_positions: &ChainPositions,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<()> {
    for position in chain_positions.as_map().values() {
        let block = load_chain_lineage_block(pool, &position.chain_id, &position.block_hash)
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
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
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
        let authoritative_position =
            load_checkpoint_position(pool, authoritative_requirement, consistency).await?;
        let upper_bound = authoritative_position.timestamp;
        positions.insert(authoritative_position.slot.clone(), authoritative_position);

        for requirement in scope.required_positions() {
            if requirement.slot == authoritative_slot {
                continue;
            }
            let position =
                load_lineage_position_at_or_before(pool, requirement, upper_bound, consistency)
                    .await?;
            positions.insert(position.slot.clone(), position);
        }

        return Ok(ChainPositions::new(positions));
    }

    for requirement in scope.required_positions() {
        let position = load_checkpoint_position(pool, requirement, consistency).await?;
        positions.insert(position.slot.clone(), position);
    }

    Ok(ChainPositions::new(positions))
}

async fn resolve_positions_at_timestamp(
    pool: &PgPool,
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
            pool,
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
            let position =
                load_lineage_position_at_or_before(pool, requirement, upper_bound, consistency)
                    .await?;
            positions.insert(position.slot.clone(), position);
        }

        return Ok(ChainPositions::new(positions));
    }

    for requirement in scope.required_positions() {
        let position =
            load_lineage_position_at_or_before(pool, requirement, timestamp, consistency).await?;
        positions.insert(position.slot.clone(), position);
    }

    Ok(ChainPositions::new(positions))
}

async fn load_checkpoint_position(
    pool: &PgPool,
    requirement: &SnapshotPositionRequirement,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPosition> {
    let checkpoint = load_chain_checkpoint(pool, &requirement.chain_id)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load checkpoint for chain {}: {error}",
                requirement.chain_id
            ))
        })?
        .ok_or_else(|| {
            SnapshotSelectionError::conflict(format!(
                "chain {} has no stored checkpoint row",
                requirement.chain_id
            ))
        })?;
    let checkpoint_ref = checkpoint_block_ref(&checkpoint, consistency).ok_or_else(|| {
        SnapshotSelectionError::conflict(format!(
            "chain {} has no {} checkpoint",
            requirement.chain_id,
            consistency.as_str()
        ))
    })?;

    let block = load_chain_lineage_block(pool, &requirement.chain_id, &checkpoint_ref.block_hash)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load lineage for checkpoint {} {}: {error}",
                requirement.chain_id, checkpoint_ref.block_hash
            ))
        })?
        .ok_or_else(|| {
            SnapshotSelectionError::conflict(format!(
                "checkpoint for chain {} references missing lineage block {}",
                requirement.chain_id, checkpoint_ref.block_hash
            ))
        })?;

    if block.block_number != checkpoint_ref.block_number {
        return Err(SnapshotSelectionError::conflict(format!(
            "checkpoint for chain {} block {} stores number {}, lineage stores {}",
            requirement.chain_id,
            checkpoint_ref.block_hash,
            checkpoint_ref.block_number,
            block.block_number
        )));
    }
    if !consistency.allows(block.canonicality_state) {
        return Err(SnapshotSelectionError::conflict(format!(
            "checkpoint for chain {} block {} does not satisfy consistency {}",
            requirement.chain_id,
            checkpoint_ref.block_hash,
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
    pool: &PgPool,
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
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND (
              ($3 = 'head' AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              ))
              OR ($3 = 'safe' AND canonicality_state IN (
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              ))
              OR ($3 = 'finalized' AND canonicality_state = 'finalized'::canonicality_state)
          )
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#,
    )
    .bind(&requirement.chain_id)
    .bind(upper_bound)
    .bind(consistency.as_str())
    .fetch_optional(pool)
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

fn checkpoint_block_ref(
    checkpoint: &ChainCheckpoint,
    consistency: SnapshotConsistency,
) -> Option<CheckpointBlockRef> {
    match consistency {
        SnapshotConsistency::Head => Some(CheckpointBlockRef {
            block_hash: checkpoint.canonical_block_hash.clone()?,
            block_number: checkpoint.canonical_block_number?,
        }),
        SnapshotConsistency::Safe => Some(CheckpointBlockRef {
            block_hash: checkpoint.safe_block_hash.clone()?,
            block_number: checkpoint.safe_block_number?,
        }),
        SnapshotConsistency::Finalized => Some(CheckpointBlockRef {
            block_hash: checkpoint.finalized_block_hash.clone()?,
            block_number: checkpoint.finalized_block_number?,
        }),
    }
}

fn decode_chain_positions_value(
    value: &Value,
    field_name: &str,
) -> SnapshotSelectionResult<ChainPositions> {
    let object = value.as_object().ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!("{field_name} must be a JSON object"))
    })?;
    let mut positions = BTreeMap::new();
    for (slot, position) in object {
        if slot.trim().is_empty() {
            return Err(SnapshotSelectionError::invalid_input(format!(
                "{field_name} contains an empty position slot"
            )));
        }
        positions.insert(
            slot.clone(),
            decode_chain_position_value(field_name, slot, position)?,
        );
    }
    Ok(ChainPositions::new(positions))
}

fn decode_chain_position_value(
    field_name: &str,
    slot: &str,
    value: &Value,
) -> SnapshotSelectionResult<ChainPosition> {
    let object = value.as_object().ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!("{field_name}.{slot} must be an object"))
    })?;
    let chain_id = required_string_field(object, "chain_id", field_name, slot)?;
    let block_hash = required_string_field(object, "block_hash", field_name, slot)?;
    let block_number = object
        .get("block_number")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            SnapshotSelectionError::invalid_input(format!(
                "{field_name}.{slot}.block_number must be an integer"
            ))
        })?;
    if block_number < 0 {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "{field_name}.{slot}.block_number must not be negative"
        )));
    }
    let timestamp = parse_rfc3339_utc_timestamp(&required_string_field(
        object,
        "timestamp",
        field_name,
        slot,
    )?)?;

    Ok(ChainPosition {
        slot: slot.to_owned(),
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

fn required_string_field(
    object: &serde_json::Map<String, Value>,
    key: &str,
    field_name: &str,
    slot: &str,
) -> SnapshotSelectionResult<String> {
    let value = object.get(key).and_then(Value::as_str).ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!("{field_name}.{slot}.{key} must be a string"))
    })?;
    if value.trim().is_empty() {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "{field_name}.{slot}.{key} must not be empty"
        )));
    }
    Ok(value.to_owned())
}

fn reject_duplicate_top_level_slots(raw: &str) -> SnapshotSelectionResult<()> {
    let slots = top_level_object_keys(raw)?;
    let mut seen = BTreeSet::new();
    for slot in slots {
        if !seen.insert(slot.clone()) {
            return Err(SnapshotSelectionError::invalid_input(format!(
                "chain_positions repeats position slot {slot}"
            )));
        }
    }
    Ok(())
}

fn top_level_object_keys(raw: &str) -> SnapshotSelectionResult<Vec<String>> {
    let mut index = skip_whitespace(raw, 0);
    if raw.as_bytes().get(index) != Some(&b'{') {
        return Err(SnapshotSelectionError::invalid_input(
            "chain_positions must be one JSON object",
        ));
    }
    index += 1;

    let mut keys = Vec::new();
    loop {
        index = skip_whitespace(raw, index);
        match raw.as_bytes().get(index) {
            Some(b'}') => {
                index += 1;
                break;
            }
            Some(b'"') => {}
            _ => {
                return Err(SnapshotSelectionError::invalid_input(
                    "chain_positions object keys must be strings",
                ));
            }
        }

        let (key, next_index) = read_json_string(raw, index)?;
        keys.push(key);
        index = skip_whitespace(raw, next_index);
        if raw.as_bytes().get(index) != Some(&b':') {
            return Err(SnapshotSelectionError::invalid_input(
                "chain_positions object key must be followed by ':'",
            ));
        }
        index = skip_json_value(raw, skip_whitespace(raw, index + 1))?;
        index = skip_whitespace(raw, index);
        match raw.as_bytes().get(index) {
            Some(b',') => index += 1,
            Some(b'}') => {
                index += 1;
                break;
            }
            _ => {
                return Err(SnapshotSelectionError::invalid_input(
                    "chain_positions object entries must be separated by ','",
                ));
            }
        }
    }

    if skip_whitespace(raw, index) != raw.len() {
        return Err(SnapshotSelectionError::invalid_input(
            "chain_positions must contain exactly one JSON object",
        ));
    }

    Ok(keys)
}

fn read_json_string(raw: &str, start: usize) -> SnapshotSelectionResult<(String, usize)> {
    let end = skip_json_string(raw, start)?;
    let decoded = serde_json::from_str::<String>(&raw[start..end]).map_err(|error| {
        SnapshotSelectionError::invalid_input(format!(
            "chain_positions object key is not a valid JSON string: {error}"
        ))
    })?;
    Ok((decoded, end))
}

fn skip_json_string(raw: &str, start: usize) -> SnapshotSelectionResult<usize> {
    if raw.as_bytes().get(start) != Some(&b'"') {
        return Err(SnapshotSelectionError::invalid_input(
            "expected JSON string",
        ));
    }
    let mut escaped = false;
    let mut index = start + 1;
    while let Some(byte) = raw.as_bytes().get(index) {
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        match byte {
            b'\\' => {
                escaped = true;
                index += 1;
            }
            b'"' => return Ok(index + 1),
            _ => index += 1,
        }
    }
    Err(SnapshotSelectionError::invalid_input(
        "unterminated JSON string",
    ))
}

fn skip_json_value(raw: &str, start: usize) -> SnapshotSelectionResult<usize> {
    if start >= raw.len() {
        return Err(SnapshotSelectionError::invalid_input(
            "chain_positions object value is missing",
        ));
    }
    let mut depth = 0_i32;
    let mut index = start;
    while let Some(byte) = raw.as_bytes().get(index) {
        match byte {
            b'"' => index = skip_json_string(raw, index)?,
            b'{' | b'[' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                if depth == 0 {
                    return Ok(index);
                }
                depth -= 1;
                index += 1;
            }
            b']' => {
                if depth == 0 {
                    return Err(SnapshotSelectionError::invalid_input(
                        "unexpected ']' in chain_positions object",
                    ));
                }
                depth -= 1;
                index += 1;
            }
            b',' if depth == 0 => return Ok(index),
            _ => index += 1,
        }
    }
    Ok(index)
}

fn skip_whitespace(raw: &str, mut index: usize) -> usize {
    while matches!(
        raw.as_bytes().get(index),
        Some(b' ' | b'\n' | b'\r' | b'\t')
    ) {
        index += 1;
    }
    index
}

fn parse_digits_i32(
    value: &str,
    start: usize,
    end: usize,
    part: &str,
) -> SnapshotSelectionResult<i32> {
    value[start..end]
        .parse::<i32>()
        .map_err(|_| SnapshotSelectionError::invalid_input(format!("timestamp has invalid {part}")))
}

fn parse_digits_u8(
    value: &str,
    start: usize,
    end: usize,
    part: &str,
) -> SnapshotSelectionResult<u8> {
    value[start..end]
        .parse::<u8>()
        .map_err(|_| SnapshotSelectionError::invalid_input(format!("timestamp has invalid {part}")))
}

fn validate_date_parts(value: &str, year: i32, month: u8, day: u8) -> SnapshotSelectionResult<()> {
    if !(1..=12).contains(&month) {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} has invalid month"
        )));
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} has invalid date"
        )));
    }
    Ok(())
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let adjusted_year = i64::from(year) - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month = i64::from(month);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn scope() -> SnapshotSelectionScope {
        SnapshotSelectionScope::new(
            vec![SnapshotPositionRequirement::new(
                "ethereum",
                "ethereum-mainnet",
            )],
            Some("ethereum".to_owned()),
        )
        .expect("test scope must be valid")
    }

    #[test]
    fn explicit_chain_positions_reject_duplicate_slots() {
        let error = ChainPositions::parse_explicit_json(
            r#"{
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 1,
                    "block_hash": "0x1",
                    "timestamp": "2026-04-17T00:00:01Z"
                },
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 2,
                    "block_hash": "0x2",
                    "timestamp": "2026-04-17T00:00:02Z"
                }
            }"#,
            &scope(),
        )
        .expect_err("duplicate slots must be invalid");

        assert_eq!(error.kind(), SnapshotSelectionErrorKind::InvalidInput);
        assert!(error.message().contains("repeats position slot ethereum"));
    }

    #[test]
    fn explicit_chain_positions_reject_missing_and_wrong_profile_slots() {
        let missing = ChainPositions::parse_explicit_json("{}", &scope())
            .expect_err("missing required slot must be invalid");
        assert_eq!(missing.kind(), SnapshotSelectionErrorKind::InvalidInput);

        let wrong_chain = ChainPositions::parse_explicit_json(
            r#"{
                "ethereum": {
                    "chain_id": "ethereum-sepolia",
                    "block_number": 1,
                    "block_hash": "0x1",
                    "timestamp": "2026-04-17T00:00:01Z"
                }
            }"#,
            &scope(),
        )
        .expect_err("mixed profile chain must be invalid");
        assert_eq!(wrong_chain.kind(), SnapshotSelectionErrorKind::InvalidInput);
        assert!(wrong_chain.message().contains("expected ethereum-mainnet"));
    }

    #[test]
    fn projection_chain_positions_match_by_chain_identity() {
        let selected = ChainPositions::from_value(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 7,
                "block_hash": "0x7",
                "timestamp": "2026-04-17T00:00:07Z"
            }
        }))
        .expect("selected positions must decode");
        let projected = json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": 7,
                "block_hash": "0x7",
                "timestamp": "2026-04-17T00:00:07Z"
            }
        });

        ensure_projection_chain_positions_match("name_current", &projected, &selected)
            .expect("slot aliases with the same chain identity should match");

        let stale = ensure_projection_chain_positions_match(
            "name_current",
            &json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 8,
                    "block_hash": "0x8",
                    "timestamp": "2026-04-17T00:00:08Z"
                }
            }),
            &selected,
        )
        .expect_err("different chain position must be stale");
        assert_eq!(stale.kind(), SnapshotSelectionErrorKind::Stale);
    }
}
