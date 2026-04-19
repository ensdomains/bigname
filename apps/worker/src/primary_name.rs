use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    clear_primary_names_current, delete_primary_name_current,
    upsert_primary_name_current_snapshots,
};
use serde_json::{Map, Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
};

const ENS_NAMESPACE: &str = "ens";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryNamesCurrentRebuildSummary {
    pub requested_tuple_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
    pub success_row_count: usize,
    pub not_found_row_count: usize,
    pub invalid_name_row_count: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PrimaryNameTupleKey {
    address: String,
    namespace: String,
    coin_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseClaimTuple {
    key: PrimaryNameTupleKey,
    claim_provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NameClaimObservation {
    key: PrimaryNameTupleKey,
    raw_name: Option<String>,
    primary_claim_source: Value,
}

pub async fn rebuild_primary_names_current(
    pool: &PgPool,
    address: Option<&str>,
    namespace: Option<&str>,
    coin_type: Option<&str>,
) -> Result<PrimaryNamesCurrentRebuildSummary> {
    match (address, namespace, coin_type) {
        (Some(address), Some(namespace), Some(coin_type)) => {
            rebuild_one_primary_name(pool, address, namespace, coin_type).await
        }
        (None, None, None) => rebuild_all_primary_names(pool).await,
        _ => bail!(
            "primary_names_current rebuild requires address, namespace, and coin_type together when targeting one tuple"
        ),
    }
}

async fn rebuild_all_primary_names(pool: &PgPool) -> Result<PrimaryNamesCurrentRebuildSummary> {
    let tuples = load_reverse_claim_tuples(pool).await?;
    let claim_observations = load_latest_name_claim_observations(pool).await?;
    let deleted_row_count = clear_primary_names_current(pool).await?;
    let projections = tuples
        .iter()
        .map(|tuple| {
            let observation = claim_observations.get(&tuple.key);
            primary_name_row(tuple, observation)
        })
        .collect::<Result<Vec<PrimaryNameCurrentSnapshot>>>()?;
    let rows = projections
        .iter()
        .map(|projection| projection.row.clone())
        .collect::<Vec<_>>();
    let upserted_row_count = upsert_primary_name_current_snapshots(pool, &projections)
        .await?
        .len();
    let status_counts = count_statuses(&rows);

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count: tuples.len(),
        upserted_row_count,
        deleted_row_count,
        success_row_count: status_counts.success_row_count,
        not_found_row_count: status_counts.not_found_row_count,
        invalid_name_row_count: status_counts.invalid_name_row_count,
    })
}

async fn rebuild_one_primary_name(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<PrimaryNamesCurrentRebuildSummary> {
    let target = PrimaryNameTupleKey {
        address: normalize_address(address),
        namespace: namespace.to_owned(),
        coin_type: coin_type.to_owned(),
    };
    let deleted_row_count =
        delete_primary_name_current(pool, &target.address, &target.namespace, &target.coin_type)
            .await?;

    let projected_row = match load_reverse_claim_tuple(pool, &target).await? {
        Some(tuple) => {
            let claim_observation = load_latest_name_claim_observation(pool, &target).await?;
            Some(primary_name_row(&tuple, claim_observation.as_ref())?)
        }
        None => None,
    };
    let upserted_row_count = match projected_row.as_ref() {
        Some(projection) => {
            upsert_primary_name_current_snapshots(pool, std::slice::from_ref(projection))
                .await?
                .len()
        }
        None => 0,
    };
    let projected_rows = projected_row
        .iter()
        .map(|projection| projection.row.clone())
        .collect::<Vec<_>>();
    let status_counts = count_statuses(&projected_rows);

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count: 1,
        upserted_row_count,
        deleted_row_count,
        success_row_count: status_counts.success_row_count,
        not_found_row_count: status_counts.not_found_row_count,
        invalid_name_row_count: status_counts.invalid_name_row_count,
    })
}

async fn load_reverse_claim_tuples(pool: &PgPool) -> Result<Vec<ReverseClaimTuple>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (
            LOWER(ne.after_state->>'address'),
            COALESCE(ne.after_state->>'namespace', ne.namespace),
            ne.after_state->>'coin_type'
        )
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            COALESCE(ne.after_state->'claim_provenance', '{{}}'::jsonb) AS claim_provenance
        FROM normalized_events ne
        WHERE ne.namespace = $1
          AND ne.event_kind = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
        ORDER BY
            LOWER(ne.after_state->>'address') ASC,
            COALESCE(ne.after_state->>'namespace', ne.namespace) ASC,
            ne.after_state->>'coin_type' ASC,
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
    ))
    .bind(ENS_NAMESPACE)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to load reverse-claim tuples from canonical ReverseChanged events")?;

    rows.into_iter().map(decode_reverse_claim_tuple).collect()
}

async fn load_reverse_claim_tuple(
    pool: &PgPool,
    target: &PrimaryNameTupleKey,
) -> Result<Option<ReverseClaimTuple>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            COALESCE(ne.after_state->'claim_provenance', '{{}}'::jsonb) AS claim_provenance
        FROM normalized_events ne
        WHERE ne.namespace = $1
          AND ne.event_kind = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND LOWER(ne.after_state->>'address') = $3
          AND ne.after_state->>'coin_type' = $4
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
        ORDER BY
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        LIMIT 1
        "#,
    ))
    .bind(&target.namespace)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .bind(&target.address)
    .bind(&target.coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse-claim tuple for address {} namespace {} coin_type {}",
            target.address, target.namespace, target.coin_type
        )
    })?;

    row.map(decode_reverse_claim_tuple).transpose()
}

async fn load_latest_name_claim_observations(
    pool: &PgPool,
) -> Result<BTreeMap<PrimaryNameTupleKey, NameClaimObservation>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (
            LOWER(ne.after_state->'primary_claim_source'->>'address'),
            COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace),
            ne.after_state->'primary_claim_source'->>'coin_type'
        )
            LOWER(ne.after_state->'primary_claim_source'->>'address') AS address,
            COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) AS namespace,
            ne.after_state->'primary_claim_source'->>'coin_type' AS coin_type,
            ne.after_state->>'raw_name' AS raw_name,
            ne.after_state->'primary_claim_source' AS primary_claim_source
        FROM normalized_events ne
        WHERE ne.namespace = $1
          AND ne.event_kind = 'RecordChanged'
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND ne.logical_name_id IS NULL
          AND ne.resource_id IS NULL
          AND ne.after_state->>'record_key' = 'name'
          AND ne.after_state ? 'primary_claim_source'
          AND ne.after_state->'primary_claim_source'->>'address' IS NOT NULL
          AND ne.after_state->'primary_claim_source'->>'address' <> ''
          AND ne.after_state->'primary_claim_source'->>'coin_type' IS NOT NULL
          AND ne.after_state->'primary_claim_source'->>'coin_type' <> ''
        ORDER BY
            LOWER(ne.after_state->'primary_claim_source'->>'address') ASC,
            COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) ASC,
            ne.after_state->'primary_claim_source'->>'coin_type' ASC,
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
    ))
    .bind(ENS_NAMESPACE)
    .fetch_all(pool)
    .await
    .context("failed to load reverse-linked name claim observations")?;

    rows.into_iter()
        .map(decode_name_claim_observation)
        .map(|result| result.map(|observation| (observation.key.clone(), observation)))
        .collect()
}

async fn load_latest_name_claim_observation(
    pool: &PgPool,
    target: &PrimaryNameTupleKey,
) -> Result<Option<NameClaimObservation>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            LOWER(ne.after_state->'primary_claim_source'->>'address') AS address,
            COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) AS namespace,
            ne.after_state->'primary_claim_source'->>'coin_type' AS coin_type,
            ne.after_state->>'raw_name' AS raw_name,
            ne.after_state->'primary_claim_source' AS primary_claim_source
        FROM normalized_events ne
        WHERE ne.namespace = $1
          AND ne.event_kind = 'RecordChanged'
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND ne.logical_name_id IS NULL
          AND ne.resource_id IS NULL
          AND ne.after_state->>'record_key' = 'name'
          AND LOWER(ne.after_state->'primary_claim_source'->>'address') = $2
          AND COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) = $3
          AND ne.after_state->'primary_claim_source'->>'coin_type' = $4
        ORDER BY
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        LIMIT 1
        "#,
    ))
    .bind(ENS_NAMESPACE)
    .bind(&target.address)
    .bind(&target.namespace)
    .bind(&target.coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse-linked name claim observation for address {} namespace {} coin_type {}",
            target.address, target.namespace, target.coin_type
        )
    })?;

    row.map(decode_name_claim_observation).transpose()
}

fn decode_reverse_claim_tuple(row: PgRow) -> Result<ReverseClaimTuple> {
    Ok(ReverseClaimTuple {
        key: decode_tuple_key(&row)?,
        claim_provenance: row
            .try_get("claim_provenance")
            .context("missing reverse-claim claim_provenance")?,
    })
}

fn decode_name_claim_observation(row: PgRow) -> Result<NameClaimObservation> {
    let primary_claim_source: Value = row
        .try_get("primary_claim_source")
        .context("missing primary_claim_source")?;
    primary_claim_source
        .as_object()
        .context("primary_claim_source must be a JSON object")?;

    Ok(NameClaimObservation {
        key: decode_tuple_key(&row)?,
        raw_name: row.try_get("raw_name").context("missing raw_name")?,
        primary_claim_source,
    })
}

fn decode_tuple_key(row: &PgRow) -> Result<PrimaryNameTupleKey> {
    let address = row
        .try_get::<String, _>("address")
        .context("missing primary-name address")?
        .to_ascii_lowercase();
    let namespace = row
        .try_get::<String, _>("namespace")
        .context("missing primary-name namespace")?;
    let coin_type = row
        .try_get::<String, _>("coin_type")
        .context("missing primary-name coin_type")?;

    if address.trim().is_empty() {
        bail!("primary-name tuple is missing address");
    }
    if namespace.trim().is_empty() {
        bail!("primary-name tuple is missing namespace");
    }
    if coin_type.trim().is_empty() {
        bail!("primary-name tuple is missing coin_type");
    }

    Ok(PrimaryNameTupleKey {
        address,
        namespace,
        coin_type,
    })
}

fn primary_name_row(
    tuple: &ReverseClaimTuple,
    claim_observation: Option<&NameClaimObservation>,
) -> Result<PrimaryNameCurrentSnapshot> {
    let (claim_status, raw_claim_name) =
        match claim_observation.and_then(|observation| observation.raw_name.as_deref()) {
            Some(raw_name) if claim_name_looks_normalizable(raw_name) => {
                (PrimaryNameClaimStatus::Success, None)
            }
            Some(raw_name) => (
                PrimaryNameClaimStatus::InvalidName,
                Some(raw_name.to_owned()),
            ),
            None => (PrimaryNameClaimStatus::NotFound, None),
        };

    let normalized_claim_name = claim_observation
        .and_then(|observation| observation.raw_name.as_deref())
        .filter(|_| claim_status == PrimaryNameClaimStatus::Success)
        .map(normalize_claim_name);

    Ok(PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: tuple.key.address.clone(),
            namespace: tuple.key.namespace.clone(),
            coin_type: tuple.key.coin_type.clone(),
            claim_status,
            raw_claim_name,
            claim_provenance: build_claim_provenance(tuple, claim_status, claim_observation)?,
        },
        normalized_claim_name,
    })
}

fn build_claim_provenance(
    tuple: &ReverseClaimTuple,
    claim_status: PrimaryNameClaimStatus,
    claim_observation: Option<&NameClaimObservation>,
) -> Result<Value> {
    let mut claim_provenance = tuple
        .claim_provenance
        .as_object()
        .cloned()
        .context("reverse-claim claim_provenance must be a JSON object")?;
    claim_provenance.insert(
        VERIFIED_PRIMARY_NAME_LOOKUP_KEY.to_owned(),
        verified_primary_name_lookup_hook(&tuple.key),
    );
    claim_provenance.insert(
        VERIFIED_PRIMARY_NAME_INVALIDATION_KEY.to_owned(),
        verified_primary_name_invalidation_hook(claim_status, claim_observation),
    );
    Ok(Value::Object(claim_provenance))
}

fn verified_primary_name_lookup_hook(key: &PrimaryNameTupleKey) -> Value {
    json!({
        "address": key.address,
        "namespace": key.namespace,
        "coin_type": key.coin_type,
    })
}

fn verified_primary_name_invalidation_hook(
    claim_status: PrimaryNameClaimStatus,
    claim_observation: Option<&NameClaimObservation>,
) -> Value {
    let mut invalidation =
        Map::from_iter([("claim_status".to_owned(), json!(claim_status.as_str()))]);
    if let Some(claim_observation) = claim_observation {
        invalidation.insert(
            "primary_claim_source".to_owned(),
            claim_observation.primary_claim_source.clone(),
        );
    }
    Value::Object(invalidation)
}

fn claim_name_looks_normalizable(raw_name: &str) -> bool {
    if raw_name.is_empty()
        || raw_name.trim() != raw_name
        || raw_name.len() > 255
        || !raw_name.is_ascii()
    {
        return false;
    }

    raw_name.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
    })
}

fn normalize_claim_name(raw_name: &str) -> String {
    raw_name.to_ascii_lowercase()
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StatusCounts {
    success_row_count: usize,
    not_found_row_count: usize,
    invalid_name_row_count: usize,
}

fn count_statuses(rows: &[PrimaryNameCurrentRow]) -> StatusCounts {
    let mut counts = StatusCounts::default();

    for row in rows {
        match row.claim_status {
            PrimaryNameClaimStatus::Success => counts.success_row_count += 1,
            PrimaryNameClaimStatus::NotFound => counts.not_found_row_count += 1,
            PrimaryNameClaimStatus::InvalidName => counts.invalid_name_row_count += 1,
            PrimaryNameClaimStatus::Unsupported => {}
        }
    }

    counts
}

fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use bigname_storage::{
        CanonicalityState, NormalizedEvent, default_database_url, load_primary_name_current,
        load_primary_name_current_snapshot, upsert_normalized_events,
        upsert_primary_name_current_rows,
    };

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for worker primary_names_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!("bn_wpn_{}_{}_{}", std::process::id(), unique, sequence);

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker primary_names_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker primary_names_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker primary_names_current tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn reverse_changed_event(
        event_identity: &str,
        address: &str,
        coin_type: &str,
        block_number: i64,
        log_index: i64,
        canonicality_state: CanonicalityState,
    ) -> NormalizedEvent {
        let normalized_address = address.to_ascii_lowercase();
        let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: ENS_NAMESPACE.to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
            source_family: "ens_v1_reverse_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xblock{block_number:064x}")),
            transaction_hash: Some(format!("0xtx{block_number:064x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "log_index": log_index,
            }),
            derivation_kind: "ens_v1_reverse_claim".to_owned(),
            canonicality_state,
            before_state: json!({}),
            after_state: json!({
                "source_event": "ReverseClaimed",
                "address": normalized_address,
                "coin_type": coin_type,
                "namespace": ENS_NAMESPACE,
                "reverse_namespace": ENS_NAMESPACE,
                "reverse_label": reverse_label,
                "reverse_name": format!("{reverse_label}.addr.reverse"),
                "reverse_node": format!("0x{block_number:064x}"),
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        }
    }

    fn reverse_linked_name_event(
        event_identity: &str,
        address: &str,
        coin_type: &str,
        raw_name: Option<&str>,
        block_number: i64,
        log_index: i64,
        canonicality_state: CanonicalityState,
    ) -> NormalizedEvent {
        let normalized_address = address.to_ascii_lowercase();
        let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
        let mut after_state = serde_json::Map::from_iter([
            ("record_key".to_owned(), json!("name")),
            ("record_family".to_owned(), json!("name")),
            ("selector_key".to_owned(), Value::Null),
            (
                "primary_claim_source".to_owned(),
                json!({
                    "address": normalized_address,
                    "namespace": ENS_NAMESPACE,
                    "coin_type": coin_type,
                    "reverse_name": format!("{reverse_label}.addr.reverse"),
                    "reverse_node": format!("0x{block_number:064x}"),
                    "claim_provenance": {
                        "source_family": "ens_v1_reverse_l1",
                        "contract_role": "reverse_registrar",
                        "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                        "emitting_address": "0x00000000000000000000000000000000000000ad",
                    },
                }),
            ),
        ]);
        if let Some(raw_name) = raw_name {
            after_state.insert("raw_name".to_owned(), json!(raw_name));
        }

        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: ENS_NAMESPACE.to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "RecordChanged".to_owned(),
            source_family: "ens_v1_unwrapped_authority".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xclaimblock{block_number:064x}")),
            transaction_hash: Some(format!("0xclaimtx{block_number:064x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "log_index": log_index,
            }),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state,
            before_state: json!({}),
            after_state: Value::Object(after_state),
        }
    }

    fn expected_claim_provenance(
        address: &str,
        coin_type: &str,
        reverse_block_number: i64,
        claim_status: PrimaryNameClaimStatus,
        primary_claim_block_number: Option<i64>,
    ) -> Value {
        let normalized_address = address.to_ascii_lowercase();
        let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
        let mut claim_provenance = Map::from_iter([
            ("source_family".to_owned(), json!("ens_v1_reverse_l1")),
            ("contract_role".to_owned(), json!("reverse_registrar")),
            (
                "contract_instance_id".to_owned(),
                json!(format!(
                    "00000000-0000-0000-0000-{reverse_block_number:012x}"
                )),
            ),
            (
                "emitting_address".to_owned(),
                json!("0x00000000000000000000000000000000000000ad"),
            ),
            (
                VERIFIED_PRIMARY_NAME_LOOKUP_KEY.to_owned(),
                json!({
                    "address": normalized_address.clone(),
                    "namespace": ENS_NAMESPACE,
                    "coin_type": coin_type,
                }),
            ),
        ]);
        let mut invalidation =
            Map::from_iter([("claim_status".to_owned(), json!(claim_status.as_str()))]);
        if let Some(primary_claim_block_number) = primary_claim_block_number {
            invalidation.insert(
                "primary_claim_source".to_owned(),
                json!({
                    "address": normalized_address.clone(),
                    "namespace": ENS_NAMESPACE,
                    "coin_type": coin_type,
                    "reverse_name": format!("{reverse_label}.addr.reverse"),
                    "reverse_node": format!("0x{primary_claim_block_number:064x}"),
                    "claim_provenance": {
                        "source_family": "ens_v1_reverse_l1",
                        "contract_role": "reverse_registrar",
                        "contract_instance_id": format!(
                            "00000000-0000-0000-0000-{primary_claim_block_number:012x}"
                        ),
                        "emitting_address": "0x00000000000000000000000000000000000000ad",
                    },
                }),
            );
        }
        claim_provenance.insert(
            VERIFIED_PRIMARY_NAME_INVALIDATION_KEY.to_owned(),
            Value::Object(invalidation),
        );

        Value::Object(claim_provenance)
    }

    #[tokio::test]
    async fn full_rebuild_projects_declared_claim_status_rows() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[
                reverse_changed_event(
                    "reverse-a-60-canonical",
                    "0x0000000000000000000000000000000000000aAa",
                    "60",
                    100,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_changed_event(
                    "reverse-a-60-finalized",
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    101,
                    0,
                    CanonicalityState::Finalized,
                ),
                reverse_changed_event(
                    "reverse-a-61-safe",
                    "0x0000000000000000000000000000000000000aaa",
                    "61",
                    102,
                    0,
                    CanonicalityState::Safe,
                ),
                reverse_changed_event(
                    "reverse-b-60-canonical",
                    "0x0000000000000000000000000000000000000bbb",
                    "60",
                    103,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_changed_event(
                    "reverse-orphaned",
                    "0x0000000000000000000000000000000000000ccc",
                    "60",
                    104,
                    0,
                    CanonicalityState::Orphaned,
                ),
                NormalizedEvent {
                    event_identity: "not-reverse".to_owned(),
                    event_kind: "ResolverChanged".to_owned(),
                    ..reverse_changed_event(
                        "not-reverse-base",
                        "0x0000000000000000000000000000000000000ddd",
                        "60",
                        105,
                        0,
                        CanonicalityState::Canonical,
                    )
                },
                reverse_linked_name_event(
                    "record-a-60-success",
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    Some("Alice.eth"),
                    201,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_linked_name_event(
                    "record-b-60-invalid",
                    "0x0000000000000000000000000000000000000bbb",
                    "60",
                    Some("alice..eth"),
                    202,
                    0,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let summary = rebuild_primary_names_current(database.pool(), None, None, None).await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 3,
                upserted_row_count: 3,
                deleted_row_count: 0,
                success_row_count: 1,
                not_found_row_count: 1,
                invalid_name_row_count: 1,
            }
        );

        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "60",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    101,
                    PrimaryNameClaimStatus::Success,
                    Some(201),
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "60",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(Some("alice.eth".to_owned()))
        );
        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "61",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "61".to_owned(),
                claim_status: PrimaryNameClaimStatus::NotFound,
                raw_claim_name: None,
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000aaa",
                    "61",
                    102,
                    PrimaryNameClaimStatus::NotFound,
                    None,
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "61",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(None)
        );
        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000bbb",
                "ens",
                "60",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::InvalidName,
                raw_claim_name: Some("alice..eth".to_owned()),
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000bbb",
                    "60",
                    103,
                    PrimaryNameClaimStatus::InvalidName,
                    Some(202),
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000bbb",
                "ens",
                "60",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(None)
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000ccc",
                "ens",
                "60",
            )
            .await?
            .is_none()
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000ddd",
                "ens",
                "60",
            )
            .await?
            .is_none()
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn targeted_rebuild_deletes_stale_tuple_when_no_reverse_event_exists() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_primary_name_current_rows(
            database.pool(),
            &[PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            }],
        )
        .await?;

        let summary = rebuild_primary_names_current(
            database.pool(),
            Some("0x0000000000000000000000000000000000000abc"),
            Some("ens"),
            Some("60"),
        )
        .await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 1,
                upserted_row_count: 0,
                deleted_row_count: 1,
                success_row_count: 0,
                not_found_row_count: 0,
                invalid_name_row_count: 0,
            }
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?
            .is_none()
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn targeted_rebuild_projects_invalid_name_from_latest_reverse_linked_observation()
    -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[
                reverse_changed_event(
                    "reverse-a-60",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    300,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_linked_name_event(
                    "record-a-60-old-success",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    Some("alice.eth"),
                    301,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_linked_name_event(
                    "record-a-60-new-invalid",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    Some("alice..eth"),
                    302,
                    0,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let summary = rebuild_primary_names_current(
            database.pool(),
            Some("0x0000000000000000000000000000000000000abc"),
            Some("ens"),
            Some("60"),
        )
        .await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
                success_row_count: 0,
                not_found_row_count: 0,
                invalid_name_row_count: 1,
            }
        );
        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::InvalidName,
                raw_claim_name: Some("alice..eth".to_owned()),
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    300,
                    PrimaryNameClaimStatus::InvalidName,
                    Some(302),
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(None)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn targeted_rebuild_rejects_non_ascii_claim_name_source() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[
                reverse_changed_event(
                    "reverse-a-60",
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    101,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_linked_name_event(
                    "record-a-60-non-ascii",
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    Some("Älice.eth"),
                    201,
                    0,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let summary = rebuild_primary_names_current(
            database.pool(),
            Some("0x0000000000000000000000000000000000000aaa"),
            Some("ens"),
            Some("60"),
        )
        .await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
                success_row_count: 0,
                not_found_row_count: 0,
                invalid_name_row_count: 1,
            }
        );

        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "60",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::InvalidName,
                raw_claim_name: Some("Älice.eth".to_owned()),
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    101,
                    PrimaryNameClaimStatus::InvalidName,
                    Some(201),
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "60",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(None)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn targeted_rebuild_projects_declared_claim_name_source_for_success_rows() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[
                reverse_changed_event(
                    "reverse-a-60",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    250,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_linked_name_event(
                    "record-a-60-success",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    Some("alice.eth"),
                    251,
                    0,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let summary = rebuild_primary_names_current(
            database.pool(),
            Some("0x0000000000000000000000000000000000000abc"),
            Some("ens"),
            Some("60"),
        )
        .await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
                success_row_count: 1,
                not_found_row_count: 0,
                invalid_name_row_count: 0,
            }
        );
        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    250,
                    PrimaryNameClaimStatus::Success,
                    Some(251),
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(Some("alice.eth".to_owned()))
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn targeted_rebuild_keeps_primary_claim_source_hook_for_not_found_rows() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[
                reverse_changed_event(
                    "reverse-a-60",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    400,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_linked_name_event(
                    "record-a-60-empty",
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    None,
                    401,
                    0,
                    CanonicalityState::Canonical,
                ),
            ],
        )
        .await?;

        let summary = rebuild_primary_names_current(
            database.pool(),
            Some("0x0000000000000000000000000000000000000abc"),
            Some("ens"),
            Some("60"),
        )
        .await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
                success_row_count: 0,
                not_found_row_count: 1,
                invalid_name_row_count: 0,
            }
        );
        assert_eq!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?,
            Some(PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::NotFound,
                raw_claim_name: None,
                claim_provenance: expected_claim_provenance(
                    "0x0000000000000000000000000000000000000abc",
                    "60",
                    400,
                    PrimaryNameClaimStatus::NotFound,
                    Some(401),
                ),
            })
        );
        assert_eq!(
            load_primary_name_current_snapshot(
                database.pool(),
                "0x0000000000000000000000000000000000000abc",
                "ens",
                "60",
            )
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
            Some(None)
        );

        database.cleanup().await
    }
}
