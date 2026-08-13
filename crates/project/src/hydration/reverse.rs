use bigname_domain::normalization::normalize_name;
use bigname_lookup::{
    ChainRpcUrls, EnsReverseNameMulticallBlock, EnsReverseNameMulticallRequest,
    EnsReverseNameMulticallResult, MULTICALL3_ADDRESS, execute_ens_reverse_name_multicall,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use super::{ETHEREUM, HYDRATION_KEY, hydration_provenance};
use crate::{Marker, ProjectError, Result};

const BATCH_SIZE: usize = 250;
const ROLLING_REFRESH_LIMIT: i64 = 250;

/// ENSv1 reverse resolvers that answer `name()` without emitting a record event, so the claim can
/// only be learned by calling them. The reference indexer records the same for this deployment
/// (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
/// (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6).
/// This list selects which reverse claims get hydrated and therefore which
/// `primary_names_current` rows exist, so it lives inside the interpreter content hash's watched
/// roots rather than in a serving crate.
const EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES: &[&str] =
    &["0xa2c122be93b0074270ebee7f6b7292c7deb45047"];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReverseIdentity {
    address: String,
    coin_type: String,
    namespace: String,
}

#[derive(Debug)]
struct ReverseCandidate {
    identity: ReverseIdentity,
    reverse_node: String,
    resolver_address: String,
    baseline: ReverseBaseline,
}

#[derive(Debug)]
struct StaleReverseCandidate {
    identity: ReverseIdentity,
    baseline: ReverseBaseline,
}

#[derive(Debug)]
struct ReverseBaseline {
    status: String,
    raw_name: Option<String>,
    is_normalized: bool,
    unsupported_reason: Option<String>,
}

pub(super) struct Candidates {
    active: Vec<ReverseCandidate>,
    stale: Vec<StaleReverseCandidate>,
}

impl Candidates {
    pub(super) fn active_len(&self) -> usize {
        self.active.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.active.is_empty() && self.stale.is_empty()
    }

    pub(super) fn needs_rpc(&self) -> bool {
        !self.active.is_empty()
    }

    pub(super) async fn execute(
        &self,
        rpc_urls: &ChainRpcUrls,
        head: &Marker,
    ) -> Vec<EnsReverseNameMulticallResult> {
        let block = EnsReverseNameMulticallBlock {
            block_number: head.number,
            block_hash: head.hash.clone(),
        };
        let mut results = Vec::with_capacity(self.active.len());
        for chunk in self.active.chunks(BATCH_SIZE) {
            let requests = chunk
                .iter()
                .map(|candidate| EnsReverseNameMulticallRequest {
                    resolver_address: candidate.resolver_address.clone(),
                    reverse_node: candidate.reverse_node.clone(),
                })
                .collect::<Vec<_>>();
            match execute_ens_reverse_name_multicall(
                rpc_urls,
                ETHEREUM,
                MULTICALL3_ADDRESS,
                &block,
                &requests,
            )
            .await
            {
                Ok(chunk_results) => results.extend(chunk_results),
                Err(error) => {
                    let message = format!("reverse-name hydration multicall failed: {error:#}");
                    results.extend(requests.iter().map(|_| {
                        EnsReverseNameMulticallResult::Failed {
                            message: message.clone(),
                        }
                    }));
                }
            }
        }
        results
    }

    pub(super) async fn publish(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        head: &Marker,
        results: Vec<EnsReverseNameMulticallResult>,
    ) -> Result<(usize, usize)> {
        if results.len() != self.active.len() {
            return Err(ProjectError::data_integrity(format!(
                "reverse hydration produced {} outcomes for {} candidates",
                results.len(),
                self.active.len()
            )));
        }
        let attempt_ordinal = if self.active.is_empty() {
            None
        } else {
            Some(
                sqlx::query_scalar::<_, i64>(
                    "SELECT nextval('reverse_hydration_attempt_ordinal_seq')",
                )
                .fetch_one(&mut **transaction)
                .await
                .map_err(|error| {
                    ProjectError::database(
                        "failed to allocate reverse hydration attempt order",
                        error,
                    )
                })?,
            )
        };
        let mut failures = 0usize;
        for (candidate, result) in self.active.iter().zip(results) {
            failures += usize::from(
                update_primary(
                    transaction,
                    candidate,
                    head,
                    attempt_ordinal.expect("active candidates have an attempt order"),
                    classify_result(result),
                )
                .await?,
            );
        }
        for candidate in &self.stale {
            restore_primary_baseline(transaction, &candidate.identity, &candidate.baseline, None)
                .await?;
        }
        Ok((self.active.len() + self.stale.len(), failures))
    }
}

pub(super) async fn load_candidates(pool: &PgPool, head: &Marker) -> Result<Candidates> {
    let active = load_active_candidates(pool, head).await?;
    let stale = load_hydrated_candidates(pool, head).await?;
    Ok(Candidates { active, stale })
}

async fn load_active_candidates(pool: &PgPool, head: &Marker) -> Result<Vec<ReverseCandidate>> {
    let resolvers = EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES;
    type Row = (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        bool,
        Option<String>,
        Value,
    );
    let rows = sqlx::query_as::<_, Row>(
        r#"
        WITH eligible AS (
            SELECT current.*,
                   lower(current.claim_provenance ->> 'reverse_node') AS reverse_node,
                   lower(current.claim_provenance ->> 'resolver_address') AS resolver_address,
                   COALESCE(current.claim_provenance ->> 'target_block_number' = $3::text
                       AND current.claim_provenance ->> 'target_block_hash' = $4, false)
                       AS delta_scoped,
                   COALESCE(
                       current.reverse_hydration_attempted_block_number = $3
                       AND current.reverse_hydration_attempted_block_hash = $4,
                       false
                   ) AS attempted_at_head
            FROM primary_names_current current
            WHERE current.coin_type = '60'
              AND current.namespace = 'ens'
              AND current.claim_provenance ->> 'chain_id' = $2
              AND current.claim_provenance ->> 'reverse_node' IS NOT NULL
              AND lower(current.claim_provenance ->> 'resolver_address') = ANY($1)
        ), delta_scoped AS (
            SELECT *
            FROM eligible
            WHERE delta_scoped AND NOT attempted_at_head
        ), rolling_priority AS (
            SELECT reverse_hydration_attempt_ordinal AS attempt_ordinal
            FROM eligible
            WHERE NOT delta_scoped AND NOT attempted_at_head
            ORDER BY
                reverse_hydration_attempt_ordinal NULLS FIRST,
                NULLIF(
                    claim_provenance -> $5 ->> 'block_number',
                    ''
                )::bigint NULLS FIRST,
                address, coin_type, namespace
            LIMIT 1
        ), rolling AS (
            SELECT eligible.*
            FROM eligible
            JOIN rolling_priority priority
              ON priority.attempt_ordinal IS NOT DISTINCT FROM
                 eligible.reverse_hydration_attempt_ordinal
            WHERE NOT eligible.delta_scoped AND NOT eligible.attempted_at_head
            ORDER BY
                NULLIF(
                    eligible.claim_provenance -> $5 ->> 'block_number',
                    ''
                )::bigint NULLS FIRST,
                eligible.address, eligible.coin_type, eligible.namespace
            LIMIT $6
        ), selected AS (
            SELECT * FROM delta_scoped
            UNION ALL
            SELECT * FROM rolling
        )
        SELECT current.address, current.coin_type, current.namespace, current.reverse_node,
               current.resolver_address, current.claim_status, current.raw_claim_name,
               current.claim_name_is_normalized, current.unsupported_reason,
               current.claim_provenance
        FROM selected current
        ORDER BY current.address, current.coin_type, current.namespace
        "#,
    )
    .bind(resolvers)
    .bind(ETHEREUM)
    .bind(head.number)
    .bind(&head.hash)
    .bind(HYDRATION_KEY)
    .bind(ROLLING_REFRESH_LIMIT)
    .fetch_all(pool)
    .await
    .map_err(|error| ProjectError::database("failed to load reverse candidates", error))?;
    rows.into_iter()
        .map(
            |(
                address,
                coin_type,
                namespace,
                reverse_node,
                resolver_address,
                status,
                raw_name,
                is_normalized,
                unsupported_reason,
                provenance,
            )| {
                Ok(ReverseCandidate {
                    identity: ReverseIdentity {
                        address,
                        coin_type,
                        namespace,
                    },
                    reverse_node,
                    resolver_address,
                    baseline: reverse_baseline(
                        status,
                        raw_name,
                        is_normalized,
                        unsupported_reason,
                        &provenance,
                    )?,
                })
            },
        )
        .collect()
}

async fn load_hydrated_candidates(
    pool: &PgPool,
    head: &Marker,
) -> Result<Vec<StaleReverseCandidate>> {
    let resolvers = EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES;
    type Row = (
        String,
        String,
        String,
        String,
        Option<String>,
        bool,
        Option<String>,
        Value,
    );
    let rows = sqlx::query_as::<_, Row>(
        r#"
        WITH stale AS (
            SELECT current.*,
                   COALESCE(current.claim_provenance ->> 'target_block_number' = $3::text
                       AND current.claim_provenance ->> 'target_block_hash' = $4, false)
                       AS delta_scoped
            FROM primary_names_current current
            WHERE (current.claim_provenance -> $1) ->> 'chain_id' = $2
              AND NOT (
                  current.coin_type = '60'
                  AND current.namespace = 'ens'
                  AND current.claim_provenance ->> 'reverse_node' IS NOT NULL
                  AND COALESCE(lower(current.claim_provenance ->> 'resolver_address') = ANY($5),
                               false)
              )
        ), delta_scoped AS (
            SELECT * FROM stale WHERE delta_scoped
        ), rolling AS (
            SELECT *
            FROM stale
            WHERE NOT delta_scoped
            ORDER BY
                NULLIF(claim_provenance -> $1 ->> 'block_number', '')::bigint NULLS FIRST,
                address, coin_type, namespace
            LIMIT $6
        )
        SELECT address, coin_type, namespace, claim_status, raw_claim_name,
               claim_name_is_normalized, unsupported_reason, claim_provenance
        FROM delta_scoped
        UNION ALL
        SELECT address, coin_type, namespace, claim_status, raw_claim_name,
               claim_name_is_normalized, unsupported_reason, claim_provenance
        FROM rolling
        ORDER BY address, coin_type, namespace
        "#,
    )
    .bind(HYDRATION_KEY)
    .bind(ETHEREUM)
    .bind(head.number)
    .bind(&head.hash)
    .bind(resolvers)
    .bind(ROLLING_REFRESH_LIMIT)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        ProjectError::database("failed to load prior reverse hydration candidates", error)
    })?;
    rows.into_iter()
        .map(
            |(
                address,
                coin_type,
                namespace,
                status,
                raw_name,
                is_normalized,
                unsupported_reason,
                provenance,
            )| {
                Ok(StaleReverseCandidate {
                    identity: ReverseIdentity {
                        address,
                        coin_type,
                        namespace,
                    },
                    baseline: reverse_baseline(
                        status,
                        raw_name,
                        is_normalized,
                        unsupported_reason,
                        &provenance,
                    )?,
                })
            },
        )
        .collect()
}

enum Claim {
    Success(String, bool),
    Invalid(String),
    NotFound,
    Failed,
}

fn classify_result(result: EnsReverseNameMulticallResult) -> Claim {
    match result {
        EnsReverseNameMulticallResult::Failed { .. } => Claim::Failed,
        EnsReverseNameMulticallResult::NotFound => Claim::NotFound,
        EnsReverseNameMulticallResult::Success { value } if value.trim().is_empty() => {
            Claim::NotFound
        }
        EnsReverseNameMulticallResult::Success { value } => match normalize_name(&value) {
            Ok(normalized) => Claim::Success(
                value.clone(),
                normalized.normalized_name.as_bytes() == value.as_bytes(),
            ),
            Err(_) => Claim::Invalid(value),
        },
    }
}

async fn update_primary(
    transaction: &mut Transaction<'_, Postgres>,
    candidate: &ReverseCandidate,
    head: &Marker,
    attempt_ordinal: i64,
    claim: Claim,
) -> Result<bool> {
    if matches!(claim, Claim::Failed) {
        restore_primary_baseline(
            transaction,
            &candidate.identity,
            &candidate.baseline,
            Some((head, attempt_ordinal)),
        )
        .await?;
        return Ok(true);
    }
    let (status, raw_name, is_normalized) = match claim {
        Claim::Success(name, normalized) => ("success", Some(name), normalized),
        Claim::Invalid(name) => ("invalid_name", Some(name), false),
        Claim::NotFound => ("not_found", None, false),
        Claim::Failed => unreachable!("handled above"),
    };
    let mut provenance = hydration_provenance(
        head,
        Some(&candidate.resolver_address),
        Some(&candidate.reverse_node),
    );
    provenance
        .as_object_mut()
        .expect("hydration provenance is an object")
        .insert("baseline".to_owned(), baseline_json(&candidate.baseline));
    let result = sqlx::query(
        "UPDATE primary_names_current
         SET claim_status = $4, raw_claim_name = $5,
             claim_name_is_normalized = $6, unsupported_reason = NULL,
             claim_provenance = (claim_provenance - $7) || jsonb_build_object($7, $8),
             reverse_hydration_attempted_block_number = $9,
             reverse_hydration_attempted_block_hash = $10,
             reverse_hydration_attempt_ordinal = $11
         WHERE address = $1 AND coin_type = $2 AND namespace = $3",
    )
    .bind(&candidate.identity.address)
    .bind(&candidate.identity.coin_type)
    .bind(&candidate.identity.namespace)
    .bind(status)
    .bind(raw_name)
    .bind(is_normalized)
    .bind(HYDRATION_KEY)
    .bind(provenance)
    .bind(head.number)
    .bind(&head.hash)
    .bind(attempt_ordinal)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to publish hydrated reverse name", error))?;
    require_updated_row(result.rows_affected(), "publication")?;
    Ok(false)
}

async fn restore_primary_baseline(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ReverseIdentity,
    baseline: &ReverseBaseline,
    attempt: Option<(&Marker, i64)>,
) -> Result<()> {
    let attempted_block_number = attempt.map(|(head, _)| head.number);
    let attempted_block_hash = attempt.map(|(head, _)| &head.hash);
    let attempt_ordinal = attempt.map(|(_, ordinal)| ordinal);
    let result = sqlx::query(
        "UPDATE primary_names_current
         SET claim_status = $4, raw_claim_name = $5,
             claim_name_is_normalized = $6, unsupported_reason = $7,
             claim_provenance = claim_provenance - $8,
             reverse_hydration_attempted_block_number = $9,
             reverse_hydration_attempted_block_hash = $10,
             reverse_hydration_attempt_ordinal = $11
         WHERE address = $1 AND coin_type = $2 AND namespace = $3",
    )
    .bind(&identity.address)
    .bind(&identity.coin_type)
    .bind(&identity.namespace)
    .bind(&baseline.status)
    .bind(&baseline.raw_name)
    .bind(baseline.is_normalized)
    .bind(&baseline.unsupported_reason)
    .bind(HYDRATION_KEY)
    .bind(attempted_block_number)
    .bind(attempted_block_hash)
    .bind(attempt_ordinal)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to retract stale reverse-name hydration", error)
    })?;
    require_updated_row(result.rows_affected(), "retraction")
}

fn require_updated_row(rows: u64, operation: &str) -> Result<()> {
    if rows != 1 {
        return Err(ProjectError::data_integrity(format!(
            "primary-name hydration candidate disappeared before {operation}"
        )));
    }
    Ok(())
}

fn reverse_baseline(
    status: String,
    raw_name: Option<String>,
    is_normalized: bool,
    unsupported_reason: Option<String>,
    provenance: &Value,
) -> Result<ReverseBaseline> {
    let Some(baseline) = provenance
        .get(HYDRATION_KEY)
        .and_then(|value| value.get("baseline"))
    else {
        return Ok(ReverseBaseline {
            status,
            raw_name,
            is_normalized,
            unsupported_reason,
        });
    };
    Ok(ReverseBaseline {
        status: baseline
            .get("claim_status")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProjectError::data_integrity("reverse hydration baseline has no status")
            })?
            .to_owned(),
        raw_name: optional_string(baseline, "raw_claim_name")?,
        is_normalized: baseline
            .get("claim_name_is_normalized")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                ProjectError::data_integrity("reverse hydration baseline has no normalization flag")
            })?,
        unsupported_reason: optional_string(baseline, "unsupported_reason")?,
    })
}

fn optional_string(value: &Value, key: &str) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ProjectError::data_integrity(format!(
            "reverse hydration baseline field {key} is not a string or null"
        ))),
    }
}

fn baseline_json(baseline: &ReverseBaseline) -> Value {
    json!({
        "claim_status": baseline.status,
        "raw_claim_name": baseline.raw_name,
        "claim_name_is_normalized": baseline.is_normalized,
        "unsupported_reason": baseline.unsupported_reason,
    })
}
