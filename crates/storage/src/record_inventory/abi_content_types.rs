//! ABI content types observed in the record writes a record inventory already selects.
//!
//! Project keeps every selected write's normalized event id in `provenance.record_event_ids`,
//! ABI writes included, even though ABI records are not inventory selectors or entries. This
//! read dereferences those ids instead of re-deriving resolver history, so the resolver
//! selection, record-version reset, record-link, mirror, and canonicality rules stay Project's.
//! The public meaning is documented under `GET /v1/names/{name}/records` in
//! `docs/api-v2-routes.md`.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::U256;
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::phase_projection_reads::DEFAULT_RESOLVER_CURRENT_READ_FILTER;

/// Why an inventory cannot list ABI content types; `as_str` is the public reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbiContentTypesUnavailable {
    /// The name has no record inventory on its serving resource.
    InventoryNotAvailable,
    /// The inventory row is `unsupported`, so it cannot speak for the resolver's storage.
    InventoryNotAuthoritative,
    /// The selected resolver storage has no admitted ABI-change event.
    ObservationsNotSupported,
    /// A selected write is no longer retained as canonical, activated evidence.
    ObservationsStale,
    /// A selected ABI write names a zero or multi-bit content type.
    ContentTypeNotSingleBit,
}

impl AbiContentTypesUnavailable {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InventoryNotAvailable => "inventory_not_available",
            Self::InventoryNotAuthoritative => "inventory_not_authoritative",
            Self::ObservationsNotSupported => "abi_observations_not_supported",
            Self::ObservationsStale => "abi_observations_stale",
            Self::ContentTypeNotSingleBit => "abi_content_type_not_single_bit",
        }
    }
}

/// The observed content types of one inventory, or why they cannot be listed. An empty list is
/// an eligible observation path with no ABI write selected, never verified absence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AbiContentTypes {
    /// Canonical decimal strings of single-bit `uint256` content types, ascending, deduplicated.
    Observed(Vec<String>),
    Unavailable(AbiContentTypesUnavailable),
}

/// One serving record inventory row. `authoritative` is the caller's serving decision for the row
/// (supported coverage); the row's supported flag alone never implies an ABI observation path.
#[derive(Clone, Copy, Debug)]
pub struct AbiContentTypesInput<'a> {
    pub authoritative: bool,
    pub provenance: &'a Value,
    pub chain_positions: &'a Value,
}

/// Whether bigname admits an ABI-change event for resolver storage of this classification.
/// ENSv1-family resolvers emit `ABIChanged` and ENSv2 record-ID resolvers emit `ABIUpdated`
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/IABIResolver.sol:L5 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/ABIResolver.sol:L25 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/setters/IABISetter.sol:L13 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L160 @ ens_v2@a971bd64).
/// A mirror stores no records of its own
/// (upstream: .refs/ens_v2/contracts/src/resolver/AbstractMirrorResolver.sol:L15-L21 @ ens_v2@a971bd64).
/// PublicResolverV2 and the Basenames L2Resolver both inherit ENS's `ABIResolver` and so emit
/// `ABIChanged` on chain
/// (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L23-L26 @ ens_v2@a971bd64)
/// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29-L31 @ basenames@1809bbc),
/// but bigname does not admit it for them: the direct PublicResolverV2 profile admits only its
/// address, text, contenthash, and version events
/// (`crates/adapters/src/schema_v2/protocol/v2_resolver.rs`), and the Basenames resolver
/// manifests declare no ABI event. The manifest-agreement test in `tests.rs` pins this table.
pub(crate) fn admits_abi_observations(source_family: &str, role: Option<&str>) -> bool {
    match source_family {
        "ens_v1_resolver_l1" => true,
        "ens_v2_resolver_l1" => {
            !matches!(role, Some("public_resolver_v2" | "ensv1_mirror_resolver"))
        }
        _ => false,
    }
}

/// Decode an ABI write's content type from its selector key. Only a canonical decimal `uint256`
/// with exactly one bit set is a content type a standard setter can write.
pub(crate) fn single_bit_content_type(selector_key: &Value) -> Option<U256> {
    let text = selector_key.as_str()?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let value = U256::from_str_radix(text, 10).ok()?;
    (value.to_string() == text && value.is_power_of_two()).then_some(value)
}

/// The per-row plan: what must be looked up before the row's answer is known.
enum Plan {
    Done(AbiContentTypesUnavailable),
    Classify {
        chain_id: String,
        resolver_address: Option<String>,
        mirrored_family: Option<String>,
        event_ids: Vec<i64>,
        link_event_ids: BTreeSet<i64>,
        max_block_number: Option<i64>,
    },
}

fn plan(input: &AbiContentTypesInput<'_>) -> Plan {
    if !input.authoritative {
        return Plan::Done(AbiContentTypesUnavailable::InventoryNotAuthoritative);
    }
    let provenance = input.provenance;
    let Some(chain_id) = provenance.get("chain_id").and_then(Value::as_str) else {
        return Plan::Done(AbiContentTypesUnavailable::ObservationsNotSupported);
    };
    let mirrored_family = provenance
        .pointer("/mirror/mirrored_source_family")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let resolver_address = provenance
        .get("resolver_address")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    if mirrored_family.is_none() && resolver_address.is_none() {
        return Plan::Done(AbiContentTypesUnavailable::ObservationsNotSupported);
    }
    let (Some(event_ids), Some(link_event_ids)) = (
        event_ids(provenance.get("record_event_ids")),
        event_ids(
            provenance
                .get("record_link_event_ids")
                .or(Some(&Value::Null)),
        ),
    ) else {
        return Plan::Done(AbiContentTypesUnavailable::ObservationsStale);
    };
    Plan::Classify {
        chain_id: chain_id.to_owned(),
        resolver_address,
        mirrored_family,
        event_ids,
        link_event_ids: link_event_ids.into_iter().collect(),
        max_block_number: input
            .chain_positions
            .get("target_block_number")
            .and_then(Value::as_i64),
    }
}

/// A missing section is an empty list; anything other than an array of integer ids is unusable.
fn event_ids(value: Option<&Value>) -> Option<Vec<i64>> {
    match value? {
        Value::Null => Some(Vec::new()),
        Value::Array(items) => items.iter().map(Value::as_i64).collect(),
        _ => None,
    }
}

/// Resolve the ABI content types of many inventory rows with a fixed number of statements: one
/// resolver-classification read and one read of the referenced events, whatever the row count.
pub async fn load_record_inventory_abi_content_types(
    pool: &PgPool,
    inputs: &[AbiContentTypesInput<'_>],
) -> Result<Vec<AbiContentTypes>> {
    let plans = inputs.iter().map(plan).collect::<Vec<_>>();
    let classifications = load_classifications(pool, &plans).await?;

    let mut answers = Vec::with_capacity(plans.len());
    let mut pending = Vec::new();
    for plan in plans {
        let answer = match plan {
            Plan::Done(reason) => Some(AbiContentTypes::Unavailable(reason)),
            Plan::Classify {
                chain_id,
                resolver_address,
                mirrored_family,
                event_ids,
                link_event_ids,
                max_block_number,
            } => {
                let admitted = match mirrored_family {
                    // A mirror reads the ENSv1 resolver its registry walk selects; Project already
                    // attributed that resolver's writes to this row.
                    Some(family) => admits_abi_observations(&family, None),
                    None => resolver_address
                        .and_then(|address| classifications.get(&(chain_id.clone(), address)))
                        .is_some_and(|(family, role)| {
                            admits_abi_observations(family, role.as_deref())
                        }),
                };
                if admitted {
                    pending.push((
                        answers.len(),
                        EventScope {
                            chain_id,
                            max_block_number,
                        },
                        event_ids,
                        link_event_ids,
                    ));
                    None
                } else {
                    Some(AbiContentTypes::Unavailable(
                        AbiContentTypesUnavailable::ObservationsNotSupported,
                    ))
                }
            }
        };
        answers.push(answer);
    }

    let requested = pending
        .iter()
        .flat_map(|(_, scope, ids, _)| ids.iter().map(move |id| (*id, scope.clone())))
        .collect::<BTreeSet<_>>();
    let evidence = load_evidence(pool, &requested).await?;
    for (index, scope, ids, link_ids) in pending {
        answers[index] = Some(content_types_from_evidence(
            &ids, &link_ids, &scope, &evidence,
        ));
    }
    Ok(answers
        .into_iter()
        .map(|answer| answer.expect("every inventory row has an ABI answer"))
        .collect())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EventScope {
    chain_id: String,
    max_block_number: Option<i64>,
}

/// What the referenced event is, when it is retained as canonical, activated evidence in scope.
#[derive(Clone, Debug)]
enum Evidence {
    Abi(Value),
    Other,
}

fn content_types_from_evidence(
    ids: &[i64],
    link_ids: &BTreeSet<i64>,
    scope: &EventScope,
    evidence: &BTreeMap<(i64, EventScope), Evidence>,
) -> AbiContentTypes {
    let mut content_types = BTreeSet::new();
    let mut not_single_bit = false;
    for id in ids {
        match evidence.get(&(*id, scope.clone())) {
            None => {
                return AbiContentTypes::Unavailable(AbiContentTypesUnavailable::ObservationsStale);
            }
            Some(Evidence::Abi(_)) if link_ids.contains(id) => {}
            Some(Evidence::Abi(selector_key)) => match single_bit_content_type(selector_key) {
                Some(content_type) => {
                    content_types.insert(content_type);
                }
                None => not_single_bit = true,
            },
            Some(Evidence::Other) => {}
        }
    }
    if not_single_bit {
        return AbiContentTypes::Unavailable(AbiContentTypesUnavailable::ContentTypeNotSingleBit);
    }
    AbiContentTypes::Observed(content_types.iter().map(U256::to_string).collect())
}

async fn load_classifications(
    pool: &PgPool,
    plans: &[Plan],
) -> Result<BTreeMap<(String, String), (String, Option<String>)>> {
    let keys = plans
        .iter()
        .filter_map(|plan| match plan {
            Plan::Classify {
                chain_id,
                resolver_address: Some(address),
                mirrored_family: None,
                ..
            } => Some((chain_id.clone(), address.clone())),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let (chain_ids, addresses): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
    let rows = sqlx::query(&format!(
        r#"
        SELECT resolver.chain_id, resolver.resolver_address,
               resolver.declared_summary #>> '{{classification,source_family}}' AS source_family,
               resolver.declared_summary #>> '{{classification,role}}' AS role
        FROM unnest($1::TEXT[], $2::TEXT[]) AS requested(chain_id, resolver_address)
        JOIN bigname_phase.resolver_current resolver
          ON resolver.chain_id = requested.chain_id
         AND resolver.resolver_address = requested.resolver_address
        WHERE TRUE
          {DEFAULT_RESOLVER_CURRENT_READ_FILTER}
        "#
    ))
    .bind(chain_ids)
    .bind(addresses)
    .fetch_all(pool)
    .await
    .context("failed to load resolver classifications for ABI content types")?;
    let mut classifications = BTreeMap::new();
    for row in rows {
        let Some(source_family) = row.try_get::<Option<String>, _>("source_family")? else {
            continue;
        };
        classifications.insert(
            (row.try_get("chain_id")?, row.try_get("resolver_address")?),
            (source_family, row.try_get("role")?),
        );
    }
    Ok(classifications)
}

/// The statement behind [`load_evidence`]: each requested id looks up its own event by primary
/// key and matches it when it is retained, canonical, and activated on the scope's chain at or
/// below the scope's block. `OFFSET 0` keeps the scope checks outside the per-id subquery, so the
/// only key the lookup can use is the event id and the read never scans events it was not asked
/// for. Only unmatched ids and ABI writes come back.
const ABI_EVIDENCE_QUERY: &str = r#"
    WITH requested AS (
        SELECT DISTINCT *
        FROM unnest($1::BIGINT[], $2::TEXT[], $3::BIGINT[])
            AS requested(normalized_event_id, chain_id, max_block_number)
    )
    SELECT requested.normalized_event_id, requested.chain_id, requested.max_block_number,
           event.normalized_event_id IS NOT NULL AS retained,
           event.after_state -> 'selector_key' AS selector_key
    FROM requested
    LEFT JOIN LATERAL (
        SELECT candidate.normalized_event_id, candidate.chain_id, candidate.block_number,
               candidate.event_kind, candidate.after_state
        FROM bigname_phase.normalized_events candidate
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = candidate.chain_id
         AND lineage.block_hash = candidate.block_hash
         AND lineage.block_number = candidate.block_number
        WHERE candidate.normalized_event_id = requested.normalized_event_id
          AND candidate.consumer_visibility = 'activated'
          AND candidate.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
        OFFSET 0
    ) event
      ON event.chain_id = requested.chain_id
     AND (requested.max_block_number IS NULL
          OR event.block_number <= requested.max_block_number)
    WHERE event.normalized_event_id IS NULL
       OR (event.event_kind = 'RecordChanged'
           AND event.after_state ->> 'record_family' = 'abi')
"#;

async fn load_evidence(
    pool: &PgPool,
    requested: &BTreeSet<(i64, EventScope)>,
) -> Result<BTreeMap<(i64, EventScope), Evidence>> {
    if requested.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut ids = Vec::with_capacity(requested.len());
    let mut chain_ids = Vec::with_capacity(requested.len());
    let mut max_blocks = Vec::with_capacity(requested.len());
    for (id, scope) in requested {
        ids.push(*id);
        chain_ids.push(scope.chain_id.clone());
        max_blocks.push(scope.max_block_number);
    }
    let rows = sqlx::query(ABI_EVIDENCE_QUERY)
        .bind(ids)
        .bind(chain_ids)
        .bind(max_blocks)
        .fetch_all(pool)
        .await
        .context("failed to load selected ABI writes")?;
    // Every requested id is retained non-ABI evidence unless the read reports otherwise.
    let mut evidence = requested
        .iter()
        .map(|key| (key.clone(), Evidence::Other))
        .collect::<BTreeMap<_, _>>();
    for row in rows {
        let key = (
            row.try_get::<i64, _>("normalized_event_id")?,
            EventScope {
                chain_id: row.try_get("chain_id")?,
                max_block_number: row.try_get("max_block_number")?,
            },
        );
        if row.try_get::<bool, _>("retained")? {
            let selector_key = row
                .try_get::<Option<Value>, _>("selector_key")?
                .unwrap_or(Value::Null);
            evidence.insert(key, Evidence::Abi(selector_key));
        } else {
            evidence.remove(&key);
        }
    }
    Ok(evidence)
}

/// The plan of the evidence read for `ids` on `chain_id`, with the planner's default settings, so
/// a caller can assert the read is keyed lookups by primary key.
#[cfg(any(test, feature = "test-support"))]
pub async fn explain_record_inventory_abi_evidence_for_test(
    pool: &PgPool,
    ids: &[i64],
    chain_id: &str,
) -> Result<String> {
    let mut transaction = pool.begin().await?;
    let plan =
        sqlx::query_scalar::<_, String>(&format!("EXPLAIN (COSTS OFF) {ABI_EVIDENCE_QUERY}"))
            .bind(ids)
            .bind(vec![chain_id.to_owned(); ids.len()])
            .bind(vec![None::<i64>; ids.len()])
            .fetch_all(&mut *transaction)
            .await?
            .join("\n");
    transaction.rollback().await?;
    Ok(plan)
}

#[cfg(test)]
#[path = "abi_content_types/tests.rs"]
mod tests;
