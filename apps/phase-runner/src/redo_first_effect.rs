//! What a manifest change can alter, and the lowest block at which the stored rows can show it.
//! The change is the difference between the manifest payloads the families were last published
//! with and the ones a redo would use now, compared per declaration. The first effect is the
//! lowest canonical block at or below the recorded head where a log or a discovery edge involves
//! what changed, or where a changed declaration starts. No arm reads interpreted event payloads:
//! no index covers their text, and on mainnet that one pass would hold the locked redo begin for
//! hours.
use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::error::{RunnerError, RunnerResult};

/// The parts of a manifest change the first-effect scan reads.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct ManifestDiff {
    /// Lower-cased addresses named by a changed declaration on either side.
    pub(crate) addresses: BTreeSet<String>,
    /// The start blocks of every changed declaration and discovery rule, on both sides.
    pub(crate) starts: BTreeSet<i64>,
    /// Compiled watch entries new on the current side, or starting lower than before:
    /// (emitter address or `None` for any emitter, topic0, start).
    pub(crate) watches: BTreeSet<(Option<String>, String, i64)>,
}

/// One side of a manifest whose stored event changed.
pub(crate) struct Side<'a> {
    pub(crate) rollout_status: Option<&'a str>,
    pub(crate) payload: &'a Value,
}

impl ManifestDiff {
    /// Add what differs between `baseline` and `current` of one manifest. A changed rollout
    /// status changes every declaration on both sides.
    pub(crate) fn add(&mut self, baseline: Option<&Side<'_>>, current: Option<&Side<'_>>) {
        let status_changed =
            baseline.map(|side| side.rollout_status) != current.map(|side| side.rollout_status);
        let before = baseline.map(declarations).unwrap_or_default();
        let after = current.map(declarations).unwrap_or_default();
        for key in before.keys().chain(after.keys()) {
            let (old, new) = (before.get(key), after.get(key));
            if status_changed || old != new {
                for declaration in old.into_iter().chain(new) {
                    self.starts.extend(declaration.start);
                    self.addresses.extend(declaration.addresses.iter().cloned());
                }
            }
        }
        let prior = baseline.map(watches).unwrap_or_default();
        for ((emitter, topic0), start) in current.map(watches).unwrap_or_default() {
            if prior
                .get(&(emitter.clone(), topic0.clone()))
                .is_none_or(|prior| start < *prior)
            {
                self.watches.insert((address_of(&emitter), topic0, start));
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Declaration<'a> {
    value: &'a Value,
    start: Option<i64>,
    addresses: Vec<String>,
}

/// Every declaration of a payload by a key that names it: roots, contracts, resolver
/// implementations and discovery rules. A missing start reads as block zero, as manifest
/// compilation reads it.
fn declarations<'a>(side: &Side<'a>) -> BTreeMap<String, Declaration<'a>> {
    let payload = side.payload;
    let mut declared = BTreeMap::new();
    let mut role_starts: BTreeMap<&str, i64> = BTreeMap::new();
    for (kind, role_field, list) in [
        ("root", "name", "roots"),
        ("contract", "role", "contracts"),
        ("implementation", "role", "resolver_implementations"),
    ] {
        for value in payload[list].as_array().into_iter().flatten() {
            let role = value[role_field].as_str().unwrap_or_default();
            let start = start_block(&value["start_block"]);
            if kind != "implementation" {
                role_starts
                    .entry(role)
                    .and_modify(|lowest| *lowest = (*lowest).min(start))
                    .or_insert(start);
            }
            let addresses: Vec<String> = [&value["address"], &value["implementation"]]
                .into_iter()
                .filter_map(Value::as_str)
                .map(str::to_ascii_lowercase)
                .collect();
            let key = format!(
                "{kind}:{role}:{}",
                addresses.first().cloned().unwrap_or_default()
            );
            declared.insert(
                key,
                Declaration {
                    value,
                    start: Some(start),
                    addresses,
                },
            );
        }
    }
    for value in payload["discovery_rules"].as_array().into_iter().flatten() {
        let field = |name: &str| value[name].as_str().unwrap_or_default();
        let from_role = field("from_role");
        declared.insert(
            format!(
                "rule:{}:{from_role}:{}",
                field("edge_kind"),
                field("admission")
            ),
            Declaration {
                value,
                start: Some(role_starts.get(from_role).copied().unwrap_or(0)),
                addresses: Vec::new(),
            },
        );
    }
    declared
}

/// The compiled watch plan of an active payload, by emitter and topic0.
fn watches(side: &Side<'_>) -> BTreeMap<(String, String), i64> {
    if side.rollout_status != Some("active") {
        return BTreeMap::new();
    }
    side.payload["_bigname_compiled_watch"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| {
            let topic0 = entry["topic0"].as_str().unwrap_or_default();
            (
                (entry["emitter"].to_string(), topic0.to_ascii_lowercase()),
                start_block(&entry["start"]),
            )
        })
        .collect()
}

/// The emitter address of a declared-address watch entry. Every other kind (all emitters, a
/// family's emitters, a resolver implementation's announcements) reads as any emitter, which
/// can only lower the first effect.
fn address_of(emitter: &str) -> Option<String> {
    let emitter: Value = serde_json::from_str(emitter).ok()?;
    (emitter["kind"] == "address")
        .then(|| emitter["address"].as_str().map(str::to_ascii_lowercase))
        .flatten()
}

fn start_block(value: &Value) -> i64 {
    match value {
        Value::Number(number) => number.as_i64().unwrap_or(0),
        Value::String(text) => text.parse().unwrap_or(0),
        _ => 0,
    }
}

/// The lowest block of each kind of first effect, at or below the recorded head.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FirstEffect {
    /// A canonical log matching a newly watched tuple.
    pub(crate) watched_log: Option<i64>,
    /// A canonical log emitted by a changed address.
    pub(crate) emitted_log: Option<i64>,
    /// A discovery edge to or from an instance of a changed address.
    pub(crate) edge: Option<i64>,
    /// A changed declaration's start.
    pub(crate) declared_start: Option<i64>,
}

impl FirstEffect {
    pub(crate) fn block(&self) -> Option<i64> {
        [
            self.watched_log,
            self.emitted_log,
            self.edge,
            self.declared_start,
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

const READABLE: &str = "lineage.canonicality_state IN ('canonical', 'safe', 'finalized')";

/// The first canonical log at or above each new watch entry's start that matches its emitter and
/// topic0. Served by `raw_logs_emitter_idx` and `raw_logs_topic_idx`.
const WATCHED_LOG: &str = "SELECT min(hit.block_number)
    FROM unnest($2::text[], $3::text[], $4::bigint[]) AS watch(address, topic0, start)
    CROSS JOIN LATERAL (
        SELECT log.block_number FROM raw_logs log
        JOIN chain_lineage lineage ON lineage.chain_id = log.chain_id
         AND lineage.block_hash = log.block_hash AND lineage.block_number = log.block_number
        WHERE log.chain_id = $1 AND cardinality(log.topics) > 0
          AND lower(log.topics[1]) = watch.topic0
          AND (watch.address IS NULL OR lower(log.emitting_address) = watch.address)
          AND log.block_number BETWEEN watch.start AND $5 AND {READABLE}
        ORDER BY log.block_number LIMIT 1
    ) hit";

/// The first canonical log each changed address emitted. Served by `raw_logs_emitter_idx`.
const EMITTED_LOG: &str = "SELECT min(hit.block_number)
    FROM unnest($2::text[]) AS target(address)
    CROSS JOIN LATERAL (
        SELECT log.block_number FROM raw_logs log
        JOIN chain_lineage lineage ON lineage.chain_id = log.chain_id
         AND lineage.block_hash = log.block_hash AND lineage.block_number = log.block_number
        WHERE log.chain_id = $1 AND lower(log.emitting_address) = target.address
          AND log.block_number <= $3 AND {READABLE}
        ORDER BY log.block_number LIMIT 1
    ) hit";

/// The first block of a discovery edge to or from an instance of a changed address. Served by
/// `discovery_edges_active_from_idx` and `discovery_edges_active_to_idx`.
const MENTIONED_EDGE: &str = "SELECT min(edge.active_from_block_number)
    FROM discovery_edges edge
    WHERE edge.chain_id = $1 AND edge.active_from_block_number <= $2
      AND EXISTS (
          SELECT 1 FROM contract_instance_addresses instance
          WHERE instance.chain_id = $1 AND lower(instance.address) = ANY($3)
            AND instance.contract_instance_id IN (
                edge.from_contract_instance_id, edge.to_contract_instance_id))";

/// The first effect of `diff` on chain `chain_id` at or below `head`, against canonical lineage
/// only, in the redo's locked begin transaction.
pub(crate) async fn scan(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    diff: &ManifestDiff,
    head: i64,
) -> RunnerResult<FirstEffect> {
    let database = |error: sqlx::Error| {
        RunnerError::database("failed to scan a manifest change's first effect", error)
    };
    let readable = |sql: &str| sql.replace("{READABLE}", READABLE);
    let addresses: Vec<String> = diff
        .addresses
        .iter()
        .filter(|address| {
            address.len() == 42
                && address.starts_with("0x")
                && address[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        .cloned()
        .collect();
    let mut effect = FirstEffect {
        declared_start: diff.starts.iter().copied().find(|start| *start <= head),
        ..FirstEffect::default()
    };
    if !diff.watches.is_empty() {
        let (mut emitters, mut topics, mut starts) = (Vec::new(), Vec::new(), Vec::new());
        for (address, topic0, start) in &diff.watches {
            emitters.push(address.clone());
            topics.push(topic0.clone());
            starts.push(*start);
        }
        effect.watched_log = sqlx::query_scalar(&readable(WATCHED_LOG))
            .bind(chain_id)
            .bind(emitters)
            .bind(topics)
            .bind(starts)
            .bind(head)
            .fetch_one(&mut **transaction)
            .await
            .map_err(database)?;
    }
    if !addresses.is_empty() {
        effect.emitted_log = sqlx::query_scalar(&readable(EMITTED_LOG))
            .bind(chain_id)
            .bind(&addresses)
            .bind(head)
            .fetch_one(&mut **transaction)
            .await
            .map_err(database)?;
        effect.edge = sqlx::query_scalar(MENTIONED_EDGE)
            .bind(chain_id)
            .bind(head)
            .bind(&addresses)
            .fetch_one(&mut **transaction)
            .await
            .map_err(database)?;
    }
    Ok(effect)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ManifestDiff, Side};

    fn side<'a>(status: &'a str, payload: &'a serde_json::Value) -> Side<'a> {
        Side {
            rollout_status: Some(status),
            payload,
        }
    }

    #[test]
    fn a_new_contract_contributes_its_start_address_and_watch() {
        let before = json!({"contracts": [], "_bigname_compiled_watch": []});
        let after = json!({
            "contracts": [{"role": "resolver", "address": "0xAB00000000000000000000000000000000000001",
                "proxy_kind": "none", "start_block": 40}],
            "_bigname_compiled_watch": [{"emitter": {"kind": "address", "family": "f",
                "address": "0xab00000000000000000000000000000000000001"}, "topic0": "0xT", "start": 40}],
        });
        let mut diff = ManifestDiff::default();
        diff.add(
            Some(&side("active", &before)),
            Some(&side("active", &after)),
        );
        assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [40]);
        assert_eq!(
            diff.addresses.into_iter().collect::<Vec<_>>(),
            ["0xab00000000000000000000000000000000000001"]
        );
        assert_eq!(
            diff.watches.into_iter().collect::<Vec<_>>(),
            [(
                Some("0xab00000000000000000000000000000000000001".to_owned()),
                "0xt".to_owned(),
                40
            )]
        );
    }

    #[test]
    fn an_unchanged_declaration_and_a_flag_edit_contribute_nothing() {
        let contract = json!({"role": "registry", "address": "0x01", "start_block": 7});
        let before = json!({"contracts": [contract], "capability_flags": {"a": "supported"}});
        let after = json!({"contracts": [contract], "capability_flags": {"a": "unsupported"}});
        let mut diff = ManifestDiff::default();
        diff.add(
            Some(&side("active", &before)),
            Some(&side("active", &after)),
        );
        assert_eq!(diff, ManifestDiff::default());
    }

    #[test]
    fn a_rule_starts_at_its_role_and_a_status_change_moves_every_declaration() {
        let payload = json!({
            "roots": [{"name": "registry", "address": "0x02", "start_block": 9}],
            "discovery_rules": [{"edge_kind": "resolver", "from_role": "registry",
                "admission": "reachable_from_root"}],
        });
        let mut diff = ManifestDiff::default();
        diff.add(
            Some(&side("shadow", &payload)),
            Some(&side("active", &payload)),
        );
        assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [9]);
        assert_eq!(diff.addresses.into_iter().collect::<Vec<_>>(), ["0x02"]);
    }
}
