use std::collections::BTreeSet;

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use super::types::{GlobalFoldEventRow, NameFoldEventRow};

pub(super) const SECONDS_PER_REGISTRATION_YEAR: i64 = 31_536_000;
pub(super) const SPONSORED_UPDATES_PER_YEAR: i64 = 5;
const WEI_PER_ETH: u128 = 1_000_000_000_000_000_000;

/// Per-name accounting folded from registration and sponsored-write facts.
/// A lease-starting registration resets the ledger: earned counts purchased
/// seconds since the latest lease start, spent counts distinct sponsored
/// operations at or after it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct NameAccounting {
    pub(super) lease_start_at: Option<OffsetDateTime>,
    pub(super) registered_seconds_total: i64,
    pub(super) earned_updates: i64,
    pub(super) spent_updates: i64,
    pub(super) last_sponsored_write_at: Option<OffsetDateTime>,
}

pub(super) fn fold_name_accounting(events: &[NameFoldEventRow]) -> NameAccounting {
    let mut accounting = NameAccounting::default();
    let mut spent_operations = BTreeSet::new();

    for event in events {
        match event.event_kind.as_str() {
            "RegistrationGranted" | "RegistrarNameRegistered" => {
                // A new lease: the ledger restarts here.
                accounting.registered_seconds_total = purchased_grant_seconds(event);
                accounting.lease_start_at = event.block_timestamp;
                spent_operations.clear();
                accounting.last_sponsored_write_at = None;
            }
            "RegistrationRenewed" => {
                accounting.registered_seconds_total = accounting
                    .registered_seconds_total
                    .saturating_add(purchased_renewal_seconds(event));
            }
            "SponsoredNameWriteObserved" => {
                if let Some(user_op_hash) = json_str(&event.after_state, "user_op_hash") {
                    spent_operations.insert(user_op_hash.to_owned());
                    accounting.last_sponsored_write_at =
                        event.block_timestamp.or(accounting.last_sponsored_write_at);
                }
            }
            _ => {}
        }
    }

    accounting.spent_updates = spent_operations.len() as i64;
    accounting.earned_updates = earned_updates(accounting.registered_seconds_total);
    accounting
}

pub(super) fn earned_updates(registered_seconds_total: i64) -> i64 {
    registered_seconds_total
        .max(0)
        .saturating_mul(SPONSORED_UPDATES_PER_YEAR)
        / SECONDS_PER_REGISTRATION_YEAR
}

/// Purchased seconds of a lease-starting registration: the ENSv2 registrar
/// payload carries `duration`; ENSv1 grants carry only the absolute expiry,
/// so the term is `expiry − block_timestamp`.
fn purchased_grant_seconds(event: &NameFoldEventRow) -> i64 {
    if let Some(duration) = json_i64(&event.after_state, "duration") {
        return duration.max(0);
    }
    match (
        json_i64(&event.after_state, "expiry"),
        event.block_timestamp,
    ) {
        (Some(expiry), Some(block_timestamp)) => expiry
            .saturating_sub(block_timestamp.unix_timestamp())
            .max(0),
        _ => 0,
    }
}

/// Purchased seconds of a renewal: `duration` when the payload carries it,
/// else the expiry delta (`after.expiry − before.expiry`).
fn purchased_renewal_seconds(event: &NameFoldEventRow) -> i64 {
    if let Some(duration) = json_i64(&event.after_state, "duration") {
        return duration.max(0);
    }
    match (
        json_i64(&event.after_state, "expiry"),
        json_i64(&event.before_state, "expiry"),
    ) {
        (Some(after_expiry), Some(before_expiry)) => {
            after_expiry.saturating_sub(before_expiry).max(0)
        }
        _ => 0,
    }
}

/// Namespace-global accounting folded from sponsored-operation and price
/// facts. USD conversion applies the latest answer at or before each
/// operation in chain order; operations before the first retained answer
/// accumulate in the unpriced bucket.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct GlobalAccounting {
    pub(super) sponsored_op_count: i64,
    pub(super) attributed_op_count: i64,
    pub(super) failed_op_count: i64,
    pub(super) gas_wei_total: u128,
    pub(super) failed_gas_wei_total: u128,
    pub(super) usd_e8_total: u128,
    pub(super) unpriced_wei_total: u128,
}

pub(super) fn fold_global_accounting(events: &[GlobalFoldEventRow]) -> GlobalAccounting {
    let mut accounting = GlobalAccounting::default();
    let mut current_answer_e8: Option<u128> = None;

    for event in events {
        match event.event_kind.as_str() {
            "PriceFeedAnswerUpdated" => {
                // Non-positive answers cannot price gas; treat them as an
                // outage until the next positive answer.
                current_answer_e8 = json_str(&event.after_state, "answer_e8")
                    .and_then(|answer| answer.parse::<i128>().ok())
                    .filter(|answer| *answer > 0)
                    .map(|answer| answer as u128);
            }
            "SponsoredUserOperationObserved" => {
                accounting.sponsored_op_count += 1;
                if json_str(&event.after_state, "attribution_status") == Some("attributed") {
                    accounting.attributed_op_count += 1;
                }
                let succeeded =
                    event.after_state.get("success").and_then(Value::as_bool) == Some(true);
                if !succeeded {
                    accounting.failed_op_count += 1;
                }

                let gas_wei = json_str(&event.after_state, "actual_gas_cost_wei")
                    .and_then(|value| value.parse::<u128>().ok())
                    .unwrap_or(0);
                accounting.gas_wei_total = accounting.gas_wei_total.saturating_add(gas_wei);
                if !succeeded {
                    accounting.failed_gas_wei_total =
                        accounting.failed_gas_wei_total.saturating_add(gas_wei);
                }
                match current_answer_e8 {
                    Some(answer_e8) => {
                        let usd_e8 = gas_wei.saturating_mul(answer_e8) / WEI_PER_ETH;
                        accounting.usd_e8_total = accounting.usd_e8_total.saturating_add(usd_e8);
                    }
                    None => {
                        accounting.unpriced_wei_total =
                            accounting.unpriced_wei_total.saturating_add(gas_wei);
                    }
                }
            }
            _ => {}
        }
    }

    accounting
}

fn json_str<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn json_i64(value: &Value, field: &str) -> Option<i64> {
    match value.get(field)? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn registration_event(
        event_kind: &str,
        after_state: Value,
        before_state: Value,
        block_timestamp: i64,
    ) -> NameFoldEventRow {
        NameFoldEventRow {
            normalized_event_id: 1,
            event_kind: event_kind.to_owned(),
            chain_id: "ethereum-sepolia".to_owned(),
            block_number: Some(block_timestamp / 12),
            block_hash: Some("0xblock".to_owned()),
            block_timestamp: Some(
                OffsetDateTime::from_unix_timestamp(block_timestamp)
                    .expect("test timestamp is valid"),
            ),
            manifest_version: 1,
            canonicality_state: "canonical".to_owned(),
            before_state,
            after_state,
        }
    }

    fn write_event(user_op_hash: &str, block_timestamp: i64) -> NameFoldEventRow {
        registration_event(
            "SponsoredNameWriteObserved",
            json!({"user_op_hash": user_op_hash, "success": true}),
            json!({}),
            block_timestamp,
        )
    }

    #[test]
    fn v2_grant_and_renewal_durations_accumulate_and_floor() {
        let events = vec![
            registration_event(
                "RegistrarNameRegistered",
                json!({"duration": SECONDS_PER_REGISTRATION_YEAR}),
                json!({}),
                1_000_000,
            ),
            registration_event(
                "RegistrationRenewed",
                json!({"duration": SECONDS_PER_REGISTRATION_YEAR / 2}),
                json!({}),
                2_000_000,
            ),
        ];
        let accounting = fold_name_accounting(&events);
        assert_eq!(
            accounting.registered_seconds_total,
            SECONDS_PER_REGISTRATION_YEAR + SECONDS_PER_REGISTRATION_YEAR / 2
        );
        // floor(5 × 1.5 years) = 7.
        assert_eq!(accounting.earned_updates, 7);
    }

    #[test]
    fn v1_grants_and_renewals_derive_durations_from_expiry() {
        let grant_ts = 1_000_000;
        let events = vec![
            registration_event(
                "RegistrationGranted",
                json!({"expiry": grant_ts + SECONDS_PER_REGISTRATION_YEAR}),
                json!({}),
                grant_ts,
            ),
            registration_event(
                "RegistrationRenewed",
                json!({"expiry": grant_ts + 3 * SECONDS_PER_REGISTRATION_YEAR}),
                json!({"expiry": grant_ts + SECONDS_PER_REGISTRATION_YEAR}),
                grant_ts + 100,
            ),
        ];
        let accounting = fold_name_accounting(&events);
        assert_eq!(
            accounting.registered_seconds_total,
            3 * SECONDS_PER_REGISTRATION_YEAR
        );
        assert_eq!(accounting.earned_updates, 15);
    }

    #[test]
    fn re_registration_resets_earned_and_spent() {
        let events = vec![
            registration_event(
                "RegistrarNameRegistered",
                json!({"duration": 10 * SECONDS_PER_REGISTRATION_YEAR}),
                json!({}),
                1_000_000,
            ),
            write_event("0xop1", 1_000_100),
            write_event("0xop2", 1_000_200),
            // Lapse, then a fresh registration: prior earned and spent are gone.
            registration_event(
                "RegistrarNameRegistered",
                json!({"duration": SECONDS_PER_REGISTRATION_YEAR}),
                json!({}),
                2_000_000,
            ),
            write_event("0xop3", 2_000_100),
        ];
        let accounting = fold_name_accounting(&events);
        assert_eq!(accounting.earned_updates, 5);
        assert_eq!(accounting.spent_updates, 1);
        assert_eq!(
            accounting.lease_start_at.map(|at| at.unix_timestamp()),
            Some(2_000_000)
        );
    }

    #[test]
    fn spent_counts_distinct_operations_including_failures() {
        let mut failed_write = write_event("0xop1", 1_000_100);
        failed_write.after_state = json!({"user_op_hash": "0xop1", "success": false});
        let events = vec![
            registration_event(
                "RegistrarNameRegistered",
                json!({"duration": SECONDS_PER_REGISTRATION_YEAR}),
                json!({}),
                1_000_000,
            ),
            failed_write,
            // Same operation touching the name twice still spends once.
            write_event("0xop1", 1_000_100),
            write_event("0xop2", 1_000_200),
        ];
        let accounting = fold_name_accounting(&events);
        assert_eq!(accounting.spent_updates, 2);
    }

    fn global_event(event_kind: &str, after_state: Value, order: i64) -> GlobalFoldEventRow {
        GlobalFoldEventRow {
            normalized_event_id: order,
            event_kind: event_kind.to_owned(),
            chain_id: "ethereum-sepolia".to_owned(),
            block_number: Some(order),
            block_hash: Some("0xblock".to_owned()),
            block_timestamp: None,
            manifest_version: 1,
            canonicality_state: "canonical".to_owned(),
            after_state,
        }
    }

    fn operation(gas_wei: u128, success: bool, attributed: bool, order: i64) -> GlobalFoldEventRow {
        global_event(
            "SponsoredUserOperationObserved",
            json!({
                "actual_gas_cost_wei": gas_wei.to_string(),
                "success": success,
                "attribution_status": if attributed { "attributed" } else { "unattributed" },
            }),
            order,
        )
    }

    #[test]
    fn global_fold_prices_at_latest_answer_and_buckets_unpriced() {
        let events = vec![
            // Before any answer: unpriced.
            operation(WEI_PER_ETH, true, true, 1),
            global_event(
                "PriceFeedAnswerUpdated",
                json!({"answer_e8": "250000000000"}),
                2,
            ),
            // 0.5 ETH at 2500 USD -> 1250 USD -> 125_000_000_000 e8.
            operation(WEI_PER_ETH / 2, false, false, 3),
            global_event(
                "PriceFeedAnswerUpdated",
                json!({"answer_e8": "300000000000"}),
                4,
            ),
            // 1 ETH at 3000 USD.
            operation(WEI_PER_ETH, true, true, 5),
        ];
        let accounting = fold_global_accounting(&events);
        assert_eq!(accounting.sponsored_op_count, 3);
        assert_eq!(accounting.attributed_op_count, 2);
        assert_eq!(accounting.failed_op_count, 1);
        assert_eq!(accounting.gas_wei_total, WEI_PER_ETH * 5 / 2);
        assert_eq!(accounting.failed_gas_wei_total, WEI_PER_ETH / 2);
        assert_eq!(accounting.unpriced_wei_total, WEI_PER_ETH);
        assert_eq!(accounting.usd_e8_total, 125_000_000_000 + 300_000_000_000);
    }

    #[test]
    fn non_positive_answers_suspend_pricing() {
        let events = vec![
            global_event("PriceFeedAnswerUpdated", json!({"answer_e8": "-1"}), 1),
            operation(WEI_PER_ETH, true, true, 2),
        ];
        let accounting = fold_global_accounting(&events);
        assert_eq!(accounting.usd_e8_total, 0);
        assert_eq!(accounting.unpriced_wei_total, WEI_PER_ETH);
    }
}
