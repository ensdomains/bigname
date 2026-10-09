//! The wrapper's immutable fallback node comes from activated migration evidence, never from
//! its current mount or an arbitrary factory salt. Eligibility and requested record node differ.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L148-L164 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L312-L327 @ ens_v2_sepolia_20261001@07e55a05)
use crate::families::{
    control::{
        lifecycle::{AuthoritySelection, NameInput, NamePlace, load_name_facts_on},
        registry::OwnerEvent,
    },
    name::FamilyPublication,
};
use alloy_primitives::{Address, B256, keccak256};
use anyhow::Result;
use serde_json::Value;
use sqlx::{PgConnection, Row};
pub(super) const MIRROR: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";

pub(super) async fn original_node(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    registry: &str,
) -> Result<Option<String>> {
    let origins: Vec<Value> = sqlx::query_scalar("/* storage:families.name.wrapper_origin */
        SELECT to_jsonb(e) FROM bigname_phase.normalized_events e
        WHERE chain_id = $1 AND lower(after_state ->> 'proxy_address') = $2
          AND source_family = 'ens_v2_migration_l1' AND event_kind = 'ContractDiscovered'
          AND consumer_visibility = 'activated' AND after_state ->> 'source_event' = 'ContractDiscovered'
          AND canonicality_state IN ('canonical','safe','finalized') AND block_number <= $3
        ORDER BY block_number, transaction_index, log_index LIMIT 2")
        .bind(&publication.chain_id).bind(registry).bind(publication.block_number).fetch_all(&mut *conn).await?;
    let [origin] = origins.as_slice() else {
        return Ok(None);
    };
    let Some(node) = origin["after_state"]["salt"]
        .as_str()
        .filter(|node| node.parse::<B256>().is_ok())
    else {
        return Ok(None);
    };
    let migrations: Vec<Value> = sqlx::query_scalar("/* storage:families.name.wrapper_migration */
        SELECT after_state -> 'evidence' FROM bigname_phase.normalized_events
        WHERE logical_name_id = $1 AND chain_id = $2 AND block_number = $3
          AND block_hash = $4 AND transaction_hash = $5 AND event_kind = 'MigrationApplied'
          AND consumer_visibility = 'activated' AND canonicality_state IN ('canonical','safe','finalized') LIMIT 2")
        .bind(format!("ens:{node}")).bind(&publication.chain_id).bind(origin["block_number"].as_i64())
        .bind(origin["block_hash"].as_str()).bind(origin["transaction_hash"].as_str()).fetch_all(&mut *conn).await?;
    let [evidence] = migrations.as_slice() else {
        return Ok(None);
    };
    let Some(evidence) = evidence.as_array() else {
        return Ok(None);
    };
    let matches_position = |item: &Value| {
        [
            "chain_id",
            "block_number",
            "block_hash",
            "transaction_hash",
            "transaction_index",
        ]
        .into_iter()
        .all(|key| !origin[key].is_null() && item[key] == origin[key])
    };
    let deployed: Vec<_> = evidence
        .iter()
        .filter(|item| {
            item["kind"] == "raw_log"
                && item["event"] == "ProxyDeployed"
                && matches_position(item)
                && item["log_index"] == origin["log_index"]
                && item["emitting_address"] == origin["raw_fact_ref"]["emitting_address"]
                && ["sender", "proxy_address", "salt", "implementation"]
                    .into_iter()
                    .all(|key| {
                        !origin["after_state"][key].is_null()
                            && item["decoded"][key] == origin["after_state"][key]
                    })
        })
        .collect();
    if deployed.len() != 1 {
        return Ok(None);
    }
    let announcements: Vec<_> = evidence
        .iter()
        .filter(|item| {
            item["kind"] == "normalized_event"
                && item["event_kind"] == "RegistryCreated"
                && matches_position(item)
                && item["log_index"]
                    .as_i64()
                    .zip(origin["log_index"].as_i64())
                    .is_some_and(|(a, b)| a < b)
        })
        .collect();
    let [announcement] = announcements.as_slice() else {
        return Ok(None);
    };
    let row = sqlx::query("/* storage:families.name.wrapper_announcement */
        SELECT to_jsonb(e) AS event FROM bigname_phase.normalized_events e
        WHERE event_identity = $1 AND chain_id = $2 AND event_kind = 'RegistryCreated'
          AND consumer_visibility = 'activated' AND canonicality_state IN ('canonical','safe','finalized')")
        .bind(announcement["event_identity"].as_str()).bind(&publication.chain_id).fetch_optional(conn).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event: Value = row.try_get("event")?;
    if !matches_position(&event)
        || event["log_index"] != announcement["log_index"]
        || event["after_state"]["registry"] != registry
        || event["raw_fact_ref"]["emitting_address"] != registry
        || event["after_state"]["contract_instance_id"]
            .as_str()
            .is_none_or(|id| id.parse::<uuid::Uuid>().is_err())
    {
        return Ok(None);
    }
    Ok(Some(node.to_owned()))
}

// WrapperRegistry checks the original ENSv1 getter, independently of public owner display.
// Select the latest owner-setting event before reading the getter: absent/unmasked evidence
// must not fall back to an older known value, a public owner string or an ENSv2 token owner.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L314-L326 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
fn ens_v1_owner_getter(events: &[OwnerEvent], publication_block: i64) -> Option<Address> {
    let event = events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registry_l1"
                && matches!(
                    event.event_kind.as_str(),
                    "AuthorityTransferred" | "SubregistryChanged"
                )
                && event.position.block_number <= publication_block
        })
        .max_by(|a, b| a.position.cmp(&b.position))?;
    if event.owner_word_unmasked == Some(true) {
        return None;
    }
    event.owner_getter.as_deref()?.parse().ok()
}

pub(super) struct Eligibility {
    pub eligible: Option<bool>,
    pub deadline: Option<i64>,
}
pub(super) async fn eligible(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    parent: &str,
    label: &str,
) -> Result<Eligibility> {
    let mut bytes = [0; 64];
    bytes[..32].copy_from_slice(parent.parse::<B256>()?.as_slice());
    bytes[32..].copy_from_slice(label.parse::<B256>()?.as_slice());
    let node = format!("{:#x}", keccak256(bytes));
    let input = NameInput {
        logical_name_id: format!("ens:{node}"),
        namehash: node,
        selection: AuthoritySelection::default(),
        place: NamePlace::Other,
    };
    let facts = load_name_facts_on(
        conn,
        &publication.chain_id,
        publication.admission.as_ref(),
        &[input],
    )
    .await?;
    let Some(facts) = facts.first() else {
        return Ok(Eligibility {
            eligible: None,
            deadline: None,
        });
    };
    let owner = facts
        .registry_node
        .as_ref()
        .and_then(|node| ens_v1_owner_getter(&node.owner_events, publication.block_number));
    let wrappers: Vec<_> = facts
        .wrappers
        .values()
        .filter(|row| {
            row.logical_name_id.as_deref() == Some(&facts.input.logical_name_id) && row.has_modifier
        })
        .collect();
    let [wrapper] = wrappers.as_slice() else {
        return Ok(Eligibility {
            eligible: None,
            deadline: None,
        });
    };
    let Some(expiry) = wrapper
        .expiry_seconds
        .as_deref()
        .and_then(|word| word.parse::<u64>().ok())
    else {
        return Ok(Eligibility {
            eligible: None,
            deadline: None,
        });
    };
    let clock = publication.timestamp_seconds().max(0) as u64;
    // getData masks fuses on expiry, but not on unwrap. The v1 registry owner is independent.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143-L153 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L855 @ ens_v1@91c966f)
    let eligible = if expiry >= clock && wrapper.fuses.is_some_and(|fuses| fuses & 65536 != 0) {
        owner.map(|owner| owner != Address::ZERO)
    } else {
        Some(false)
    };
    let deadline = expiry
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .filter(|value| *value > publication.timestamp_seconds());
    Ok(Eligibility { eligible, deadline })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::control::position::Position;

    const HOLDER: &str = "0x0000000000000000000000000000000000c0ffee";
    const GRAVEYARD: &str = "0xb58a90a39d13cce1d0e192b5da5c47640855b04d";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";

    fn owner_event(block: i64, getter: Option<&str>) -> OwnerEvent {
        OwnerEvent {
            position: Position {
                block_number: block,
                transaction_index: Some(0),
                log_index: Some(0),
                event_identity: format!("owner:{block}"),
            },
            transaction_hash: None,
            logical_name_id: None,
            resource_id: None,
            event_kind: "AuthorityTransferred".into(),
            source_family: "ens_v1_registry_l1".into(),
            authority_kind: Some("registry".into()),
            // Public owner strings must never substitute for missing getter evidence.
            owner: Some(HOLDER.into()),
            registry_owner: Some(HOLDER.into()),
            owner_word_unmasked: Some(false),
            owner_getter: getter.map(Into::into),
            owner_getter_reason: None,
        }
    }

    #[test]
    fn wrapper_getter_preserves_nonzero_graveyard_and_exact_zero() {
        let mut graveyard = owner_event(10, Some(GRAVEYARD));
        graveyard.owner_getter_reason = Some("graveyard".into());
        assert_eq!(graveyard.reported_owner(), None);
        assert_eq!(
            ens_v1_owner_getter(&[graveyard], 10),
            Some(GRAVEYARD.parse().unwrap())
        );
        for reason in ["zero", "registry_self"] {
            let mut zero = owner_event(10, Some(ZERO));
            zero.owner_getter_reason = Some(reason.into());
            assert_eq!(ens_v1_owner_getter(&[zero], 10), Some(Address::ZERO));
        }
    }

    #[test]
    fn wrapper_getter_uses_only_published_ens_v1_ownership_events() {
        let mut events = vec![owner_event(10, Some(HOLDER))];
        for family in [
            "ens_v2_registry_l1",
            "ens_v2_root_l1",
            "basenames_base_registry",
        ] {
            let mut unrelated = owner_event(11, Some(ZERO));
            unrelated.source_family = family.into();
            events.push(unrelated);
        }
        let mut wrong_kind = owner_event(11, Some(ZERO));
        wrong_kind.event_kind = "ResolverChanged".into();
        events.push(wrong_kind);
        events.push(owner_event(12, Some(ZERO)));
        assert_eq!(
            ens_v1_owner_getter(&events, 11),
            Some(HOLDER.parse().unwrap())
        );
        assert_eq!(ens_v1_owner_getter(&events, 12), Some(Address::ZERO));
        let mut child_owner = owner_event(13, Some(GRAVEYARD));
        child_owner.event_kind = "SubregistryChanged".into();
        events.push(child_owner);
        assert_eq!(
            ens_v1_owner_getter(&events, 13),
            Some(GRAVEYARD.parse().unwrap())
        );
    }

    #[test]
    fn wrapper_getter_keeps_latest_missing_or_unmasked_evidence_unknown() {
        for getter in [None, Some("invalid")] {
            let events = [owner_event(10, Some(HOLDER)), owner_event(11, getter)];
            assert_eq!(ens_v1_owner_getter(&events, 11), None);
        }
        let mut unmasked = owner_event(11, Some(HOLDER));
        unmasked.owner_word_unmasked = Some(true);
        assert_eq!(
            ens_v1_owner_getter(&[owner_event(10, Some(HOLDER)), unmasked], 11),
            None
        );
        assert_eq!(ens_v1_owner_getter(&[], 11), None);
    }
}
