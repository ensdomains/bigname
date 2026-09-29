//! Which expiry a `.eth` registration serves and when its renewal grace ends.
//!
//! A `.eth` second-level name can hold an ENSv1 BaseRegistrar lease and an ENSv2 `eth` registry
//! entry at once: premigration reserves every live ENSv1 name in ENSv2 with the lease's expiry
//! plus a continuity bonus of 62 days (90-day ENSv1 grace minus 28-day ENSv2 grace), so that both
//! renewal deadlines fall on the same second, and claiming the reservation keeps its expiry.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L38-L42 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L246-L256 @ ens_v2_sepolia_20260916@366de741)
//! Authority still follows the chain (ADR 0007): a reservation defers ownership to ENSv1. The
//! expiry does not: once the chain resolves through ENSv2 (the Universal Resolver cutover,
//! `families::control::cutover`), a name with a live ENSv2 entry serves that entry's expiry and
//! the ENSv2 grace period whichever arm holds authority. Before the cutover resolution starts at
//! ENSv1, so the lease's expiry and the ENSv1 grace apply, reservation or not.
use alloy_primitives::{B256, keccak256};
use anyhow::Result;
use serde_json::{Map, Value, json};

use super::{NameFacts, NamePlace, laterals::expiry_candidate, select::select_v2, served::Tagged};
use crate::families::control::rows::LifecycleEvent;

/// The ENSv1 BaseRegistrar grace period: a lease stays renewable, and unavailable to others,
/// until its expiry plus 90 days.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
pub(crate) const ENS_V1_GRACE_PERIOD_SECONDS: i64 = 7_776_000;
/// The ENSv2 `ETHRegistrar` grace period, an immutable constructor argument: 28 days in the
/// deployment constants and in the admitted Sepolia deployment's constructor arguments (word 4 of
/// `argsData`). A registration past its expiry stays renewable by its last holder while the
/// time since expiry is below it.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L43 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L275-L292 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/script/deploy-constants.ts:L233 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ETHRegistrar.json:L1419 @ ens_v2_sepolia_20260916@366de741)
pub(crate) const ENS_V2_GRACE_PERIOD_SECONDS: i64 = 2_419_200;
/// The Basenames registrar grace period.
/// (upstream: .refs/basenames/src/util/Constants.sol:L15 @ basenames@1809bbc)
pub(crate) const BASENAMES_GRACE_PERIOD_SECONDS: i64 = 7_776_000;

/// The logical name id of a name given its label hashes in name order (`0x`-prefixed hex).
pub(super) fn logical_name_of_labelhashes(
    namespace: &str,
    labelhashes: &[String],
) -> Option<String> {
    let node = labelhashes
        .iter()
        .rev()
        .try_fold(B256::ZERO, |parent, labelhash| {
            let labelhash: B256 = labelhash.parse().ok()?;
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(parent.as_slice());
            input[32..].copy_from_slice(labelhash.as_slice());
            Some(keccak256(input))
        })?;
    Some(format!("{namespace}:{node:#x}"))
}

/// A name's live ENSv2 registry entry: the ENSv2 registration candidate
/// (`select::select_v2`) when it is a reservation or registration with no release after it, with
/// its lifecycle key and expiry. Interpret writes the path-expiry release at the first block
/// whose time has reached the expiry, so an unreleased candidate has not expired at the
/// publication.
pub(super) struct LiveEntry<'a> {
    pub(super) key: String,
    pub(super) event: &'a LifecycleEvent,
    /// The expiry lateral over the key's own ENSv2 events, else the candidate's own expiry.
    pub(super) expiry: Value,
}

pub(super) fn live_entry<'a>(
    facts: &'a NameFacts,
    tagged: &[Tagged<'a>],
) -> Result<Option<LiveEntry<'a>>> {
    let mut scratch = Map::new();
    let selected = select_v2(facts, tagged, None, &mut scratch)?;
    let (Some(event), Some(key)) = (selected.event, selected.lifecycle_key) else {
        return Ok(None);
    };
    if !matches!(
        event.event_kind.as_str(),
        "RegistrationGranted" | "RegistrationReserved"
    ) {
        return Ok(None);
    }
    let scope: Vec<&Tagged<'_>> = tagged
        .iter()
        .filter(|tagged| {
            tagged.staged == super::admission::StagedName::Ours
                && tagged.event.is_v2_family()
                && tagged.key.as_deref() == Some(key.as_str())
        })
        .collect();
    let expiry =
        expiry_candidate(&scope).map_or_else(|| event.expiry.clone(), |seconds| json!(seconds));
    Ok(Some(LiveEntry { key, event, expiry }))
}

/// The renewal grace a served expiry carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Grace {
    EnsV1,
    EnsV2,
    Basenames,
    /// No registrar grace: the registration ends at its expiry.
    None,
}

impl Grace {
    fn seconds(self) -> i64 {
        match self {
            Self::EnsV1 => ENS_V1_GRACE_PERIOD_SECONDS,
            Self::EnsV2 => ENS_V2_GRACE_PERIOD_SECONDS,
            Self::Basenames => BASENAMES_GRACE_PERIOD_SECONDS,
            Self::None => 0,
        }
    }
}

/// The expiry the registration serves and its grace. `served` is the expiry the selected arm
/// serves; `entry` the name's live ENSv2 entry.
pub(super) fn choose(
    facts: &NameFacts,
    is_v2: bool,
    served: Value,
    entry: Option<&LiveEntry<'_>>,
    trace: &mut Map<String, Value>,
) -> (Value, Grace) {
    trace.insert("resolution_cutover".into(), json!(facts.resolution_cutover));
    trace.insert(
        "ens_v2_entry".into(),
        entry.map_or(Value::Null, |entry| {
            json!({"key": entry.key, "event": entry.event.position.event_identity,
                   "expiry": entry.expiry})
        }),
    );
    match facts.input.place {
        NamePlace::EthSecondLevel if is_v2 => (served, Grace::EnsV2),
        NamePlace::EthSecondLevel => match entry.filter(|_| facts.resolution_cutover) {
            Some(entry) => (entry.expiry.clone(), Grace::EnsV2),
            None => (served, Grace::EnsV1),
        },
        NamePlace::BasenamesSecondLevel => (served, Grace::Basenames),
        NamePlace::BelowEthSecondLevel(_) | NamePlace::Other => (served, Grace::None),
    }
}

/// When the renewal grace of `expiry` ends: the expiry plus the grace period, in seconds. Null
/// when the expiry is not an integral second or the sum passes the largest signed 64-bit second.
pub(super) fn grace_ends_at(expiry: Option<&Value>, grace: Grace) -> Value {
    expiry
        .and_then(|expiry| expiry.as_i64().or_else(|| expiry.as_str()?.parse().ok()))
        .and_then(|expiry| expiry.checked_add(grace.seconds()))
        .map_or(Value::Null, |seconds| json!(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grace_accepts_the_same_quoted_seconds_as_the_served_expiry() {
        for expiry in [json!(2_000_000_000_i64), json!("2000000000")] {
            assert_eq!(
                grace_ends_at(Some(&expiry), Grace::EnsV2),
                json!(2_002_419_200_i64)
            );
        }
        assert_eq!(
            grace_ends_at(Some(&json!(i64::MAX)), Grace::EnsV2),
            Value::Null
        );
    }
}
