//! `coin_type=evm` on `relation=resolves_to`: one read across every EVM coin type (`60` and
//! `[2^31, 2^32)`, the ENSIP-19 EVM set including the default coin type; upstream:
//! .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f), one row per name with the
//! matched coin types in `resolutions`.
//!
//! The single-coin read keeps its parser, cursor value, `resolution` field, and per-namespace
//! primary lookup; this module only adds the selector, the row matches, and the batched
//! primary-claim read the `evm` rows need.

use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{
    AddressRecordCurrentEntry, AddressRecordEvmEntry, EVM_MATCHED_COIN_TYPES_PER_ROW_LIMIT,
    PrimaryNameClaimStatus,
};

use super::resolves_to::{AddressNameResolution, parse_resolves_to_coin_type};
use crate::AppState;
use crate::v2::{V2Error, V2Result, collection_snapshot::CollectionSnapshot};

/// The only non-numeric `coin_type` spelling. It is matched exactly after the shared query-value
/// trim, so `EVM` is rejected like any other non-numeric value.
const EVM_COIN_SELECTOR: &str = "evm";

/// The parsed `coin_type` of a `resolves_to` read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ResolvesToCoins {
    /// One canonical decimal coin type (default `60`).
    Single { coin_type: String, numeric: u64 },
    /// Every EVM coin type.
    Evm,
}

impl ResolvesToCoins {
    /// The value the cursor binds: the canonical decimal coin type, or `evm`.
    pub(super) fn cursor_value(&self) -> &str {
        match self {
            Self::Single { coin_type, .. } => coin_type,
            Self::Evm => EVM_COIN_SELECTOR,
        }
    }
}

/// Parse `coin_type` for a `resolves_to` read. An empty or whitespace-only value arrives here as
/// absent (shared query normalization) and so still means `60`.
pub(super) fn parse_resolves_to_coins(value: Option<&str>) -> V2Result<ResolvesToCoins> {
    if value == Some(EVM_COIN_SELECTOR) {
        return Ok(ResolvesToCoins::Evm);
    }
    let (coin_type, numeric) = parse_resolves_to_coin_type(value)?;
    Ok(ResolvesToCoins::Single { coin_type, numeric })
}

/// Reject an `evm` page when a served row's group matched more coin types than the storage read
/// aggregates (`EVM_MATCHED_COIN_TYPES_PER_ROW_LIMIT`): such a row is never served with a
/// truncated `resolutions` list. As with the inline role-summary budget, the overflow is reported
/// only for a publication that is still the captured one.
pub(super) async fn reject_rows_past_coin_type_limit(
    state: &AppState,
    snapshot: &CollectionSnapshot,
    rows: &[AddressRecordEvmEntry],
) -> V2Result<()> {
    if rows
        .iter()
        .all(|row| row.matched_coin_type_count <= EVM_MATCHED_COIN_TYPES_PER_ROW_LIMIT)
    {
        return Ok(());
    }
    snapshot.finish(state).await?;
    Err(V2Error::unsupported(format!(
        "coin_type=evm matched more than {EVM_MATCHED_COIN_TYPES_PER_ROW_LIMIT} EVM coin types on one row; request a single decimal coin_type instead"
    )))
}

/// One served `resolves_to` row and what it matched.
pub(super) struct ResolvesToRow {
    pub(super) entry: AddressRecordCurrentEntry,
    pub(super) matches: ResolvesToMatches,
}

pub(super) enum ResolvesToMatches {
    Single(AddressNameResolution),
    Evm {
        /// The dedupe group's matches, ascending by coin type.
        resolutions: Vec<AddressNameResolution>,
        /// The coin types the displayed name itself matched; only these decide `is_primary`.
        own_coin_types: Vec<String>,
    },
}

impl ResolvesToRow {
    pub(super) fn from_evm(row: AddressRecordEvmEntry) -> V2Result<Self> {
        let resolutions = row
            .resolutions
            .into_iter()
            .map(|matched| {
                let coin_type = matched.coin_type.parse::<u64>().map_err(|_| {
                    V2Error::internal_error(format!(
                        "stored EVM coin type {} does not fit in u64",
                        matched.coin_type
                    ))
                })?;
                Ok(AddressNameResolution {
                    coin_type,
                    record_key: matched.record_key,
                })
            })
            .collect::<V2Result<Vec<_>>>()?;
        Ok(Self {
            entry: row.entry,
            matches: ResolvesToMatches::Evm {
                resolutions,
                own_coin_types: row.representative_coin_types,
            },
        })
    }

    pub(super) fn resolution_fields(
        &self,
    ) -> (
        Option<AddressNameResolution>,
        Option<Vec<AddressNameResolution>>,
    ) {
        match &self.matches {
            ResolvesToMatches::Single(resolution) => (Some(resolution.clone()), None),
            ResolvesToMatches::Evm { resolutions, .. } => (None, Some(resolutions.clone())),
        }
    }
}

/// `is_primary` for each `evm` row: true when the row namespace's successful claim for one of the
/// row's own matched coin types names the row. All `(namespace, coin_type)` claims the page needs
/// are read in one statement with the primary-name snapshot rules (canonicality, hydration
/// fallback, normalized-spelling decoding).
pub(super) async fn evm_primary_flags(
    pool: &sqlx::PgPool,
    address: &str,
    rows: &[ResolvesToRow],
) -> V2Result<Vec<bool>> {
    let keys = rows
        .iter()
        .flat_map(|row| {
            own_coin_types(row)
                .iter()
                .map(|coin_type| (row.entry.namespace.clone(), coin_type.clone()))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let claims = bigname_storage::load_primary_name_current_snapshots(pool, address, &keys)
        .await
        .map_err(|_| {
            V2Error::internal_error(format!(
                "failed to load primary names for address {address}"
            ))
        })?
        .into_iter()
        .filter(|(_, snapshot)| snapshot.row.claim_status == PrimaryNameClaimStatus::Success)
        .filter_map(|(key, snapshot)| {
            snapshot
                .normalized_claim_name
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty())
                .map(|name| (key, name))
        })
        .collect::<BTreeMap<_, _>>();
    Ok(rows
        .iter()
        .map(|row| {
            own_coin_types(row).iter().any(|coin_type| {
                claims
                    .get(&(row.entry.namespace.clone(), coin_type.clone()))
                    .is_some_and(|name| *name == row.entry.normalized_name)
            })
        })
        .collect())
}

fn own_coin_types(row: &ResolvesToRow) -> &[String] {
    match &row.matches {
        ResolvesToMatches::Evm { own_coin_types, .. } => own_coin_types,
        ResolvesToMatches::Single(_) => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evm_selector_is_exact_and_blank_still_means_sixty() {
        assert_eq!(
            parse_resolves_to_coins(Some("evm")).expect("evm"),
            ResolvesToCoins::Evm
        );
        assert_eq!(
            parse_resolves_to_coins(None).expect("default"),
            ResolvesToCoins::Single {
                coin_type: "60".to_owned(),
                numeric: 60
            }
        );
        assert_eq!(ResolvesToCoins::Evm.cursor_value(), "evm");
        assert_eq!(
            parse_resolves_to_coins(Some("0060"))
                .expect("canonical")
                .cursor_value(),
            "60"
        );
        for rejected in ["EVM", "Evm", "evm,60", "60,61", "any"] {
            assert!(
                parse_resolves_to_coins(Some(rejected)).is_err(),
                "{rejected}"
            );
        }
    }
}
