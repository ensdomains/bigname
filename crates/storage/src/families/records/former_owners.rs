//! `relation=former_owner` on `GET /v1/addresses/{address}/names` (TYR-63): the released
//! names whose last owner was the address, for reminders and grace renewal.
//!
//! A former owner is the holder a registration had when it ended, which the composed row
//! serves as `registration.lapsed_registration.owner`: the holder of an ENSv1 lease that
//! lapsed past its grace, or of an ENSv2 registration that expired or was unregistered
//! (`control::lifecycle::served`). It exists only while the name is released, so a
//! re-registration drops it. The address index (`project_address_name_index`) already lists
//! every address a name's retained registration events named, the last holder included, so the
//! read composes the address's indexed names and keeps those whose lapsed owner is the address.
//! It never feeds `owner` or `manager`.
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L341-L362 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
//!
//! Rows sort by the served expiry, then namespace, name and namehash; a row without an expiry (an
//! unregistered ENSv2 name) is the smallest value, before every dated row ascending and after
//! them descending, and an expiry window leaves it out. A page is read in one snapshot.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListOrder, NameCurrentRow,
    UnixSeconds,
    families::name::{CoverageShape, load_composed, servable_publication},
    name_current::parent_like_patterns,
};

/// What a former-owner page selects besides its order and position.
#[derive(Clone, Copy, Debug)]
pub struct FormerOwnerFilter<'a> {
    pub address: &'a str,
    pub namespace: Option<&'a str>,
    /// Inclusive lower bound on the served expiry.
    pub expires_after: Option<UnixSeconds>,
    /// Exclusive upper bound on the served expiry.
    pub expires_before: Option<UnixSeconds>,
    /// A normalized name: only names exactly one label below it.
    pub parent: Option<&'a str>,
}

/// One page of former-owner rows and the position after its last row, when more follow.
#[derive(Clone, Debug)]
pub struct FormerOwnerPage {
    pub rows: Vec<NameCurrentRow>,
    pub next_cursor: Option<NameCurrentListCursor>,
}

/// The composed name the address formerly held, with its served expiry.
struct Held {
    row: NameCurrentRow,
    expiry: Option<UnixSeconds>,
}

/// The sort key: expiry, then namespace, name, namehash. A missing expiry is the smallest value,
/// since `None` sorts before `Some`.
type Key = (Option<UnixSeconds>, String, String, String);

impl Held {
    fn key(&self) -> Key {
        (
            self.expiry,
            self.row.namespace.clone(),
            self.row.normalized_name.clone(),
            self.row.namehash.clone(),
        )
    }
}

fn cursor_key(cursor: &NameCurrentListCursor) -> Result<Key> {
    let NameCurrentListCursorValue::Timestamp(expiry) = cursor.sort_value else {
        anyhow::bail!("a former-owner cursor must carry an expiry position");
    };
    Ok((
        expiry,
        cursor.namespace.clone(),
        cursor.normalized_name.clone(),
        cursor.namehash.clone(),
    ))
}

/// The lapsed owner of a composed row, lower-cased.
pub fn lapsed_owner(row: &NameCurrentRow) -> Option<String> {
    row.declared_summary
        .pointer("/registration/lapsed_registration/owner")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
}

/// The served registration expiry, including finite values beyond the calendar range.
fn served_expiry(row: &NameCurrentRow) -> Option<UnixSeconds> {
    row.declared_summary
        .pointer("/registration/expiry")
        .and_then(UnixSeconds::from_json)
}

/// The page of names `filter.address` formerly held.
pub async fn load_family_former_owner_page(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &FormerOwnerFilter<'_>,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<FormerOwnerPage> {
    let after = cursor.map(cursor_key).transpose()?;
    let address = filter.address.to_ascii_lowercase();
    // A served name composes from an active surface, whose stored spelling is normalized, so
    // `parent` can match it in the index query.
    let parent = filter.parent.map(parent_like_patterns);
    let mut snapshot = db.into().snapshot().await?;
    let indexed: Vec<(String, String)> = sqlx::query_as(
        "/* storage:families.records.former_owner_index */
         SELECT DISTINCT indexed.chain_id, indexed.logical_name_id
         FROM bigname_phase.project_address_name_index indexed
         WHERE indexed.address = $1
           AND ($2::text IS NULL OR EXISTS (
               SELECT 1 FROM bigname_phase.name_surfaces surface
               WHERE surface.logical_name_id = indexed.logical_name_id
                 AND surface.namespace = $2))
           AND ($3::text IS NULL OR EXISTS (
               SELECT 1 FROM bigname_phase.name_surfaces surface
               WHERE surface.logical_name_id = indexed.logical_name_id
                 AND surface.raw_name LIKE $3 ESCAPE '\\'
                 AND surface.raw_name NOT LIKE $4 ESCAPE '\\'))",
    )
    .bind(&address)
    .bind(filter.namespace)
    .bind(parent.as_ref().map(|(one_below, _)| one_below.as_str()))
    .bind(parent.as_ref().map(|(_, deeper)| deeper.as_str()))
    .fetch_all(&mut *snapshot)
    .await
    .with_context(|| format!("failed to load the address index of {address}"))?;
    let mut by_chain: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (chain_id, name) in indexed {
        by_chain.entry(chain_id).or_default().push(name);
    }
    let mut held = Vec::new();
    for (chain_id, ids) in by_chain {
        // Refuses a chain whose families are not published, as the other address reads do.
        servable_publication(&mut snapshot, &chain_id).await?;
        let composed = load_composed(&mut snapshot, &ids, CoverageShape::Plain).await?;
        held.extend(
            composed
                .into_values()
                .filter(|row| lapsed_owner(row).as_deref() == Some(address.as_str()))
                .map(|row| Held {
                    expiry: served_expiry(&row),
                    row,
                }),
        );
    }
    snapshot.close().await?;

    let windowed = filter.expires_after.is_some() || filter.expires_before.is_some();
    held.retain(|held| match held.expiry {
        None => !windowed,
        Some(expiry) => {
            filter.expires_after.is_none_or(|after| expiry >= after)
                && filter.expires_before.is_none_or(|before| expiry < before)
        }
    });
    let descending = order == NameCurrentListOrder::Desc;
    held.sort_by(|left, right| {
        let ordering = left.key().cmp(&right.key());
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
    if let Some(after) = &after {
        held.retain(|held| {
            let key = held.key();
            if descending {
                key < *after
            } else {
                key > *after
            }
        });
    }
    let size = usize::try_from(page_size).context("page_size does not fit in usize")?;
    let more = held.len() > size;
    held.truncate(size);
    let next_cursor = more
        .then(|| held.last())
        .flatten()
        .map(|last| NameCurrentListCursor {
            sort_value: NameCurrentListCursorValue::Timestamp(last.expiry),
            namespace: last.row.namespace.clone(),
            normalized_name: last.row.normalized_name.clone(),
            namehash: last.row.namehash.clone(),
        });
    Ok(FormerOwnerPage {
        rows: held.into_iter().map(|held| held.row).collect(),
        next_cursor,
    })
}
