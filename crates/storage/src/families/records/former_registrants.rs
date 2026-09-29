//! `relation=former_registrant` on `GET /v1/addresses/{address}/names` (TYR-63): the released
//! names whose last registrant was the address, for reminders and grace renewal.
//!
//! A former registrant is the holder a registration had when it ended, which the composed row
//! serves as `registration.lapsed_registration.registrant`: the holder of an ENSv1 lease that
//! lapsed past its grace, or of an ENSv2 registration that expired or was unregistered
//! (`control::lifecycle::served`). It exists only while the name is released, so a
//! re-registration drops it. The address index (`project_address_name_index`) already lists
//! every address a name's retained registration events named, the last registrant included, so
//! the read composes the address's indexed names and keeps those whose lapsed registrant is the
//! address. It never feeds `owner`, `manager` or `registrant`.
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L341-L362 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
//!
//! Rows sort by the served expiry, then namespace, name and namehash; a row without an expiry (an
//! unregistered ENSv2 name) sorts after every dated row ascending and before them descending,
//! and an expiry window leaves it out. A page is read in one snapshot.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};

use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListOrder, NameCurrentRow,
    families::name::{CoverageShape, load_composed, read_snapshot, servable_publication},
};

/// What a former-registrant page selects besides its order and position.
#[derive(Clone, Copy, Debug)]
pub struct FormerRegistrantFilter<'a> {
    pub address: &'a str,
    pub namespace: Option<&'a str>,
    /// Inclusive lower bound on the served expiry.
    pub expires_after: Option<OffsetDateTime>,
    /// Exclusive upper bound on the served expiry.
    pub expires_before: Option<OffsetDateTime>,
}

/// One page of former-registrant rows and the position after its last row, when more follow.
#[derive(Clone, Debug)]
pub struct FormerRegistrantPage {
    pub rows: Vec<NameCurrentRow>,
    pub next_cursor: Option<NameCurrentListCursor>,
}

/// The composed name the address formerly held, with its served expiry.
struct Held {
    row: NameCurrentRow,
    expiry: Option<OffsetDateTime>,
}

/// The sort key: undated rows after dated ones ascending, then expiry, namespace, name,
/// namehash.
type Key = (bool, Option<OffsetDateTime>, String, String, String);

impl Held {
    fn key(&self) -> Key {
        (
            self.expiry.is_none(),
            self.expiry,
            self.row.namespace.clone(),
            self.row.normalized_name.clone(),
            self.row.namehash.clone(),
        )
    }
}

fn cursor_key(cursor: &NameCurrentListCursor) -> Result<Key> {
    let NameCurrentListCursorValue::Timestamp(expiry) = cursor.sort_value else {
        anyhow::bail!("a former-registrant cursor must carry an expiry position");
    };
    Ok((
        expiry.is_none(),
        expiry,
        cursor.namespace.clone(),
        cursor.normalized_name.clone(),
        cursor.namehash.clone(),
    ))
}

/// The lapsed registrant of a composed row, lower-cased.
pub fn lapsed_registrant(row: &NameCurrentRow) -> Option<String> {
    row.declared_summary
        .pointer("/registration/lapsed_registration/registrant")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
}

/// The served registration expiry of a composed row, when it is a whole second in range.
fn served_expiry(row: &NameCurrentRow) -> Option<OffsetDateTime> {
    let seconds = row
        .declared_summary
        .pointer("/registration/expiry")
        .and_then(Value::as_i64)?;
    OffsetDateTime::from_unix_timestamp(seconds).ok()
}

/// The page of names `filter.address` formerly held.
pub async fn load_family_former_registrant_page(
    pool: &PgPool,
    filter: &FormerRegistrantFilter<'_>,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<FormerRegistrantPage> {
    let after = cursor.map(cursor_key).transpose()?;
    let address = filter.address.to_ascii_lowercase();
    let mut snapshot = read_snapshot(pool).await?;
    let indexed: Vec<(String, String)> = sqlx::query_as(
        "/* storage:families.records.former_registrant_index */
         SELECT DISTINCT indexed.chain_id, indexed.logical_name_id
         FROM bigname_phase.project_address_name_index indexed
         WHERE indexed.address = $1
           AND ($2::text IS NULL OR EXISTS (
               SELECT 1 FROM bigname_phase.name_surfaces surface
               WHERE surface.logical_name_id = indexed.logical_name_id
                 AND surface.namespace = $2))",
    )
    .bind(&address)
    .bind(filter.namespace)
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
                .filter(|row| lapsed_registrant(row).as_deref() == Some(address.as_str()))
                .map(|row| Held {
                    expiry: served_expiry(&row),
                    row,
                }),
        );
    }
    snapshot.commit().await?;

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
    Ok(FormerRegistrantPage {
        rows: held.into_iter().map(|held| held.row).collect(),
        next_cursor,
    })
}
