//! The `coin_type=evm` read over `address_records_current`: names whose stored `addr:<coin_type>`
//! record for any EVM coin type holds the address, one row per dedupe group, with every matched
//! coin type retained.
//!
//! The EVM coin types are `60` and `[2^31, 2^32)`, the set ENSIP-19 treats as EVM, including the
//! default coin type `2^31` itself
//! (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f).
//! The read enumerates stored rows: the ENSIP-19 default record matches once, under its own coin
//! type, and is never expanded into the chains it would answer.
//!
//! Matches are aggregated from the full filtered set before the representative row is chosen,
//! so a representative never hides another coin type the group matched.

use anyhow::Result;
use bigname_domain::resolver_read::{ENSIP19_DEFAULT_COIN_TYPE, ETH_COIN_TYPE};
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgRow};

use super::{
    resolves_to::{
        AddressRecordCurrentEntry, AddressRecordsCoinSelector, AddressRecordsFilter,
        load_sorted_entries,
    },
    types::{
        AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
        AddressNamesCurrentSortedCursor,
    },
};

/// Exclusive upper bound of the ENSIP-11 coin-type range (`2^32`).
const EVM_COIN_TYPE_END: u64 = 1 << 32;

/// One EVM coin type whose stored record matched the address, and the stored record key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressRecordCoinMatch {
    pub coin_type: String,
    pub record_key: String,
}

/// One row of a `coin_type=evm` page.
///
/// `entry` is the dedupe group's representative; its `coin_type` and `record_key` are that
/// representative row's own stored values. `resolutions` is the group's union of matches, ascending
/// by coin type. `representative_coin_types` are the coin types the representative name itself
/// matched; with `dedupe=name` they equal the `resolutions` coin types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressRecordEvmEntry {
    pub entry: AddressRecordCurrentEntry,
    pub resolutions: Vec<AddressRecordCoinMatch>,
    pub representative_coin_types: Vec<String>,
}

/// Bounded sorted page of names resolving to an address on any EVM coin type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressRecordsCurrentEvmPage {
    pub entries: Vec<AddressRecordEvmEntry>,
    pub next_cursor: Option<AddressNamesCurrentSortedCursor>,
}

/// Load a bounded page of current names whose stored `addr:<coin_type>` record for any EVM coin
/// type resolves to `address`. Arguments mean what they mean for
/// [`super::load_address_records_current_page`].
#[allow(clippy::too_many_arguments)]
pub async fn load_address_records_current_evm_page(
    pool: &PgPool,
    address: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    authority_arm: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentEvmPage> {
    let filter = AddressRecordsFilter {
        address,
        coins: AddressRecordsCoinSelector::Evm,
        namespaces,
        dedupe_by,
        q,
        authority_arm,
    };
    let (rows, next_cursor) =
        load_sorted_entries(pool, &filter, sort, order, cursor, page_size).await?;
    let entries = rows
        .into_iter()
        .map(|row| {
            let facets = row
                .evm
                .ok_or_else(|| anyhow::anyhow!("evm address_records_current row lacks facets"))?;
            Ok(AddressRecordEvmEntry {
                entry: row.entry,
                resolutions: facets.resolutions,
                representative_coin_types: facets.representative_coin_types,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(AddressRecordsCurrentEvmPage {
        entries,
        next_cursor,
    })
}

/// Test support: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of the exact statements a
/// `coin_type=evm` request runs, the continuation check first when `cursor` is present, then the
/// page statement.
#[cfg(any(test, feature = "test-support"))]
pub async fn explain_address_records_current_evm_page_for_test(
    pool: &PgPool,
    address: &str,
    dedupe_by: AddressNamesCurrentDedupe,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<Vec<serde_json::Value>> {
    use super::resolves_to::{push_cursor_exists_statement, push_page_statement};
    const EXPLAIN: &str = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ";
    let filter = AddressRecordsFilter {
        address,
        coins: AddressRecordsCoinSelector::Evm,
        namespaces: None,
        dedupe_by,
        q: None,
        authority_arm: None,
    };
    let mut plans = Vec::new();
    if let Some(cursor) = cursor {
        let mut builder = QueryBuilder::<Postgres>::new(EXPLAIN);
        push_cursor_exists_statement(&mut builder, &filter, sort, cursor);
        plans.push(builder.build_query_scalar().fetch_one(pool).await?);
    }
    let mut builder = QueryBuilder::<Postgres>::new(EXPLAIN);
    let page_limit = i64::try_from(page_size)? + 1;
    push_page_statement(&mut builder, &filter, sort, order, cursor, page_limit);
    plans.push(builder.build_query_scalar().fetch_one(pool).await?);
    Ok(plans)
}

/// Test support: the text of the first-page `coin_type=evm` statement, so a test can pin its
/// shape (for example the materialized address fence) independently of any plan.
#[cfg(any(test, feature = "test-support"))]
pub fn address_records_current_evm_page_sql_for_test(
    address: &str,
    dedupe_by: AddressNamesCurrentDedupe,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
) -> String {
    use super::resolves_to::push_page_statement;
    let filter = AddressRecordsFilter {
        address,
        coins: AddressRecordsCoinSelector::Evm,
        namespaces: None,
        dedupe_by,
        q: None,
        authority_arm: None,
    };
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_page_statement(&mut builder, &filter, sort, order, None, 51);
    builder.sql().to_owned()
}

pub(super) struct EvmFacets {
    pub(super) resolutions: Vec<AddressRecordCoinMatch>,
    pub(super) representative_coin_types: Vec<String>,
}

/// The EVM coin-type predicate on `arc`. The bounds are domain constants, not caller input.
fn push_evm_coin_predicate(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(format!(
        " AND (arc.coin_type = '{ETH_COIN_TYPE}' \
           OR (arc.coin_type::numeric >= {ENSIP19_DEFAULT_COIN_TYPE} \
               AND arc.coin_type::numeric < {EVM_COIN_TYPE_END}))"
    ));
}

/// The address's stored EVM rows, read first and materialized so the address-leading index is the
/// access path whatever the planner estimates for the canonicality joins that follow. Without the
/// fence a misestimate can drive the read from `resources` through the record-resource index,
/// which visits every address's rows.
pub(super) fn push_evm_address_rows_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
) {
    builder.push(
        r#"evm_address_rows AS MATERIALIZED (
            SELECT arc.*
            FROM bigname_phase.address_records_current arc
            WHERE arc.address = "#,
    );
    builder.push_bind(address);
    push_evm_coin_predicate(builder);
    builder.push("\n        ),\n        ");
}

/// One window pass over the whole `filtered` set: every match is aggregated before the
/// representative is chosen, and no self-join can multiply the work by the number of groups.
/// The group facet may repeat a coin type under `dedupe=registration` when several names in the
/// group matched it; the decoder keeps the first (lowest record key) of each.
pub(super) fn push_evm_entries_ctes(
    builder: &mut QueryBuilder<'_, Postgres>,
    dedupe_by: AddressNamesCurrentDedupe,
) {
    let (group_key, tie_break) = match dedupe_by {
        AddressNamesCurrentDedupe::Surface => {
            ("address, logical_name_id", "matched_coin_type::numeric ASC")
        }
        AddressNamesCurrentDedupe::Resource => (
            "address, resource_id",
            "canonical_display_name ASC, logical_name_id ASC, matched_coin_type::numeric ASC",
        ),
    };
    builder.push(format!(
        r#",
        evm_ranked AS (
            SELECT filtered.*,
                row_number() OVER (PARTITION BY {group_key} ORDER BY {tie_break})
                    AS evm_representative_rank,
                array_agg(matched_coin_type) OVER evm_group AS resolution_coin_types,
                array_agg(record_key) OVER evm_group AS resolution_record_keys,
                array_agg(matched_coin_type) OVER evm_name AS representative_coin_types
            FROM filtered
            WINDOW
                evm_group AS (
                    PARTITION BY {group_key}
                    ORDER BY matched_coin_type::numeric ASC, record_key ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ),
                evm_name AS (
                    PARTITION BY address, resource_id, logical_name_id
                    ORDER BY matched_coin_type::numeric ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
        ),
        entries AS (
            SELECT
                address, logical_name_id, namespace, canonical_display_name, normalized_name,
                namehash, surface_binding_id, authority_resource_id, resource_id,
                record_resource_id, binding_kind, matched_coin_type, record_key, provenance,
                coverage, chain_positions, canonicality_summary, manifest_version,
                last_recomputed_at, resolution_coin_types, resolution_record_keys,
                representative_coin_types
            FROM evm_ranked
            WHERE evm_representative_rank = 1
        )
        "#
    ));
}

pub(super) const EVM_OUTER_COLUMNS: &str = "matched_coin_type, resolution_coin_types, \
    resolution_record_keys, representative_coin_types, ";

pub(super) fn decode_evm_facets(row: &PgRow) -> Result<(String, EvmFacets)> {
    let coin_types = crate::sql_row::get::<Vec<String>>(row, "resolution_coin_types")?;
    let record_keys = crate::sql_row::get::<Vec<String>>(row, "resolution_record_keys")?;
    if coin_types.len() != record_keys.len() || coin_types.is_empty() {
        anyhow::bail!("address_records_current evm row has mismatched resolution facets");
    }
    let mut resolutions = Vec::<AddressRecordCoinMatch>::with_capacity(coin_types.len());
    for (coin_type, record_key) in coin_types.into_iter().zip(record_keys) {
        if resolutions
            .last()
            .is_some_and(|previous| previous.coin_type == coin_type)
        {
            continue;
        }
        resolutions.push(AddressRecordCoinMatch {
            coin_type,
            record_key,
        });
    }
    Ok((
        crate::sql_row::get(row, "matched_coin_type")?,
        EvmFacets {
            resolutions,
            representative_coin_types: crate::sql_row::get(row, "representative_coin_types")?,
        },
    ))
}
