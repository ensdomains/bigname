//! The resources whose records may resolve to an address, for the inverse address read.
//!
//! The derived inverse address index (F14) keeps an address only for a value positioned after its
//! partition's latest version change by block, transaction and log, and reads only a value row's
//! own `value`. The forward read can still serve a value the index drops: a later selected link
//! wins the combined version boundary and lifts the cutoff, a value that shares the version's
//! block, transaction and log comes after it by event identity, an `AddressChanged` value is kept
//! only as `address_bytes_hex` or as the `sibling_value` of a coin-60 pair. So the candidates are
//! the index rows together with every retained F6 and F7 address value that names the address in
//! any of those columns, with no version cutoff: a superset, which the forward inventory assembly
//! then narrows to what is served. Which candidates only the retained values found is kept, per
//! resource and coin type, so the harness can name each entry the index alone would miss.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// A resource whose records may answer for the address.
#[derive(Clone, Debug, Default)]
pub(crate) struct Candidate {
    /// The coin types the derived index found the resource through.
    pub(crate) indexed_coin_types: BTreeSet<String>,
}

/// `address` as the text a stored JSON value holds it in, lower-cased.
const VALUE_TEXT: &str = "lower(CASE WHEN jsonb_typeof(%) = 'string' THEN % #>> '{}'
                                   ELSE COALESCE(% ->> 'value', % ->> 'bytes') END)";

fn value_text(column: &str) -> String {
    VALUE_TEXT.replace('%', column)
}

/// The candidate resources of `address` for `coin_types`, keyed by chain and resource.
pub(crate) async fn candidate_resources(
    pool: &PgPool,
    address: &str,
    coin_types: &[String],
) -> Result<BTreeMap<(String, Uuid), Candidate>> {
    let retained = |table: &str, key: &str| {
        format!(
            "SELECT chain_id, resolver_address, {key},
                    selector_key::numeric::text AS coin_type, false AS indexed
             FROM bigname_phase.{table}
             WHERE record_family = 'addr' AND selector_key ~ '^[0-9]{{1,30}}$'
               AND selector_key::numeric::text = ANY($2::text[])
               AND $1 IN ({}, {}, lower(address_bytes_hex))",
            value_text("value"),
            if table == "project_node_record_value" {
                value_text("sibling_value")
            } else {
                "NULL".to_owned()
            }
        )
    };
    let sql = format!(
        "WITH keys AS (
             SELECT chain_id, resolver_address, node, NULL::text AS record_id, coin_type,
                    true AS indexed
             FROM bigname_phase.project_address_record_node_index
             WHERE address = $1 AND coin_type = ANY($2::text[])
             UNION ALL
             SELECT chain_id, resolver_address, NULL, record_id, coin_type, true
             FROM bigname_phase.project_address_record_id_index
             WHERE address = $1 AND coin_type = ANY($2::text[])
             UNION ALL
             {}
             UNION ALL
             {}
         ),
         mirrors AS (
             SELECT chain_id, resolver_address FROM bigname_phase.resolver_current
             WHERE declared_summary #>> '{{classification,role}}' = 'ensv1_mirror_resolver'
             UNION
             SELECT chain_id, resolver_address
             FROM bigname_phase.project_resolver_classification
             WHERE classification ->> 'role' = 'ensv1_mirror_resolver'
         )
         SELECT pointer.chain_id, pointer.resource_id, keys.coin_type, bool_or(keys.indexed)
                    AS indexed
         FROM keys
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = keys.chain_id
          AND (
              (keys.node IS NOT NULL AND pointer.namehash = keys.node
               AND (pointer.resolver_address = keys.resolver_address
                    OR pointer.resolver_address IN (
                        SELECT mirror.resolver_address FROM mirrors mirror
                        WHERE mirror.chain_id = keys.chain_id)))
              OR (keys.record_id IS NOT NULL
                  AND pointer.resolver_address = keys.resolver_address)
          )
         GROUP BY pointer.chain_id, pointer.resource_id, keys.coin_type",
        retained("project_node_record_value", "node, NULL::text AS record_id"),
        retained("project_record_id_value", "NULL::text, record_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(address)
        .bind(coin_types)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to find the family candidates of {address}"))?;
    let mut candidates: BTreeMap<(String, Uuid), Candidate> = BTreeMap::new();
    for row in rows {
        let candidate = candidates
            .entry((row.try_get("chain_id")?, row.try_get("resource_id")?))
            .or_default();
        if row.try_get::<bool, _>("indexed")? {
            candidate
                .indexed_coin_types
                .insert(row.try_get("coin_type")?);
        }
    }
    Ok(candidates)
}
