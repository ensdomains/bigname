//! The resources whose records may resolve to an address, for the inverse address read.
//!
//! The derived inverse address index (F14) keeps an address only for a value positioned after its
//! partition's latest version change in the canonical order (event identity included); it reads
//! the served payload, the `AddressChanged` half of a coin-60 pair, from `value` or raw address
//! bytes. The forward read can still serve a value the index drops, because a later selected link
//! wins the combined version boundary and lifts the cutoff. So the candidates are the index rows
//! together with every retained F6 and F7 address value that names the address in any stored
//! shape (`value`, `address_bytes_hex`, and for a pair `sibling_value` and
//! `sibling_address_bytes_hex`), with no version cutoff and no arm test, which the forward
//! inventory assembly then narrows to what is served.
//!
//! A retained value reaches the resources whose pointer can admit it: a node-keyed value the
//! pointers at its node and resolver (or at a mirror resolver for that node), a named value also
//! the pointers of its own resource or of its logical name's namehash at its resolver (the named
//! arm admits by logical name with no node test), and a record-id value every pointer at its
//! resolver. Those are the conditions under which the forward read's arms and link selection
//! (`rows.rs`, `links.rs`) can load the value, so every resource that serves the address is a
//! candidate. Which candidates only the retained values found is kept, per resource and coin type,
//! so the harness can name each entry the index alone would miss.
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
               AND $1 IN ({}, {}, lower(address_bytes_hex), {})",
            value_text("value"),
            if table == "project_node_record_value" {
                value_text("sibling_value")
            } else {
                "NULL".to_owned()
            },
            if table == "project_node_record_value" {
                "lower(sibling_address_bytes_hex)"
            } else {
                "NULL"
            }
        )
    };
    let sql = format!(
        "WITH keys AS (
             SELECT chain_id, resolver_address, node, NULL::text AS record_id,
                    NULL::uuid AS named_resource, NULL::text AS named_namehash, coin_type,
                    true AS indexed
             FROM bigname_phase.project_address_record_node_index
             WHERE address = $1 AND coin_type = ANY($2::text[])
             UNION ALL
             SELECT chain_id, resolver_address, NULL, record_id, NULL, NULL, coin_type, true
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
              ((pointer.namehash = keys.node OR pointer.resource_id = keys.named_resource
                OR pointer.namehash = keys.named_namehash)
               AND (pointer.resolver_address = keys.resolver_address
                    OR pointer.resolver_address IN (
                        SELECT mirror.resolver_address FROM mirrors mirror
                        WHERE mirror.chain_id = keys.chain_id)))
              OR (keys.record_id IS NOT NULL
                  AND pointer.resolver_address = keys.resolver_address)
          )
         GROUP BY pointer.chain_id, pointer.resource_id, keys.coin_type",
        retained(
            "project_node_record_value",
            "node, NULL::text AS record_id,
             CASE WHEN arm = 'named' THEN resource_id END AS named_resource,
             CASE WHEN arm = 'named' THEN lower(split_part(arm_identity, ':', 2)) END
                 AS named_namehash"
        ),
        retained(
            "project_record_id_value",
            "NULL::text, record_id, NULL::uuid, NULL::text"
        ),
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
