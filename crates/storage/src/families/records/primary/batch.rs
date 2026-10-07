//! Shared-snapshot tuple selection and hydration for a bounded reverse lookup chunk.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde_json::json;
use sqlx::{PgConnection, Row};

use super::{DEFAULT_COIN_TYPE, addr_reverse_has_resolver, apply_hydration, has_resolver, stamp};
use crate::{
    PrimaryNameCurrentSnapshot, ReverseIdentityStorageInput,
    families::{
        name::servable_publication,
        records::reverse::batch::{self, TupleKey},
    },
};

pub(crate) async fn load(
    conn: &mut PgConnection,
    inputs: &[ReverseIdentityStorageInput],
    namespaces: &[String],
    selected_chains: Option<&[String]>,
) -> Result<Vec<BTreeMap<(String, String), PrimaryNameCurrentSnapshot>>> {
    let mut wanted = BTreeSet::new();
    for input in inputs {
        for namespace in namespaces {
            wanted.insert((
                input.address.to_ascii_lowercase(),
                namespace.clone(),
                input.coin_type.clone(),
            ));
            if input.coin_type == "60" {
                wanted.insert((
                    input.address.to_ascii_lowercase(),
                    namespace.clone(),
                    DEFAULT_COIN_TYPE.to_owned(),
                ));
            }
        }
    }
    let requests: Vec<_> = wanted
        .into_iter()
        .map(|(address, namespace, coin_type)| {
            json!({
                "address": address, "namespace": namespace, "coin_type": coin_type,
            })
        })
        .collect();
    let keys: Vec<TupleKey> = sqlx::query_as(
        "/* storage:families.records.primary_tuple_chains_batch */
         SELECT DISTINCT ON (tuple.address, tuple.namespace, tuple.coin_type)
                tuple.chain_id, tuple.address, tuple.namespace, tuple.coin_type
         FROM jsonb_to_recordset($1::jsonb) AS request(address text, namespace text, coin_type text)
         JOIN bigname_phase.project_reverse_tuple tuple USING (address, namespace, coin_type)
         WHERE tuple.reverse_position IS NOT NULL
           AND ($2::text[] IS NULL OR tuple.chain_id = ANY($2))
         ORDER BY tuple.address, tuple.namespace, tuple.coin_type, tuple.chain_id",
    )
    .bind(json!(requests))
    .bind(selected_chains)
    .fetch_all(&mut *conn)
    .await?;
    let mut publications = BTreeMap::new();
    for (chain, ..) in &keys {
        if !publications.contains_key(chain) {
            publications.insert(chain.clone(), servable_publication(conn, chain).await?);
        }
    }
    let mut claims = batch::load(conn, &keys).await?;
    let requests: Vec<_> = claims.iter().map(|(key, claim)| json!({
        "chain_id": key.0, "address": key.1, "namespace": key.2, "coin_type": key.3,
        "reverse_node": claim.snapshot.row.claim_provenance.get("reverse_node").and_then(serde_json::Value::as_str),
        "resolver_address": claim.snapshot.row.claim_provenance.get("resolver_address").and_then(serde_json::Value::as_str),
    })).collect();
    let hydrated = sqlx::query(
        "/* storage:families.records.primary_hydration_batch */
         SELECT tuple.chain_id, tuple.address, tuple.namespace, tuple.coin_type,
                tuple.hydrated_name, tuple.attempt_block, tuple.attempt_hash, tuple.baseline
         FROM jsonb_to_recordset($1::jsonb) AS request(
             chain_id text, address text, namespace text, coin_type text,
             reverse_node text, resolver_address text)
         JOIN bigname_phase.project_reverse_tuple tuple USING (chain_id, address, namespace, coin_type)
         WHERE tuple.hydrated_name IS NOT NULL AND tuple.baseline IS NOT NULL
           AND (tuple.baseline ->> 'reverse_node') IS NOT DISTINCT FROM request.reverse_node
           AND (tuple.baseline ->> 'resolver_address') IS NOT DISTINCT FROM request.resolver_address
           AND EXISTS (SELECT 1 FROM bigname_phase.chain_lineage lineage
                       WHERE lineage.chain_id = tuple.chain_id
                         AND lineage.block_number = tuple.attempt_block
                         AND lineage.block_hash = tuple.attempt_hash
                         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
    ).bind(json!(requests)).fetch_all(&mut *conn).await?;
    for (key, claim) in &mut claims {
        stamp(&mut claim.snapshot, &publications[&key.0]);
    }
    for row in hydrated {
        let key: TupleKey = (
            row.try_get("chain_id")?,
            row.try_get("address")?,
            row.try_get("namespace")?,
            row.try_get("coin_type")?,
        );
        if let Some(claim) = claims.get_mut(&key) {
            claim.claim_value_empty = apply_hydration(&key.0, &mut claim.snapshot, &row)?;
        }
    }
    let mut tuples = BTreeMap::new();
    for ((_, address, namespace, coin_type), claim) in claims {
        tuples.insert((address, namespace, coin_type), claim);
    }
    let mut out = Vec::with_capacity(inputs.len());
    for input in inputs {
        let address = input.address.to_ascii_lowercase();
        let mut group = BTreeMap::new();
        for namespace in namespaces {
            let loaded = tuples.get(&(address.clone(), namespace.clone(), input.coin_type.clone()));
            let addr_has_resolver = loaded.is_some_and(|claim| has_resolver(&claim.snapshot));
            let names_addr_reverse =
                addr_has_resolver && loaded.is_some_and(|claim| !claim.claim_value_empty);
            let mut claim = loaded.map(|claim| claim.snapshot.clone());
            if input.coin_type == "60"
                && !names_addr_reverse
                && let Some(fallback) = tuples.get(&(
                    address.clone(),
                    namespace.clone(),
                    DEFAULT_COIN_TYPE.to_owned(),
                ))
            {
                let mut fallback = fallback.snapshot.clone();
                fallback.row.coin_type = input.coin_type.clone();
                fallback.default_past_resolver = if loaded.is_some() {
                    addr_has_resolver
                } else {
                    addr_reverse_has_resolver(conn, &fallback, &address, namespace).await?
                };
                claim = Some(fallback);
            }
            if let Some(claim) = claim {
                group.insert((namespace.clone(), input.coin_type.clone()), claim);
            }
        }
        out.push(group);
    }
    Ok(out)
}
