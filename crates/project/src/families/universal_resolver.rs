//! The Universal Resolver proxy family: per declared `ens_execution` proxy, the implementation its
//! latest `Upgraded` installed. Its role and implementation classification follow the active
//! manifest snapshot captured by the family run, so a retired declaration remains replayable
//! without remaining the client-facing entrypoint. The rows are for monitoring only: the
//! phase-runner reports where the client-facing proxy forwards (storage resolution_state.rs).
//! No name reads them, and the cutover is the ENSv2 root registry's admission (storage
//! families/control/cutover.rs).
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, Preload, current, in_family, key_of, load_rows, raw_lower, set},
    store::{Row, RowSet},
    tables,
};
use crate::{ProjectError, Result};

/// The family whose `Upgraded` events this family keeps.
pub(crate) const FAMILY: &str = "ens_execution";

/// A Universal Resolver proxy's `Upgraded` with its proxy address.
fn upgrades(events: &[BlockEvent]) -> Vec<(&BlockEvent, String)> {
    events
        .iter()
        .filter(|event| event.event_kind == "Upgraded" && event.source_family == FAMILY)
        .filter_map(|event| Some((event, raw_lower(&event.after, "proxy_address")?)))
        .collect()
}

fn proxy_key(chain: &Value, proxy: &str) -> Row {
    key_of(
        &tables::UNIVERSAL_RESOLVER_PROXY,
        [chain.clone(), json!(proxy)],
    )
}

pub(super) fn preload(chain: &Value, events: &[BlockEvent], into: &mut Preload) {
    into.add(
        &tables::UNIVERSAL_RESOLVER_PROXY,
        upgrades(events)
            .iter()
            .map(|(_, proxy)| proxy_key(chain, proxy)),
    );
}

/// Current declarations, from the same captured manifest set that classifies ordinary resolvers.
/// Historical upgrade events retain their observed role, but only these declarations may select
/// the entrypoint or admit another proxy hop at this publication.
#[derive(Default)]
struct Authority {
    proxies: BTreeMap<String, String>,
    implementations: BTreeSet<String>,
    starts_here: bool,
}

impl Authority {
    fn at(context: &Context<'_>) -> Self {
        let mut authority = Self::default();
        for manifest in context.manifests.rows.as_array().into_iter().flatten() {
            if manifest["source_family"] != FAMILY {
                continue;
            }
            let payload = &manifest["manifest_payload"];
            authority.implementations.extend(
                payload["universal_resolver_implementations"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_ascii_lowercase),
            );
            for declaration in payload["contracts"].as_array().into_iter().flatten() {
                let Some(role @ ("universal_resolver" | "universal_resolver_managed")) =
                    declaration["role"].as_str()
                else {
                    continue;
                };
                let start = declaration["start_block"].as_i64().unwrap_or(0);
                authority.starts_here |= start == context.block.number;
                if start <= context.block.number
                    && let Some(address) = declaration["address"].as_str()
                {
                    authority
                        .proxies
                        .insert(address.to_ascii_lowercase(), role.to_owned());
                }
            }
        }
        authority
    }

    fn classify(&self, row: &mut Row) {
        let role = row
            .get("proxy_address")
            .and_then(Value::as_str)
            .and_then(|proxy| self.proxies.get(proxy))
            .cloned();
        set(row, "proxy_role", role.map_or(Value::Null, Value::String));
        let implementation = row
            .get("implementation")
            .and_then(Value::as_str)
            .unwrap_or("");
        let kind = if self.proxies.contains_key(implementation) {
            "universal_resolver_proxy"
        } else if self.implementations.contains(implementation) {
            "admitted_universal_resolver"
        } else {
            "other"
        };
        set(row, "implementation_kind", kind);
    }
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let table = &tables::UNIVERSAL_RESOLVER_PROXY;
    let relevant = upgrades(events);
    let authority = Authority::at(context);
    let mut touched: BTreeSet<String> = relevant.iter().map(|(_, proxy)| proxy.clone()).collect();
    if context.manifests_changed || authority.starts_here {
        // A declaration can rotate with no new Upgraded. Include both stored rows and rows an
        // earlier block of this rebuild range folded, preserving their upgrade positions.
        let stored: Vec<String> = sqlx::query_scalar(
            "/* project:families.universal_resolver.stored */ SELECT proxy_address
             FROM project_universal_resolver_proxy WHERE chain_id = $1",
        )
        .bind(context.chain_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read Universal Resolver proxies", error)
        })?;
        touched.extend(stored);
        touched.extend(
            rows.overlay(table, Vec::new(), |_| true)
                .iter()
                .filter_map(|row| row.get("proxy_address").and_then(Value::as_str))
                .map(str::to_owned),
        );
    }
    load_rows(
        transaction,
        rows,
        table,
        touched
            .iter()
            .map(|proxy| proxy_key(&chain, proxy))
            .collect(),
    )
    .await?;
    for (event, proxy) in relevant {
        let Some(implementation) = raw_lower(&event.after, "implementation") else {
            continue;
        };
        let mut row = current(rows, table, &proxy_key(&chain, &proxy));
        set(&mut row, "implementation", implementation);
        event.write_position(&mut row);
        authority.classify(&mut row);
        rows.put(table, row).map_err(in_family(table.name))?;
    }
    for proxy in touched {
        if let Some(mut row) = rows.get(table, &proxy_key(&chain, &proxy)).cloned() {
            authority.classify(&mut row);
            rows.put(table, row).map_err(in_family(table.name))?;
        }
    }
    Ok(())
}
