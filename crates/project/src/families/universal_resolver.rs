//! The Universal Resolver proxy family: per declared `ens_execution` proxy, the implementation its
//! latest `Upgraded` installed. Its role and implementation classification follow the active
//! manifest snapshot captured by the family run, so a retired declaration remains replayable
//! without remaining the client-facing entrypoint. The composed name reader follows the
//! client-facing proxy's chain through these rows to decide whether a block resolves through
//! ENSv2 (storage families/control/cutover.rs).
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

/// A proxy change can move reserved names' served expiry and null other names' resolver.
/// Refresh reservations plus names with active disagreements on this chain, so publication
/// retires newly outdated evidence without scanning every name. A later ENSv2 entry release
/// adds active-evidence descendants of the normal work list's affected parent names. The
/// journal guards admit the candidate scans only for a proxy change or an ENSv2 release.
pub(super) async fn cutover_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
    touched_names: &[String],
) -> Result<Vec<String>> {
    sqlx::query_scalar(CUTOVER_NAMES)
        .bind(chain_id)
        .bind(number)
        .bind(touched_names)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(
                "failed to read the names a Universal Resolver change moves",
                error,
            )
        })
}

const CUTOVER_NAMES: &str = r#"/* project:families.derived.cutover_names */
    SELECT DISTINCT logical_name_id FROM (
        SELECT COALESCE(event.decoded_logical_name_id, event.original_logical_name_id)
               AS logical_name_id
        FROM project_lifecycle_event event
        WHERE event.chain_id = $1
          AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
          AND event.event_kind = 'RegistrationReserved'
        UNION ALL
        SELECT logical_name_id FROM resolution_divergences
        WHERE resolver_chain_id = $1 AND cleared_at IS NULL
    ) candidates
    WHERE logical_name_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM project_family_undo undo
        WHERE undo.chain_id = $1 AND undo.block_number = $2
          AND undo.family = 'project_universal_resolver_proxy'
    )
    UNION
    SELECT DISTINCT evidence.logical_name_id
    FROM resolution_divergences evidence
    JOIN name_surfaces child ON child.logical_name_id = evidence.logical_name_id
    WHERE evidence.resolver_chain_id = $1 AND evidence.cleared_at IS NULL
      AND child.namespace = 'ens' AND cardinality(child.labelhashes) > 2
      AND EXISTS (
          SELECT 1 FROM name_surfaces parent
          WHERE parent.logical_name_id = ANY($3)
            AND parent.namespace = 'ens' AND cardinality(parent.labelhashes) = 2
            -- keccak256 of the label eth: a .eth second-level parent, whether or not its
            -- surface stores the label text.
            AND lower(parent.labelhashes[2]) =
                '0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0'
            AND child.labelhashes[cardinality(child.labelhashes)-1:] = parent.labelhashes
      )
      AND EXISTS (
          SELECT 1 FROM project_family_undo undo
          JOIN project_lifecycle_event event
            ON event.chain_id = $1 AND event.state_kind = undo.key::jsonb ->> 1
           AND event.state_key = undo.key::jsonb ->> 2
           AND event.event_identity = undo.key::jsonb ->> 3
          WHERE undo.chain_id = $1 AND undo.block_number = $2
            AND undo.family = 'project_lifecycle_event'
            AND event.event_kind = 'RegistrationReleased'
            AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
      )
"#;

#[cfg(test)]
mod tests {
    #[test]
    fn the_cutover_parent_test_names_the_eth_labelhash() {
        assert!(super::CUTOVER_NAMES.contains(&format!(
            "'{}'",
            bigname_storage::families::control::lifecycle::ETH_LABELHASH
        )));
    }
}
