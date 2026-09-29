//! The Universal Resolver proxy family: per declared `ens_execution` proxy, the implementation its
//! latest `Upgraded` installed, as the adapter classified it against the manifest
//! (adapters schema_v2/protocol/universal_resolver.rs). The composed name reader follows the
//! client-facing proxy's chain through these rows to decide whether a block resolves through
//! ENSv2 (storage families/control/cutover.rs).
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, Preload, current, key_of, load_rows, put, raw_lower, raw_text, set},
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

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let table = &tables::UNIVERSAL_RESOLVER_PROXY;
    let relevant = upgrades(events);
    let keys = relevant
        .iter()
        .map(|(_, proxy)| proxy_key(&chain, proxy))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, proxy) in relevant {
        let Some(implementation) = raw_lower(&event.after, "implementation") else {
            continue;
        };
        let kind = raw_text(&event.after, "implementation_kind")
            .filter(|kind| {
                matches!(
                    kind.as_str(),
                    "admitted_universal_resolver" | "universal_resolver_proxy"
                )
            })
            .unwrap_or_else(|| "other".to_owned());
        let mut row = current(rows, table, &proxy_key(&chain, &proxy));
        set(
            &mut row,
            "proxy_role",
            raw_text(&event.after, "proxy_role").map_or(Value::Null, Value::String),
        );
        set(&mut row, "implementation", implementation);
        set(&mut row, "implementation_kind", kind);
        put(rows, table, row, event)?;
    }
    Ok(())
}

/// The names whose served expiry a change of the Universal Resolver proxies can move: every name
/// with an ENSv2 reservation. A reserved `.eth` name serves its ENSv1 lease's expiry before the
/// cutover and the reservation's after it (storage families/control/lifecycle/expiry.rs), so a
/// block that changed a proxy row recomposes their summaries. Empty for any other block: the
/// guard reads the block's journal by its primary key, and the scan runs only past it.
pub(super) async fn cutover_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
) -> Result<Vec<String>> {
    sqlx::query_scalar(CUTOVER_NAMES)
        .bind(chain_id)
        .bind(number)
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
    SELECT DISTINCT COALESCE(event.decoded_logical_name_id, event.original_logical_name_id)
    FROM project_lifecycle_event event
    WHERE EXISTS (
            SELECT 1 FROM project_family_undo undo
            WHERE undo.chain_id = $1 AND undo.block_number = $2
              AND undo.family = 'project_universal_resolver_proxy'
          )
      AND event.chain_id = $1
      AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
      AND event.event_kind = 'RegistrationReserved'
      AND COALESCE(event.decoded_logical_name_id, event.original_logical_name_id) IS NOT NULL
"#;
