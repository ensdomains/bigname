//! Exact physical entry and its current pointers. Registered generations have resource identities;
//! the initial reservation has token/resource generation zero. Clear events remain clear.
use crate::{families::name::FamilyPublication, identity::ens_v2_registry_resource_id};
use alloy_primitives::B256;
use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
pub(super) struct Entry {
    pub instance: Option<Uuid>,
    pub expiry: Option<i64>,
    pub missing: bool,
    pub known: bool,
    pub resolver: Option<String>,
    pub subregistry: Option<String>,
}
impl Entry {
    pub fn expired(&self, clock: i64) -> bool {
        self.expiry.is_some_and(|expiry| expiry <= clock)
    }
    pub fn deadline(&self, clock: i64) -> Option<i64> {
        self.expiry.filter(|expiry| *expiry > clock)
    }
}

pub(super) async fn load_entry(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    registry: &str,
    label: &str,
) -> Result<Entry> {
    let mut word = label.parse::<B256>()?.0;
    word[28..].fill(0);
    let key = format!("{:#x}", B256::from(word));
    let row = sqlx::query(
        "/* storage:families.name.resolution_entry */
        SELECT registry_contract_instance_id, token_id, resource_id, status, expiry::text
        FROM bigname_phase.project_ens_v2_entry_owner
        WHERE chain_id = $1 AND registry = $2 AND entry_key = $3",
    )
    .bind(&publication.chain_id)
    .bind(registry)
    .bind(&key)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(Entry {
            missing: true,
            ..Entry::default()
        });
    };
    let instance = row
        .try_get::<Option<String>, _>("registry_contract_instance_id")?
        .and_then(|id| id.parse().ok());
    let expiry = row
        .try_get::<Option<String>, _>("expiry")?
        .and_then(|expiry| {
            expiry
                .parse::<u64>()
                .ok()
                .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
        });
    let status: String = row.try_get("status")?;
    let mut entry = Entry {
        instance,
        expiry,
        ..Entry::default()
    };
    if status == "unregistered" || entry.expired(publication.timestamp_seconds()) {
        entry.known = true;
        return Ok(entry);
    }
    if !matches!(status.as_str(), "registered" | "reserved") || expiry.is_none() {
        return Ok(entry);
    }
    let resource = row.try_get::<Option<Uuid>, _>("resource_id")?.or_else(|| {
        let token: String = row.try_get("token_id").ok()?;
        if status == "reserved" && token == key {
            Some(ens_v2_registry_resource_id(
                &publication.chain_id,
                instance?,
                &token,
            ))
        } else {
            None
        }
    });
    let Some(resource) = resource else {
        return Ok(entry);
    };
    // Resource-history indexes bound these probes to one physical entry. A complete registered
    // generation starts with zero pointers; nonzero constructor arguments emit watched updates.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L495-L513 @ ens_v2_sepolia_20261001@07e55a05)
    let pointer = sqlx::query("/* storage:families.name.resolution_entry_pointers */
        SELECT (SELECT resolver_address FROM bigname_phase.project_resource_pointer
                WHERE chain_id = $1 AND resource_id = $2) AS resolver,
               (SELECT after_state ->> 'subregistry' FROM bigname_phase.normalized_events
                WHERE chain_id = $1 AND resource_id = $2 AND event_kind = 'SubregistryChanged'
                  AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                  AND consumer_visibility = 'activated'
                  AND canonicality_state IN ('canonical','safe','finalized') AND block_number <= $3
                ORDER BY block_number DESC, transaction_index DESC NULLS LAST,
                         log_index DESC NULLS LAST, event_identity COLLATE \"C\" DESC LIMIT 1) AS subregistry")
        .bind(&publication.chain_id).bind(resource).bind(publication.block_number)
        .fetch_one(conn).await.context("failed to read a resolution path entry's pointers")?;
    entry.resolver = pointer.try_get("resolver")?;
    entry.subregistry = pointer.try_get("subregistry")?;
    entry.known = true;
    Ok(entry)
}
