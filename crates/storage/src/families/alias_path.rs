//! The ENSv2 token a requested name path reaches, when that path is not the token's canonical
//! name (docs/glossary.md#alias-path). The path is walked top-down as the Universal Resolver
//! walks it: each registry answers the next registry from its own unexpired entry, and the
//! parent claim is never read.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L280-L283 @ ens_v2_sepolia_20261001@07e55a05)
//!
//! The walk starts at the root registry the family publication was composed with
//! (`FamilyPublication::admission`), as UniversalResolverV2 starts at its root, so a manifest
//! sync reaches the walk only through the redo that republishes. A publication with no admission
//! walks nothing. It costs one statement for the publication, two per hop and two at the leaf.
//! A registry mounted under itself is walked once per label, so a name longer than
//! [`MAX_ALIAS_LABELS`] is not walked at all.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20261001@07e55a05) The entry and pointer statements are twins of the composed name
//! reader's (`name/resolution_path/facts.rs`), which this reader cannot call without changing a
//! hashed file. `alias_path_tests.rs` pins the twins to each other.
use alloy_primitives::B256;
use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::{
    ReadDb,
    families::name::{FamilyPublication, PHYSICAL_POINTER_EVENT_SQL, publication_on},
    identity::ens_v2_registry_resource_id,
    rendered_name::label_hash,
    snapshot_selection::{ChainPosition, ChainPositions, SnapshotSelectionError},
};

const ENS_L1_CHAINS: [&str; 2] = ["ethereum-mainnet", "ethereum-sepolia"];

/// The most labels an alias walk reads. A name with more labels is not walked and runs no
/// statement, so it answers as a name with no alias path. This bounds a walk at
/// `2 * MAX_ALIAS_LABELS + 1` statements, including a path through a cycle.
pub const MAX_ALIAS_LABELS: usize = 32;

pub(crate) const ENTRY_SQL: &str = "/* storage:families.alias_path.entry */
        SELECT registry_contract_instance_id, token_id, resource_id, status, expiry::text
        FROM bigname_phase.project_ens_v2_entry_owner
        WHERE chain_id = $1 AND registry = $2 AND entry_key = $3";

/// The subregistry half of the composed reader's pointer statement, with its physical-pointer
/// predicate left as the placeholder that statement formats in.
pub(crate) const POINTER_TEMPLATE: &str = r#"SELECT event.after_state ->> 'subregistry' FROM bigname_phase.normalized_events event
                WHERE event.chain_id = $1 AND event.resource_id = $2 AND event.event_kind = 'SubregistryChanged'
                  AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                  AND event.consumer_visibility = 'activated'
                  AND event.canonicality_state IN ('canonical','safe','finalized') AND event.block_number <= $3
                  AND {PHYSICAL_POINTER_EVENT_SQL}
                ORDER BY event.block_number DESC, event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST, event.event_identity COLLATE "C" DESC LIMIT 1"#;

fn pointer_sql() -> String {
    format!(
        "/* storage:families.alias_path.entry_pointer */ {}",
        POINTER_TEMPLATE.replace("{PHYSICAL_POINTER_EVENT_SQL}", PHYSICAL_POINTER_EVENT_SQL)
    )
}

/// The latest association names the token's canonical path, and `unbound` says a later release
/// of that path came from its binding: the mount changed or lapsed without naming another path.
/// A lapsed token's own release carries the same reason, so the caller reads `unbound` only for a
/// live token.
const ASSOCIATION_SQL: &str = r#"/* storage:families.alias_path.association */
    SELECT association.logical_name_id,
           EXISTS (
               SELECT 1 FROM bigname_phase.normalized_events event
               WHERE event.chain_id = association.chain_id
                 AND event.resource_id = association.target_resource_id
                 AND event.logical_name_id = association.logical_name_id
                 AND event.event_kind = 'RegistrationReleased'
                 AND event.after_state ->> 'terminal_reason'
                     IN ('registry_name_binding_changed', 'registry_name_binding_expired')
                 AND event.consumer_visibility = 'activated'
                 AND event.canonicality_state IN ('canonical','safe','finalized')
                 AND event.block_number <= $3
                 AND (event.block_number, coalesce(event.transaction_index, -1),
                      coalesce(event.log_index, -1))
                   > (association.block_number, coalesce(association.transaction_index, -1),
                      coalesce(association.log_index, -1))
           ) AS unbound
    FROM bigname_phase.project_lifecycle_association association
    WHERE association.chain_id = $1 AND association.target_resource_id = $2
    ORDER BY association.block_number DESC, association.transaction_index DESC NULLS LAST,
             association.log_index DESC NULLS LAST, association.event_identity COLLATE "C" DESC
    LIMIT 1"#;

/// The token an alias path reaches, named by its canonical name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AliasTarget {
    pub canonical_logical_name_id: String,
    pub resource_id: Uuid,
    /// The leaf entry's expiry when the leaf is a reservation. A reservation's row is bound to
    /// no resource, so its expiry is what ties the row to this reservation. A reserved entry
    /// has no owner (`project_ens_v2_entry_owner` allows an owner only on a registered entry).
    pub reservation_expiry: Option<String>,
}

/// One walk's answer and the statements it ran, which bound the cost a miss adds.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AliasWalk {
    pub target: Option<AliasTarget>,
    pub statements: usize,
}

/// The canonical name of the ENSv2 token `name` reaches, when `name` is not that canonical
/// name. `name` is a route's normalized name, a bracketed label standing for its labelhash, and
/// `logical_name_id` is its name id. The walk reads the family publication the composed rows
/// describe, so a selected position other than the publication is stale, as it is for a
/// composed row (`name_current::snapshot`).
pub async fn resolve_alias_path(
    db: impl Into<ReadDb<'_>>,
    namespace: &str,
    name: &str,
    logical_name_id: &str,
    selected: &ChainPositions,
) -> std::result::Result<AliasWalk, SnapshotSelectionError> {
    if namespace != "ens" || name.split('.').count() > MAX_ALIAS_LABELS {
        return Ok(AliasWalk::default());
    }
    let Some(position) = selected
        .as_map()
        .values()
        .find(|position| ENS_L1_CHAINS.contains(&position.chain_id.as_str()))
    else {
        return Ok(AliasWalk::default());
    };
    let mut snapshot = db.into().snapshot().await.map_err(internal)?;
    let mut walk = Walker {
        conn: &mut snapshot,
        statements: 0,
    };
    let result = walk.resolve(name, logical_name_id, position).await;
    let statements = walk.statements;
    snapshot.close().await.map_err(internal)?;
    Ok(AliasWalk {
        target: result?,
        statements,
    })
}

fn internal(error: anyhow::Error) -> SnapshotSelectionError {
    SnapshotSelectionError::internal(format!("failed to walk an ENSv2 alias path: {error:#}"))
}

struct Walker<'a> {
    conn: &'a mut PgConnection,
    statements: usize,
}

/// A registry entry as the walk reads it: live for a hop, or the leaf's resource.
struct Entry {
    status: String,
    expiry: Option<i64>,
    /// The expiry as stored, which a reservation's row is compared with.
    stored_expiry: Option<String>,
    resource: Option<Uuid>,
}

impl Entry {
    /// Registered or reserved, and unexpired at the publication, as `_isExpired` reads it.
    fn is_live(&self, publication: &FamilyPublication) -> bool {
        matches!(self.status.as_str(), "registered" | "reserved")
            && self
                .expiry
                .is_some_and(|expiry| expiry > publication.timestamp_seconds())
    }
}

impl Walker<'_> {
    async fn resolve(
        &mut self,
        name: &str,
        logical_name_id: &str,
        position: &ChainPosition,
    ) -> std::result::Result<Option<AliasTarget>, SnapshotSelectionError> {
        let chain_id = position.chain_id.as_str();
        self.statements += 1;
        let publication = publication_on(&mut *self.conn, chain_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                SnapshotSelectionError::stale(format!(
                    "name data is unavailable while the families of {chain_id} rebuild"
                ))
            })?;
        // A chain that is not cut over has no ENSv2 path to walk.
        let Some(admission) = publication.admission.clone() else {
            return Ok(None);
        };
        if publication.block_number != position.block_number
            || publication.block_hash != position.block_hash
        {
            return Err(SnapshotSelectionError::stale(
                "name data is unavailable at the selected historical position",
            ));
        }
        let mut registry = admission.root_registry.to_ascii_lowercase();
        // Labels root first, as the walk visits them.
        let labels: Vec<[u8; 32]> = name.split('.').rev().map(label_hash).collect();
        let (leaf, hops) = labels.split_last().expect("a name has a label");
        for label in hops {
            let Some(entry) = self
                .entry(&publication, &registry, label)
                .await
                .map_err(internal)?
            else {
                return Ok(None);
            };
            let live = entry.is_live(&publication);
            let Some(resource) = entry.resource.filter(|_| live) else {
                return Ok(None);
            };
            match self
                .subregistry(&publication, resource)
                .await
                .map_err(internal)?
            {
                Some(next) if !crate::families::records::is_cleared(Some(&next)) => {
                    registry = next.to_ascii_lowercase();
                }
                _ => return Ok(None),
            }
        }
        // The leaf is served as its canonical row serves it, expired or released included.
        let Some(entry) = self
            .entry(&publication, &registry, leaf)
            .await
            .map_err(internal)?
            .filter(|entry| entry.status != "unknown")
        else {
            return Ok(None);
        };
        let Some(resource) = entry.resource else {
            return Ok(None);
        };
        let canonical = self
            .association(&publication, resource, entry.is_live(&publication))
            .await
            .map_err(internal)?;
        Ok(canonical
            .filter(|canonical| canonical != logical_name_id)
            .map(|canonical_logical_name_id| AliasTarget {
                canonical_logical_name_id,
                resource_id: resource,
                reservation_expiry: (entry.status == "reserved")
                    .then_some(entry.stored_expiry)
                    .flatten(),
            }))
    }

    async fn entry(
        &mut self,
        publication: &FamilyPublication,
        registry: &str,
        labelhash: &[u8; 32],
    ) -> Result<Option<Entry>> {
        let mut word = *labelhash;
        word[28..].fill(0);
        let key = format!("{:#x}", B256::from(word));
        self.statements += 1;
        let Some(row) = sqlx::query(ENTRY_SQL)
            .bind(&publication.chain_id)
            .bind(registry)
            .bind(&key)
            .fetch_optional(&mut *self.conn)
            .await
            .context("failed to read an alias path entry")?
        else {
            return Ok(None);
        };
        let instance: Option<Uuid> = row
            .try_get::<Option<String>, _>("registry_contract_instance_id")?
            .and_then(|id| id.parse().ok());
        let stored_expiry = row.try_get::<Option<String>, _>("expiry")?;
        let expiry = stored_expiry
            .as_deref()
            .and_then(|expiry| expiry.parse::<u64>().ok())
            .map(|expiry| i64::try_from(expiry).unwrap_or(i64::MAX));
        let status: String = row.try_get("status")?;
        let token: String = row.try_get("token_id")?;
        // The initial reservation has no resource row: its resource is derived as Interpret
        // derives it.
        let resource = row.try_get::<Option<Uuid>, _>("resource_id")?.or_else(|| {
            (status == "reserved" && token == key)
                .then_some(instance?)
                .map(|instance| {
                    ens_v2_registry_resource_id(&publication.chain_id, instance, &token)
                })
        });
        Ok(Some(Entry {
            status,
            expiry,
            stored_expiry,
            resource,
        }))
    }

    async fn subregistry(
        &mut self,
        publication: &FamilyPublication,
        resource: Uuid,
    ) -> Result<Option<String>> {
        self.statements += 1;
        sqlx::query_scalar::<_, Option<String>>(&pointer_sql())
            .bind(&publication.chain_id)
            .bind(resource)
            .bind(publication.block_number)
            .fetch_optional(&mut *self.conn)
            .await
            .map(Option::flatten)
            .context("failed to read an alias path pointer")
    }

    /// The token's canonical name. A live token whose path was released by its binding has none:
    /// its registry lost its name. A token that lapsed itself keeps its row's name.
    async fn association(
        &mut self,
        publication: &FamilyPublication,
        resource: Uuid,
        token_live: bool,
    ) -> Result<Option<String>> {
        self.statements += 1;
        let row = sqlx::query(ASSOCIATION_SQL)
            .bind(&publication.chain_id)
            .bind(resource)
            .bind(publication.block_number)
            .fetch_optional(&mut *self.conn)
            .await
            .context("failed to read an alias path's canonical name")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let unbound: bool = row.try_get("unbound")?;
        Ok((!(unbound && token_live))
            .then(|| row.try_get("logical_name_id"))
            .transpose()?)
    }
}

#[cfg(test)]
#[path = "alias_path_tests.rs"]
mod tests;
