//! Historical membership of direct child registration events.
//!
//! One `child_registration_events` row records that a registration event belongs to the history
//! of the name one label above the name the event carried when it happened. The row depends only
//! on that event and the event's own name surface: never on the parent's current subregistry,
//! `children_current`, `name_current`, or current contract address ranges. A released child, an
//! unlinked registry, or a registry that later moved under another parent therefore keeps every
//! row it had. The surface's current `visibility_state` is the one input that can change without
//! a new event: a normalizer recompute can flip it, and the Project redo that recompute stamps
//! rebuilds the rows. See docs/projections.md, "Child registration events".

use alloy_primitives::{B256, keccak256};
use sqlx::{Postgres, QueryBuilder, Transaction};

use crate::{Marker, ProjectError, Result};

/// Parents whose children the table does not list. Every second-level registrar grant is a child
/// of one of them, and name history refuses `include=child_registrations` for both names in every
/// namespace, so the rows would never be served.
pub const EXCLUDED_CHILD_REGISTRATION_PARENTS: &[&str] = &["eth", "base.eth"];

/// Stages the rows derived from this batch's events: every staged event for a full rebuild, the
/// affected range's changed events otherwise.
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    let source = if full_rebuild {
        "project_events"
    } else {
        "project_changed_events"
    };
    for statement in [
        "/* project:builders.child_registrations.build.create_stage_child_registration_events */ CREATE TEMP TABLE project_stage_child_registration_events
         (LIKE child_registration_events INCLUDING DEFAULTS) ON COMMIT DROP",
        "/* project:builders.child_registrations.build.create_child_registration_parents */ CREATE TEMP TABLE project_child_registration_parents (
             child_logical_name_id text PRIMARY KEY,
             parent_logical_name_id text NOT NULL
         ) ON COMMIT DROP",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to create child registration stage", error)
            })?;
    }

    let mut children = QueryBuilder::<Postgres>::new(
        "/* project:builders.child_registrations.build.select_children */ SELECT DISTINCT child.logical_name_id, child.namespace, child.labelhashes",
    );
    push_qualifying_events(&mut children, source, false);
    let children = children
        .build_query_as::<(String, String, Vec<String>)>()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to load child registration candidates", error)
        })?;

    // Postgres has no keccak, so the parent identity is computed here from the child's own label
    // hashes. The parent's surface is not consulted: the row must not depend on whether or when
    // the parent was surfaced.
    let excluded = EXCLUDED_CHILD_REGISTRATION_PARENTS
        .iter()
        .map(|name| namehash(&name_labelhashes(name)))
        .collect::<Vec<_>>();
    let (child_ids, parent_ids): (Vec<String>, Vec<String>) = children
        .into_iter()
        .filter_map(|(child_id, namespace, labelhashes)| {
            let parent = parent_namehash(&labelhashes)?;
            (!excluded.contains(&parent)).then(|| (child_id, format!("{namespace}:{parent:#x}")))
        })
        .unzip();
    sqlx::query(
        "/* project:builders.child_registrations.build.insert_child_registration_parents */ INSERT INTO project_child_registration_parents
         SELECT * FROM unnest($1::text[], $2::text[])",
    )
    .bind(&child_ids)
    .bind(&parent_ids)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage child registration parents", error))?;

    let mut insert = QueryBuilder::<Postgres>::new(
        "/* project:builders.child_registrations.build.insert_stage_child_registration_events */ INSERT INTO project_stage_child_registration_events (
             parent_logical_name_id, event_identity, child_logical_name_id, namespace,
             chain_id, block_number, block_hash, transaction_order_key, log_order_key,
             event_kind, manifest_version, provenance, target_block_number, target_block_hash
         )
         SELECT parent.parent_logical_name_id, event.event_identity, event.logical_name_id,
                child.namespace, event.chain_id, event.block_number, event.block_hash,
                COALESCE(event.transaction_hash, ''), COALESCE(event.log_index, -1),
                event.event_kind, event.manifest_version,
                jsonb_build_object(
                    'normalized_event_id', event.normalized_event_id,
                    'source_family', event.source_family,
                    'derivation_kind', 'child_registration_events_rebuild'
                ), ",
    );
    insert.push_bind(target.number);
    insert.push(", ");
    insert.push_bind(&target.hash);
    push_qualifying_events(&mut insert, source, true);
    insert
        .build()
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to stage child registrations", error))?;
    Ok(())
}

/// Replaces the published rows the batch covers. A full rebuild replaces the chain. Otherwise a
/// row depends only on its own event and that event's surface, so the batch replaces the affected block range, plus rows
/// at or above the range start whose block is no longer readable canonical lineage. Rows above
/// the range stay: an operator redo may end below an already published target.
pub(crate) async fn publish(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    full_rebuild: bool,
    from_block: i64,
    to_block: i64,
) -> Result<u64> {
    let delete = if full_rebuild {
        sqlx::query("/* project:builders.child_registrations.publish.delete_all */ DELETE FROM child_registration_events WHERE chain_id = $1").bind(chain_id)
    } else {
        sqlx::query(
            "/* project:builders.child_registrations.publish.delete_window */ DELETE FROM child_registration_events row
             WHERE row.chain_id = $1
               AND row.block_number >= $2
               AND (
                   row.block_number <= $3
                   OR NOT EXISTS (
                       SELECT 1 FROM chain_lineage lineage
                       WHERE lineage.chain_id = row.chain_id
                         AND lineage.block_number = row.block_number
                         AND lineage.block_hash = row.block_hash
                         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                   )
               )",
        )
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
    };
    delete.execute(&mut **transaction).await.map_err(|error| {
        ProjectError::database("failed to clear child registration scope", error)
    })?;
    let inserted = sqlx::query(
        "/* project:builders.child_registrations.publish.upsert */ INSERT INTO child_registration_events
         SELECT * FROM project_stage_child_registration_events
         ON CONFLICT (parent_logical_name_id, event_identity) DO UPDATE SET
             child_logical_name_id = EXCLUDED.child_logical_name_id,
             namespace = EXCLUDED.namespace,
             chain_id = EXCLUDED.chain_id,
             block_number = EXCLUDED.block_number,
             block_hash = EXCLUDED.block_hash,
             transaction_order_key = EXCLUDED.transaction_order_key,
             log_order_key = EXCLUDED.log_order_key,
             event_kind = EXCLUDED.event_kind,
             manifest_version = EXCLUDED.manifest_version,
             provenance = EXCLUDED.provenance,
             target_block_number = EXCLUDED.target_block_number,
             target_block_hash = EXCLUDED.target_block_hash,
             last_recomputed_at = EXCLUDED.last_recomputed_at",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to publish child registrations", error))?
    .rows_affected();
    Ok(inserted)
}

/// `FROM … WHERE` over the staged events that can be a direct child registration: an activated,
/// readable `registration` event with a chain position and a name, other than the registrar
/// surface snapshot product history already suppresses, whose own surface is an active name with
/// a parent below the root that is not one of the excluded parents.
fn push_qualifying_events(builder: &mut QueryBuilder<'_, Postgres>, source: &str, parents: bool) {
    builder.push(format!(
        " FROM {source} event
          JOIN project_surfaces child
            ON child.logical_name_id = event.logical_name_id
           AND child.chain_id = event.chain_id"
    ));
    if parents {
        builder.push(
            " JOIN project_child_registration_parents parent
                ON parent.child_logical_name_id = event.logical_name_id",
        );
    }
    builder.push(
        " WHERE event.event_kind IN ('RegistrationGranted', 'LabelRegistered')
            AND event.consumer_visibility = 'activated'
            AND event.logical_name_id IS NOT NULL
            AND event.block_number IS NOT NULL
            AND event.block_hash IS NOT NULL
            AND NOT (event.after_state @>
                '{\"state_derived\":true,\"registrar_surface_snapshot\":true}'::jsonb)
            AND child.visibility_state = 'active'
            AND cardinality(child.labelhashes) >= 2",
    );
    // The same exclusion as the parent check above, applied first so registrar grants never
    // leave the database.
    for name in EXCLUDED_CHILD_REGISTRATION_PARENTS {
        let labelhashes = name_labelhashes(name);
        builder.push(" AND NOT (cardinality(child.labelhashes) = ");
        builder.push_bind(i32::try_from(labelhashes.len() + 1).unwrap_or(i32::MAX));
        for (offset, labelhash) in labelhashes.iter().enumerate() {
            builder.push(format!(" AND lower(child.labelhashes[{}]) = ", offset + 2));
            builder.push_bind(format!("{labelhash:#x}"));
        }
        builder.push(")");
    }
}

/// Label hashes of a dotted name, leaf first, as `name_surfaces.labelhashes` stores them.
fn name_labelhashes(name: &str) -> Vec<B256> {
    name.split('.')
        .map(|label| keccak256(label.as_bytes()))
        .collect()
}

/// The namehash of the name one label above the surface whose leaf-first label hashes are given,
/// or `None` when a label hash is not a 32-byte hex word.
fn parent_namehash(labelhashes: &[String]) -> Option<B256> {
    let parent = labelhashes
        .get(1..)?
        .iter()
        .map(|labelhash| labelhash.parse::<B256>().ok())
        .collect::<Option<Vec<_>>>()?;
    (!parent.is_empty()).then(|| namehash(&parent))
}

/// ENS namehash over leaf-first label hashes.
fn namehash(labelhashes: &[B256]) -> B256 {
    labelhashes
        .iter()
        .rev()
        .fold(B256::ZERO, |node, labelhash| {
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(node.as_slice());
            input[32..].copy_from_slice(labelhash.as_slice());
            keccak256(input)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_namehash_drops_exactly_the_leaf_label() {
        let child = name_labelhashes("bob.alice.eth")
            .iter()
            .map(|labelhash| format!("{labelhash:#x}"))
            .collect::<Vec<_>>();
        assert_eq!(
            parent_namehash(&child),
            Some(namehash(&name_labelhashes("alice.eth")))
        );
        // `B256` parsing accepts upper-case hex, as stored label hashes may carry it.
        let upper = child
            .iter()
            .map(|labelhash| labelhash.to_uppercase().replace("0X", "0x"));
        assert_eq!(
            parent_namehash(&upper.collect::<Vec<_>>()),
            Some(namehash(&name_labelhashes("alice.eth")))
        );
        assert_eq!(
            parent_namehash(&child[2..]),
            None,
            "a top-level name has no parent here"
        );
        assert_eq!(
            parent_namehash(&["0x01".to_owned(), "0x02".to_owned()]),
            None
        );
    }

    #[test]
    fn namehash_matches_the_ens_definition() {
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
        // derives a subnode as keccak256(node, label); `eth` is the well-known constant below.
        assert_eq!(
            format!("{:#x}", namehash(&name_labelhashes("eth"))),
            "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
        );
    }
}
