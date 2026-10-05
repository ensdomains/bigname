//! Address-indexed candidate witnesses. Every event arm probes a name, resource, node, or
//! resolver/record key; current candidates are hints until the composer validates them.

use sqlx::{Postgres, QueryBuilder};

use super::AddressRead;
use crate::history::{
    EventHistoryReadFilter, HistoryScope,
    address_evidence::push_historical_address_matches_query,
    attribution::{push_readable_event, push_readable_surface},
    duplicates::push_fixed_product_history_duplicate_filter,
    filters::push_publication_bound,
    keyset::{
        HistoryKeyset, push_history_cursor_after, push_history_cursor_block_bound,
        push_history_cursor_cte,
    },
    paging::{push_history_filters, push_history_order_terms},
    source::push_history_lineage_join,
};

pub(super) const EVENT_COLUMNS: &str = "ne.normalized_event_id, ne.event_identity, ne.chain_id,
    ne.block_number, ne.block_hash, ne.transaction_index, ne.log_index,
    CASE WHEN strpos(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:') > 0
         THEN ne.after_state ->> 'node' END AS node";

const READABLE: &str = "('canonical', 'safe', 'finalized')";
const ZERO_NODE: &str = "'0x0000000000000000000000000000000000000000000000000000000000000000'";

/// The caller supplies WITH or a comma. These sets remain in PostgreSQL; the API never fetches
/// their complete rows. Current resources include only bindings the existing bound validator
/// could accept. The selected relation/resource still requires composition.
fn push_address_ctes<'a>(builder: &mut QueryBuilder<'a, Postgres>, read: &'a AddressRead<'a>) {
    builder.push("address_current_names AS (SELECT DISTINCT indexed.chain_id, indexed.logical_name_id FROM bigname_phase.project_address_name_index indexed WHERE indexed.address = ");
    builder.push_bind(read.address);
    if let Some(namespace) = read.namespace {
        builder.push(" AND EXISTS (SELECT 1 FROM bigname_phase.name_surfaces surface WHERE surface.logical_name_id = indexed.logical_name_id AND surface.namespace = ");
        builder.push_bind(namespace);
        builder.push(")");
    }
    if crate::families::records::includes_roles(read.relations) {
        builder.push(" UNION SELECT DISTINCT grant_row.chain_id, candidate.logical_name_id FROM bigname_phase.project_grant grant_row JOIN bigname_phase.project_binding_candidate candidate ON candidate.chain_id = grant_row.chain_id AND candidate.resource_id = grant_row.resource_id WHERE grant_row.subject = ");
        builder.push_bind(read.address);
        builder.push(" AND grant_row.scope_kind = 'registry' AND NOT grant_row.revoked");
        if let Some(namespace) = read.namespace {
            builder.push(" AND candidate.namespace = ");
            builder.push_bind(namespace);
        }
    }
    builder.push("), address_historical AS (");
    push_historical_address_matches_query(
        builder,
        read.address,
        read.namespace,
        read.relations,
        read.canonical_only,
        false,
        read.published,
    );
    // Name and resource membership are independent proofs: a historically held name can
    // now select another resource whose history still needs current composition.
    // Keep the exclusions uncorrelated so PostgreSQL can hash each qualified historical
    // set once rather than repeatedly scan it under a low anchor-cardinality estimate.
    builder.push("), address_current_surface_names AS (SELECT current_name.* FROM address_current_names current_name WHERE current_name.logical_name_id NOT IN (SELECT historical.logical_name_id FROM address_historical historical WHERE historical.logical_name_id IS NOT NULL))");
    if read.scope == HistoryScope::Surface {
        return;
    }
    builder.push(", address_resource_witnesses AS (SELECT DISTINCT 0 AS witness_kind, NULL::text AS current_chain, NULL::text AS current_name, resource_id FROM address_historical WHERE resource_id IS NOT NULL UNION SELECT 2, current_name.chain_id, current_name.logical_name_id, binding.resource_id FROM address_current_names current_name JOIN bigname_phase.surface_bindings binding ON binding.logical_name_id = current_name.logical_name_id AND binding.chain_id = current_name.chain_id WHERE binding.resource_id NOT IN (SELECT historical.resource_id FROM address_historical historical WHERE historical.resource_id IS NOT NULL)");
    if read.canonical_only {
        builder.push(format!(" AND binding.canonicality_state IN {READABLE}"));
    }
    push_publication_bound(builder, "binding", read.published);
    builder.push(")");
    push_pointer_ctes(builder, read);
}

fn push_pointer_ctes<'a>(builder: &mut QueryBuilder<'a, Postgres>, read: &'a AddressRead<'a>) {
    builder.push(
        ", address_pointers AS (
         SELECT DISTINCT witness.witness_kind, witness.current_chain, witness.current_name,
                pointer.resource_id, pointer.chain_id, lower(surface.namehash) AS namehash,
                lower(pointer.after_state ->> 'resolver') AS resolver_address
         FROM address_resource_witnesses witness
         JOIN bigname_phase.normalized_events pointer ON pointer.resource_id = witness.resource_id
         JOIN bigname_phase.name_surfaces surface ON surface.logical_name_id = pointer.logical_name_id AND surface.chain_id = pointer.chain_id
         WHERE pointer.event_kind = 'ResolverChanged'"
    );
    push_readable_event(builder, "pointer", read.published);
    push_readable_surface(builder, "surface", read.published);
    builder.push(format!(
        "), address_links AS (
         SELECT DISTINCT pointer.witness_kind, pointer.current_chain, pointer.current_name,
                pointer.resource_id, link.normalized_event_id, link.chain_id,
                lower(link.after_state ->> 'resolver') AS resolver_address,
                link.after_state ->> 'resolver_record_id' AS record_id
         FROM address_pointers pointer
         CROSS JOIN LATERAL (SELECT pointer.namehash AS node UNION SELECT {ZERO_NODE}) wanted_node
         CROSS JOIN LATERAL (
           SELECT link.normalized_event_id, link.chain_id, link.after_state
           FROM bigname_phase.normalized_events link
           WHERE link.chain_id = pointer.chain_id
             AND lower(link.after_state ->> 'resolver') = pointer.resolver_address
             AND lower(link.after_state ->> 'node') = wanted_node.node
             AND link.event_kind = 'ResolverRecordLinked' AND link.after_state ->> 'storage_model' = 'resolver_record_id'
           AND link.consumer_visibility = 'activated' AND link.canonicality_state IN {READABLE}"
    ));
    push_publication_bound(builder, "link", read.published);
    // Keep both exact-node and default-node probes parameterized by the full index key.
    builder.push(" OFFSET 0) link)");
}

/// Current-name witness 1 needs only eligible current membership; 2 additionally needs the
/// selected resource to equal witness_resource. Attribution witnesses 3/4 need a valid
/// resource/event pair, with 4 also requiring current membership on that resource. Kind 0 is
/// already qualified by historical matching.
fn push_event_arms<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
) {
    let mut arm = false;
    if read.scope != HistoryScope::Surface
        && read
            .relations
            .is_none_or(|relations| relations.contains(&crate::AddressNameRelation::RoleHolder))
    {
        // Root roles are independent of name/resource membership and belong only to the
        // changed subject. Match the root-history index's expression exactly.
        builder.push(format!("SELECT {EVENT_COLUMNS}, 0 AS witness_kind, NULL::text AS current_chain, NULL::text AS current_name, NULL::uuid AS witness_resource FROM normalized_events ne"));
        push_arm_filters(builder, read, filter, keyset);
        builder.push(" AND ne.event_kind = 'RootPermissionChanged' AND lower(ne.after_state ->> 'subject') = ").push_bind(read.address);
        if let Some(namespace) = read.namespace {
            builder.push(" AND ne.namespace = ").push_bind(namespace);
        }
        arm = true;
    }
    if read.scope != HistoryScope::Resource {
        if arm {
            builder.push(" UNION ALL ");
        }
        push_probe(
            builder,
            read,
            filter,
            keyset,
            "(SELECT DISTINCT logical_name_id FROM address_historical WHERE logical_name_id IS NOT NULL) historical",
            "0 AS witness_kind, NULL::text AS current_chain, NULL::text AS current_name, NULL::uuid AS witness_resource",
            "ne.logical_name_id = historical.logical_name_id",
        );
        builder.push(" UNION ALL ");
        push_probe(
            builder,
            read,
            filter,
            keyset,
            "address_current_surface_names current_name",
            "1, current_name.chain_id, current_name.logical_name_id, NULL::uuid",
            "ne.logical_name_id = current_name.logical_name_id",
        );
        arm = true;
    }
    if read.scope != HistoryScope::Surface {
        if arm {
            builder.push(" UNION ALL ");
        }
        push_probe(
            builder,
            read,
            filter,
            keyset,
            "address_resource_witnesses witness",
            "witness.witness_kind, witness.current_chain, witness.current_name, CASE WHEN witness.witness_kind = 0 THEN NULL::uuid ELSE witness.resource_id END AS witness_resource",
            "ne.resource_id = witness.resource_id",
        );
        builder.push(" UNION ALL ");
        push_probe(
            builder,
            read,
            filter,
            keyset,
            "address_pointers pointer",
            "CASE pointer.witness_kind WHEN 0 THEN 3 ELSE 4 END, pointer.current_chain, pointer.current_name, pointer.resource_id",
            "ne.chain_id = pointer.chain_id AND lower(ne.after_state ->> 'node') = pointer.namehash AND ne.logical_name_id IS NULL AND ne.after_state ->> 'node' IS NOT NULL AND ne.event_kind IN ('RecordChanged', 'RecordVersionChanged') AND ne.source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver')",
        );
        builder.push(" UNION ALL ");
        push_probe(
            builder,
            read,
            filter,
            keyset,
            "address_links link",
            "CASE link.witness_kind WHEN 0 THEN 3 ELSE 4 END, link.current_chain, link.current_name, link.resource_id",
            "ne.chain_id = link.chain_id AND lower(ne.after_state ->> 'resolver') = link.resolver_address AND ne.after_state ->> 'resolver_record_id' = link.record_id AND ne.event_kind = 'RecordChanged' AND ne.after_state ->> 'storage_model' = 'resolver_record_id'",
        );
        builder.push(" UNION ALL ");
        push_probe(
            builder,
            read,
            filter,
            keyset,
            "address_links link",
            "CASE link.witness_kind WHEN 0 THEN 3 ELSE 4 END, link.current_chain, link.current_name, link.resource_id",
            "ne.normalized_event_id = link.normalized_event_id",
        );
    }
}

/// Keep each event probe parameterized by the address witness even when the planner estimates
/// many names. OFFSET 0 prevents flattening this lateral relation into a global event hash
/// join. All event filters remain inside the probe, before this planning boundary.
fn push_probe<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    source: &str,
    witness: &str,
    predicate: &str,
) {
    builder.push(format!("SELECT ne.*, {witness} FROM {source} CROSS JOIN LATERAL (SELECT {EVENT_COLUMNS} FROM normalized_events ne"));
    push_arm_filters(builder, read, filter, keyset);
    builder.push(" AND ne.event_kind <> 'RootPermissionChanged' AND ");
    builder.push(predicate);
    builder.push(" OFFSET 0) ne");
}

pub(super) fn push_arm_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    read: &AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
) {
    if keyset.is_some() {
        builder.push(" CROSS JOIN history_cursor_row cursor_row ");
    }
    push_history_lineage_join(builder);
    builder.push(" WHERE ne.consumer_visibility = 'activated'");
    push_history_filters(builder, filter, read.canonical_only);
    push_fixed_product_history_duplicate_filter(builder);
    if let Some(keyset) = keyset {
        builder.push(" AND ");
        push_history_cursor_after(builder, filter.order);
        push_history_cursor_block_bound(builder, filter, keyset);
    }
}

/// No payload is projected from the candidate relation. PostgreSQL can enumerate and spill its
/// address-specific witness sort; the API fetches only fixed-size slices of this narrow shape.
pub(super) fn push_candidate_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
) {
    if let Some(keyset) = keyset {
        push_history_cursor_cte(builder, keyset.cursor);
        builder.push(", ");
    } else {
        builder.push("WITH ");
    }
    push_address_ctes(builder, read);
    builder.push(", witnesses AS (");
    push_event_arms(builder, read, filter, keyset);
    builder.push(") SELECT DISTINCT ne.normalized_event_id, ne.event_identity, ne.chain_id, ne.block_number, ne.block_hash, ne.transaction_index, ne.log_index, ne.node, ne.witness_kind, ne.current_chain, ne.current_name, ne.witness_resource FROM witnesses ne ORDER BY ");
    push_history_order_terms(builder, filter.order);
    builder.push(", ne.witness_kind, ne.current_chain, ne.current_name, ne.witness_resource, ne.normalized_event_id, node");
}

/// Batched same-origin peers. The chain/block probe is constrained before address witnesses
/// join it; neither public continuation nor an application batch decides the winner.
pub(super) fn push_handoff_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    groups: &'a serde_json::Value,
) {
    if read.catalogue {
        super::catalogue_source::push_handoff_query(builder, read, filter, groups);
        return;
    }
    builder.push("WITH ");
    push_address_ctes(builder, read);
    builder.push(", peer_events AS MATERIALIZED (SELECT ne.* FROM jsonb_to_recordset(");
    builder.push_bind(groups);
    builder.push("::jsonb) AS peer(chain text, block bigint, hash text, node text, origin text) CROSS JOIN LATERAL (SELECT ne.* FROM normalized_events ne");
    push_arm_filters(builder, read, filter, None);
    builder.push(" AND ne.event_kind = 'ResolverChanged' AND ne.chain_id = peer.chain AND ne.block_number = peer.block AND ne.block_hash IS NOT DISTINCT FROM peer.hash AND ne.after_state ->> 'node' IS NOT DISTINCT FROM peer.node AND strpos(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:') > 0 AND split_part(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:', 1) = peer.origin OFFSET 0) ne), witnesses AS (");
    let mut has_arm = false;
    if read.scope != HistoryScope::Resource {
        builder.push(format!("SELECT {EVENT_COLUMNS}, 0 AS witness_kind, NULL::text AS current_chain, NULL::text AS current_name, NULL::uuid AS witness_resource FROM peer_events ne JOIN address_historical historical ON historical.logical_name_id = ne.logical_name_id UNION ALL SELECT {EVENT_COLUMNS}, 1, current_name.chain_id, current_name.logical_name_id, NULL::uuid FROM peer_events ne JOIN address_current_surface_names current_name ON current_name.logical_name_id = ne.logical_name_id"));
        has_arm = true;
    }
    if read.scope != HistoryScope::Surface {
        if has_arm {
            builder.push(" UNION ALL ");
        }
        builder.push(format!("SELECT {EVENT_COLUMNS}, witness.witness_kind, witness.current_chain, witness.current_name, CASE WHEN witness.witness_kind = 0 THEN NULL::uuid ELSE witness.resource_id END AS witness_resource FROM peer_events ne JOIN address_resource_witnesses witness ON witness.resource_id = ne.resource_id"));
    }
    builder.push(") SELECT DISTINCT * FROM witnesses ORDER BY event_identity, witness_kind, current_chain, current_name, witness_resource");
}
