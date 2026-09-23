//! Statement text for the bounded attribution reader. Each arm restates one arm of the producer's
//! attribution with the evidence limited to the read's published block.

use std::collections::BTreeMap;

use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::history::filters::push_publication_bound;

const READABLE: &str = "('canonical', 'safe', 'finalized')";
pub(super) const ENS_V1_POINTER_FAMILIES: &str =
    "('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')";
pub(super) const ENS_V2_POINTER_FAMILIES: &str = "('ens_v2_registry_l1', 'ens_v2_root_l1')";
pub(super) const CLEARED: &str = "('0x0000000000000000000000000000000000000000', '')";

/// `alias` is an activated, canonical event on a readable block at or below the bound of its
/// chain, or an event with no chain position at all: the rows Project stages for its target
/// (`crates/project/src/stage.rs`, `create_events`).
pub(super) fn push_readable_event(
    builder: &mut QueryBuilder<'_, Postgres>,
    alias: &str,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(format!(
        " AND {alias}.consumer_visibility = 'activated'
          AND {alias}.canonicality_state IN {READABLE}
          AND (({alias}.block_number IS NULL AND {alias}.block_hash IS NULL)
               OR (EXISTS (
                       SELECT 1 FROM bigname_phase.chain_lineage {alias}_lineage
                       WHERE {alias}_lineage.chain_id = {alias}.chain_id
                         AND {alias}_lineage.block_hash = {alias}.block_hash
                         AND {alias}_lineage.block_number = {alias}.block_number
                         AND {alias}_lineage.canonicality_state IN {READABLE})"
    ));
    push_publication_bound(builder, alias, published);
    builder.push("))");
}

/// `alias` is a name surface Project would stage at the bound: canonical, on a readable block at
/// or below the bound of its chain.
pub(super) fn push_readable_surface(
    builder: &mut QueryBuilder<'_, Postgres>,
    alias: &str,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(format!(
        " AND {alias}.canonicality_state IN {READABLE}
          AND EXISTS (
              SELECT 1 FROM bigname_phase.chain_lineage {alias}_lineage
              WHERE {alias}_lineage.chain_id = {alias}.chain_id
                AND {alias}_lineage.block_hash = {alias}.block_hash
                AND {alias}_lineage.block_number = {alias}.block_number
                AND {alias}_lineage.canonicality_state IN {READABLE})"
    ));
    push_publication_bound(builder, alias, published);
}

/// `LATERAL (...) declaration`: the latest readable `SourceManifestUpdated` row of the manifest
/// `manifest_id` names, as Project stages its admitted manifests (`create_manifests`). The caller
/// requires `declaration.active` and compares `declaration.namespace`.
pub(super) fn push_declaration_manifest(
    builder: &mut QueryBuilder<'_, Postgres>,
    manifest_id: &str,
    chain_id: &str,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(format!(
        " LATERAL (
            SELECT manifest.source_manifest_id AS manifest_id,
                   manifest.namespace,
                   manifest.after_state ->> 'rollout_status' = 'active'
                       AND manifest.after_state -> 'manifest_payload' IS NOT NULL AS active
            FROM bigname_phase.normalized_events manifest
            WHERE manifest.event_kind = 'SourceManifestUpdated'
              AND manifest.source_manifest_id = {manifest_id}
              AND (manifest.chain_id = {chain_id}
                   OR ({chain_id} = 'base-mainnet'
                       AND manifest.namespace = 'basenames'
                       AND manifest.source_family = 'basenames_execution'
                       AND manifest.chain_id = 'ethereum-mainnet'))
              AND manifest.canonicality_state IN {READABLE}
              AND (manifest.block_hash IS NULL OR EXISTS (
                  SELECT 1 FROM bigname_phase.chain_lineage manifest_lineage
                  WHERE manifest_lineage.chain_id = manifest.chain_id
                    AND manifest_lineage.block_hash = manifest.block_hash
                    AND manifest_lineage.block_number = manifest.block_number
                    AND manifest_lineage.canonicality_state IN {READABLE}))
              AND (manifest.block_number IS NULL OR (TRUE"
    ));
    push_publication_bound(builder, "manifest", published);
    builder.push(
        "))
            ORDER BY manifest.normalized_event_id DESC
            LIMIT 1
        ) declaration",
    );
}

/// The chain position Project orders events by, with missing parts first.
pub(super) fn position(alias: &str) -> String {
    format!(
        "ARRAY[COALESCE({alias}.block_number, -1), COALESCE({alias}.transaction_index, -1),
               COALESCE({alias}.log_index, -1), {alias}.normalized_event_id]"
    )
}

/// `WITH pointer_rows, pointer_windows, pointers`: every readable `ResolverChanged` of the
/// resources whose name surface is readable, each with the position of the pointer after it
/// (`end_position`, null on the latest), and the subset that selects a resolver. Clears take part
/// in the windows, so a clear closes the window before it and opens none.
pub(super) fn push_pointer_ctes<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(format!(
        "WITH pointer_rows AS (
            SELECT pointer.chain_id,
                   pointer.resource_id,
                   pointer.normalized_event_id AS pointer_event_id,
                   pointer.namespace AS pointer_namespace,
                   pointer.source_family AS pointer_source_family,
                   surface.namespace AS surface_namespace,
                   surface.raw_labels,
                   surface.labelhashes,
                   lower(surface.namehash) AS namehash,
                   lower(pointer.after_state ->> 'resolver') AS resolver_address,
                   {} AS event_position
            FROM bigname_phase.normalized_events pointer
            JOIN bigname_phase.name_surfaces surface
              ON surface.logical_name_id = pointer.logical_name_id
             AND surface.chain_id = pointer.chain_id
            WHERE pointer.resource_id = ANY(",
        position("pointer")
    ));
    builder.push_bind(resource_ids);
    builder.push(
        "::uuid[])
              AND pointer.event_kind = 'ResolverChanged'
              AND pointer.logical_name_id IS NOT NULL",
    );
    push_readable_event(builder, "pointer", published);
    push_readable_surface(builder, "surface", published);
    builder.push(format!(
        "
        ),
        pointer_windows AS (
            SELECT pointer_rows.*,
                   lead(event_position) OVER (
                       PARTITION BY chain_id, resource_id ORDER BY event_position
                   ) AS end_position
            FROM pointer_rows
        ),
        pointers AS (
            SELECT * FROM pointer_windows
            WHERE resolver_address IS NOT NULL AND resolver_address NOT IN {CLEARED}
        )"
    ));
}

/// The `record` write lies inside `window`'s pointer or link interval.
fn inside(window: &str) -> String {
    format!(
        "({window}.end_position IS NULL OR {} < {window}.end_position)",
        position("record")
    )
}

/// `record` is a node-keyed write on `pointer`'s resolver for `pointer`'s name.
fn node_keyed_on(pointer: &str) -> String {
    format!(
        "record.chain_id = {pointer}.chain_id
         AND record.logical_name_id IS NULL
         AND lower(record.after_state ->> 'node') = {pointer}.namehash
         AND lower(COALESCE(
                 NULLIF(record.after_state ->> 'resolver', ''),
                 NULLIF(record.raw_fact_ref ->> 'emitting_address', '')
             )) = {pointer}.resolver_address
         AND record.event_kind IN ('RecordChanged', 'RecordVersionChanged')"
    )
}

/// `SELECT resource_id, normalized_event_id` for every write a pointer or record link at or below
/// the bound attributes to one of `resource_ids`.
pub(super) fn push_pointer_window_attribution<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
    published: Option<&BTreeMap<String, i64>>,
) {
    push_pointer_ctes(builder, resource_ids, published);
    push_record_link_ctes(builder, published);
    // ENSv1 and Basenames: a registry-side pointer attributes the node-keyed writes on its
    // resolver made before the next pointer; the latest pointer is open-ended. Each arm names its
    // source family literally so it matches that family's node and resolver index.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f)
    for (record_family, pointer_families) in [
        ("ens_v1_resolver_l1", ENS_V1_POINTER_FAMILIES),
        ("basenames_base_resolver", "('basenames_base_registry')"),
    ] {
        builder.push(format!(
            "
        SELECT pointer.resource_id, record.normalized_event_id
        FROM pointers pointer
        JOIN bigname_phase.normalized_events record
          ON {}
         AND record.source_family = '{record_family}'
        WHERE pointer.pointer_source_family IN {pointer_families}
          AND {}",
            node_keyed_on("pointer"),
            inside("pointer"),
        ));
        push_readable_event(builder, "record", published);
        builder.push("\n        UNION");
    }
    push_declared_resolver_arm(builder, published);
    builder.push("\n        UNION");
    push_record_link_arm(builder, published);
}

/// ENSv2-origin writes: an ENSv2 registry or root pointer attributes the node-keyed writes of a
/// resolver whose classification is a supported, manifest-declared ENSv1 resolver or
/// `public_resolver_v2`, when the declaring manifest is in the pointer's namespace. The
/// classification is read from `resolver_current`, as the producer reads it.
fn push_declared_resolver_arm(
    builder: &mut QueryBuilder<'_, Postgres>,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(
        "
        SELECT pointer.resource_id, record.normalized_event_id
        FROM pointers pointer
        JOIN bigname_phase.resolver_current resolver
          ON resolver.chain_id = pointer.chain_id
         AND resolver.resolver_address = pointer.resolver_address
         AND resolver.support_status = 'supported'
         AND (resolver.declared_summary #>> '{classification,source_family}' =
                 'ens_v1_resolver_l1'
              OR (resolver.declared_summary #>> '{classification,source_family}' =
                      'ens_v2_resolver_l1'
                  AND resolver.declared_summary #>> '{classification,role}' =
                      'public_resolver_v2'))
         AND resolver.declared_summary #>> '{classification,basis}' =
             'manifest_declared_address'
        JOIN",
    );
    push_declaration_manifest(
        builder,
        "(resolver.provenance ->> 'manifest_id')::bigint",
        "pointer.chain_id",
        published,
    );
    builder.push(format!(
        "
          ON declaration.active AND declaration.namespace = pointer.pointer_namespace
        JOIN bigname_phase.normalized_events record
          ON {}
         AND record.source_family =
             resolver.declared_summary #>> '{{classification,source_family}}'
         AND (record.source_family <> 'ens_v2_resolver_l1'
              OR (record.namespace = pointer.pointer_namespace
                  AND record.source_manifest_id = declaration.manifest_id))
        WHERE pointer.pointer_source_family IN {ENS_V2_POINTER_FAMILIES}
          AND {}",
        node_keyed_on("pointer"),
        inside("pointer"),
    ));
    push_readable_event(builder, "record", published);
}

/// `, links, link_boundaries, link_spans, link_selections`: on a record-ID resolver each pointer's
/// window is split wherever the node's exact link or the resolver's default link changes, and each
/// span selects the record its latest exact link, else its default link, names
/// (`crates/project/src/builders/linked_records/history.rs`).
fn push_record_link_ctes(
    builder: &mut QueryBuilder<'_, Postgres>,
    published: Option<&BTreeMap<String, i64>>,
) {
    const DEFAULT_NODE: &str =
        "'0x0000000000000000000000000000000000000000000000000000000000000000'";
    builder.push(format!(
        ",
        links AS (
            SELECT link.chain_id,
                   link.normalized_event_id,
                   lower(link.after_state ->> 'resolver') AS resolver_address,
                   lower(link.after_state ->> 'node') AS node,
                   link.after_state ->> 'resolver_record_id' AS record_id,
                   {} AS event_position
            FROM bigname_phase.normalized_events link
            WHERE link.event_kind = 'ResolverRecordLinked'
              AND link.after_state ->> 'storage_model' = 'resolver_record_id'
              AND lower(link.after_state ->> 'resolver') IN (SELECT resolver_address FROM pointers)",
        position("link")
    ));
    push_readable_event(builder, "link", published);
    builder.push(format!(
        "
        ),
        link_boundaries AS (
            SELECT pointer_event_id, event_position FROM pointers
            UNION
            SELECT pointer.pointer_event_id, link.event_position
            FROM pointers pointer
            JOIN links link
              ON link.chain_id = pointer.chain_id
             AND link.resolver_address = pointer.resolver_address
             AND link.node IN (pointer.namehash, {DEFAULT_NODE})
             AND link.event_position > pointer.event_position
             AND (pointer.end_position IS NULL OR link.event_position < pointer.end_position)
        ),
        link_spans AS (
            SELECT pointer.chain_id, pointer.resource_id, pointer.resolver_address,
                   pointer.namehash, boundary.event_position,
                   COALESCE(lead(boundary.event_position) OVER (
                       PARTITION BY boundary.pointer_event_id ORDER BY boundary.event_position
                   ), pointer.end_position) AS end_position
            FROM link_boundaries boundary
            JOIN pointers pointer USING (pointer_event_id)
        ),
        link_selections AS (
            SELECT span.*,
                   CASE WHEN exact.record_id <> '0' THEN exact.record_id
                        ELSE defaults.record_id END AS record_id,
                   exact.normalized_event_id AS exact_link_event_id,
                   CASE WHEN COALESCE(exact.record_id, '0') = '0'
                        THEN defaults.normalized_event_id END AS default_link_event_id
            FROM link_spans span
            LEFT JOIN LATERAL (
                SELECT link.* FROM links link
                WHERE link.chain_id = span.chain_id
                  AND link.resolver_address = span.resolver_address
                  AND link.node = span.namehash
                  AND link.event_position <= span.event_position
                ORDER BY link.event_position DESC LIMIT 1
            ) exact ON TRUE
            LEFT JOIN LATERAL (
                SELECT link.* FROM links link
                WHERE link.chain_id = span.chain_id
                  AND link.resolver_address = span.resolver_address
                  AND link.node = {DEFAULT_NODE}
                  AND link.event_position <= span.event_position
                ORDER BY link.event_position DESC LIMIT 1
            ) defaults ON TRUE
        )"
    ));
}

/// A selected record contributes its writes made before its span ends, and the selecting links
/// are attributed too, as the producer retains them.
fn push_record_link_arm(
    builder: &mut QueryBuilder<'_, Postgres>,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(format!(
        "
        SELECT selection.resource_id, record.normalized_event_id
        FROM link_selections selection
        JOIN bigname_phase.normalized_events record
          ON record.chain_id = selection.chain_id
         AND lower(record.after_state ->> 'resolver') = selection.resolver_address
         AND record.after_state ->> 'storage_model' = 'resolver_record_id'
         AND record.after_state ->> 'resolver_record_id' = selection.record_id
         AND record.event_kind = 'RecordChanged'
        WHERE selection.record_id IS NOT NULL
          AND {}",
        inside("selection"),
    ));
    push_readable_event(builder, "record", published);
    builder.push(
        "
        UNION
        SELECT selection.resource_id, link.event_id
        FROM link_selections selection
        CROSS JOIN LATERAL (VALUES (selection.exact_link_event_id),
                                   (selection.default_link_event_id)) link(event_id)
        WHERE link.event_id IS NOT NULL",
    );
}
