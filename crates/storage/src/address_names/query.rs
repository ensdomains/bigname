#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;

#[path = "query/timestamps.rs"]
mod timestamps;
pub(crate) use timestamps::{push_expiry_paths_expr, push_json_timestamp_expr};

use crate::UnixSeconds;
use sqlx::{Postgres, QueryBuilder};

use super::source::RowSource;
use super::types::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    NameQuery,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn push_address_names_current_grouped_entries_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    source: RowSource<'a>,
    address: &'a str,
    namespace: Option<&'a str>,
    relations: Option<&'a [AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authorities: Option<&[&str]>,
    is_migrated: Option<bool>,
) {
    source.push_with(builder);
    source.push_served_address_names(builder);
    builder.push(
        r#"filtered AS (
            SELECT
                anc.address,
                anc.logical_name_id,
                anc.relation,
                anc.namespace,
                anc.raw_name AS canonical_display_name,
                anc.normalized_name,
                anc.namehash,
                anc.surface_binding_id,
                anc.resource_id,
                anc.token_lineage_id,
                anc.binding_kind,
                anc.provenance,
                CASE WHEN anc.support_status = 'supported'
                     THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                     ELSE jsonb_build_object(
                         'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                         'unsupported_reason', anc.unsupported_reason
                     ) END AS coverage,
                anc.chain_positions,
                anc.canonicality_summary,
                anc.manifest_version,
                anc.last_recomputed_at,
                anc.served_owner,
                anc.served_authority,
                CASE anc.relation
                    WHEN 'registrant' THEN 0
                    WHEN 'token_holder' THEN 1
                    WHEN 'effective_controller' THEN 2
                    WHEN 'role_holder' THEN 3
                    ELSE 99
                END AS relation_rank
            FROM served_rows anc"#,
    );
    builder.push(" WHERE anc.address =");
    builder.push(" ");
    builder.push_bind(address);

    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
        let relation_values = relations
            .iter()
            .map(|relation| relation.as_str().to_owned())
            .collect::<Vec<_>>();
        builder.push(" AND anc.relation::TEXT = ANY(");
        builder.push_bind(relation_values);
        builder.push(")");
    }
    if let Some(q) = q {
        builder.push(" AND anc.normalized_name LIKE ");
        builder.push_bind(q.like_pattern());
        builder.push(" ESCAPE '\\'");
    }
    if let Some(authorities) = authorities.filter(|authorities| !authorities.is_empty()) {
        // A surface-less registry child has no name row; it matches by its registry's authority.
        builder.push(" AND (anc.registry_child IS TRUE AND anc.served_authority = ANY(");
        builder.push_bind(
            authorities
                .iter()
                .map(|authority| (*authority).to_owned())
                .collect::<Vec<_>>(),
        );
        builder.push(") OR anc.registry_child IS NOT TRUE");
        crate::name_current::push_public_authority_filter_in(
            builder,
            source.names(),
            "anc.logical_name_id",
            authorities,
        );
        builder.push(")");
    }
    if let Some(is_migrated) = is_migrated {
        if is_migrated {
            builder.push(" AND ");
        } else {
            builder.push(" AND NOT ");
        }
        // Use the same proof and timestamp join as load_name_migration_transition_timestamps.
        builder.push(format!(
            "EXISTS (SELECT 1 FROM {} migration_nc",
            source.names()
        ));
        builder.push(r#"
            JOIN bigname_phase.normalized_events proof
              ON proof.normalized_event_id = CASE
                WHEN migration_nc.provenance #>> '{authority_selection,proof_event_id}' ~ '^[0-9]+$'
                THEN (migration_nc.provenance #>> '{authority_selection,proof_event_id}')::bigint END
            JOIN bigname_phase.chain_lineage migration_lineage
              ON migration_lineage.chain_id = proof.chain_id
             AND migration_lineage.block_hash = proof.block_hash
            WHERE migration_nc.logical_name_id = anc.logical_name_id
              AND migration_nc.provenance #>> '{authority_selection,authority_arm}' = 'ens_v2'
              AND migration_nc.provenance #>> '{authority_selection,proof_kind}' = "#);
        builder.push_bind(crate::MIGRATION_AUTHORITY_TRANSITION_PROOF_KIND);
        builder.push(")");
    }
    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, logical_name_id)
                address,
                logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                surface_binding_id,
                resource_id,
                token_lineage_id,
                binding_kind,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at,
                served_owner,
                served_authority
            FROM filtered
            ORDER BY
                address ASC,
                logical_name_id ASC,
                canonical_display_name ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                logical_name_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, logical_name_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                logical_name_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, logical_name_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at,
                representatives.served_owner,
                representatives.served_authority
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.logical_name_id = representatives.logical_name_id
        )
            "#,
        ),
        AddressNamesCurrentDedupe::Resource => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, resource_id)
                address,
                logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                surface_binding_id,
                resource_id,
                token_lineage_id,
                binding_kind,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at,
                served_owner,
                served_authority
            FROM filtered
            ORDER BY
                address ASC,
                resource_id ASC,
                canonical_display_name ASC,
                logical_name_id ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                resource_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, resource_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                resource_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, resource_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at,
                representatives.served_owner,
                representatives.served_authority
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.resource_id = representatives.resource_id
        )
            "#,
        ),
    };
}

pub(super) fn push_address_names_current_sortable_entries_cte(
    builder: &mut QueryBuilder<'_, Postgres>,
    names: &str,
    sort: AddressNamesCurrentSort,
) {
    if !sort.is_timestamp() {
        return;
    }

    builder.push(
        r#",
        sortable_entries AS (
            SELECT
                entries.*,
        "#,
    );
    push_address_names_current_sort_timestamp_expr(builder, sort);
    builder.push(format!(
        "
                AS sort_timestamp
            FROM entries
            LEFT JOIN {names} nc
              ON nc.logical_name_id = entries.logical_name_id
        )
        "
    ));
}

pub(super) fn push_address_names_current_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            let AddressNamesCurrentSortedCursorValue::Name(sort_value) = &cursor.sort_value else {
                return;
            };
            builder.push(" AND (canonical_display_name ");
            builder.push(match order {
                AddressNamesCurrentOrder::Asc => "> ",
                AddressNamesCurrentOrder::Desc => "< ",
            });
            builder.push_bind(sort_value);
            push_address_names_current_name_tie_after(builder, sort_value, cursor);
            builder.push(")");
        }
        AddressNamesCurrentSort::ExpiresAt
        | AddressNamesCurrentSort::RegisteredAt
        | AddressNamesCurrentSort::CreatedAt => {
            let sort_value = match &cursor.sort_value {
                AddressNamesCurrentSortedCursorValue::Timestamp(sort_value) => *sort_value,
                AddressNamesCurrentSortedCursorValue::Name(_) => return,
            };
            let cursor_rank = timestamp_null_rank(sort_value, order);
            builder.push(" AND (");
            builder.push(timestamp_rank_expr("sort_timestamp", order));
            builder.push(" > ");
            builder.push_bind(cursor_rank);
            builder.push(" OR (");
            builder.push(timestamp_rank_expr("sort_timestamp", order));
            builder.push(" = ");
            builder.push_bind(cursor_rank);
            builder.push(" AND ");
            match sort_value {
                None => {
                    push_address_names_current_timestamp_tie_after(builder, None, cursor);
                }
                Some(value) => match order {
                    AddressNamesCurrentOrder::Asc => {
                        builder.push("(sort_timestamp > ");
                        builder.push_bind(value);
                        builder.push(" OR (sort_timestamp = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_address_names_current_timestamp_tie_after(
                            builder,
                            Some(value),
                            cursor,
                        );
                        builder.push("))");
                    }
                    AddressNamesCurrentOrder::Desc => {
                        builder.push("(sort_timestamp < ");
                        builder.push_bind(value);
                        builder.push(" OR (sort_timestamp = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_address_names_current_timestamp_tie_after(
                            builder,
                            Some(value),
                            cursor,
                        );
                        builder.push("))");
                    }
                },
            }
            builder.push("))");
        }
    }
}

pub(super) fn push_address_names_current_order(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            builder.push(" ORDER BY canonical_display_name ");
            builder.push(match order {
                AddressNamesCurrentOrder::Asc => "ASC",
                AddressNamesCurrentOrder::Desc => "DESC",
            });
            builder.push(", logical_name_id ASC, resource_id::TEXT ASC");
        }
        AddressNamesCurrentSort::ExpiresAt
        | AddressNamesCurrentSort::RegisteredAt
        | AddressNamesCurrentSort::CreatedAt => {
            builder.push(" ORDER BY ");
            builder.push(timestamp_rank_expr("sort_timestamp", order));
            builder.push(" ASC, sort_timestamp ");
            builder.push(match order {
                AddressNamesCurrentOrder::Asc => "ASC",
                AddressNamesCurrentOrder::Desc => "DESC",
            });
            builder.push(", logical_name_id ASC, resource_id::TEXT ASC");
        }
    }
}

fn push_address_names_current_name_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort_value: &'a str,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    builder.push(" OR (canonical_display_name = ");
    builder.push_bind(sort_value);
    builder.push(" AND ");
    push_address_names_current_tie_after(builder, cursor);
    builder.push(")");
}

fn push_address_names_current_timestamp_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    value: Option<UnixSeconds>,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    match value {
        None => {
            builder.push("sort_timestamp IS NULL AND ");
        }
        Some(value) => {
            builder.push("sort_timestamp = ");
            builder.push_bind(value);
            builder.push(" AND ");
        }
    }
    push_address_names_current_tie_after(builder, cursor);
}

fn push_address_names_current_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    builder.push("(logical_name_id, resource_id::TEXT) > (");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(", ");
    builder.push_bind(cursor.resource_id.to_string());
    builder.push(")");
}

fn push_address_names_current_sort_timestamp_expr(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: AddressNamesCurrentSort,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            builder.push("NULL::NUMERIC");
        }
        AddressNamesCurrentSort::ExpiresAt => push_expires_at_timestamp_expr(builder),
        AddressNamesCurrentSort::RegisteredAt => {
            builder.push("EXTRACT(EPOCH FROM ");
            push_registered_at_timestamp_expr(builder);
            builder.push(")");
        }
        AddressNamesCurrentSort::CreatedAt => {
            builder.push("EXTRACT(EPOCH FROM ");
            push_created_at_timestamp_expr(builder);
            builder.push(")");
        }
    };
}

/// Push exact expiry seconds from a composed name aliased `nc`. The summary writer and
/// collection sorts use the same priority and preserve an explicitly absent registration expiry.
pub(crate) fn push_expires_at_timestamp_expr(builder: &mut QueryBuilder<'_, Postgres>) {
    push_expiry_paths_expr(
        builder,
        &[
            &["registration", "expires_at"],
            &["registration", "expiry_date"],
            &["registration", "expiry"],
            &["control", "expires_at"],
            &["control", "expiry_date"],
            &["control", "expiry"],
        ],
    );
}

/// Push the registration timestamp read of a `name_current` row aliased `nc`.
pub(crate) fn push_registered_at_timestamp_expr(builder: &mut QueryBuilder<'_, Postgres>) {
    push_json_timestamp_coalesce_expr(
        builder,
        &[
            &["registration", "registered_at"],
            &["registration", "registration_date"],
        ],
    );
}

/// Push the first-observation timestamp read of a `name_current` row aliased `nc`: the paths the
/// served `created_at` reads (composition always writes `registration.created_at`).
fn push_created_at_timestamp_expr(builder: &mut QueryBuilder<'_, Postgres>) {
    push_json_timestamp_coalesce_expr(
        builder,
        &[&["registration", "created_at"], &["history", "created_at"]],
    );
}

fn push_json_timestamp_coalesce_expr(builder: &mut QueryBuilder<'_, Postgres>, paths: &[&[&str]]) {
    builder.push("COALESCE(");
    for (index, path) in paths.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        push_json_timestamp_expr(builder, path);
    }
    builder.push(")");
}

fn timestamp_rank_expr(column: &str, order: AddressNamesCurrentOrder) -> String {
    match order {
        AddressNamesCurrentOrder::Asc => {
            format!("CASE WHEN {column} IS NULL THEN 1 ELSE 0 END")
        }
        AddressNamesCurrentOrder::Desc => {
            format!("CASE WHEN {column} IS NULL THEN 0 ELSE 1 END")
        }
    }
}

fn timestamp_null_rank(value: Option<UnixSeconds>, order: AddressNamesCurrentOrder) -> i32 {
    match (value.is_none(), order) {
        (true, AddressNamesCurrentOrder::Asc) => 1,
        (false, AddressNamesCurrentOrder::Asc) => 0,
        (true, AddressNamesCurrentOrder::Desc) => 0,
        (false, AddressNamesCurrentOrder::Desc) => 1,
    }
}
