use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApiLookupDdlKind {
    Relation,
    Column,
    Function,
    Type,
}

impl ApiLookupDdlKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Relation => "relation",
            Self::Column => "column",
            Self::Function => "function",
            Self::Type => "type",
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApiLookupDdlObject {
    pub kind: ApiLookupDdlKind,
    pub identity: String,
}

pub async fn phase_schema_exists(pool: &PgPool) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (\
             SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'bigname_phase'\
         )",
    )
    .fetch_one(pool)
    .await
    .context("failed to inspect phase-schema presence")
}

pub async fn load_missing_api_lookup_ddl(pool: &PgPool) -> Result<Vec<ApiLookupDdlObject>> {
    let rows = sqlx::query(
        r#"
        WITH required(kind, identity) AS (
            VALUES
                ('relation', 'bigname_phase.chain_heads'),
                ('relation', 'bigname_phase.chain_header_audit'),
                ('relation', 'bigname_phase.chain_lineage'),
                ('relation', 'bigname_phase.chain_phase_state'),
                ('relation', 'bigname_phase.normalized_events'),
                ('relation', 'bigname_phase.migration_event_associations'),
                ('relation', 'bigname_phase.child_registration_events'),
                ('relation', 'bigname_phase.name_surfaces'),
                ('relation', 'bigname_phase.name_search_documents'),
                ('relation', 'bigname_phase.name_search_postings'),
                ('relation', 'bigname_phase.resources'),
                ('relation', 'bigname_phase.surface_bindings'),
                ('relation', 'bigname_phase.token_lineages'),
                ('relation', 'bigname_phase.service_heartbeats'),
                ('relation', 'bigname_phase.manifest_versions'),
                ('relation', 'bigname_phase.manifest_contract_instances'),
                ('relation', 'bigname_phase.contract_instance_addresses'),
                ('relation', 'bigname_phase.discovery_edges'),
                ('relation', 'bigname_phase.migration_discovery_associations'),
                ('relation', 'bigname_phase.label_preimages'),
                ('relation', 'bigname_phase.project_family_marker'),
                ('relation', 'bigname_phase.project_family_undo'),
                ('relation', 'bigname_phase.project_repair_record'),
                ('relation', 'bigname_phase.project_name_state'),
                ('relation', 'bigname_phase.project_binding_candidate'),
                ('relation', 'bigname_phase.project_lifecycle_key_state'),
                ('relation', 'bigname_phase.project_lifecycle_triple_summary'),
                ('relation', 'bigname_phase.project_lifecycle_association'),
                ('relation', 'bigname_phase.project_lifecycle_event'),
                ('relation', 'bigname_phase.project_child_registration_state'),
                ('relation', 'bigname_phase.project_wrapper_state'),
                ('relation', 'bigname_phase.project_registry_node_state'),
                ('relation', 'bigname_phase.project_registry_owner_event'),
                ('relation', 'bigname_phase.project_registry_binding_observation'),
                ('relation', 'bigname_phase.project_resolver_classification'),
                ('relation', 'bigname_phase.project_registry_pointer'),
                ('relation', 'bigname_phase.project_resource_pointer'),
                ('relation', 'bigname_phase.project_named_resource_pointer'),
                ('relation', 'bigname_phase.project_universal_resolver_proxy'),
                ('relation', 'bigname_phase.project_node_record_partition'),
                ('relation', 'bigname_phase.project_node_record_value'),
                ('relation', 'bigname_phase.project_record_id_value'),
                ('relation', 'bigname_phase.project_resolver_link'),
                ('relation', 'bigname_phase.project_grant'),
                ('relation', 'bigname_phase.project_resource_admin_aggregate'),
                ('relation', 'bigname_phase.project_account_approval'),
                ('relation', 'bigname_phase.project_ens_v2_entry_owner'),
                ('relation', 'bigname_phase.project_ens_v2_registry_parent'),
                ('relation', 'bigname_phase.project_child_edge_candidate'),
                ('relation', 'bigname_phase.project_parent_subregistry'),
                ('relation', 'bigname_phase.project_reverse_tuple'),
                ('relation', 'bigname_phase.project_reverse_node_claim'),
                ('relation', 'bigname_phase.project_claim_normalization'),
                ('relation', 'bigname_phase.project_address_name_fold'),
                ('relation', 'bigname_phase.project_address_controller_candidate'),
                ('relation', 'bigname_phase.project_address_name_index'),
                ('relation', 'bigname_phase.project_address_history_anchor'),
                ('relation', 'bigname_phase.project_history_source'),
                ('relation', 'bigname_phase.project_history_source_edge'),
                ('relation', 'bigname_phase.project_history_catalogue_marker'),
                ('relation', 'bigname_phase.project_address_record_node_index'),
                ('relation', 'bigname_phase.project_address_record_id_index'),
                ('relation', 'bigname_phase.project_name_history'),
                ('relation', 'bigname_phase.project_name_summary'),
                ('column', 'bigname_phase.project_name_summary.search_supported'),
                ('column', 'bigname_phase.project_name_summary.search_fields'),
                ('column', 'bigname_phase.project_name_summary.search_creation_transport_resource_id'),

                (
                    'function',
                    'bigname_phase.revalidate_resolution_lookup_state_read_only(text,bigint,text,jsonb,jsonb,uuid,text,text)'
                ),
                ('type', 'bigname_phase.canonicality_state')
        )
        SELECT kind, identity
        FROM required
        WHERE CASE kind
            WHEN 'relation' THEN CASE WHEN to_regnamespace(split_part(identity, '.', 1)) IS NULL THEN TRUE
                WHEN NOT has_schema_privilege(current_user, to_regnamespace(split_part(identity, '.', 1)), 'USAGE') THEN TRUE
                WHEN to_regclass(identity) IS NULL THEN TRUE ELSE NOT has_table_privilege(current_user, identity, 'SELECT') END
            WHEN 'column' THEN NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid=to_regclass(split_part(identity, '.', 1) || '.' || split_part(identity, '.', 2))
                  AND attname=split_part(identity, '.', 3) AND attnum>0 AND NOT attisdropped)
            WHEN 'function' THEN CASE WHEN to_regnamespace(split_part(identity, '.', 1)) IS NULL THEN TRUE WHEN NOT has_schema_privilege(current_user, to_regnamespace(split_part(identity, '.', 1)), 'USAGE') THEN TRUE WHEN to_regprocedure(identity) IS NULL THEN TRUE ELSE NOT has_function_privilege(current_user, identity, 'EXECUTE') END
            WHEN 'type' THEN CASE WHEN to_regnamespace(split_part(identity, '.', 1)) IS NULL THEN TRUE WHEN NOT has_schema_privilege(current_user, to_regnamespace(split_part(identity, '.', 1)), 'USAGE') THEN TRUE ELSE to_regtype(identity) IS NULL END
        END
        ORDER BY kind, identity
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to inspect required API lookup DDL")?;

    rows.into_iter()
        .map(|row| {
            let kind = match row.try_get::<&str, _>("kind")? {
                "relation" => ApiLookupDdlKind::Relation,
                "column" => ApiLookupDdlKind::Column,
                "function" => ApiLookupDdlKind::Function,
                "type" => ApiLookupDdlKind::Type,
                unexpected => {
                    return Err(anyhow::anyhow!(
                        "unexpected API lookup DDL kind {unexpected}"
                    ));
                }
            };
            Ok(ApiLookupDdlObject {
                kind,
                identity: row.try_get("identity")?,
            })
        })
        .collect()
}
