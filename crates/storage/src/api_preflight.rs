use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApiLookupDdlKind {
    Relation,
    Function,
    Type,
}

impl ApiLookupDdlKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Relation => "relation",
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
        WITH required(kind, identity, needed) AS (
            VALUES
                ('relation', 'bigname_phase.chain_heads', 'always'),
                ('relation', 'bigname_phase.chain_header_audit', 'always'),
                ('relation', 'bigname_phase.chain_lineage', 'always'),
                ('relation', 'bigname_phase.chain_phase_state', 'always'),
                ('relation', 'bigname_phase.normalized_events', 'always'),
                ('relation', 'bigname_phase.migration_event_associations', 'always'),
                ('relation', 'bigname_phase.name_current', 'always'),
                ('relation', 'bigname_phase.address_names_current', 'always'),
                ('relation', 'bigname_phase.address_records_current', 'always'),
                ('relation', 'bigname_phase.children_current', 'always'),
                ('relation', 'bigname_phase.child_registration_events', 'always'),
                ('relation', 'bigname_phase.permissions_current', 'always'),
                ('relation', 'bigname_phase.permissions_current_resource_summary', 'always'),
                ('relation', 'bigname_phase.account_permission_state_current', 'always'),
                ('relation', 'bigname_phase.primary_names_current', 'always'),
                ('relation', 'bigname_phase.resolver_current', 'always'),
                ('relation', 'bigname_phase.name_surfaces', 'always'),
                ('relation', 'bigname_phase.resources', 'always'),
                ('relation', 'bigname_phase.surface_bindings', 'always'),
                ('relation', 'bigname_phase.token_lineages', 'always'),
                ('relation', 'bigname_phase.record_inventory_current', 'always'),
                ('relation', 'bigname_phase.service_heartbeats', 'always'),
                ('relation', 'bigname_phase.manifest_versions', 'always'),
                ('relation', 'bigname_phase.manifest_contract_instances', 'always'),
                ('relation', 'bigname_phase.contract_instance_addresses', 'always'),
                ('relation', 'bigname_phase.resolution_divergences', 'always'),
                ('relation', 'bigname_phase.project_family_marker', 'families'),
                -- Read only by the owned key family readers under the switch.
                ('relation', 'bigname_phase.discovery_edges', 'families'),
                ('relation', 'bigname_phase.label_preimages', 'families'),
                ('relation', 'bigname_phase.migration_discovery_associations', 'families'),
                ('relation', 'bigname_phase.project_account_approval', 'families'),
                ('relation', 'bigname_phase.project_address_controller_candidate', 'families'),
                ('relation', 'bigname_phase.project_address_name_index', 'families'),
                ('relation', 'bigname_phase.project_address_record_id_index', 'families'),
                ('relation', 'bigname_phase.project_address_record_node_index', 'families'),
                ('relation', 'bigname_phase.project_binding_candidate', 'families'),
                ('relation', 'bigname_phase.project_child_edge_candidate', 'families'),
                ('relation', 'bigname_phase.project_child_registration_state', 'families'),
                ('relation', 'bigname_phase.project_claim_normalization', 'families'),
                ('relation', 'bigname_phase.project_grant', 'families'),
                ('relation', 'bigname_phase.project_lifecycle_association', 'families'),
                ('relation', 'bigname_phase.project_lifecycle_event', 'families'),
                ('relation', 'bigname_phase.project_lifecycle_key_state', 'families'),
                ('relation', 'bigname_phase.project_lifecycle_triple_summary', 'families'),
                ('relation', 'bigname_phase.project_name_alias', 'families'),
                ('relation', 'bigname_phase.project_name_history', 'families'),
                ('relation', 'bigname_phase.project_name_state', 'families'),
                ('relation', 'bigname_phase.project_name_summary', 'families'),
                ('relation', 'bigname_phase.project_named_resource_pointer', 'families'),
                ('relation', 'bigname_phase.project_node_record_partition', 'families'),
                ('relation', 'bigname_phase.project_node_record_value', 'families'),
                ('relation', 'bigname_phase.project_parent_subregistry', 'families'),
                ('relation', 'bigname_phase.project_record_id_value', 'families'),
                ('relation', 'bigname_phase.project_registry_binding_observation', 'families'),
                ('relation', 'bigname_phase.project_registry_node_state', 'families'),
                ('relation', 'bigname_phase.project_registry_owner_event', 'families'),
                ('relation', 'bigname_phase.project_registry_pointer', 'families'),
                ('relation', 'bigname_phase.project_resolver_alias', 'families'),
                ('relation', 'bigname_phase.project_resolver_classification', 'families'),
                ('relation', 'bigname_phase.project_resolver_link', 'families'),
                ('relation', 'bigname_phase.project_resource_admin_aggregate', 'families'),
                ('relation', 'bigname_phase.project_resource_pointer', 'families'),
                ('relation', 'bigname_phase.project_reverse_node_claim', 'families'),
                ('relation', 'bigname_phase.project_reverse_tuple', 'families'),
                ('relation', 'bigname_phase.project_wrapper_state', 'families'),
                (
                    'function',
                    'bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text)',
                    'always'
                ),
                (
                    'function',
                    'bigname_phase.write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)',
                    'always'
                ),
                ('type', 'bigname_phase.canonicality_state', 'always')
        )
        SELECT kind, identity
        FROM required
        WHERE (needed = 'always' OR (needed = 'families') = $1)
          AND CASE kind
            WHEN 'relation' THEN CASE WHEN to_regnamespace(split_part(identity, '.', 1)) IS NULL THEN TRUE
                WHEN NOT has_schema_privilege(current_user, to_regnamespace(split_part(identity, '.', 1)), 'USAGE') THEN TRUE
                WHEN to_regclass(identity) IS NULL THEN TRUE ELSE identity <> 'bigname_phase.resolution_divergences'
                    AND NOT has_table_privilege(current_user, identity, 'SELECT') END
            WHEN 'function' THEN CASE WHEN to_regnamespace(split_part(identity, '.', 1)) IS NULL THEN TRUE WHEN NOT has_schema_privilege(current_user, to_regnamespace(split_part(identity, '.', 1)), 'USAGE') THEN TRUE ELSE to_regprocedure(identity) IS NULL END
            WHEN 'type' THEN CASE WHEN to_regnamespace(split_part(identity, '.', 1)) IS NULL THEN TRUE WHEN NOT has_schema_privilege(current_user, to_regnamespace(split_part(identity, '.', 1)), 'USAGE') THEN TRUE ELSE to_regtype(identity) IS NULL END
        END
        ORDER BY kind, identity
        "#,
    )
    // The family marker and the owned key families are serving reads only while the
    // publication switch is on.
    .bind(crate::publication_source::serve_from_families())
    .fetch_all(pool)
    .await
    .context("failed to inspect required API lookup DDL")?;

    rows.into_iter()
        .map(|row| {
            let kind = match row.try_get::<&str, _>("kind")? {
                "relation" => ApiLookupDdlKind::Relation,
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
