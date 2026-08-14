use anyhow::{Context, Result};
use bigname_storage::{
    DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER,
    DEFAULT_PERMISSIONS_CURRENT_READ_FILTER,
};
use sqlx::PgPool;

#[derive(Clone, Debug)]
pub(crate) struct PermissionTarget {
    pub(crate) address: String,
    pub(crate) registration_id: String,
    pub(crate) retained_registration: bool,
    pub(crate) namespace: String,
    pub(crate) name: String,
}

pub(super) async fn load(pool: &PgPool, limit: i64) -> Result<Vec<PermissionTarget>> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&sql())
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load permission-subject benchmark corpus")?;
    let mut targets = rows
        .into_iter()
        .map(
            |(address, registration_id, namespace, name)| PermissionTarget {
                address,
                registration_id,
                retained_registration: false,
                namespace,
                name,
            },
        )
        .collect::<Vec<_>>();
    let audit_registration_ids: Vec<String> = sqlx::query_scalar(&audit_registration_sql())
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load retained-registration permission benchmark corpus")?;
    if !audit_registration_ids.is_empty() {
        for (index, target) in targets.iter_mut().enumerate().step_by(2) {
            target.registration_id =
                audit_registration_ids[(index / 2) % audit_registration_ids.len()].clone();
            target.retained_registration = true;
        }
    }
    Ok(targets)
}

pub(super) fn sql() -> String {
    format!(
        r#"SELECT DISTINCT pc.subject, pc.resource_id::text, nc.namespace, nc.raw_name
           FROM bigname_phase.permissions_current pc
           JOIN bigname_phase.name_current nc
             ON nc.resource_id = pc.resource_id
           JOIN bigname_phase.name_surfaces surface
             ON surface.logical_name_id = nc.logical_name_id
           LEFT JOIN bigname_phase.resources resource
             ON resource.resource_id = nc.resource_id
           LEFT JOIN bigname_phase.surface_bindings binding
             ON binding.surface_binding_id = nc.surface_binding_id
           LEFT JOIN bigname_phase.token_lineages token_lineage
             ON token_lineage.token_lineage_id = nc.token_lineage_id
           {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
           WHERE pc.subject ~ '^0x[0-9A-Fa-f]{{40}}$'
             AND nc.support_status = 'supported'
             {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
             {DEFAULT_NAME_CURRENT_READ_FILTER}
           ORDER BY pc.subject, pc.resource_id::text, nc.namespace, nc.raw_name
           LIMIT $1"#
    )
}

fn audit_registration_sql() -> String {
    format!(
        r#"SELECT DISTINCT pc.resource_id::text
           FROM bigname_phase.permissions_current pc
           WHERE pc.subject ~ '^0x[0-9A-Fa-f]{{40}}$'
             {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
             AND NOT EXISTS (
                 SELECT 1
                 FROM bigname_phase.name_current nc
                 WHERE nc.resource_id = pc.resource_id
             )
           ORDER BY pc.resource_id::text
           LIMIT $1"#
    )
}
