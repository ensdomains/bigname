//! The permission reads `GET /v1/permissions` serves, by the
//! [publication switch](crate::publication_source): off, the served tables
//! (`permissions_current`, `account_permission_state_current`,
//! `permissions_current_resource_summary`); on, the owned key families
//! (`families::control::permissions::page`), with the same page, cursor and summary shapes.
use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

use super::types::{
    EffectivePermissionsAccountResourcePage, PermissionsCurrentAccountResourceCursor,
    PermissionsCurrentResourceSummary,
};
use crate::families::control::permissions::page::{
    load_family_effective_permissions_page, load_family_permission_summaries,
};

/// The effective-permission page of `subject` and/or `resource_id`.
pub async fn load_serving_effective_permissions_page(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<EffectivePermissionsAccountResourcePage> {
    load_family_effective_permissions_page(pool, subject, resource_id, namespace, cursor, page_size)
        .await
}

/// The permission summaries of `resource_ids`, keyed by resource.
pub async fn load_serving_permission_summaries(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, PermissionsCurrentResourceSummary>> {
    load_family_permission_summaries(pool, resource_ids).await
}
