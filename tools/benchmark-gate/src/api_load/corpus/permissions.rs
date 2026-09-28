use anyhow::{Context, Result};
use bigname_storage::{
    EffectivePermissionScope, families::name::load_family_names_by_logical_name_ids,
};
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct PermissionTarget {
    pub(crate) address: String,
    pub(crate) registration_id: String,
    pub(crate) retained_registration: bool,
    pub(crate) namespace: String,
    pub(crate) name: String,
}

pub(super) async fn load(pool: &PgPool, limit: i64) -> Result<Vec<PermissionTarget>> {
    let limit = usize::try_from(limit).context("permission corpus limit must be nonnegative")?;
    let mut selected = BTreeMap::new();
    let mut audit = BTreeSet::new();
    let mut after = None::<Uuid>;
    loop {
        let resources: Vec<Uuid> = sqlx::query_scalar(
            "SELECT DISTINCT resource_id FROM bigname_phase.project_grant
             WHERE ($1::uuid IS NULL OR resource_id > $1) ORDER BY resource_id LIMIT 256",
        )
        .bind(after)
        .fetch_all(pool)
        .await?;
        let Some(last) = resources.last() else {
            break;
        };
        after = Some(*last);
        for resource in resources {
            let ids: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT logical_name_id FROM bigname_phase.project_binding_candidate
                 WHERE resource_id = $1 ORDER BY logical_name_id",
            )
            .bind(resource)
            .fetch_all(pool)
            .await?;
            let names: Vec<_> = load_family_names_by_logical_name_ids(pool, &ids)
                .await?
                .into_values()
                .filter(|name| name.resource_id == Some(resource))
                .collect();
            let mut cursor = None;
            loop {
                let page = bigname_storage::load_serving_effective_permissions_page(
                    pool,
                    None,
                    Some(resource),
                    None,
                    cursor.as_ref(),
                    256,
                )
                .await?;
                for row in page.rows {
                    // Registry account operators were not part of the direct-grant corpus.
                    if !matches!(row.scope, EffectivePermissionScope::Direct(_))
                        || row.subject.len() != 42
                        || !row.subject.starts_with("0x")
                        || !row.subject[2..]
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit())
                    {
                        continue;
                    }
                    if names.is_empty() {
                        audit.insert(resource.to_string());
                        if audit.len() > limit {
                            audit.pop_last();
                        }
                    }
                    for name in names.iter().filter(|name| super::readers::supported(name)) {
                        let key = (
                            row.subject.clone(),
                            resource.to_string(),
                            name.namespace.clone(),
                            name.canonical_display_name.clone(),
                        );
                        selected.insert(
                            key,
                            PermissionTarget {
                                address: row.subject.clone(),
                                registration_id: resource.to_string(),
                                retained_registration: false,
                                namespace: name.namespace.clone(),
                                name: name.canonical_display_name.clone(),
                            },
                        );
                        if selected.len() > limit {
                            selected.pop_last();
                        }
                    }
                }
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }
    }
    let mut targets: Vec<_> = selected.into_values().collect();
    let audit: Vec<_> = audit.into_iter().collect();
    if !audit.is_empty() {
        for (index, target) in targets.iter_mut().enumerate().step_by(2) {
            target.registration_id = audit[(index / 2) % audit.len()].clone();
            target.retained_registration = true;
        }
    }
    Ok(targets)
}
