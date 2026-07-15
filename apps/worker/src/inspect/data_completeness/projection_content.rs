use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::ActiveManifestEventSource;
use sqlx::{PgPool, Row};

use crate::replay::ALL_CURRENT_PROJECTION_ORDER;

#[cfg(test)]
mod tests;

const CHILDREN_EVENT_KINDS: &[&str] = &[
    "ParentChanged",
    "RegistrationGranted",
    "RegistrationReleased",
    "RegistrationRenewed",
    "SubregistryChanged",
];
const PERMISSIONS_EVENT_KINDS: &[&str] = &[
    "PermissionChanged",
    "PermissionScopeChanged",
    "RootPermissionChanged",
];
const RECORD_INVENTORY_EVENT_KINDS: &[&str] =
    &["RecordChanged", "RecordVersionChanged", "ResolverChanged"];
const RESOLVER_EVENT_KINDS: &[&str] = &[
    "AliasChanged",
    "PermissionChanged",
    "PermissionScopeChanged",
    "ResolverChanged",
];
const ADDRESS_NAMES_EVENT_KINDS: &[&str] = &[
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    "PermissionChanged",
    "PermissionScopeChanged",
    "RegistrationGranted",
    "TokenControlTransferred",
    "TokenRegenerated",
];
const PRIMARY_NAME_EVENT_KINDS: &[&str] = &["ReverseChanged"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProjectionScopeCount {
    pub(super) scope: String,
    pub(super) count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProjectionTableContent {
    pub(super) projection: String,
    pub(super) scope_kind: &'static str,
    pub(super) total_count: i64,
    pub(super) scoped_counts: Vec<ProjectionScopeCount>,
    pub(super) expected_scopes: Vec<String>,
    pub(super) missing_scopes: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ProjectionContentInspection {
    pub(super) tables: Vec<ProjectionTableContent>,
}

impl ProjectionContentInspection {
    pub(super) fn complete(&self) -> bool {
        self.tables.len() == ALL_CURRENT_PROJECTION_ORDER.len()
            && self
                .tables
                .iter()
                .zip(ALL_CURRENT_PROJECTION_ORDER)
                .all(|(table, expected)| {
                    table.projection == *expected && table.missing_scopes.is_empty()
                })
    }
}

pub(super) async fn load_projection_content(
    pool: &PgPool,
    sources: &[ActiveManifestEventSource],
) -> Result<ProjectionContentInspection> {
    let mut tables = Vec::with_capacity(ALL_CURRENT_PROJECTION_ORDER.len());
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        tables.push(load_projection_table(pool, projection, sources).await?);
    }
    Ok(ProjectionContentInspection { tables })
}

async fn load_projection_table(
    pool: &PgPool,
    projection: &str,
    sources: &[ActiveManifestEventSource],
) -> Result<ProjectionTableContent> {
    let (scope_kind, scoped_counts) = match projection {
        "name_current" | "children_current" | "address_names_current" | "primary_names_current" => {
            (
                "namespace",
                load_grouped_counts(pool, projection, "namespace").await?,
            )
        }
        "resolver_current" => (
            "chain",
            load_grouped_counts(pool, projection, "chain_id").await?,
        ),
        "permissions_current" | "record_inventory_current" => {
            let count = load_global_count(pool, projection).await?;
            (
                "global",
                (count > 0)
                    .then_some(ProjectionScopeCount {
                        scope: "global".to_owned(),
                        count,
                    })
                    .into_iter()
                    .collect(),
            )
        }
        _ => bail!("current projection {projection} has no content-inspection rule"),
    };

    let expected_scopes = expected_scopes(projection, sources);
    let counts_by_scope = scoped_counts
        .iter()
        .map(|entry| (entry.scope.as_str(), entry.count))
        .collect::<BTreeMap<_, _>>();
    let missing_scopes = expected_scopes
        .iter()
        .filter(|scope| counts_by_scope.get(scope.as_str()).copied().unwrap_or(0) == 0)
        .cloned()
        .collect();
    let total_count = scoped_counts.iter().map(|entry| entry.count).sum();

    Ok(ProjectionTableContent {
        projection: projection.to_owned(),
        scope_kind,
        total_count,
        scoped_counts,
        expected_scopes,
        missing_scopes,
    })
}

async fn load_grouped_counts(
    pool: &PgPool,
    projection: &str,
    scope_column: &str,
) -> Result<Vec<ProjectionScopeCount>> {
    let query = format!(
        "SELECT {scope_column} AS scope, COUNT(*)::BIGINT AS count \
         FROM {projection} GROUP BY {scope_column} ORDER BY {scope_column}"
    );
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to count {projection} by {scope_column}"))?;
    rows.into_iter()
        .map(|row| {
            Ok(ProjectionScopeCount {
                scope: row.try_get("scope")?,
                count: row.try_get("count")?,
            })
        })
        .collect()
}

async fn load_global_count(pool: &PgPool, projection: &str) -> Result<i64> {
    sqlx::query_scalar(&format!("SELECT COUNT(*)::BIGINT FROM {projection}"))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {projection}"))
}

fn expected_scopes(projection: &str, sources: &[ActiveManifestEventSource]) -> Vec<String> {
    let feeds_projection = |source: &&ActiveManifestEventSource| match projection {
        // `name_current` starts from canonical name surfaces. Every event-producing active
        // source is therefore a namespace content expectation, matching the existing gate.
        "name_current" => true,
        "children_current" => source_declares_any(source, CHILDREN_EVENT_KINDS),
        "permissions_current" => source_declares_any(source, PERMISSIONS_EVENT_KINDS),
        "record_inventory_current" => source_declares_any(source, RECORD_INVENTORY_EVENT_KINDS),
        "resolver_current" => source_declares_any(source, RESOLVER_EVENT_KINDS),
        "address_names_current" => source_declares_any(source, ADDRESS_NAMES_EVENT_KINDS),
        "primary_names_current" => source_declares_any(source, PRIMARY_NAME_EVENT_KINDS),
        _ => false,
    };
    let matching = sources.iter().filter(feeds_projection).collect::<Vec<_>>();
    let scopes = match projection {
        "name_current" | "children_current" | "address_names_current" | "primary_names_current" => {
            matching
                .into_iter()
                .map(|source| source.namespace.clone())
                .collect::<BTreeSet<_>>()
        }
        "resolver_current" => matching
            .into_iter()
            .map(|source| source.chain.clone())
            .collect::<BTreeSet<_>>(),
        "permissions_current" | "record_inventory_current" => {
            if matching.is_empty() {
                BTreeSet::new()
            } else {
                BTreeSet::from(["global".to_owned()])
            }
        }
        _ => BTreeSet::new(),
    };
    scopes.into_iter().collect()
}

fn source_declares_any(source: &ActiveManifestEventSource, event_kinds: &[&str]) -> bool {
    source
        .normalized_event_kinds
        .iter()
        .any(|kind| event_kinds.contains(&kind.as_str()))
}
