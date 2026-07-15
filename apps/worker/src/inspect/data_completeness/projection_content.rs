use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_storage::ActiveManifestEventSource;
use sqlx::PgPool;

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
    pub(super) raw_total_count: i64,
    pub(super) raw_scoped_counts: Vec<ProjectionScopeCount>,
    pub(super) servable_total_count: i64,
    pub(super) servable_scoped_counts: Vec<ProjectionScopeCount>,
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
    let counts = bigname_storage::load_projection_content_counts(pool, projection).await?;
    let raw_scoped_counts = counts
        .raw_scoped_counts
        .into_iter()
        .map(|entry| ProjectionScopeCount {
            scope: entry.scope,
            count: entry.count,
        })
        .collect::<Vec<_>>();
    let servable_scoped_counts = counts
        .servable_scoped_counts
        .into_iter()
        .map(|entry| ProjectionScopeCount {
            scope: entry.scope,
            count: entry.count,
        })
        .collect::<Vec<_>>();

    let expected_scopes = expected_scopes(projection, sources);
    let counts_by_scope = servable_scoped_counts
        .iter()
        .map(|entry| (entry.scope.as_str(), entry.count))
        .collect::<BTreeMap<_, _>>();
    let missing_scopes = expected_scopes
        .iter()
        .filter(|scope| counts_by_scope.get(scope.as_str()).copied().unwrap_or(0) == 0)
        .cloned()
        .collect();

    Ok(ProjectionTableContent {
        projection: projection.to_owned(),
        scope_kind: counts.scope_kind,
        raw_total_count: counts.raw_total_count,
        raw_scoped_counts,
        servable_total_count: counts.servable_total_count,
        servable_scoped_counts,
        expected_scopes,
        missing_scopes,
    })
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
        "permissions_current" | "record_inventory_current" | "resolver_current" => matching
            .into_iter()
            .map(|source| source.chain.clone())
            .collect::<BTreeSet<_>>(),
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
