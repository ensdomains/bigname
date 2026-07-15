use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

/// Persisted current effective permission row for one resource-anchored subject and scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentRow {
    pub resource_id: Uuid,
    pub subject: String,
    pub scope: PermissionScope,
    pub effective_powers: Value,
    pub grant_source: Value,
    pub revocation_source: Option<Value>,
    pub inheritance_path: Value,
    pub transfer_behavior: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Projection-owned support and authority metadata for one permission resource.
///
/// This row exists independently of holder rows so an empty `permissions_current` collection can
/// still report whether it is authoritative. `root_resource_id` is the optional ENSv2 registry
/// root whose permission rows are composed by app-facing role reads. Chain-position and
/// canonicality fields preserve the authority input evidence even when no holder row exists.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct PermissionsCurrentResourceSummary {
    pub resource_id: Uuid,
    pub authority_kind: Option<String>,
    pub root_resource_id: Option<Uuid>,
    pub coverage: Value,
    pub provenance: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Keyset cursor fields for the frozen subject/scope permissions order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentKeysetCursor {
    pub subject: String,
    pub scope: String,
}

/// Keyset cursor fields for account/resource app-facing role rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentAccountResourceCursor {
    pub subject: String,
    pub resource_id: Uuid,
    pub scope: String,
}

/// Compact summary over the full filtered permissions collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentFullFilterSummary {
    pub row_count: i64,
    pub provenance: Vec<Value>,
    pub coverage: Option<Value>,
    pub chain_positions: Vec<Value>,
    pub canonicality_summaries: Vec<Value>,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

/// Bounded keyset page plus full-filter summary data for permissions reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentPage {
    pub rows: Vec<PermissionsCurrentRow>,
    pub next_cursor: Option<PermissionsCurrentKeysetCursor>,
    pub summary: PermissionsCurrentFullFilterSummary,
}

/// Bounded account/resource role page plus full-filter summary data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentAccountResourcePage {
    pub rows: Vec<PermissionsCurrentRow>,
    pub next_cursor: Option<PermissionsCurrentAccountResourceCursor>,
    pub summary: PermissionsCurrentFullFilterSummary,
}

/// Stable storage representation for permission scope keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PermissionScope {
    Root,
    Registry,
    Resource,
    Resolver {
        chain_id: String,
        resolver_address: String,
    },
    RecordManager {
        chain_id: String,
        manager_address: String,
    },
    MigrationDerived {
        predecessor_resource_id: Uuid,
    },
    TransportDerived {
        transport: String,
    },
}

impl PermissionScope {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Registry => "registry",
            Self::Resource => "resource",
            Self::Resolver { .. } => "resolver",
            Self::RecordManager { .. } => "record_manager",
            Self::MigrationDerived { .. } => "migration_derived",
            Self::TransportDerived { .. } => "transport_derived",
        }
    }

    pub fn storage_key(&self) -> String {
        match self {
            Self::Root => "root".to_owned(),
            Self::Registry => "registry".to_owned(),
            Self::Resource => "resource".to_owned(),
            Self::Resolver {
                chain_id,
                resolver_address,
            } => format!(
                "resolver:{chain_id}:{}",
                resolver_address.to_ascii_lowercase()
            ),
            Self::RecordManager {
                chain_id,
                manager_address,
            } => format!(
                "record_manager:{chain_id}:{}",
                manager_address.to_ascii_lowercase()
            ),
            Self::MigrationDerived {
                predecessor_resource_id,
            } => format!("migration_derived:{predecessor_resource_id}"),
            Self::TransportDerived { transport } => format!("transport_derived:{transport}"),
        }
    }

    pub fn detail(&self) -> Value {
        match self {
            Self::Root | Self::Registry | Self::Resource => json!({}),
            Self::Resolver {
                chain_id,
                resolver_address,
            } => json!({
                "chain_id": chain_id,
                "resolver_address": resolver_address.to_ascii_lowercase(),
            }),
            Self::RecordManager {
                chain_id,
                manager_address,
            } => json!({
                "chain_id": chain_id,
                "manager_address": manager_address.to_ascii_lowercase(),
            }),
            Self::MigrationDerived {
                predecessor_resource_id,
            } => json!({
                "predecessor_resource_id": predecessor_resource_id,
            }),
            Self::TransportDerived { transport } => json!({
                "transport": transport,
            }),
        }
    }

    pub(super) fn parse(scope_kind: &str, scope_detail: &Value) -> Result<Self> {
        match scope_kind {
            "root" => Ok(Self::Root),
            "registry" => Ok(Self::Registry),
            "resource" => Ok(Self::Resource),
            "resolver" => {
                let chain_id = json_text_field(scope_detail, "chain_id")?;
                let resolver_address = json_text_field(scope_detail, "resolver_address")?;
                Ok(Self::Resolver {
                    chain_id,
                    resolver_address: resolver_address.to_ascii_lowercase(),
                })
            }
            "record_manager" => {
                let chain_id = json_text_field(scope_detail, "chain_id")?;
                let manager_address = json_text_field(scope_detail, "manager_address")?;
                Ok(Self::RecordManager {
                    chain_id,
                    manager_address: manager_address.to_ascii_lowercase(),
                })
            }
            "migration_derived" => {
                let predecessor_resource_id = Uuid::parse_str(&json_text_field(
                    scope_detail,
                    "predecessor_resource_id",
                )?)
                .context(
                    "permissions_current scope_detail.predecessor_resource_id must be a UUID",
                )?;
                Ok(Self::MigrationDerived {
                    predecessor_resource_id,
                })
            }
            "transport_derived" => Ok(Self::TransportDerived {
                transport: json_text_field(scope_detail, "transport")?,
            }),
            _ => bail!("unknown permissions_current scope_kind {scope_kind}"),
        }
    }
}

impl From<&PermissionsCurrentRow> for PermissionsCurrentKeysetCursor {
    fn from(row: &PermissionsCurrentRow) -> Self {
        Self {
            subject: row.subject.clone(),
            scope: row.scope.storage_key(),
        }
    }
}

impl From<&PermissionsCurrentRow> for PermissionsCurrentAccountResourceCursor {
    fn from(row: &PermissionsCurrentRow) -> Self {
        Self {
            subject: row.subject.clone(),
            resource_id: row.resource_id,
            scope: row.scope.storage_key(),
        }
    }
}

fn json_text_field(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("permissions_current scope_detail must include {field}"))
}
