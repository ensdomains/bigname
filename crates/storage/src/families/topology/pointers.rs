//! Minimal reads of the resource resolver pointer (`project_resource_pointer`) and the resolver
//! links (`project_resolver_link`). The records module (`families::records`, `links.rs`) now holds
//! its own readers under the same names: both take the newest link, but records returns eager
//! default candidates and provenance and optional wildcard fields, where these return narrower
//! views. The two contracts must be reconciled, with explicit adapters and comparison tests, before
//! one copy replaces the other.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

const ROOT_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// The resource's latest non-zero pointer and its version boundary, the wildcard source.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyWildcardSource {
    /// The latest pointer whose resolver is neither empty nor the zero address. When
    /// `nonzero_position` is populated by the F5 reducer (crates/project/src/families/resolver.rs),
    /// `nonzero_resolver_address` is non-null, nonempty and not the zero address. Both fields may
    /// be null before any qualifying pointer. The reader passes a manually supplied null or empty
    /// address with a populated position through unchanged; no producer writes one. The served
    /// wildcard lateral does admit a null or empty pointer (docs/projections.md, F5).
    pub nonzero_resolver_address: Option<String>,
    pub nonzero_position: Value,
    /// The latest RecordVersionChanged or ResolverChanged on the resource, zero pointers included.
    pub boundary_kind: String,
    pub boundary_position: Value,
    /// The boundary block's timestamp as `to_jsonb` renders it.
    pub boundary_block_timestamp: Value,
}

/// The wildcard source of a resource, when it has both a non-zero pointer and a boundary.
pub async fn load_family_wildcard_source(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<FamilyWildcardSource>> {
    let mut conn = pool.acquire().await?;
    load_family_wildcard_source_on(&mut conn, chain_id, resource_id).await
}

pub(crate) async fn load_family_wildcard_source_on(
    conn: &mut PgConnection,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<FamilyWildcardSource>> {
    let row: Option<(Option<String>, Value, String, Value, Value)> = sqlx::query_as(
        "SELECT nonzero_resolver_address, nonzero_position, boundary_kind, boundary_position,
                to_jsonb(boundary_block_timestamp)
         FROM bigname_phase.project_resource_pointer
         WHERE chain_id = $1 AND resource_id = $2
           AND nonzero_position IS NOT NULL AND boundary_position IS NOT NULL
           AND boundary_kind IS NOT NULL",
    )
    .bind(chain_id)
    .bind(resource_id)
    .fetch_optional(&mut *conn)
    .await
    .with_context(|| format!("failed to load the wildcard source of {resource_id}"))?;
    Ok(row.map(
        |(
            nonzero_resolver_address,
            nonzero_position,
            boundary_kind,
            boundary_position,
            boundary_block_timestamp,
        )| FamilyWildcardSource {
            nonzero_resolver_address,
            nonzero_position,
            boundary_kind,
            boundary_position,
            boundary_block_timestamp,
        },
    ))
}

/// One `project_resolver_link` row: the latest `Linked` for a node at a resolver, record id `0`
/// a clear. `storage_model` is a bigname annotation (docs/glossary.md, "Storage model"), not
/// chain state: the record-ID adapter
/// stamps `resolver_record_id` on every `Linked`
/// (crates/adapters/src/schema_v2/protocol/v2_record_resolver.rs, `metadata`), and no producer
/// writes another value. The reads never consult it.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyLink {
    pub node: String,
    pub record_id: String,
    pub storage_model: Option<String>,
    pub block_number: i64,
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
    pub event_identity: String,
    pub normalized_event_id: Option<i64>,
}

impl FamilyLink {
    /// Whether this link serves its record: any link but a clear (record id `0`), as the chain
    /// serves it.
    pub fn serves(&self) -> bool {
        self.record_id != "0"
    }
}

/// The exact-then-default link selection for one name at a record-ID resolver.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkSelection {
    /// The latest link at the name's own node, a clear included.
    pub exact: Option<FamilyLink>,
    /// The latest link at the empty-name node, read only when the exact link serves no record.
    pub default: Option<FamilyLink>,
}

impl LinkSelection {
    /// The link whose record serves the name: the exact link when it serves, else the default
    /// link when it serves.
    pub fn selected(&self) -> Option<&FamilyLink> {
        self.exact
            .as_ref()
            .filter(|link| link.serves())
            .or_else(|| self.default.as_ref().filter(|link| link.serves()))
    }
}

/// Two probes of `project_resolver_link`: the name's node, then the empty-name node when the
/// exact link is absent or a clear. This is the chain's rule: the resolver keeps one record id
/// per node (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L96-L97 @
/// ens_v2@a971bd64), each `Linked` overwrites it (upstream:
/// .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L363-L367 @ ens_v2@a971bd64), and
/// resolution serves that record, consulting the default node only when it is 0 (upstream:
/// .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L380-L387 @ ens_v2@a971bd64). So
/// the newest link per (resolver, node) wins (Tate, 2026-09-26), and each probe reads the one row
/// the F7 reducer keeps there (crates/project/src/families/records.rs, `link`; the F7 row in
/// docs/glossary.md). `storage_model` is an annotation and plays no part. `None` when neither
/// probe finds a row.
pub async fn load_family_link_selection(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namehash: &str,
) -> Result<Option<LinkSelection>> {
    let resolver_address = resolver_address.to_ascii_lowercase();
    let exact = load_link(
        pool,
        chain_id,
        &resolver_address,
        &namehash.to_ascii_lowercase(),
    )
    .await?;
    let default = if exact.as_ref().is_some_and(FamilyLink::serves) {
        None
    } else {
        load_link(pool, chain_id, &resolver_address, ROOT_NODE).await?
    };
    Ok((exact.is_some() || default.is_some()).then_some(LinkSelection { exact, default }))
}

async fn load_link(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    node: &str,
) -> Result<Option<FamilyLink>> {
    type LinkRow = (
        String,
        String,
        Option<String>,
        i64,
        Option<i64>,
        Option<i64>,
        String,
        Option<i64>,
    );
    let row: Option<LinkRow> = sqlx::query_as(
        "SELECT node, record_id, storage_model, block_number, transaction_index, log_index,
                event_identity, normalized_event_id
         FROM bigname_phase.project_resolver_link
         WHERE chain_id = $1 AND resolver_address = $2 AND node = $3",
    )
    .bind(chain_id)
    .bind(resolver_address)
    .bind(node)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the link of {node} at {resolver_address}"))?;
    Ok(row.map(
        |(
            node,
            record_id,
            storage_model,
            block_number,
            transaction_index,
            log_index,
            event_identity,
            normalized_event_id,
        )| FamilyLink {
            node,
            record_id,
            storage_model,
            block_number,
            transaction_index,
            log_index,
            event_identity,
            normalized_event_id,
        },
    ))
}
