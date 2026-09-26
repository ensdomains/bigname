//! Minimal reads of the resource resolver pointer (`project_resource_pointer`) and the resolver
//! links (`project_resolver_link`). The full pointer and record readers are being written in
//! parallel under the same names and signatures; when both land, one copy of each is kept and
//! this file goes.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ROOT_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// The resource's current pointer after the "no resolver" rejection: its latest named
/// ResolverChanged, clears included, and nothing when that latest pointer is null, zero or empty
/// (crates/project/src/builders/name_topology.rs, the alias resolver lateral;
/// crates/project/src/builders/linked_records.rs, the pointer drop). An empty resolver means no
/// alias (Tate, 2026-09-26), the record pointer's rule; the ENSv2 registry adapter writes the
/// resolver through `nullable_address` (crates/adapters/src/schema_v2/protocol/v2_registry.rs),
/// so only a fixture can hold an empty one.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyAliasSourcePointer {
    pub chain_id: String,
    pub resource_id: Uuid,
    pub resolver_address: String,
    pub pointer_position: Value,
    pub namespace: Option<String>,
    pub source_family: Option<String>,
    pub namehash: Option<String>,
}

/// The resource's current resolver pointer for the alias topology join: latest, then reject
/// null, zero or empty. Never the historical non-zero pointer, so a clear exposes no older
/// pointer.
pub async fn load_family_alias_source_pointer(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<FamilyAliasSourcePointer>> {
    type PointerRow = (
        Option<String>,
        Option<Value>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let row: Option<PointerRow> = sqlx::query_as(
        "SELECT resolver_address, pointer_position, namespace, source_family, namehash
             FROM bigname_phase.project_resource_pointer
             WHERE chain_id = $1 AND resource_id = $2 AND pointer_position IS NOT NULL",
    )
    .bind(chain_id)
    .bind(resource_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the resource pointer of {resource_id}"))?;
    Ok(row.and_then(
        |(resolver_address, pointer_position, namespace, source_family, namehash)| {
            let resolver_address = resolver_address
                .filter(|address| !address.is_empty() && address != ZERO_ADDRESS)?;
            Some(FamilyAliasSourcePointer {
                chain_id: chain_id.to_owned(),
                resource_id,
                resolver_address,
                pointer_position: pointer_position?,
                namespace,
                source_family,
                namehash,
            })
        },
    ))
}

/// The resource's latest non-zero pointer and its version boundary, the wildcard source.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyWildcardSource {
    /// The latest pointer whose resolver is neither empty nor the zero address. When
    /// `nonzero_position` is populated by the F5 reducer (crates/project/src/families/resolver.rs),
    /// `nonzero_resolver_address` is non-null, nonempty and not the zero address. Both fields may
    /// be null before any qualifying pointer. The reader defensively passes through manually
    /// supplied null or empty addresses with a populated position, which the tests' null and empty
    /// cases cover. The served wildcard lateral does admit a null or empty pointer
    /// (docs/projections.md, F5).
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
    .fetch_optional(pool)
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

/// The exact-then-default link selection for one name at a record-ID resolver
/// (crates/project/src/builders/linked_records.rs, `project_selected_records`).
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
/// docs/glossary.md). `storage_model` is an annotation and plays no part. Today's link staging
/// drops links not annotated `resolver_record_id`
/// (crates/project/src/builders/resolver/link_summary.rs), so for such a row, which only a fixture
/// writes, the served read serves an older link instead. `None` when neither probe finds a row.
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
