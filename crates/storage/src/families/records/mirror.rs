//! The ENSv1 mirror walk over F4 (record_inventory/mirror.rs). A resource whose serving pointer is
//! an ENSv2 registry or root pointer at a resolver classified as an ENSv1 mirror resolver serves
//! the records of the ENSv1 resolver the mirror would call: the walk consults the queried name and
//! each proper ancestor below the root, deepest first, reads the ENSv1 registry's current resolver
//! of each node from `project_registry_pointer` (clears included, so a cleared node falls through),
//! and keeps the nearest node with a resolver. Records are derived only when that resolver was
//! found at the queried node itself, is a supported ENSv1 resolver declared in its pointer's
//! namespace, and is not itself a mirror; otherwise the row is unsupported with the reason.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)
use alloy_primitives::{B256, keccak256};
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row};

use super::{
    facts::{ResolverClassification, load_classification},
    is_cleared,
    serving::ServingPointer,
};

const V2_POINTER_FAMILIES: [&str; 2] = ["ens_v2_registry_l1", "ens_v2_root_l1"];

/// The node the walk selected: the nearest consulted node with a non-zero registry resolver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MirrorNearest {
    pub(crate) ancestor_depth: i32,
    pub(crate) mirrored_node: String,
    pub(crate) mirrored_name: String,
    pub(crate) mirrored_resource_id: Option<String>,
    pub(crate) mirrored_resolver_address: String,
    pub(crate) mirrored_pointer_event_id: Option<i64>,
    pub(crate) mirrored_pointer_source_family: String,
    pub(crate) mirrored_pointer_namespace: String,
    pub(crate) mirrored_block_number: i64,
    pub(crate) forwarding: &'static str,
    pub(crate) mirrored_unsupported_reason: Option<String>,
}

/// What the mirror evaluation decided for one mirror pointer.
#[derive(Clone, Debug)]
pub(crate) struct MirrorSelection {
    /// The mirror resolver's own classification.
    pub(crate) mirror: ResolverClassification,
    pub(crate) mirror_namespace_matches: bool,
    pub(crate) nearest: Option<MirrorNearest>,
}

impl MirrorSelection {
    /// The pointer re-pointed at the mirrored ENSv1 resolver, when the records can be derived.
    pub(crate) fn substituted(&self, pointer: &ServingPointer) -> Option<ServingPointer> {
        let nearest = self.nearest.as_ref()?;
        (self.mirror.supported()
            && self.mirror_namespace_matches
            && nearest.mirrored_unsupported_reason.is_none())
        .then(|| ServingPointer {
            namespace: nearest.mirrored_pointer_namespace.clone(),
            source_family: nearest.mirrored_pointer_source_family.clone(),
            resolver_address: nearest.mirrored_resolver_address.clone(),
            block_number: pointer.block_number.max(nearest.mirrored_block_number),
            ..pointer.clone()
        })
    }

    /// The reason an underivable mirror row is unsupported.
    pub(crate) fn unsupported_reason(&self) -> String {
        if !self.mirror.supported() {
            self.mirror
                .unsupported_reason
                .clone()
                .unwrap_or_else(|| "resolver_classification_missing".to_owned())
        } else if !self.mirror_namespace_matches {
            "resolver_classification_missing".to_owned()
        } else {
            "mirrored_resolver_not_projected".to_owned()
        }
    }

    /// `provenance.mirror` of the resource's row, nulls stripped.
    pub(crate) fn provenance(&self, pointer: &ServingPointer) -> Value {
        let mut mirror = Map::new();
        let mut put = |key: &str, value: Value| {
            if !value.is_null() {
                mirror.insert(key.to_owned(), value);
            }
        };
        put("resolver_address", json!(pointer.resolver_address));
        put("mirrored_source_family", json!("ens_v1_resolver_l1"));
        put(
            "mirrored_registry_source_family",
            json!("ens_v1_registry_l1"),
        );
        put(
            "mirrored_registry_address",
            self.mirror
                .classification
                .pointer("/mirror/mirrored_registry_address")
                .filter(|value| !value.is_null())
                .map(|value| {
                    json!(
                        value
                            .as_str()
                            .map_or_else(|| value.to_string(), str::to_owned)
                    )
                })
                .unwrap_or(Value::Null),
        );
        put("queried_node", json!(pointer.namehash));
        if let Some(nearest) = &self.nearest {
            put("mirrored_node", json!(nearest.mirrored_node));
            put("mirrored_name", json!(nearest.mirrored_name));
            put("ancestor_depth", json!(nearest.ancestor_depth));
            put("forwarding", json!(nearest.forwarding));
            put(
                "mirrored_resolver_address",
                json!(nearest.mirrored_resolver_address),
            );
            put("mirrored_resource_id", json!(nearest.mirrored_resource_id));
            put(
                "mirrored_pointer_event_id",
                json!(nearest.mirrored_pointer_event_id),
            );
            put(
                "mirrored_pointer_source_family",
                json!(nearest.mirrored_pointer_source_family),
            );
            put(
                "mirrored_unsupported_reason",
                json!(nearest.mirrored_unsupported_reason),
            );
        }
        Value::Object(mirror)
    }
}

/// Whether `pointer` is a mirror pointer: an ENSv2 registry or root pointer at a resolver
/// classified as an ENSv1 mirror resolver.
pub(crate) fn is_mirror_pointer(
    pointer: &ServingPointer,
    classification: Option<&ResolverClassification>,
) -> bool {
    V2_POINTER_FAMILIES.contains(&pointer.source_family.as_str())
        && classification.is_some_and(|classification| {
            classification.field("source_family") == Some("ens_v2_resolver_l1")
                && classification.field("role") == Some("ensv1_mirror_resolver")
        })
}

/// Walk the ENSv1 registry for a mirror pointer: at most one `project_registry_pointer` probe per
/// label of the queried name, then one classification probe for the nearest resolver.
pub(crate) async fn evaluate_family_mirror(
    pool: &PgPool,
    chain_id: &str,
    pointer: &ServingPointer,
    mirror: ResolverClassification,
) -> Result<MirrorSelection> {
    let mirror_namespace_matches = mirror.declared_in(&pointer.namespace);
    let nearest = nearest(pool, chain_id, pointer).await?;
    let nearest = match nearest {
        Some(mut nearest) => {
            let classification =
                load_classification(pool, chain_id, &nearest.mirrored_resolver_address).await?;
            classify(&mut nearest, classification.as_ref());
            Some(nearest)
        }
        None => None,
    };
    Ok(MirrorSelection {
        mirror,
        mirror_namespace_matches,
        nearest,
    })
}

async fn nearest(
    pool: &PgPool,
    chain_id: &str,
    pointer: &ServingPointer,
) -> Result<Option<MirrorNearest>> {
    let surface = sqlx::query(
        "SELECT namespace, raw_labels, labelhashes FROM bigname_phase.name_surfaces
         WHERE logical_name_id = $1 AND chain_id = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
         LIMIT 1",
    )
    .bind(&pointer.logical_name_id)
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .context("failed to load the queried name of a mirror pointer")?;
    let Some(surface) = surface else {
        return Ok(None);
    };
    let namespace: String = surface.try_get("namespace")?;
    let raw_labels: Vec<String> = surface.try_get("raw_labels")?;
    let labelhashes: Vec<String> = surface.try_get("labelhashes")?;
    let (mut depths, mut nodes, mut labels) = (Vec::new(), Vec::new(), Vec::new());
    for depth in 0..raw_labels.len() {
        depths.push(i32::try_from(depth).unwrap_or(i32::MAX));
        nodes.push(suffix_namehash(
            &raw_labels[depth..],
            labelhashes.get(depth..).unwrap_or_default(),
        ));
        labels.push(Value::from(raw_labels[depth..].to_vec()));
    }
    let rows = sqlx::query(
        "SELECT walk.ancestor_depth, registry.node, surface.raw_name, registry.resource_id::text
                    AS resource_id,
                registry.resolver_address, registry.normalized_event_id, registry.source_family,
                registry.namespace, registry.block_number
         FROM unnest($3::int[], $4::text[], $5::jsonb[]) walk (ancestor_depth, node, labels)
         JOIN bigname_phase.name_surfaces surface
           ON surface.namespace = $2 AND surface.namehash = walk.node
          AND surface.chain_id = $1 AND to_jsonb(surface.raw_labels) = walk.labels
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
         JOIN bigname_phase.project_registry_pointer registry
           ON registry.chain_id = $1 AND registry.namespace = surface.namespace
          AND registry.node = walk.node
         ORDER BY walk.ancestor_depth ASC, registry.normalized_event_id DESC",
    )
    .bind(chain_id)
    .bind(&namespace)
    .bind(&depths)
    .bind(&nodes)
    .bind(&labels)
    .fetch_all(pool)
    .await
    .context("failed to walk the ENSv1 registry pointers of a mirror pointer")?;
    for row in rows {
        let resolver: String = row.try_get("resolver_address")?;
        if is_cleared(Some(&resolver)) {
            continue;
        }
        return Ok(Some(MirrorNearest {
            ancestor_depth: row.try_get("ancestor_depth")?,
            mirrored_node: row.try_get("node")?,
            mirrored_name: row.try_get("raw_name")?,
            mirrored_resource_id: row.try_get("resource_id")?,
            mirrored_resolver_address: resolver,
            mirrored_pointer_event_id: row.try_get("normalized_event_id")?,
            mirrored_pointer_source_family: row.try_get("source_family")?,
            mirrored_pointer_namespace: row.try_get("namespace")?,
            mirrored_block_number: row.try_get("block_number")?,
            forwarding: "direct_call",
            mirrored_unsupported_reason: None,
        }));
    }
    Ok(None)
}

/// The forwarding mode and the stopping rules of the mirror selection.
fn classify(nearest: &mut MirrorNearest, resolver: Option<&ResolverClassification>) {
    let extended =
        resolver.is_some_and(|resolver| resolver.has_read_feature("ensip10_extended_resolver"));
    nearest.forwarding = if extended {
        "extended_resolve"
    } else {
        "direct_call"
    };
    nearest.mirrored_unsupported_reason = match resolver {
        None => Some("resolver_classification_missing".to_owned()),
        Some(resolver) if resolver.field("role") == Some("ensv1_mirror_resolver") => {
            Some("mirrored_resolver_is_mirror".to_owned())
        }
        Some(resolver) if !resolver.supported() => Some(
            resolver
                .unsupported_reason
                .clone()
                .unwrap_or_else(|| "resolver_classification_missing".to_owned()),
        ),
        Some(resolver) if resolver.field("source_family") != Some("ens_v1_resolver_l1") => {
            Some("mirrored_resolver_not_ensv1".to_owned())
        }
        Some(resolver) if !resolver.declared_in(&nearest.mirrored_pointer_namespace) => {
            Some("resolver_classification_missing".to_owned())
        }
        Some(_) if nearest.ancestor_depth > 0 && extended => {
            Some("ensip10_extended_resolver".to_owned())
        }
        Some(_) if nearest.ancestor_depth > 0 => Some("ancestor_resolver_not_extended".to_owned()),
        Some(_) => None,
    };
}

/// The namehash of a name suffix. The stored labelhashes are used when each is a 32-byte hash, so
/// labels known only by their hash still resolve; otherwise the raw labels are hashed.
fn suffix_namehash(raw_labels: &[String], labelhashes: &[String]) -> String {
    let parsed = (labelhashes.len() == raw_labels.len())
        .then(|| {
            labelhashes
                .iter()
                .map(|labelhash| labelhash.parse::<B256>().ok())
                .collect::<Option<Vec<_>>>()
        })
        .flatten();
    let labelhashes = parsed.unwrap_or_else(|| {
        raw_labels
            .iter()
            .map(|label| keccak256(label.as_bytes()))
            .collect()
    });
    let node = labelhashes
        .iter()
        .rev()
        .fold(B256::ZERO, |parent, labelhash| {
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(parent.as_slice());
            input[32..].copy_from_slice(labelhash.as_slice());
            keccak256(input)
        });
    format!("{node:#x}")
}
