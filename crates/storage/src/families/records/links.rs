//! Readers the edge and topology step shares: the resolver link selection over F7, and the alias
//! and wildcard views of the F5 resource pointer.
use anyhow::{Context, Result};
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{FamilyPosition, is_cleared, load_family_resource_pointer};

/// The empty-name node, `namehash("")`: a record linked there is the resolver's default record,
/// answering any node with no link of its own.
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L380-L386 @ ens_v2@a971bd64)
pub const DEFAULT_RECORD_NODE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

/// One `project_resolver_link` row: the latest `ResolverRecordLinked` of a resolver node, record
/// id `0` included as the unlinked state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyLink {
    pub node: String,
    pub record_id: String,
    pub position: FamilyPosition,
    pub normalized_event_id: Option<i64>,
}

/// The record id a resolver serves for a node: the exact link at the node unless it is absent or
/// a clear to `0`, else the default link at `namehash("")` (linked_records.rs,
/// `project_selected_records`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkSelection {
    /// The selected record id; null when the exact link is a clear and no default link exists.
    pub record_id: Option<String>,
    pub exact: Option<FamilyLink>,
    pub default: Option<FamilyLink>,
    /// The exact link's event whenever an exact link exists, a clear included.
    pub exact_link_event_id: Option<i64>,
    /// The default link's event, only when the exact link is absent or a clear.
    pub default_link_event_id: Option<i64>,
}

impl LinkSelection {
    /// The record id an active link selects: `None` when the selection is empty or a clear to
    /// `0`, as when the exact link is absent or a clear and the default link is a clear too. The
    /// raw [`LinkSelection::record_id`] keeps `0` in that case, and the clear events still
    /// contribute to the version boundary and the provenance through
    /// [`LinkSelection::contributing_links`].
    pub fn active_record_id(&self) -> Option<&str> {
        self.record_id
            .as_deref()
            .filter(|record_id| *record_id != "0")
    }

    /// The links that take part in the record selection: the exact link whenever it exists, and
    /// the default link when the exact link is absent or a clear. Both are version boundary
    /// candidates.
    pub fn contributing_links(&self) -> impl Iterator<Item = &FamilyLink> {
        let default_contributes = self
            .exact
            .as_ref()
            .is_none_or(|exact| exact.record_id == "0");
        self.exact
            .iter()
            .chain(self.default.iter().filter(move |_| default_contributes))
    }
}

/// The link selection of `resolver_address` for `namehash`: two probes of
/// `project_resolver_link`, exact then default. `None` when the resolver has neither link.
pub async fn load_family_link_selection(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namehash: &str,
) -> Result<Option<LinkSelection>> {
    let rows = sqlx::query(
        "SELECT node, record_id, block_number, transaction_index, log_index, event_identity,
                normalized_event_id
         FROM bigname_phase.project_resolver_link
         WHERE chain_id = $1 AND resolver_address = $2 AND node = ANY($3::text[])
           AND storage_model = 'resolver_record_id'",
    )
    .bind(chain_id)
    .bind(resolver_address.to_ascii_lowercase())
    .bind([
        namehash.to_ascii_lowercase(),
        DEFAULT_RECORD_NODE.to_owned(),
    ])
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load the family links of resolver {resolver_address}"))?;
    let mut exact = None;
    let mut default = None;
    for row in rows {
        let link = FamilyLink {
            node: row.try_get("node")?,
            record_id: row.try_get("record_id")?,
            position: FamilyPosition::from_row(&row)?,
            normalized_event_id: row.try_get("normalized_event_id")?,
        };
        if link.node == namehash.to_ascii_lowercase() {
            exact = Some(link.clone());
        }
        if link.node == DEFAULT_RECORD_NODE {
            default = Some(link);
        }
    }
    Ok(select(exact, default))
}

fn select(exact: Option<FamilyLink>, default: Option<FamilyLink>) -> Option<LinkSelection> {
    if exact.is_none() && default.is_none() {
        return None;
    }
    let exact_clears = exact.as_ref().is_none_or(|exact| exact.record_id == "0");
    let record_id = if exact_clears {
        default.as_ref().map(|link| link.record_id.clone())
    } else {
        exact.as_ref().map(|link| link.record_id.clone())
    };
    Some(LinkSelection {
        record_id,
        exact_link_event_id: exact.as_ref().and_then(|link| link.normalized_event_id),
        default_link_event_id: default
            .as_ref()
            .filter(|_| exact_clears)
            .and_then(|link| link.normalized_event_id),
        exact,
        default,
    })
}

/// The pointer the alias topology join reads: the resource's current pointer, and nothing when
/// that pointer is a clear (name_topology.rs, the binding resource's latest `ResolverChanged`,
/// then zero rejected). An older non-zero pointer is never exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyAliasSourcePointer {
    pub resource_id: Uuid,
    pub resolver_address: String,
    pub pointer_position: FamilyPosition,
    pub namespace: Option<String>,
    pub source_family: Option<String>,
    pub namehash: Option<String>,
}

/// The current pointer of `resource_id` after the zero rejection.
pub async fn load_family_alias_source_pointer(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<FamilyAliasSourcePointer>> {
    let Some(pointer) = load_family_resource_pointer(pool, chain_id, resource_id).await? else {
        return Ok(None);
    };
    let (Some(resolver_address), Some(pointer_position)) =
        (pointer.resolver_address, pointer.pointer_position)
    else {
        return Ok(None);
    };
    if is_cleared(Some(&resolver_address)) {
        return Ok(None);
    }
    Ok(Some(FamilyAliasSourcePointer {
        resource_id,
        resolver_address,
        pointer_position,
        namespace: pointer.namespace,
        source_family: pointer.source_family,
        namehash: pointer.namehash,
    }))
}

/// What the wildcard read takes from a resource on its longest ancestor (name_topology.rs, the
/// wildcard lateral): the latest non-zero pointer, zero filtered before the latest is taken, and
/// the latest `RecordVersionChanged` or `ResolverChanged` with clears included as its boundary.
/// A non-zero pointer followed by a clear keeps the non-zero resolver with the clear as boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyWildcardSource {
    pub resource_id: Uuid,
    pub nonzero_resolver_address: Option<String>,
    pub nonzero_position: Option<FamilyPosition>,
    pub boundary_kind: Option<String>,
    pub boundary_position: Option<FamilyPosition>,
    pub boundary_block_timestamp: Option<OffsetDateTime>,
}

/// The wildcard source columns of `resource_id`.
pub async fn load_family_wildcard_source(
    pool: &PgPool,
    chain_id: &str,
    resource_id: Uuid,
) -> Result<Option<FamilyWildcardSource>> {
    Ok(load_family_resource_pointer(pool, chain_id, resource_id)
        .await?
        .map(|pointer| FamilyWildcardSource {
            resource_id,
            nonzero_resolver_address: pointer.nonzero_resolver_address,
            nonzero_position: pointer.nonzero_position,
            boundary_kind: pointer.boundary_kind,
            boundary_position: pointer.boundary_position,
            boundary_block_timestamp: pointer.boundary_block_timestamp,
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(node: &str, record_id: &str, event: i64) -> FamilyLink {
        FamilyLink {
            node: node.to_owned(),
            record_id: record_id.to_owned(),
            position: FamilyPosition {
                block_number: event,
                transaction_index: Some(0),
                log_index: Some(0),
                event_identity: format!("link:{event}"),
            },
            normalized_event_id: Some(event),
        }
    }

    #[test]
    fn an_exact_link_wins_and_hides_the_default_event() {
        let selection = select(
            Some(link("0x01", "7", 2)),
            Some(link(DEFAULT_RECORD_NODE, "9", 1)),
        )
        .expect("a selection");
        assert_eq!(selection.record_id.as_deref(), Some("7"));
        assert_eq!(selection.exact_link_event_id, Some(2));
        assert_eq!(selection.default_link_event_id, None);
        assert_eq!(selection.contributing_links().count(), 1);
    }

    #[test]
    fn an_exact_clear_falls_back_to_the_default_and_both_contribute() {
        let selection = select(
            Some(link("0x01", "0", 2)),
            Some(link(DEFAULT_RECORD_NODE, "9", 1)),
        )
        .expect("a selection");
        assert_eq!(selection.record_id.as_deref(), Some("9"));
        assert_eq!(selection.exact_link_event_id, Some(2));
        assert_eq!(selection.default_link_event_id, Some(1));
        assert_eq!(selection.contributing_links().count(), 2);
    }

    #[test]
    fn a_cleared_default_keeps_its_raw_selection_and_boundary_but_selects_no_record() {
        let selection = select(None, Some(link(DEFAULT_RECORD_NODE, "0", 3))).expect("a selection");
        assert_eq!(selection.record_id.as_deref(), Some("0"));
        assert_eq!(selection.active_record_id(), None);
        assert_eq!(selection.exact_link_event_id, None);
        assert_eq!(selection.default_link_event_id, Some(3));
        assert_eq!(
            selection
                .contributing_links()
                .map(|link| link.normalized_event_id)
                .collect::<Vec<_>>(),
            [Some(3)]
        );
        let exact_clear = select(
            Some(link("0x01", "0", 4)),
            Some(link(DEFAULT_RECORD_NODE, "0", 3)),
        )
        .expect("a selection");
        assert_eq!(exact_clear.active_record_id(), None);
        assert_eq!(exact_clear.contributing_links().count(), 2);
        let active = select(Some(link("0x01", "7", 4)), None).expect("a selection");
        assert_eq!(active.active_record_id(), Some("7"));
    }

    #[test]
    fn an_exact_clear_without_a_default_selects_no_record() {
        let selection = select(Some(link("0x01", "0", 2)), None).expect("a selection");
        assert_eq!(selection.record_id, None);
        assert_eq!(selection.exact_link_event_id, Some(2));
        assert!(select(None, None).is_none());
    }
}
