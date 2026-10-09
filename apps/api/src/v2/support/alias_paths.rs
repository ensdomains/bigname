//! The name the direct read and the records route serve: the requested name's own row, or, on
//! an ENSv2 alias path (docs/glossary.md#alias-path), the canonical name's row under the
//! requested path (docs/api-v1.md, "ENSv2 name path").
use sqlx::PgConnection;

use super::*;
use crate::v2::name_record::row_has_current_registration;

#[cfg(test)]
tokio::task_local! {
    /// The statements the alias walks of one request ran, for the tests that bound its cost.
    pub(crate) static ALIAS_WALK_STATEMENTS: std::cell::Cell<usize>;
}

/// A requested path that is not the canonical name of the token it reaches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AliasPath {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namehash: String,
    pub(crate) canonical_name: String,
    lookup: Option<bigname_lookup::LookupPath>,
}

impl AliasPath {
    fn new(name: &str, canonical_name: String) -> Self {
        let labels: Vec<&str> = name.split('.').collect();
        let node = labels.iter().rev().fold([0_u8; 32], |node, label| {
            let labelhash = bigname_storage::rendered_name::label_hash(label);
            alloy_primitives::keccak256([node, labelhash].concat()).0
        });
        // A bracketed label has no bytes to send, so such a path has no verified read, as a
        // stored name without label bytes has none (`bigname_lookup` store, textless names).
        let normalized = bigname_domain::normalization::normalize_name(name).ok();
        Self {
            name: name.to_owned(),
            display_name: normalized.as_ref().map_or_else(
                || name.to_owned(),
                |normalized| normalized.canonical_display_name.clone(),
            ),
            namehash: format!("0x{}", alloy_primitives::hex::encode(node)),
            canonical_name,
            lookup: normalized.map(|normalized| bigname_lookup::LookupPath {
                name: name.to_owned(),
                dns_name: normalized.dns_encoded_name,
                node,
            }),
        }
    }

    /// The path a verified read executes: the chain resolves the requested path, whose
    /// resolver may key records by the requested node.
    pub(crate) fn lookup_path(&self) -> Option<&bigname_lookup::LookupPath> {
        self.lookup.as_ref()
    }
}

/// The row a route serves for `name`, and the alias path it is served under, if any.
pub(crate) struct ServedName {
    pub(crate) row: NameCurrentRow,
    pub(crate) alias: Option<AliasPath>,
}

/// The requested name's row when it holds a current registration. Otherwise the requested
/// path is walked: when it reaches a token whose canonical name is another, that name's row is
/// served under the requested path. A path that reaches nothing serves what it served before,
/// the row or `404 not_found`. Every read runs on `conn`'s one snapshot.
pub(crate) async fn load_served_name_for_selected_snapshot(
    conn: &mut PgConnection,
    namespace: &str,
    name: &str,
    selected_snapshot: &SelectedSnapshot,
) -> ApiResult<ServedName> {
    let stored =
        load_name_current_for_selected_snapshot(&mut *conn, namespace, name, selected_snapshot)
            .await;
    let walks = match &stored {
        Ok(row) => !row_has_current_registration(row),
        Err(error) => error.status == StatusCode::NOT_FOUND,
    };
    if !walks {
        return stored.map(|row| ServedName { row, alias: None });
    }
    let walk = bigname_storage::families::alias_path::resolve_alias_path(
        &mut *conn,
        namespace,
        name,
        &super::route_logical_name_id(namespace, name),
        &selected_snapshot.chain_positions,
    )
    .await
    .map_err(snapshot_selection_api_error)?;
    #[cfg(test)]
    let _ = ALIAS_WALK_STATEMENTS.try_with(|count| count.set(count.get() + walk.statements));
    let Some(target) = walk.target else {
        return stored.map(|row| ServedName { row, alias: None });
    };
    let row = match load_name_current_for_snapshot(
        &mut *conn,
        &target.canonical_logical_name_id,
        &selected_snapshot.chain_positions,
    )
    .await
    .map_err(snapshot_selection_api_error)?
    {
        SnapshotProjectionRead::Found(row) => row,
        // The canonical row is not composed yet: the alias waits for it.
        SnapshotProjectionRead::NotFound => {
            return Err(ApiError {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: format!("name {name} was not found in namespace {namespace}"),
            });
        }
    };
    let canonical_name = row.normalized_name.clone();
    Ok(ServedName {
        row,
        alias: Some(AliasPath::new(name, canonical_name)),
    })
}
