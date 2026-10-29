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
    /// What the row serves through its ENSv1 registry pointer, decided on the same snapshot.
    pub(crate) registry: RegistryRead,
    /// Whether a verified read runs Universal Resolver discovery
    /// (`name_records::verified_discovery`). The lookup engine reloads the composed row, not
    /// `row` as the registry read left it, so this is decided before the registry read. It is
    /// decided on the registration `serve_legacy_lease` serves, which changes it only for a
    /// released lease, whose verified read does not execute.
    pub(crate) lookup_discovery: bool,
}

/// The requested name's row when it holds a current registration. Otherwise the requested
/// path is walked: when it reaches a token whose canonical name is another, that name's row is
/// served under the requested path, if it is still the token's own row. A path that reaches
/// nothing serves what it served before, the row or `404 not_found`. Every read runs on
/// `conn`'s one snapshot. A requested name's own row serves the ENSv1 lease its registrar events
/// prove when its registration does not carry one (`serve_legacy_lease`). An ENSv1 name serves
/// the resolver its registry record holds (`serve_ens_v1_registry_resolver`).
pub(crate) async fn load_served_name_for_selected_snapshot(
    conn: &mut PgConnection,
    namespace: &str,
    name: &str,
    selected_snapshot: &SelectedSnapshot,
) -> ApiResult<ServedName> {
    let (mut row, alias) =
        load_composed_served_name(&mut *conn, namespace, name, selected_snapshot).await?;
    // A registration that does not carry its ENSv1 lease serves the lease its registrar events
    // prove, and discovery and the registry read are decided on that registration. A failed
    // read rolls back to its savepoint and serves the composed registration.
    if alias.is_none()
        && let Err(error) =
            serve_legacy_lease_in_savepoint(&mut *conn, &mut row, selected_snapshot).await
    {
        error!(
            service = "api",
            error = %error,
            "legacy lease read failed; serving the composed registration of {namespace}/{name}"
        );
    }
    let lookup_discovery = crate::v2::name_records::verified_discovery(&row, alias.as_ref());
    let registry = serve_ens_v1_registry_resolver(&mut *conn, &mut row)
        .await
        .map_err(|error| {
            error!(service = "api", error = %error, "failed to read the ENSv1 registry resolver");
            ApiError::internal_error(format!(
                "failed to read the ENSv1 registry resolver of {namespace}/{name}"
            ))
        })?;
    Ok(ServedName {
        row,
        alias,
        registry,
        lookup_discovery,
    })
}

async fn load_composed_served_name(
    conn: &mut PgConnection,
    namespace: &str,
    name: &str,
    selected_snapshot: &SelectedSnapshot,
) -> ApiResult<(NameCurrentRow, Option<AliasPath>)> {
    let stored =
        load_name_current_for_selected_snapshot(&mut *conn, namespace, name, selected_snapshot)
            .await;
    let walks = match &stored {
        Ok(row) => !row_has_current_registration(row),
        Err(error) => error.status == StatusCode::NOT_FOUND,
    };
    if !walks {
        return stored.map(|row| (row, None));
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
        return stored.map(|row| (row, None));
    };
    let row = match load_name_current_for_snapshot(
        &mut *conn,
        &target.canonical_logical_name_id,
        &selected_snapshot.chain_positions,
    )
    .await
    .map_err(snapshot_selection_api_error)?
    {
        SnapshotProjectionRead::Found(row) if is_the_targets_row(&row, &target) => row,
        // The canonical row is not composed yet, so the alias waits for it, or the canonical
        // path now holds another token, whose row is not this path's.
        SnapshotProjectionRead::Found(_) | SnapshotProjectionRead::NotFound => {
            return Err(ApiError {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: format!("name {name} was not found in namespace {namespace}"),
            });
        }
    };
    let canonical_name = row.normalized_name.clone();
    Ok((row, Some(AliasPath::new(name, canonical_name))))
}

/// Whether `row` is the reached token's own row. A token's row is bound to its resource. A
/// reservation's row is bound to none, so it must be a reservation with no registrant, as a
/// reserved entry has no owner, and with the reached reservation's expiry. When the row's
/// selected authority arm is ENSv1, the row is bound to the ENSv1 registration, not to an
/// ENSv2 one, so the reservation's association alone names it.
fn is_the_targets_row(
    row: &NameCurrentRow,
    target: &bigname_storage::families::alias_path::AliasTarget,
) -> bool {
    let decided_by_ens_v1 = row
        .provenance
        .pointer("/authority_selection/authority_arm")
        .and_then(serde_json::Value::as_str)
        == Some("ens_v1");
    match (row.resource_id, &target.reservation_expiry) {
        (_, Some(_)) if decided_by_ens_v1 => true,
        (Some(resource), _) => resource == target.resource_id,
        (None, Some(expiry)) => {
            let registration = &row.declared_summary["registration"];
            let number = |value: &str| value.parse::<u128>().ok();
            registration["status"] == "reserved"
                && registration["registrant"].is_null()
                && registration["expiry"]
                    .as_str()
                    .and_then(number)
                    .is_some_and(|row_expiry| number(expiry) == Some(row_expiry))
        }
        (None, None) => false,
    }
}
