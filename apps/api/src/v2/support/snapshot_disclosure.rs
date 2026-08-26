use super::*;

use crate::v2::{AsOfCompleteness, Completeness, Meta, V2Result, api_error_to_v2, as_of_meta};

const TEMPORARILY_UNAVAILABLE: &str = "temporarily_unavailable";

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ExplicitNamespaceRequestScope {
    meta: Meta,
    project_generations: Option<BTreeMap<String, String>>,
}

pub(crate) fn request_scope_meta(scopes: &[RequestScopeSnapshot]) -> V2Result<Meta> {
    let mut meta = Meta::default();
    let as_of = scopes
        .iter()
        .filter_map(RequestScopeSnapshot::selected)
        .map(as_of_meta)
        .collect::<V2Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeMap<_, _>>();
    if !as_of.is_empty() {
        meta.as_of = Some(as_of);
    }
    apply_request_scope_completeness(&mut meta, scopes)?;
    Ok(meta)
}

pub(crate) fn apply_request_scope_completeness(
    meta: &mut Meta,
    scopes: &[RequestScopeSnapshot],
) -> V2Result<()> {
    let mut suppressed = meta.as_of_completeness.take().unwrap_or_default();
    for scope in scopes.iter().filter(|scope| scope.selected().is_none()) {
        for position in scope.scope().required_positions() {
            let chain_id = crate::v2::slug_to_numeric(&position.chain_id).ok_or_else(|| {
                crate::v2::V2Error::internal_error(format!(
                    "request scope uses unmapped chain_id {}",
                    position.chain_id
                ))
            })?;
            suppressed.insert(
                chain_id.to_string(),
                AsOfCompleteness {
                    completeness: Completeness::Unsupported,
                    unsupported_reason: TEMPORARILY_UNAVAILABLE.to_owned(),
                },
            );
        }
    }
    if !suppressed.is_empty() {
        if let Some(as_of) = meta.as_of.as_mut() {
            as_of.retain(|chain_id, _| !suppressed.contains_key(chain_id));
            if as_of.is_empty() {
                meta.as_of = None;
            }
        }
        meta.as_of_completeness = Some(suppressed);
    }
    Ok(())
}

pub(crate) async fn explicit_namespace_request_scope(
    state: &AppState,
    namespace: &str,
) -> V2Result<ExplicitNamespaceRequestScope> {
    let scope = exact_name_snapshot_scope(
        &state.pool,
        namespace,
        ExactNameSnapshotSelector::default(),
        false,
    )
    .await
    .map_err(api_error_to_v2)?;
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(snapshot_selection_api_error)
        .map_err(api_error_to_v2)?;
    let (selected, project_generations) =
        match resolve_exact_name_snapshot_selection(&state.pool, &scope, &input).await {
            Ok(selected) => {
                let readable_generations =
                    load_selected_project_generations_for_read(&state.pool, &selected, true)
                        .await
                        .map_err(|_| {
                            crate::v2::V2Error::internal_error(
                                "failed to validate request-scope metadata",
                            )
                        })?;
                let project_generations = match readable_generations.as_ref() {
                    Some(_) => readable_generations.clone(),
                    None => {
                        load_selected_project_generations_for_read(&state.pool, &selected, false)
                            .await
                            .map_err(|_| {
                                crate::v2::V2Error::internal_error(
                                    "failed to validate request-scope metadata",
                                )
                            })?
                    }
                };
                (readable_generations.map(|_| selected), project_generations)
            }
            Err(error)
                if matches!(
                    error.kind(),
                    SnapshotSelectionErrorKind::Conflict | SnapshotSelectionErrorKind::Stale
                ) =>
            {
                (None, None)
            }
            Err(error) => return Err(api_error_to_v2(snapshot_selection_api_error(error))),
        };
    Ok(ExplicitNamespaceRequestScope {
        meta: request_scope_meta(&[RequestScopeSnapshot { scope, selected }])?,
        project_generations,
    })
}

pub(crate) async fn revalidate_explicit_namespace_request_scope(
    state: &AppState,
    namespace: &str,
    expected: ExplicitNamespaceRequestScope,
) -> V2Result<Meta> {
    let current = explicit_namespace_request_scope(state, namespace).await?;
    if current != expected {
        return Err(crate::v2::V2Error::conflict(
            "search namespace position changed while the request was being read",
        ));
    }
    Ok(expected.meta)
}

#[cfg(test)]
mod tests {
    use bigname_storage::{
        ChainPosition, ChainPositions, SelectedSnapshot, SnapshotConsistency,
        SnapshotPositionRequirement, SnapshotSelectionScope,
    };

    use super::*;

    fn ethereum_scope() -> SnapshotSelectionScope {
        SnapshotSelectionScope::new(
            vec![SnapshotPositionRequirement::new(
                "ethereum",
                "ethereum-mainnet",
            )],
            Some("ethereum".to_owned()),
        )
        .expect("test scope must be valid")
    }

    fn ethereum_snapshot() -> SelectedSnapshot {
        SelectedSnapshot {
            chain_positions: ChainPositions::new(BTreeMap::from([(
                "ethereum".to_owned(),
                ChainPosition {
                    slot: "ethereum".to_owned(),
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_number: 100,
                    block_hash: "0xoverlap".to_owned(),
                    timestamp: parse_rfc3339_utc_timestamp("2026-08-26T00:00:00Z")
                        .expect("test timestamp must parse"),
                },
            )])),
            consistency: SnapshotConsistency::Head,
        }
    }

    #[test]
    fn request_scope_meta_gives_suppression_precedence_for_overlapping_chain_scopes() {
        let meta = request_scope_meta(&[
            RequestScopeSnapshot {
                scope: ethereum_scope(),
                selected: Some(ethereum_snapshot()),
            },
            RequestScopeSnapshot {
                scope: ethereum_scope(),
                selected: None,
            },
        ])
        .expect("overlapping request scopes must produce metadata");

        assert!(meta.as_of.is_none());
        assert_eq!(
            meta.as_of_completeness
                .expect("suppressed chain must be disclosed")["1"],
            AsOfCompleteness {
                completeness: Completeness::Unsupported,
                unsupported_reason: TEMPORARILY_UNAVAILABLE.to_owned(),
            }
        );
    }
}
