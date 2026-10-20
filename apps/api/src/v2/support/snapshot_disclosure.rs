use super::*;

#[cfg(test)]
use crate::v2::search_public_namespace_read_test_hooks::{self as read_hooks, ReadHookPoint};
use crate::v2::{AsOfCompleteness, Completeness, Meta, V2Result, api_error_to_v2, as_of_meta};

const TEMPORARILY_UNAVAILABLE: &str = "temporarily_unavailable";

#[derive(Debug)]
pub(crate) struct ExplicitNamespaceRequestScope {
    meta: Meta,
    selected: Option<SelectedSnapshot>,
    project_generations: Option<BTreeMap<String, String>>,
    interpret_redo_state: InterpretRedoCapture,
}

/// The Interpret redo state of the chains a request reads, captured before its read.
#[derive(Clone, Debug)]
pub(crate) struct InterpretRedoCapture {
    chain_ids: Vec<String>,
    state: bigname_storage::SelectedInterpretRedoState,
}

impl InterpretRedoCapture {
    pub(crate) async fn load(pool: &PgPool, chain_ids: Vec<String>) -> V2Result<Self> {
        let state = bigname_storage::load_selected_interpret_redo_state(pool, &chain_ids)
            .await
            .map_err(|_| {
                crate::v2::V2Error::internal_error("failed to validate request-scope metadata")
            })?;
        Ok(Self { chain_ids, state })
    }

    fn in_progress(&self) -> bool {
        self.state.changed_or_active(&self.state)
    }

    /// `409 stale` when a captured chain began, ran or finished an Interpret redo since the
    /// capture.
    pub(crate) async fn revalidate(&self, pool: &PgPool) -> V2Result<()> {
        let current = Self::load(pool, self.chain_ids.clone()).await?;
        if self.state.changed_or_active(&current.state) {
            return Err(interpret_redo_in_progress());
        }
        Ok(())
    }
}

fn interpret_redo_in_progress() -> crate::v2::V2Error {
    crate::v2::V2Error::stale(
        "served data is temporarily unavailable while Interpret redo is in progress",
    )
}

impl ExplicitNamespaceRequestScope {
    pub(crate) fn has_publication(&self) -> bool {
        self.selected.is_some()
    }

    /// Preserve unavailable-scope disclosure. A ready capture must still be the same
    /// publication on the request's snapshot, including an empty search page.
    pub(crate) async fn served_on(&self, conn: &mut sqlx::PgConnection) -> V2Result<bool> {
        let Some(selected) = self.selected.as_ref() else {
            return Ok(true);
        };
        let current = load_selected_project_generations_on(conn, selected, true)
            .await
            .map_err(|_| {
                crate::v2::V2Error::internal_error("failed to validate request-scope metadata")
            })?;
        Ok(current == self.project_generations)
    }
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

pub(crate) fn apply_suppressed_request_scope_completeness(
    meta: &mut Meta,
    scope: &SnapshotSelectionScope,
) -> V2Result<()> {
    apply_request_scope_completeness(
        meta,
        &[RequestScopeSnapshot {
            scope: scope.clone(),
            selected: None,
        }],
    )
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
    let input = crate::v2::snapshots::selector_input(None, None, SnapshotConsistency::Head)
        .map_err(snapshot_selection_api_error)
        .map_err(api_error_to_v2)?;
    let (selected, project_generations, interpret_redo_state) =
        match resolve_exact_name_snapshot_selection(&state.pool, &scope, &input).await {
            Ok(selected) => {
                #[cfg(test)]
                read_hooks::run_at(&state.pool, ReadHookPoint::AfterExplicitSelection).await?;
                let chain_ids = selected
                    .chain_positions
                    .as_map()
                    .values()
                    .map(|position| position.chain_id.clone())
                    .collect::<Vec<_>>();
                let redo_state = InterpretRedoCapture::load(&state.pool, chain_ids).await?;
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
                        load_unfenced_explicit_namespace_generations(
                            &state.pool,
                            &selected,
                            &redo_state,
                        )
                        .await?
                    }
                };
                (
                    readable_generations.map(|_| selected),
                    project_generations,
                    redo_state,
                )
            }
            Err(error)
                if matches!(
                    error.kind(),
                    SnapshotSelectionErrorKind::Conflict | SnapshotSelectionErrorKind::Stale
                ) =>
            {
                let redo_state =
                    load_explicit_namespace_interpret_redo_state(&state.pool, &scope).await?;
                (None, None, redo_state)
            }
            Err(error) => return Err(api_error_to_v2(snapshot_selection_api_error(error))),
        };
    Ok(ExplicitNamespaceRequestScope {
        meta: request_scope_meta(&[RequestScopeSnapshot {
            scope,
            selected: selected.clone(),
        }])?,
        selected,
        project_generations,
        interpret_redo_state,
    })
}

async fn load_explicit_namespace_interpret_redo_state(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
) -> V2Result<InterpretRedoCapture> {
    let chain_ids = scope
        .required_positions()
        .iter()
        .map(|position| position.chain_id.clone())
        .collect::<Vec<_>>();
    let redo_state = InterpretRedoCapture::load(pool, chain_ids).await?;
    match redo_state.in_progress() {
        true => Err(interpret_redo_in_progress()),
        false => Ok(redo_state),
    }
}

async fn load_unfenced_explicit_namespace_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
    expected_redo_state: &InterpretRedoCapture,
) -> V2Result<Option<BTreeMap<String, String>>> {
    #[cfg(test)]
    read_hooks::run_at(pool, ReadHookPoint::BeforeUnfencedGenerations).await?;
    let generations = load_selected_project_generations_for_read(pool, selected, false)
        .await
        .map_err(|_| {
            crate::v2::V2Error::internal_error("failed to validate request-scope metadata")
        })?;
    expected_redo_state.revalidate(pool).await?;
    if generations.is_some() {
        return Err(crate::v2::V2Error::conflict(
            "search namespace readiness changed while the request was being read",
        ));
    }
    Ok(None)
}

/// The page was read on a snapshot that still served the captured publication (`served_on`),
/// so a head or publication that moved after it is not a refusal. A redo during the read is.
pub(crate) async fn revalidate_explicit_namespace_request_scope(
    state: &AppState,
    expected: ExplicitNamespaceRequestScope,
) -> V2Result<Meta> {
    expected
        .interpret_redo_state
        .revalidate(&state.pool)
        .await?;
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
