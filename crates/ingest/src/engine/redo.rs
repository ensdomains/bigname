use std::{future::Future, pin::Pin};

use sqlx::PgPool;

use crate::{
    BatchRequest, IngestError, Marker, REDO_BOUNDARY_DIVERGENCE_PREFIX, Result, SourceProgress,
    coinbase_sql::source_error,
    engine::{Engine, SourceDescriptor},
    plan::BASE_COINBASE_SEAM_BLOCK,
    provider::{ProviderKind, normalized_kind},
};

pub(super) struct LoadedWindow {
    pub(super) first: Marker,
    pub(super) marker: Marker,
    pub(super) first_parent_hash: Option<String>,
    pub(super) estimated_write_bytes: u64,
}

pub(super) type RedoLoadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<LoadedWindow>> + Send + 'a>>;

pub(super) trait RedoWindowLoader: Send + Sync {
    fn load<'a>(
        &'a self,
        engine: &'a Engine,
        chain_id: &'a str,
        source: &'a SourceDescriptor,
        all_sources: &'a [SourceDescriptor],
        from: i64,
        to: i64,
    ) -> RedoLoadFuture<'a>;
}

pub(super) struct ProductionRedoWindowLoader;

impl RedoWindowLoader for ProductionRedoWindowLoader {
    fn load<'a>(
        &'a self,
        engine: &'a Engine,
        chain_id: &'a str,
        source: &'a SourceDescriptor,
        all_sources: &'a [SourceDescriptor],
        from: i64,
        to: i64,
    ) -> RedoLoadFuture<'a> {
        Box::pin(engine.load_window(chain_id, source, all_sources, from, to))
    }
}

impl Engine {
    pub(super) async fn require_independent_base_source_seam(
        &self,
        request: &BatchRequest,
    ) -> Result<()> {
        let Some((range_from, range_to)) = request.redo_range else {
            return Ok(());
        };
        if request.chain_id != "base-mainnet"
            || !(range_from..=range_to).contains(&BASE_COINBASE_SEAM_BLOCK)
        {
            return Ok(());
        }
        let coinbase = request
            .sources
            .iter()
            .find(|source| normalized_kind(&source.kind) == ProviderKind::Coinbase)
            .expect("validated Base request has one Coinbase SQL source");
        let rpc = request
            .sources
            .iter()
            .find(|source| normalized_kind(&source.kind) == ProviderKind::Rpc)
            .expect("validated Base request has one RPC source");
        let coinbase_marker = self.coinbase_block_marker(coinbase).await?;
        let rpc_provider = self.provider(&request.chain_id, rpc).await?;
        let rpc_marker = super::resolve_marker(&rpc_provider, BASE_COINBASE_SEAM_BLOCK).await?;
        require_independent_source_seam(
            &request.chain_id,
            &coinbase.key,
            &coinbase_marker,
            &rpc.key,
            &rpc_marker,
        )
    }

    async fn coinbase_block_marker(&self, source: &SourceDescriptor) -> Result<Marker> {
        #[cfg(test)]
        if let Some(marker) = crate::coinbase_sql::test_seam_markers::marker(
            &source.endpoint,
            BASE_COINBASE_SEAM_BLOCK,
        ) {
            return Ok(marker);
        }
        let marker = self
            .coinbase_source("base-mainnet", source)
            .await?
            .block_marker(BASE_COINBASE_SEAM_BLOCK)
            .await
            .map_err(|error| {
                source_error("failed to fetch Coinbase SQL seam block identity", error)
            })?;
        Ok(Marker {
            number: marker.number,
            hash: marker.hash,
        })
    }
}

pub(super) fn require_resumed_window_parent(
    chain_id: &str,
    source_key: &str,
    resume: Option<&Marker>,
    window_from: i64,
    first_parent_hash: Option<&str>,
) -> Result<()> {
    let Some(resume) = resume.filter(|marker| marker.number.checked_add(1) == Some(window_from))
    else {
        return Ok(());
    };
    if first_parent_hash == Some(resume.hash.as_str()) {
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} source {source_key} at block \
         {window_from}: loaded window parent {}, durable prior-batch hash {}; rerun the Ingest \
         redo so it starts fresh and reloads the full range on one fork under the current watch plan",
        first_parent_hash.unwrap_or("missing"),
        resume.hash
    )))
}

pub(super) fn require_source_seam(
    request: &BatchRequest,
    source: &SourceDescriptor,
    prior_progress: &[SourceProgress],
    loaded_first: &Marker,
) -> Result<()> {
    for prior in prior_progress {
        let Some(prior_boundary) = prior
            .current
            .as_ref()
            .filter(|marker| marker.number == loaded_first.number)
        else {
            continue;
        };
        if prior_boundary.hash != loaded_first.hash {
            return Err(IngestError::data_integrity(format!(
                "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {} at cross-source block {}: source \
                 {} loaded hash {}, source {} loaded hash {}; rerun the Ingest redo so it starts \
                 fresh and reloads the full range on one fork under the current watch plan",
                request.chain_id,
                loaded_first.number,
                prior.key,
                prior_boundary.hash,
                source.key,
                loaded_first.hash
            )));
        }
    }
    Ok(())
}

pub(super) fn require_independent_source_seam(
    chain_id: &str,
    coinbase_key: &str,
    coinbase: &Marker,
    rpc_key: &str,
    rpc: &Marker,
) -> Result<()> {
    if coinbase.number == rpc.number && coinbase.hash == rpc.hash {
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at independently queried source \
         seam block {}: Coinbase SQL source {coinbase_key} returned hash {}, RPC source {rpc_key} \
         returned hash {}; rerun the Ingest redo after both sources expose the same canonical block",
        coinbase.number, coinbase.hash, rpc.hash
    )))
}

pub(super) fn running_summary_from_loaded_source(
    progress: &[SourceProgress],
    primary_key: &str,
    block_number: i64,
) -> Option<(Marker, Marker)> {
    let primary = progress.iter().find(|source| source.key == primary_key)?;
    let loaded = primary
        .current
        .as_ref()
        .filter(|marker| marker.number == block_number)
        .or_else(|| {
            progress
                .iter()
                .filter_map(|source| source.current.as_ref())
                .find(|marker| marker.number == block_number)
        })?;
    Some((loaded.clone(), primary.target.clone()))
}

pub(super) fn must_reload_completed_source_boundary(
    completing: bool,
    range_from: i64,
    range_to: i64,
    resume_current: Option<&Marker>,
    source_target_number: i64,
) -> bool {
    completing
        && source_target_number >= range_from
        && source_target_number < range_to
        && resume_current.is_some_and(|resume| resume.number > source_target_number)
}

fn require_loaded_boundary(
    chain_id: &str,
    loaded: &Marker,
    pre_load_target: &Marker,
) -> Result<()> {
    if loaded == pre_load_target {
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at block {}: loaded boundary hash {}, pre-load target hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
        pre_load_target.number, loaded.hash, pre_load_target.hash
    )))
}

pub(super) fn adopt_loaded_boundary(
    chain_id: &str,
    loaded: Marker,
    pre_load_target: &Marker,
) -> Result<Marker> {
    require_loaded_boundary(chain_id, &loaded, pre_load_target)?;
    Ok(loaded)
}

pub(super) fn adopt_persisted_loaded_boundary(
    chain_id: &str,
    source_key: &str,
    loaded: Option<&Marker>,
    fresh_target: &Marker,
) -> Result<Marker> {
    let Some(loaded) = loaded else {
        return Err(IngestError::data_integrity(format!(
            "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} source {source_key} at block {}: no load-derived boundary marker was persisted; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
            fresh_target.number
        )));
    };
    if loaded.number != fresh_target.number || loaded.hash != fresh_target.hash {
        return Err(IngestError::data_integrity(format!(
            "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} source {source_key} at block {}: load-derived boundary hash {}, freshly observed hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
            fresh_target.number, loaded.hash, fresh_target.hash
        )));
    }
    Ok(loaded.clone())
}

pub(super) fn completing_summary_from_boundary(
    chain_id: &str,
    progress: &[SourceProgress],
    range_to: i64,
) -> Result<Option<(Marker, Marker)>> {
    let mut matching = progress
        .iter()
        .filter(|source| source.target.number == range_to);
    let Some(first) = matching.next() else {
        return Ok(None);
    };
    let first_marker = guarded_source_boundary(chain_id, first)?;
    for source in matching {
        let marker = guarded_source_boundary(chain_id, source)?;
        if marker != first_marker {
            return Err(IngestError::data_integrity(format!(
                "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at block {range_to}: completing source {} boundary hash {}, completing source {} boundary hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
                first.key, first_marker.hash, source.key, marker.hash
            )));
        }
    }
    Ok(Some((first_marker.clone(), first_marker.clone())))
}

fn guarded_source_boundary<'a>(chain_id: &str, source: &'a SourceProgress) -> Result<&'a Marker> {
    let current = source.current.as_ref().ok_or_else(|| {
        IngestError::data_integrity(format!(
            "completed redo for chain {chain_id} produced no current boundary for source {}",
            source.key
        ))
    })?;
    require_loaded_boundary(chain_id, current, &source.target)?;
    Ok(current)
}

pub(super) async fn reject_lineage_backed_boundary_change(
    pool: &PgPool,
    chain_id: &str,
    resume_current: Option<&Marker>,
    fresh_target: &Marker,
) -> Result<()> {
    let Some(durable) = resume_current else {
        return Ok(());
    };
    if durable.number != fresh_target.number || durable.hash == fresh_target.hash {
        return Ok(());
    }
    let has_lineage: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM chain_lineage
             WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
         )",
    )
    .bind(chain_id)
    .bind(fresh_target.number)
    .bind(&fresh_target.hash)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!(
                "failed to check boundary lineage while resuming Ingest redo for chain {chain_id}"
            ),
            error,
        )
    })?;
    if !has_lineage {
        // Cursor reconciliation independently refuses hashes absent from retained lineage.
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at block {}: durable redo hash {}, freshly observed hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
        durable.number, durable.hash, fresh_target.hash
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(number: i64) -> Marker {
        Marker {
            number,
            hash: format!("hash-{number}"),
        }
    }

    #[test]
    fn running_summary_uses_a_source_that_loaded_the_height_before_primary_start() {
        let progress = [
            SourceProgress {
                key: "bulk".to_owned(),
                current: Some(marker(100)),
                target: marker(200),
                loaded_boundary: None,
            },
            SourceProgress {
                key: "rpc".to_owned(),
                current: None,
                target: marker(300),
                loaded_boundary: None,
            },
        ];
        assert_eq!(
            running_summary_from_loaded_source(&progress, "rpc", 100),
            Some((marker(100), marker(300)))
        );
    }

    #[test]
    fn final_batch_reloads_an_in_range_source_boundary_covered_by_an_earlier_batch() {
        let resume = marker(300);

        assert!(must_reload_completed_source_boundary(
            true,
            0,
            400,
            Some(&resume),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            false,
            0,
            400,
            Some(&resume),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            true,
            200,
            400,
            Some(&resume),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            true,
            0,
            200,
            Some(&marker(100)),
            100
        ));
        assert!(must_reload_completed_source_boundary(
            true,
            100,
            200,
            Some(&marker(101)),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            true,
            0,
            100,
            Some(&marker(100)),
            100
        ));
    }

    #[test]
    fn a_reloaded_boundary_is_adopted_only_after_matching_the_pre_load_target() {
        let loaded = marker(100);
        let adopted = adopt_loaded_boundary("test-chain", loaded.clone(), &loaded)
            .expect("an equal loaded marker must be adopted");
        assert_eq!(adopted, loaded, "the load-returned marker must survive");

        let error = adopt_loaded_boundary("test-chain", marker(101), &loaded)
            .expect_err("a divergent loaded marker must fail closed");
        assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
        assert!(
            error.to_string().contains(REDO_BOUNDARY_DIVERGENCE_PREFIX),
            "{error}"
        );
    }

    #[test]
    fn an_equal_height_resume_requires_its_persisted_loaded_boundary() {
        let loaded = marker(100);
        assert_eq!(
            adopt_persisted_loaded_boundary("test-chain", "bulk", Some(&loaded), &loaded)
                .expect("matching load-derived evidence must be adopted"),
            loaded
        );

        let divergent = marker(101);
        for evidence in [None, Some(&divergent)] {
            let error =
                adopt_persisted_loaded_boundary("test-chain", "bulk", evidence, &marker(100))
                    .expect_err("missing or divergent load-derived evidence must fail closed");
            assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
            assert!(
                error.to_string().contains(REDO_BOUNDARY_DIVERGENCE_PREFIX),
                "{error}"
            );
        }
    }

    #[test]
    fn completing_summary_uses_the_source_that_owns_the_range_end() {
        let range_end = marker(200);
        let later_source_start = marker(300);
        let summary = completing_summary_from_boundary(
            "base-mainnet",
            &[
                SourceProgress {
                    key: "base-coinbase".to_owned(),
                    current: Some(range_end.clone()),
                    target: range_end.clone(),
                    loaded_boundary: Some(range_end.clone()),
                },
                SourceProgress {
                    key: "base-rpc".to_owned(),
                    current: Some(later_source_start.clone()),
                    target: later_source_start,
                    loaded_boundary: None,
                },
            ],
            range_end.number,
        )
        .expect("a guarded boundary source must produce the phase summary");

        assert_eq!(summary, Some((range_end.clone(), range_end)));
    }

    #[test]
    fn completing_summary_requires_all_range_end_sources_to_agree() {
        let boundary = marker(200);
        let matching = SourceProgress {
            key: "base-coinbase".to_owned(),
            current: Some(boundary.clone()),
            target: boundary.clone(),
            loaded_boundary: Some(boundary.clone()),
        };
        let summary = completing_summary_from_boundary(
            "base-mainnet",
            &[
                matching.clone(),
                SourceProgress {
                    key: "base-rpc".to_owned(),
                    current: Some(boundary.clone()),
                    target: boundary.clone(),
                    loaded_boundary: Some(boundary.clone()),
                },
            ],
            boundary.number,
        )
        .expect("matching seam sources must produce one boundary");
        assert_eq!(summary, Some((boundary.clone(), boundary.clone())));

        let divergent = Marker {
            number: boundary.number,
            hash: "other-hash".to_owned(),
        };
        let error = completing_summary_from_boundary(
            "base-mainnet",
            &[
                matching,
                SourceProgress {
                    key: "base-rpc".to_owned(),
                    current: Some(divergent.clone()),
                    target: divergent,
                    loaded_boundary: None,
                },
            ],
            boundary.number,
        )
        .expect_err("different range-end source markers must fail closed");
        assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
        assert!(
            error.to_string().contains(REDO_BOUNDARY_DIVERGENCE_PREFIX),
            "{error}"
        );
    }

    #[test]
    fn completing_summary_has_no_boundary_owner_below_every_source_start() {
        assert_eq!(
            completing_summary_from_boundary(
                "test-chain",
                &[SourceProgress {
                    key: "future-rpc".to_owned(),
                    current: None,
                    target: marker(300),
                    loaded_boundary: None,
                }],
                200,
            )
            .expect("the caller may use the no-owner fallback"),
            None
        );
    }
}
