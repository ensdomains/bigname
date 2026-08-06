use crate::{
    Result,
    engine::{BatchRequest, Engine, SourceDescriptor},
    plan::enforce_source_floor,
    provider::{ProviderKind, normalized_kind, provider_error},
};

impl Engine {
    /// Refuses, before any ingest work, a plan whose range starts below what a source holds.
    ///
    /// The refusal is judged on the source's declared start block rather than on cursor
    /// progress: a cursor standing above the floor recorded coverage the node never had.
    /// Only a source reading a node's database directly reports a floor at all.
    pub(super) async fn enforce_source_floors(&self, request: &BatchRequest) -> Result<()> {
        for source in &request.sources {
            if normalized_kind(&source.kind) == ProviderKind::Coinbase {
                continue;
            }
            let Some((from, to)) = planned_range(source, request.redo_range) else {
                continue;
            };
            let Some(floor) = self.source_floor(&request.chain_id, source).await? else {
                continue;
            };
            enforce_source_floor(&source.key, from, to, floor)?;
        }
        Ok(())
    }

    async fn source_floor(&self, chain_id: &str, source: &SourceDescriptor) -> Result<Option<i64>> {
        if let Some(floor) = injected_floor(&source.endpoint) {
            return Ok(Some(floor));
        }
        let provider = self.provider(chain_id, source).await?;
        provider.earliest_available_block().await.map_err(|error| {
            provider_error(
                &format!(
                    "failed to read the earliest available block for source {}",
                    source.key
                ),
                error,
            )
        })
    }
}

/// The block range a batch plans for one source, or `None` when it plans nothing.
///
/// A normal batch plans the source's whole declared window up to the chain head, cursor
/// progress included: a cursor that already advanced past a pruned window recorded coverage
/// the node never had, so the plan stays refusable until the node or the window changes.
fn planned_range(
    source: &SourceDescriptor,
    redo_range: Option<(i64, i64)>,
) -> Option<(i64, Option<i64>)> {
    let Some((from, to)) = redo_range else {
        return Some((source.start_block, None));
    };
    let from = from.max(source.start_block);
    (from <= to).then_some((from, Some(to)))
}

#[cfg(test)]
fn injected_floor(endpoint: &str) -> Option<i64> {
    test_floors::floor(endpoint)
}

#[cfg(not(test))]
const fn injected_floor(_endpoint: &str) -> Option<i64> {
    None
}

/// Stands in for a pruned datadir, which no unit test can build.
#[cfg(test)]
mod test_floors {
    use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry};

    static FLOORS: ScopedTestHookRegistry<String, i64> = ScopedTestHookRegistry::new();

    pub(super) fn install(endpoint: &str, floor: i64) -> ScopedTestHookGuard<String, i64> {
        FLOORS.install(endpoint.to_owned(), floor)
    }

    pub(super) fn floor(endpoint: &str) -> Option<i64> {
        FLOORS.get_cloned(&endpoint.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result as AnyResult;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;
    use crate::{ErrorKind, provider::ChainProvider};

    const DATADIR: &str = "/var/lib/reth/pruned-datadir-fixture";
    const V1_REGISTRY_START: i64 = 3_327_417;
    const MERGE_RECEIPT_SEGMENT_START: i64 = 15_500_000;

    #[tokio::test]
    async fn a_range_below_the_node_floor_fails_the_phase_instead_of_completing() -> AnyResult<()> {
        let database = TestDatabase::create(TestDatabaseConfig::new("ingest_source_floor")).await?;
        let _floor = test_floors::install(DATADIR, MERGE_RECEIPT_SEGMENT_START);
        let engine = Engine::new(database.pool().clone());

        let historical = engine
            .run_batch(request(None))
            .await
            .expect_err("planning a pruned range must fail rather than complete");
        let redo = engine
            .run_batch(request(Some((3_000_000, 4_000_000))))
            .await
            .expect_err("redoing a pruned range must fail rather than complete");

        // Only ErrorKind::Transient is retried; a floor refusal has to stop the phase.
        assert_eq!(historical.kind(), ErrorKind::Configuration);
        assert_eq!(redo.kind(), ErrorKind::Configuration);
        assert!(
            historical.to_string().contains("15500000")
                && historical.to_string().contains("3327417..=head"),
            "{historical}"
        );
        assert!(
            redo.to_string().contains("15500000") && redo.to_string().contains("3327417..=4000000"),
            "{redo}"
        );
        database.cleanup().await
    }

    #[tokio::test]
    async fn an_rpc_source_reports_no_floor() -> AnyResult<()> {
        let provider = ChainProvider::new("base-mainnet", "rpc", "https://rpc.example.com/")?;

        assert_eq!(provider.earliest_available_block().await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_warehouse_source_is_planned_without_asking_it_for_a_floor() -> AnyResult<()> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("ingest_source_floor_base")).await?;
        let engine = Engine::new(database.pool().clone());

        // Coinbase SQL is not a block provider at all, so asking it for a floor would
        // fail every base-mainnet batch.
        engine
            .enforce_source_floors(&BatchRequest {
                chain_id: "base-mainnet".to_owned(),
                sources: vec![
                    SourceDescriptor {
                        key: "base-coinbase".to_owned(),
                        kind: "coinbase-sql".to_owned(),
                        start_block: 0,
                        endpoint: "coinbase-sql://warehouse".to_owned(),
                    },
                    SourceDescriptor {
                        key: "base-rpc".to_owned(),
                        kind: "rpc".to_owned(),
                        start_block: crate::BASE_COINBASE_SEAM_BLOCK,
                        endpoint: "https://rpc.example.com/".to_owned(),
                    },
                ],
                cursors: Vec::new(),
                redo_range: None,
                resume_current: None,
            })
            .await?;
        database.cleanup().await
    }

    #[cfg(feature = "reth-db")]
    #[tokio::test]
    async fn planning_reads_the_floor_from_the_configured_datadir() -> AnyResult<()> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("ingest_source_floor_datadir")).await?;
        let engine = Engine::new(database.pool().clone());

        let error = engine
            .enforce_source_floors(&request(None))
            .await
            .expect_err("an unreadable datadir must fail the floor read");

        assert!(
            error
                .to_string()
                .contains("failed to read the earliest available block for source ethereum-reth"),
            "{error}"
        );
        database.cleanup().await
    }

    #[test]
    fn a_redo_range_that_ends_below_the_source_window_plans_nothing() {
        let source = source();

        assert_eq!(
            planned_range(&source, Some((0, V1_REGISTRY_START - 1))),
            None
        );
        assert_eq!(
            planned_range(&source, Some((0, V1_REGISTRY_START))),
            Some((V1_REGISTRY_START, Some(V1_REGISTRY_START)))
        );
        assert_eq!(
            planned_range(&source, None),
            Some((V1_REGISTRY_START, None))
        );
    }

    fn request(redo_range: Option<(i64, i64)>) -> BatchRequest {
        BatchRequest {
            chain_id: "ethereum-mainnet".to_owned(),
            sources: vec![source()],
            cursors: Vec::new(),
            redo_range,
            resume_current: None,
        }
    }

    fn source() -> SourceDescriptor {
        SourceDescriptor {
            key: "ethereum-reth".to_owned(),
            kind: "reth-db".to_owned(),
            start_block: V1_REGISTRY_START,
            endpoint: DATADIR.to_owned(),
        }
    }
}
