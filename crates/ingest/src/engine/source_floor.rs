use crate::{
    Result,
    engine::{BatchRequest, Engine, SourceDescriptor},
    plan::enforce_source_floor,
    provider::{ProviderKind, normalized_kind, provider_error},
};

impl Engine {
    /// Refuses, before any ingest work, a plan whose range starts below what a source holds.
    ///
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

    /// Re-reads the floor for a window that has been fetched but not yet stored.
    ///
    /// Planning refuses ranges the node cannot serve, but a node can prune while a window is
    /// in flight. Reading the floor again — through the same call that makes a read-only
    /// provider catch up with the node — keeps that window from becoming recorded coverage.
    pub(super) async fn enforce_fetched_window_floor(
        &self,
        chain_id: &str,
        source: &SourceDescriptor,
        from: i64,
        to: i64,
    ) -> Result<()> {
        if normalized_kind(&source.kind) == ProviderKind::Coinbase {
            return Ok(());
        }
        let Some(floor) = self.source_floor(chain_id, source).await? else {
            return Ok(());
        };
        enforce_source_floor(&source.key, from, Some(to), floor)
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
/// A normal batch is judged against the source's whole declared window rather than the
/// one window it fetches: planning cannot tell coverage recorded before the node pruned
/// from coverage recorded through a pruned window, so it refuses both until the node holds
/// the declared range again or the declared start block moves. A redo carries its own
/// range, so a redo above the floor stays available on a pruned node.
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
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry};

    type Floors = Arc<Mutex<VecDeque<i64>>>;

    static FLOORS: ScopedTestHookRegistry<String, Floors> = ScopedTestHookRegistry::new();

    /// Each read takes the next floor and the last one repeats, so a node that prunes
    /// mid-batch is expressed as `[low, raised]`.
    pub(super) fn install(
        endpoint: &str,
        floors: impl IntoIterator<Item = i64>,
    ) -> ScopedTestHookGuard<String, Floors> {
        FLOORS.install(
            endpoint.to_owned(),
            Arc::new(Mutex::new(floors.into_iter().collect())),
        )
    }

    pub(super) fn floor(endpoint: &str) -> Option<i64> {
        let floors = FLOORS.get_cloned(&endpoint.to_owned())?;
        let mut floors = floors.lock().expect("floor queue mutex poisoned");
        if floors.len() > 1 {
            floors.pop_front()
        } else {
            floors.front().copied()
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result as AnyResult;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::{ErrorKind, provider::ChainProvider};

    // Each test owns its endpoint: the injected floor is keyed by endpoint, and CI runs
    // these as threads in one process.
    const PRUNED_DATADIR: &str = "/var/lib/reth/pruned-datadir-fixture";
    const UNREADABLE_DATADIR: &str = "/var/lib/reth/absent-datadir-fixture";
    const REDO_DATADIR: &str = "/var/lib/reth/pruned-redo-datadir-fixture";
    const V1_REGISTRY_START: i64 = 3_327_417;
    const MERGE_RECEIPT_SEGMENT_START: i64 = 15_500_000;
    const RACE_CHAIN: &str = "ingest-floor-race";
    const RACE_BLOCK_HASH: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000001";
    const RACE_ADDRESS: &str = "0x0000000000000000000000000000000000000002";

    #[tokio::test]
    async fn a_range_below_the_node_floor_fails_the_phase_instead_of_completing() -> AnyResult<()> {
        let database = TestDatabase::create(TestDatabaseConfig::new("ingest_source_floor")).await?;
        let _floor = test_floors::install(PRUNED_DATADIR, [MERGE_RECEIPT_SEGMENT_START]);
        let engine = Engine::new(database.pool().clone());

        let historical = engine
            .run_batch(request(PRUNED_DATADIR, None))
            .await
            .expect_err("planning a pruned range must fail rather than complete");
        let redo = engine
            .run_batch(request(PRUNED_DATADIR, Some((3_000_000, 4_000_000))))
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
    async fn a_floor_rising_while_a_window_is_in_flight_stops_the_write() -> AnyResult<()> {
        let database = single_block_database("ingest_source_floor_race").await?;
        let endpoint = single_block_chain_endpoint().await?;
        // The node prunes mid-batch: planning finds block 0 servable, the re-read does not.
        let _floor = test_floors::install(&endpoint, [0, 1]);
        let engine = Engine::new(database.pool().clone());

        let error = engine
            .run_batch(BatchRequest {
                chain_id: RACE_CHAIN.to_owned(),
                sources: vec![SourceDescriptor {
                    key: "race-rpc".to_owned(),
                    kind: "rpc".to_owned(),
                    start_block: 0,
                    endpoint: endpoint.clone(),
                }],
                cursors: Vec::new(),
                redo_range: None,
                resume_current: None,
            })
            .await
            .expect_err("a window fetched below a risen floor must not be recorded");

        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("0..=0"), "{error}");
        let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(recorded, 0, "the refused window must leave no coverage");
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
            .enforce_source_floors(&request(UNREADABLE_DATADIR, None))
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

    #[tokio::test]
    async fn a_redo_range_above_the_floor_is_planned_on_a_pruned_node() -> AnyResult<()> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("ingest_source_floor_redo")).await?;
        let _floor = test_floors::install(REDO_DATADIR, [MERGE_RECEIPT_SEGMENT_START]);
        let engine = Engine::new(database.pool().clone());

        // The declared window starts below the floor, but this redo range does not.
        engine
            .enforce_source_floors(&request(
                REDO_DATADIR,
                Some((
                    MERGE_RECEIPT_SEGMENT_START,
                    MERGE_RECEIPT_SEGMENT_START + 100,
                )),
            ))
            .await?;
        database.cleanup().await
    }

    #[test]
    fn a_redo_range_that_ends_below_the_source_window_plans_nothing() {
        let source = source(PRUNED_DATADIR);

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

    async fn single_block_database(name: &str) -> AnyResult<TestDatabase> {
        let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
        for schema in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        ] {
            sqlx::raw_sql(schema).execute(database.pool()).await?;
        }
        sqlx::query(
            "
            INSERT INTO manifest_versions (
                manifest_version, namespace, source_family, chain_id,
                deployment_label, rollout_status, normalizer_version,
                file_path, manifest_payload
            )
            VALUES (1, 'test', 'test_floor', $1, 'fixture', 'active',
                    'ensip15@ens-normalize-0.1.1', 'fixture.toml', $2)
            ",
        )
        .bind(RACE_CHAIN)
        .bind(json!({
            "manifest_version": 1,
            "namespace": "test",
            "source_family": "test_floor",
            "chain": RACE_CHAIN,
            "deployment_epoch": "fixture",
            "rollout_status": "active",
            "normalizer_version": "ensip15@ens-normalize-0.1.1",
            "resolver_implementations": [],
            "capability_flags": {},
            "roots": [],
            "contracts": [{
                "name": "registry",
                "role": "registry",
                "address": RACE_ADDRESS,
                "proxy_kind": "none",
                "start_block": 0,
                "events": ["Transfer"]
            }],
            "discovery_rules": [],
            "abi": {
                "events": [{
                    "name": "Transfer",
                    "fragment": "event Transfer(address indexed from, address indexed to, uint256 value)",
                    "emitter_roles": ["registry"],
                    "normalized_events": []
                }],
                "calls": []
            }
        }))
        .execute(database.pool())
        .await?;
        let manifest_id: i64 =
            sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE chain_id = $1")
                .bind(RACE_CHAIN)
                .fetch_one(database.pool())
                .await?;
        let contract_id = uuid::Uuid::new_v4();
        sqlx::query(
            "
            INSERT INTO contract_instances (
                contract_instance_id, chain_id, contract_kind, provenance
            )
            VALUES ($1, $2, 'contract', '{}'::jsonb)
            ",
        )
        .bind(contract_id)
        .bind(RACE_CHAIN)
        .execute(database.pool())
        .await?;
        sqlx::query(
            "
            INSERT INTO manifest_contract_instances (
                manifest_id, chain_id, declaration_kind, declaration_name,
                contract_instance_id, declared_address, role, proxy_kind,
                start_block_number
            )
            VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
            ",
        )
        .bind(manifest_id)
        .bind(RACE_CHAIN)
        .bind(contract_id)
        .bind(RACE_ADDRESS)
        .execute(database.pool())
        .await?;
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, source_manifest_id, provenance
            )
            VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
            ",
        )
        .bind(contract_id)
        .bind(RACE_CHAIN)
        .bind(RACE_ADDRESS)
        .bind(manifest_id)
        .execute(database.pool())
        .await?;
        Ok(database)
    }

    /// Serves one canonical block, enough for a batch to plan, fetch, and try to store.
    async fn single_block_chain_endpoint() -> AnyResult<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/", listener.local_addr()?);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Some(body) = read_request_body(&mut socket).await {
                        let response =
                            serde_json::from_str::<Value>(&body).map_or(Value::Null, |request| {
                                match request {
                                    Value::Array(calls) => {
                                        Value::Array(calls.iter().map(respond).collect())
                                    }
                                    single => respond(&single),
                                }
                            });
                        let payload = response.to_string();
                        let http = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}",
                            payload.len()
                        );
                        if socket.write_all(http.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        Ok(endpoint)
    }

    async fn read_request_body(socket: &mut tokio::net::TcpStream) -> Option<String> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
            let text = String::from_utf8_lossy(&request).into_owned();
            if let Some(end) = text.find("\r\n\r\n") {
                let declared = text[..end]
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                let body = &text[end + 4..];
                if body.len() >= declared {
                    return Some(body.to_owned());
                }
            }
        }
    }

    fn respond(call: &Value) -> Value {
        let result = match call.get("method").and_then(Value::as_str) {
            Some("eth_getBlockByNumber" | "eth_getBlockByHash") => json!({
                "hash": RACE_BLOCK_HASH,
                "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "number": "0x0",
                "timestamp": "0x64"
            }),
            Some("eth_getLogs") => json!([]),
            _ => Value::Null,
        };
        json!({
            "jsonrpc": "2.0",
            "id": call.get("id").cloned().unwrap_or_else(|| json!(1)),
            "result": result
        })
    }

    fn request(endpoint: &str, redo_range: Option<(i64, i64)>) -> BatchRequest {
        BatchRequest {
            chain_id: "ethereum-mainnet".to_owned(),
            sources: vec![source(endpoint)],
            cursors: Vec::new(),
            redo_range,
            resume_current: None,
        }
    }

    fn source(endpoint: &str) -> SourceDescriptor {
        SourceDescriptor {
            key: "ethereum-reth".to_owned(),
            kind: "reth-db".to_owned(),
            start_block: V1_REGISTRY_START,
            endpoint: endpoint.to_owned(),
        }
    }
}
