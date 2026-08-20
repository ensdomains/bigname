use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result as AnyResult;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{ErrorKind, SourceCursor, engine::LiveBatchRequest, provider::ChainProvider};

#[derive(Clone)]
struct ScriptedRedoWindowLoader {
    windows: Arc<Mutex<VecDeque<ScriptedWindow>>>,
    loaded: Arc<Mutex<Vec<(String, Marker)>>>,
}

type ScriptedWindow = (String, i64, i64, Marker, Option<String>);

impl ScriptedRedoWindowLoader {
    fn new(windows: impl IntoIterator<Item = ScriptedWindow>) -> Self {
        Self {
            windows: Arc::new(Mutex::new(windows.into_iter().collect())),
            loaded: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl crate::engine::redo::RedoWindowLoader for ScriptedRedoWindowLoader {
    fn load<'a>(
        &'a self,
        _engine: &'a Engine,
        _chain_id: &'a str,
        source: &'a SourceDescriptor,
        _all_sources: &'a [SourceDescriptor],
        from: i64,
        to: i64,
    ) -> crate::engine::redo::RedoLoadFuture<'a> {
        Box::pin(async move {
            let (expected_source, expected_from, expected_to, marker, first_parent_hash) = self
                .windows
                .lock()
                .expect("scripted redo windows lock")
                .pop_front()
                .expect("scripted redo window");
            assert_eq!(
                (source.key.as_str(), from, to),
                (expected_source.as_str(), expected_from, expected_to)
            );
            self.loaded
                .lock()
                .expect("loaded redo windows lock")
                .push((source.key.clone(), marker.clone()));
            Ok(crate::engine::LoadedWindow {
                marker,
                first_parent_hash,
                estimated_write_bytes: 0,
            })
        })
    }
}

// Each test owns its endpoint: the injected floor is keyed by endpoint, and CI runs
// these as threads in one process.
const PRUNED_DATADIR: &str = "/var/lib/reth/pruned-datadir-fixture";
const UNREADABLE_DATADIR: &str = "/var/lib/reth/absent-datadir-fixture";
const REDO_DATADIR: &str = "/var/lib/reth/pruned-redo-datadir-fixture";
const V1_REGISTRY_START: i64 = 3_327_417;
const MERGE_RECEIPT_SEGMENT_START: i64 = 15_500_000;
const RACE_CHAIN: &str = "ingest-floor-race";
const RACE_BLOCK_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const RACE_ADDRESS: &str = "0x0000000000000000000000000000000000000002";
const HEAD_BLOCK_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
const TOP_BLOCK_SELECTOR: &str = "0x7fffffffffffffff";
const TOP_BLOCK_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000004";

#[tokio::test]
async fn a_range_below_the_node_floor_fails_the_phase_instead_of_completing() -> AnyResult<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("ingest_source_floor")).await?;
    let _floor = test_floors::install(PRUNED_DATADIR, MERGE_RECEIPT_SEGMENT_START);
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
    // The node prunes the moment it has served the window: planning finds block 0
    // servable, and only a floor read taken after the fetch sees otherwise.
    let node = test_floors::pruning_node(0, 1);
    let endpoint = single_block_chain_endpoint(Arc::clone(&node)).await?;
    let _floor = test_floors::install_node(&endpoint, node);
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
async fn a_live_suffix_below_the_floor_is_refused_before_it_is_loaded() -> AnyResult<()> {
    let database = single_block_database("ingest_source_floor_live").await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, NULL, 0, now(), 'finalized')
        ",
    )
    .bind(RACE_CHAIN)
    .bind(RACE_BLOCK_HASH)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO chain_heads (
            chain_id, latest_block_hash, latest_block_number,
            safe_block_hash, safe_block_number,
            finalized_block_hash, finalized_block_number
        )
        VALUES ($1, $2, 0, $2, 0, $2, 0)
        ",
    )
    .bind(RACE_CHAIN)
    .bind(RACE_BLOCK_HASH)
    .execute(database.pool())
    .await?;
    // The node pruned past the published head while this chain was not following. The
    // floor drops back once the window is served, so only a check taken before loading
    // can refuse it.
    let node = test_floors::pruning_node(2, 0);
    let endpoint = single_block_chain_endpoint(Arc::clone(&node)).await?;
    let _floor = test_floors::install_node(&endpoint, node);
    let engine = Engine::new(database.pool().clone());

    let error = engine
        .run_live_batch(LiveBatchRequest {
            chain_id: RACE_CHAIN.to_owned(),
            sources: vec![SourceDescriptor {
                key: "live-rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint: endpoint.clone(),
            }],
            live_handoff: Marker {
                number: 0,
                hash: RACE_BLOCK_HASH.to_owned(),
            },
        })
        .await
        .expect_err("a live suffix below the floor must be refused");

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("1..=1"), "{error}");
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_lineage")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(
        recorded, 1,
        "the refused suffix must leave the ancestry alone"
    );
    database.cleanup().await
}

#[tokio::test]
async fn an_out_of_range_resume_marker_fails_the_batch_before_any_network_use() -> AnyResult<()> {
    let database = single_block_database("ingest_redo_resume_refused").await?;
    let engine = Engine::new(database.pool().clone());

    // The endpoint is unroutable on purpose: the refusal must reach the caller before
    // any provider network use. Planning is side-effect-free — an RPC floor read
    // answers None without network — so the first resolve is the earliest touch.
    let error = engine
        .run_batch(BatchRequest {
            chain_id: RACE_CHAIN.to_owned(),
            sources: vec![SourceDescriptor {
                key: "redo-rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint: "http://127.0.0.1:9/".to_owned(),
            }],
            cursors: Vec::new(),
            redo_range: Some((10, 20)),
            resume_current: Some(Marker {
                number: 5,
                hash: "stale".to_owned(),
            }),
        })
        .await
        .expect_err("a resume marker below the redo range must be refused");

    // Only ErrorKind::Transient is retried; a refused marker has to stop the phase.
    assert_eq!(error.kind(), ErrorKind::Configuration);
    let message = error.to_string();
    for expected in ["5", "10..=20"] {
        assert!(message.contains(expected), "{message} must name {expected}");
    }
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_lineage")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(recorded, 0, "the refused batch must leave no coverage");
    database.cleanup().await
}

#[tokio::test]
async fn a_marker_at_the_top_of_the_block_space_completes_without_loading() -> AnyResult<()> {
    let database = single_block_database("ingest_redo_resume_top").await?;
    // The chain serves marker resolution only: a finished redo with durable evidence from
    // its completed boundary load must not fetch the window again.
    let node = test_floors::pruning_node(0, 0);
    let endpoint = single_block_chain_endpoint(node).await?;
    let engine = Engine::new(database.pool().clone());

    let outcome = engine
        .run_batch(BatchRequest {
            chain_id: RACE_CHAIN.to_owned(),
            sources: vec![SourceDescriptor {
                key: "redo-rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint,
            }],
            cursors: vec![SourceCursor {
                key: "redo-rpc".to_owned(),
                next_block: i64::MAX,
                target_block: Some(i64::MAX),
                last_processed: None,
                redo_loaded_boundary: Some(Marker {
                    number: i64::MAX,
                    hash: TOP_BLOCK_HASH.to_owned(),
                }),
            }],
            redo_range: Some((i64::MAX, i64::MAX)),
            resume_current: Some(Marker {
                number: i64::MAX,
                hash: TOP_BLOCK_HASH.to_owned(),
            }),
        })
        .await?;

    assert!(
        outcome.complete,
        "a marker at the inclusive range end completes the redo"
    );
    assert_eq!(
        outcome.current.number,
        i64::MAX,
        "completion resolves the range-end marker"
    );
    assert_eq!(
        outcome.estimated_write_bytes, 0,
        "nothing was left to fetch"
    );
    assert!(
        outcome
            .sources
            .iter()
            .all(|source| source.current.as_ref() == Some(&source.target)),
        "a complete redo marks every source at its target"
    );
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_lineage")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(recorded, 0, "no window may load: nothing was left to read");
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

#[tokio::test]
async fn a_completed_multi_source_redo_reloads_an_earlier_source_boundary() -> AnyResult<()> {
    let database = intake_database("ingest_redo_earlier_source_boundary", "base-mainnet").await?;
    let endpoint = marker_resolution_endpoint().await?;
    let engine = Engine::new(database.pool().clone());
    let seam = crate::BASE_COINBASE_SEAM_BLOCK;

    let error = engine
        .run_batch(BatchRequest {
            chain_id: "base-mainnet".to_owned(),
            sources: vec![
                SourceDescriptor {
                    key: "base-coinbase".to_owned(),
                    kind: "coinbase-sql".to_owned(),
                    start_block: 0,
                    endpoint: "coinbase-sql://must-be-reloaded".to_owned(),
                },
                SourceDescriptor {
                    key: "base-rpc".to_owned(),
                    kind: "rpc".to_owned(),
                    start_block: seam,
                    endpoint,
                },
            ],
            cursors: Vec::new(),
            redo_range: Some((seam - 255, seam + 1)),
            resume_current: Some(Marker {
                number: seam + 1,
                hash: marker_hash(seam + 1),
            }),
        })
        .await
        .expect_err("the final batch must attempt to reload the earlier Coinbase boundary");

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(
        error
            .to_string()
            .contains("failed to configure Coinbase SQL source"),
        "the observed error must come from entering the boundary load: {error}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn an_equal_height_source_boundary_uses_loaded_evidence_without_reloading() -> AnyResult<()> {
    let database = intake_database("ingest_redo_equal_source_boundary", "base-mainnet").await?;
    let endpoint = marker_resolution_endpoint().await?;
    let engine = Engine::new(database.pool().clone());
    let seam = crate::BASE_COINBASE_SEAM_BLOCK;

    let outcome = engine
        .run_batch(BatchRequest {
            chain_id: "base-mainnet".to_owned(),
            sources: vec![
                SourceDescriptor {
                    key: "base-coinbase".to_owned(),
                    kind: "coinbase-sql".to_owned(),
                    start_block: 0,
                    endpoint: "coinbase-sql://must-not-be-reloaded".to_owned(),
                },
                SourceDescriptor {
                    key: "base-rpc".to_owned(),
                    kind: "rpc".to_owned(),
                    start_block: seam,
                    endpoint,
                },
            ],
            cursors: vec![SourceCursor {
                key: "base-coinbase".to_owned(),
                next_block: seam + 1,
                target_block: Some(seam),
                last_processed: None,
                redo_loaded_boundary: Some(Marker {
                    number: seam,
                    hash: marker_hash(seam),
                }),
            }],
            redo_range: Some((seam - 255, seam + 1)),
            resume_current: Some(Marker {
                number: seam,
                hash: marker_hash(seam),
            }),
        })
        .await?;

    let range_end = Marker {
        number: seam + 1,
        hash: marker_hash(seam + 1),
    };
    assert!(outcome.complete);
    assert_eq!(outcome.current, range_end);
    assert_eq!(outcome.target, range_end);
    assert_eq!(
        outcome.sources[0].current,
        Some(Marker {
            number: seam,
            hash: marker_hash(seam),
        })
    );
    database.cleanup().await
}

#[tokio::test]
async fn an_intermediate_loaded_source_boundary_is_not_replaced_by_a_phase_summary() -> AnyResult<()>
{
    let database =
        intake_database("ingest_redo_intermediate_source_boundary", "base-mainnet").await?;
    let seam = crate::BASE_COINBASE_SEAM_BLOCK;
    let loaded_fork = Marker {
        number: seam,
        hash: marker_hash(seam + 10),
    };
    let summary_fork = Marker {
        number: seam,
        hash: marker_hash(seam + 20),
    };
    let endpoint = scripted_marker_endpoint(BTreeMap::from([(
        seam,
        vec![
            loaded_fork.hash.clone(),
            summary_fork.hash.clone(),
            summary_fork.hash.clone(),
        ],
    )]))
    .await?;
    let range_end = Marker {
        number: seam + 1,
        hash: marker_hash(seam + 1),
    };
    let loader = ScriptedRedoWindowLoader::new([
        (
            "base-coinbase".to_owned(),
            seam - 255,
            seam,
            loaded_fork.clone(),
            None,
        ),
        (
            "base-rpc".to_owned(),
            seam,
            seam,
            summary_fork.clone(),
            None,
        ),
        (
            "base-rpc".to_owned(),
            seam + 1,
            seam + 1,
            range_end.clone(),
            Some(summary_fork.hash.clone()),
        ),
    ]);
    let sources = vec![
        SourceDescriptor {
            key: "base-coinbase".to_owned(),
            kind: "coinbase-sql".to_owned(),
            start_block: 0,
            endpoint: "https://unused.invalid/".to_owned(),
        },
        SourceDescriptor {
            key: "base-rpc".to_owned(),
            kind: "rpc".to_owned(),
            start_block: seam,
            endpoint,
        },
    ];
    let engine = Engine::new(database.pool().clone());
    let first = engine
        .run_redo_batch_with_loader(
            &loader,
            BatchRequest {
                chain_id: "base-mainnet".to_owned(),
                sources: sources.clone(),
                cursors: Vec::new(),
                redo_range: Some((seam - 255, seam + 1)),
                resume_current: None,
            },
        )
        .await?;
    assert!(!first.complete);
    assert_eq!(first.current, summary_fork);
    assert_eq!(first.sources[0].current, Some(loaded_fork.clone()));

    let error = engine
        .run_redo_batch_with_loader(
            &loader,
            BatchRequest {
                chain_id: "base-mainnet".to_owned(),
                sources,
                cursors: vec![SourceCursor {
                    key: "base-coinbase".to_owned(),
                    next_block: seam + 1,
                    target_block: Some(seam),
                    last_processed: None,
                    redo_loaded_boundary: first.sources[0].loaded_boundary.clone(),
                }],
                redo_range: Some((seam - 255, seam + 1)),
                resume_current: Some(first.current),
            },
        )
        .await
        .expect_err("batch two must reject a fresh fork that differs from its loaded boundary");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains(crate::REDO_BOUNDARY_DIVERGENCE_PREFIX)
            && error.to_string().contains(&seam.to_string())
            && error.to_string().contains(&loaded_fork.hash)
            && error.to_string().contains(&summary_fork.hash),
        "{error}"
    );
    assert_eq!(
        loader
            .loaded
            .lock()
            .expect("loaded redo windows lock")
            .iter()
            .filter(|(source, _)| source == "base-coinbase")
            .count(),
        1,
        "batch two silently substituted the fresh phase summary without reloading Coinbase"
    );

    let consistent_endpoint = scripted_marker_endpoint(BTreeMap::from([(
        seam,
        vec![
            loaded_fork.hash.clone(),
            loaded_fork.hash.clone(),
            loaded_fork.hash.clone(),
        ],
    )]))
    .await?;
    let consistent_loader = ScriptedRedoWindowLoader::new([
        (
            "base-coinbase".to_owned(),
            seam - 255,
            seam,
            loaded_fork.clone(),
            None,
        ),
        ("base-rpc".to_owned(), seam, seam, loaded_fork.clone(), None),
        (
            "base-rpc".to_owned(),
            seam + 1,
            seam + 1,
            range_end.clone(),
            Some(loaded_fork.hash.clone()),
        ),
    ]);
    let consistent_sources = vec![
        SourceDescriptor {
            key: "base-coinbase".to_owned(),
            kind: "coinbase-sql".to_owned(),
            start_block: 0,
            endpoint: "https://unused.invalid/".to_owned(),
        },
        SourceDescriptor {
            key: "base-rpc".to_owned(),
            kind: "rpc".to_owned(),
            start_block: seam,
            endpoint: consistent_endpoint,
        },
    ];
    let retry_first = engine
        .run_redo_batch_with_loader(
            &consistent_loader,
            BatchRequest {
                chain_id: "base-mainnet".to_owned(),
                sources: consistent_sources.clone(),
                cursors: Vec::new(),
                redo_range: Some((seam - 255, seam + 1)),
                resume_current: None,
            },
        )
        .await?;
    let retry = engine
        .run_redo_batch_with_loader(
            &consistent_loader,
            BatchRequest {
                chain_id: "base-mainnet".to_owned(),
                sources: consistent_sources,
                cursors: vec![SourceCursor {
                    key: "base-coinbase".to_owned(),
                    next_block: seam + 1,
                    target_block: Some(seam),
                    last_processed: None,
                    redo_loaded_boundary: retry_first.sources[0].loaded_boundary.clone(),
                }],
                redo_range: Some((seam - 255, seam + 1)),
                resume_current: Some(retry_first.current),
            },
        )
        .await?;
    assert!(retry.complete);
    assert_eq!(retry.sources[0].current, Some(loaded_fork.clone()));
    assert!(
        consistent_loader
            .loaded
            .lock()
            .expect("loaded redo windows lock")
            .contains(&("base-coinbase".to_owned(), loaded_fork)),
        "the consistent rerun must retain the fork returned by the source load"
    );
    database.cleanup().await
}

#[tokio::test]
async fn resumed_redo_rejects_a_loaded_window_from_a_sibling_fork() -> AnyResult<()> {
    let chain_id = "ingest-redo-window-continuity";
    let database = intake_database("ingest_redo_window_continuity", chain_id).await?;
    let a_255 = marker_hash(255);
    let c_256 = marker_hash(1_256);
    let endpoint = scripted_marker_endpoint(BTreeMap::from([
        (255, vec![a_255.clone()]),
        (256, vec![c_256.clone(), c_256.clone(), c_256.clone()]),
    ]))
    .await?;
    let source = SourceDescriptor {
        key: "rpc".to_owned(),
        kind: "rpc".to_owned(),
        start_block: 0,
        endpoint,
    };
    let loader = ScriptedRedoWindowLoader::new([
        (
            "rpc".to_owned(),
            0,
            255,
            Marker {
                number: 255,
                hash: a_255.clone(),
            },
            None,
        ),
        (
            "rpc".to_owned(),
            256,
            256,
            Marker {
                number: 256,
                hash: c_256,
            },
            Some(marker_hash(1_255)),
        ),
    ]);
    let engine = Engine::new(database.pool().clone());
    let first = engine
        .run_redo_batch_with_loader(
            &loader,
            BatchRequest {
                chain_id: chain_id.to_owned(),
                sources: vec![source.clone()],
                cursors: Vec::new(),
                redo_range: Some((0, 256)),
                resume_current: None,
            },
        )
        .await?;
    assert_eq!(first.current.hash, a_255);

    let error = engine
        .run_redo_batch_with_loader(
            &loader,
            BatchRequest {
                chain_id: chain_id.to_owned(),
                sources: vec![source],
                cursors: Vec::new(),
                redo_range: Some((0, 256)),
                resume_current: Some(first.current),
            },
        )
        .await
        .expect_err("a resumed redo must stay on the fork loaded by its prior batch");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    database.cleanup().await
}

#[tokio::test]
async fn a_completed_range_below_every_source_start_resolves_its_summary_once() -> AnyResult<()> {
    let chain_id = "ingest-redo-no-range-end-owner";
    let database = intake_database("ingest_redo_no_range_end_owner", chain_id).await?;
    let (endpoint, range_end_resolutions) = changing_marker_endpoint(1).await?;
    let engine = Engine::new(database.pool().clone());

    let outcome = engine
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            sources: vec![SourceDescriptor {
                key: "future-rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 2,
                endpoint,
            }],
            cursors: Vec::new(),
            redo_range: Some((0, 1)),
            resume_current: None,
        })
        .await?;

    assert!(outcome.complete);
    assert_eq!(outcome.current, outcome.target);
    assert_eq!(outcome.current.number, 1);
    assert_eq!(
        range_end_resolutions.load(Ordering::SeqCst),
        1,
        "the no-owner summary must reuse one range-end resolution"
    );
    database.cleanup().await
}

#[tokio::test]
async fn an_equal_height_redo_marker_uses_durable_lineage_evidence() -> AnyResult<()> {
    let chain_id = "ingest-redo-equal-boundary";
    let database = intake_database("ingest_redo_equal_boundary", chain_id).await?;
    let durable = Marker {
        number: 100,
        hash: marker_hash(100),
    };
    let fresh = Marker {
        number: 100,
        hash: marker_hash(101),
    };
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, NULL, $3, now(), 'observed')
        ",
    )
    .bind(chain_id)
    .bind(&fresh.hash)
    .bind(fresh.number)
    .execute(database.pool())
    .await?;

    let error = crate::engine::redo::reject_lineage_backed_boundary_change(
        database.pool(),
        chain_id,
        Some(&durable),
        &fresh,
    )
    .await
    .expect_err("an equal-height lineage-backed fork change must fail closed");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains(crate::REDO_BOUNDARY_DIVERGENCE_PREFIX),
        "{error}"
    );

    crate::engine::redo::reject_lineage_backed_boundary_change(
        database.pool(),
        chain_id,
        Some(&durable),
        &durable,
    )
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
    let _floor = test_floors::install(REDO_DATADIR, MERGE_RECEIPT_SEGMENT_START);
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
        planned_range(&source, Some((0, V1_REGISTRY_START - 1)), None),
        None
    );
    assert_eq!(
        planned_range(&source, Some((0, V1_REGISTRY_START)), None),
        Some((V1_REGISTRY_START, Some(V1_REGISTRY_START)))
    );
    assert_eq!(
        planned_range(&source, None, None),
        Some((V1_REGISTRY_START, None))
    );
}

#[test]
fn a_resumed_redo_is_judged_on_what_it_has_left_to_read() {
    let source = source(PRUNED_DATADIR);
    let resumed = Marker {
        number: V1_REGISTRY_START + 150,
        hash: "resume".to_owned(),
    };

    assert_eq!(
        planned_range(
            &source,
            Some((V1_REGISTRY_START, V1_REGISTRY_START + 200)),
            Some(&resumed)
        ),
        Some((V1_REGISTRY_START + 151, Some(V1_REGISTRY_START + 200))),
        "a redo already durable through 150 must be judged on 151.."
    );
    assert_eq!(
        planned_range(
            &source,
            Some((V1_REGISTRY_START, V1_REGISTRY_START + 150)),
            Some(&resumed)
        ),
        None,
        "a redo with nothing left to read plans nothing"
    );
    let stale = Marker {
        number: V1_REGISTRY_START - 50,
        hash: "stale".to_owned(),
    };
    assert_eq!(
        planned_range(
            &source,
            Some((V1_REGISTRY_START, V1_REGISTRY_START + 200)),
            Some(&stale)
        ),
        Some((V1_REGISTRY_START, Some(V1_REGISTRY_START + 200))),
        "a marker below the range still starts at the range start"
    );
}

#[test]
fn a_marker_at_the_top_of_the_block_space_plans_nothing() {
    let top = Marker {
        number: i64::MAX,
        hash: TOP_BLOCK_HASH.to_owned(),
    };

    assert_eq!(
        planned_range(
            &source(PRUNED_DATADIR),
            Some((i64::MAX, i64::MAX)),
            Some(&top)
        ),
        None,
        "a marker at i64::MAX has no successor, so nothing remains to plan"
    );
}

async fn single_block_database(name: &str) -> AnyResult<TestDatabase> {
    intake_database(name, RACE_CHAIN).await
}

async fn intake_database(name: &str, chain_id: &str) -> AnyResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for schema in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
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
    .bind(chain_id)
    .bind(json!({
        "manifest_version": 1,
        "namespace": "test",
        "source_family": "test_floor",
        "chain": chain_id,
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
            .bind(chain_id)
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
    .bind(chain_id)
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
    .bind(chain_id)
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
    .bind(chain_id)
    .bind(RACE_ADDRESS)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;
    Ok(database)
}

async fn marker_resolution_endpoint() -> AnyResult<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                while let Some(body) = read_request_body(&mut socket).await {
                    let response =
                        serde_json::from_str::<Value>(&body).map_or(Value::Null, |request| {
                            match request {
                                Value::Array(calls) => Value::Array(
                                    calls.iter().map(marker_resolution_response).collect(),
                                ),
                                single => marker_resolution_response(&single),
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

async fn scripted_marker_endpoint(markers: BTreeMap<i64, Vec<String>>) -> AnyResult<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let markers = Arc::new(Mutex::new(
        markers
            .into_iter()
            .map(|(number, hashes)| (number, hashes.into_iter().collect::<VecDeque<_>>()))
            .collect::<BTreeMap<_, _>>(),
    ));
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let markers = Arc::clone(&markers);
            tokio::spawn(async move {
                while let Some(body) = read_request_body(&mut socket).await {
                    let response =
                        serde_json::from_str::<Value>(&body).map_or(Value::Null, |request| {
                            match request {
                                Value::Array(calls) => Value::Array(
                                    calls
                                        .iter()
                                        .map(|call| scripted_marker_response(call, &markers))
                                        .collect(),
                                ),
                                single => scripted_marker_response(&single, &markers),
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

fn scripted_marker_response(
    call: &Value,
    markers: &Mutex<BTreeMap<i64, VecDeque<String>>>,
) -> Value {
    let mut response = marker_resolution_response(call);
    if call.get("method").and_then(Value::as_str) != Some("eth_getBlockByNumber") {
        return response;
    }
    let number = call
        .pointer("/params/0")
        .and_then(Value::as_str)
        .and_then(|selector| selector.strip_prefix("0x"))
        .and_then(|number| i64::from_str_radix(number, 16).ok())
        .unwrap_or_default();
    if let Some(hash) = markers
        .lock()
        .expect("scripted marker lock")
        .get_mut(&number)
        .and_then(VecDeque::pop_front)
    {
        response["result"]["hash"] = json!(hash);
    }
    response
}

async fn changing_marker_endpoint(counted_number: i64) -> AnyResult<(String, Arc<AtomicUsize>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let resolutions = Arc::new(AtomicUsize::new(0));
    let server_resolutions = Arc::clone(&resolutions);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let resolutions = Arc::clone(&server_resolutions);
            tokio::spawn(async move {
                while let Some(body) = read_request_body(&mut socket).await {
                    let response = serde_json::from_str::<Value>(&body)
                        .map_or(Value::Null, |request| {
                            changing_marker_response(&request, counted_number, &resolutions)
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
    Ok((endpoint, resolutions))
}

fn changing_marker_response(call: &Value, counted_number: i64, resolutions: &AtomicUsize) -> Value {
    let mut response = marker_resolution_response(call);
    let selector = call
        .pointer("/params/0")
        .and_then(Value::as_str)
        .unwrap_or("0x0");
    let number = selector
        .strip_prefix("0x")
        .and_then(|number| i64::from_str_radix(number, 16).ok())
        .unwrap_or_default();
    if call.get("method").and_then(Value::as_str) == Some("eth_getBlockByNumber")
        && number == counted_number
    {
        let resolution = resolutions.fetch_add(1, Ordering::SeqCst) as i64;
        response["result"]["hash"] = json!(marker_hash(number + resolution));
    }
    response
}

fn marker_resolution_response(call: &Value) -> Value {
    if call.get("method").and_then(Value::as_str) == Some("eth_getLogs") {
        return json!({
            "jsonrpc": "2.0",
            "id": call.get("id").cloned().unwrap_or_else(|| json!(1)),
            "result": []
        });
    }
    let selector = call
        .pointer("/params/0")
        .and_then(Value::as_str)
        .unwrap_or("0x0");
    let number = selector
        .strip_prefix("0x")
        .and_then(|number| i64::from_str_radix(number, 16).ok())
        .unwrap_or_default();
    json!({
        "jsonrpc": "2.0",
        "id": call.get("id").cloned().unwrap_or_else(|| json!(1)),
        "result": {
            "hash": marker_hash(number),
            "parentHash": marker_hash(number.saturating_sub(1)),
            "number": format!("0x{number:x}"),
            "timestamp": "0x65"
        }
    })
}

fn marker_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

/// Serves one canonical block, enough for a batch to plan, fetch, and try to store.
///
/// Serving the block payloads is the last read of a window, so the node prunes there:
/// a floor read taken before the fetch still sees the pre-prune floor.
async fn single_block_chain_endpoint(node: Arc<test_floors::PruningNode>) -> AnyResult<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let node = Arc::clone(&node);
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
                    if body.contains("eth_getBlockByHash") {
                        node.observe_fetch();
                    }
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
    let selector = call
        .get("params")
        .and_then(|params| params.get(0))
        .and_then(Value::as_str)
        .unwrap_or("latest");
    let result = match call.get("method").and_then(Value::as_str) {
        Some("eth_getBlockByNumber" | "eth_getBlockByHash") => block_json(selector),
        Some("eth_getLogs") => json!([]),
        _ => Value::Null,
    };
    json!({
        "jsonrpc": "2.0",
        "id": call.get("id").cloned().unwrap_or_else(|| json!(1)),
        "result": result
    })
}

/// Block 0, plus block 1 for callers that need a head above a stored ancestor, and
/// the top of the block space for a redo whose range ends there.
fn block_json(selector: &str) -> Value {
    if selector == TOP_BLOCK_SELECTOR {
        return json!({
            "hash": TOP_BLOCK_HASH,
            "parentHash": HEAD_BLOCK_HASH,
            "number": TOP_BLOCK_SELECTOR,
            "timestamp": "0x66"
        });
    }
    let one =
        matches!(selector, "0x1" | "latest" | "safe" | "finalized") || selector == HEAD_BLOCK_HASH;
    if one {
        return json!({
            "hash": HEAD_BLOCK_HASH,
            "parentHash": RACE_BLOCK_HASH,
            "number": "0x1",
            "timestamp": "0x65"
        });
    }
    json!({
        "hash": RACE_BLOCK_HASH,
        "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "number": "0x0",
        "timestamp": "0x64"
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
