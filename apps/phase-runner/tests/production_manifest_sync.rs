#[allow(dead_code)]
mod support;

use std::{fs, time::Duration};

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    heads::BlockMarker,
    phase::{BlockRange, PhaseName, PhaseSet, RunMode},
    rewind::rewind_to_ancestor,
    runner::{PhaseRunner, RedoPhase},
    state::{PhaseStore, StartDisposition},
};
use sqlx::types::Uuid;
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

type AddressEpoch = (i64, Uuid, Option<i64>, Option<i64>, bool);

struct WatchManifestFixture {
    root: std::path::PathBuf,
    chain_id: String,
    source_family: String,
}

impl WatchManifestFixture {
    fn new(chain_id: &str) -> Result<Self> {
        Self::with_source_family(chain_id, "test_events")
    }

    fn with_source_family(chain_id: &str, source_family: &str) -> Result<Self> {
        let root =
            std::env::temp_dir().join(format!("bigname-manifest-widening-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("test").join(source_family))?;
        Ok(Self {
            root,
            chain_id: chain_id.to_owned(),
            source_family: source_family.to_owned(),
        })
    }

    fn write(&self, include_new_event: bool, include_new_contract: bool) -> Result<()> {
        self.write_with_start(include_new_event, include_new_contract, 0)
    }

    fn write_with_start(
        &self,
        include_new_event: bool,
        include_new_contract: bool,
        source_a_start: u64,
    ) -> Result<()> {
        let new_event = include_new_event.then_some(
            r#"
[[abi.events]]
name = "Widened"
fragment = "event Widened(bytes32 indexed value)"
emitter_roles = ["source_a"]
normalized_events = []
status = "supported"
"#,
        );
        let new_contract = include_new_contract.then_some(
            r#"
[[contracts]]
role = "source_b"
address = "0x0000000000000000000000000000000000000006"
proxy_kind = "none"
start_block = 0
"#,
        );
        let manifest = format!(
            r#"manifest_version = 1
namespace = "test"
source_family = "{}"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]

[[contracts]]
role = "source_a"
address = "0x0000000000000000000000000000000000000004"
proxy_kind = "none"
start_block = {}
{}

[[abi.events]]
name = "Transfer"
fragment = "event Transfer(address indexed from, address indexed to, uint256 value)"
emitter_roles = ["source_a"]
normalized_events = []
status = "supported"
{}
"#,
            self.source_family,
            self.chain_id,
            source_a_start,
            new_contract.unwrap_or_default(),
            new_event.unwrap_or_default(),
        );
        fs::write(
            self.root
                .join("test")
                .join(&self.source_family)
                .join("v1.toml"),
            manifest,
        )?;
        Ok(())
    }

    fn add_discovery_rule(&self, edge_kind: &str) -> Result<()> {
        self.add_discovery_rule_from_role(edge_kind, "source_a")
    }

    fn add_discovery_rule_from_role(&self, edge_kind: &str, from_role: &str) -> Result<()> {
        let path = self
            .root
            .join("test")
            .join(&self.source_family)
            .join("v1.toml");
        let manifest = fs::read_to_string(&path)?.replace(
            "discovery_rules = []",
            &format!(
                r#"[[discovery_rules]]
edge_kind = "{edge_kind}"
from_role = "{from_role}"
admission = "reachable_from_root""#
            ),
        );
        fs::write(path, manifest)?;
        Ok(())
    }

    fn replace_source_a(&self, address: &str, start_block: u64) -> Result<()> {
        let path = self
            .root
            .join("test")
            .join(&self.source_family)
            .join("v1.toml");
        let prior = r#"address = "0x0000000000000000000000000000000000000004"
proxy_kind = "none"
start_block = 0"#;
        let replacement = format!(
            r#"address = "{address}"
proxy_kind = "none"
start_block = {start_block}"#
        );
        let manifest = fs::read_to_string(&path)?.replacen(prior, &replacement, 1);
        fs::write(path, manifest)?;
        Ok(())
    }

    fn add_source_a_root(&self, address: &str, start_block: u64) -> Result<()> {
        let path = self
            .root
            .join("test")
            .join(&self.source_family)
            .join("v1.toml");
        let root = format!(
            r#"[[roots]]
name = "source_a"
address = "{address}"
start_block = {start_block}"#
        );
        let manifest = fs::read_to_string(&path)?.replace("roots = []", &root);
        fs::write(path, manifest)?;
        Ok(())
    }

    fn move_source_a_root_start(&self, from: u64, to: u64) -> Result<()> {
        let path = self
            .root
            .join("test")
            .join(&self.source_family)
            .join("v1.toml");
        let manifest = fs::read_to_string(&path)?.replacen(
            &format!("start_block = {from}"),
            &format!("start_block = {to}"),
            1,
        );
        fs::write(path, manifest)?;
        Ok(())
    }

    fn write_cross_namespace_pair(
        &self,
        earlier_start: u64,
        later_start: u64,
        edge_kind: &str,
    ) -> Result<()> {
        self.write_namespace_emitter("alpha", earlier_start, edge_kind, true)?;
        self.write_namespace_emitter("zeta", later_start, edge_kind, false)
    }

    fn write_namespace_emitter(
        &self,
        namespace: &str,
        start: u64,
        edge_kind: &str,
        as_root: bool,
    ) -> Result<()> {
        let (roots, contracts, emitter_role) = if as_root {
            (
                format!(
                    r#"[[roots]]
name = "source_a"
address = "0x0000000000000000000000000000000000000004"
start_block = {start}"#
                ),
                r#"[[contracts]]
role = "event_source"
address = "0x0000000000000000000000000000000000000005"
proxy_kind = "none"
start_block = 100"#
                    .to_owned(),
                "event_source",
            )
        } else {
            (
                "roots = []".to_owned(),
                format!(
                    r#"[[contracts]]
role = "source_a"
address = "0x0000000000000000000000000000000000000004"
proxy_kind = "none"
start_block = {start}"#
                ),
                "source_a",
            )
        };
        let manifest = format!(
            r#"manifest_version = 1
namespace = "{namespace}"
source_family = "{}"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
{roots}

[capability_flags]

{contracts}

[[discovery_rules]]
edge_kind = "{edge_kind}"
from_role = "source_a"
admission = "reachable_from_root"

[[abi.events]]
name = "Transfer"
fragment = "event Transfer(address indexed from, address indexed to, uint256 value)"
emitter_roles = ["{emitter_role}"]
normalized_events = []
status = "supported"
"#,
            self.source_family, self.chain_id,
        );
        let directory = self.root.join(namespace).join(&self.source_family);
        fs::create_dir_all(&directory)?;
        fs::write(directory.join("v1.toml"), manifest)?;
        Ok(())
    }
}

impl Drop for WatchManifestFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn loopback_runner(scratch: &ScratchDatabase, instance_id: &str) -> Result<PhaseRunner> {
    Ok(PhaseRunner::new(
        scratch.runner(),
        PhaseSet::loopback(),
        CapacityGuard::system(CapacityConfig::default()),
        instance_id,
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?)
}

#[tokio::test]
async fn widening_an_ingested_manifest_event_blocks_initial_derivation_until_reingest() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_manifest_sync_ingest_widening_gap").await?;
    let chain_id = "manifest-widening-gap";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;

    let chain = seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write(true, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    let raw_new_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(raw_new_event_count, 0);
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1))
    );

    let runner = loopback_runner(&scratch, "manifest-widening-gap-runner")?;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        runner.run_chain(&chain, CancellationToken::new()),
    )
    .await
    .context("manifest widening allowed derivation over raw facts without the new event")?
    .expect_err("manifest widening must block derivation until the explicit Ingest redo");
    assert!(error.to_string().contains("redo --chain"));

    scratch.cleanup().await
}

#[tokio::test]
async fn widening_after_rewind_targets_the_readable_head_and_can_complete() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_widening_after_rewind").await?;
    let chain_id = "manifest-widening-after-rewind";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    let chain = seed_completed_ingest_range(&scratch, chain_id).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = CASE WHEN phase_name = 'interpret' THEN 1 ELSE 0 END,
             current_block_hash = CASE WHEN phase_name = 'interpret' THEN $2 ELSE $3 END,
             target_block_number = CASE WHEN phase_name = 'interpret' THEN 1 ELSE 0 END,
             target_block_hash = CASE WHEN phase_name = 'interpret' THEN $2 ELSE $3 END,
             input_content_hash = $4,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-manifest-sync-head-1"))
    .bind(format!("{chain_id}-manifest-sync-head-0"))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;

    let block_0 = format!("{chain_id}-manifest-sync-head-0");
    rewind_to_ancestor(&scratch.runner(), chain_id, BlockMarker::new(0, block_0)?).await?;
    fixture.write(true, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 0)),
        "rewound orphaned cursor suffixes are not currently published coverage"
    );

    loopback_runner(&scratch, "manifest-widening-rewind-redo")?
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);

    let disposition = PhaseStore::new(scratch.pool().clone())
        .start_phase(chain_id, PhaseName::Live, &RunMode::Normal)
        .await
        .context("Live must be able to republish the replacement suffix before derived redo")?;
    assert_eq!(disposition, StartDisposition::Started);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_preserves_a_required_ingest_obligation_above_the_new_head() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_widening_before_rewind").await?;
    let chain_id = "manifest-widening-before-rewind";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write(true, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1))
    );

    let block_0 = format!("{chain_id}-manifest-sync-head-0");
    rewind_to_ancestor(&scratch.runner(), chain_id, BlockMarker::new(0, block_0)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1)),
        "rewind must preserve the obligation for Live to make readable again"
    );
    let latest: i64 =
        sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(
        latest, 0,
        "rewind must publish the requested readable ancestor"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_watch_comparison_trips_only_for_covered_widenings() -> Result<()> {
    for (name, before, after, before_start, after_start, already_ingested, expected_redo) in [
        (
            "new-contract",
            (false, false),
            (false, true),
            0,
            0,
            true,
            Some((0, 1)),
        ),
        (
            "start-block-lowering",
            (false, false),
            (false, false),
            1,
            0,
            true,
            Some((0, 1)),
        ),
        ("narrowing", (true, true), (false, false), 0, 0, true, None),
        ("same-set", (false, false), (false, false), 0, 0, true, None),
        (
            "fresh-chain",
            (false, false),
            (true, false),
            0,
            0,
            false,
            None,
        ),
    ] {
        let scratch =
            ScratchDatabase::create(&format!("production_manifest_watch_comparison_{}", name))
                .await?;
        let chain_id = format!("manifest-watch-{name}");
        let fixture = WatchManifestFixture::new(&chain_id)?;
        fixture.write_with_start(before.0, before.1, before_start)?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        if already_ingested {
            seed_completed_ingest_range(&scratch, &chain_id).await?;
        }

        fixture.write_with_start(after.0, after.1, after_start)?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        assert_eq!(
            required_ingest_redo(scratch.pool(), &chain_id).await?,
            expected_redo,
            "unexpected Ingest obligation for {name}"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn required_ingest_obligation_persists_after_the_watch_plan_narrows_back() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_narrow_back_keeps_redo").await?;
    let chain_id = "manifest-narrow-back-keeps-redo";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write(true, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1))
    );

    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1)),
        "narrowing must not clear an uncompleted historical-fetch obligation"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn adding_a_declared_address_to_a_discovery_family_requires_reingest() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_discovery_family_address").await?;
    let chain_id = "manifest-discovery-family-address";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v1_resolver_l1")?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write(false, true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1)),
        "a family-scoped discovery watch does not cover a newly declared direct address"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn adding_a_discovery_rule_over_ingested_history_is_rejected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_discovery_rule_widening").await?;
    let chain_id = "manifest-discovery-rule-widening";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.add_discovery_rule("resolver")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("historical indexability admission needs prior discovery derivation");
    assert!(error.to_string().contains("discovery rule"));
    for edge_kind in ["subregistry", "registry_announcement"] {
        fixture.write(false, false)?;
        fixture.add_discovery_rule(edge_kind)?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    }
    scratch.cleanup().await?;

    let fresh = ScratchDatabase::create("production_manifest_fresh_discovery_rule").await?;
    let fresh_chain = "manifest-fresh-discovery-rule";
    let fresh_fixture = WatchManifestFixture::new(fresh_chain)?;
    fresh_fixture.write(false, false)?;
    fresh_fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(fresh.pool(), &load_repository(&fresh_fixture.root)?).await?;
    seed_completed_ingest_range(&fresh, fresh_chain).await?;
    fresh_fixture.write(false, false)?;
    sync_schema_v2_repository(fresh.pool(), &load_repository(&fresh_fixture.root)?).await?;

    fresh.cleanup().await
}

#[tokio::test]
async fn replacing_a_resolver_discovery_source_over_ingested_history_is_rejected() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_manifest_resolver_source_replacement").await?;
    let chain_id = "manifest-resolver-source-replacement";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_source_a("0x0000000000000000000000000000000000000007", 0)?;
    let result = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            assert_eq!(
                required_ingest_redo(scratch.pool(), chain_id).await?,
                Some((0, 1)),
                "the slipped transition should only stamp the new declaration's watched logs"
            );
            let resolver_logs: i64 =
                sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE chain_id = $1")
                    .bind(chain_id)
                    .fetch_one(scratch.pool())
                    .await?;
            assert_eq!(
                resolver_logs, 0,
                "the stamped pass has no resolver addresses until Interpret materializes them"
            );
            panic!(
                "resolver discovery source replacement slipped through as an ordinary watch-plan widening"
            );
        }
    };
    assert!(
        error
            .to_string()
            .contains("resolver discovery source replacement"),
        "the loud rejection must name the transition: {error}"
    );

    scratch.cleanup().await?;

    let fresh = ScratchDatabase::create("production_manifest_fresh_resolver_replacement").await?;
    let fresh_chain = "manifest-fresh-resolver-replacement";
    let fresh_fixture = WatchManifestFixture::new(fresh_chain)?;
    fresh_fixture.write(false, false)?;
    fresh_fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(fresh.pool(), &load_repository(&fresh_fixture.root)?).await?;
    seed_chain_head(fresh.pool(), fresh_chain, 0).await?;
    fresh_fixture.replace_source_a("0x0000000000000000000000000000000000000007", 0)?;
    sync_schema_v2_repository(fresh.pool(), &load_repository(&fresh_fixture.root)?).await?;
    fresh.cleanup().await?;

    let future = ScratchDatabase::create("production_manifest_future_resolver_replacement").await?;
    let future_chain = "manifest-future-resolver-replacement";
    let future_fixture = WatchManifestFixture::new(future_chain)?;
    future_fixture.write(false, false)?;
    future_fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(future.pool(), &load_repository(&future_fixture.root)?).await?;
    seed_completed_ingest_range(&future, future_chain).await?;
    future_fixture.replace_source_a("0x0000000000000000000000000000000000000007", 2)?;
    sync_schema_v2_repository(future.pool(), &load_repository(&future_fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(future.pool(), future_chain).await?,
        None,
        "a source replacement beginning after retained history remains admissible"
    );

    future.cleanup().await
}

#[tokio::test]
async fn moving_one_of_multiple_resolver_sources_into_history_is_rejected() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_manifest_resolver_source_start_widening").await?;
    let chain_id = "manifest-resolver-source-start-widening";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    fixture.add_source_a_root("0x0000000000000000000000000000000000000008", 1)?;
    fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.move_source_a_root_start(1, 0)?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("each resolver-rule emitter must retain its own covered start");
    assert!(
        error.to_string().contains("discovery rule widening"),
        "{error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn cross_namespace_resolver_start_widening_is_order_independent() -> Result<()> {
    for (case, earlier_start, later_start) in
        [("earlier-namespace", 50, 100), ("later-namespace", 100, 50)]
    {
        let scratch = ScratchDatabase::create(&format!(
            "production_manifest_cross_namespace_resolver_{case}"
        ))
        .await?;
        let chain_id = format!("manifest-cross-namespace-resolver-{case}");
        let fixture = WatchManifestFixture::with_source_family(&chain_id, "ens_v1_registrar_l1")?;
        fixture.write_cross_namespace_pair(100, 100, "resolver")?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        seed_completed_ingest_range_through(&scratch, &chain_id, 100).await?;

        fixture.write_cross_namespace_pair(earlier_start, later_start, "resolver")?;
        let result =
            sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
        let error = match result {
            Err(error) => error,
            Ok(_) => {
                assert_eq!(
                    required_ingest_redo(scratch.pool(), &chain_id).await?,
                    Some((50, 100)),
                    "the slipped transition should stamp only an ordinary Ingest redo"
                );
                panic!(
                    "{case} resolver discovery widening slipped through as an ordinary watch-plan widening"
                );
            }
        };
        assert!(
            error.to_string().contains("discovery rule widening"),
            "the loud rejection must name the transition for {case}: {error}"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn cross_namespace_watch_entries_min_merge_the_earlier_start() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_cross_namespace_watch_min").await?;
    let chain_id = "manifest-cross-namespace-watch-min";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v1_registrar_l1")?;
    fixture.write_cross_namespace_pair(100, 100, "subregistry")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range_through(&scratch, chain_id, 100).await?;

    fixture.write_cross_namespace_pair(50, 100, "subregistry")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((50, 100)),
        "duplicate cross-namespace watch keys must retain the minimum start"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn future_resolver_source_replacement_stays_admissible_beside_a_historical_emitter()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_future_resolver_source_set").await?;
    let chain_id = "manifest-future-resolver-source-set";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    fixture.add_source_a_root("0x0000000000000000000000000000000000000008", 0)?;
    fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    let path = fixture
        .root
        .join("test")
        .join(&fixture.source_family)
        .join("v1.toml");
    let manifest = fs::read_to_string(&path)?.replace(
        "0x0000000000000000000000000000000000000008",
        "0x0000000000000000000000000000000000000009",
    );
    fs::write(path, manifest)?;
    fixture.move_source_a_root_start(0, 2)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);

    scratch.cleanup().await
}

#[tokio::test]
async fn adding_a_resolver_source_names_rule_widening_not_replacement() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_resolver_source_addition").await?;
    let chain_id = "manifest-resolver-source-addition";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    fixture.add_discovery_rule("resolver")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.add_source_a_root("0x0000000000000000000000000000000000000008", 0)?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("adding a historical resolver-rule emitter must reject");
    assert!(
        error.to_string().contains("discovery rule widening")
            && !error.to_string().contains("source replacement"),
        "an additive transition must be diagnosed accurately: {error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn removing_the_only_resolver_rule_emitter_is_admissible_narrowing() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_resolver_emitter_removal").await?;
    let chain_id = "manifest-resolver-emitter-removal";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, true)?;
    fixture.add_discovery_rule_from_role("resolver", "source_b")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write(false, false)?;
    fixture.add_discovery_rule_from_role("resolver", "source_b")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        None,
        "removing the only emitter narrows the discovery rule and needs no redo"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn adding_the_first_resolver_rule_emitter_names_rule_widening() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_first_resolver_emitter").await?;
    let chain_id = "manifest-first-resolver-emitter";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    fixture.add_discovery_rule_from_role("resolver", "source_b")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write(false, true)?;
    fixture.add_discovery_rule_from_role("resolver", "source_b")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("adding the first historical resolver-rule emitter must reject");
    assert!(
        error.to_string().contains("discovery rule widening")
            && !error.to_string().contains("source replacement"),
        "the first emitter is widening, not replacement: {error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn compiled_emitter_policy_widening_with_unchanged_manifest_requires_reingest() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_manifest_compiled_policy_widening").await?;
    let chain_id = "manifest-compiled-policy-widening";
    let family = "ens_v1_resolver_l1";
    let fixture = WatchManifestFixture::with_source_family(chain_id, family)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    let topic0 = format!("{:#x}", keccak256(b"Transfer(address,address,uint256)"));
    let narrower_compiled_watch = serde_json::json!([{
        "emitter": {
            "kind": "address",
            "family": family,
            "address": "0x0000000000000000000000000000000000000004"
        },
        "topic0": topic0,
        "start": 0
    }]);
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload, '{_bigname_compiled_watch}', $2::jsonb
         )
         WHERE chain_id = $1 AND rollout_status = 'active'",
    )
    .bind(chain_id)
    .bind(narrower_compiled_watch)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0, redo_to_block_number = 1,
             redo_current_block_number = 1, redo_current_block_hash = 'old-policy-checkpoint',
             redo_manifest_authority_fingerprint = repeat('a', 64),
             last_error = 'interrupted old-policy redo',
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1)),
        "persisted compiled emitter scope must survive a binary policy change"
    );
    let discarded_checkpoint: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT redo_current_block_number, redo_manifest_authority_fingerprint
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(discarded_checkpoint, (None, None));

    scratch.cleanup().await
}

#[tokio::test]
async fn legacy_payload_upgrade_materializes_compiled_watch_without_spurious_ingest() -> Result<()>
{
    for (case, has_derived_output) in [("derived", true), ("fresh", false)] {
        let scratch =
            ScratchDatabase::create(&format!("production_manifest_legacy_compiled_watch_{case}"))
                .await?;
        let chain_id = format!("manifest-legacy-compiled-watch-{case}");
        let fixture = WatchManifestFixture::new(&chain_id)?;
        fixture.write(false, false)?;
        let repository = load_repository(&fixture.root)?;
        sync_schema_v2_repository(scratch.pool(), &repository).await?;

        if has_derived_output {
            seed_completed_ingest_range(&scratch, &chain_id).await?;
            let head_hash = format!("{chain_id}-manifest-sync-head-1");
            sqlx::query(
                "UPDATE chain_phase_state
                 SET phase_status = 'completed', current_block_number = 1,
                     current_block_hash = $2, target_block_number = 1,
                     target_block_hash = $2, input_content_hash = $3,
                     started_at = now(), finished_at = now()
                 WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
            )
            .bind(&chain_id)
            .bind(head_hash)
            .bind(INTERPRETER_CONTENT_HASH)
            .execute(scratch.pool())
            .await?;
        } else {
            PhaseStore::new(scratch.pool().clone())
                .initialize_chain(&chain_id)
                .await?;
        }

        let removed = sqlx::query(
            "UPDATE manifest_versions
             SET manifest_payload = manifest_payload - '_bigname_compiled_watch'
             WHERE chain_id = $1 AND rollout_status = 'active'",
        )
        .bind(&chain_id)
        .execute(scratch.pool())
        .await?;
        assert_eq!(removed.rows_affected(), 1);

        sync_schema_v2_repository(scratch.pool(), &repository).await?;

        let snapshot_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM manifest_versions
             WHERE chain_id = $1 AND rollout_status = 'active'
               AND manifest_payload ? '_bigname_compiled_watch'",
        )
        .bind(&chain_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(
            snapshot_count, 1,
            "the first upgraded sync must persist the snapshot"
        );
        assert_eq!(
            required_ingest_redo(scratch.pool(), &chain_id).await?,
            None,
            "compiling the legacy side under the current binary must not invent widening"
        );

        let derived_hashes: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT input_content_hash FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
             ORDER BY phase_name",
        )
        .bind(&chain_id)
        .fetch_all(scratch.pool())
        .await?;
        assert_eq!(derived_hashes.len(), 2);
        if has_derived_output {
            assert!(derived_hashes.iter().all(|hash| {
                hash.as_deref()
                    .is_some_and(|hash| hash.starts_with("manifest-authority:"))
            }));
        } else {
            assert_eq!(derived_hashes, [None, None]);
        }

        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn schema_v2_manifest_sync_is_idempotent_and_retires_absent_history() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&root)?;

    let first = sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let manifest_ids: Vec<i64> =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions ORDER BY manifest_id")
            .fetch_all(scratch.pool())
            .await?;
    let first_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'SourceManifestUpdated'",
    )
    .fetch_one(scratch.pool())
    .await?;
    let second = sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let repeated_manifest_ids: Vec<i64> =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions ORDER BY manifest_id")
            .fetch_all(scratch.pool())
            .await?;

    assert!(first.manifest_count > 0);
    assert!(first.declaration_count > 0);
    assert!(first.discovery_rule_count > 0);
    assert_eq!(first, second);
    assert_eq!(manifest_ids, repeated_manifest_ids);
    assert_eq!(first_event_count, first.manifest_count as i64);
    let repeated_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'SourceManifestUpdated'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repeated_event_count, first_event_count);
    let mismatched_epoch_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM manifest_versions
        WHERE deployment_label <> manifest_payload ->> 'deployment_epoch'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(mismatched_epoch_count, 0);

    let retained_manifest: (i64, String, i64) = sqlx::query_as(
        "
        SELECT manifest.manifest_id,
               manifest.rollout_status,
               count(declaration.manifest_contract_instance_id)
        FROM manifest_versions manifest
        LEFT JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
        WHERE manifest.namespace = 'ens'
          AND manifest.source_family = 'ens_v1_reverse_l1'
          AND manifest.chain_id = 'ethereum-mainnet'
        GROUP BY manifest.manifest_id, manifest.rollout_status
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(retained_manifest.1, "active");
    assert!(retained_manifest.2 > 0);

    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    let base_repository = load_repository(root.join("base"))?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    let after_subset_sync: (i64, String, i64) = sqlx::query_as(
        "
        SELECT manifest.manifest_id,
               manifest.rollout_status,
               count(declaration.manifest_contract_instance_id)
        FROM manifest_versions manifest
        LEFT JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
        WHERE manifest.namespace = 'ens'
          AND manifest.source_family = 'ens_v1_reverse_l1'
          AND manifest.chain_id = 'ethereum-mainnet'
        GROUP BY manifest.manifest_id, manifest.rollout_status
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after_subset_sync.0, retained_manifest.0);
    assert_eq!(after_subset_sync.1, "deprecated");
    assert_eq!(after_subset_sync.2, retained_manifest.2);
    let manifest_event_states: Vec<(Option<String>, String)> = sqlx::query_as(
        "
        SELECT before_state ->> 'rollout_status',
               after_state ->> 'rollout_status'
        FROM normalized_events
        WHERE source_manifest_id = $1
          AND event_kind = 'SourceManifestUpdated'
        ORDER BY normalized_event_id
        ",
    )
    .bind(retained_manifest.0)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        manifest_event_states,
        [
            (None, "active".into()),
            (Some("active".into()), "deprecated".into()),
        ]
    );

    sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let manifest_event_states: Vec<(Option<String>, String)> = sqlx::query_as(
        "
        SELECT before_state ->> 'rollout_status',
               after_state ->> 'rollout_status'
        FROM normalized_events
        WHERE source_manifest_id = $1
          AND event_kind = 'SourceManifestUpdated'
        ORDER BY normalized_event_id
        ",
    )
    .bind(retained_manifest.0)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        manifest_event_states,
        [
            (None, "active".into()),
            (Some("active".into()), "deprecated".into()),
            (Some("deprecated".into()), "active".into()),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn schema_v2_manifest_sync_refuses_a_running_chain_phase() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_phase_lock").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&root)?;
    let mut lock_connection = scratch.pool().acquire().await?;
    let lock_name = "phase-runner:ethereum-mainnet:interpret";
    let acquired: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(lock_name)
            .fetch_one(&mut *lock_connection)
            .await?;
    assert!(acquired);

    let error = sync_schema_v2_repository(scratch.pool(), &repository)
        .await
        .expect_err("manifest sync must not race a running phase");
    assert!(error.to_string().contains("phase advisory lock"));
    let released: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(lock_name)
            .fetch_one(&mut *lock_connection)
            .await?;
    assert!(released);
    drop(lock_connection);

    let manifests: i64 = sqlx::query_scalar("SELECT count(*) FROM manifest_versions")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(manifests, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn schema_v2_manifest_authority_change_requires_derived_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_invalidation").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(scratch.pool(), &load_repository(&root)?).await?;

    let chain_id = "ethereum-mainnet";
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(chain_id)
        .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            input_content_hash = $2,
            started_at = now(),
            finished_at = now()
        WHERE chain_id = $1
          AND phase_name IN ('interpret', 'project')
        ",
    )
    .bind(chain_id)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;

    seed_chain_head(scratch.pool(), chain_id, 30_000_000).await?;
    let base_repository = load_repository(root.join("base"))?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    let hashes: Vec<String> = sqlx::query_scalar(
        "
        SELECT input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name IN ('interpret', 'project')
        ORDER BY phase_name
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(hashes.len(), 2);
    assert!(
        hashes
            .iter()
            .all(|hash| hash.starts_with("manifest-authority:"))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn returning_to_the_same_manifest_authority_mints_a_new_generation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_generation_aba").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let full_repository = load_repository(&root)?;
    let base_repository = load_repository(root.join("base"))?;
    sync_schema_v2_repository(scratch.pool(), &full_repository).await?;

    let chain_id = "ethereum-mainnet";
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(chain_id)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             input_content_hash = $2,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain_id)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    seed_chain_head(scratch.pool(), chain_id, 30_000_000).await?;

    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    let first_marker: String = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;

    sync_schema_v2_repository(scratch.pool(), &full_repository).await?;
    advance_chain_head(scratch.pool(), chain_id, 30_000_001).await?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    let second_marker: String = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;

    let (first_fingerprint, first_generation) = manifest_marker_parts(&first_marker)?;
    let (second_fingerprint, second_generation) = manifest_marker_parts(&second_marker)?;
    assert_eq!(
        second_fingerprint, first_fingerprint,
        "returning to the same desired manifests must preserve the authority fingerprint"
    );
    assert_ne!(
        second_generation, first_generation,
        "each distinct invalidation must mint a new generation token"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn basenames_execution_retirement_invalidates_the_base_project_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_basenames_dependency").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(scratch.pool(), &load_repository(&root)?).await?;

    let store = PhaseStore::new(scratch.pool().clone());
    for chain_id in ["ethereum-mainnet", "base-mainnet"] {
        store.initialize_chain(chain_id).await?;
        sqlx::query(
            "
            UPDATE chain_phase_state
            SET phase_status = 'completed',
                input_content_hash = $2,
                started_at = now(),
                finished_at = now()
            WHERE chain_id = $1
              AND phase_name IN ('interpret', 'project')
            ",
        )
        .bind(chain_id)
        .bind(INTERPRETER_CONTENT_HASH)
        .execute(scratch.pool())
        .await?;
    }

    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(root.join("base"))?).await?;

    let project_hashes: Vec<(String, String)> = sqlx::query_as(
        "
        SELECT chain_id, input_content_hash
        FROM chain_phase_state
        WHERE chain_id IN ('ethereum-mainnet', 'base-mainnet')
          AND phase_name = 'project'
        ORDER BY chain_id
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(project_hashes.len(), 2);
    assert!(
        project_hashes
            .iter()
            .all(|(_, hash)| hash.starts_with("manifest-authority:")),
        "both the Ethereum authority owner and its Base projection consumer must redo: {project_hashes:?}"
    );
    let base_interpret_hash: String = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = 'base-mainnet' AND phase_name = 'interpret'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(base_interpret_hash, INTERPRETER_CONTENT_HASH);
    scratch.cleanup().await
}

#[tokio::test]
async fn schema_v2_manifest_readmission_appends_an_address_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_sync_address_epoch").await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&root)?;
    sync_schema_v2_repository(scratch.pool(), &repository).await?;

    let address = "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb";
    let (first_row_id, instance_id, active_from): (i64, Uuid, Option<i64>) = sqlx::query_as(
        "
        SELECT contract_instance_address_id,
               contract_instance_id,
               active_from_block_number
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND lower(address) = $1
          AND deactivated_at IS NULL
        ",
    )
    .bind(address)
    .fetch_one(scratch.pool())
    .await?;

    let inactive_at = 30_000_000;
    seed_chain_head(scratch.pool(), "ethereum-mainnet", inactive_at).await?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(root.join("base"))?).await?;
    sync_schema_v2_repository(scratch.pool(), &repository).await?;
    let rows: Vec<AddressEpoch> = sqlx::query_as(
        "
        SELECT contract_instance_address_id,
               contract_instance_id,
               active_from_block_number,
               active_to_block_number,
               deactivated_at IS NULL
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND lower(address) = $1
        ORDER BY contract_instance_address_id
        ",
    )
    .bind(address)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        (
            first_row_id,
            instance_id,
            active_from,
            Some(inactive_at),
            false
        )
    );
    assert_eq!(rows[1].1, instance_id);
    assert_eq!(rows[1].2, Some(inactive_at + 1));
    assert_eq!(rows[1].3, None);
    assert!(rows[1].4);
    scratch.cleanup().await
}

async fn seed_completed_ingest_range(
    scratch: &ScratchDatabase,
    chain_id: &str,
) -> Result<ChainConfig> {
    seed_completed_ingest_range_through(scratch, chain_id, 1).await
}

async fn seed_completed_ingest_range_through(
    scratch: &ScratchDatabase,
    chain_id: &str,
    through: i64,
) -> Result<ChainConfig> {
    let source = SourceConfig::new(
        chain_id,
        "rpc",
        "rpc",
        SeedBasis::NewSignatureRange,
        0,
        "http://127.0.0.1:1",
    )?;
    let chain = ChainConfig::new(chain_id, vec![source.clone()], false)?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain_id).await?;
    store.ensure_ingest_sources(chain_id, &[source]).await?;
    seed_chain_head(scratch.pool(), chain_id, 0).await?;
    advance_chain_head(scratch.pool(), chain_id, through).await?;
    let block_hash = format!("{chain_id}-manifest-sync-head-{through}");
    sqlx::query(
        "UPDATE ingest_cursors
         SET next_block_number = $2 + 1, target_block_number = $2,
             last_processed_block_number = $2,
             last_processed_block_hash = $3
         WHERE chain_id = $1 AND source_key = 'rpc'",
    )
    .bind(chain_id)
    .bind(through)
    .bind(&block_hash)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2,
             current_block_hash = $3, target_block_number = $2,
             target_block_hash = $3, live_handoff_block_number = $2,
             live_handoff_block_hash = $3, started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .bind(through)
    .bind(block_hash)
    .execute(scratch.pool())
    .await?;
    Ok(chain)
}

async fn required_ingest_redo(pool: &sqlx::PgPool, chain_id: &str) -> Result<Option<(i64, i64)>> {
    Ok(sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'
           AND redo_in_progress
           AND last_error LIKE 'required downstream redo: %'",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?)
}

async fn seed_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-manifest-sync-head-{number}");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, to_timestamp($3), 'canonical')
        ",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
        VALUES ($1, $2, $3)
        ",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn advance_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-manifest-sync-head-{number}");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = $2, latest_block_number = $3
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

fn manifest_marker_parts(marker: &str) -> Result<(&str, &str)> {
    let encoded = marker
        .strip_prefix("manifest-authority:")
        .ok_or_else(|| anyhow::anyhow!("marker has no manifest-authority prefix: {marker}"))?;
    encoded
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("marker has no invalidation generation: {marker}"))
}
