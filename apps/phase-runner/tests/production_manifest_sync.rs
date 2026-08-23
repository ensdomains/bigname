#[allow(dead_code)]
mod support;

use std::{fs, time::Duration};

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_ingest::load_persisted_watch_filter;
use bigname_manifests::{load_repository, registry_announcement_topic0, sync_schema_v2_repository};
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
        let registry_created = (self.source_family == "ens_v2_registry_l1").then_some(
            r#"[[abi.events]]
name = "RegistryCreated"
fragment = "event RegistryCreated()"
emitter_roles = ["source_a"]
normalized_events = []
status = "supported"
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
{}
"#,
            self.source_family,
            self.chain_id,
            source_a_start,
            new_contract.unwrap_or_default(),
            registry_created.unwrap_or_default(),
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

    fn write_cross_namespace_resolver_sources(
        &self,
        alpha: Option<(&str, u64)>,
        zeta: Option<(&str, u64)>,
    ) -> Result<()> {
        self.write_namespace_resolver_source(
            "alpha",
            "0x0000000000000000000000000000000000000005",
            alpha,
        )?;
        self.write_namespace_resolver_source(
            "zeta",
            "0x0000000000000000000000000000000000000007",
            zeta,
        )
    }

    fn write_namespace_resolver_source(
        &self,
        namespace: &str,
        event_source: &str,
        source: Option<(&str, u64)>,
    ) -> Result<()> {
        let source = source.map_or_else(String::new, |(address, start)| {
            format!(
                r#"[[contracts]]
role = "source_a"
address = "{address}"
proxy_kind = "none"
start_block = {start}"#
            )
        });
        let manifest = format!(
            r#"manifest_version = 1
namespace = "{namespace}"
source_family = "{}"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []

[capability_flags]

[[contracts]]
role = "event_source"
address = "{event_source}"
proxy_kind = "none"
start_block = 0

{source}

[[discovery_rules]]
edge_kind = "resolver"
from_role = "source_a"
admission = "reachable_from_root"

[[abi.events]]
name = "Transfer"
fragment = "event Transfer(address indexed from, address indexed to, uint256 value)"
emitter_roles = ["event_source"]
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

    fn write_discovered_resolver_namespaces(
        &self,
        widening_namespace: &str,
        peer_namespace: &str,
        widening_has_address_changed: bool,
    ) -> Result<()> {
        for (namespace, registry_address, has_address_changed) in [
            (
                widening_namespace,
                "0x0000000000000000000000000000000000000004",
                widening_has_address_changed,
            ),
            (
                peer_namespace,
                "0x0000000000000000000000000000000000000006",
                true,
            ),
        ] {
            let registry = format!(
                r#"manifest_version = 2
namespace = "{namespace}"
source_family = "ens_v2_registry_l1"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []

[capability_flags]

[[contracts]]
role = "registry"
address = "{registry_address}"
proxy_kind = "none"
start_block = 0

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"

[[abi.events]]
name = "RegistryCreated"
fragment = "event RegistryCreated()"
emitter_roles = ["registry"]
normalized_events = ["RegistryCreated"]

[[abi.events]]
name = "ResolverUpdated"
fragment = "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)"
emitter_roles = ["registry"]
normalized_events = ["ResolverChanged"]
"#,
                self.chain_id,
            );
            let registry_dir = self.root.join(namespace).join("ens_v2_registry_l1");
            fs::create_dir_all(&registry_dir)?;
            fs::write(registry_dir.join("v2.toml"), registry)?;

            let address_changed = has_address_changed.then_some(
                r#"
[[abi.events]]
name = "AddressChanged"
fragment = "event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress)"
normalized_events = []
"#,
            );
            let resolver = format!(
                r#"manifest_version = 2
namespace = "{namespace}"
source_family = "ens_v2_resolver_l1"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
contracts = []
discovery_rules = []

[capability_flags]

[[abi.events]]
name = "TextChanged"
fragment = "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)"
normalized_events = []
{}
"#,
                self.chain_id,
                address_changed.unwrap_or_default(),
            );
            let resolver_dir = self.root.join(namespace).join("ens_v2_resolver_l1");
            fs::create_dir_all(&resolver_dir)?;
            fs::write(resolver_dir.join("v2.toml"), resolver)?;
        }
        Ok(())
    }

    fn set_deployment_epoch(
        &self,
        namespace: &str,
        family: &str,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let path = self.root.join(namespace).join(family).join("v2.toml");
        let manifest = fs::read_to_string(&path)?.replacen(
            &format!("deployment_epoch = \"{from}\""),
            &format!("deployment_epoch = \"{to}\""),
            1,
        );
        fs::write(path, manifest)?;
        Ok(())
    }

    fn replace_deployment_epoch(
        &self,
        namespace: &str,
        family: &str,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let directory = self.root.join(namespace).join(family);
        let old_path = directory.join("v2.toml");
        let manifest = fs::read_to_string(&old_path)?
            .replacen("manifest_version = 2", "manifest_version = 3", 1)
            .replacen(
                &format!("deployment_epoch = \"{from}\""),
                &format!("deployment_epoch = \"{to}\""),
                1,
            );
        fs::write(directory.join("v3.toml"), manifest)?;
        fs::remove_file(old_path)?;
        Ok(())
    }

    fn rotate_manifest_version(&self, namespace: &str, family: &str) -> Result<()> {
        let directory = self.root.join(namespace).join(family);
        let old_path = directory.join("v2.toml");
        let manifest = fs::read_to_string(&old_path)?.replacen(
            "manifest_version = 2",
            "manifest_version = 3",
            1,
        );
        fs::write(directory.join("v3.toml"), manifest)?;
        fs::remove_file(old_path)?;
        Ok(())
    }

    fn set_contract_start(&self, namespace: &str, family: &str, from: u64, to: u64) -> Result<()> {
        let path = self.root.join(namespace).join(family).join("v2.toml");
        let manifest = fs::read_to_string(&path)?.replacen(
            &format!("start_block = {from}"),
            &format!("start_block = {to}"),
            1,
        );
        fs::write(path, manifest)?;
        Ok(())
    }

    fn use_only_announced_test_registry_emitters(&self) -> Result<()> {
        let path = self.root.join("test/ens_v2_registry_l1/v2.toml");
        let manifest = fs::read_to_string(&path)?
            .replacen(r#"role = "registry""#, r#"role = "event_source""#, 1)
            .replace(
                r#"emitter_roles = ["registry"]"#,
                r#"emitter_roles = ["event_source"]"#,
            );
        fs::write(path, manifest)?;
        Ok(())
    }

    fn add_test_registry_announcement_rule(&self) -> Result<()> {
        let path = self.root.join("test/ens_v2_registry_l1/v2.toml");
        let manifest = fs::read_to_string(&path)?.replacen(
            "[[abi.events]]",
            r#"[[discovery_rules]]
edge_kind = "registry_announcement"
from_role = "registry"
admission = "reachable_from_root"

[[abi.events]]"#,
            1,
        );
        fs::write(path, manifest)?;
        Ok(())
    }

    fn set_test_registry_discovery_events(&self, present: bool) -> Result<()> {
        let path = self.root.join("test/ens_v2_registry_l1/v2.toml");
        let mut manifest = fs::read_to_string(&path)?;
        if present {
            manifest.push_str(
                r#"
[[abi.events]]
name = "RegistryCreated"
fragment = "event RegistryCreated()"
emitter_roles = ["event_source"]
normalized_events = ["RegistryCreated"]

[[abi.events]]
name = "ResolverUpdated"
fragment = "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)"
emitter_roles = ["event_source"]
normalized_events = ["ResolverChanged"]
"#,
            );
        } else {
            manifest = manifest
                .split_once("[[abi.events]]")
                .context("test registry has no discovery event section")?
                .0
                .to_owned();
        }
        fs::write(path, manifest)?;
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
async fn interrupted_recompute_resume_waits_for_required_ingest() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_recompute_ingest_fence").await?;
    let chain_id = "manifest-recompute-ingest-fence";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    let chain = seed_completed_ingest_range(&scratch, chain_id).await?;

    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true,
             current_block_number = 1, current_block_hash = $2,
             target_block_number = 1, target_block_hash = $2, input_content_hash = $3,
             redo_mode = CASE phase_name WHEN 'interpret' THEN 'recompute_flags' ELSE 'redo' END,
             redo_previous_phase_status = 'completed', redo_previous_started_at = now(),
             redo_previous_finished_at = now(), redo_from_block_number = 0, redo_to_block_number = 1,
             last_error = CASE phase_name
                 WHEN 'interpret' THEN 'injected interrupted recompute-flags'
                 ELSE 'recompute-flags project refresh complete; interpret flags pending' END,
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-manifest-sync-head-1"))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;

    fixture.write(true, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1))
    );
    let marker_before = interpret_input_hash(scratch.pool(), chain_id).await?;
    assert!(marker_before.starts_with("manifest-authority:"));

    let resume_error = loopback_runner(&scratch, "manifest-recompute-ingest-fence-resume")?
        .redo(
            &chain,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("recompute-flags must wait for the required Ingest redo");
    let command = "rerun `phase-runner redo --chain manifest-recompute-ingest-fence --phase \
                   ingest --from-block 0 --to-block 1`";
    assert!(resume_error.to_string().contains(command), "{resume_error}");

    let marker_after = interpret_input_hash(scratch.pool(), chain_id).await?;
    assert_eq!(marker_after, marker_before);
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1))
    );

    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', finished_at = now(), last_error = NULL,
             redo_in_progress = false, redo_mode = NULL, redo_previous_phase_status = NULL,
             redo_previous_last_error = NULL, redo_previous_started_at = NULL,
             redo_previous_finished_at = NULL, redo_from_block_number = NULL,
             redo_to_block_number = NULL, redo_current_block_number = NULL,
             redo_current_block_hash = NULL, redo_target_block_number = NULL, redo_target_block_hash = NULL
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await
    .context("failed to reset the interrupted Interpret marker")?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET redo_current_block_number = 0, redo_current_block_hash = 'project-checkpoint'
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await
    .context("failed to seed the Project-only recompute marker")?;
    loopback_runner(&scratch, "manifest-recompute-ingest-fence-project-resume")?
        .redo(
            &chain,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("Project-only recompute resume must wait for required Ingest");
    let project_checkpoint: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT redo_current_block_number, redo_current_block_hash
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        project_checkpoint,
        (Some(0), Some("project-checkpoint".into()))
    );

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
    fixture.write(false, false)?;
    fixture.add_discovery_rule("registry_announcement")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("unsupported historical announcement widening must reject");
    assert!(error.to_string().contains("registry announcement"));
    fixture.write(false, false)?;
    fixture.add_discovery_rule("subregistry")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    scratch.cleanup().await?;

    let fresh = ScratchDatabase::create("production_manifest_fresh_discovery_rule").await?;
    let fresh_chain = "manifest-fresh-discovery-rule";
    let fresh_fixture =
        WatchManifestFixture::with_source_family(fresh_chain, "ens_v2_registry_l1")?;
    fresh_fixture.write(false, false)?;
    fresh_fixture.add_discovery_rule("registry_announcement")?;
    sync_schema_v2_repository(fresh.pool(), &load_repository(&fresh_fixture.root)?).await?;
    seed_completed_ingest_range(&fresh, fresh_chain).await?;
    fresh_fixture.write(false, false)?;
    sync_schema_v2_repository(fresh.pool(), &load_repository(&fresh_fixture.root)?).await?;

    fresh.cleanup().await
}

#[tokio::test]
async fn historical_registry_announcement_rule_stamps_backfillable_ingest() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_announcement_rule_widening").await?;
    let chain_id = "manifest-announcement-rule-widening";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v2_registry_l1")?;
    fixture.write(false, false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.add_discovery_rule("registry_announcement")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((0, 1))
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn announcement_widening_starts_at_an_earlier_retained_announcement() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_pre_anchor_announcement").await?;
    let chain_id = "manifest-pre-anchor-announcement";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v2_registry_l1")?;
    fixture.write_with_start(false, false, 3)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range_through(&scratch, chain_id, 5).await?;
    seed_retained_registry_announcement(scratch.pool(), chain_id, 1).await?;

    fixture.add_discovery_rule("registry_announcement")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        Some((1, 5)),
        "the redo must include the earliest retained announcement admitted by the new rule"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn future_registry_announcement_rule_remains_admissible() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_future_announcement").await?;
    let chain_id = "manifest-future-announcement";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v2_registry_l1")?;
    fixture.write_with_start(false, false, 2)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;
    fixture.add_discovery_rule("registry_announcement")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_discovery_widening_uses_the_published_head_after_a_rewind() -> Result<()> {
    for (case, published_head, emitter_start, should_admit) in [
        ("future-of-published-head", Some(4), 7, true),
        ("at-published-head", Some(4), 4, false),
        ("missing-published-head", None, 10, false),
    ] {
        let scratch = ScratchDatabase::create(&format!(
            "production_manifest_discovery_published_head_{case}"
        ))
        .await?;
        let chain_id = format!("manifest-discovery-published-head-{case}");
        let fixture = WatchManifestFixture::new(&chain_id)?;
        fixture.write_namespace_resolver_source(
            "test",
            "0x0000000000000000000000000000000000000005",
            None,
        )?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        seed_completed_ingest_range_through(&scratch, &chain_id, 10).await?;
        if let Some(head) = published_head {
            advance_chain_head(scratch.pool(), &chain_id, head).await?;
        } else {
            sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
                .bind(&chain_id)
                .execute(scratch.pool())
                .await?;
        }

        fixture.write_namespace_resolver_source(
            "test",
            "0x0000000000000000000000000000000000000005",
            Some(("0x0000000000000000000000000000000000000004", emitter_start)),
        )?;
        let result =
            sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
        if should_admit {
            result.context("future resolver discovery work must be admitted after a rewind")?;
            assert_eq!(required_ingest_redo(scratch.pool(), &chain_id).await?, None);
        } else {
            let error = result.expect_err("covered resolver discovery work must still reject");
            assert!(
                error.to_string().contains("discovery rule widening"),
                "{error}"
            );
        }
        scratch.cleanup().await?;
    }
    Ok(())
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
async fn namespace_scoped_resolver_start_widening_is_not_masked_by_an_earlier_peer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_namespace_start_key").await?;
    let chain_id = "manifest-namespace-start-key";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v1_registrar_l1")?;
    fixture.write_cross_namespace_pair(100, 30, "resolver")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range_through(&scratch, chain_id, 100).await?;

    fixture.write_cross_namespace_pair(50, 30, "resolver")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("an earlier peer namespace must not mask resolver discovery widening");
    assert!(
        error.to_string().contains("discovery rule widening"),
        "the namespace-scoped transition must reject loudly: {error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn cross_namespace_different_address_swap_is_rule_widening_not_replacement() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_namespace_address_key").await?;
    let chain_id = "manifest-namespace-address-key";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v1_registrar_l1")?;
    fixture.write_cross_namespace_resolver_sources(
        Some(("0x0000000000000000000000000000000000000004", 0)),
        None,
    )?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write_cross_namespace_resolver_sources(
        None,
        Some(("0x0000000000000000000000000000000000000006", 0)),
    )?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("adding zeta's historical emitter must reject as rule widening");
    assert!(
        error.to_string().contains("discovery rule widening")
            && !error.to_string().contains("source replacement"),
        "alpha's removed emitter must not leak into zeta's diagnosis: {error}"
    );

    scratch.cleanup().await
}

// This covers only `insert_watch`'s minimum merge. `insert_discovery_rule`'s equivalent merge is
// defensive: repository loading permits only one active manifest per namespace/source family, and
// each manifest has already merged declarations with the same role/address to their earliest start,
// so a valid repository cannot insert one discovery-rule key with differing starts.
#[tokio::test]
async fn cross_namespace_watch_plan_entries_min_merge_the_earlier_start() -> Result<()> {
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
async fn discovered_family_event_widening_is_namespace_scoped_in_both_orderings() -> Result<()> {
    for (case, widening_namespace, peer_namespace, widening_address, peer_address) in [
        (
            "first",
            "alpha",
            "zeta",
            "0x0000000000000000000000000000000000000005",
            "0x0000000000000000000000000000000000000007",
        ),
        (
            "last",
            "zeta",
            "alpha",
            "0x0000000000000000000000000000000000000005",
            "0x0000000000000000000000000000000000000007",
        ),
    ] {
        let scratch = ScratchDatabase::create(&format!(
            "production_manifest_discovered_namespace_watch_{case}"
        ))
        .await?;
        let chain_id = format!("manifest-discovered-namespace-watch-{case}");
        let fixture = WatchManifestFixture::new(&chain_id)?;
        fixture.write_discovered_resolver_namespaces(widening_namespace, peer_namespace, false)?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        seed_discovered_resolver_address(
            scratch.pool(),
            &chain_id,
            widening_namespace,
            widening_address,
        )
        .await?;
        seed_discovered_resolver_address(scratch.pool(), &chain_id, peer_namespace, peer_address)
            .await?;

        let filter = load_persisted_watch_filter(scratch.pool(), &chain_id, 0, 1).await?;
        let address_changed = format!("{:#x}", keccak256(b"AddressChanged(bytes32,uint256,bytes)"));
        assert!(!filter.includes(widening_address, &address_changed, 0));
        assert!(filter.includes(peer_address, &address_changed, 0));

        seed_completed_ingest_range(&scratch, &chain_id).await?;
        fixture.write_discovered_resolver_namespaces(widening_namespace, peer_namespace, true)?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        assert_eq!(
            required_ingest_redo(scratch.pool(), &chain_id).await?,
            Some((0, 1)),
            "{case} namespace must refetch its already-discovered resolver history"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn resolver_epoch_flip_without_complete_discovery_is_rejected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_resolver_epoch_flip").await?;
    let chain_id = "manifest-resolver-epoch-flip";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    fixture.set_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "matched")?;
    fixture.set_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "resolver-old")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_deployment_epoch("test", "ens_v2_resolver_l1", "resolver-old", "matched")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err(
            "manifest snapshots cannot prove that Interpret materialized every resolver edge",
        );
    assert!(
        error
            .to_string()
            .contains("resolver discovery rule widening from a newly matching deployment epoch"),
        "{error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn future_only_resolver_epoch_match_is_admissible() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_future_epoch_match").await?;
    let chain_id = "manifest-future-epoch-match";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    fixture.set_contract_start("test", "ens_v2_registry_l1", 0, 2)?;
    fixture.set_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "matched")?;
    fixture.set_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "resolver-old")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_deployment_epoch("test", "ens_v2_resolver_l1", "resolver-old", "matched")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);

    scratch.cleanup().await
}

#[tokio::test]
async fn retained_announcement_precedes_a_future_direct_epoch_emitter() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_epoch_announcement_floor").await?;
    let chain_id = "manifest-epoch-announcement-floor";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    fixture.set_contract_start("test", "ens_v2_registry_l1", 0, 2)?;
    fixture.add_test_registry_announcement_rule()?;
    fixture.set_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "matched")?;
    fixture.set_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "resolver-old")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;
    seed_announced_registry_address(
        scratch.pool(),
        chain_id,
        "test",
        "0x0000000000000000000000000000000000000009",
    )
    .await?;

    fixture.replace_deployment_epoch("test", "ens_v2_resolver_l1", "resolver-old", "matched")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("the role-free announcement path starts before the direct declaration");
    assert!(
        error
            .to_string()
            .contains("resolver discovery rule widening from a newly matching deployment epoch"),
        "{error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_epoch_flip_away_from_registry_is_admissible_narrowing() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_resolver_epoch_narrowing").await?;
    let chain_id = "manifest-resolver-epoch-narrowing";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "unmatched")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(
        required_ingest_redo(scratch.pool(), chain_id).await?,
        None,
        "stopping the label join removes discovered intervals and needs no historical fetch"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn deployment_epoch_match_without_a_resolver_rule_is_admissible() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_epoch_without_rule").await?;
    let chain_id = "manifest-epoch-without-rule";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    let path = fixture.root.join("test/ens_v2_registry_l1/v2.toml");
    let manifest = fs::read_to_string(&path)?
        .replace("roots = []", "roots = []\ndiscovery_rules = []")
        .replace(
            "[[discovery_rules]]\nedge_kind = \"resolver\"\nfrom_role = \"registry\"\nadmission = \"reachable_from_root\"",
            "",
        );
    fs::write(path, manifest)?;
    fixture.set_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "registry-old")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_deployment_epoch("test", "ens_v2_registry_l1", "registry-old", "fixture")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn registry_epoch_flip_over_retained_history_is_rejected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_registry_epoch_flip").await?;
    let chain_id = "manifest-registry-epoch-flip";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    fixture.set_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "registry-old")?;
    fixture.set_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "matched")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    let resolver = "0x0000000000000000000000000000000000000005";
    seed_discovered_resolver_address(scratch.pool(), chain_id, "test", resolver).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_deployment_epoch("test", "ens_v2_registry_l1", "registry-old", "matched")?;
    let result = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);
            panic!(
                "registry epoch flip synced without rejection or required Ingest; its retained \
                 discovery edge still names the deprecated source manifest"
            );
        }
    };
    assert!(
        error
            .to_string()
            .contains("resolver discovery rule widening from a newly matching deployment epoch"),
        "{error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn matched_registry_and_resolver_epoch_rotation_is_rejected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_matched_epoch_rotation").await?;
    let chain_id = "manifest-matched-epoch-rotation";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.replace_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "next")?;
    fixture.replace_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "next")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("replacing the matching source manifest invalidates its retained edges");
    scratch.cleanup().await
}

#[tokio::test]
async fn same_epoch_registry_rotation_with_resolver_event_widening_is_rejected() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_manifest_registry_rotation_with_resolver_widening")
            .await?;
    let chain_id = "manifest-registry-rotation-with-resolver-widening";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    let resolver = "0x0000000000000000000000000000000000000005";
    seed_discovered_resolver_address(scratch.pool(), chain_id, "test", resolver).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;

    fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
    fixture.rotate_manifest_version("test", "ens_v2_registry_l1")?;
    let result = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            assert_eq!(
                required_ingest_redo(scratch.pool(), chain_id).await?,
                Some((0, 1))
            );
            let filter = load_persisted_watch_filter(scratch.pool(), chain_id, 0, 1).await?;
            let widened_topic =
                format!("{:#x}", keccak256(b"AddressChanged(bytes32,uint256,bytes)"));
            assert!(
                !filter.includes(resolver, &widened_topic, 0),
                "the redo filter omits edges anchored by the deprecated registry manifest"
            );
            panic!("same-epoch source-manifest replacement was not classified");
        }
    };
    assert!(
        error
            .to_string()
            .contains("resolver discovery source replacement"),
        "{error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn same_epoch_registry_rotation_without_retained_history_is_admissible() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_fresh_registry_rotation").await?;
    let chain_id = "manifest-fresh-registry-rotation";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;

    fixture.rotate_manifest_version("test", "ens_v2_registry_l1")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);

    scratch.cleanup().await
}

#[tokio::test]
async fn emitterless_resolver_rule_with_retained_announced_registry_epoch_match_is_rejected()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_announced_registry_epoch").await?;
    let chain_id = "manifest-announced-registry-epoch";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", false)?;
    fixture.add_test_registry_announcement_rule()?;
    fixture.use_only_announced_test_registry_emitters()?;
    fixture.set_deployment_epoch("test", "ens_v2_registry_l1", "fixture", "matched")?;
    fixture.set_deployment_epoch("test", "ens_v2_resolver_l1", "fixture", "resolver-old")?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;
    seed_announced_registry_address(
        scratch.pool(),
        chain_id,
        "test",
        "0x0000000000000000000000000000000000000009",
    )
    .await?;

    fixture.replace_deployment_epoch("test", "ens_v2_resolver_l1", "resolver-old", "matched")?;
    let error = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?)
        .await
        .expect_err("announced registries can emit resolver discovery events without a role");
    assert!(
        error
            .to_string()
            .contains("resolver discovery rule widening from a newly matching deployment epoch"),
        "{error}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn adding_announcement_path_to_emitterless_resolver_rule_is_rejected() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_manifest_add_announcement_resolver_path").await?;
    let chain_id = "manifest-add-announcement-resolver-path";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", false)?;
    fixture.use_only_announced_test_registry_emitters()?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;
    fixture.add_test_registry_announcement_rule()?;
    let result = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            assert_eq!(
                required_ingest_redo(scratch.pool(), chain_id).await?,
                Some((0, 1)),
                "only the announcement redo was stamped"
            );
            panic!("the new role-free resolver emitter path evaded widening classification");
        }
    };
    assert!(
        error.to_string().contains("discovery rule widening"),
        "{error}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn adding_discovery_producer_topics_without_version_rotation_is_rejected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_add_discovery_topics").await?;
    let chain_id = "manifest-add-discovery-topics";
    let fixture = WatchManifestFixture::new(chain_id)?;
    fixture.write_discovered_resolver_namespaces("test", "peer", false)?;
    fixture.add_test_registry_announcement_rule()?;
    fixture.use_only_announced_test_registry_emitters()?;
    fixture.set_test_registry_discovery_events(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;
    fixture.set_test_registry_discovery_events(true)?;
    let result = sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            assert_eq!(
                required_ingest_redo(scratch.pool(), chain_id).await?,
                Some((0, 1)),
                "only ordinary ABI watch widening was stamped"
            );
            let resolver = "0x0000000000000000000000000000000000000005";
            let text_changed = format!(
                "{:#x}",
                keccak256(b"TextChanged(bytes32,string,string,string)")
            );
            let filter = load_persisted_watch_filter(scratch.pool(), chain_id, 0, 1).await?;
            assert!(
                !filter.includes(resolver, &text_changed, 0),
                "the one-pass redo cannot watch a resolver edge that Interpret has not created"
            );
            panic!("discovery-producing ABI topics evaded discovery widening classification");
        }
    };
    assert!(
        error.to_string().contains("discovery rule widening"),
        "{error}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn epoch_flips_without_retained_ingest_history_are_admissible() -> Result<()> {
    for (case, initialize, flipped_family, old_epoch) in [
        (
            "fresh-resolver",
            false,
            "ens_v2_resolver_l1",
            "resolver-old",
        ),
        (
            "fresh-registry",
            false,
            "ens_v2_registry_l1",
            "registry-old",
        ),
        ("empty-resolver", true, "ens_v2_resolver_l1", "resolver-old"),
        ("empty-registry", true, "ens_v2_registry_l1", "registry-old"),
    ] {
        let scratch = ScratchDatabase::create(&format!("production_manifest_epoch_{case}")).await?;
        let chain_id = format!("manifest-epoch-{case}");
        let fixture = WatchManifestFixture::new(&chain_id)?;
        fixture.write_discovered_resolver_namespaces("test", "peer", true)?;
        fixture.set_deployment_epoch("test", flipped_family, "fixture", old_epoch)?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        if initialize {
            PhaseStore::new(scratch.pool().clone())
                .initialize_chain(&chain_id)
                .await?;
        }

        fixture.replace_deployment_epoch("test", flipped_family, old_epoch, "fixture")?;
        sync_schema_v2_repository(scratch.pool(), &load_repository(&fixture.root)?).await?;
        assert_eq!(required_ingest_redo(scratch.pool(), &chain_id).await?, None);
        scratch.cleanup().await?;
    }
    Ok(())
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
async fn legacy_family_watch_without_namespace_syncs_without_spurious_ingest() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_legacy_family_namespace").await?;
    let chain_id = "manifest-legacy-family-namespace";
    let fixture = WatchManifestFixture::with_source_family(chain_id, "ens_v2_resolver_l1")?;
    fixture.write(false, false)?;
    let repository = load_repository(&fixture.root)?;
    sync_schema_v2_repository(scratch.pool(), &repository).await?;
    seed_completed_ingest_range(&scratch, chain_id).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = 1,
             current_block_hash = $2, target_block_number = 1,
             target_block_hash = $2, input_content_hash = $3,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-manifest-sync-head-1"))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;

    let changed = sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload,
             '{_bigname_compiled_watch}',
             (
                 SELECT jsonb_agg(
                     CASE WHEN entry -> 'emitter' ->> 'kind' = 'family'
                          THEN jsonb_set(
                              entry, '{emitter}', (entry -> 'emitter') - 'namespace'
                          )
                          ELSE entry
                     END ORDER BY position
                 )
                 FROM jsonb_array_elements(
                     manifest_payload -> '_bigname_compiled_watch'
                 ) WITH ORDINALITY AS compiled(entry, position)
             )
         )
         WHERE chain_id = $1 AND rollout_status = 'active'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    assert_eq!(changed.rows_affected(), 1);

    sync_schema_v2_repository(scratch.pool(), &repository).await?;
    assert_eq!(required_ingest_redo(scratch.pool(), chain_id).await?, None);
    let derived_hashes: Vec<String> = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert!(
        derived_hashes
            .iter()
            .all(|hash| hash.starts_with("manifest-authority:"))
    );
    let missing_namespace: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM manifest_versions manifest
         CROSS JOIN LATERAL jsonb_array_elements(
             manifest.manifest_payload -> '_bigname_compiled_watch'
         ) AS compiled(entry)
         WHERE manifest.chain_id = $1
           AND entry -> 'emitter' ->> 'kind' = 'family'
           AND NOT (entry -> 'emitter' ? 'namespace')",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(missing_namespace, 0, "sync must rewrite the enriched shape");

    scratch.cleanup().await
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

async fn interpret_input_hash(pool: &sqlx::PgPool, chain_id: &str) -> Result<String> {
    let query = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    );
    Ok(query.bind(chain_id).fetch_one(pool).await?)
}

async fn seed_discovered_resolver_address(
    pool: &sqlx::PgPool,
    chain_id: &str,
    namespace: &str,
    address: &str,
) -> Result<()> {
    let (source_manifest_id, source_instance_id): (i64, Uuid) = sqlx::query_as(
        "SELECT manifest.manifest_id, declaration.contract_instance_id
         FROM manifest_versions manifest
         JOIN manifest_contract_instances declaration
           ON declaration.manifest_id = manifest.manifest_id
         WHERE manifest.chain_id = $1 AND manifest.namespace = $2
           AND manifest.source_family = 'ens_v2_registry_l1'",
    )
    .bind(chain_id)
    .bind(namespace)
    .fetch_one(pool)
    .await?;
    let resolver_manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND namespace = $2
           AND source_family = 'ens_v2_resolver_l1'",
    )
    .bind(chain_id)
    .bind(namespace)
    .fetch_one(pool)
    .await?;
    let resolver_instance_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind, provenance
         ) VALUES ($1, $2, 'contract', '{}'::jsonb)",
    )
    .bind(resolver_instance_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(resolver_instance_id)
    .bind(chain_id)
    .bind(address)
    .bind(resolver_manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state,
             provenance
         ) VALUES ($1, 'resolver', $2, $3, 'fixture', 'reachable_from_root',
                   $4, 0, $5, 'finalized', '{}'::jsonb)",
    )
    .bind(chain_id)
    .bind(source_instance_id)
    .bind(resolver_instance_id)
    .bind(source_manifest_id)
    .bind(format!("{chain_id}-{namespace}-discovery-block-0"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_announced_registry_address(
    pool: &sqlx::PgPool,
    chain_id: &str,
    namespace: &str,
    address: &str,
) -> Result<()> {
    let source_manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND namespace = $2
           AND source_family = 'ens_v2_registry_l1' AND rollout_status = 'active'",
    )
    .bind(chain_id)
    .bind(namespace)
    .fetch_one(pool)
    .await?;
    let instance_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind, provenance
         ) VALUES ($1, $2, 'contract', '{}'::jsonb)",
    )
    .bind(instance_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(instance_id)
    .bind(chain_id)
    .bind(address)
    .bind(source_manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state,
             provenance
         ) VALUES ($1, 'registry_announcement', $2, $2, 'RegistryCreated',
                   'reachable_from_root', $3, 0, $4, 'finalized', '{}'::jsonb)",
    )
    .bind(chain_id)
    .bind(instance_id)
    .bind(source_manifest_id)
    .bind(format!("{chain_id}-manifest-sync-head-0"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_retained_registry_announcement(
    pool: &sqlx::PgPool,
    chain_id: &str,
    block_number: i64,
) -> Result<()> {
    let block_hash = format!("{chain_id}-manifest-sync-head-{block_number}");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
    )
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    let transaction_hash = format!("{chain_id}-announcement-{block_number}");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .bind(&transaction_hash)
    .bind("0x0000000000000000000000000000000000000008")
    .bind("0x0000000000000000000000000000000000000009")
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics
         ) VALUES ($1, $2, $3, $4, 0, 0, $5, $6)",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .bind(transaction_hash)
    .bind("0x0000000000000000000000000000000000000009")
    .bind(vec![registry_announcement_topic0()])
    .execute(pool)
    .await?;
    Ok(())
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
