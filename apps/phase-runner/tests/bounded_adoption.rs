//! A manifest sync's redo bounded to the change's first effect (docs/deployment.md §
//! Manifest-authority marker). The chain is a copy of the checked-in mainnet manifests over a
//! short window of mainnet heights, interpreted and projected by the real phases. Each adoptable
//! case discharges the marker with the printed full-range command, which runs from the first
//! effect, then compares the derived tables with an ordinary full Interpret and Project redo on
//! the same database: `normalized_events`, Interpret's identity and discovery rows, and every
//! owned key family table except the hydration work tables. Each refused case gets today's
//! full-range wording with the reason appended.
#[allow(dead_code)]
mod support;

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, B256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result, ensure};
use bigname_interpret::AdaptiveBatchConfig;
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use phase_runner::{
    CREATE_STAMP, EXTEND_STAMP, INTERPRETER_CONTENT_HASH, PROJECTION_CONTENT_HASH,
    REQUIRED_REDO_PREFIX,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    interpret_phase::{InterpretPhase, batch_request},
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseContext, PhaseFuture, PhaseName, PhaseSet, RunMode,
    },
    phase_lock::PhaseLock,
    project_phase::{FamilySettings, ProjectPhase},
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

use support::{CapturedLogs, ScratchDatabase};

const CHAIN: &str = "ethereum-mainnet";
/// The first block of the window: the ingest cursor's start, so the full range starts here.
const START: i64 = 9_380_400;
const REGISTRY: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
const REGISTRY_OLD: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
const RESOLVER: &str = "0x0000000000000000000000000000000000100001";
const SECOND_RESOLVER: &str = "0x0000000000000000000000000000000000100002";
const MANAGED_PROXY: &str = "0x0000000000000000000000000000000000100003";
const OWNER: &str = "0x0000000000000000000000000000000000200001";
const SENDER: &str = "0x0000000000000000000000000000000000200002";
const RESOLVER_FILE: &str = "ethereum/ens/ens_v1_resolver_l1/v1.toml";
const EXECUTION_FILE: &str = "ethereum/ens/ens_execution/v1.toml";
const BASE: &str = "base-mainnet";
const BASE_RESOLVER_FILE: &str = "base/basenames/basenames_base_resolver/v1.toml";
const BASENAMES_EXECUTION_FILE: &str = "ethereum/basenames/basenames_execution/v2.toml";
const REGISTRY_FILE: &str = "ethereum/ens/ens_v1_registry_l1/v3.toml";
const MOVED_FILE: &str = "ethereum/basenames/ens_v1_resolver_l1/v1.toml";
const UNDECLARED: &str = "0x0000000000000000000000000000000000100009";
const HYDRATION_WORK: [&str; 2] = [
    "project_text_hydration_work",
    "project_reverse_hydration_work",
];
const INTERPRET_TABLES: [&str; 13] = [
    "normalized_events",
    "contract_instances",
    "contract_instance_addresses",
    "discovery_edges",
    "token_lineages",
    "resources",
    "name_surfaces",
    "surface_bindings",
    "name_search_documents",
    "name_search_postings",
    "migration_event_associations",
    "migration_discovery_associations",
    "migration_candidate_identity_effects",
];

/// The compared tables the rows-below snapshot (`Fixture::dated_rows`) leaves out, each with why.
/// A table with no block column cannot date a write: a start too low rewrites its rows with the
/// same content, and the snapshot drops write times on purpose. Any other compared table without
/// a block column fails the snapshot, so a new one is listed here, not skipped. Project's family
/// rows are keyed upserts, so the family half of the snapshot catches only content changes: a
/// Project start too low shows in the recorded driver request and in the repair record's trusted
/// base (row 41).
const UNDATED: [(&str, &str); 16] = [
    ("contract_instances", "no block column"),
    ("name_search_documents", "no block column"),
    ("name_search_postings", "no block column"),
    (
        "project_resolver_classification",
        "a set change reclassifies a resolver without moving its row, which keeps the block of the \
         latest event that named it (crates/project/src/families/classification.rs)",
    ),
    ("project_lookup_name", "no block column"),
    ("project_lookup_relation", "no block column"),
    ("project_lookup_inventory", "no block column"),
    ("project_lookup_record", "no block column"),
    ("project_lookup_dependency", "no block column"),
    ("project_name_summary", "no block column"),
    ("project_address_history_anchor", "no block column"),
    ("project_history_source", "no block column"),
    ("project_history_source_edge", "no block column"),
    ("project_address_name_index", "no block column"),
    ("project_address_record_node_index", "no block column"),
    ("project_address_record_id_index", "no block column"),
];

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event AddrChanged(bytes32 indexed node, address a);
}

// Row 1. A new resolver declared at S below the head, its first own log above S and no earlier
// mention. The printed full range runs from S: the notice names it, the audit row records
// S..=H, and the Project redo it stamps starts at S.
#[tokio::test]
async fn a_declared_resolver_below_head_redoes_from_its_start() -> Result<()> {
    // While a single scoped dispatch exists, tracing-core takes a callsite's interest from the
    // default of whichever thread reaches it first, so a parallel test reaching the notice first
    // leaves it disabled here. A second, silent dispatch alive for the test makes every callsite
    // take its interest from both.
    let logs = CapturedLogs::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish(),
    );
    let _silent = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let fixture = Fixture::new("bounded_resolver_below_head", 59).await?;
    let start = START + 20;
    fixture
        .log(START + 25, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture
        .log(START + 30, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    let run = fixture
        .discharge(&token, None)
        .with_subscriber(dispatch)
        .await?;
    let notice = format!(
        "interpret redo on chain {CHAIN} requested {START}..={} but runs {start}..={}: the \
         manifest change's first effect is block {start}",
        fixture.head, fixture.head
    );
    ensure!(
        logs.text().contains(&notice),
        "{notice:?} not in {}",
        logs.text()
    );
    run.assert_ranges(start, start)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_equals_full_pair().await
}

// Row 2. A resolver declared above the head: no first effect, so each phase replays one block,
// the head, and both rows adopt the binary's hashes.
#[tokio::test]
async fn an_admission_above_the_head_replays_one_block() -> Result<()> {
    let fixture = Fixture::new("bounded_admission_above_head", 59).await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, fixture.head + 100)?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(fixture.head, fixture.head)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 3, the guard for reading no interpreted event payloads. The registry names the resolver
// at M, below its declared start S, and the resolver's own log is above S. The mention leaves
// no discovery edge, so the redo starts at S, and the tables still equal the full pair's.
#[tokio::test]
async fn a_mention_below_the_declared_start_still_equals_the_full_pair() -> Result<()> {
    let fixture = Fixture::new("bounded_mention_below_start", 59).await?;
    let (mention, start) = (START + 10, START + 30);
    fixture
        .log(mention, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture
        .log(START + 40, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture.discharge(&token, None).await?;
    let edge: Option<i64> = sqlx::query_scalar(
        "SELECT min(edge.active_from_block_number) FROM discovery_edges edge
         JOIN contract_instance_addresses instance
           ON instance.contract_instance_id = edge.to_contract_instance_id
         WHERE edge.chain_id = $1 AND lower(instance.address) = $2",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(fixture.pool())
    .await?;
    ensure!(
        edge.is_none(),
        "the mention left a discovery edge at {edge:?}"
    );
    ensure!(
        fixture.audits().await? == vec![(start, fixture.head)],
        "the redo starts at the declared start {start}, not the mention {mention}"
    );
    fixture.assert_equals_full_pair().await
}

// Row 4. A declaration added to a family whose manifest declares no events (`ens_execution`,
// active in this baseline), with a start below the head. The redo starts at the start.
#[tokio::test]
async fn a_declaration_only_family_is_bounded() -> Result<()> {
    let fixture = Fixture::with("bounded_declaration_only", 59, active_execution).await?;
    let start = START + 15;
    fixture.initial_pair().await?;
    fixture.append(
        EXECUTION_FILE,
        &format!(
            "\n[[contracts]]\nrole = \"universal_resolver_managed\"\naddress = \
             \"{MANAGED_PROXY}\"\nproxy_kind = \"none\"\nstart_block = {start}\n"
        ),
    )?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, start)?;
    fixture.assert_equals_full_pair().await
}

// Row 5. The families were last published under another interpreter content hash, as after a
// release that also edits an event block. The bounded redo does not apply: a short range is
// refused with the full range and the reason, and the full range runs from the cursor start.
#[tokio::test]
async fn an_interpreter_rotation_keeps_the_full_range() -> Result<()> {
    let fixture = Fixture::new("bounded_interpreter_rotation", 59).await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, START + 20)?;
    let token = fixture.sync().await?;
    sqlx::query(
        "UPDATE project_family_marker SET interpret_input_content_hash = 'keccak256:prior'
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(fixture.pool())
    .await?;
    fixture
        .assert_short_range_refused(
            &token,
            "the owned key families were last published under interpreter content hash \
             keccak256:prior",
        )
        .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.cleanup().await
}

// Rows 6, 7 (first part) and 8. A changed manifest of an ENSv2 family refuses the bounded redo:
// a registry declaration edit, a root's proxy kind, a migration manifest, a root replacement.
// Each is a SourceManifestUpdated event the published set does not hold, beside the marker sync
// writes.
#[tokio::test]
async fn an_ens_v2_manifest_change_is_refused_whole() -> Result<()> {
    let fixture = Fixture::new("bounded_ens_v2_refused", 59).await?;
    fixture.initial_pair().await?;
    for (family, contracts) in [
        (
            "ens_v2_registry_l1",
            json!([{"role": "registry", "address": SECOND_RESOLVER, "proxy_kind": "none",
                "start_block": START + 5}]),
        ),
        (
            "ens_v2_root_l1",
            json!([{"role": "root_registry", "address": SECOND_RESOLVER, "proxy_kind": "uups",
                "start_block": START + 5}]),
        ),
        (
            "ens_v2_migration_l1",
            json!([{"role": "migration_controller", "address": SECOND_RESOLVER,
                "proxy_kind": "none", "start_block": START + 5}]),
        ),
    ] {
        let planted = fixture.plant_manifest(family, contracts).await?;
        let token = fixture.plant_marker(family).await?;
        fixture
            .assert_short_range_refused(&token, &format!("of family {family} changed"))
            .await?;
        fixture.remove_planted(planted).await?;
    }
    fixture.cleanup().await
}

// Row 7, second part. A resolver declaration moved to another manifest: the resolver family's
// version is bumped and the old version deprecated, so every declaration changes manifest. The
// bounded redo applies, from the lowest start of both sides (here the cursor start), and the
// tables equal the full pair's.
#[tokio::test]
async fn a_moved_declaration_is_bounded() -> Result<()> {
    let fixture = Fixture::new("bounded_moved_declaration", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 5, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture.discharge(&token, None).await?;
    let path = fixture.profile.join(RESOLVER_FILE);
    let v1 = std::fs::read_to_string(&path)?;
    std::fs::write(
        path.with_file_name("v2.toml"),
        v1.replacen("manifest_version = 1", "manifest_version = 2", 1),
    )?;
    std::fs::write(
        &path,
        v1.replacen(
            "rollout_status = \"active\"",
            "rollout_status = \"deprecated\"",
            1,
        ),
    )?;
    let token = fixture.sync().await?;
    // Every declaration of v1 moves, and those with no start read as block zero, so the bounded
    // start is the first ingested block. The refusal names it as the first effect.
    fixture.assert_bounded_refusal(&token, START).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head), (START, fixture.head)]);
    fixture.assert_equals_full_pair().await
}

// Row 9. A payload edit that changes no declaration, watch or rule (a capability flag): no first
// effect, one block per phase.
#[tokio::test]
async fn a_change_with_no_first_effect_replays_one_block() -> Result<()> {
    let fixture = Fixture::new("bounded_no_first_effect", 59).await?;
    fixture.initial_pair().await?;
    fixture.edit(RESOLVER_FILE, |manifest| {
        manifest.replacen(
            "[capability_flags]\n",
            "[capability_flags]\nbounded_fixture = { status = \"supported\", notes = \"fixture\" }\n",
            1,
        )
    })?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(fixture.head, fixture.head)?;
    fixture.assert_equals_full_pair().await
}

// Row 10. A marker on both rows while the manifest set equals the published one, as manifest
// sync leaves after repairing an address floor with no event. The runner cannot see the old
// floor, so the redo is the full range. This also pins the runner's set key to the family
// marker's: any drift in the selection would read as a change here.
#[tokio::test]
async fn a_marker_with_no_set_change_keeps_the_full_range() -> Result<()> {
    let fixture = Fixture::new("bounded_no_set_change", 59).await?;
    fixture.initial_pair().await?;
    let token = fixture.plant_marker("repair").await?;
    fixture
        .assert_short_range_refused(
            &token,
            "the marker records no change of the published manifest set",
        )
        .await?;
    fixture.cleanup().await
}

// Row 11. Two syncs before any discharge: A at S1, then C at S2 above it. The diff is against
// the published set, so both are new and the redo starts at S1. Only the second token is valid.
#[tokio::test]
async fn two_syncs_bound_from_the_oldest_first_effect() -> Result<()> {
    let fixture = Fixture::new("bounded_two_syncs", 59).await?;
    let (first, second) = (START + 20, START + 40);
    fixture
        .log(first + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture
        .log(second + 1, 0, SECOND_RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, first)?;
    let stale = fixture.sync().await?;
    fixture.add_resolver("public_resolver_fixture_c", SECOND_RESOLVER, second)?;
    let token = fixture.sync().await?;
    let refused = fixture
        .discharge(&stale, None)
        .await
        .err()
        .context("the first token no longer matches")?;
    ensure!(refused.kind() == ErrorKind::Configuration, "{refused}");
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(first, first)?;
    fixture.assert_equals_full_pair().await
}

// Row 12. A resolver declaration and an ENSv2 registry edit in one sync: refused as a whole.
#[tokio::test]
async fn a_mixed_sync_refuses_as_a_whole() -> Result<()> {
    let fixture = Fixture::new("bounded_mixed_sync", 59).await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, START + 20)?;
    fixture.sync().await?;
    fixture.complete_required_ingest().await?;
    fixture
        .plant_manifest(
            "ens_v2_registry_l1",
            json!([{"role": "registry", "address": SECOND_RESOLVER, "proxy_kind": "none",
                "start_block": START + 5}]),
        )
        .await?;
    let token = fixture.plant_marker("mixed").await?;
    fixture
        .assert_short_range_refused(&token, "of family ens_v2_registry_l1 changed")
        .await?;
    fixture.cleanup().await
}

// Row 13. A sync while a redo holds its phase lock is refused, so nothing changes under a
// running redo.
#[tokio::test]
async fn a_sync_during_a_running_redo_is_refused() -> Result<()> {
    let fixture = Fixture::new("bounded_sync_under_lock", 59).await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, START + 20)?;
    let lock = PhaseLock::acquire(
        fixture.scratch.writer_connect_options(),
        CHAIN,
        PhaseName::Interpret,
    )
    .await?;
    let error = sync_schema_v2_repository(fixture.pool(), &load_repository(&fixture.profile)?)
        .await
        .expect_err("the held Interpret lock refuses the sync");
    ensure!(
        format!("{error:#}").contains("phase advisory lock is held"),
        "{error:#}"
    );
    drop(lock);
    fixture.cleanup().await
}

// Row 14. A bounded Interpret redo stops after its start. Then a second sync declares C below A
// and mints a new token. The old token is refused, and the new one recomputes the first effect
// over both changes and restarts the range.
#[tokio::test]
async fn a_sync_between_attempts_mints_a_new_token_and_recomputes() -> Result<()> {
    let fixture = Fixture::new("bounded_sync_between_attempts", 59).await?;
    let (first, second) = (START + 30, START + 10);
    fixture
        .log(first + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture
        .log(second + 1, 0, SECOND_RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, first)?;
    let stale = fixture.sync().await?;
    fixture
        .discharge(&stale, Some(PhaseName::Interpret))
        .await
        .err()
        .context("the first attempt stops")?;
    fixture.add_resolver("public_resolver_fixture_c", SECOND_RESOLVER, second)?;
    let token = fixture.sync().await?;
    ensure!(token != stale, "the second sync mints a new token");
    let refused = fixture
        .discharge(&stale, None)
        .await
        .err()
        .context("the old token is refused")?;
    ensure!(refused.kind() == ErrorKind::Configuration, "{refused}");
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(second, second)?;
    ensure!(fixture.audits().await? == vec![(first, fixture.head), (second, fixture.head)]);
    fixture.assert_equals_full_pair().await
}

// Row 15. A bounded Interpret redo stops after its start. Rerunning the same full-range command
// and token resumes the audited range rather than a recomputed one.
#[tokio::test]
async fn an_interrupted_bounded_interpret_redo_resumes_its_audited_range() -> Result<()> {
    let fixture = Fixture::new("bounded_interrupted_interpret", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, Some(PhaseName::Interpret))
        .await
        .err()
        .context("the first attempt stops")?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, start)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_equals_full_pair().await
}

// Row 16. The Interpret half completes and the Project half stops. The Project row keeps the
// bounded stamp, and rerunning the stamped Project redo as recorded completes the pair.
#[tokio::test]
async fn the_stamped_project_half_survives_a_crash() -> Result<()> {
    let fixture = Fixture::new("bounded_project_crash", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, Some(PhaseName::Project))
        .await
        .err()
        .context("the Project half stops")?;
    let stamp: (bool, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_requested_from_block_number FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(fixture.pool())
    .await?;
    ensure!(stamp == (true, Some(start)), "{stamp:?}");
    let run = fixture.run(None, None);
    run.runner
        .redo(
            &chain()?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(start, fixture.head)?,
            CancellationToken::new(),
        )
        .await?;
    ensure!(
        run.ranges(PhaseName::Project).first() == Some(&BlockRange::new(start, fixture.head)?),
        "{:?}",
        run.ranges(PhaseName::Project)
    );
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 17. A Project rebuild stopped part way leaves the family marker bootstrapping when the
// sync lands. The open Project redo lowers the Interpret start to its own, the cursor start, and
// the Project redo restarts the rebuild under the new input revision.
#[tokio::test]
async fn a_bounded_pair_on_a_rebuilding_chain_restarts_the_rebuild() -> Result<()> {
    let fixture = Fixture::new("bounded_rebuilding", 299).await?;
    let start = START + 200;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    // A second work block, so the rebuild below takes more than one batch.
    let bob = NewOwner {
        node: namehash(&[b"eth"]),
        label: keccak256(b"bob"),
        owner: OWNER.parse()?,
    }
    .encode_log_data();
    fixture.log(START + 100, 0, REGISTRY, &bob).await?;
    fixture.initial_pair().await?;
    fixture
        .stop_project_redo(BlockRange::new(START, fixture.head)?)
        .await?;
    ensure!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM project_family_marker WHERE chain_id = $1"
        )
        .bind(CHAIN)
        .fetch_one(fixture.pool())
        .await?
            == "bootstrap_pending",
        "the stopped redo left a rebuild"
    );
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture.discharge(&token, None).await?;
    ensure!(fixture.audits().await? == vec![(START, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 18. A Project redo stopped part way leaves a repair record open when the sync lands. The
// bounded pair's Project redo opens a new undo or restarts the replay, and the tables equal the
// full pair's.
#[tokio::test]
async fn a_bounded_pair_over_an_open_repair_record() -> Result<()> {
    let fixture = Fixture::new("bounded_open_repair", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture
        .stop_project_redo(BlockRange::new(START + 30, fixture.head)?)
        .await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, start)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 19. The families were written under another projection content hash, as in a release that
// rotates it beside the sync. The Interpret half is bounded and the Project stamp is the full,
// unclipped range.
#[tokio::test]
async fn a_projection_rotation_keeps_the_project_half_full() -> Result<()> {
    let fixture = Fixture::new("bounded_projection_rotation", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    sqlx::query(
        "UPDATE project_family_marker SET input_content_hash = 'keccak256:prior-projection'
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(fixture.pool())
    .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, START)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 20. Project stands below the first effect when the sync lands: its stamp is one block at
// its own head.
#[tokio::test]
async fn a_project_head_below_the_first_effect_gets_a_one_block_stamp() -> Result<()> {
    let fixture = Fixture::new("bounded_project_behind", 59).await?;
    let (start, project) = (START + 20, START + 10);
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    sqlx::query(
        "UPDATE chain_phase_state SET current_block_number = $2, current_block_hash = $3,
             target_block_number = $2, target_block_hash = $3
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .bind(project)
    .bind(block_hash(project))
    .execute(fixture.pool())
    .await?;
    let run = fixture.discharge(&token, None).await?;
    ensure!(
        run.ranges(PhaseName::Project).first() == Some(&BlockRange::new(project, project)?),
        "{:?}",
        run.ranges(PhaseName::Project)
    );
    fixture.cleanup().await
}

// Row 22. A family marker with no published manifest set (a fresh corpus, a cold restore): the
// bounded redo is never offered.
#[tokio::test]
async fn a_fresh_corpus_is_never_bounded() -> Result<()> {
    let fixture = Fixture::new("bounded_fresh_corpus", 59).await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, START + 20)?;
    let token = fixture.sync().await?;
    sqlx::query("UPDATE project_family_marker SET admission_manifests = NULL WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(fixture.pool())
        .await?;
    fixture
        .assert_short_range_refused(
            &token,
            "the family marker records no published manifest set",
        )
        .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.cleanup().await
}

// Every-block fields. A changed `universal_resolver_implementations` refuses the
// bounded redo: Interpret classifies every `Upgraded` log against the list at any block, and no
// remaining arm of the first-effect scan finds that log.
#[tokio::test]
async fn a_universal_resolver_implementation_change_keeps_the_full_range() -> Result<()> {
    let fixture = Fixture::with(
        "bounded_universal_resolver_implementations",
        59,
        active_execution,
    )
    .await?;
    fixture.initial_pair().await?;
    fixture.edit(EXECUTION_FILE, |manifest| {
        manifest.replacen(
            "discovery_rules = []\n",
            &format!(
                "discovery_rules = []\nuniversal_resolver_implementations = [\"{MANAGED_PROXY}\"]\n"
            ),
            1,
        )
    })?;
    let token = fixture.sync().await?;
    fixture
        .assert_short_range_refused(
            &token,
            "changed `universal_resolver_implementations`, which Interpret reads at every block",
        )
        .await?;
    fixture.cleanup().await
}

// Row 23. An Ethereum Mainnet `basenames_execution` change invalidates Base's Project, which reads
// that manifest at every block and has no Interpret redo to bound its own. The Base Project redo
// keeps the full range and says why.
#[tokio::test]
async fn a_basenames_execution_change_keeps_the_base_project_redo_full() -> Result<()> {
    let fixture = Fixture::new("bounded_base_project", 59).await?;
    fixture.seed_chain(BASE, "base_seam").await?;
    fixture
        .run(None, None)
        .runner
        .redo(
            &base_chain()?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(START, fixture.head)?,
            CancellationToken::new(),
        )
        .await?;
    fixture.edit(BASENAMES_EXECUTION_FILE, |manifest| {
        manifest.replacen("the frozen exact-surface", "the exact-surface", 1)
    })?;
    sync_schema_v2_repository(fixture.pool(), &load_repository(&fixture.profile)?).await?;
    let marker: Option<String> = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(BASE)
    .fetch_one(fixture.pool())
    .await?;
    ensure!(
        marker
            .as_deref()
            .is_some_and(|hash| hash.starts_with("manifest-authority:")),
        "the sync marks Base's Project: {marker:?}"
    );
    let error = fixture
        .run(None, None)
        .runner
        .redo(
            &base_chain()?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(START + 1, fixture.head)?,
            CancellationToken::new(),
        )
        .await
        .err()
        .context("a Base Project redo short of the full range is refused")?;
    let message = error.to_string();
    let expected = format!(
        "redo the full range {START}..={}, because manifest",
        fixture.head
    );
    ensure!(message.contains(&expected), "{expected:?} not in {message}");
    ensure!(
        message.contains(
            "is on chain ethereum-mainnet, whose change this chain's Project reads at every block"
        ),
        "{message}"
    );
    fixture.cleanup().await
}

// Row 24. A floor repair beside a payload change. The repair's Ingest redo from the first
// ingested block stamps Interpret from there, which R4 cannot see because the set changed too.
// The bounded start is lowered to that stamp, and the tables equal the full pair's.
#[tokio::test]
async fn a_pending_interpret_stamp_lowers_the_bounded_start() -> Result<()> {
    let fixture = Fixture::new("bounded_pending_stamp", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    // The stamp the runner writes on Interpret when the repair's Ingest redo starts
    // (`redo_stamp.rs`, `stamp_ingest_redo_dependents`), by the runner's own statement. The
    // harness's test source fails the intake check an Ingest redo makes.
    sqlx::query(CREATE_STAMP)
        .bind(CHAIN)
        .bind(PhaseName::Interpret.as_str())
        .bind(START)
        .bind(fixture.head)
        .bind(format!(
            "{REQUIRED_REDO_PREFIX}ingest redo may change the raw-fact extent"
        ))
        .execute(fixture.pool())
        .await?;
    let stamp: (bool, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_from_block_number FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(CHAIN)
    .fetch_one(fixture.pool())
    .await?;
    ensure!(
        stamp == (true, Some(START)),
        "the Interpret row takes the stamp from the first ingested block: {stamp:?}"
    );
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    ensure!(fixture.audits().await? == vec![(START, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 25. An edit to a declaration whose start lies below the first ingested block. The redo
// starts at the first ingested block, above the first effect, whatever end was requested, and
// replays through the recorded head. The notice it logs names both blocks
// (`redo_presence.rs`, `notice_names_both_blocks_when_the_redo_starts_elsewhere`).
#[tokio::test]
async fn a_first_effect_below_the_cursor_start_redoes_from_the_cursor_start() -> Result<()> {
    let fixture = Fixture::new("bounded_effect_below_cursor", 59).await?;
    fixture.initial_pair().await?;
    fixture.edit(RESOLVER_FILE, |manifest| {
        manifest.replacen(
            "address = \"0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA\"\nproxy_kind = \"none\"\n",
            "address = \"0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA\"\nproxy_kind = \"none\"\n\
             read_features = [\"ensip19_default_address\"]\n",
            1,
        )
    })?;
    let token = fixture.sync().await?;
    fixture
        .assert_bounded_refusal(&token, START)
        .await
        .context("the first effect lies below the cursor start")?;
    let run = fixture.run(Some(&token), None);
    run.runner
        .redo(
            &chain()?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(START, fixture.head - 10)?,
            CancellationToken::new(),
        )
        .await?;
    run.assert_ranges(START, START)?;
    ensure!(fixture.audits().await? == vec![(START, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 26. One sync changes a Base manifest and the Ethereum Mainnet `basenames_execution`
// manifest together. Base's Interpret does not read `basenames_execution`, so its redo bounds to
// the Base change, a capability flag with no first effect, and replays the head alone. Base's
// Project reads it at every block, so the Project redo it stamps keeps the full range.
#[tokio::test]
async fn a_combined_base_sync_bounds_only_the_base_interpret_redo() -> Result<()> {
    let fixture = Fixture::new("bounded_base_combined", 59).await?;
    fixture.seed_chain(BASE, "base_seam").await?;
    fixture
        .run(None, None)
        .runner
        .redo(
            &base_chain()?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(START, fixture.head)?,
            CancellationToken::new(),
        )
        .await?;
    fixture.edit(BASE_RESOLVER_FILE, |manifest| {
        manifest.replacen(
            "[capability_flags]\n",
            "[capability_flags]\nbounded_fixture = { status = \"supported\", notes = \"fixture\" }\n",
            1,
        )
    })?;
    fixture.edit(BASENAMES_EXECUTION_FILE, |manifest| {
        manifest.replacen("the frozen exact-surface", "the exact-surface", 1)
    })?;
    sync_schema_v2_repository(fixture.pool(), &load_repository(&fixture.profile)?).await?;
    let token = fixture.token_on(BASE).await?;
    let run = fixture.run_on(BASE, Some(&token), None);
    run.runner
        .redo(
            &base_chain()?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(START, fixture.head)?,
            CancellationToken::new(),
        )
        .await?;
    run.assert_ranges(fixture.head, START)?;
    fixture.cleanup().await
}

// Row 29. A bounded Interpret redo stops after its start, and a deploy then rotates the
// interpreter content hash. The rerun of the printed full range with the token runs that whole
// range, since the rows below the audited start were derived by the prior interpreter, and the
// Project redo it stamps is the full range too. The audit row keeps the range it attested.
#[tokio::test]
async fn a_resume_under_a_new_interpreter_hash_runs_the_full_requested_range() -> Result<()> {
    let fixture = Fixture::new("bounded_resume_rotated", 59).await?;
    let start = START + 20;
    let token = fixture.interrupted_bounded_redo(start).await?;
    fixture.rotate_interpreter_hash().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 30. Row 29's widened run stops too. The rerun finds the audit by its token again, because
// the active range covers the audited one, and resumes the widened range.
#[tokio::test]
async fn a_widened_resume_is_found_by_its_token_again() -> Result<()> {
    let fixture = Fixture::new("bounded_resume_rotated_twice", 59).await?;
    let start = START + 20;
    let token = fixture.interrupted_bounded_redo(start).await?;
    fixture.rotate_interpreter_hash().await?;
    fixture
        .discharge(&token, Some(PhaseName::Interpret))
        .await
        .err()
        .context("the widened attempt stops")?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 31. A bounded Interpret redo stops after its start, and a deploy then rotates only the
// projection content hash. Interpret resumes its audited range, and the Project redo it stamps
// is the full range.
#[tokio::test]
async fn a_projection_rotation_at_resume_keeps_interpret_bounded_and_project_full() -> Result<()> {
    let fixture = Fixture::new("bounded_resume_projection_rotated", 59).await?;
    let start = START + 20;
    let token = fixture.interrupted_bounded_redo(start).await?;
    sqlx::query(
        "UPDATE project_family_marker SET input_content_hash = 'keccak256:prior-projection'
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(fixture.pool())
    .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, START)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 32. After row 29's rotation a second sync lands before the rerun. The old token is
// refused, and the new one keeps the full range, since the families were published under the
// prior interpreter.
#[tokio::test]
async fn a_sync_after_a_rotated_interruption_keeps_the_full_range() -> Result<()> {
    let fixture = Fixture::new("bounded_resume_rotated_sync", 59).await?;
    let start = START + 20;
    let stale = fixture.interrupted_bounded_redo(start).await?;
    fixture.rotate_interpreter_hash().await?;
    fixture.add_resolver("public_resolver_fixture_c", SECOND_RESOLVER, START + 40)?;
    let token = fixture.sync().await?;
    ensure!(token != stale, "the second sync mints a new token");
    let refused = fixture
        .discharge(&stale, None)
        .await
        .err()
        .context("the old token is refused")?;
    ensure!(refused.kind() == ErrorKind::Configuration, "{refused}");
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 33. Retiring a family that matches events from every emitter. Every declaration starts
// above an undeclared emitter's `AddrChanged`, which the family's all-emitter watch matched. The
// removed watch is read from its old start, so the redo starts at that log and the event goes,
// as on the full pair.
#[tokio::test]
async fn a_retired_match_all_family_redoes_from_its_first_watched_log() -> Result<()> {
    let fixture = Fixture::with("bounded_retired_match_all", 59, |profile| {
        resolver_starts_at(profile, START + 30)
    })
    .await?;
    fixture
        .log(START + 5, 0, UNDECLARED, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.edit(RESOLVER_FILE, |manifest| {
        manifest.replacen(
            "rollout_status = \"active\"",
            "rollout_status = \"deprecated\"",
            1,
        )
    })?;
    let token = fixture.sync().await?;
    let run = fixture.discharge(&token, None).await?;
    fixture.assert_hashes().await?;
    // The tables first, so a redo that starts too late shows the rows it kept.
    let ranges = run.assert_ranges(START + 5, START + 5);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 34. A resolver discovery rule removed while its from-role address is also declared as a
// root with a lower start, so the address was admitted below the rule's role start. The redo
// starts no higher than the address's persisted floor (here the cursor start), and the tables
// equal the full pair's. The ENSv1 registry's `NewResolver` creates no discovery edge, so the
// floor, not a produced edge, sets the start here.
#[tokio::test]
async fn a_removed_discovery_rule_redoes_from_its_source_floor() -> Result<()> {
    let fixture = Fixture::with("bounded_removed_rule", 59, |profile| {
        registry_with_resolver_rule(profile, None, START + 30)
    })
    .await?;
    fixture
        .log(START + 10, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture.initial_pair().await?;
    fixture.edit(REGISTRY_FILE, remove_resolver_rule)?;
    let token = fixture.sync().await?;
    fixture
        .assert_bounded_refusal(&token, START)
        .await
        .context("the source floor is the first effect")?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 35. A sync restores the published set between the halves of a bounded pair: the
// Interpret half completed under the added declaration and Project has not published. The open
// Project redo is the earlier bound, so the next Interpret redo starts there, and the tables
// equal the full pair's.
#[tokio::test]
async fn a_restore_between_the_halves_redoes_from_the_earlier_discharge() -> Result<()> {
    let fixture = Fixture::new("bounded_restore_between_halves", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    let path = fixture.profile.join(RESOLVER_FILE);
    let published = std::fs::read_to_string(&path)?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, Some(PhaseName::Project))
        .await
        .err()
        .context("the Project half stops")?;
    std::fs::write(&path, &published)?;
    let token = fixture.sync().await?;
    let run = fixture.discharge(&token, None).await?;
    fixture.assert_hashes().await?;
    // The tables first, so a redo that starts too late shows the rows it kept.
    let ranges = run.assert_ranges(start, start);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 36. A resolver discovery rule removed after its from-role's declared start was raised
// over the address's persisted floor. The floor stays, so the redo starts at it, not at the
// raised role start.
#[tokio::test]
async fn a_removed_rule_after_a_raised_start_redoes_from_the_kept_floor() -> Result<()> {
    let (floor, raised) = (START + 5, START + 30);
    let fixture = Fixture::with("bounded_rule_kept_floor", 59, |profile| {
        registry_with_resolver_rule(profile, Some(floor), floor)
    })
    .await?;
    fixture
        .log(START + 10, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture.initial_pair().await?;
    fixture.edit(REGISTRY_FILE, |manifest| {
        manifest.replace(
            &format!("start_block = {floor}"),
            &format!("start_block = {raised}"),
        )
    })?;
    let raise = fixture.sync().await?;
    fixture.discharge(&raise, None).await?;
    fixture.edit(REGISTRY_FILE, remove_resolver_rule)?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(floor, floor)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 37. A declaration moved across namespaces retires its manifest and adds another, so no
// manifest changes namespace under one id. The move is bounded like any other, from the lowest
// start of both sides, and every compared row, namespace columns included, equals the full
// pair's.
#[tokio::test]
async fn a_cross_namespace_move_is_bounded() -> Result<()> {
    let fixture = Fixture::new("bounded_namespace_move", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 5, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    let path = fixture.profile.join(RESOLVER_FILE);
    let published = std::fs::read_to_string(&path)?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture.discharge(&token, None).await?;
    std::fs::write(&path, &published)?;
    let moved = fixture.profile.join(MOVED_FILE);
    std::fs::create_dir_all(
        moved
            .parent()
            .context("the moved manifest has a directory")?,
    )?;
    std::fs::write(
        moved,
        format!(
            "manifest_version = 1\nnamespace = \"basenames\"\nsource_family = \
             \"ens_v1_resolver_l1\"\nchain = \"{CHAIN}\"\ndeployment_epoch = \"ens_v1\"\n\
             rollout_status = \"active\"\nnormalizer_version = \"ensip15@ens-normalize-0.1.1\"\n\
             roots = []\ndiscovery_rules = []\n\n[capability_flags]\n\n[[contracts]]\nrole = \
             \"public_resolver_fixture\"\naddress = \"{RESOLVER}\"\nproxy_kind = \"none\"\n\
             start_block = {start}\n"
        ),
    )?;
    let token = fixture.sync().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, start)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 38. A required stamp merges into an interrupted bounded Interpret redo below its audited
// start before the rerun. The rerun resumes the merged range under the same token, the audit
// keeps its attested range, and Project is stamped from the start the Interpret redo ran.
#[tokio::test]
async fn a_stamp_merged_below_an_interrupted_bounded_redo_stamps_project_from_it() -> Result<()> {
    let fixture = Fixture::new("bounded_merged_resume", 59).await?;
    let start = START + 20;
    let token = fixture.interrupted_bounded_redo(start).await?;
    // The merge the runner writes when a required stamp lands on a redo in progress
    // (`redo_stamp.rs`, `extend_active`), by the runner's own statement.
    sqlx::query(EXTEND_STAMP)
        .bind(CHAIN)
        .bind(PhaseName::Interpret.as_str())
        .bind(START)
        .bind(fixture.head)
        .bind(START)
        .bind(fixture.head)
        .execute(fixture.pool())
        .await?;
    let run = fixture.discharge(&token, None).await?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    fixture.assert_hashes().await?;
    // The tables first, so a Project stamp that starts too late shows the rows it kept.
    let ranges = run.assert_ranges(START, START);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 39. Row 36 with an edge of the rule's kind from the from-role's instance below the kept
// floor, the row a rule produces. The redo starts at that edge, and the tables equal the full
// pair's.
#[tokio::test]
async fn a_removed_rule_redoes_from_an_edge_it_produced_below_the_floor() -> Result<()> {
    let (floor, raised, edge) = (START + 5, START + 30, START + 2);
    let fixture = Fixture::with("bounded_rule_edge", 59, |profile| {
        registry_with_resolver_rule(profile, Some(floor), floor)
    })
    .await?;
    fixture
        .log(START + 10, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture.initial_pair().await?;
    fixture.edit(REGISTRY_FILE, |manifest| {
        manifest.replace(
            &format!("start_block = {floor}"),
            &format!("start_block = {raised}"),
        )
    })?;
    let raise = fixture.sync().await?;
    fixture.discharge(&raise, None).await?;
    let inserted = sqlx::query(
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis,
             active_from_block_number, active_from_block_hash, canonicality_state)
         SELECT $1, 'resolver',
                (SELECT contract_instance_id FROM contract_instance_addresses
                 WHERE chain_id = $1 AND lower(address) = lower($2) AND deactivated_at IS NULL
                 LIMIT 1),
                (SELECT contract_instance_id FROM contract_instance_addresses
                 WHERE chain_id = $1 AND lower(address) = lower($3) AND deactivated_at IS NULL
                 LIMIT 1),
                'NewResolver', 'fixture', $4, $5, 'canonical'",
    )
    .bind(CHAIN)
    .bind(REGISTRY)
    .bind(REGISTRY_OLD)
    .bind(edge)
    .bind(block_hash(edge))
    .execute(fixture.pool())
    .await?;
    ensure!(
        inserted.rows_affected() == 1,
        "the registry edge is planted"
    );
    fixture.edit(REGISTRY_FILE, remove_resolver_rule)?;
    let token = fixture.sync().await?;
    let run = fixture.discharge(&token, None).await?;
    fixture.assert_hashes().await?;
    // The tables first, so a redo that starts at the floor shows the edge it kept.
    let ranges = run.assert_ranges(edge, edge);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 40. Interpret rows below the bounded start are left as they were. A registry event below
// the start of a newly declared resolver keeps its row, its id and its state through the bounded
// run, which rewrites only from the start, and the tables still equal the full pair's.
#[tokio::test]
async fn rows_below_the_bounded_start_are_untouched() -> Result<()> {
    let fixture = Fixture::new("bounded_rows_below_start", 59).await?;
    let start = START + 20;
    fixture
        .log(START + 5, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    let before = events_below(fixture.pool(), start).await?;
    ensure!(
        !before.is_empty(),
        "the registry event below the start has a row"
    );
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    let run = fixture.discharge(&token, None).await?;
    let after = events_below(fixture.pool(), start).await?;
    ensure!(
        after == before,
        "rows below the bounded start changed: {before:?} became {after:?}"
    );
    fixture.assert_hashes().await?;
    let ranges = run.assert_ranges(start, start);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 41. A bounded Interpret discharge stamps Project from the bound, and the binary stops before
// the Project redo begins. The operator then types the full range for Project instead of the
// stamp. The runner narrows the request to the bound and the family driver runs from it: the
// repair record is an undo that stops below the bound and at or above START, not a rebuild, the
// family rows below the bound are left as they were, and the tables equal the full pair's.
#[tokio::test]
async fn an_operator_project_range_wider_than_the_stamp_runs_from_the_bound() -> Result<()> {
    let fixture = Fixture::new("bounded_operator_project", 59).await?;
    let start = START + 20;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    fixture
        .redo(
            fixture.run_refusing(CHAIN, Some(&token), None, Some(PhaseName::Project)),
            PhaseName::Interpret,
            BlockRange::new(START, fixture.head)?,
        )
        .await
        .err()
        .context("the Project redo does not begin")?;
    let stamp: (bool, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_requested_from_block_number FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(fixture.pool())
    .await?;
    ensure!(stamp == (true, Some(start)), "the Project stamp: {stamp:?}");
    let run = fixture
        .redo(
            fixture.run(None, None),
            PhaseName::Project,
            BlockRange::new(START, fixture.head)?,
        )
        .await?;
    let repair: (String, Option<i64>) = sqlx::query_as(
        "SELECT state, trusted_base_number FROM project_repair_record WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(fixture.pool())
    .await?;
    // The driver's trusted base is the block below the requested start, or the lower block its
    // undo stops on at a journal generation boundary (`crates/project/src/families/driver.rs`).
    // From the bound it stays at or above START. A driver asked from START undoes below START,
    // or rebuilds and records no base.
    ensure!(
        repair.0 == "complete" && repair.1.is_some_and(|base| (START..start).contains(&base)),
        "the family repair is not an undo stopping in {START}..{start}: {repair:?}"
    );
    let projected = run.ranges(PhaseName::Project);
    ensure!(
        projected.first() == Some(&BlockRange::new(start, fixture.head)?),
        "the family driver was asked for {projected:?}, expected from {start}"
    );
    run.assert_rows_below(fixture.head + 1, start)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 42. The adaptive Interpret path, which sizes redo batches by raw input, opens the bounded
// redo at the bound too. With one block per batch the discharge runs many Interpret batches, the
// first engine request starts at the bound, the rows below it are left as they were, and the
// tables equal the full pair's. The start and the rows below are checked before the redo's own
// outcome, so an engine that starts low fails here even when the store also refuses its
// progress: the engine commits its opening reset before the runner records that progress, so
// the snapshot taken after the failed redo still shows the rows below the bound rewritten.
#[tokio::test]
async fn an_adaptive_bounded_interpret_redo_runs_from_the_bound() -> Result<()> {
    let fixture = Fixture::new("bounded_adaptive", 59).await?;
    let start = START + 20;
    fixture
        .log(START + 5, 0, REGISTRY, &new_resolver(RESOLVER)?)
        .await?;
    fixture
        .log(start + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, start)?;
    let token = fixture.sync().await?;
    let one_block = AdaptiveBatchConfig {
        max_blocks: std::num::NonZeroU32::MIN,
        ..AdaptiveBatchConfig::default()
    };
    let (run, outcome) = fixture
        .redo_outcome(
            fixture.run_with(CHAIN, Some(&token), None, None, Some(one_block)),
            PhaseName::Interpret,
            BlockRange::new(START, fixture.head)?,
        )
        .await?;
    run.assert_rows_below(start, start)?;
    let interpreted = run.ranges(PhaseName::Interpret);
    ensure!(
        interpreted.first().map(|range| range.from) == Some(start),
        "Interpret ran {interpreted:?}, expected from {start}"
    );
    outcome?;
    let batches = run
        .batches
        .lock()
        .expect("batches lock")
        .iter()
        .filter(|(phase, _)| *phase == PhaseName::Interpret)
        .count();
    let blocks = usize::try_from(fixture.head - start + 1)?;
    ensure!(
        batches >= blocks,
        "the adaptive redo ran {batches} Interpret batches for {blocks} blocks"
    );
    fixture.assert_hashes().await?;
    let ranges = run.assert_ranges(start, start);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// The first-effect statements read the indexes they are written for. Over a corpus of 20,000 logs
// from 200 emitters on 20 topics, 2,000 contract instances and 10,000 edges, the generic plan a
// prepared statement settles on probes each address's own logs, each topic's own logs, each
// instance's edges at both ends, and a rule source's floor and produced edges. None walks a
// whole table, or a shared topic for an address watch, inside the locked begin.
#[tokio::test]
async fn the_first_effect_statements_read_their_indexes() -> Result<()> {
    let fixture = Fixture::new("bounded_statement_plans", 1_999).await?;
    for corpus in [
        "INSERT INTO raw_logs (chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data)
         SELECT $1, $1 || '-block-' || height, height, $1 || '-transaction-' || height, 0,
                slot, '0x' || lpad(to_hex(4096 + (height * 10 + slot) % 200), 40, '0'),
                ARRAY['0x' || lpad(to_hex((height + slot) % 20), 64, '0')], ''::bytea
         FROM generate_series($2::bigint, $3::bigint) height
         CROSS JOIN generate_series(1, 10) slot",
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         SELECT md5($1 || n)::uuid, $1, 'contract' FROM generate_series(1, 2000) n",
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address)
         SELECT md5($1 || n)::uuid, $1, '0x' || lpad(to_hex(8192 + n), 40, '0')
         FROM generate_series(1, 2000) n",
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis,
             active_from_block_number, active_from_block_hash, canonicality_state)
         SELECT $1, 'resolver', md5($1 || (1 + n % 2000))::uuid,
                md5($1 || (1 + (7 * n + 1) % 2000))::uuid, 'NewResolver', 'corpus',
                $2 + n % 2000, $1 || '-block-' || ($2 + n % 2000), 'canonical'
         FROM generate_series(1, 10000) n",
    ] {
        sqlx::query(corpus)
            .bind(CHAIN)
            .bind(START)
            .bind(fixture.head)
            .execute(fixture.pool())
            .await?;
    }
    sqlx::raw_sql(
        "ANALYZE raw_logs, chain_lineage, contract_instances, contract_instance_addresses,
             discovery_edges",
    )
    .execute(fixture.pool())
    .await?;
    let head = fixture.head;
    let address = "ARRAY['0x0000000000000000000000000000000000001001']::text[]";
    let instance = "ARRAY['0x0000000000000000000000000000000000002001']::text[]";
    let topic =
        "ARRAY['0x0000000000000000000000000000000000000000000000000000000000000005']::text[]";
    let cases: [(&str, String, &[&str]); 6] = [
        (
            "emitted log",
            format!("'{CHAIN}', {address}, {head}"),
            &[
                "raw_logs_emitter_idx",
                "lower(emitting_address) = target.address",
            ],
        ),
        (
            "mentioned edge",
            format!("'{CHAIN}', {head}, {instance}"),
            &[
                "contract_instance_addresses_active_idx",
                "from_contract_instance_id = instance.contract_instance_id",
                "to_contract_instance_id = instance.contract_instance_id",
            ],
        ),
        (
            "rule source floor",
            format!("'{CHAIN}', {instance}, {head}"),
            &[
                "contract_instance_addresses_active_idx",
                "lower(address) = ANY",
            ],
        ),
        (
            "rule edge",
            format!("'{CHAIN}', {instance}, ARRAY['resolver']::text[], {head}"),
            &[
                "contract_instance_addresses_active_idx",
                "from_contract_instance_id = instance.contract_instance_id",
                "edge_kind = rule.edge_kind",
            ],
        ),
        (
            "watched address log",
            format!(
                "'{CHAIN}', {address}, {topic}, ARRAY[0]::bigint[], ARRAY[NULL]::bigint[], {head}"
            ),
            &[
                "raw_logs_emitter_idx",
                "lower(emitting_address) = watch.address",
            ],
        ),
        (
            "watched topic log",
            format!("'{CHAIN}', {topic}, ARRAY[0]::bigint[], ARRAY[NULL]::bigint[], {head}"),
            &["raw_logs_topic_idx", "lower(topics[1]) = watch.topic0"],
        ),
    ];
    for ((name, statement), (case, arguments, expected)) in phase_runner::first_effect_statements()
        .into_iter()
        .zip(cases)
    {
        ensure!(name == case, "statement {name} is not case {case}");
        let mut transaction = fixture.pool().begin().await?;
        sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
            .execute(&mut *transaction)
            .await?;
        sqlx::raw_sql(&format!("PREPARE first_effect AS {statement}"))
            .execute(&mut *transaction)
            .await?;
        let plan = sqlx::query_scalar::<_, String>(&format!(
            "EXPLAIN (COSTS OFF) EXECUTE first_effect({arguments})"
        ))
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");
        sqlx::raw_sql("DEALLOCATE first_effect")
            .execute(&mut *transaction)
            .await?;
        transaction.rollback().await?;
        println!("{name}:\n{plan}\n");
        // An index condition, not a filter, names each probe.
        for text in expected {
            ensure!(
                plan.lines()
                    .any(|line| line.contains(text) && !line.contains("Filter:")),
                "{name} does not probe {text}:\n{plan}"
            );
        }
        for table in ["raw_logs", "discovery_edges", "contract_instance_addresses"] {
            ensure!(
                !plan.contains(&format!("Seq Scan on {table}")),
                "{name} scans {table} whole:\n{plan}"
            );
        }
    }
    fixture.cleanup().await
}

struct Fixture {
    scratch: ScratchDatabase,
    profile: PathBuf,
    head: i64,
}

impl Fixture {
    /// The checked-in mainnet manifests synced, blocks `START..=START + span` canonical with one
    /// transaction each, a name registered at `START + 1`, and every phase completed through the
    /// head under this binary's hashes with nothing derived yet.
    async fn new(prefix: &str, span: i64) -> Result<Self> {
        Self::with(prefix, span, |_| Ok(())).await
    }

    /// The same, with `baseline` applied to the copied manifests before the first sync.
    async fn with(
        prefix: &str,
        span: i64,
        baseline: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<Self> {
        let scratch = ScratchDatabase::create(prefix).await?;
        let profile = std::env::temp_dir().join(format!("bigname-{prefix}-{}", std::process::id()));
        if profile.exists() {
            std::fs::remove_dir_all(&profile)?;
        }
        copy_tree(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
            &profile,
        )?;
        baseline(&profile)?;
        let head = START + span;
        let fixture = Self {
            scratch,
            profile,
            head,
        };
        sync_schema_v2_repository(fixture.pool(), &load_repository(&fixture.profile)?).await?;
        fixture.seed_chain(CHAIN, "ethereum_head").await?;
        let registered = NewOwner {
            node: namehash(&[b"eth"]),
            label: keccak256(b"alice"),
            owner: OWNER.parse()?,
        }
        .encode_log_data();
        fixture.log(START + 1, 0, REGISTRY, &registered).await?;
        Ok(fixture)
    }

    /// Blocks `START..=head` of `chain` canonical with one transaction each, an ingest cursor
    /// seeded on `basis`, and every phase completed through the head under this binary's hashes
    /// with nothing derived yet.
    async fn seed_chain(&self, chain: &str, basis: &str) -> Result<()> {
        let head = self.head;
        sqlx::query(
            "WITH lineage AS (
                 INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state)
                 SELECT $1, $1 || '-block-' || height, $1 || '-block-' || (height - 1), height,
                        to_timestamp(height), 'canonical'::canonicality_state
                 FROM generate_series($2::bigint, $3::bigint) height
                 RETURNING block_number, block_hash
             )
             INSERT INTO raw_transactions (chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, from_address, to_address)
             SELECT $1, block_hash, block_number, $1 || '-transaction-' || block_number, 0, $4, $5
             FROM lineage",
        )
        .bind(chain)
        .bind(START)
        .bind(head)
        .bind(SENDER)
        .bind(REGISTRY)
        .execute(self.pool())
        .await?;
        sqlx::query(
            "INSERT INTO ingest_cursors (chain_id, source_key, source_kind, seed_basis,
                 start_block_number, next_block_number, target_block_number,
                 last_processed_block_number, last_processed_block_hash)
             VALUES ($1, 'source', 'test', $5, $2, $3 + 1, $3, $3, $4)",
        )
        .bind(chain)
        .bind(START)
        .bind(head)
        .bind(chain_block_hash(chain, head))
        .bind(basis)
        .execute(self.pool())
        .await?;
        PhaseStore::new(self.pool().clone())
            .initialize_chain(chain)
            .await?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'completed', current_block_number = $2, current_block_hash = $3,
                 target_block_number = $2, target_block_hash = $3,
                 live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
                 live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
                 input_content_hash = CASE phase_name WHEN 'project' THEN $5
                     WHEN 'interpret' THEN $4 END,
                 started_at = now(), finished_at = now()
             WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
        )
        .bind(chain)
        .bind(head)
        .bind(chain_block_hash(chain, head))
        .bind(INTERPRETER_CONTENT_HASH)
        .bind(PROJECTION_CONTENT_HASH)
        .execute(self.pool())
        .await?;
        publish_heads(
            self.pool(),
            chain,
            &HeadMarkers {
                latest: BlockMarker::new(head, chain_block_hash(chain, head))?,
                safe: None,
                finalized: None,
            },
        )
        .await?;
        Ok(())
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.scratch.pool()
    }

    async fn cleanup(self) -> Result<()> {
        let _ = std::fs::remove_dir_all(&self.profile);
        self.scratch.cleanup().await
    }

    async fn log(
        &self,
        block: i64,
        log_index: i64,
        emitter: &str,
        log: &alloy_primitives::LogData,
    ) -> Result<()> {
        let topics: Vec<String> = log
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect();
        sqlx::query(
            "INSERT INTO raw_logs (chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, log_index, emitting_address, topics, data)
             VALUES ($1, $2, $3, $1 || '-transaction-' || $3, 0, $4, $5, $6, $7)",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind(block)
        .bind(log_index)
        .bind(emitter)
        .bind(topics)
        .bind(log.data.as_ref())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    fn edit(&self, file: &str, change: impl FnOnce(&str) -> String) -> Result<()> {
        let path = self.profile.join(file);
        let before = std::fs::read_to_string(&path)?;
        let after = change(&before);
        ensure!(after != before, "the edit of {file} changed nothing");
        std::fs::write(path, after)?;
        Ok(())
    }

    fn append(&self, file: &str, text: &str) -> Result<()> {
        self.edit(file, |manifest| format!("{manifest}{text}"))
    }

    fn add_resolver(&self, role: &str, address: &str, start: impl std::fmt::Display) -> Result<()> {
        self.append(
            RESOLVER_FILE,
            &format!(
                "\n[[contracts]]\nrole = \"{role}\"\naddress = \"{address}\"\nproxy_kind = \
                 \"none\"\nstart_block = {start}\n"
            ),
        )
    }

    /// Sync the edited manifests, complete any Ingest redo the sync requires, and return the
    /// marker's invalidation token.
    async fn sync(&self) -> Result<String> {
        sync_schema_v2_repository(self.pool(), &load_repository(&self.profile)?).await?;
        self.complete_required_ingest().await?;
        self.token().await
    }

    /// A bounded Interpret redo of a resolver declared at `start`, with its own log above it,
    /// stopped at its first batch. Returns its token.
    async fn interrupted_bounded_redo(&self, start: i64) -> Result<String> {
        self.log(start + 1, 0, RESOLVER, &addr_changed()?).await?;
        self.initial_pair().await?;
        self.add_resolver("public_resolver_fixture", RESOLVER, start)?;
        let token = self.sync().await?;
        self.discharge(&token, Some(PhaseName::Interpret))
            .await
            .err()
            .context("the first attempt stops")?;
        Ok(token)
    }

    /// A deploy with another interpreter content hash while a redo is interrupted: the Interpret
    /// row and the family marker record the prior binary's hash.
    async fn rotate_interpreter_hash(&self) -> Result<()> {
        sqlx::query(
            "UPDATE chain_phase_state SET input_content_hash = 'keccak256:prior'
             WHERE chain_id = $1 AND phase_name = 'interpret'",
        )
        .bind(CHAIN)
        .execute(self.pool())
        .await?;
        sqlx::query(
            "UPDATE project_family_marker SET interpret_input_content_hash = 'keccak256:prior'
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn token(&self) -> Result<String> {
        self.token_on(CHAIN).await
    }

    async fn token_on(&self, chain: &str) -> Result<String> {
        let hash: Option<String> = sqlx::query_scalar(
            "SELECT input_content_hash FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'interpret'",
        )
        .bind(chain)
        .fetch_one(self.pool())
        .await?;
        let hash = hash.context("the Interpret row records a hash")?;
        let (_, token) = hash
            .strip_prefix("manifest-authority:")
            .and_then(|marker| marker.rsplit_once(':'))
            .with_context(|| format!("the Interpret row records a marker, not {hash}"))?;
        Ok(token.to_owned())
    }

    async fn complete_required_ingest(&self) -> Result<()> {
        let required: Option<(i64, i64)> = sqlx::query_as(
            "SELECT redo_from_block_number, redo_to_block_number FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
        )
        .bind(CHAIN)
        .fetch_optional(self.pool())
        .await?;
        if let Some((from, to)) = required {
            self.run(None, None)
                .runner
                .redo(
                    &chain()?,
                    RedoPhase::Phase(PhaseName::Ingest),
                    BlockRange::new(from, to)?,
                    CancellationToken::new(),
                )
                .await?;
        }
        Ok(())
    }

    /// An ordinary full Interpret redo from the cursor start, with no marker, and the Project redo
    /// it stamps.
    async fn initial_pair(&self) -> Result<()> {
        self.run(None, None)
            .runner
            .redo(
                &chain()?,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(START, self.head)?,
                CancellationToken::new(),
            )
            .await?;
        self.assert_hashes().await
    }

    /// The printed full-range Interpret redo with `token`, and the Project redo it stamps. A
    /// `stop` phase fails its first batch, as a stopped binary leaves it.
    async fn discharge(&self, token: &str, stop: Option<PhaseName>) -> RunnerResult<Run> {
        self.redo(
            self.run(Some(token), stop),
            PhaseName::Interpret,
            BlockRange::new(START, self.head)?,
        )
        .await
    }

    /// `run` redoes `phase` over `range`, with every dated row snapshot before and after, so
    /// `Run::assert_ranges` can check the rows below each half's start were left as they were.
    async fn redo(&self, run: Run, phase: PhaseName, range: BlockRange) -> RunnerResult<Run> {
        let (run, outcome) = self.redo_outcome(run, phase, range).await?;
        outcome.map(|()| run)
    }

    /// `redo`, returning the run and the redo's own outcome apart, with the rows snapshot after
    /// a failed redo too, so a row can check what the redo wrote before it asks how it ended.
    async fn redo_outcome(
        &self,
        mut run: Run,
        phase: PhaseName,
        range: BlockRange,
    ) -> RunnerResult<(Run, RunnerResult<()>)> {
        let before = self.dated_rows().await?;
        let outcome = run
            .runner
            .redo(
                &chain()?,
                RedoPhase::Phase(phase),
                range,
                CancellationToken::new(),
            )
            .await
            .map(|_| ());
        run.rows = Some((before, self.dated_rows().await?));
        Ok((run, outcome))
    }

    /// Every row of a compared table that carries a block, as (family table, block, row text),
    /// the text without write times. Ids stay in, so a row rewritten in place shows.
    async fn dated_rows(&self) -> RunnerResult<Vec<(bool, i64, String)>> {
        let database = |error: sqlx::Error| {
            RunnerError::data_integrity(format!("failed to snapshot the dated rows: {error}"))
        };
        let tables = INTERPRET_TABLES
            .into_iter()
            .map(|table| (false, table))
            .chain(
                bigname_project::families::family_tables()
                    .filter(|table| !HYDRATION_WORK.contains(table))
                    .map(|table| (true, table)),
            );
        let mut rows = Vec::new();
        let undated = |table: &str| UNDATED.iter().any(|(name, _)| *name == table);
        for (family, table) in tables.filter(|(_, table)| !undated(table)) {
            let columns: Vec<(String, bool)> = sqlx::query_as(
                "SELECT column_name::text,
                        COALESCE(column_default LIKE '%now()%', false)
                            OR data_type LIKE 'timestamp%' OR column_name = 'deactivated_at'
                 FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = $1",
            )
            .bind(table)
            .fetch_all(self.pool())
            .await
            .map_err(database)?;
            let Some(block) = ["block_number", "active_from_block_number"]
                .into_iter()
                .find(|block| columns.iter().any(|(name, _)| name == block))
            else {
                return Err(RunnerError::data_integrity(format!(
                    "compared table {table} has no block column and is not in UNDATED"
                )));
            };
            let volatile: Vec<String> = columns
                .into_iter()
                .filter(|(_, volatile)| *volatile)
                .map(|(name, _)| name)
                .collect();
            let dated: Vec<(i64, String)> = sqlx::query_as(&format!(
                "SELECT {block}, (to_jsonb(t) - $1::text[])::text FROM {table} t
                 WHERE {block} IS NOT NULL"
            ))
            .bind(&volatile)
            .fetch_all(self.pool())
            .await
            .map_err(database)?;
            rows.extend(
                dated
                    .into_iter()
                    .map(|(number, row)| (family, number, format!("{table} {row}"))),
            );
        }
        rows.sort();
        Ok(rows)
    }

    fn run(&self, token: Option<&str>, stop: Option<PhaseName>) -> Run {
        self.run_on(CHAIN, token, stop)
    }

    /// The runner, attesting `token` for `chain` when given.
    fn run_on(&self, chain: &str, token: Option<&str>, stop: Option<PhaseName>) -> Run {
        self.run_refusing(chain, token, stop, None)
    }

    /// `run_on`, with the redo of `refuse` refused before it begins.
    fn run_refusing(
        &self,
        chain: &str,
        token: Option<&str>,
        stop: Option<PhaseName>,
        refuse: Option<PhaseName>,
    ) -> Run {
        self.run_with(chain, token, stop, refuse, None)
    }

    /// `run_refusing`, with Interpret's redo batches sized by `adaptive` when given, as
    /// `--interpret-adaptive` builds it.
    fn run_with(
        &self,
        chain: &str,
        token: Option<&str>,
        stop: Option<PhaseName>,
        refuse: Option<PhaseName>,
        adaptive: Option<AdaptiveBatchConfig>,
    ) -> Run {
        let interpret = InterpretPhase::new(self.pool().clone());
        let interpret = match adaptive {
            Some(config) => interpret.with_adaptive_batching(config),
            None => interpret,
        };
        let batches = Arc::new(Mutex::new(Vec::new()));
        let recorded = |inner: Arc<dyn Phase>| -> Arc<dyn Phase> {
            Arc::new(Recorded {
                stop: stop == Some(inner.name()),
                refuse: refuse == Some(inner.name()),
                inner,
                batches: Arc::clone(&batches),
                calls: AtomicUsize::new(0),
            })
        };
        let project = ProjectPhase::new(self.pool().clone()).with_family_settings(FamilySettings {
            retry_family_failures: false,
            require_hydration_url: false,
            ..FamilySettings::default()
        });
        let phases = PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            recorded(Arc::new(interpret)),
            recorded(Arc::new(project)),
        )
        .expect("the phase set is complete");
        let runner = PhaseRunner::new(
            self.scratch.runner(),
            phases,
            CapacityGuard::system(CapacityConfig::default()),
            "bounded-adoption",
            TimingConfig {
                initial_backoff: Duration::from_millis(1),
                maximum_backoff: Duration::from_millis(4),
                live_poll_interval: Duration::from_millis(1),
            },
        )
        .expect("the runner builds");
        let runner = match token {
            Some(token) => runner.with_watch_set_coverage_attestation(chain, token),
            None => runner,
        };
        Run {
            runner,
            batches,
            rows: None,
        }
    }

    /// Run a Project redo over `range` one work block per batch and stop it at its second batch,
    /// leaving its repair record and redo open.
    async fn stop_project_redo(&self, range: BlockRange) -> Result<()> {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let project = ProjectPhase::new(self.pool().clone()).with_family_settings(FamilySettings {
            max_blocks_per_run: 1,
            retry_family_failures: false,
            rebuild_ranges: bigname_project::families::RebuildRanges::Off,
            require_hydration_url: false,
            ..FamilySettings::default()
        });
        let phases = PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(StopAt {
                inner: Arc::new(project),
                at: 2,
                calls: AtomicUsize::new(0),
                batches,
            }),
        )?;
        let error = PhaseRunner::new(
            self.scratch.runner(),
            phases,
            CapacityGuard::system(CapacityConfig::default()),
            "bounded-adoption-stopped",
            TimingConfig {
                initial_backoff: Duration::from_millis(1),
                maximum_backoff: Duration::from_millis(4),
                live_poll_interval: Duration::from_millis(1),
            },
        )?
        .redo(
            &chain()?,
            RedoPhase::Phase(PhaseName::Project),
            range,
            CancellationToken::new(),
        )
        .await
        .expect_err("the Project redo stops at its second batch");
        ensure!(
            error.to_string().contains("stopped by the fixture"),
            "{error}"
        );
        Ok(())
    }

    /// Plant a SourceManifestUpdated event of an active manifest of `family`, as manifest sync
    /// writes one. Returns the manifest id.
    async fn plant_manifest(&self, family: &str, contracts: Value) -> Result<i64> {
        let payload = json!({
            "manifest_version": 1, "namespace": "ens", "source_family": family, "chain": CHAIN,
            "deployment_epoch": "fixture", "rollout_status": "active", "roots": [],
            "contracts": contracts, "discovery_rules": [], "capability_flags": {},
        });
        let manifest_id: i64 = sqlx::query_scalar(
            "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
                 deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
             VALUES (1, 'ens', $1, $2, 'fixture', 'active', 'fixture', $3, $4)
             RETURNING manifest_id",
        )
        .bind(family)
        .bind(CHAIN)
        .bind(format!("fixture/{family}.toml"))
        .bind(&payload)
        .fetch_one(self.pool())
        .await?;
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, source_manifest_id, chain_id, derivation_kind,
                 canonicality_state, after_state)
             VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, 'manifest_sync',
                 'finalized', jsonb_build_object('rollout_status', 'active',
                     'manifest_payload', $5::jsonb))",
        )
        .bind(format!("fixture:{family}:{manifest_id}"))
        .bind(family)
        .bind(manifest_id)
        .bind(CHAIN)
        .bind(&payload)
        .execute(self.pool())
        .await?;
        Ok(manifest_id)
    }

    async fn remove_planted(&self, manifest_id: i64) -> Result<()> {
        sqlx::query("DELETE FROM normalized_events WHERE source_manifest_id = $1")
            .bind(manifest_id)
            .execute(self.pool())
            .await?;
        sqlx::query("DELETE FROM manifest_versions WHERE manifest_id = $1")
            .bind(manifest_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// The marker manifest sync writes on both rows, with token `label`.
    async fn plant_marker(&self, label: &str) -> Result<String> {
        let token = format!("fixture-{label}");
        sqlx::query(
            "UPDATE chain_phase_state SET input_content_hash = $2
             WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
        )
        .bind(CHAIN)
        .bind(format!("manifest-authority:{}:{token}", "e".repeat(64)))
        .execute(self.pool())
        .await?;
        Ok(token)
    }

    /// A range above the cursor start is refused with today's full-range wording and `reason`.
    async fn assert_short_range_refused(&self, token: &str, reason: &str) -> Result<()> {
        let run = self.run(Some(token), None);
        let error = run
            .runner
            .redo(
                &chain()?,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(START + 1, self.head)?,
                CancellationToken::new(),
            )
            .await
            .expect_err("a range short of the full range is refused");
        ensure!(error.kind() == ErrorKind::ContentHashMismatch, "{error}");
        let message = error.to_string();
        let expected = format!(
            "cannot adopt a new interpretation-input hash for chain {CHAIN} phase interpret with \
             range {}..={}; redo the full range {START}..={}, because",
            START + 1,
            self.head,
            self.head
        );
        ensure!(message.contains(&expected), "{expected:?} not in {message}");
        ensure!(message.contains(reason), "{reason:?} not in {message}");
        Ok(())
    }

    /// A redo requested from `START + 1` is refused with the full range and the bounded start
    /// `from`, not with a reason for keeping the full range.
    async fn assert_bounded_refusal(&self, token: &str, from: i64) -> Result<()> {
        let run = self.run(Some(token), None);
        let error = run
            .runner
            .redo(
                &chain()?,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(START + 1, self.head)?,
                CancellationToken::new(),
            )
            .await
            .expect_err("a start above the first effect is refused");
        let message = error.to_string();
        let expected = format!(
            "redo the full range {START}..={}, which runs from block {from}, the manifest \
             change's first effect",
            self.head
        );
        ensure!(message.contains(&expected), "{expected:?} not in {message}");
        Ok(())
    }

    async fn audits(&self) -> Result<Vec<(i64, i64)>> {
        Ok(sqlx::query_as(
            "SELECT redo_from_block_number, redo_to_block_number
             FROM manifest_authority_attestations WHERE chain_id = $1 ORDER BY attested_at",
        )
        .bind(CHAIN)
        .fetch_all(self.pool())
        .await?)
    }

    async fn assert_hashes(&self) -> Result<()> {
        let rows: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
            "SELECT phase_name, phase_status, redo_in_progress, input_content_hash
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name IN ('interpret', 'project') ORDER BY phase_name",
        )
        .bind(CHAIN)
        .fetch_all(self.pool())
        .await?;
        let expected = vec![
            (
                "interpret".to_owned(),
                "completed".to_owned(),
                false,
                Some(INTERPRETER_CONTENT_HASH.to_owned()),
            ),
            (
                "project".to_owned(),
                "completed".to_owned(),
                false,
                Some(PROJECTION_CONTENT_HASH.to_owned()),
            ),
        ];
        ensure!(rows == expected, "{rows:?}");
        Ok(())
    }

    /// Every compared table, each as its rows' text without generated ids and write times.
    async fn tables(&self) -> Result<Vec<(String, String)>> {
        let tables = INTERPRET_TABLES.into_iter().chain(
            bigname_project::families::family_tables()
                .filter(|table| !HYDRATION_WORK.contains(table)),
        );
        let mut snapshot = Vec::new();
        for table in tables {
            // A row's `normalized_event_id` is the id the event got when it was last written: the
            // full pair writes every event again and the bounded pair only those from its start.
            // The row's `event_identity` names the same event either way and is compared.
            let volatile: Vec<String> = sqlx::query_scalar(
                "SELECT column_name::text FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = $1
                   AND (is_identity = 'YES' OR column_default LIKE '%now()%'
                        OR column_default LIKE 'nextval%'
                        OR column_name IN ('deactivated_at', 'publication_sequence',
                                           'normalized_event_id'))",
            )
            .bind(table)
            .fetch_all(self.pool())
            .await?;
            let rows: String = sqlx::query_scalar(&format!(
                "SELECT coalesce(jsonb_agg(row ORDER BY row::text), '[]')::text
                 FROM (SELECT to_jsonb(t) - $1::text[] AS row FROM {table} t) rows"
            ))
            .bind(&volatile)
            .fetch_one(self.pool())
            .await?;
            snapshot.push((table.to_owned(), rows));
        }
        Ok(snapshot)
    }

    /// The tables as they stand, against an ordinary full Interpret and Project redo of the same
    /// database.
    async fn assert_equals_full_pair(self) -> Result<()> {
        let bounded = self.tables().await?;
        self.initial_pair().await?;
        let full = self.tables().await?;
        let differing: Vec<String> = bounded
            .iter()
            .zip(&full)
            .filter(|((_, was), (_, now))| was != now)
            .map(|((table, was), (_, now))| difference(table, was, now))
            .collect();
        self.cleanup().await?;
        ensure!(
            differing.is_empty(),
            "the bounded pair differs from the full pair in {} tables:\n{}",
            differing.len(),
            differing.join("\n")
        );
        Ok(())
    }
}

/// The batches the recorded phases ran, with each batch's redo range.
type Batches = Arc<Mutex<Vec<(PhaseName, Option<BlockRange>)>>>;

struct Run {
    runner: PhaseRunner,
    batches: Batches,
    /// The dated rows before and after the redo (`Fixture::redo`).
    rows: Option<(DatedRows, DatedRows)>,
}

type DatedRows = Vec<(bool, i64, String)>;

impl Run {
    /// The recorded range of each redo batch of `phase`, in order (`Recorded`).
    fn ranges(&self, phase: PhaseName) -> Vec<BlockRange> {
        self.batches
            .lock()
            .expect("batches lock")
            .iter()
            .filter(|(name, _)| *name == phase)
            .filter_map(|(_, range)| *range)
            .collect()
    }

    /// The first Interpret engine request that opens the redo, and so its opening reset, starts at
    /// `interpret`, and the first Project batch was requested from `project`. When the run
    /// snapshot its rows, the rows below both starts are as they were (`assert_rows_below`).
    fn assert_ranges(&self, interpret: i64, project: i64) -> Result<()> {
        self.assert_rows_below(interpret, project)?;
        let interpreted = self.ranges(PhaseName::Interpret);
        let projected = self.ranges(PhaseName::Project);
        ensure!(
            interpreted.first().map(|range| range.from) == Some(interpret),
            "Interpret ran {interpreted:?}, expected from {interpret}"
        );
        ensure!(
            projected.first().map(|range| range.from) == Some(project),
            "Project was requested {projected:?}, expected from {project}"
        );
        Ok(())
    }

    /// The Interpret rows below `interpret` and the family rows below `project` are as they were
    /// before the run (`Fixture::redo`).
    fn assert_rows_below(&self, interpret: i64, project: i64) -> Result<()> {
        if let Some((before, after)) = &self.rows {
            let below = |rows: &DatedRows| -> Vec<String> {
                rows.iter()
                    .filter(|(family, block, _)| *block < if *family { project } else { interpret })
                    .map(|(_, _, row)| row.clone())
                    .collect()
            };
            let (was, now) = (below(before), below(after));
            let changed: Vec<&String> = was
                .iter()
                .filter(|row| !now.contains(row))
                .chain(now.iter().filter(|row| !was.contains(row)))
                .collect();
            ensure!(
                changed.is_empty(),
                "the redo touched {} rows below its start: {changed:?}",
                changed.len()
            );
        }
        Ok(())
    }
}

/// A phase that records each batch's range (the range Interpret's engine opens its redo over,
/// Project's requested range) and, when `stop` is set, fails its first batch without running it.
struct Recorded {
    inner: Arc<dyn Phase>,
    batches: Batches,
    stop: bool,
    /// Refuse a redo's preflight, so the redo never begins, as a binary stopped before it would.
    refuse: bool,
    calls: AtomicUsize,
}

impl Phase for Recorded {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn preflight(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
        mode: &RunMode,
    ) -> RunnerResult<()> {
        if self.refuse && mode.is_redo() {
            return Err(RunnerError::data_integrity("refused by the fixture"));
        }
        self.inner.preflight(chain_id, sources, mode)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        // Interpret: the range of the request its engine receives for a batch that opens the
        // redo, which is the range the engine's opening reset covers. Project: its requested
        // range, which its family driver runs from.
        let range = match self.inner.name() {
            PhaseName::Interpret if context.mode.is_redo() => batch_request(&context)
                .ok()
                .flatten()
                .filter(|request| request.resume_current.is_none())
                .and_then(|request| BlockRange::new(request.from_block, request.to_block).ok()),
            PhaseName::Interpret => None,
            _ => context.mode.range(),
        };
        self.batches
            .lock()
            .expect("batches lock")
            .push((self.inner.name(), range));
        if self.stop && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Box::pin(async { Err(RunnerError::data_integrity("stopped by the fixture")) });
        }
        self.inner.run_batch(context)
    }

    fn after_progress_recorded(
        &self,
        chain_id: &str,
    ) -> phase_runner::phase::AfterProgressFuture<'_> {
        self.inner.after_progress_recorded(chain_id)
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.inner.has_after_progress_work(chain_id)
    }

    fn after_redo(&self, chain_id: &str) -> RunnerResult<()> {
        self.inner.after_redo(chain_id)
    }
}

/// A phase that fails its `at`-th batch.
struct StopAt {
    inner: Arc<dyn Phase>,
    at: usize,
    calls: AtomicUsize,
    batches: Batches,
}

impl Phase for StopAt {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        self.batches
            .lock()
            .expect("batches lock")
            .push((self.inner.name(), context.mode.range()));
        if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.at {
            return Box::pin(async { Err(RunnerError::data_integrity("stopped by the fixture")) });
        }
        self.inner.run_batch(context)
    }

    fn after_progress_recorded(
        &self,
        chain_id: &str,
    ) -> phase_runner::phase::AfterProgressFuture<'_> {
        self.inner.after_progress_recorded(chain_id)
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.inner.has_after_progress_work(chain_id)
    }
}

fn difference(table: &str, was: &str, now: &str) -> String {
    let rows = |text: &str| -> Vec<Value> {
        serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default()
    };
    let (was, now) = (rows(was), rows(now));
    let only = |side: &[Value], other: &[Value]| -> Vec<Value> {
        side.iter()
            .filter(|row| !other.contains(row))
            .cloned()
            .collect()
    };
    let (bounded, full) = (only(&was, &now), only(&now, &was));
    // Name the fields that differ between the first unmatched rows, then the rows themselves.
    let fields: Vec<String> = match (bounded.first(), full.first()) {
        (Some(Value::Object(left)), Some(Value::Object(right))) => left
            .keys()
            .chain(right.keys().filter(|key| !left.contains_key(*key)))
            .filter(|key| left.get(*key) != right.get(*key))
            .map(|key| {
                let text = |value: Option<&Value>| -> String {
                    value.map_or("absent".to_owned(), |value| {
                        value.to_string().chars().take(160).collect()
                    })
                };
                format!("{key}: {} vs {}", text(left.get(key)), text(right.get(key)))
            })
            .collect(),
        _ => Vec::new(),
    };
    let rows = |side: &[Value]| -> Vec<String> {
        side.iter()
            .take(2)
            .map(|row| row.to_string().chars().take(400).collect())
            .collect()
    };
    format!(
        "{table}: {} only bounded, {} only full; fields {fields:?}; only bounded {:?}; only full {:?}",
        bounded.len(),
        full.len(),
        rows(&bounded),
        rows(&full)
    )
}

/// `ens_execution` active, as the baseline of the cases that edit a family with no events.
/// Every declaration of the resolver family starts at `start`.
/// Each normalized event below `block` on the chain: its id, block and state.
async fn events_below(pool: &sqlx::PgPool, block: i64) -> Result<Vec<(i64, i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT normalized_event_id, block_number, canonicality_state::text
         FROM normalized_events WHERE chain_id = $1 AND block_number < $2
         ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .bind(block)
    .fetch_all(pool)
    .await?)
}

fn resolver_starts_at(profile: &Path, start: i64) -> Result<()> {
    let path = profile.join(RESOLVER_FILE);
    let manifest = std::fs::read_to_string(&path)?;
    let edited: String = manifest
        .lines()
        .map(|line| {
            if line.starts_with("start_block = ") {
                format!("start_block = {start}\n")
            } else {
                format!("{line}\n")
            }
        })
        .collect();
    ensure!(edited != manifest, "the resolver family declares starts");
    std::fs::write(path, edited)?;
    Ok(())
}

/// The registry gains a resolver discovery rule from its `registry` role, declared at
/// `contract`. The registry root keeps its checked-in start, or starts at `root` when given.
fn registry_with_resolver_rule(profile: &Path, root: Option<i64>, contract: i64) -> Result<()> {
    let path = profile.join(REGISTRY_FILE);
    let manifest = std::fs::read_to_string(&path)?;
    let checked_in = "start_block = 9380380";
    ensure!(
        manifest.matches(checked_in).count() == 2,
        "the registry root and contract start at 9380380"
    );
    let root_start = root.map_or_else(
        || checked_in.to_owned(),
        |root| format!("start_block = {root}"),
    );
    let edited = manifest
        .replacen(checked_in, "ROOT_START", 1)
        .replacen(checked_in, &format!("start_block = {contract}"), 1)
        .replacen("ROOT_START", &root_start, 1)
        .replacen(
            "discovery_rules = []\n",
            "discovery_rules = [{ edge_kind = \"resolver\", from_role = \"registry\", \
             admission = \"reachable_from_root\" }]\n",
            1,
        );
    std::fs::write(path, edited)?;
    Ok(())
}

fn remove_resolver_rule(manifest: &str) -> String {
    manifest.replacen(
        "discovery_rules = [{ edge_kind = \"resolver\", from_role = \"registry\", admission = \
         \"reachable_from_root\" }]\n",
        "discovery_rules = []\n",
        1,
    )
}

fn active_execution(profile: &Path) -> Result<()> {
    let path = profile.join(EXECUTION_FILE);
    let manifest = std::fs::read_to_string(&path)?;
    let active = manifest.replacen(
        "rollout_status = \"shadow\"",
        "rollout_status = \"active\"",
        1,
    );
    ensure!(
        active != manifest,
        "ens_execution is shadow in the checked-in manifests"
    );
    std::fs::write(path, active)?;
    Ok(())
}

fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn chain() -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        CHAIN,
        vec![SourceConfig::new(
            CHAIN,
            "source",
            "test",
            SeedBasis::EthereumHead,
            START,
            "http://source.invalid",
        )?],
        false,
    )
}

fn base_chain() -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        BASE,
        vec![SourceConfig::new(
            BASE,
            "source",
            "test",
            SeedBasis::BaseSeam,
            START,
            "http://source.invalid",
        )?],
        false,
    )
}

fn block_hash(number: i64) -> String {
    chain_block_hash(CHAIN, number)
}

fn chain_block_hash(chain: &str, number: i64) -> String {
    format!("{chain}-block-{number}")
}

fn namehash(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label).as_slice());
        keccak256(input)
    })
}

fn alice() -> B256 {
    namehash(&[b"alice", b"eth"])
}

fn addr_changed() -> Result<alloy_primitives::LogData> {
    Ok(AddrChanged {
        node: alice(),
        a: OWNER.parse::<Address>()?,
    }
    .encode_log_data())
}

fn new_resolver(resolver: &str) -> Result<alloy_primitives::LogData> {
    Ok(NewResolver {
        node: alice(),
        resolver: resolver.parse()?,
    }
    .encode_log_data())
}
