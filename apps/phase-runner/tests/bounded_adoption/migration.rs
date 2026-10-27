//! The `ens_v2_migration_l1` rows. An address added under a role, or a singleton role moved to
//! another address, is redone from the lowest start on either side of the change. Every other
//! change of the family keeps the full range and says why. The fixture's migration manifest is the
//! checked-in mainnet one, with the factory declared from the window's first block, so a factory
//! `ProxyDeployed` in the window is interpreted. A factory origin naming a declared registry
//! implementation is retained as a `ContractDiscovered` row, from that implementation's start
//! only (`crates/adapters/src/schema_v2/migration/registry_origin.rs`), which is how these rows
//! observe what an added or moved implementation changes.
use alloy_primitives::U256;

use super::*;

const MIGRATION_FILE: &str = "ethereum/ens/ens_v2_migration_l1/v1.toml";
const FACTORY: &str = "0x77510ba162dfea3d8989e3f0928a691b345730d2";
const USER_IMPLEMENTATION: &str = "0x75ff9adbcfce2469b097ac68f6ae650eae1351bb";
const WRAPPER_IMPLEMENTATION: &str = "0xcf3edb3f5501de63b57254d910b6782ee370039b";
const LOCKED_CONTROLLER: &str = "0xc46bf3b6c7ebcffd4ecfc3b012a1726a2463694d";
const UNLOCKED_CONTROLLER: &str = "0xab821ca676cf2e0790fa273526a165cfa86aeb64";
/// The registry family's declared `registry`, the checked-in ETHRegistry.
const ETH_REGISTRY: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
/// A redeployed contract the checked-in manifest does not declare.
const REDEPLOYED: &str = "0x0000000000000000000000000000000000300001";
const PROXY: &str = "0x0000000000000000000000000000000000300002";
/// An implementation no manifest declares, so a factory log naming it leaves no origin.
const UNDECLARED: &str = "0x0000000000000000000000000000000000300003";

sol! {
    event ProxyDeployed(
        address indexed sender, address indexed proxyAddress, uint256 salt, address implementation
    );
    event Initialized(uint64 version);
    event LabelRegistered(
        uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner,
        uint64 expiry, address indexed sender
    );
}

/// The `[[contracts]]` entry of `role` and the text before and after it, as checked in.
fn split<'a>(manifest: &'a str, role: &str) -> Option<(&'a str, &'a str, &'a str)> {
    let at = manifest.find(&format!("[[contracts]]\nrole = \"{role}\"\n"))?;
    let rest = &manifest[at..];
    let start = rest.find("start_block = ")?;
    let end = start + rest[start..].find('\n')? + 1;
    Some((&manifest[..at], &rest[..end], &rest[end..]))
}

fn declaration(role: &str, address: &str, start: i64) -> String {
    format!(
        "[[contracts]]\nrole = \"{role}\"\naddress = \"{address}\"\nproxy_kind = \"none\"\n\
         start_block = {start}\n"
    )
}

/// The migration manifest with `role` declared at `address` from `start`, in place of its
/// checked-in entry or added after the rest, or with its entry removed when `declared` is `None`.
fn declare(manifest: &str, role: &str, declared: Option<(&str, i64)>) -> String {
    let declared = declared.map(|(address, start)| declaration(role, address, start));
    match (split(manifest, role), declared) {
        (Some((before, _, after)), declared) => {
            format!("{before}{}{after}", declared.unwrap_or_default())
        }
        (None, Some(declared)) => format!("{manifest}\n{declared}"),
        (None, None) => manifest.to_owned(),
    }
}

/// The checked-in migration manifest with the factory declared from `START`, then `changes`.
fn migration_baseline(profile: &Path, changes: &[(&str, Option<(&str, i64)>)]) -> Result<()> {
    let path = profile.join(MIGRATION_FILE);
    let mut manifest = declare(
        &std::fs::read_to_string(&path)?,
        "verifiable_factory",
        Some((FACTORY, START)),
    );
    for (role, declared) in changes {
        manifest = declare(&manifest, role, *declared);
    }
    std::fs::write(path, manifest)?;
    Ok(())
}

fn proxy_deployed(implementation: &str) -> Result<alloy_primitives::LogData> {
    proxy_deployed_by(SENDER, PROXY, implementation)
}

fn proxy_deployed_by(
    sender: &str,
    proxy: &str,
    implementation: &str,
) -> Result<alloy_primitives::LogData> {
    Ok(ProxyDeployed {
        sender: sender.parse()?,
        proxyAddress: proxy.parse()?,
        salt: U256::from_be_bytes(alice().0),
        implementation: implementation.parse()?,
    }
    .encode_log_data())
}

fn label_registered(sender: &str) -> Result<alloy_primitives::LogData> {
    Ok(LabelRegistered {
        tokenId: U256::from(1),
        labelHash: alice(),
        label: "alice".to_owned(),
        owner: SENDER.parse()?,
        expiry: 0,
        sender: sender.parse()?,
    }
    .encode_log_data())
}

fn initialized() -> alloy_primitives::LogData {
    Initialized { version: 1 }.encode_log_data()
}

/// The blocks of the retained factory origins.
async fn origins(pool: &sqlx::PgPool) -> Result<Vec<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND source_family = 'ens_v2_migration_l1'
           AND event_kind = 'ContractDiscovered'
         ORDER BY block_number",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?)
}

impl Fixture {
    /// `sync`, completing the Ingest redo a widened watch plan stamps by the runner's own effects
    /// on the phase rows. The harness's test source fails the intake shape an Ethereum Mainnet
    /// Ingest redo checks (`src/verify_source.rs`, `validate_intake_shape`), and Ingest loads
    /// nothing here, so what the redo leaves is the Interpret stamp it writes as it begins
    /// (`src/redo_stamp.rs`, `stamp_ingest_redo_dependents`) and its restored row
    /// (`src/redo_state.rs`, the completion statement). The real begin stamps Verify too. These
    /// rows leave it out because Verify takes no part in the attested pair and `assert_hashes`
    /// reads only the Interpret and Project rows, so no row here can observe it.
    async fn sync_migration(&self) -> Result<String> {
        sync_schema_v2_repository(self.pool(), &load_repository(&self.profile)?).await?;
        let required: Option<(i64, i64)> = sqlx::query_as(
            "SELECT redo_from_block_number, redo_to_block_number FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
        )
        .bind(CHAIN)
        .fetch_optional(self.pool())
        .await?;
        if let Some((from, to)) = required {
            let stamped = sqlx::query(CREATE_STAMP)
                .bind(CHAIN)
                .bind(PhaseName::Interpret.as_str())
                .bind(from)
                .bind(to)
                .bind(format!(
                    "{REQUIRED_REDO_PREFIX}ingest redo may change the raw-fact extent"
                ))
                .execute(self.pool())
                .await?;
            ensure!(
                stamped.rows_affected() == 1,
                "the Interpret row takes the Ingest redo's stamp from {from}"
            );
            sqlx::query(
                "UPDATE chain_phase_state
                 SET phase_status = redo_previous_phase_status, redo_in_progress = false,
                     redo_mode = NULL, last_error = redo_previous_last_error,
                     started_at = redo_previous_started_at, finished_at = now(),
                     redo_previous_phase_status = NULL, redo_previous_last_error = NULL,
                     redo_previous_started_at = NULL, redo_previous_finished_at = NULL,
                     redo_from_block_number = NULL, redo_to_block_number = NULL,
                     redo_requested_from_block_number = NULL,
                     redo_requested_to_block_number = NULL, updated_at = now()
                 WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
            )
            .bind(CHAIN)
            .execute(self.pool())
            .await?;
        }
        self.token().await
    }

    fn declare_migration(&self, role: &str, declared: Option<(&str, i64)>) -> Result<()> {
        self.edit(MIGRATION_FILE, |manifest| declare(manifest, role, declared))
    }

    /// Blocks `head + 1..=to` canonical as `seed_chain` makes them, and every phase completed
    /// through `to` with those blocks not derived yet, as a chain that followed the head leaves
    /// them before its next full pair.
    async fn advance(&mut self, to: i64) -> Result<()> {
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
        .bind(CHAIN)
        .bind(self.head + 1)
        .bind(to)
        .bind(SENDER)
        .bind(REGISTRY)
        .execute(self.pool())
        .await?;
        sqlx::query(
            "UPDATE ingest_cursors SET next_block_number = $2 + 1, target_block_number = $2,
                 last_processed_block_number = $2, last_processed_block_hash = $3
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .bind(to)
        .bind(block_hash(to))
        .execute(self.pool())
        .await?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET current_block_number = $2, current_block_hash = $3,
                 target_block_number = $2, target_block_hash = $3,
                 live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
                 live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END
             WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
        )
        .bind(CHAIN)
        .bind(to)
        .bind(block_hash(to))
        .execute(self.pool())
        .await?;
        publish_heads(
            self.pool(),
            CHAIN,
            &HeadMarkers {
                latest: BlockMarker::new(to, block_hash(to))?,
                safe: None,
                finalized: None,
            },
        )
        .await?;
        self.head = to;
        Ok(())
    }

    /// Publish the head as the safe and finalized block too, so each Project publication prunes
    /// its undo journal to the configured depth below it, as on a chain with finality heads.
    async fn finalize_head(&self) -> Result<()> {
        let head = BlockMarker::new(self.head, block_hash(self.head))?;
        publish_heads(
            self.pool(),
            CHAIN,
            &HeadMarkers {
                latest: head.clone(),
                safe: Some(head.clone()),
                finalized: Some(head),
            },
        )
        .await?;
        Ok(())
    }
}

/// A fixture whose baseline does not declare the UserRegistry implementation, with factory
/// origins naming it at `START + 10` and at `start + 5`.
async fn without_user_implementation(prefix: &str, span: i64, start: i64) -> Result<Fixture> {
    let fixture = Fixture::with(prefix, span, |profile| {
        migration_baseline(profile, &[("user_registry_implementation", None)])
    })
    .await?;
    fixture
        .log(
            START + 10,
            0,
            FACTORY,
            &proxy_deployed(USER_IMPLEMENTATION)?,
        )
        .await?;
    fixture
        .log(start + 5, 0, FACTORY, &proxy_deployed(USER_IMPLEMENTATION)?)
        .await?;
    Ok(fixture)
}

// Row 1. The UserRegistry implementation added at S below the head, its own log at S. The
// printed full range runs from S, the audit row records S..=H, Project undoes to below S and
// replays, the origin above S is retained and the one below is not, and the tables equal the
// full pair's.
#[tokio::test]
async fn an_added_implementation_below_the_head_redoes_from_its_start() -> Result<()> {
    let start = START + 20;
    let fixture = without_user_implementation("migration_added", 59, start).await?;
    fixture
        .log(start, 1, USER_IMPLEMENTATION, &initialized())
        .await?;
    fixture.initial_pair().await?;
    ensure!(
        origins(fixture.pool()).await?.is_empty(),
        "nothing retained"
    );
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture.assert_bounded_refusal(&token, start).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, start)?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    ensure!(origins(fixture.pool()).await? == vec![start + 5]);
    let repair = repair_record(fixture.pool()).await?;
    ensure!(
        repair.0 == "complete" && repair.1.is_some_and(|base| (START..start).contains(&base)),
        "the family repair is not an undo stopping in {START}..{start}: {repair:?}"
    );
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 2. The UserRegistry implementation moved from A at S1 to B at S2 above it, with a factory
// origin naming A at S1 + 1. The redo runs from S1, not S2, so the origin A no longer declares
// goes, as on the full pair under the moved manifest.
#[tokio::test]
async fn a_moved_implementation_redoes_from_its_old_start() -> Result<()> {
    let (old, new) = (START + 10, START + 30);
    let fixture = Fixture::with("migration_moved", 59, |profile| {
        migration_baseline(
            profile,
            &[(
                "user_registry_implementation",
                Some((USER_IMPLEMENTATION, old)),
            )],
        )
    })
    .await?;
    fixture
        .log(old + 1, 0, FACTORY, &proxy_deployed(USER_IMPLEMENTATION)?)
        .await?;
    fixture.initial_pair().await?;
    ensure!(origins(fixture.pool()).await? == vec![old + 1]);
    fixture.declare_migration("user_registry_implementation", Some((REDEPLOYED, new)))?;
    let token = fixture.sync_migration().await?;
    fixture.assert_bounded_refusal(&token, old).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(old, old)?;
    ensure!(fixture.audits().await? == vec![(old, fixture.head)]);
    ensure!(origins(fixture.pool()).await?.is_empty(), "A's origin goes");
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 2b. The locked migration controller moved from A at S1 to B at S2 with no log naming
// either: the bound is still S1. It does not depend on what the old address did.
#[tokio::test]
async fn a_moved_controller_with_no_logs_redoes_from_its_old_start() -> Result<()> {
    let (old, new) = (START + 10, START + 30);
    let fixture = Fixture::with("migration_moved_quiet", 59, |profile| {
        migration_baseline(
            profile,
            &[(
                "locked_migration_controller",
                Some((LOCKED_CONTROLLER, old)),
            )],
        )
    })
    .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration("locked_migration_controller", Some((REDEPLOYED, new)))?;
    let token = fixture.sync_migration().await?;
    fixture.assert_bounded_refusal(&token, old).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(old, old)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 3. A declaration removed from the migration manifest keeps the full range and says why.
#[tokio::test]
async fn a_removed_migration_declaration_keeps_the_full_range() -> Result<()> {
    let fixture = Fixture::with("migration_removed", 59, |profile| {
        migration_baseline(
            profile,
            &[(
                "migration_helper",
                Some(("0x0b92006fa187b4c98d0a7dfc9d3b918b6d243485", START + 10)),
            )],
        )
    })
    .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration("migration_helper", None)?;
    let token = fixture.sync_migration().await?;
    fixture
        .assert_short_range_refused(&token, "of family ens_v2_migration_l1 changed")
        .await?;
    fixture
        .assert_short_range_refused(&token, "it removes the migration_helper declaration")
        .await?;
    fixture.cleanup().await
}

// Row 4. A qualifying addition after the families were published under another interpreter
// content hash, as in a release that also edits an event block: the hash reason wins.
#[tokio::test]
async fn an_interpreter_rotation_keeps_a_migration_addition_full() -> Result<()> {
    let start = START + 20;
    let fixture = without_user_implementation("migration_rotation", 59, start).await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
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
    fixture.cleanup().await
}

// Row 5. A changed migration `correlation_addresses` keeps the full range with the every-block
// reason, which comes before the family's own predicate.
#[tokio::test]
async fn a_migration_correlation_change_names_the_every_block_field() -> Result<()> {
    let fixture = Fixture::with("migration_correlation", 59, |profile| {
        migration_baseline(profile, &[])
    })
    .await?;
    fixture.initial_pair().await?;
    fixture.edit(MIGRATION_FILE, |manifest| {
        manifest.replacen(
            "[correlation_addresses]\n",
            &format!("[correlation_addresses]\nfixture_correlation = \"{REDEPLOYED}\"\n"),
            1,
        )
    })?;
    let token = fixture.sync_migration().await?;
    fixture
        .assert_short_range_refused(
            &token,
            "changed `correlation_addresses`, which Interpret reads at every block",
        )
        .await?;
    fixture.cleanup().await
}

// Row 6. The implementation admitted at S above the head costs one block. The head then passes S
// and an ordinary full pair publishes the set. A move to S2 is then bounded from S, the old start
// now below the head, and the origin at S + 5 that the move undeclares goes.
#[tokio::test]
async fn an_admission_ahead_then_moved_redoes_from_its_old_start() -> Result<()> {
    let mut fixture = Fixture::with("migration_ahead_then_moved", 59, |profile| {
        migration_baseline(profile, &[("user_registry_implementation", None)])
    })
    .await?;
    let start = fixture.head + 20;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(fixture.head, fixture.head)?;
    fixture.advance(start + 40).await?;
    fixture
        .log(start + 5, 0, FACTORY, &proxy_deployed(USER_IMPLEMENTATION)?)
        .await?;
    fixture.initial_pair().await?;
    ensure!(origins(fixture.pool()).await? == vec![start + 5]);
    fixture.declare_migration(
        "user_registry_implementation",
        Some((REDEPLOYED, start + 20)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture.assert_bounded_refusal(&token, start).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(start, start)?;
    ensure!(
        origins(fixture.pool()).await?.is_empty(),
        "the old origin goes"
    );
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 7. A second addition after the first bounded pair published diffs against the set that
// pair published, so it bounds to its own start S2, not to the first one's S1.
#[tokio::test]
async fn a_second_addition_after_a_completed_pair_bounds_to_its_own_start() -> Result<()> {
    let (first, second) = (START + 20, START + 40);
    let fixture = Fixture::with("migration_second_addition", 59, |profile| {
        migration_baseline(
            profile,
            &[
                ("user_registry_implementation", None),
                ("wrapper_registry_implementation", None),
            ],
        )
    })
    .await?;
    fixture
        .log(first + 5, 0, FACTORY, &proxy_deployed(USER_IMPLEMENTATION)?)
        .await?;
    fixture
        .log(
            second + 5,
            0,
            FACTORY,
            &proxy_deployed(WRAPPER_IMPLEMENTATION)?,
        )
        .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, first)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture.discharge(&token, None).await?;
    fixture.declare_migration(
        "wrapper_registry_implementation",
        Some((WRAPPER_IMPLEMENTATION, second)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture.assert_bounded_refusal(&token, second).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(second, second)?;
    ensure!(
        fixture.audits().await? == vec![(first, fixture.head), (second, fixture.head)],
        "{:?}",
        fixture.audits().await?
    );
    ensure!(origins(fixture.pool()).await? == vec![first + 5, second + 5]);
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 7b. A second addition after the first one's Interpret half completed and before Project
// published it. The open Project redo lowers the next Interpret start to S1, below the second
// addition's S2.
#[tokio::test]
async fn a_second_addition_between_the_halves_redoes_from_the_first() -> Result<()> {
    let (first, second) = (START + 20, START + 40);
    let fixture = Fixture::with("migration_between_halves", 59, |profile| {
        migration_baseline(
            profile,
            &[
                ("user_registry_implementation", None),
                ("wrapper_registry_implementation", None),
            ],
        )
    })
    .await?;
    fixture
        .log(first + 5, 0, FACTORY, &proxy_deployed(USER_IMPLEMENTATION)?)
        .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, first)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture
        .discharge(&token, Some(PhaseName::Project))
        .await
        .err()
        .context("the Project half stops")?;
    fixture.declare_migration(
        "wrapper_registry_implementation",
        Some((WRAPPER_IMPLEMENTATION, second)),
    )?;
    let token = fixture.sync_migration().await?;
    let run = fixture.discharge(&token, None).await?;
    fixture.assert_hashes().await?;
    // The tables first, so a redo that starts too late shows the rows it kept.
    let ranges = run.assert_ranges(first, first);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 10. A qualifying migration addition at S1 and a resolver declared at S2 above it in one
// sync bound together from S1.
#[tokio::test]
async fn a_migration_and_a_resolver_change_in_one_sync_bound_from_the_lower_start() -> Result<()> {
    let (migration, resolver) = (START + 15, START + 25);
    let fixture = without_user_implementation("migration_mixed", 59, migration).await?;
    fixture
        .log(resolver + 1, 0, RESOLVER, &addr_changed()?)
        .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, migration)),
    )?;
    fixture.add_resolver("public_resolver_fixture", RESOLVER, resolver)?;
    let token = fixture.sync_migration().await?;
    fixture.assert_bounded_refusal(&token, migration).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(migration, migration)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Row 11, the integration leg. A start edit of an unchanged address keeps the full range and says
// why. A new or retired migration manifest is row 13's leg in
// `an_ens_v2_manifest_change_is_refused_whole`, whose planted manifest is a new one.
#[tokio::test]
async fn a_migration_start_edit_keeps_the_full_range() -> Result<()> {
    let fixture = Fixture::with("migration_start_edit", 59, |profile| {
        migration_baseline(
            profile,
            &[(
                "locked_migration_controller",
                Some((LOCKED_CONTROLLER, START + 10)),
            )],
        )
    })
    .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "locked_migration_controller",
        Some((LOCKED_CONTROLLER, START + 20)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "it edits the locked_migration_controller declaration of {LOCKED_CONTROLLER} in \
                 place"
            ),
        )
        .await?;
    fixture.cleanup().await
}

// Row 17. The families were written under another projection content hash: the Interpret half
// is bounded and the Project stamp is the full range.
#[tokio::test]
async fn a_projection_rotation_keeps_the_migration_project_half_full() -> Result<()> {
    let start = START + 20;
    let fixture = without_user_implementation("migration_projection_rotation", 59, start).await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
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

// Row 18. Project stands below the first effect when the sync lands: its stamp is one block at
// its own head.
#[tokio::test]
async fn a_project_head_below_a_migration_first_effect_gets_a_one_block_stamp() -> Result<()> {
    let (start, project) = (START + 20, START + 10);
    let fixture = without_user_implementation("migration_project_behind", 59, start).await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
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
        run.ranges(PhaseName::Interpret)
            .first()
            .map(|range| range.from)
            == Some(start),
        "{:?}",
        run.ranges(PhaseName::Interpret)
    );
    ensure!(
        run.ranges(PhaseName::Project).first() == Some(&BlockRange::new(project, project)?),
        "{:?}",
        run.ranges(PhaseName::Project)
    );
    fixture.cleanup().await
}

// Row 19. The bound lies deeper than the kept undo journal. With finality heads published, each
// publication prunes the journal to 256 blocks below the family marker, so over 300 blocks it no
// longer reaches S. Interpret still runs from S. Project is asked from S, finds no journal to undo
// to, clears its families and rebuilds them, and the tables equal the full pair's.
#[tokio::test]
async fn a_bound_below_the_kept_journal_rebuilds_project_and_equals_the_full_pair() -> Result<()> {
    let start = START + 20;
    let fixture = without_user_implementation("migration_below_journal", 299, start).await?;
    fixture.finalize_head().await?;
    fixture.initial_pair().await?;
    let journal: Option<i64> =
        sqlx::query_scalar("SELECT min(block_number) FROM project_family_undo WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(fixture.pool())
            .await?;
    ensure!(
        journal.is_none_or(|lowest| lowest > start),
        "the journal still reaches {start}: its lowest block is {journal:?}"
    );
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
    let run = fixture.discharge(&token, None).await?;
    ensure!(fixture.audits().await? == vec![(start, fixture.head)]);
    let repair = repair_record(fixture.pool()).await?;
    ensure!(
        repair == ("complete".to_owned(), None),
        "the family repair is not a completed rebuild: {repair:?}"
    );
    // An undo with an unreadable block below its start also leaves no trusted base, but it
    // clears the reset sequence that only a rebuild's begin writes
    // (`crates/project/src/families/repair.rs`).
    let reset: Option<i64> =
        sqlx::query_scalar("SELECT reset_sequence FROM project_repair_record WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(fixture.pool())
            .await?;
    ensure!(
        reset.is_some(),
        "the family repair was an undo, not a rebuild"
    );
    ensure!(origins(fixture.pool()).await? == vec![start + 5]);
    fixture.assert_hashes().await?;
    let ranges = run.assert_ranges(start, start);
    fixture.assert_equals_full_pair().await?;
    ranges
}

// Row 22. A migration address admitted above the head: no first effect, so each phase replays the
// head block alone.
#[tokio::test]
async fn a_migration_admission_above_the_head_replays_one_block() -> Result<()> {
    let fixture = Fixture::with("migration_above_head", 59, |profile| {
        migration_baseline(profile, &[("user_registry_implementation", None)])
    })
    .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, fixture.head + 100)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(fixture.head, fixture.head)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// The start guard. An added address that emitted a log below its declared start was not deployed
// there, so a factory log naming it as `sender` could lie below the bound: the redo keeps the full
// range and names the log. The full range then runs, and the tables equal the full pair's.
#[tokio::test]
async fn an_added_address_with_a_log_below_its_start_keeps_the_full_range() -> Result<()> {
    let start = START + 20;
    let fixture = without_user_implementation("migration_guard_added", 59, start).await?;
    fixture
        .log(start - 5, 1, USER_IMPLEMENTATION, &initialized())
        .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration(
        "user_registry_implementation",
        Some((USER_IMPLEMENTATION, start)),
    )?;
    let token = fixture.sync_migration().await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "declares user_registry_implementation {USER_IMPLEMENTATION} from block {start}, \
                 but that address emitted a log at block {}, so the declared start is not its \
                 deployment block",
                start - 5
            ),
        )
        .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// The start guard on the old side of a move: the address the move retires logged below its own
// declared start.
#[tokio::test]
async fn a_moved_address_with_a_log_below_its_old_start_keeps_the_full_range() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let fixture = Fixture::with("migration_guard_moved", 59, |profile| {
        migration_baseline(
            profile,
            &[(
                "locked_migration_controller",
                Some((LOCKED_CONTROLLER, old)),
            )],
        )
    })
    .await?;
    fixture
        .log(old - 1, 0, LOCKED_CONTROLLER, &initialized())
        .await?;
    fixture.initial_pair().await?;
    fixture.declare_migration("locked_migration_controller", Some((REDEPLOYED, new)))?;
    let token = fixture.sync_migration().await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "declares locked_migration_controller {LOCKED_CONTROLLER} from block {old}, but \
                 that address emitted a log at block {}",
                old - 1
            ),
        )
        .await?;
    fixture.cleanup().await
}

/// A fixture whose `role` moves from `from` at `old` to `REDEPLOYED` at `new`, with `logs` of
/// `(block, emitter, log)`, synced after the initial pair.
async fn role_moved(
    prefix: &str,
    role: &str,
    (from, old, new): (&str, i64, i64),
    logs: &[(i64, &str, alloy_primitives::LogData)],
) -> Result<(Fixture, String)> {
    let fixture = Fixture::with(prefix, 59, |profile| {
        migration_baseline(profile, &[(role, Some((from, old)))])
    })
    .await?;
    for (block, emitter, log) in logs {
        fixture.log(*block, 0, emitter, log).await?;
    }
    fixture.initial_pair().await?;
    fixture.declare_migration(role, Some((REDEPLOYED, new)))?;
    let token = fixture.sync_migration().await?;
    Ok((fixture, token))
}

/// The locked migration controller moved from `LOCKED_CONTROLLER` at `old` to `REDEPLOYED` at
/// `new`, with factory logs at `logs` naming `(sender, proxy)`.
async fn controller_moved(
    prefix: &str,
    (old, new): (i64, i64),
    logs: &[(i64, &str, &str)],
) -> Result<(Fixture, String)> {
    let logs = logs
        .iter()
        .map(|(block, sender, proxy)| {
            Ok((
                *block,
                FACTORY,
                proxy_deployed_by(sender, proxy, UNDECLARED)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    role_moved(
        prefix,
        "locked_migration_controller",
        (LOCKED_CONTROLLER, old, new),
        &logs,
    )
    .await
}

// The factory-sender guard. The controller lookup reads a factory `ProxyDeployed` naming the
// declared controller as its indexed `sender`, whatever the block
// (`crates/adapters/src/schema_v2/migration/registry.rs`). A factory log naming the new controller
// below its declared start, and below the bound, shows the start is not its deployment block: the
// redo keeps the full range and names the factory log's block. The full range then runs, and the
// tables equal the full pair's.
#[tokio::test]
async fn a_factory_log_below_the_new_controllers_start_keeps_the_full_range() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let (fixture, token) = controller_moved(
        "migration_sender_new",
        (old, new),
        &[(START + 15, REDEPLOYED, PROXY)],
    )
    .await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "declares locked_migration_controller {REDEPLOYED} from block {new}, but the \
                 ProxyDeployed log of {FACTORY} at block {} names that address as `sender`, so \
                 the declared start is not its deployment block",
                START + 15
            ),
        )
        .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// The factory-sender guard on the old side of a move: a factory log one block below the old
// controller's declared start.
#[tokio::test]
async fn a_factory_log_below_the_old_controllers_start_keeps_the_full_range() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let (fixture, token) = controller_moved(
        "migration_sender_old",
        (old, new),
        &[(old - 1, LOCKED_CONTROLLER, PROXY)],
    )
    .await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "declares locked_migration_controller {LOCKED_CONTROLLER} from block {old}, but \
                 the ProxyDeployed log of {FACTORY} at block {} names that address as `sender`",
                old - 1
            ),
        )
        .await?;
    fixture.cleanup().await
}

// The guard's lower edge. A factory log naming each side of the move exactly at its declared
// start stays inside the bound, so the redo is bounded from the old start.
#[tokio::test]
async fn factory_logs_naming_a_controller_at_its_start_stay_bounded() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let (fixture, token) = controller_moved(
        "migration_sender_at_start",
        (old, new),
        &[(old, LOCKED_CONTROLLER, PROXY), (new, REDEPLOYED, PROXY)],
    )
    .await?;
    fixture.assert_bounded_refusal(&token, old).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(old, old)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// Each side is probed for its own address in the `sender` topic only. Below the new controller's
// start, a factory log naming the old controller at or above the old start, and one naming the new
// controller as `proxyAddress`, the next indexed topic, leave the redo bounded.
#[tokio::test]
async fn a_factory_log_naming_only_the_other_side_of_a_move_stays_bounded() -> Result<()> {
    let (old, new) = (START + 10, START + 30);
    let (fixture, token) = controller_moved(
        "migration_sender_other_side",
        (old, new),
        &[
            (START + 20, LOCKED_CONTROLLER, PROXY),
            (START + 5, SENDER, REDEPLOYED),
        ],
    )
    .await?;
    fixture.assert_bounded_refusal(&token, old).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(old, old)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// The registry-sender guard. The migration adapter compares the `sender` of a same-transaction
// registration, built from the registry's `LabelRegistered`, with both controllers by address
// alone (`crates/adapters/src/schema_v2/migration.rs`). A registration naming the new unlocked
// controller below its declared start, and below the bound, keeps the full range and names the
// registry log's block. The full range then runs, and the tables equal the full pair's.
#[tokio::test]
async fn a_registration_below_the_unlocked_controllers_start_keeps_the_full_range() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let (fixture, token) = role_moved(
        "migration_registration_unlocked",
        "unlocked_migration_controller",
        (UNLOCKED_CONTROLLER, old, new),
        &[(START + 15, ETH_REGISTRY, label_registered(REDEPLOYED)?)],
    )
    .await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "declares unlocked_migration_controller {REDEPLOYED} from block {new}, but the \
                 LabelRegistered log of {ETH_REGISTRY} at block {} names that address as \
                 `sender`, so the declared start is not its deployment block",
                START + 15
            ),
        )
        .await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(START, START)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// The registry-sender guard for the locked controller, on the old side of a move: a registration
// one block below the old controller's declared start.
#[tokio::test]
async fn a_registration_below_the_locked_controllers_start_keeps_the_full_range() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let (fixture, token) = role_moved(
        "migration_registration_locked",
        "locked_migration_controller",
        (LOCKED_CONTROLLER, old, new),
        &[(old - 1, ETH_REGISTRY, label_registered(LOCKED_CONTROLLER)?)],
    )
    .await?;
    fixture
        .assert_short_range_refused(
            &token,
            &format!(
                "declares locked_migration_controller {LOCKED_CONTROLLER} from block {old}, but \
                 the LabelRegistered log of {ETH_REGISTRY} at block {} names that address as \
                 `sender`",
                old - 1
            ),
        )
        .await?;
    fixture.cleanup().await
}

// The registry guard's lower edge. A registration naming each side of the move exactly at its
// declared start stays inside the bound, so the redo is bounded from the old start.
#[tokio::test]
async fn registrations_naming_a_controller_at_its_start_stay_bounded() -> Result<()> {
    let (old, new) = (START + 20, START + 30);
    let (fixture, token) = role_moved(
        "migration_registration_at_start",
        "locked_migration_controller",
        (LOCKED_CONTROLLER, old, new),
        &[
            (old, ETH_REGISTRY, label_registered(LOCKED_CONTROLLER)?),
            (new, ETH_REGISTRY, label_registered(REDEPLOYED)?),
        ],
    )
    .await?;
    fixture.assert_bounded_refusal(&token, old).await?;
    fixture
        .discharge(&token, None)
        .await?
        .assert_ranges(old, old)?;
    fixture.assert_hashes().await?;
    fixture.assert_equals_full_pair().await
}

// A registry change beside a controller move refuses the bound by the registry family's
// whole-redo rule, before any probe reads the registry it replaces.
#[tokio::test]
async fn a_registry_change_beside_a_controller_move_refuses_as_a_whole() -> Result<()> {
    let (fixture, _) =
        controller_moved("migration_registry_moved", (START + 20, START + 30), &[]).await?;
    fixture
        .plant_manifest(
            "ens_v2_registry_l1",
            json!([{"role": "registry", "address": PROXY, "proxy_kind": "none",
                "start_block": START + 5}]),
        )
        .await?;
    let token = fixture.plant_marker("registry_moved").await?;
    fixture
        .assert_short_range_refused(&token, "of family ens_v2_registry_l1 changed")
        .await?;
    fixture.cleanup().await
}

// The sender statement probes one emitter's own logs through `raw_logs_emitter_idx`, with the
// topics as filters, under the generic plan a prepared statement settles on. Over 20,000 logs
// from 200 emitters it walks no table whole.
#[tokio::test]
async fn the_sender_statement_reads_the_emitter_index() -> Result<()> {
    let fixture = Fixture::new("migration_sender_plan", 1_999).await?;
    sqlx::query(
        "INSERT INTO raw_logs (chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data)
         SELECT $1, $1 || '-block-' || height, height, $1 || '-transaction-' || height, 0,
                slot, '0x' || lpad(to_hex(4096 + (height * 10 + slot) % 200), 40, '0'),
                ARRAY['0x' || lpad(to_hex((height + slot) % 20), 64, '0'),
                      '0x' || lpad(to_hex(height % 50), 64, '0')], ''::bytea
         FROM generate_series($2::bigint, $3::bigint) height
         CROSS JOIN generate_series(1, 10) slot",
    )
    .bind(CHAIN)
    .bind(START)
    .bind(fixture.head)
    .execute(fixture.pool())
    .await?;
    sqlx::raw_sql("ANALYZE raw_logs, chain_lineage")
        .execute(fixture.pool())
        .await?;
    let mut transaction = fixture.pool().begin().await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(&format!(
        "PREPARE sender_log AS {}",
        phase_runner::sender_statement()
    ))
    .execute(&mut *transaction)
    .await?;
    let head = fixture.head;
    let (topic0, sender) = (format!("0x{:0>64}", 5), format!("0x{:0>64}", 7));
    let plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (COSTS OFF) EXECUTE sender_log('{CHAIN}',
             '0x0000000000000000000000000000000000001001', {head}, '{topic0}', 2, '{sender}')"
    ))
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    transaction.rollback().await?;
    println!("factory sender:\n{plan}\n");
    ensure!(
        plan.contains("raw_logs_emitter_idx"),
        "the statement does not probe raw_logs_emitter_idx:\n{plan}"
    );
    ensure!(
        plan.lines()
            .any(|line| line.contains("Index Cond") && line.contains("emitting_address")),
        "the emitter is not an index condition:\n{plan}"
    );
    ensure!(
        !plan.contains("Seq Scan on raw_logs"),
        "the statement scans raw_logs whole:\n{plan}"
    );
    fixture.cleanup().await
}
