//! TYR-363: the checked-in mainnet profile admits the official 2026-10-02 ENSv2 deployment over a
//! chain that ran the ENSv1-only profile. Ingest's redo is simulated by fetching only the logs the
//! synced watch plan includes, so a contract no manifest watches leaves nothing to interpret, as
//! on the node. The Interpret redo runs on the engine directly.
use super::*;
use bigname_ingest::WatchFilter;
use bigname_storage::{
    ResolutionState,
    families::control::cutover::{Admission, load_admission_on},
    load_resolution_state_on,
};

const CHAIN: &str = "ethereum-mainnet";
const FIRST: i64 = 26_105_000;
const HEAD: i64 = 26_217_000;
/// The intake cursor's start in `seed_chain`, below the deployment.
const INGEST_CURSOR_START: i64 = 0;
/// The deployment's first declared start, the ENSv1 mirror resolver's.
const DEPLOYMENT_START: i64 = 26_105_292;
const ROOT: &str = "0x2553c09be6447beffdb86fa622ad41fb8f77a91e";
const ROOT_START: i64 = 26_183_742;
/// The RootRegistry the 2026-10-13 redeploy replaced, created at 26105295.
const REPLACED_ROOT: &str = "0x51bbc58b3dae7d45b14fba8cf31b7c8f7904a750";
const ETH_REGISTRY: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
const ETH_REGISTRAR: &str = "0xd6d8369e55c94ae8afa714e6cf7ab5be54d5cc81";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const RENEWER: &str = "0x56c447f308f463a01121c41e72b0e7e7557fcb68";
const GRAVEYARD: &str = "0xda021a2c1c2f99574d861ef8c6fa745851beb7f6";
const CLIENT_PROXY: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const MANAGED_PROXY: &str = "0xcffd2ae680cd110d28739a70922636b5937b022b";
const UNIVERSAL_RESOLVER_V2: &str = "0x06fdd07f007adac667177b46ab2eb9a75132cfd5";
/// The ENSv1 UniversalResolver both proxies forward to before the DAO proposal executes.
/// (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
const UNIVERSAL_RESOLVER_V1: &str = "0xed73a03f19e8d849e44a39252d222c6ad5217e1e";
const NICK_RESERVATION: i64 = 26_105_700;
const NICK_EXPIRY: u64 = 1_900_000_000;
const FRESH_REGISTRATION: i64 = 26_216_500;

mod registrar {
    alloy_sol_types::sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
    }
}

mod base_registrar {
    alloy_sol_types::sol! {
        event NameRenewed(uint256 indexed id, uint256 expires);
        event ControllerAdded(address indexed controller);
        event ControllerRemoved(address indexed controller);
    }
}

mod renewer {
    alloy_sol_types::sol! {
        event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    }
}

/// The deleted `ens_execution/v1.toml`: the shadow entrypoint the ENSv1-only profile declared.
const ENS_V1_EXECUTION: &str = r#"# Shadow-scoped bootstrap for the canonical ENS execution entrypoint on Ethereum mainnet.
manifest_version = 1
namespace = "ens"
source_family = "ens_execution"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "shadow"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]
verified_resolution = "supported"

[[contracts]]
role = "universal_resolver"
address = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
proxy_kind = "none"
"#;

fn checked_in_profile() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet")
}

fn copy_dir(source: &std::path::Path, target: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &target.join(entry.file_name()))?;
        } else {
            std::fs::copy(&path, target.join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// The profile the previous release synced: no ENSv2 family and the shadow execution manifest.
fn ens_v1_profile() -> Result<std::path::PathBuf> {
    let root = std::env::temp_dir().join(format!("bigname-mainnet-ens-v1-{}", Uuid::new_v4()));
    copy_dir(&checked_in_profile(), &root)?;
    let ens = root.join("ethereum/ens");
    for entry in std::fs::read_dir(&ens)? {
        let path = entry?.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("ens_v2_"))
        {
            std::fs::remove_dir_all(path)?;
        }
    }
    let execution = ens.join("ens_execution");
    if execution.join("v2.toml").exists() {
        std::fs::remove_file(execution.join("v2.toml"))?;
    }
    std::fs::write(execution.join("v1.toml"), ENS_V1_EXECUTION)?;
    Ok(root)
}

type Fact = (i64, &'static str, alloy_primitives::LogData);

/// The canonical chain from `first` to `head`, its head, and an intake cursor at `head`.
async fn seed_chain(pool: &PgPool, first: i64, head: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         )
         SELECT $1, $1 || '-block-' || height::text,
                CASE WHEN height > $2 THEN $1 || '-block-' || (height - 1)::text END,
                height, to_timestamp(height), 'canonical'::canonicality_state
         FROM generate_series($2::bigint, $3::bigint) AS height",
    )
    .bind(CHAIN)
    .bind(first)
    .bind(head)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, head))
    .bind(head)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'intake', 'drpc', 'ethereum_head', $4, $2 + 1, $2, $2, $3)",
    )
    .bind(CHAIN)
    .bind(head)
    .bind(block_hash(CHAIN, head))
    .bind(INGEST_CURSOR_START)
    .execute(pool)
    .await?;
    PhaseStore::new(pool.clone())
        .initialize_chain(CHAIN)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', started_at = now(), finished_at = now(),
             current_block_number = $2,
             current_block_hash = $3,
             input_content_hash = CASE phase_name WHEN 'project' THEN $5
                 WHEN 'interpret' THEN $4 END
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(head)
    .bind(block_hash(CHAIN, head))
    .bind(INTERPRETER_CONTENT_HASH)
    .bind(PROJECTION_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

/// One transaction per block, whose logs are the block's facts in order.
async fn insert_facts(pool: &PgPool, facts: &[Fact]) -> Result<()> {
    let mut previous = None;
    let mut index = 0;
    for (number, emitter, fact) in facts {
        let transaction = format!("{CHAIN}-transaction-{number}");
        if previous != Some(*number) {
            sqlx::query(
                "INSERT INTO raw_transactions (
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     from_address, to_address
                 ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
            )
            .bind(CHAIN)
            .bind(block_hash(CHAIN, *number))
            .bind(number)
            .bind(&transaction)
            .bind(SENDER)
            .bind(emitter)
            .execute(pool)
            .await?;
            index = 0;
        }
        insert_log_at(
            pool,
            CHAIN,
            *number,
            &transaction,
            index,
            emitter,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
        previous = Some(*number);
        index += 1;
    }
    Ok(())
}

fn topics(fact: &alloy_primitives::LogData) -> Vec<String> {
    fact.topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect()
}

/// What the stamped Ingest redo fetches: every fact the synced watch plan includes. A fact can
/// widen the plan (an announced registry), so the filter is reloaded until nothing more matches.
/// Returns the facts no watched tuple covers.
async fn ingest(pool: &PgPool, from: i64, to: i64, facts: Vec<Fact>) -> Result<Vec<Fact>> {
    let mut pending = facts;
    loop {
        let watch: WatchFilter = load_watch_filter(pool, CHAIN, from, to).await?;
        let (fetched, rest): (Vec<Fact>, Vec<Fact>) =
            pending.into_iter().partition(|(number, emitter, fact)| {
                (from..=to).contains(number) && watch.includes_log(emitter, &topics(fact), *number)
            });
        if fetched.is_empty() {
            return Ok(rest);
        }
        insert_facts(pool, &fetched).await?;
        pending = rest;
    }
}

async fn interpret_through(
    pool: &PgPool,
    from_block: i64,
    to_block: i64,
    mode: InterpretRunMode,
) -> Result<()> {
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(std::num::NonZeroU32::new(200_000).expect("non-zero"));
    let mut resume_current = None;
    loop {
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block,
                to_block,
                resume_current,
                mode,
            })
            .await?;
        if outcome.complete {
            return Ok(());
        }
        resume_current = Some(outcome.current);
    }
}

fn eth_name(label: &str) -> String {
    format!("ens:{:#x}", raw_namehash(&[label.as_bytes(), b"eth"]))
}

fn reserved(label: &str, expiry: u64) -> Result<alloy_primitives::LogData> {
    Ok(v2_registry_events::LabelReserved {
        tokenId: versioned_token(label, 0),
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        expiry,
        sender: SENDER.parse()?,
    }
    .encode_log_data())
}

fn registered(label: &str) -> Result<alloy_primitives::LogData> {
    Ok(v2_registry_events::LabelRegistered {
        tokenId: versioned_token(label, 0),
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: SENDER.parse()?,
        expiry: u64::MAX,
        sender: SENDER.parse()?,
    }
    .encode_log_data())
}

fn upgraded(implementation: &str) -> Result<alloy_primitives::LogData> {
    Ok(Upgraded {
        implementation: implementation.parse()?,
    }
    .encode_log_data())
}

/// The ENSv2 logs of the deployment window. The replaced RootRegistry announced itself at
/// 26105295, as the chain shows, and then registered its own `eth` entry. The admitted root's
/// `eth` entry points at the ETHRegistry (block 26183992, the chain's only such log), and a
/// premigration reservation precedes it.
fn deployment_facts() -> Result<Vec<Fact>> {
    Ok(vec![
        (
            26_105_295,
            REPLACED_ROOT,
            RegistryCreated {}.encode_log_data(),
        ),
        (26_105_296, REPLACED_ROOT, registered("eth")?),
        (
            26_105_621,
            ETH_REGISTRY,
            RegistryCreated {}.encode_log_data(),
        ),
        (
            NICK_RESERVATION,
            ETH_REGISTRY,
            reserved("nick", NICK_EXPIRY)?,
        ),
        (26_183_777, ROOT, registered("eth")?),
        (
            26_183_992,
            ROOT,
            v2_registry_events::SubregistryUpdated {
                tokenId: versioned_token("eth", 0),
                subregistry: ETH_REGISTRY.parse()?,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            FRESH_REGISTRATION,
            ETH_REGISTRAR,
            registrar::NameRegistered {
                tokenId: versioned_token("fresh", 0),
                label: "fresh".to_owned(),
                owner: SENDER.parse()?,
                subregistry: Address::ZERO,
                resolver: Address::ZERO,
                duration: 31_536_000,
                paymentToken: Address::ZERO,
                referrer: B256::ZERO,
                base: U256::from(1_u64),
                premium: U256::ZERO,
            }
            .encode_log_data(),
        ),
    ])
}

async fn admission(pool: &PgPool) -> Result<Option<Admission>> {
    load_admission_on(&mut *pool.acquire().await?, CHAIN).await
}

/// The resolution state with its addresses lower-cased, as the constants spell them.
async fn resolution(pool: &PgPool) -> Result<Option<ResolutionState>> {
    Ok(load_resolution_state_on(&mut *pool.acquire().await?, CHAIN)
        .await?
        .map(|state| ResolutionState {
            proxy: state.proxy.to_ascii_lowercase(),
            implementation: state.implementation.to_ascii_lowercase(),
            ..state
        }))
}

/// The published admission: the family marker's root registry and its start.
async fn published_admission(pool: &PgPool) -> Result<(Option<String>, Option<i64>)> {
    Ok(sqlx::query_as(
        "SELECT root_registry, since_block FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?)
}

/// `(block, expiry)` of every reservation that names `label`.eth.
async fn named_reservations(pool: &PgPool, label: &str) -> Result<Vec<(i64, Option<i64>)>> {
    Ok(sqlx::query_as(
        "SELECT block_number, (after_state ->> 'expiry')::bigint FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationReserved' AND logical_name_id = $2
         ORDER BY block_number",
    )
    .bind(CHAIN)
    .bind(eth_name(label))
    .fetch_all(pool)
    .await?)
}

async fn registrar_registrations(pool: &PgPool, label: &str) -> Result<Vec<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrarNameRegistered' AND logical_name_id = $2
           AND lower(raw_fact_ref ->> 'emitting_address') = $3",
    )
    .bind(CHAIN)
    .bind(eth_name(label))
    .bind(ETH_REGISTRAR)
    .fetch_all(pool)
    .await?)
}

/// The events `address` emitted that Interpret derived, and how many of them name a name.
async fn derived_from(pool: &PgPool, address: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(logical_name_id) FROM normalized_events
         WHERE chain_id = $1 AND lower(raw_fact_ref ->> 'emitting_address') = $2",
    )
    .bind(CHAIN)
    .bind(address)
    .fetch_one(pool)
    .await?)
}

/// Rows 2, 4, 7 and 9 of the TYR-363 scope: sync, the stamped Ingest redo from the deployment's
/// first block, the Interpret redo and Project over a chain that ran the ENSv1-only profile.
#[tokio::test]
async fn the_mainnet_admission_cuts_the_chain_over_through_the_stamped_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_mainnet_admission").await?;
    let pool = scratch.pool();
    let previous = ens_v1_profile()?;
    sync_schema_v2_repository(pool, &load_repository(&previous)?).await?;
    seed_chain(pool, FIRST, HEAD).await?;
    interpret_through(pool, FIRST, HEAD, InterpretRunMode::Normal).await?;
    run_project(pool, CHAIN, HEAD, 0, HEAD).await?;
    assert_eq!(admission(pool).await?, None);
    assert_eq!(published_admission(pool).await?, (None, None));

    sync_schema_v2_repository(pool, &load_repository(checked_in_profile())?).await?;
    let markers: Vec<String> = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    assert_eq!(markers.len(), 2);
    assert!(
        markers
            .iter()
            .all(|marker| marker.starts_with("manifest-authority:")),
        "{markers:?}"
    );
    let ingest_redo: (bool, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let (true, Some(redo_from), Some(redo_to)) = ingest_redo else {
        panic!("no required Ingest redo: {ingest_redo:?}");
    };
    // Each new family-wide and all-emitter entry starts at its family's first declaration, so the
    // stamped Ingest redo runs from the deployment's first block, the ENSv1 mirror resolver's
    // 26105292, to the head, not from the first ingest cursor.
    assert_eq!((redo_from, redo_to), (DEPLOYMENT_START, HEAD));
    // Sync retires only active manifests. The deleted shadow manifest keeps its row, and a shadow
    // manifest is watched and interpreted by nothing.
    let execution: Vec<(String, String)> = sqlx::query_as(
        "SELECT file_path, rollout_status FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_execution' ORDER BY file_path",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        execution
            .iter()
            .map(|(path, status)| (path.rsplit('/').next().unwrap_or(path), status.as_str()))
            .collect::<Vec<_>>(),
        [("v1.toml", "shadow"), ("v2.toml", "active")]
    );

    // The redo fetches every fact. The registry family's `RegistryCreated` watch covers every
    // emitter over the whole fetched range, so the replaced RootRegistry's announcement is
    // fetched and admits its own later logs.
    let unfetched = ingest(pool, redo_from, redo_to, deployment_facts()?).await?;
    assert_eq!(
        unfetched
            .iter()
            .map(|(number, emitter, _)| (*number, *emitter))
            .collect::<Vec<_>>(),
        []
    );
    interpret_through(pool, FIRST, HEAD, InterpretRunMode::Redo).await?;
    run_project(pool, CHAIN, HEAD, 0, HEAD).await?;

    // Row 2: the chain is cut over at the admitted root, and the publication records it.
    let admitted = Some(Admission {
        root_registry: ROOT.to_owned(),
        since_block: Some(ROOT_START),
    });
    assert_eq!(admission(pool).await?, admitted);
    assert_eq!(
        published_admission(pool).await?,
        (Some(ROOT.to_owned()), Some(ROOT_START))
    );
    // Row 4: a reservation below the root's `eth` entry is named under the ETHRegistry anchor.
    assert_eq!(
        named_reservations(pool, "nick").await?,
        [(NICK_RESERVATION, Some(i64::try_from(NICK_EXPIRY)?))]
    );
    // Row 7: the ETHRegistrar's registration names its label under `.eth`.
    assert_eq!(
        registrar_registrations(pool, "fresh").await?,
        [FRESH_REGISTRATION]
    );
    // Row 9: the admitted root names `eth`. The replaced root's announcement was fetched, so it is
    // indexed like any self-announced registry, but nothing admitted reaches it, and none of its
    // three derived events, its own `eth` entry included, names a name.
    let (root_events, root_named) = derived_from(pool, ROOT).await?;
    assert!(
        root_events > 0 && root_named > 0,
        "{root_events} {root_named}"
    );
    let (replaced_events, replaced_named) = derived_from(pool, REPLACED_ROOT).await?;
    let replaced_admissions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM contract_instance_addresses
         WHERE chain_id = $1 AND lower(address) = $2",
    )
    .bind(CHAIN)
    .bind(REPLACED_ROOT)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        (replaced_events, replaced_named, replaced_admissions),
        (3, 0, 1)
    );
    // Before the DAO proposal neither proxy has an `Upgraded`, so there is no proxy row: the
    // unadmitted gauge reads 0 because nothing was observed, not because the chain is admitted.
    assert_eq!(resolution(pool).await?, None);

    std::fs::remove_dir_all(previous)?;
    scratch.cleanup().await
}

/// A fresh mainnet corpus at `start`, `facts` interpreted and published one block at a time,
/// with the resolution state after each block.
async fn follow(
    name: &str,
    start: i64,
    facts: Vec<Fact>,
) -> Result<(ScratchDatabase, Vec<Option<ResolutionState>>)> {
    let scratch = ScratchDatabase::create(name).await?;
    let pool = scratch.pool();
    sync_schema_v2_repository(pool, &load_repository(checked_in_profile())?).await?;
    let head = facts
        .iter()
        .map(|(number, _, _)| *number)
        .max()
        .unwrap_or(start);
    seed_chain(pool, start, head).await?;
    insert_facts(pool, &facts).await?;
    let mut blocks: Vec<i64> = facts.iter().map(|(number, _, _)| *number).collect();
    blocks.dedup();
    let mut states = Vec::new();
    let mut from = start;
    for block in blocks {
        interpret_through(pool, from, block, InterpretRunMode::Normal).await?;
        run_project(pool, CHAIN, block, 0, block).await?;
        states.push(resolution(pool).await?);
        from = block + 1;
    }
    Ok((scratch, states))
}

fn state(
    proxy: &str,
    implementation: &str,
    since_block: i64,
    unadmitted: bool,
) -> Option<ResolutionState> {
    Some(ResolutionState {
        since_block,
        proxy: proxy.to_owned(),
        implementation: implementation.to_owned(),
        unadmitted,
    })
}

/// `(implementation, implementation_kind)` of `proxy`'s row.
async fn proxy_row(pool: &PgPool, proxy: &str) -> Result<Option<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT lower(implementation), implementation_kind FROM project_universal_resolver_proxy
         WHERE chain_id = $1 AND lower(proxy_address) = $2",
    )
    .bind(CHAIN)
    .bind(proxy)
    .fetch_optional(pool)
    .await?)
}

const DAO: i64 = 26_300_000;

/// Row 8: the DAO proposal's controller calls (calls 7 to 13), a renewal through the
/// ETHRenewerV1 bridge, call 15 (the client-facing proxy to the managed proxy), then the
/// deployment step that upgrades the managed proxy to UniversalResolverV2.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/dao/proposal.md:L13-L21 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deploy/universalResolver/05_setup_ManagedUniversalResolverProxyToUniversalResolverImplementation.ts:L26-L46 @ ens_v2_mainnet_20261002@b1f97b91)
#[tokio::test]
async fn the_dao_calls_derive_controller_bridge_and_proxy_rows() -> Result<()> {
    let removed = [
        "0x59e16fccd424cc24e280be16e11bcd56fb0ce547",
        "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
        "0x60c7c2a24b5e86c38639fd1586917a8fef66a56d",
        "0xcf60916b6cb4753f58533808fa610fcbd4098ec0",
        "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5",
    ];
    let mut facts: Vec<Fact> = Vec::new();
    for controller in removed {
        facts.push((
            DAO,
            BASE_REGISTRAR,
            base_registrar::ControllerRemoved {
                controller: controller.parse()?,
            }
            .encode_log_data(),
        ));
    }
    for controller in [GRAVEYARD, RENEWER] {
        facts.push((
            DAO,
            BASE_REGISTRAR,
            base_registrar::ControllerAdded {
                controller: controller.parse()?,
            }
            .encode_log_data(),
        ));
    }
    // A premigrated reservation, then its renewal through the bridge: the ETHRegistry expiry,
    // the BaseRegistrar renewal and the bridge's label-bearing `NameRenewed`, in one transaction.
    const V1_EXPIRY: u64 = 1_848_966_582;
    const V2_EXPIRY: u64 = 1_854_323_383;
    facts.push((DAO + 1, ETH_REGISTRY, reserved("nick", 1_822_787_383)?));
    facts.extend([
        (
            DAO + 2,
            ETH_REGISTRY,
            v2_registry_events::ExpiryUpdated {
                tokenId: versioned_token("nick", 0),
                newExpiry: V2_EXPIRY,
                sender: RENEWER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            DAO + 2,
            BASE_REGISTRAR,
            base_registrar::NameRenewed {
                id: U256::from_be_bytes(keccak256(b"nick").0),
                expires: U256::from(V1_EXPIRY),
            }
            .encode_log_data(),
        ),
        (
            DAO + 2,
            RENEWER,
            renewer::NameRenewed {
                tokenId: versioned_token("nick", 0),
                label: "nick".to_owned(),
                duration: 31_536_000,
                newExpiry: V2_EXPIRY,
                paymentToken: Address::ZERO,
                referrer: B256::ZERO,
                amount: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
    ]);
    facts.push((DAO + 3, CLIENT_PROXY, upgraded(MANAGED_PROXY)?));
    facts.push((DAO + 4, MANAGED_PROXY, upgraded(UNIVERSAL_RESOLVER_V2)?));
    let (scratch, states) = follow("production_interpret_mainnet_dao_calls", DAO, facts).await?;
    let pool = scratch.pool();

    for controller in removed.iter().chain(&[GRAVEYARD, RENEWER]) {
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events
             WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'PermissionChanged'
               AND lower(raw_fact_ref ->> 'emitting_address') = $3
               AND lower(after_state::text) LIKE '%' || $4 || '%'",
        )
        .bind(CHAIN)
        .bind(DAO)
        .bind(BASE_REGISTRAR)
        .bind(*controller)
        .fetch_one(pool)
        .await?;
        assert!(rows > 0, "controller {controller}");
    }
    let bridged: Vec<(Option<String>, Option<i64>)> = sqlx::query_as(
        "SELECT logical_name_id, (after_state ->> 'expiry')::bigint FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationRenewed'
           AND lower(raw_fact_ref ->> 'emitting_address') = $2",
    )
    .bind(CHAIN)
    .bind(RENEWER)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        bridged,
        [(Some(eth_name("nick")), Some(i64::try_from(V2_EXPIRY)?))]
    );

    // No proxy row until call 15. Then the client-facing proxy hops to a proxy with no
    // `Upgraded` yet, which is not unadmitted, and the managed proxy's upgrade ends the walk at
    // the listed implementation.
    assert_eq!(
        states,
        [
            None,
            None,
            None,
            state(CLIENT_PROXY, MANAGED_PROXY, DAO + 3, false),
            state(MANAGED_PROXY, UNIVERSAL_RESOLVER_V2, DAO + 4, false),
        ]
    );
    assert_eq!(
        proxy_row(pool, CLIENT_PROXY).await?,
        Some((
            MANAGED_PROXY.to_owned(),
            "universal_resolver_proxy".to_owned()
        ))
    );
    assert_eq!(
        proxy_row(pool, MANAGED_PROXY).await?,
        Some((
            UNIVERSAL_RESOLVER_V2.to_owned(),
            "admitted_universal_resolver".to_owned()
        ))
    );
    scratch.cleanup().await
}

/// Row 8, the other order and the page: the managed proxy upgrades before call 15, which leaves
/// the chain with no client-facing row until call 15 lands on it admitted. A client-facing proxy
/// that ends at an implementation the manifest does not list reads unadmitted.
#[tokio::test]
async fn either_proxy_order_reads_admitted_and_an_unlisted_implementation_pages() -> Result<()> {
    let (scratch, states) = follow(
        "production_interpret_mainnet_proxy_order",
        DAO,
        vec![
            (DAO, MANAGED_PROXY, upgraded(UNIVERSAL_RESOLVER_V2)?),
            (DAO + 1, CLIENT_PROXY, upgraded(MANAGED_PROXY)?),
        ],
    )
    .await?;
    assert_eq!(
        states,
        [
            None,
            state(MANAGED_PROXY, UNIVERSAL_RESOLVER_V2, DAO + 1, false),
        ]
    );
    scratch.cleanup().await?;

    let (scratch, states) = follow(
        "production_interpret_mainnet_proxy_unlisted",
        DAO,
        vec![(DAO, CLIENT_PROXY, upgraded(UNIVERSAL_RESOLVER_V1)?)],
    )
    .await?;
    assert_eq!(
        states,
        [state(CLIENT_PROXY, UNIVERSAL_RESOLVER_V1, DAO, true)]
    );
    scratch.cleanup().await
}
