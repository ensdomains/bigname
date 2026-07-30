use anyhow::{Context, Result, bail, ensure};
use bigname_adapters::{
    EnsV1UnwrappedAuthoritySyncSummary, EnsV2PermissionsSyncSummary, EnsV2RegistrarSyncSummary,
    EnsV2RegistryResourceSurfaceSyncSummary, EnsV2ResolverSyncSummary,
    sync_block_derived_normalized_events, sync_ens_v1_reverse_claim,
};
use bigname_manifests::load_repository;
use bigname_storage::{
    CanonicalityState, MIGRATOR, NormalizedEvent, RawBlock, RawLog,
    load_normalized_events_by_namespace, mark_block_derived_normalized_events_range_orphaned,
    mark_chain_lineage_range_orphaned, mark_identity_rows_range_orphaned,
    mark_raw_block_facts_range_orphaned, upsert_raw_blocks, upsert_raw_logs,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};
use std::path::Path;
use uuid::Uuid;

const RAW_EVENTS: &str = include_str!("fixtures/interpreters/raw-events.json");
const EXPECTED_OUTPUTS: &str = include_str!("fixtures/interpreters/expected-outputs.json");

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    id: String,
    runner: Runner,
    #[serde(default)]
    upstream_citations: Vec<String>,
    manifests: Vec<Manifest>,
    #[serde(default)]
    known_contracts: Vec<KnownContract>,
    blocks: Vec<Block>,
    logs: Vec<Log>,
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Runner {
    ReverseClaim,
    BlockDerived,
    UnwrappedAuthority,
    EnsV2Registry,
    EnsV2Permissions,
    EnsV2Resolver,
    EnsV2Registrar,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Step {
    Sync {
        id: String,
        block_hashes: Vec<String>,
    },
    Orphan {
        id: String,
        from_hash: String,
        stop_before_hash: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct Manifest {
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    file_path: String,
    declaration_name: String,
    role: String,
    address: String,
    contract_instance_id: Uuid,
    #[serde(default)]
    events: Vec<AbiEvent>,
}

#[derive(Debug, Deserialize)]
struct KnownContract {
    chain: String,
    address: String,
    contract_instance_id: Uuid,
    contract_kind: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct AbiEvent {
    name: String,
    fragment: String,
}

#[derive(Debug, Deserialize)]
struct Block {
    chain: String,
    hash: String,
    parent_hash: Option<String>,
    number: i64,
    timestamp: i64,
    #[serde(default)]
    finalized: bool,
}

#[derive(Debug, Deserialize)]
struct Log {
    chain: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    topics: Vec<String>,
    data: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OutputSuite {
    cases: Vec<CaseOutput>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CaseOutput {
    id: String,
    #[serde(flatten)]
    state: OutputState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    snapshots: Vec<SnapshotOutput>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SnapshotOutput {
    id: String,
    #[serde(flatten)]
    state: OutputState,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OutputState {
    normalized_events: Vec<NormalizedEvent>,
    name_surfaces: Value,
    surface_bindings: Value,
    resources: Value,
    token_lineages: Value,
    #[serde(default = "json_empty_array", skip_serializing_if = "empty_json_array")]
    discovery_edges: Value,
}

#[tokio::test]
async fn raw_event_interpreter_outputs_match_committed_expectations() -> Result<()> {
    let corpus: Corpus =
        serde_json::from_str(RAW_EVENTS).context("raw-event fixture is invalid")?;
    let expected: OutputSuite =
        serde_json::from_str(EXPECTED_OUTPUTS).context("expected-output fixture is invalid")?;
    let mut outputs = Vec::with_capacity(corpus.cases.len());

    for case in &corpus.cases {
        validate_upstream_citations(case)?;
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new(format!("bn_interpreter_{}", case.id)),
            &MIGRATOR,
            "failed to migrate interpreter fixture database",
        )
        .await?;
        let output = run_case(database.pool(), case).await;
        let cleanup = database.cleanup().await;
        cleanup?;
        outputs.push(output?);
    }

    let actual = OutputSuite { cases: outputs };
    validate_required_corpus_coverage(&actual)?;
    if std::env::var_os("BIGNAME_BLESS_INTERPRETER_FIXTURES").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/interpreters/expected-outputs.json");
        std::fs::write(
            &fixture_path,
            format!("{}\n", serde_json::to_string_pretty(&actual)?),
        )
        .with_context(|| {
            format!(
                "failed to bless interpreter expectations at {}",
                fixture_path.display()
            )
        })?;
        return Ok(());
    }
    if actual != expected {
        let differing_case = actual
            .cases
            .iter()
            .zip(&expected.cases)
            .find(|(actual, expected)| actual != expected);
        let (actual_case, expected_case) = differing_case.context(
            "interpreter corpus case count or ordering changed without a differing paired case",
        )?;
        bail!(
            "interpreter output changed for case {}; update the committed expectation with the \
             semantic change\nexpected:\n{}\nactual:\n{}",
            actual_case.id,
            serde_json::to_string_pretty(&expected_case)?,
            serde_json::to_string_pretty(&actual_case)?,
        );
    }
    Ok(())
}

async fn run_case(pool: &PgPool, case: &Case) -> Result<CaseOutput> {
    for manifest in &case.manifests {
        seed_manifest(pool, manifest).await?;
    }
    for contract in &case.known_contracts {
        seed_known_contract(pool, contract).await?;
    }

    let mut snapshots = Vec::new();
    if case.steps.is_empty() {
        let block_hashes = case
            .blocks
            .iter()
            .map(|block| block.hash.clone())
            .collect::<Vec<_>>();
        seed_raw_events(pool, case, &block_hashes).await?;
        run_interpreter(pool, case, &block_hashes).await?;
    } else {
        for step in &case.steps {
            match step {
                Step::Sync { id, block_hashes } => {
                    seed_raw_events(pool, case, block_hashes).await?;
                    run_interpreter(pool, case, block_hashes).await?;
                    snapshots.push(SnapshotOutput {
                        id: id.clone(),
                        state: load_output_state(pool, case).await?,
                    });
                }
                Step::Orphan {
                    id,
                    from_hash,
                    stop_before_hash,
                } => {
                    orphan_branch(
                        pool,
                        one_chain(case)?,
                        from_hash,
                        stop_before_hash.as_deref(),
                    )
                    .await?;
                    snapshots.push(SnapshotOutput {
                        id: id.clone(),
                        state: load_output_state(pool, case).await?,
                    });
                }
            }
        }
    }

    Ok(CaseOutput {
        id: case.id.clone(),
        state: load_output_state(pool, case).await?,
        snapshots,
    })
}

async fn run_interpreter(pool: &PgPool, case: &Case, block_hashes: &[String]) -> Result<()> {
    let chain = one_chain(case)?;
    match case.runner {
        Runner::ReverseClaim => {
            sync_ens_v1_reverse_claim(pool, chain).await?;
        }
        Runner::BlockDerived => {
            sync_block_derived_normalized_events(pool, chain, block_hashes, None).await?;
        }
        Runner::UnwrappedAuthority => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(pool, chain, block_hashes)
                .await?;
        }
        Runner::EnsV2Registry => {
            establish_fixture_raw_log_closure_proof(pool, chain).await?;
            EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_canonical_only(
                pool,
                chain,
                block_hashes,
            )
            .await?;
        }
        Runner::EnsV2Permissions => {
            EnsV2PermissionsSyncSummary::sync_for_block_hashes(pool, chain, block_hashes).await?;
        }
        Runner::EnsV2Resolver => {
            EnsV2ResolverSyncSummary::sync_for_block_hashes(pool, chain, block_hashes).await?;
        }
        Runner::EnsV2Registrar => {
            EnsV2RegistrarSyncSummary::sync_for_block_hashes(pool, chain, block_hashes).await?;
        }
    }
    Ok(())
}

async fn establish_fixture_raw_log_closure_proof(pool: &PgPool, chain: &str) -> Result<()> {
    // Corpus cases insert a complete, bounded raw-log history directly. Mark
    // that history complete so the production restricted replay path may
    // reconstruct the registry state before each selected log.
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, 0)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES ($1, 0, 0, true, NULL, 0, 0, 9223372036854775807)
        ON CONFLICT (chain_id) DO UPDATE
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation =
                raw_log_staging_input_revisions.retention_generation,
            proven_discovery_admission_epoch = (
                SELECT epoch
                FROM discovery_admission_epochs
                WHERE chain_id = $1
            ),
            proven_through_block = 9223372036854775807
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_known_contract(pool: &PgPool, contract: &KnownContract) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, $3, '{"fixture": "known_contract"}'::jsonb)
        "#,
    )
    .bind(contract.contract_instance_id)
    .bind(&contract.chain)
    .bind(&contract.contract_kind)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, provenance
        )
        VALUES ($1, $2, $3, '{"fixture": "known_contract"}'::jsonb)
        "#,
    )
    .bind(contract.contract_instance_id)
    .bind(&contract.chain)
    .bind(&contract.address)
    .execute(pool)
    .await?;
    Ok(())
}

async fn load_output_state(pool: &PgPool, case: &Case) -> Result<OutputState> {
    let namespace = case
        .manifests
        .first()
        .context("fixture case must declare a manifest")?
        .namespace
        .as_str();
    Ok(OutputState {
        normalized_events: load_normalized_events_by_namespace(pool, namespace).await?,
        name_surfaces: output_rows(pool, "name_surfaces", "logical_name_id").await?,
        surface_bindings: output_rows(pool, "surface_bindings", "surface_binding_id").await?,
        resources: output_rows(pool, "resources", "resource_id").await?,
        token_lineages: output_rows(pool, "token_lineages", "token_lineage_id").await?,
        discovery_edges: output_rows(pool, "discovery_edges", "discovery_edge_id").await?,
    })
}

fn one_chain(case: &Case) -> Result<&str> {
    let chain = case
        .manifests
        .first()
        .context("fixture case must declare a manifest")?
        .chain
        .as_str();
    if case
        .manifests
        .iter()
        .any(|manifest| manifest.chain != chain)
    {
        bail!("fixture case {} spans more than one chain", case.id);
    }
    Ok(chain)
}

async fn seed_manifest(pool: &PgPool, manifest: &Manifest) -> Result<()> {
    let events = if manifest.events.is_empty() {
        checked_in_manifest_events(&manifest.file_path)?
    } else {
        serde_json::to_value(&manifest.events)?
    };
    let registry_discovery = manifest.source_family == "ens_v2_registry_l1";
    let roots = if registry_discovery {
        serde_json::json!([{
            "name": "registry_root",
            "address": manifest.address,
        }])
    } else {
        serde_json::json!([])
    };
    let discovery_rules = if registry_discovery {
        serde_json::json!([
            {
                "edge_kind": "subregistry",
                "from_role": "registry",
                "admission": "reachable_from_root",
            },
            {
                "edge_kind": "resolver",
                "from_role": "registry",
                "admission": "reachable_from_root",
            },
        ])
    } else {
        serde_json::json!([])
    };
    let payload = serde_json::json!({
        "manifest_version": 1,
        "namespace": manifest.namespace,
        "source_family": manifest.source_family,
        "chain": manifest.chain,
        "deployment_epoch": manifest.deployment_epoch,
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "capability_flags": {},
        "roots": roots,
        "contracts": [],
        "discovery_rules": discovery_rules,
        "abi": { "events": events },
    });
    let manifest_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain, deployment_epoch,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, $1, $2, $3, $4, 'active', 'ensip15@ens-normalize-0.1.1', $5, $6)
        RETURNING manifest_id
        "#,
    )
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(&manifest.chain)
    .bind(&manifest.deployment_epoch)
    .bind(&manifest.file_path)
    .bind(payload)
    .fetch_one(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        "#,
    )
    .bind(manifest.contract_instance_id)
    .bind(&manifest.chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id, declaration_kind, declaration_name, contract_instance_id,
            declared_address, role, proxy_kind
        )
        VALUES ($1, 'contract', $2, $3, $4, $5, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(&manifest.declaration_name)
    .bind(manifest.contract_instance_id)
    .bind(&manifest.address)
    .bind(&manifest.role)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, $4, '{}'::jsonb)
        "#,
    )
    .bind(manifest.contract_instance_id)
    .bind(&manifest.chain)
    .bind(&manifest.address)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    if registry_discovery {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id, declaration_kind, declaration_name,
                contract_instance_id, declared_address
            )
            VALUES ($1, 'root', 'registry_root', $2, $3)
            "#,
        )
        .bind(manifest_id)
        .bind(manifest.contract_instance_id)
        .bind(&manifest.address)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO manifest_discovery_rules (
                manifest_id, edge_kind, from_role, admission
            )
            VALUES ($1, 'subregistry', 'registry', 'reachable_from_root'),
                   ($1, 'resolver', 'registry', 'reachable_from_root')
            "#,
        )
        .bind(manifest_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn checked_in_manifest_events(file_path: &str) -> Result<Value> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .context("adapters crate must be two directories below the workspace root")?;
    let manifest_relative_path = Path::new(file_path)
        .strip_prefix("manifests")
        .with_context(|| format!("fixture manifest path {file_path} must start with manifests/"))?;
    let manifests_root = workspace_root.join("manifests");
    let mut matches = Vec::new();

    for profile in ["mainnet", "sepolia"] {
        let profile_root = manifests_root.join(profile);
        let repository = load_repository(&profile_root)?;
        let path_below_profile = manifest_relative_path
            .strip_prefix(profile)
            .unwrap_or(manifest_relative_path);

        for loaded in repository.manifests().iter().filter(|loaded| {
            loaded.relative_path == path_below_profile
                || loaded.relative_path.ends_with(path_below_profile)
        }) {
            matches.push((
                profile_root.join(&loaded.relative_path),
                serde_json::to_value(&loaded.manifest.abi.events)
                    .context("failed to serialize checked-in fixture manifest events")?,
            ));
        }
    }

    match matches.as_slice() {
        [(_, events)] => Ok(events.clone()),
        [] => bail!(
            "fixture manifest {file_path} does not identify a checked-in mainnet or sepolia manifest"
        ),
        matches => bail!(
            "fixture manifest {file_path} is ambiguous; it matches {}",
            matches
                .iter()
                .map(|(path, _)| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

async fn seed_raw_events(pool: &PgPool, case: &Case, block_hashes: &[String]) -> Result<()> {
    let selected = block_hashes
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let blocks = case
        .blocks
        .iter()
        .filter(|block| selected.contains(block.hash.as_str()))
        .map(|block| {
            Ok(RawBlock {
                chain_id: block.chain.clone(),
                block_hash: block.hash.clone(),
                parent_hash: block.parent_hash.clone(),
                block_number: block.number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)
                    .context("fixture block timestamp is outside the supported range")?,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: if block.finalized {
                    CanonicalityState::Finalized
                } else {
                    CanonicalityState::Canonical
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let logs = case
        .logs
        .iter()
        .filter(|log| selected.contains(log.block_hash.as_str()))
        .map(|log| {
            Ok(RawLog {
                chain_id: log.chain.clone(),
                block_hash: log.block_hash.clone(),
                block_number: log.block_number,
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                emitting_address: log.emitting_address.clone(),
                topics: log.topics.clone(),
                data: alloy_primitives::hex::decode(log.data.trim_start_matches("0x"))
                    .context("fixture log data is not hexadecimal")?,
                canonicality_state: CanonicalityState::Canonical,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if blocks.len() != selected.len() {
        bail!(
            "fixture case {} sync step selected {} block hashes but resolved {} blocks",
            case.id,
            selected.len(),
            blocks.len()
        );
    }
    upsert_raw_blocks(pool, &blocks).await?;
    upsert_raw_logs(pool, &logs).await?;
    Ok(())
}

async fn orphan_branch(
    pool: &PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<()> {
    mark_chain_lineage_range_orphaned(pool, chain, from_hash, stop_before_hash).await?;
    mark_raw_block_facts_range_orphaned(pool, chain, from_hash, stop_before_hash).await?;
    mark_block_derived_normalized_events_range_orphaned(pool, chain, from_hash, stop_before_hash)
        .await?;
    mark_identity_rows_range_orphaned(pool, chain, from_hash, stop_before_hash).await?;
    Ok(())
}

async fn output_rows(pool: &PgPool, table: &str, order_column: &str) -> Result<Value> {
    let query = format!(
        "SELECT COALESCE( \
             jsonb_agg(to_jsonb(output_row) \
                 - 'observed_at' - 'inserted_at' - 'admitted_at' - 'deactivated_at' \
                 ORDER BY output_row.{order_column}), \
             '[]'::jsonb \
         ) \
         FROM {table} output_row"
    );
    sqlx::query_scalar(&query)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to read interpreter output table {table}"))
}

fn validate_upstream_citations(case: &Case) -> Result<()> {
    for citation in &case.upstream_citations {
        if !citation.starts_with("(upstream: .refs/")
            || !citation.contains(" @ ")
            || !citation.ends_with(')')
        {
            bail!(
                "fixture case {} has malformed pinned upstream citation {citation:?}",
                case.id
            );
        }
    }
    Ok(())
}

fn validate_required_corpus_coverage(suite: &OutputSuite) -> Result<()> {
    let registry = output_case(suite, "ens_v2_registry_subregistry_registration")?;
    for event_kind in [
        "RegistrationGranted",
        "TokenResourceLinked",
        "SubregistryChanged",
    ] {
        ensure!(
            has_event_kind(&registry.state, event_kind),
            "ENSv2 registry corpus case is missing {event_kind}"
        );
    }

    let permissions = output_case(suite, "ens_v2_permissions_grant_revoke")?;
    ensure!(
        permissions
            .state
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "PermissionChanged")
            .count()
            == 2,
        "ENSv2 permissions corpus case must retain its grant and revoke"
    );
    ensure!(
        has_event_kind(
            &output_case(suite, "ens_v2_resolver_text_record")?.state,
            "RecordChanged"
        ),
        "ENSv2 resolver corpus case must derive a record change"
    );
    ensure!(
        has_event_kind(
            &output_case(suite, "ens_v2_registrar_registration")?.state,
            "RegistrarNameRegistered"
        ),
        "ENSv2 registrar corpus case must derive a registration"
    );

    let renewal = output_case(suite, "ens_v1_registration_then_renewal")?;
    let renewal_state = snapshot(renewal, "after_renewal")?;
    let renewal_event = renewal_state
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationRenewed")
        .context("second-pass corpus case must derive RegistrationRenewed")?;
    ensure!(
        renewal_event.before_state["expiry"].as_i64() == Some(1_800_000_000)
            && renewal_event.after_state["expiry"].as_i64() == Some(1_900_000_000),
        "second-pass renewal must expose the diff from the existing expiry"
    );

    let reorg = output_case(suite, "ens_v1_registration_reorg_restore")?;
    let orphaned = snapshot(reorg, "after_orphan")?;
    ensure!(
        !orphaned.normalized_events.is_empty()
            && orphaned
                .normalized_events
                .iter()
                .all(|event| event.canonicality_state == CanonicalityState::Orphaned),
        "reorg corpus case must retain orphaned normalized output"
    );
    let restored = snapshot(reorg, "winning_branch_restored")?;
    ensure!(
        restored
            .normalized_events
            .iter()
            .any(|event| event.canonicality_state == CanonicalityState::Canonical)
            && json_rows_have_canonicality(&restored.resources, "canonical"),
        "reorg corpus case must restore canonical normalized and projection state"
    );

    let wrapper = output_case(suite, "ens_v1_wrapper_lifecycle")?;
    let wrapped = wrapper
        .state
        .normalized_events
        .iter()
        .position(|event| event.event_kind == "TokenControlTransferred")
        .context("wrapper lifecycle must derive the NameWrapped transition")?;
    let fuses = wrapper
        .state
        .normalized_events
        .iter()
        .position(|event| {
            event.event_kind == "PermissionScopeChanged"
                && event.before_state["fuses"].as_i64() == Some(0)
                && event.after_state["fuses"].as_i64() == Some(1)
        })
        .context("wrapper lifecycle must derive the FusesSet transition")?;
    let unwrapped = wrapper
        .state
        .normalized_events
        .iter()
        .position(|event| event.event_kind == "SurfaceUnbound")
        .context("wrapper lifecycle must derive the NameUnwrapped transition")?;
    ensure!(
        wrapped < fuses && fuses < unwrapped,
        "wrapper lifecycle transitions must remain ordered NameWrapped -> FusesSet -> NameUnwrapped"
    );
    Ok(())
}

fn output_case<'a>(suite: &'a OutputSuite, id: &str) -> Result<&'a CaseOutput> {
    suite
        .cases
        .iter()
        .find(|case| case.id == id)
        .with_context(|| format!("required interpreter corpus case {id} is absent"))
}

fn snapshot<'a>(case: &'a CaseOutput, id: &str) -> Result<&'a OutputState> {
    case.snapshots
        .iter()
        .find(|snapshot| snapshot.id == id)
        .map(|snapshot| &snapshot.state)
        .with_context(|| format!("required snapshot {}:{id} is absent", case.id))
}

fn has_event_kind(state: &OutputState, event_kind: &str) -> bool {
    state
        .normalized_events
        .iter()
        .any(|event| event.event_kind == event_kind)
}

fn json_rows_have_canonicality(rows: &Value, canonicality: &str) -> bool {
    rows.as_array().is_some_and(|rows| {
        rows.iter()
            .any(|row| row["canonicality_state"].as_str() == Some(canonicality))
    })
}

fn json_empty_array() -> Value {
    Value::Array(Vec::new())
}

fn empty_json_array(value: &Value) -> bool {
    value.as_array().is_some_and(Vec::is_empty)
}
