mod address_names;
mod children;
mod execution;
mod inspect;
mod name_current;
mod permissions;
mod primary_name;
mod record_inventory;
mod replay;
mod resolver;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, DatabaseConfig, ManifestDriftAlertInspection, ManifestDriftAlertKind,
    ManifestDriftAlertObservation,
};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};
use sqlx::types::time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "bigname-worker",
    about = "Bootstrap worker process for bigname"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
    Migrate(MigrateArgs),
    AddressNamesCurrent(AddressNamesCurrentArgs),
    ChildrenCurrent(ChildrenCurrentArgs),
    Execution(ExecutionArgs),
    Inspect(inspect::InspectArgs),
    ManifestDrift(ManifestDriftArgs),
    NameCurrent(NameCurrentArgs),
    PermissionsCurrent(PermissionsCurrentArgs),
    PrimaryNamesCurrent(PrimaryNamesCurrentArgs),
    Replay(ReplayArgs),
    RecordInventoryCurrent(RecordInventoryCurrentArgs),
    ResolverCurrent(ResolverCurrentArgs),
}

#[derive(Args, Debug)]
struct RunArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_WORKER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    poll_interval_secs: u64,
}

#[derive(Args, Debug)]
struct MigrateArgs {
    #[command(flatten)]
    database: DatabaseConfig,
}

#[derive(Args, Debug)]
struct NameCurrentArgs {
    #[command(subcommand)]
    command: NameCurrentCommand,
}

#[derive(Args, Debug)]
struct AddressNamesCurrentArgs {
    #[command(subcommand)]
    command: AddressNamesCurrentCommand,
}

#[derive(Args, Debug)]
struct ChildrenCurrentArgs {
    #[command(subcommand)]
    command: ChildrenCurrentCommand,
}

#[derive(Args, Debug)]
struct ExecutionArgs {
    #[command(subcommand)]
    command: ExecutionCommand,
}

#[derive(Args, Debug)]
struct ManifestDriftArgs {
    #[command(subcommand)]
    command: ManifestDriftCommand,
}

#[derive(Args, Debug)]
struct PermissionsCurrentArgs {
    #[command(subcommand)]
    command: PermissionsCurrentCommand,
}

#[derive(Args, Debug)]
struct PrimaryNamesCurrentArgs {
    #[command(subcommand)]
    command: PrimaryNamesCurrentCommand,
}

#[derive(Args, Debug)]
struct ReplayArgs {
    #[command(subcommand)]
    command: ReplayCommand,
}

#[derive(Args, Debug)]
struct RecordInventoryCurrentArgs {
    #[command(subcommand)]
    command: RecordInventoryCurrentCommand,
}

#[derive(Args, Debug)]
struct ResolverCurrentArgs {
    #[command(subcommand)]
    command: ResolverCurrentCommand,
}

#[derive(Subcommand, Debug)]
enum NameCurrentCommand {
    Rebuild(NameCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum AddressNamesCurrentCommand {
    Rebuild(AddressNamesCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum ChildrenCurrentCommand {
    Rebuild(ChildrenCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
#[allow(clippy::enum_variant_names)]
enum ExecutionCommand {
    InvalidateVerifiedResolutionManifest(InvalidateVerifiedResolutionManifestArgs),
    InvalidateVerifiedResolutionTopologyBoundary(InvalidateVerifiedResolutionBoundaryArgs),
    InvalidateVerifiedResolutionRecordBoundary(InvalidateVerifiedResolutionBoundaryArgs),
    InvalidateVerifiedPrimaryNameManifest(InvalidateVerifiedPrimaryNameManifestArgs),
    InvalidateVerifiedPrimaryNameTopologyBoundary(InvalidateVerifiedPrimaryNameBoundaryArgs),
    InvalidateVerifiedPrimaryNameRecordBoundary(InvalidateVerifiedPrimaryNameBoundaryArgs),
}

#[derive(Subcommand, Debug)]
enum ManifestDriftCommand {
    Audit(ManifestDriftAuditArgs),
}

#[derive(Subcommand, Debug)]
enum PermissionsCurrentCommand {
    Rebuild(PermissionsCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum PrimaryNamesCurrentCommand {
    Rebuild(PrimaryNamesCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum ReplayCommand {
    AllCurrentProjections(AllCurrentProjectionsArgs),
}

#[derive(Subcommand, Debug)]
enum RecordInventoryCurrentCommand {
    Rebuild(RecordInventoryCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
enum ResolverCurrentCommand {
    Rebuild(ResolverCurrentRebuildArgs),
}

#[derive(Args, Debug)]
struct NameCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    logical_name_id: Option<String>,
}

#[derive(Args, Debug)]
struct AddressNamesCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    address: Option<String>,
}

#[derive(Args, Debug)]
struct ChildrenCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    logical_name_id: Option<String>,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedResolutionManifestArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    source_manifest_id: Option<i64>,
    #[arg(long)]
    source_family: Option<String>,
    #[arg(long)]
    manifest_version: i64,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedResolutionBoundaryArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    logical_name_id: String,
    #[arg(long)]
    resource_id: Uuid,
    #[arg(long)]
    normalized_event_id: Option<i64>,
    #[arg(long)]
    event_kind: Option<String>,
    #[arg(long)]
    chain_id: String,
    #[arg(long)]
    block_number: i64,
    #[arg(long)]
    block_hash: String,
    #[arg(long)]
    timestamp: String,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedPrimaryNameManifestArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    address: String,
    #[arg(long)]
    coin_type: String,
    #[arg(long)]
    source_manifest_id: Option<i64>,
    #[arg(long)]
    source_family: Option<String>,
    #[arg(long)]
    manifest_version: i64,
}

#[derive(Args, Debug)]
struct InvalidateVerifiedPrimaryNameBoundaryArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    address: String,
    #[arg(long)]
    coin_type: String,
    #[arg(long)]
    logical_name_id: String,
    #[arg(long)]
    resource_id: Uuid,
    #[arg(long)]
    normalized_event_id: Option<i64>,
    #[arg(long)]
    event_kind: Option<String>,
    #[arg(long)]
    chain_id: String,
    #[arg(long)]
    block_number: i64,
    #[arg(long)]
    block_hash: String,
    #[arg(long)]
    timestamp: String,
}

#[derive(Args, Debug)]
struct ManifestDriftAuditArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    fail_on_alert: bool,
}

#[derive(Args, Debug)]
struct PermissionsCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    resource_id: Option<String>,
}

#[derive(Args, Debug)]
struct PrimaryNamesCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    namespace: Option<String>,
    #[arg(long)]
    coin_type: Option<String>,
}

#[derive(Args, Debug)]
struct AllCurrentProjectionsArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct RecordInventoryCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    resource_id: Option<String>,
}

#[derive(Args, Debug)]
struct ResolverCurrentRebuildArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(long)]
    chain_id: Option<String>,
    #[arg(long)]
    resolver_address: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing("bigname-worker", cli.writes_machine_json());

    match cli.command {
        Command::Run(args) => run(args).await,
        Command::Migrate(args) => migrate(args).await,
        Command::AddressNamesCurrent(args) => address_names_current(args).await,
        Command::ChildrenCurrent(args) => children_current(args).await,
        Command::Execution(args) => execution_command(args).await,
        Command::Inspect(args) => inspect::inspect_command(args).await,
        Command::ManifestDrift(args) => manifest_drift_command(args).await,
        Command::NameCurrent(args) => name_current(args).await,
        Command::PermissionsCurrent(args) => permissions_current(args).await,
        Command::PrimaryNamesCurrent(args) => primary_names_current(args).await,
        Command::Replay(args) => replay_command(args).await,
        Command::RecordInventoryCurrent(args) => record_inventory_current(args).await,
        Command::ResolverCurrent(args) => resolver_current(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    let _pool = bigname_storage::connect(&args.database).await?;

    info!(
        service = "worker",
        phase = bigname_domain::bootstrap_phase(),
        execution_status = bigname_execution::bootstrap_status(),
        poll_interval_secs = args.poll_interval_secs,
        "worker booted"
    );

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")?;
    info!(service = "worker", "shutdown signal received");
    Ok(())
}

async fn migrate(args: MigrateArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    bigname_storage::migrate(&pool).await?;
    info!(service = "worker", "database migrations applied");
    Ok(())
}

async fn name_current(args: NameCurrentArgs) -> Result<()> {
    match args.command {
        NameCurrentCommand::Rebuild(args) => rebuild_name_current(args).await,
    }
}

async fn address_names_current(args: AddressNamesCurrentArgs) -> Result<()> {
    match args.command {
        AddressNamesCurrentCommand::Rebuild(args) => rebuild_address_names_current(args).await,
    }
}

async fn children_current(args: ChildrenCurrentArgs) -> Result<()> {
    match args.command {
        ChildrenCurrentCommand::Rebuild(args) => rebuild_children_current(args).await,
    }
}

async fn execution_command(args: ExecutionArgs) -> Result<()> {
    match args.command {
        ExecutionCommand::InvalidateVerifiedResolutionManifest(args) => {
            invalidate_verified_resolution_manifest(args).await
        }
        ExecutionCommand::InvalidateVerifiedResolutionTopologyBoundary(args) => {
            invalidate_verified_resolution_topology_boundary(args).await
        }
        ExecutionCommand::InvalidateVerifiedResolutionRecordBoundary(args) => {
            invalidate_verified_resolution_record_boundary(args).await
        }
        ExecutionCommand::InvalidateVerifiedPrimaryNameManifest(args) => {
            invalidate_verified_primary_name_manifest(args).await
        }
        ExecutionCommand::InvalidateVerifiedPrimaryNameTopologyBoundary(args) => {
            invalidate_verified_primary_name_topology_boundary(args).await
        }
        ExecutionCommand::InvalidateVerifiedPrimaryNameRecordBoundary(args) => {
            invalidate_verified_primary_name_record_boundary(args).await
        }
    }
}

async fn manifest_drift_command(args: ManifestDriftArgs) -> Result<()> {
    match args.command {
        ManifestDriftCommand::Audit(args) => manifest_drift_audit(args).await,
    }
}

async fn permissions_current(args: PermissionsCurrentArgs) -> Result<()> {
    match args.command {
        PermissionsCurrentCommand::Rebuild(args) => rebuild_permissions_current(args).await,
    }
}

async fn record_inventory_current(args: RecordInventoryCurrentArgs) -> Result<()> {
    match args.command {
        RecordInventoryCurrentCommand::Rebuild(args) => {
            rebuild_record_inventory_current(args).await
        }
    }
}

async fn primary_names_current(args: PrimaryNamesCurrentArgs) -> Result<()> {
    match args.command {
        PrimaryNamesCurrentCommand::Rebuild(args) => rebuild_primary_names_current(args).await,
    }
}

async fn replay_command(args: ReplayArgs) -> Result<()> {
    match args.command {
        ReplayCommand::AllCurrentProjections(args) => replay_all_current_projections(args).await,
    }
}

async fn resolver_current(args: ResolverCurrentArgs) -> Result<()> {
    match args.command {
        ResolverCurrentCommand::Rebuild(args) => rebuild_resolver_current(args).await,
    }
}

async fn manifest_drift_audit(args: ManifestDriftAuditArgs) -> Result<()> {
    let _emit_json = args.json;
    let pool = bigname_storage::connect(&args.database).await?;
    let live_audit =
        bigname_storage::ManifestDriftAlertInspection::compute_live_manifest_drift_audit(&pool)
            .await?;
    let persisted = persist_manifest_drift_audit_observations(&pool, &live_audit).await?;
    let audit = render_manifest_drift_audit(&live_audit, &persisted);

    println!("{audit}");
    enforce_manifest_drift_audit_exit_policy(&persisted, args.fail_on_alert)?;
    Ok(())
}

async fn persist_manifest_drift_audit_observations(
    pool: &sqlx::PgPool,
    audit: &Value,
) -> Result<ManifestDriftAlertInspection> {
    for candidate in audit_alert_array(audit, "manifest_code_hash_drift_alerts")? {
        let observation = manifest_code_hash_drift_candidate_observation(candidate)?;
        ManifestDriftAlertInspection::persist_manifest_drift_alert_observation(pool, &observation)
            .await?;
    }

    for candidate in audit_alert_array(audit, "proxy_implementation_alerts")? {
        let observation = manifest_proxy_implementation_candidate_observation(
            candidate,
            manifest_drift_observed_at(),
        )?;
        ManifestDriftAlertInspection::persist_manifest_drift_alert_observation(pool, &observation)
            .await?;
    }

    bigname_storage::list_manifest_drift_alert_observations(pool).await
}

fn render_manifest_drift_audit(
    live_audit: &Value,
    persisted: &ManifestDriftAlertInspection,
) -> Value {
    let mut rendered =
        inspect::render_manifest_drift_alert_observations("manifest-drift audit", false, persisted);
    if let Some(object) = rendered.as_object_mut() {
        object.insert(
            "persistence".to_owned(),
            json!({
                "writes_normalized_events": false,
                "writes_alert_table": true,
                "mutates_manifest_truth": false,
                "mutates_discovery_edges": false,
                "mutates_watch_plan": false,
            }),
        );
        object.insert(
            "live_candidate_counts".to_owned(),
            live_audit
                .get("counts")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        object.insert(
            "actionable_persisted_alert_count".to_owned(),
            json!(manifest_drift_actionable_alert_count(persisted)),
        );
    }

    rendered
}

fn enforce_manifest_drift_audit_exit_policy(
    inspection: &ManifestDriftAlertInspection,
    fail_on_alert: bool,
) -> Result<()> {
    if !fail_on_alert {
        return Ok(());
    }

    let alert_count = manifest_drift_actionable_alert_count(inspection);
    if alert_count > 0 {
        bail!("manifest drift audit found {alert_count} actionable persisted alert(s)");
    }

    Ok(())
}

fn manifest_drift_actionable_alert_count(inspection: &ManifestDriftAlertInspection) -> usize {
    inspection
        .code_hash_drift_alerts
        .iter()
        .chain(inspection.proxy_implementation_alerts.iter())
        .filter(|alert| manifest_drift_alert_is_actionable(alert))
        .count()
}

fn manifest_drift_alert_is_actionable(alert: &ManifestDriftAlertObservation) -> bool {
    !matches!(
        alert
            .alert_state
            .get("alert_status")
            .and_then(Value::as_str),
        Some("dismissed" | "remediated")
    )
}

fn audit_alert_array<'a>(audit: &'a Value, field: &str) -> Result<&'a [Value]> {
    audit
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .with_context(|| format!("manifest drift audit JSON is missing {field}"))
}

fn manifest_code_hash_drift_candidate_observation(
    candidate: &Value,
) -> Result<ManifestDriftAlertObservation> {
    let declaration = required_object(candidate, "declaration")?;
    let contract = required_object(candidate, "contract")?;
    let code_hash = required_object(candidate, "code_hash")?;
    let observed_block = required_object(candidate, "observed_block")?;
    let watched_target = required_object(candidate, "watched_target")?;
    let timestamps = required_object(candidate, "timestamps")?;
    let chain = required_string(candidate, "chain")?;
    let source_family = required_string(candidate, "source_family")?;
    let source_manifest_id = required_i64(candidate, "source_manifest_id")?;
    let block_number = required_i64(observed_block, "number")?;
    let block_hash = required_string(observed_block, "hash")?;
    let canonicality_state =
        parse_manifest_drift_canonicality(required_string(observed_block, "canonicality_state")?)?;
    let raw_fact_ref = required_value(watched_target, "raw_fact_ref")?.clone();
    ensure_json_object(&raw_fact_ref, "manifest drift code-hash raw_fact_ref")?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: 0,
        event_identity: required_string(candidate, "candidate_identity")?.to_owned(),
        alert_kind: ManifestDriftAlertKind::CodeHashDrift,
        namespace: required_string(candidate, "namespace")?.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: required_i64(candidate, "manifest_version")?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        raw_fact_ref,
        canonicality_state,
        alert_state: json!({
            "alert_status": "active",
            "declaration_kind": required_string(declaration, "kind")?,
            "declaration_name": required_string(declaration, "name")?,
            "contract_instance_id": required_string(contract, "contract_instance_id")?,
            "address": required_string(contract, "address")?,
            "expected_code_hash": required_string(code_hash, "expected")?,
            "observed_code_hash": required_string(code_hash, "observed")?,
            "observed_code_byte_length": required_i64(code_hash, "observed_byte_length")?,
            "observed_block_number": block_number,
            "observed_block_hash": block_hash,
            "observed_canonicality_state": canonicality_state.as_str(),
            "watched_source": required_string(watched_target, "source")?,
            "source_manifest_id": source_manifest_id,
        }),
        observed_at: parse_manifest_drift_timestamp(required_string(timestamps, "observed_at")?)?,
    })
}

fn manifest_proxy_implementation_candidate_observation(
    candidate: &Value,
    observed_at: OffsetDateTime,
) -> Result<ManifestDriftAlertObservation> {
    let declaration = required_object(candidate, "declaration")?;
    let proxy = required_object(candidate, "proxy")?;
    let expected = required_object(candidate, "expected_implementation")?;
    let observed = required_object(candidate, "observed_implementation")?;
    let implementation_edge = required_object(candidate, "implementation_edge")?;
    let chain = required_string(candidate, "chain")?;
    let source_family = required_string(candidate, "source_family")?;
    let source_manifest_id = required_i64(candidate, "source_manifest_id")?;
    let discovery_edge_id = optional_i64(implementation_edge, "discovery_edge_id")?;
    let observed_implementation_contract_instance_id =
        optional_string(observed, "contract_instance_id")?;
    let observed_implementation_address = optional_string(observed, "address")?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: 0,
        event_identity: required_string(candidate, "candidate_identity")?.to_owned(),
        alert_kind: ManifestDriftAlertKind::ProxyImplementation,
        namespace: required_string(candidate, "namespace")?.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: required_i64(candidate, "manifest_version")?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.to_owned()),
        block_number: None,
        block_hash: None,
        raw_fact_ref: json!({
            "manifest_id": source_manifest_id,
            "discovery_edge_id": discovery_edge_id,
            "proxy_contract_instance_id": required_string(proxy, "contract_instance_id")?,
            "expected_implementation_contract_instance_id": required_string(expected, "contract_instance_id")?,
            "observed_implementation_contract_instance_id": observed_implementation_contract_instance_id,
        }),
        canonicality_state: CanonicalityState::Observed,
        alert_state: json!({
            "alert_status": "active",
            "candidate_reason": required_string(candidate, "candidate_reason")?,
            "declaration_name": required_string(declaration, "name")?,
            "role": optional_string(declaration, "role")?,
            "proxy_kind": optional_string(declaration, "proxy_kind")?,
            "proxy_contract_instance_id": required_string(proxy, "contract_instance_id")?,
            "proxy_address": required_string(proxy, "address")?,
            "expected_implementation_contract_instance_id": required_string(expected, "contract_instance_id")?,
            "expected_implementation_address": optional_string(expected, "address")?,
            "observed_implementation_contract_instance_id": observed_implementation_contract_instance_id,
            "implementation_contract_instance_id": observed_implementation_contract_instance_id,
            "implementation_address": observed_implementation_address,
            "discovery_edge_id": discovery_edge_id,
            "admission": optional_string(implementation_edge, "admission")?,
            "active_from_block_number": optional_i64(implementation_edge, "active_from_block_number")?,
            "active_to_block_number": optional_i64(implementation_edge, "active_to_block_number")?,
            "provenance": required_value(implementation_edge, "provenance")?.clone(),
            "source_manifest_id": source_manifest_id,
        }),
        observed_at,
    })
}

fn required_value<'a>(object: &'a Value, field: &str) -> Result<&'a Value> {
    object
        .get(field)
        .with_context(|| format!("manifest drift candidate is missing {field}"))
}

fn required_object<'a>(object: &'a Value, field: &str) -> Result<&'a Value> {
    let value = required_value(object, field)?;
    ensure_json_object(value, field)?;
    Ok(value)
}

fn required_string<'a>(object: &'a Value, field: &str) -> Result<&'a str> {
    required_value(object, field)?
        .as_str()
        .with_context(|| format!("manifest drift candidate {field} must be a string"))
}

fn optional_string<'a>(object: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match object.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .with_context(|| format!("manifest drift candidate {field} must be a string or null")),
    }
}

fn required_i64(object: &Value, field: &str) -> Result<i64> {
    required_value(object, field)?
        .as_i64()
        .with_context(|| format!("manifest drift candidate {field} must be an integer"))
}

fn optional_i64(object: &Value, field: &str) -> Result<Option<i64>> {
    match object.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value.as_i64().map(Some).with_context(|| {
            format!("manifest drift candidate {field} must be an integer or null")
        }),
    }
}

fn ensure_json_object(value: &Value, context: &str) -> Result<()> {
    if !value.is_object() {
        bail!("{context} must be a JSON object");
    }
    Ok(())
}

fn parse_manifest_drift_canonicality(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown manifest drift canonicality_state value {value}"),
    }
}

fn parse_manifest_drift_timestamp(value: &str) -> Result<OffsetDateTime> {
    if value.len() != 20 || !value.ends_with('Z') {
        bail!("manifest drift timestamp {value} must use YYYY-MM-DDTHH:MM:SSZ");
    }

    let year = value[0..4]
        .parse::<i32>()
        .with_context(|| format!("invalid manifest drift timestamp year in {value}"))?;
    let month = value[5..7]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp month in {value}"))?;
    let day = value[8..10]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp day in {value}"))?;
    let hour = value[11..13]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp hour in {value}"))?;
    let minute = value[14..16]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp minute in {value}"))?;
    let second = value[17..19]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp second in {value}"))?;
    if &value[4..5] != "-"
        || &value[7..8] != "-"
        || &value[10..11] != "T"
        || &value[13..14] != ":"
        || &value[16..17] != ":"
    {
        bail!("manifest drift timestamp {value} must use YYYY-MM-DDTHH:MM:SSZ");
    }

    let date = Date::from_ordinal_date(year, ordinal_day(year, month, day)?)
        .with_context(|| format!("invalid manifest drift timestamp date in {value}"))?;
    let time = Time::from_hms(hour, minute, second)
        .with_context(|| format!("invalid manifest drift timestamp time in {value}"))?;

    Ok(PrimitiveDateTime::new(date, time).assume_offset(UtcOffset::UTC))
}

fn manifest_drift_observed_at() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn ordinal_day(year: i32, month: u8, day: u8) -> Result<u16> {
    let leap_adjusted_days = [
        31_u16,
        if is_leap_year(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let month_index = usize::from(
        month
            .checked_sub(1)
            .context("manifest drift timestamp month must be in 1..=12")?,
    );
    let month_days = *leap_adjusted_days
        .get(month_index)
        .context("manifest drift timestamp month must be in 1..=12")?;
    if day == 0 || u16::from(day) > month_days {
        bail!("manifest drift timestamp day {day} is invalid for month {month}");
    }

    Ok(leap_adjusted_days[..month_index].iter().sum::<u16>() + u16::from(day))
}

const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

async fn rebuild_name_current(args: NameCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        name_current::rebuild_name_current(&pool, args.logical_name_id.as_deref()).await?;

    info!(
        service = "worker",
        projection = "name_current",
        requested_name_count = summary.requested_name_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        logical_name_id = args.logical_name_id.as_deref().unwrap_or("all"),
        "name_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_address_names_current(args: AddressNamesCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        address_names::rebuild_address_names_current(&pool, args.address.as_deref()).await?;

    info!(
        service = "worker",
        projection = "address_names_current",
        requested_address_count = summary.requested_address_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        address = args.address.as_deref().unwrap_or("all"),
        "address_names_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_children_current(args: ChildrenCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        children::rebuild_children_current(&pool, args.logical_name_id.as_deref()).await?;

    info!(
        service = "worker",
        projection = "children_current",
        requested_parent_count = summary.requested_parent_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        logical_name_id = args.logical_name_id.as_deref().unwrap_or("all"),
        "children_current rebuild completed"
    );

    Ok(())
}

async fn invalidate_verified_resolution_manifest(
    args: InvalidateVerifiedResolutionManifestArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = execution::invalidate_verified_resolution_manifest_version(
        &pool,
        &execution::VerifiedResolutionManifestInvalidation {
            namespace: args.namespace.clone(),
            source_manifest_id: args.source_manifest_id,
            source_family: args.source_family.clone(),
            manifest_version: args.manifest_version,
        },
    )
    .await?;

    info!(
        service = "worker",
        execution_request_type = "verified_resolution",
        invalidation_cause = "manifest_version",
        namespace = args.namespace.as_str(),
        manifest_version = args.manifest_version,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_resolution execution outcome invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_resolution_topology_boundary(
    args: InvalidateVerifiedResolutionBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedResolutionBoundaryInvalidation {
        namespace: args.namespace.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_resolution_topology_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_resolution",
        invalidation_cause = "topology_boundary",
        namespace = args.namespace.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_resolution topology invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_resolution_record_boundary(
    args: InvalidateVerifiedResolutionBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedResolutionBoundaryInvalidation {
        namespace: args.namespace.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_resolution_record_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_resolution",
        invalidation_cause = "record_boundary",
        namespace = args.namespace.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_resolution record invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_primary_name_manifest(
    args: InvalidateVerifiedPrimaryNameManifestArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = execution::invalidate_verified_primary_name_manifest_version(
        &pool,
        &execution::VerifiedPrimaryNameManifestInvalidation {
            namespace: args.namespace.clone(),
            address: args.address.clone(),
            coin_type: args.coin_type.clone(),
            source_manifest_id: args.source_manifest_id,
            source_family: args.source_family.clone(),
            manifest_version: args.manifest_version,
        },
    )
    .await?;

    info!(
        service = "worker",
        execution_request_type = "verified_primary_name",
        invalidation_cause = "manifest_version",
        namespace = args.namespace.as_str(),
        address = args.address.as_str(),
        coin_type = args.coin_type.as_str(),
        manifest_version = args.manifest_version,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_primary_name execution outcome invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_primary_name_topology_boundary(
    args: InvalidateVerifiedPrimaryNameBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedPrimaryNameBoundaryInvalidation {
        namespace: args.namespace.clone(),
        address: args.address.clone(),
        coin_type: args.coin_type.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_primary_name_topology_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_primary_name",
        invalidation_cause = "topology_boundary",
        namespace = args.namespace.as_str(),
        address = args.address.as_str(),
        coin_type = args.coin_type.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_primary_name topology invalidation completed"
    );

    Ok(())
}

async fn invalidate_verified_primary_name_record_boundary(
    args: InvalidateVerifiedPrimaryNameBoundaryArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let invalidation = execution::VerifiedPrimaryNameBoundaryInvalidation {
        namespace: args.namespace.clone(),
        address: args.address.clone(),
        coin_type: args.coin_type.clone(),
        logical_name_id: args.logical_name_id.clone(),
        resource_id: args.resource_id,
        normalized_event_id: args.normalized_event_id,
        event_kind: args.event_kind.clone(),
        chain_id: args.chain_id.clone(),
        block_number: args.block_number,
        block_hash: args.block_hash.clone(),
        timestamp: args.timestamp.clone(),
    };
    let summary =
        execution::invalidate_verified_primary_name_record_boundary(&pool, &invalidation).await?;

    info!(
        service = "worker",
        execution_request_type = "verified_primary_name",
        invalidation_cause = "record_boundary",
        namespace = args.namespace.as_str(),
        address = args.address.as_str(),
        coin_type = args.coin_type.as_str(),
        logical_name_id = args.logical_name_id.as_str(),
        resource_id = %args.resource_id,
        deleted_outcome_count = summary.deleted_outcome_count,
        "verified_primary_name record invalidation completed"
    );

    Ok(())
}

async fn rebuild_permissions_current(args: PermissionsCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        permissions::rebuild_permissions_current(&pool, args.resource_id.as_deref()).await?;

    info!(
        service = "worker",
        projection = "permissions_current",
        requested_resource_count = summary.requested_resource_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        resource_id = args.resource_id.as_deref().unwrap_or("all"),
        "permissions_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_primary_names_current(args: PrimaryNamesCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = primary_name::rebuild_primary_names_current(
        &pool,
        args.address.as_deref(),
        args.namespace.as_deref(),
        args.coin_type.as_deref(),
    )
    .await?;

    info!(
        service = "worker",
        projection = "primary_names_current",
        requested_tuple_count = summary.requested_tuple_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        success_row_count = summary.success_row_count,
        not_found_row_count = summary.not_found_row_count,
        invalid_name_row_count = summary.invalid_name_row_count,
        address = args.address.as_deref().unwrap_or("all"),
        namespace = args.namespace.as_deref().unwrap_or("all"),
        coin_type = args.coin_type.as_deref().unwrap_or("all"),
        "primary_names_current rebuild completed"
    );

    Ok(())
}

async fn replay_all_current_projections(args: AllCurrentProjectionsArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = replay::rebuild_all_current_projections(&pool).await?;

    if args.json {
        let payload = summary
            .json_summary_string()
            .context("failed to serialize all-current-projections replay JSON summary")?;
        println!("{payload}");
        return Ok(());
    }

    info!(
        service = "worker",
        replay = "all_current_projections",
        projection_order = ?summary.projection_order(),
        projection_count = summary.steps.len(),
        total_upserted_row_count = summary.total_upserted_row_count(),
        total_deleted_row_count = summary.total_deleted_row_count(),
        "all current projections replay completed"
    );

    Ok(())
}

async fn rebuild_record_inventory_current(args: RecordInventoryCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary =
        record_inventory::rebuild_record_inventory_current(&pool, args.resource_id.as_deref())
            .await?;

    info!(
        service = "worker",
        projection = "record_inventory_current",
        requested_resource_count = summary.requested_resource_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        resource_id = args.resource_id.as_deref().unwrap_or("all"),
        "record_inventory_current rebuild completed"
    );

    Ok(())
}

async fn rebuild_resolver_current(args: ResolverCurrentRebuildArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let summary = resolver::rebuild_resolver_current(
        &pool,
        args.chain_id.as_deref(),
        args.resolver_address.as_deref(),
    )
    .await?;

    info!(
        service = "worker",
        projection = "resolver_current",
        requested_resolver_count = summary.requested_resolver_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        chain_id = args.chain_id.as_deref().unwrap_or("all"),
        resolver_address = args.resolver_address.as_deref().unwrap_or("all"),
        "resolver_current rebuild completed"
    );

    Ok(())
}

impl Cli {
    fn writes_machine_json(&self) -> bool {
        matches!(
            &self.command,
            Command::Inspect(_)
                | Command::ManifestDrift(ManifestDriftArgs {
                    command: ManifestDriftCommand::Audit(ManifestDriftAuditArgs { json: true, .. })
                })
                | Command::Replay(ReplayArgs {
                    command: ReplayCommand::AllCurrentProjections(AllCurrentProjectionsArgs {
                        json: true,
                        ..
                    })
                })
        )
    }
}

fn init_tracing(service: &'static str, emit_logs_to_stderr: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    } else {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_manifest_drift_observed_at() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000)
            .expect("fixed manifest drift timestamp must be valid")
    }

    fn manifest_drift_alert(status: &str) -> ManifestDriftAlertObservation {
        ManifestDriftAlertObservation {
            normalized_event_id: 1,
            event_identity: format!("manifest_alert:{status}"),
            alert_kind: ManifestDriftAlertKind::CodeHashDrift,
            namespace: "ens".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("eth-mainnet".to_owned()),
            block_number: Some(1),
            block_hash: Some("0x01".to_owned()),
            raw_fact_ref: json!({}),
            canonicality_state: CanonicalityState::Canonical,
            alert_state: json!({
                "alert_status": status,
            }),
            observed_at: fixed_manifest_drift_observed_at(),
        }
    }

    #[test]
    fn replay_all_current_projections_cli_is_available() {
        let cli = Cli::parse_from(["bigname-worker", "replay", "all-current-projections"]);
        assert!(!cli.writes_machine_json());

        match cli.command {
            Command::Replay(args) => match args.command {
                ReplayCommand::AllCurrentProjections(args) => {
                    assert!(!args.json);
                }
            },
            other => panic!("expected replay command, got {other:?}"),
        }
    }

    #[test]
    fn replay_all_current_projections_json_flag_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "replay",
            "all-current-projections",
            "--json",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Replay(args) => match args.command {
                ReplayCommand::AllCurrentProjections(args) => {
                    assert!(args.json);
                }
            },
            other => panic!("expected replay command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_canonicality_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "canonicality",
            "--chain-id",
            "eth-mainnet",
            "--block-hash",
            "0xabc",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::Canonicality(args) => {
                    assert_eq!(args.chain_id, "eth-mainnet");
                    assert_eq!(args.block_hash, "0xabc");
                }
                other => panic!("expected canonicality inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_backfill_job_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "backfill-job",
            "--backfill-job-id",
            "42",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::BackfillJob(args) => {
                    assert_eq!(args.backfill_job_id, 42);
                }
                other => panic!("expected backfill job inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_execution_trace_cli_is_available() {
        let trace_id = "0e7ec7ac-e000-0000-0000-000000000abc";
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "execution-trace",
            "--execution-trace-id",
            trace_id,
            "--json",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::ExecutionTrace(args) => {
                    assert_eq!(args.execution_trace_id.to_string(), trace_id);
                    assert!(args.json);
                }
                other => panic!("expected execution trace inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_manifest_drift_cli_is_available() {
        let cli = Cli::parse_from(["bigname-worker", "inspect", "manifest-drift", "--json"]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::ManifestDrift(args) => {
                    assert!(args.json);
                }
                other => panic!("expected manifest drift inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn manifest_drift_audit_cli_is_available() {
        let cli = Cli::parse_from(["bigname-worker", "manifest-drift", "audit", "--json"]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::ManifestDrift(args) => match args.command {
                ManifestDriftCommand::Audit(args) => {
                    assert!(args.json);
                    assert!(!args.fail_on_alert);
                }
            },
            other => panic!("expected manifest drift command, got {other:?}"),
        }
    }

    #[test]
    fn manifest_drift_audit_fail_on_alert_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "manifest-drift",
            "audit",
            "--json",
            "--fail-on-alert",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::ManifestDrift(args) => match args.command {
                ManifestDriftCommand::Audit(args) => {
                    assert!(args.json);
                    assert!(args.fail_on_alert);
                }
            },
            other => panic!("expected manifest drift command, got {other:?}"),
        }
    }

    #[test]
    fn manifest_drift_audit_exit_policy_fails_only_when_requested() -> Result<()> {
        let clean_inspection = ManifestDriftAlertInspection::default();
        let drift_inspection = ManifestDriftAlertInspection {
            code_hash_drift_alerts: vec![manifest_drift_alert("active")],
            proxy_implementation_alerts: vec![manifest_drift_alert("acknowledged")],
        };

        enforce_manifest_drift_audit_exit_policy(&clean_inspection, true)?;
        enforce_manifest_drift_audit_exit_policy(&drift_inspection, false)?;
        let error = enforce_manifest_drift_audit_exit_policy(&drift_inspection, true)
            .expect_err("fail-on-alert must fail when audit contains actionable alerts");
        assert!(
            error
                .to_string()
                .contains("manifest drift audit found 2 actionable persisted alert(s)"),
            "unexpected error: {error:#}"
        );

        Ok(())
    }

    #[test]
    fn manifest_drift_audit_exit_policy_ignores_inactive_persisted_alerts() -> Result<()> {
        let inspection = ManifestDriftAlertInspection {
            code_hash_drift_alerts: vec![manifest_drift_alert("remediated")],
            proxy_implementation_alerts: vec![manifest_drift_alert("dismissed")],
        };

        enforce_manifest_drift_audit_exit_policy(&inspection, true)
    }

    #[test]
    fn manifest_proxy_implementation_observation_uses_supplied_timestamp() -> Result<()> {
        let observed_at = fixed_manifest_drift_observed_at();
        let candidate = json!({
            "candidate_identity": "manifest_alert:proxy:test",
            "candidate_reason": "implementation_mismatch",
            "namespace": "ens",
            "source_family": "ens_v1_registry_l1",
            "manifest_version": 7,
            "source_manifest_id": 11,
            "chain": "eth-mainnet",
            "declaration": {
                "name": "RegistryProxy",
                "role": "registry",
                "proxy_kind": "eip1967"
            },
            "proxy": {
                "contract_instance_id": "contract:proxy",
                "address": "0x0000000000000000000000000000000000000001"
            },
            "expected_implementation": {
                "contract_instance_id": "contract:expected",
                "address": "0x0000000000000000000000000000000000000002"
            },
            "observed_implementation": {
                "contract_instance_id": "contract:observed",
                "address": "0x0000000000000000000000000000000000000003"
            },
            "implementation_edge": {
                "discovery_edge_id": 19,
                "admission": "active",
                "active_from_block_number": 100,
                "active_to_block_number": null,
                "provenance": {
                    "source": "test"
                }
            }
        });

        let observation =
            manifest_proxy_implementation_candidate_observation(&candidate, observed_at)?;

        assert_eq!(observation.observed_at, observed_at);

        Ok(())
    }

    #[test]
    fn inspect_watch_plan_cli_is_available() {
        let cli = Cli::parse_from(["bigname-worker", "inspect", "watch-plan", "--json"]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::WatchPlan(args) => {
                    assert!(args.json);
                }
                other => panic!("expected watch plan inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }

    #[test]
    fn inspect_stored_lineage_range_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-worker",
            "inspect",
            "stored-lineage-range",
            "--chain-id",
            "eth-mainnet",
            "--range-start-block-number",
            "10",
            "--range-end-block-number",
            "12",
        ]);
        assert!(cli.writes_machine_json());

        match cli.command {
            Command::Inspect(args) => match args.command {
                inspect::InspectCommand::StoredLineageRange(args) => {
                    assert_eq!(args.chain_id, "eth-mainnet");
                    assert_eq!(args.range_start_block_number, 10);
                    assert_eq!(args.range_end_block_number, 12);
                }
                other => panic!("expected stored lineage range inspect command, got {other:?}"),
            },
            other => panic!("expected inspect command, got {other:?}"),
        }
    }
}
