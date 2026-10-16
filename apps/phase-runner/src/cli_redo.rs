//! Resolution of the one-shot `redo` command.
use super::*;

pub(super) fn resolve_redo(args: RedoArgs) -> RunnerResult<ResolvedCommand> {
    validate_heartbeat_threshold(args.heartbeat.heartbeat_stale_after_secs)?;
    let interpret_adaptive = args
        .adaptive
        .resolve(args.capacity.explicit_interpret_blocks_per_batch())?;
    let phase = parse_redo_phase(&args.phase)?;
    let range = BlockRange::new(args.from_block, args.to_block)?;
    let watch_set_coverage_attestations = watch_set_attestation::resolve(
        &args.attest_watch_set_coverage,
        args.all_chains,
        &args.chains,
    )?;
    let sources = args
        .sources
        .iter()
        .map(|source| parse_source(source))
        .collect::<RunnerResult<Vec<_>>>()?;
    if phase.requires_intake_sources() && sources.is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "ingest, interpret, project, verify, or all-phase redo requires at least one \
             intake-capable --source",
        ));
    }
    if phase.requires_verify() && args.verification_database_url.is_none() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "verify or all-phase redo requires --verification-database-url backed by a SELECT-only role",
        ));
    }
    let chains = if args.all_chains {
        RedoChains::All { sources }
    } else {
        RedoChains::Explicit(resolve_explicit_redo_chains(
            args.chains,
            sources,
            phase.requires_intake_sources(),
        )?)
    };
    if let RedoChains::Explicit(chains) = &chains {
        validate_deployment_table_set(chains, COMPILED_CHAIN_NAMESPACES.iter().copied())?;
    }
    Ok(ResolvedCommand::Redo {
        database_url: args.connection.database_url,
        verification_database_url: args.verification_database_url,
        metrics_bind_addr: args.metrics_bind_addr,
        heartbeat_stale_after_secs: args.heartbeat.heartbeat_stale_after_secs,
        manifests_root: args.manifests.manifests_root,
        instance_id: resolve_instance_id(args.connection.instance_id)?,
        chains,
        capacity: resolve_capacity(args.capacity)?,
        interpret_adaptive,
        timing: resolve_timing(args.timing)?,
        phase,
        range,
        watch_set_coverage_attestations,
        hydration_rpc_urls: resolve_hydration_rpc_urls(&args.hydration_rpc_urls)?,
        // The one-shot command does not retry a family failure of any kind: each retry would run
        // the served redo again, and a persistent failure such as Interpret being in redo would
        // never exit. The supervised runner, its required redos included, retries them all.
        // It also runs without the hydration RPC URL: a redo undoes and replays, which never
        // reads RPC, and the supervised runner refreshes the overlays it leaves empty.
        project_families: FamilySettings {
            retry_family_failures: false,
            require_hydration_url: false,
            ..args.project_families.into()
        },
    })
}
