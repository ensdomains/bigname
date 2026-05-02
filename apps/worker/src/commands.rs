use anyhow::{Context, Result};
use tracing::info;

use crate::cli::*;
use crate::{
    address_names, automatic_projection_replay, children, execution, inspect, manifest_drift,
    name_current, permissions, primary_name, raw_facts, record_inventory, replay, resolver,
};

pub(crate) async fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::Run(args) => automatic_projection_replay::run_worker(args).await,
        Command::Migrate(args) => migrate(args).await,
        Command::AddressNamesCurrent(args) => address_names_current(args).await,
        Command::ChildrenCurrent(args) => children_current(args).await,
        Command::Execution(args) => execution_command(args).await,
        Command::Inspect(args) => inspect::inspect_command(args).await,
        Command::ManifestDrift(args) => manifest_drift_command(args).await,
        Command::NameCurrent(args) => name_current(args).await,
        Command::PermissionsCurrent(args) => permissions_current(args).await,
        Command::PrimaryNamesCurrent(args) => primary_names_current(args).await,
        Command::RawFacts(args) => raw_facts_command(args).await,
        Command::Replay(args) => replay_command(args).await,
        Command::RecordInventoryCurrent(args) => record_inventory_current(args).await,
        Command::ResolverCurrent(args) => resolver_current(args).await,
    }
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
        ManifestDriftCommand::Audit(args) => manifest_drift::audit(args).await,
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

async fn raw_facts_command(args: RawFactsArgs) -> Result<()> {
    match args.command {
        RawFactsCommand::CompactLogStaging(args) => raw_facts::compact_log_staging(args).await,
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
    let database =
        automatic_projection_replay::all_current_projections_database_config(args.database);
    let pool = bigname_storage::connect(&database).await?;
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
        total_requested_key_count = summary.total_requested_key_count(),
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
