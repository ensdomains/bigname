use super::RequiredIndexDescriptor;

pub(super) const ACTIVE_CONTRACT_ADDRESS_INDEX: &str =
    "contract_instance_addresses_active_lower_address_idx";
pub(super) const HISTORICAL_CONTRACT_ADDRESS_INDEX: &str =
    "contract_instance_addresses_historical_lower_address_idx";
pub(super) const NON_ORPHANED_RAW_CODE_LOWER_ADDRESS_INDEX: &str =
    "raw_code_hashes_non_orphaned_lower_address_idx";
pub(super) const EXECUTION_CACHE_OUTCOMES_REQUEST_LOOKUP_INDEX: &str =
    "execution_cache_outcomes_request_lookup_idx";
const PRIMARY_NAME_ROUTE_OUTCOME_RETENTION_INDEX: &str =
    "execution_cache_outcomes_route_primary_checkpoint_idx";
const PRIMARY_NAME_ROUTE_TRACE_RETENTION_INDEX: &str =
    "execution_traces_route_primary_checkpoint_idx";

pub(super) const REQUIRED_RUNTIME_INDEXES: &[RequiredIndexDescriptor] = &[
    RequiredIndexDescriptor {
        name: ACTIVE_CONTRACT_ADDRESS_INDEX,
        table: "public.contract_instance_addresses",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY contract_instance_addresses_active_lower_address_idx
            ON public.contract_instance_addresses (chain_id, LOWER(address))
            WHERE deactivated_at IS NULL
        "#,
    },
    RequiredIndexDescriptor {
        name: HISTORICAL_CONTRACT_ADDRESS_INDEX,
        table: "public.contract_instance_addresses",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY contract_instance_addresses_historical_lower_address_idx
            ON public.contract_instance_addresses (chain_id, LOWER(address))
            WHERE deactivated_at IS NOT NULL
              AND active_to_block_number IS NOT NULL
        "#,
    },
    RequiredIndexDescriptor {
        name: NON_ORPHANED_RAW_CODE_LOWER_ADDRESS_INDEX,
        table: "public.raw_code_hashes",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY raw_code_hashes_non_orphaned_lower_address_idx
            ON public.raw_code_hashes (chain_id, LOWER(contract_address))
            WHERE canonicality_state <> 'orphaned'::public.canonicality_state
        "#,
    },
    RequiredIndexDescriptor {
        name: EXECUTION_CACHE_OUTCOMES_REQUEST_LOOKUP_INDEX,
        table: "public.execution_cache_outcomes",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY IF NOT EXISTS execution_cache_outcomes_request_lookup_idx
            ON public.execution_cache_outcomes (request_type, namespace, request_key)
        "#,
    },
    RequiredIndexDescriptor {
        name: PRIMARY_NAME_ROUTE_OUTCOME_RETENTION_INDEX,
        table: "public.execution_cache_outcomes",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY IF NOT EXISTS execution_cache_outcomes_route_primary_checkpoint_idx
            ON public.execution_cache_outcomes (
                ((topology_version_boundary #>> '{chain_position,block_number}')::numeric),
                execution_cache_key
            )
            WHERE request_type = 'verified_primary_name'
              AND namespace = 'ens'
              AND topology_version_boundary ->> 'boundary_kind' = 'selected_checkpoint'
              AND record_version_boundary = topology_version_boundary
              AND topology_version_boundary #>> '{chain_position,chain_id}' = 'ethereum-mainnet'
              AND jsonb_typeof(
                    topology_version_boundary #> '{chain_position,block_number}'
                  ) = 'number'
        "#,
    },
    RequiredIndexDescriptor {
        name: PRIMARY_NAME_ROUTE_TRACE_RETENTION_INDEX,
        table: "public.execution_traces",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY IF NOT EXISTS execution_traces_route_primary_checkpoint_idx
            ON public.execution_traces (
                ((request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,block_number}')::numeric),
                execution_trace_id
            )
            WHERE request_type = 'verified_primary_name'
              AND namespace = 'ens'
              AND request_metadata ? 'route_local_claim'
              AND request_metadata ->> 'coin_type' = '60'
              AND request_metadata #>> '{cache_identity,topology_version_boundary,boundary_kind}' = 'selected_checkpoint'
              AND request_metadata #> '{cache_identity,record_version_boundary}'
                  = request_metadata #> '{cache_identity,topology_version_boundary}'
              AND request_metadata #>> '{cache_identity,topology_version_boundary,chain_position,chain_id}' = 'ethereum-mainnet'
              AND jsonb_typeof(
                    request_metadata #> '{cache_identity,topology_version_boundary,chain_position,block_number}'
                  ) = 'number'
        "#,
    },
];
