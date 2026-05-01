-- Baseline bootstrap schema for bigname.
-- Collapsed from the pre-production development migration chain on 2026-04-30.

--
-- PostgreSQL database dump
--


-- Dumped from database version 16.13
-- Dumped by pg_dump version 16.13

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--



--
-- Name: backfill_lifecycle_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.backfill_lifecycle_status AS ENUM (
    'pending',
    'reserved',
    'running',
    'completed',
    'failed'
);


--
-- Name: canonicality_state; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.canonicality_state AS ENUM (
    'observed',
    'canonical',
    'safe',
    'finalized',
    'orphaned'
);


--
-- Name: capability_support_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.capability_support_status AS ENUM (
    'unsupported',
    'shadow',
    'supported'
);


--
-- Name: manifest_rollout_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.manifest_rollout_status AS ENUM (
    'draft',
    'shadow',
    'active',
    'deprecated'
);


--
-- Name: address_names_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.address_names_current (
    address text NOT NULL,
    logical_name_id text NOT NULL,
    relation text NOT NULL,
    namespace text NOT NULL,
    canonical_display_name text NOT NULL,
    normalized_name text NOT NULL,
    namehash text NOT NULL,
    surface_binding_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    token_lineage_id uuid,
    binding_kind text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT address_names_current_binding_kind_check CHECK ((binding_kind = ANY (ARRAY['declared_registry_path'::text, 'linked_subregistry_path'::text, 'resolver_alias_path'::text, 'observed_wildcard_path'::text, 'migration_rebind'::text, 'observed_only'::text]))),
    CONSTRAINT address_names_current_check CHECK ((logical_name_id = ((namespace || ':'::text) || normalized_name))),
    CONSTRAINT address_names_current_manifest_version_check CHECK ((manifest_version > 0)),
    CONSTRAINT address_names_current_relation_check CHECK ((relation = ANY (ARRAY['registrant'::text, 'token_holder'::text, 'effective_controller'::text])))
);


--
-- Name: backfill_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backfill_jobs (
    backfill_job_id bigint NOT NULL,
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    source_identity jsonb NOT NULL,
    scan_mode text NOT NULL,
    range_start_block_number bigint NOT NULL,
    range_end_block_number bigint NOT NULL,
    idempotency_key text NOT NULL,
    status public.backfill_lifecycle_status DEFAULT 'pending'::public.backfill_lifecycle_status NOT NULL,
    failure_reason text,
    failure_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT backfill_jobs_check CHECK ((range_end_block_number >= range_start_block_number)),
    CONSTRAINT backfill_jobs_check1 CHECK ((((status = 'failed'::public.backfill_lifecycle_status) = (failure_reason IS NOT NULL)) OR (status <> 'failed'::public.backfill_lifecycle_status))),
    CONSTRAINT backfill_jobs_check2 CHECK ((((status = 'completed'::public.backfill_lifecycle_status) = (completed_at IS NOT NULL)) OR (status <> 'completed'::public.backfill_lifecycle_status))),
    CONSTRAINT backfill_jobs_failure_metadata_check CHECK ((jsonb_typeof(failure_metadata) = 'object'::text)),
    CONSTRAINT backfill_jobs_range_start_block_number_check CHECK ((range_start_block_number >= 0)),
    CONSTRAINT backfill_jobs_source_identity_check CHECK ((jsonb_typeof(source_identity) = ANY (ARRAY['object'::text, 'array'::text])))
);


--
-- Name: backfill_jobs_backfill_job_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.backfill_jobs ALTER COLUMN backfill_job_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.backfill_jobs_backfill_job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: backfill_ranges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backfill_ranges (
    backfill_range_id bigint NOT NULL,
    backfill_job_id bigint NOT NULL,
    range_start_block_number bigint NOT NULL,
    range_end_block_number bigint NOT NULL,
    checkpoint_block_number bigint NOT NULL,
    status public.backfill_lifecycle_status DEFAULT 'pending'::public.backfill_lifecycle_status NOT NULL,
    lease_token text,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    attempt_count bigint DEFAULT 0 NOT NULL,
    failure_reason text,
    failure_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT backfill_ranges_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT backfill_ranges_check CHECK ((range_end_block_number >= range_start_block_number)),
    CONSTRAINT backfill_ranges_check1 CHECK (((checkpoint_block_number >= range_start_block_number) AND (checkpoint_block_number <= range_end_block_number))),
    CONSTRAINT backfill_ranges_check2 CHECK (((lease_token IS NULL) = (lease_owner IS NULL))),
    CONSTRAINT backfill_ranges_check3 CHECK (((lease_token IS NULL) = (lease_expires_at IS NULL))),
    CONSTRAINT backfill_ranges_check4 CHECK (((status = ANY (ARRAY['reserved'::public.backfill_lifecycle_status, 'running'::public.backfill_lifecycle_status])) = (lease_token IS NOT NULL))),
    CONSTRAINT backfill_ranges_check5 CHECK ((((status = 'failed'::public.backfill_lifecycle_status) = (failure_reason IS NOT NULL)) OR (status <> 'failed'::public.backfill_lifecycle_status))),
    CONSTRAINT backfill_ranges_check6 CHECK ((((status = 'completed'::public.backfill_lifecycle_status) = (completed_at IS NOT NULL)) OR (status <> 'completed'::public.backfill_lifecycle_status))),
    CONSTRAINT backfill_ranges_failure_metadata_check CHECK ((jsonb_typeof(failure_metadata) = 'object'::text)),
    CONSTRAINT backfill_ranges_range_start_block_number_check CHECK ((range_start_block_number >= 0))
);


--
-- Name: backfill_ranges_backfill_range_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.backfill_ranges ALTER COLUMN backfill_range_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.backfill_ranges_backfill_range_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: chain_checkpoints; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chain_checkpoints (
    chain_id text NOT NULL,
    canonical_block_hash text,
    canonical_block_number bigint,
    safe_block_hash text,
    safe_block_number bigint,
    finalized_block_hash text,
    finalized_block_number bigint,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chain_checkpoints_check CHECK (((canonical_block_hash IS NULL) = (canonical_block_number IS NULL))),
    CONSTRAINT chain_checkpoints_check1 CHECK (((safe_block_hash IS NULL) = (safe_block_number IS NULL))),
    CONSTRAINT chain_checkpoints_check2 CHECK (((finalized_block_hash IS NULL) = (finalized_block_number IS NULL)))
);


--
-- Name: chain_header_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chain_header_audit (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    logs_bloom bytea,
    transactions_root text,
    receipts_root text,
    state_root text,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chain_header_audit_check CHECK (((logs_bloom IS NOT NULL) OR (transactions_root IS NOT NULL) OR (receipts_root IS NOT NULL) OR (state_root IS NOT NULL)))
);


--
-- Name: chain_lineage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chain_lineage (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    parent_hash text,
    block_number bigint NOT NULL,
    block_timestamp timestamp with time zone NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chain_lineage_block_number_check CHECK ((block_number >= 0))
);


--
-- Name: children_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.children_current (
    parent_logical_name_id text NOT NULL,
    child_logical_name_id text NOT NULL,
    surface_class text DEFAULT 'declared'::text NOT NULL,
    namespace text NOT NULL,
    canonical_display_name text NOT NULL,
    normalized_name text NOT NULL,
    namehash text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT children_current_check CHECK ((parent_logical_name_id <> child_logical_name_id)),
    CONSTRAINT children_current_check1 CHECK ((child_logical_name_id = ((namespace || ':'::text) || normalized_name))),
    CONSTRAINT children_current_manifest_version_check CHECK ((manifest_version > 0)),
    CONSTRAINT children_current_surface_class_check CHECK ((surface_class = 'declared'::text))
);


--
-- Name: contract_instance_addresses; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contract_instance_addresses (
    contract_instance_address_id bigint NOT NULL,
    contract_instance_id uuid NOT NULL,
    chain_id text NOT NULL,
    address text NOT NULL,
    admitted_at timestamp with time zone DEFAULT now() NOT NULL,
    deactivated_at timestamp with time zone,
    active_from_block_number bigint,
    active_from_block_hash text,
    active_to_block_number bigint,
    active_to_block_hash text,
    source_manifest_id bigint,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT contract_instance_addresses_active_from_block_number_check CHECK (((active_from_block_number IS NULL) OR (active_from_block_number >= 0))),
    CONSTRAINT contract_instance_addresses_active_to_block_number_check CHECK (((active_to_block_number IS NULL) OR (active_to_block_number >= 0))),
    CONSTRAINT contract_instance_addresses_check CHECK (((deactivated_at IS NULL) OR (deactivated_at >= admitted_at)))
);


--
-- Name: contract_instance_addresses_contract_instance_address_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.contract_instance_addresses ALTER COLUMN contract_instance_address_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.contract_instance_addresses_contract_instance_address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: contract_instances; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contract_instances (
    contract_instance_id uuid NOT NULL,
    chain_id text NOT NULL,
    contract_kind text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: discovery_edges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.discovery_edges (
    discovery_edge_id bigint NOT NULL,
    chain_id text NOT NULL,
    edge_kind text NOT NULL,
    from_contract_instance_id uuid NOT NULL,
    to_contract_instance_id uuid NOT NULL,
    discovery_source text NOT NULL,
    source_manifest_id bigint,
    admission text NOT NULL,
    admitted_at timestamp with time zone DEFAULT now() NOT NULL,
    deactivated_at timestamp with time zone,
    active_from_block_number bigint,
    active_from_block_hash text,
    active_to_block_number bigint,
    active_to_block_hash text,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT discovery_edges_active_from_block_number_check1 CHECK (((active_from_block_number IS NULL) OR (active_from_block_number >= 0))),
    CONSTRAINT discovery_edges_active_to_block_number_check1 CHECK (((active_to_block_number IS NULL) OR (active_to_block_number >= 0))),
    CONSTRAINT discovery_edges_check CHECK (((deactivated_at IS NULL) OR (deactivated_at >= admitted_at)))
);


--
-- Name: discovery_edges_discovery_edge_id_seq1; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.discovery_edges ALTER COLUMN discovery_edge_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.discovery_edges_discovery_edge_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: execution_cache_outcomes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.execution_cache_outcomes (
    execution_cache_key text NOT NULL,
    request_key text NOT NULL,
    requested_chain_positions jsonb DEFAULT '[]'::jsonb NOT NULL,
    manifest_versions jsonb DEFAULT '[]'::jsonb NOT NULL,
    topology_version_boundary jsonb DEFAULT '{}'::jsonb NOT NULL,
    record_version_boundary jsonb DEFAULT '{}'::jsonb NOT NULL,
    execution_trace_id uuid NOT NULL,
    request_type text NOT NULL,
    namespace text NOT NULL,
    outcome_payload jsonb,
    failure_payload jsonb,
    finished_at timestamp with time zone NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_cache_outcomes_check CHECK (((outcome_payload IS NOT NULL) OR (failure_payload IS NOT NULL))),
    CONSTRAINT execution_cache_outcomes_manifest_versions_check CHECK (((jsonb_typeof(manifest_versions) = 'array'::text) AND (manifest_versions <> '[]'::jsonb))),
    CONSTRAINT execution_cache_outcomes_record_version_boundary_check CHECK (((jsonb_typeof(record_version_boundary) = 'object'::text) AND (record_version_boundary <> '{}'::jsonb))),
    CONSTRAINT execution_cache_outcomes_request_key_check CHECK ((request_key <> ''::text)),
    CONSTRAINT execution_cache_outcomes_requested_chain_positions_check CHECK (((jsonb_typeof(requested_chain_positions) = 'array'::text) AND (requested_chain_positions <> '[]'::jsonb))),
    CONSTRAINT execution_cache_outcomes_topology_version_boundary_check CHECK (((jsonb_typeof(topology_version_boundary) = 'object'::text) AND (topology_version_boundary <> '{}'::jsonb)))
);


--
-- Name: execution_steps; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.execution_steps (
    execution_trace_id uuid NOT NULL,
    step_index bigint NOT NULL,
    step_kind text NOT NULL,
    input_digest text,
    output_digest text,
    latency_ms bigint,
    canonicality_dependency jsonb DEFAULT '{}'::jsonb NOT NULL,
    step_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_steps_canonicality_dependency_check CHECK (((jsonb_typeof(canonicality_dependency) = 'object'::text) AND (canonicality_dependency <> '{}'::jsonb))),
    CONSTRAINT execution_steps_latency_ms_check CHECK (((latency_ms IS NULL) OR (latency_ms >= 0))),
    CONSTRAINT execution_steps_step_index_check CHECK ((step_index >= 0)),
    CONSTRAINT execution_steps_step_payload_check CHECK ((jsonb_typeof(step_payload) = 'object'::text))
);


--
-- Name: execution_traces; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.execution_traces (
    execution_trace_id uuid NOT NULL,
    request_type text NOT NULL,
    request_key text NOT NULL,
    namespace text NOT NULL,
    chain_context jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_context jsonb DEFAULT '{}'::jsonb NOT NULL,
    contracts_called jsonb DEFAULT '[]'::jsonb NOT NULL,
    gateway_digests jsonb DEFAULT '[]'::jsonb NOT NULL,
    final_payload jsonb,
    failure_payload jsonb,
    request_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    finished_at timestamp with time zone NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_traces_chain_context_check CHECK (((jsonb_typeof(chain_context) = 'object'::text) AND (chain_context <> '{}'::jsonb))),
    CONSTRAINT execution_traces_check CHECK (((final_payload IS NOT NULL) OR (failure_payload IS NOT NULL))),
    CONSTRAINT execution_traces_contracts_called_check CHECK ((jsonb_typeof(contracts_called) = 'array'::text)),
    CONSTRAINT execution_traces_gateway_digests_check CHECK ((jsonb_typeof(gateway_digests) = 'array'::text)),
    CONSTRAINT execution_traces_manifest_context_check CHECK (((jsonb_typeof(manifest_context) = 'object'::text) AND (manifest_context <> '{}'::jsonb))),
    CONSTRAINT execution_traces_request_metadata_check CHECK ((jsonb_typeof(request_metadata) = 'object'::text))
);


--
-- Name: manifest_alert_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.manifest_alert_observations (
    manifest_alert_observation_id bigint NOT NULL,
    observation_identity text NOT NULL,
    observation_kind text NOT NULL,
    lifecycle_status text DEFAULT 'active'::text NOT NULL,
    namespace text NOT NULL,
    source_family text NOT NULL,
    manifest_version bigint NOT NULL,
    source_manifest_id bigint,
    chain_id text NOT NULL,
    contract_instance_id uuid,
    proxy_contract_instance_id uuid,
    expected_implementation_contract_instance_id uuid,
    observed_implementation_contract_instance_id uuid,
    discovery_edge_id bigint,
    expected_code_hash text,
    observed_code_hash text,
    observed_code_byte_length bigint,
    observed_block_number bigint,
    observed_block_hash text,
    observed_canonicality_state public.canonicality_state,
    raw_fact_ref jsonb DEFAULT '{}'::jsonb NOT NULL,
    expected_material jsonb DEFAULT '{}'::jsonb NOT NULL,
    observed_material jsonb DEFAULT '{}'::jsonb NOT NULL,
    watch_plan_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    alert_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    remediation_status text,
    remediation_metadata jsonb,
    first_observed_at timestamp with time zone NOT NULL,
    last_observed_at timestamp with time zone NOT NULL,
    remediated_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT manifest_alert_observations_alert_metadata_check CHECK ((jsonb_typeof(alert_metadata) = 'object'::text)),
    CONSTRAINT manifest_alert_observations_check CHECK (((observed_block_hash IS NULL) = (observed_block_number IS NULL))),
    CONSTRAINT manifest_alert_observations_check1 CHECK ((last_observed_at >= first_observed_at)),
    CONSTRAINT manifest_alert_observations_check2 CHECK (((remediated_at IS NULL) OR (remediated_at >= first_observed_at))),
    CONSTRAINT manifest_alert_observations_check3 CHECK (((observation_kind <> 'manifest_drift'::text) OR ((contract_instance_id IS NOT NULL) AND (proxy_contract_instance_id IS NULL) AND (expected_code_hash IS NOT NULL) AND (observed_code_hash IS NOT NULL) AND (observed_canonicality_state IS NOT NULL)))),
    CONSTRAINT manifest_alert_observations_check4 CHECK (((observation_kind <> 'proxy_implementation_drift'::text) OR ((contract_instance_id IS NOT NULL) AND (proxy_contract_instance_id IS NOT NULL) AND (contract_instance_id = proxy_contract_instance_id)))),
    CONSTRAINT manifest_alert_observations_expected_material_check CHECK ((jsonb_typeof(expected_material) = 'object'::text)),
    CONSTRAINT manifest_alert_observations_lifecycle_status_check CHECK ((lifecycle_status = ANY (ARRAY['active'::text, 'acknowledged'::text, 'remediated'::text, 'dismissed'::text]))),
    CONSTRAINT manifest_alert_observations_manifest_version_check CHECK ((manifest_version > 0)),
    CONSTRAINT manifest_alert_observations_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['manifest_drift'::text, 'proxy_implementation_drift'::text]))),
    CONSTRAINT manifest_alert_observations_observed_block_number_check CHECK (((observed_block_number IS NULL) OR (observed_block_number >= 0))),
    CONSTRAINT manifest_alert_observations_observed_code_byte_length_check CHECK (((observed_code_byte_length IS NULL) OR (observed_code_byte_length >= 0))),
    CONSTRAINT manifest_alert_observations_observed_material_check CHECK ((jsonb_typeof(observed_material) = 'object'::text)),
    CONSTRAINT manifest_alert_observations_raw_fact_ref_check CHECK ((jsonb_typeof(raw_fact_ref) = 'object'::text)),
    CONSTRAINT manifest_alert_observations_remediation_metadata_check CHECK (((remediation_metadata IS NULL) OR (jsonb_typeof(remediation_metadata) = 'object'::text))),
    CONSTRAINT manifest_alert_observations_watch_plan_metadata_check CHECK ((jsonb_typeof(watch_plan_metadata) = 'object'::text))
);


--
-- Name: manifest_alert_observations_manifest_alert_observation_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.manifest_alert_observations ALTER COLUMN manifest_alert_observation_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.manifest_alert_observations_manifest_alert_observation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: manifest_capability_flags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.manifest_capability_flags (
    manifest_id bigint NOT NULL,
    capability_name text NOT NULL,
    status public.capability_support_status NOT NULL,
    notes text
);


--
-- Name: manifest_contract_instances; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.manifest_contract_instances (
    manifest_contract_instance_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    declaration_kind text NOT NULL,
    declaration_name text NOT NULL,
    contract_instance_id uuid NOT NULL,
    declared_address text NOT NULL,
    code_hash text,
    abi_ref text,
    role text,
    proxy_kind text,
    implementation_contract_instance_id uuid,
    declared_implementation_address text,
    CONSTRAINT manifest_contract_instances_check CHECK ((((declaration_kind = 'root'::text) AND (role IS NULL) AND (proxy_kind IS NULL) AND (implementation_contract_instance_id IS NULL) AND (declared_implementation_address IS NULL)) OR ((declaration_kind = 'contract'::text) AND (role IS NOT NULL)))),
    CONSTRAINT manifest_contract_instances_declaration_kind_check CHECK ((declaration_kind = ANY (ARRAY['root'::text, 'contract'::text])))
);


--
-- Name: manifest_contract_instances_manifest_contract_instance_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.manifest_contract_instances ALTER COLUMN manifest_contract_instance_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.manifest_contract_instances_manifest_contract_instance_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: manifest_discovery_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.manifest_discovery_rules (
    manifest_discovery_rule_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    edge_kind text NOT NULL,
    from_role text NOT NULL,
    admission text NOT NULL,
    rule_payload jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: manifest_discovery_rules_manifest_discovery_rule_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.manifest_discovery_rules ALTER COLUMN manifest_discovery_rule_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.manifest_discovery_rules_manifest_discovery_rule_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: manifest_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.manifest_versions (
    manifest_id bigint NOT NULL,
    manifest_version bigint NOT NULL,
    namespace text NOT NULL,
    source_family text NOT NULL,
    chain text NOT NULL,
    deployment_epoch text NOT NULL,
    rollout_status public.manifest_rollout_status NOT NULL,
    normalizer_version text NOT NULL,
    file_path text NOT NULL,
    manifest_payload jsonb NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT manifest_versions_manifest_version_check CHECK ((manifest_version > 0))
);


--
-- Name: manifest_versions_manifest_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.manifest_versions ALTER COLUMN manifest_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.manifest_versions_manifest_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: name_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.name_current (
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    canonical_display_name text NOT NULL,
    normalized_name text NOT NULL,
    namehash text NOT NULL,
    surface_binding_id uuid,
    resource_id uuid,
    token_lineage_id uuid,
    binding_kind text,
    declared_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT name_current_binding_kind_check CHECK (((binding_kind IS NULL) OR (binding_kind = ANY (ARRAY['declared_registry_path'::text, 'linked_subregistry_path'::text, 'resolver_alias_path'::text, 'observed_wildcard_path'::text, 'migration_rebind'::text, 'observed_only'::text])))),
    CONSTRAINT name_current_check CHECK ((logical_name_id = ((namespace || ':'::text) || normalized_name))),
    CONSTRAINT name_current_check1 CHECK ((((surface_binding_id IS NULL) AND (resource_id IS NULL) AND (binding_kind IS NULL)) OR ((surface_binding_id IS NOT NULL) AND (resource_id IS NOT NULL) AND (binding_kind IS NOT NULL)))),
    CONSTRAINT name_current_check2 CHECK (((token_lineage_id IS NULL) OR (resource_id IS NOT NULL))),
    CONSTRAINT name_current_manifest_version_check CHECK ((manifest_version > 0))
);


--
-- Name: name_surfaces; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.name_surfaces (
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    input_name text NOT NULL,
    canonical_display_name text NOT NULL,
    normalized_name text NOT NULL,
    dns_encoded_name bytea NOT NULL,
    namehash text NOT NULL,
    labelhashes text[] NOT NULL,
    normalizer_version text NOT NULL,
    normalization_warnings jsonb DEFAULT '[]'::jsonb NOT NULL,
    normalization_errors jsonb DEFAULT '[]'::jsonb NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT name_surfaces_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT name_surfaces_check CHECK ((logical_name_id = ((namespace || ':'::text) || normalized_name)))
);


--
-- Name: normalized_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.normalized_events (
    normalized_event_id bigint NOT NULL,
    event_identity text NOT NULL,
    namespace text NOT NULL,
    logical_name_id text,
    resource_id uuid,
    event_kind text NOT NULL,
    source_family text NOT NULL,
    manifest_version bigint NOT NULL,
    source_manifest_id bigint,
    chain_id text,
    block_number bigint,
    block_hash text,
    transaction_hash text,
    log_index bigint,
    raw_fact_ref jsonb DEFAULT '{}'::jsonb NOT NULL,
    derivation_kind text NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    before_state jsonb DEFAULT '{}'::jsonb NOT NULL,
    after_state jsonb DEFAULT '{}'::jsonb NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT normalized_events_block_number_check CHECK (((block_number IS NULL) OR (block_number >= 0))),
    CONSTRAINT normalized_events_check CHECK (((block_hash IS NULL) = (block_number IS NULL))),
    CONSTRAINT normalized_events_check1 CHECK (((transaction_hash IS NOT NULL) OR (log_index IS NULL))),
    CONSTRAINT normalized_events_log_index_check CHECK (((log_index IS NULL) OR (log_index >= 0))),
    CONSTRAINT normalized_events_manifest_version_check CHECK ((manifest_version > 0))
);


--
-- Name: normalized_events_normalized_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.normalized_events ALTER COLUMN normalized_event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.normalized_events_normalized_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: normalized_replay_cursors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.normalized_replay_cursors (
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    cursor_kind text NOT NULL,
    range_start_block_number bigint NOT NULL,
    next_block_number bigint NOT NULL,
    target_block_number bigint NOT NULL,
    last_completed_block_number bigint,
    last_selected_block_count bigint DEFAULT 0 NOT NULL,
    last_canonical_raw_log_count bigint DEFAULT 0 NOT NULL,
    last_scanned_raw_log_count bigint DEFAULT 0 NOT NULL,
    last_matched_raw_log_count bigint DEFAULT 0 NOT NULL,
    last_normalized_event_synced_count bigint DEFAULT 0 NOT NULL,
    last_normalized_event_inserted_count bigint DEFAULT 0 NOT NULL,
    last_replayed_at timestamp with time zone,
    last_failure_reason text,
    last_failure_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT normalized_replay_cursors_check CHECK ((next_block_number >= range_start_block_number)),
    CONSTRAINT normalized_replay_cursors_check1 CHECK ((target_block_number >= range_start_block_number)),
    CONSTRAINT normalized_replay_cursors_check2 CHECK (((last_completed_block_number IS NULL) OR (last_completed_block_number >= range_start_block_number))),
    CONSTRAINT normalized_replay_cursors_check3 CHECK ((next_block_number <= (target_block_number + 1))),
    CONSTRAINT normalized_replay_cursors_last_canonical_raw_log_count_check CHECK ((last_canonical_raw_log_count >= 0)),
    CONSTRAINT normalized_replay_cursors_last_matched_raw_log_count_check CHECK ((last_matched_raw_log_count >= 0)),
    CONSTRAINT normalized_replay_cursors_last_normalized_event_inserted__check CHECK ((last_normalized_event_inserted_count >= 0)),
    CONSTRAINT normalized_replay_cursors_last_normalized_event_synced_co_check CHECK ((last_normalized_event_synced_count >= 0)),
    CONSTRAINT normalized_replay_cursors_last_scanned_raw_log_count_check CHECK ((last_scanned_raw_log_count >= 0)),
    CONSTRAINT normalized_replay_cursors_last_selected_block_count_check CHECK ((last_selected_block_count >= 0)),
    CONSTRAINT normalized_replay_cursors_range_start_block_number_check CHECK ((range_start_block_number >= 0))
);


--
-- Name: permissions_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.permissions_current (
    resource_id uuid NOT NULL,
    subject text NOT NULL,
    scope text NOT NULL,
    scope_kind text NOT NULL,
    scope_detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    effective_powers jsonb DEFAULT '[]'::jsonb NOT NULL,
    grant_source jsonb DEFAULT '{}'::jsonb NOT NULL,
    revocation_source jsonb,
    inheritance_path jsonb DEFAULT '[]'::jsonb NOT NULL,
    transfer_behavior jsonb DEFAULT '{}'::jsonb NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT permissions_current_manifest_version_check CHECK ((manifest_version > 0)),
    CONSTRAINT permissions_current_scope_check CHECK ((scope <> ''::text)),
    CONSTRAINT permissions_current_scope_kind_check CHECK ((scope_kind = ANY (ARRAY['root'::text, 'registry'::text, 'resource'::text, 'resolver'::text, 'record_manager'::text, 'migration_derived'::text, 'transport_derived'::text]))),
    CONSTRAINT permissions_current_subject_check CHECK ((subject <> ''::text))
);


--
-- Name: primary_names_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.primary_names_current (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    claim_status text DEFAULT 'unsupported'::text NOT NULL,
    raw_claim_name text,
    claim_provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    normalized_claim_name text,
    CONSTRAINT primary_names_current_address_check CHECK ((address <> ''::text)),
    CONSTRAINT primary_names_current_claim_provenance_object_check CHECK ((jsonb_typeof(claim_provenance) = 'object'::text)),
    CONSTRAINT primary_names_current_claim_status_check CHECK ((claim_status = ANY (ARRAY['success'::text, 'not_found'::text, 'unsupported'::text, 'invalid_name'::text]))),
    CONSTRAINT primary_names_current_coin_type_check CHECK ((coin_type <> ''::text)),
    CONSTRAINT primary_names_current_namespace_check CHECK ((namespace <> ''::text)),
    CONSTRAINT primary_names_current_normalized_claim_name_check CHECK (((normalized_claim_name IS NULL) OR (btrim(normalized_claim_name) <> ''::text))),
    CONSTRAINT primary_names_current_raw_claim_name_check CHECK ((((claim_status = 'invalid_name'::text) AND (raw_claim_name IS NOT NULL) AND (btrim(raw_claim_name) <> ''::text)) OR ((claim_status <> 'invalid_name'::text) AND (raw_claim_name IS NULL))))
);


--
-- Name: raw_call_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_call_snapshots (
    raw_call_snapshot_id bigint NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    request_hash text NOT NULL,
    request_payload jsonb NOT NULL,
    response_hash text NOT NULL,
    response_payload jsonb NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_call_snapshots_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT raw_call_snapshots_request_payload_check CHECK ((jsonb_typeof(request_payload) = 'object'::text))
);


--
-- Name: raw_call_snapshots_raw_call_snapshot_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_call_snapshots ALTER COLUMN raw_call_snapshot_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_call_snapshots_raw_call_snapshot_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_code_hashes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_code_hashes (
    raw_code_hash_id bigint NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    contract_address text NOT NULL,
    code_hash text NOT NULL,
    code_byte_length bigint NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_code_hashes_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT raw_code_hashes_code_byte_length_check CHECK ((code_byte_length >= 0))
);


--
-- Name: raw_code_hashes_raw_code_hash_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_code_hashes ALTER COLUMN raw_code_hash_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_code_hashes_raw_code_hash_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_logs (
    raw_log_id bigint NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    log_index bigint NOT NULL,
    emitting_address text NOT NULL,
    topics text[] DEFAULT '{}'::text[] NOT NULL,
    data bytea DEFAULT '\x'::bytea NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_logs_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT raw_logs_log_index_check CHECK ((log_index >= 0)),
    CONSTRAINT raw_logs_transaction_index_check CHECK ((transaction_index >= 0))
);


--
-- Name: raw_logs_raw_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_logs ALTER COLUMN raw_log_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_logs_raw_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_payload_cache_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_payload_cache_metadata (
    raw_payload_cache_metadata_id bigint NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    payload_kind text NOT NULL,
    digest_algorithm text,
    retained_digest text,
    block_number bigint,
    payload_size_bytes bigint NOT NULL,
    content_type text,
    content_encoding text,
    cache_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    first_observed_at timestamp with time zone DEFAULT now() NOT NULL,
    last_observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_payload_cache_metadata_block_hash_check CHECK ((btrim(block_hash) <> ''::text)),
    CONSTRAINT raw_payload_cache_metadata_block_number_check CHECK (((block_number IS NULL) OR (block_number >= 0))),
    CONSTRAINT raw_payload_cache_metadata_cache_metadata_check CHECK ((jsonb_typeof(cache_metadata) = 'object'::text)),
    CONSTRAINT raw_payload_cache_metadata_chain_id_check CHECK ((btrim(chain_id) <> ''::text)),
    CONSTRAINT raw_payload_cache_metadata_check CHECK (((digest_algorithm IS NULL) = (retained_digest IS NULL))),
    CONSTRAINT raw_payload_cache_metadata_content_encoding_check CHECK (((content_encoding IS NULL) OR (btrim(content_encoding) <> ''::text))),
    CONSTRAINT raw_payload_cache_metadata_content_type_check CHECK (((content_type IS NULL) OR (btrim(content_type) <> ''::text))),
    CONSTRAINT raw_payload_cache_metadata_digest_algorithm_check CHECK (((digest_algorithm IS NULL) OR (btrim(digest_algorithm) <> ''::text))),
    CONSTRAINT raw_payload_cache_metadata_payload_kind_check CHECK ((btrim(payload_kind) <> ''::text)),
    CONSTRAINT raw_payload_cache_metadata_payload_size_bytes_check CHECK ((payload_size_bytes >= 0)),
    CONSTRAINT raw_payload_cache_metadata_retained_digest_check CHECK (((retained_digest IS NULL) OR (btrim(retained_digest) <> ''::text)))
);


--
-- Name: raw_payload_cache_metadata_raw_payload_cache_metadata_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_payload_cache_metadata ALTER COLUMN raw_payload_cache_metadata_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_payload_cache_metadata_raw_payload_cache_metadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_receipts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_receipts (
    raw_receipt_id bigint NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    contract_address text,
    status boolean,
    gas_used bigint,
    cumulative_gas_used bigint,
    logs_bloom bytea,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_receipts_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT raw_receipts_cumulative_gas_used_check CHECK ((cumulative_gas_used >= 0)),
    CONSTRAINT raw_receipts_gas_used_check CHECK ((gas_used >= 0)),
    CONSTRAINT raw_receipts_transaction_index_check CHECK ((transaction_index >= 0))
);


--
-- Name: raw_receipts_raw_receipt_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_receipts ALTER COLUMN raw_receipt_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_receipts_raw_receipt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_transactions (
    raw_transaction_id bigint NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    transaction_index bigint NOT NULL,
    from_address text NOT NULL,
    to_address text,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_transactions_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT raw_transactions_transaction_index_check CHECK ((transaction_index >= 0))
);


--
-- Name: raw_transactions_raw_transaction_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.raw_transactions ALTER COLUMN raw_transaction_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.raw_transactions_raw_transaction_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: record_inventory_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.record_inventory_current (
    resource_id uuid NOT NULL,
    record_version_boundary_key text NOT NULL,
    record_version_boundary jsonb DEFAULT '{}'::jsonb NOT NULL,
    enumeration_basis jsonb DEFAULT '{}'::jsonb NOT NULL,
    selectors jsonb DEFAULT '[]'::jsonb NOT NULL,
    explicit_gaps jsonb DEFAULT '[]'::jsonb NOT NULL,
    unsupported_families jsonb DEFAULT '[]'::jsonb NOT NULL,
    last_change jsonb,
    entries jsonb DEFAULT '[]'::jsonb NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT record_inventory_current_manifest_version_check CHECK ((manifest_version > 0)),
    CONSTRAINT record_inventory_current_record_version_boundary_key_check CHECK ((record_version_boundary_key <> ''::text))
);


--
-- Name: resolver_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resolver_current (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    declared_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT resolver_current_chain_id_check CHECK ((chain_id <> ''::text)),
    CONSTRAINT resolver_current_manifest_version_check CHECK ((manifest_version > 0)),
    CONSTRAINT resolver_current_resolver_address_check CHECK ((resolver_address <> ''::text))
);


--
-- Name: resources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resources (
    resource_id uuid NOT NULL,
    token_lineage_id uuid,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT resources_block_number_check CHECK ((block_number >= 0))
);


--
-- Name: surface_bindings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.surface_bindings (
    surface_binding_id uuid NOT NULL,
    logical_name_id text NOT NULL,
    resource_id uuid NOT NULL,
    binding_kind text NOT NULL,
    active_from timestamp with time zone NOT NULL,
    active_to timestamp with time zone,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT surface_bindings_binding_kind_check CHECK ((binding_kind = ANY (ARRAY['declared_registry_path'::text, 'linked_subregistry_path'::text, 'resolver_alias_path'::text, 'observed_wildcard_path'::text, 'migration_rebind'::text, 'observed_only'::text]))),
    CONSTRAINT surface_bindings_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT surface_bindings_check CHECK (((active_to IS NULL) OR (active_to > active_from)))
);


--
-- Name: token_lineages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.token_lineages (
    token_lineage_id uuid NOT NULL,
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT token_lineages_block_number_check CHECK ((block_number >= 0))
);


--
-- Name: address_names_current address_names_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.address_names_current
    ADD CONSTRAINT address_names_current_pkey PRIMARY KEY (address, logical_name_id, relation);


--
-- Name: backfill_jobs backfill_jobs_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backfill_jobs
    ADD CONSTRAINT backfill_jobs_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: backfill_jobs backfill_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backfill_jobs
    ADD CONSTRAINT backfill_jobs_pkey PRIMARY KEY (backfill_job_id);


--
-- Name: backfill_ranges backfill_ranges_backfill_job_id_range_start_block_number_ra_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backfill_ranges
    ADD CONSTRAINT backfill_ranges_backfill_job_id_range_start_block_number_ra_key UNIQUE (backfill_job_id, range_start_block_number, range_end_block_number);


--
-- Name: backfill_ranges backfill_ranges_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backfill_ranges
    ADD CONSTRAINT backfill_ranges_pkey PRIMARY KEY (backfill_range_id);


--
-- Name: chain_checkpoints chain_checkpoints_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chain_checkpoints
    ADD CONSTRAINT chain_checkpoints_pkey PRIMARY KEY (chain_id);


--
-- Name: chain_header_audit chain_header_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chain_header_audit
    ADD CONSTRAINT chain_header_audit_pkey PRIMARY KEY (chain_id, block_hash);


--
-- Name: chain_lineage chain_lineage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chain_lineage
    ADD CONSTRAINT chain_lineage_pkey PRIMARY KEY (chain_id, block_hash);


--
-- Name: children_current children_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.children_current
    ADD CONSTRAINT children_current_pkey PRIMARY KEY (parent_logical_name_id, child_logical_name_id, surface_class);


--
-- Name: contract_instance_addresses contract_instance_addresses_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contract_instance_addresses
    ADD CONSTRAINT contract_instance_addresses_pkey PRIMARY KEY (contract_instance_address_id);


--
-- Name: contract_instances contract_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contract_instances
    ADD CONSTRAINT contract_instances_pkey PRIMARY KEY (contract_instance_id);


--
-- Name: discovery_edges discovery_edges_pkey1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.discovery_edges
    ADD CONSTRAINT discovery_edges_pkey1 PRIMARY KEY (discovery_edge_id);


--
-- Name: execution_cache_outcomes execution_cache_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_cache_outcomes
    ADD CONSTRAINT execution_cache_outcomes_pkey PRIMARY KEY (execution_cache_key);


--
-- Name: execution_steps execution_steps_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_steps
    ADD CONSTRAINT execution_steps_pkey PRIMARY KEY (execution_trace_id, step_index);


--
-- Name: execution_traces execution_traces_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_traces
    ADD CONSTRAINT execution_traces_pkey PRIMARY KEY (execution_trace_id);


--
-- Name: manifest_alert_observations manifest_alert_observations_observation_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_observation_identity_key UNIQUE (observation_identity);


--
-- Name: manifest_alert_observations manifest_alert_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_pkey PRIMARY KEY (manifest_alert_observation_id);


--
-- Name: manifest_capability_flags manifest_capability_flags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_capability_flags
    ADD CONSTRAINT manifest_capability_flags_pkey PRIMARY KEY (manifest_id, capability_name);


--
-- Name: manifest_contract_instances manifest_contract_instances_manifest_id_declaration_kind_de_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_contract_instances
    ADD CONSTRAINT manifest_contract_instances_manifest_id_declaration_kind_de_key UNIQUE (manifest_id, declaration_kind, declaration_name);


--
-- Name: manifest_contract_instances manifest_contract_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_contract_instances
    ADD CONSTRAINT manifest_contract_instances_pkey PRIMARY KEY (manifest_contract_instance_id);


--
-- Name: manifest_discovery_rules manifest_discovery_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_discovery_rules
    ADD CONSTRAINT manifest_discovery_rules_pkey PRIMARY KEY (manifest_discovery_rule_id);


--
-- Name: manifest_versions manifest_versions_file_path_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_versions
    ADD CONSTRAINT manifest_versions_file_path_key UNIQUE (file_path);


--
-- Name: manifest_versions manifest_versions_namespace_source_family_chain_deployment__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_versions
    ADD CONSTRAINT manifest_versions_namespace_source_family_chain_deployment__key UNIQUE (namespace, source_family, chain, deployment_epoch, manifest_version);


--
-- Name: manifest_versions manifest_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_versions
    ADD CONSTRAINT manifest_versions_pkey PRIMARY KEY (manifest_id);


--
-- Name: name_current name_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.name_current
    ADD CONSTRAINT name_current_pkey PRIMARY KEY (logical_name_id);


--
-- Name: name_surfaces name_surfaces_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.name_surfaces
    ADD CONSTRAINT name_surfaces_pkey PRIMARY KEY (logical_name_id);


--
-- Name: normalized_events normalized_events_event_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.normalized_events
    ADD CONSTRAINT normalized_events_event_identity_key UNIQUE (event_identity);


--
-- Name: normalized_events normalized_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.normalized_events
    ADD CONSTRAINT normalized_events_pkey PRIMARY KEY (normalized_event_id);


--
-- Name: normalized_replay_cursors normalized_replay_cursors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.normalized_replay_cursors
    ADD CONSTRAINT normalized_replay_cursors_pkey PRIMARY KEY (deployment_profile, chain_id, cursor_kind);


--
-- Name: permissions_current permissions_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.permissions_current
    ADD CONSTRAINT permissions_current_pkey PRIMARY KEY (resource_id, subject, scope);


--
-- Name: primary_names_current primary_names_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.primary_names_current
    ADD CONSTRAINT primary_names_current_pkey PRIMARY KEY (address, coin_type, namespace);


--
-- Name: raw_call_snapshots raw_call_snapshots_chain_id_block_hash_request_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_call_snapshots
    ADD CONSTRAINT raw_call_snapshots_chain_id_block_hash_request_hash_key UNIQUE (chain_id, block_hash, request_hash);


--
-- Name: raw_call_snapshots raw_call_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_call_snapshots
    ADD CONSTRAINT raw_call_snapshots_pkey PRIMARY KEY (raw_call_snapshot_id);


--
-- Name: raw_code_hashes raw_code_hashes_chain_id_block_hash_contract_address_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_code_hashes
    ADD CONSTRAINT raw_code_hashes_chain_id_block_hash_contract_address_key UNIQUE (chain_id, block_hash, contract_address);


--
-- Name: raw_code_hashes raw_code_hashes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_code_hashes
    ADD CONSTRAINT raw_code_hashes_pkey PRIMARY KEY (raw_code_hash_id);


--
-- Name: raw_logs raw_logs_chain_id_block_hash_log_index_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_logs
    ADD CONSTRAINT raw_logs_chain_id_block_hash_log_index_key UNIQUE (chain_id, block_hash, log_index);


--
-- Name: raw_logs raw_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_logs
    ADD CONSTRAINT raw_logs_pkey PRIMARY KEY (raw_log_id);


--
-- Name: raw_payload_cache_metadata raw_payload_cache_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_payload_cache_metadata
    ADD CONSTRAINT raw_payload_cache_metadata_pkey PRIMARY KEY (raw_payload_cache_metadata_id);


--
-- Name: raw_receipts raw_receipts_chain_id_block_hash_transaction_index_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_receipts
    ADD CONSTRAINT raw_receipts_chain_id_block_hash_transaction_index_key UNIQUE (chain_id, block_hash, transaction_index);


--
-- Name: raw_receipts raw_receipts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_receipts
    ADD CONSTRAINT raw_receipts_pkey PRIMARY KEY (raw_receipt_id);


--
-- Name: raw_transactions raw_transactions_chain_id_block_hash_transaction_index_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_transactions
    ADD CONSTRAINT raw_transactions_chain_id_block_hash_transaction_index_key UNIQUE (chain_id, block_hash, transaction_index);


--
-- Name: raw_transactions raw_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_transactions
    ADD CONSTRAINT raw_transactions_pkey PRIMARY KEY (raw_transaction_id);


--
-- Name: record_inventory_current record_inventory_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.record_inventory_current
    ADD CONSTRAINT record_inventory_current_pkey PRIMARY KEY (resource_id, record_version_boundary_key);


--
-- Name: resolver_current resolver_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resolver_current
    ADD CONSTRAINT resolver_current_pkey PRIMARY KEY (chain_id, resolver_address);


--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (resource_id);


--
-- Name: resources resources_token_lineage_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_token_lineage_id_key UNIQUE (token_lineage_id);


--
-- Name: surface_bindings surface_bindings_no_overlap; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.surface_bindings
    ADD CONSTRAINT surface_bindings_no_overlap EXCLUDE USING gist (logical_name_id WITH =, tstzrange(active_from, COALESCE(active_to, 'infinity'::timestamp with time zone), '[)'::text) WITH &&) WHERE ((canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));


--
-- Name: surface_bindings surface_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.surface_bindings
    ADD CONSTRAINT surface_bindings_pkey PRIMARY KEY (surface_binding_id);


--
-- Name: token_lineages token_lineages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_lineages
    ADD CONSTRAINT token_lineages_pkey PRIMARY KEY (token_lineage_id);


--
-- Name: address_names_current_address_sort_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX address_names_current_address_sort_idx ON public.address_names_current USING btree (address, namespace, canonical_display_name, logical_name_id);


--
-- Name: backfill_jobs_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX backfill_jobs_lookup_idx ON public.backfill_jobs USING btree (deployment_profile, chain_id, scan_mode, status);


--
-- Name: backfill_jobs_range_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX backfill_jobs_range_idx ON public.backfill_jobs USING btree (chain_id, range_start_block_number, range_end_block_number);


--
-- Name: backfill_ranges_active_lease_token_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX backfill_ranges_active_lease_token_idx ON public.backfill_ranges USING btree (lease_token) WHERE ((lease_token IS NOT NULL) AND (status = ANY (ARRAY['reserved'::public.backfill_lifecycle_status, 'running'::public.backfill_lifecycle_status])));


--
-- Name: backfill_ranges_lease_expiry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX backfill_ranges_lease_expiry_idx ON public.backfill_ranges USING btree (lease_expires_at) WHERE (lease_expires_at IS NOT NULL);


--
-- Name: backfill_ranges_reservation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX backfill_ranges_reservation_idx ON public.backfill_ranges USING btree (backfill_job_id, status, range_start_block_number, range_end_block_number);


--
-- Name: chain_lineage_by_number_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX chain_lineage_by_number_idx ON public.chain_lineage USING btree (chain_id, block_number DESC);


--
-- Name: chain_lineage_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX chain_lineage_by_state_idx ON public.chain_lineage USING btree (chain_id, canonicality_state, block_number DESC);


--
-- Name: chain_lineage_chain_timestamp_canonical_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX chain_lineage_chain_timestamp_canonical_idx ON public.chain_lineage USING btree (chain_id, block_timestamp, block_number) INCLUDE (block_hash, canonicality_state) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: children_current_child_parent_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX children_current_child_parent_idx ON public.children_current USING btree (child_logical_name_id, surface_class, parent_logical_name_id);


--
-- Name: children_current_parent_sort_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX children_current_parent_sort_idx ON public.children_current USING btree (parent_logical_name_id, surface_class, canonical_display_name, child_logical_name_id);


--
-- Name: contract_instance_addresses_active_address_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX contract_instance_addresses_active_address_idx ON public.contract_instance_addresses USING btree (chain_id, address) WHERE (deactivated_at IS NULL);


--
-- Name: contract_instance_addresses_active_instance_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX contract_instance_addresses_active_instance_idx ON public.contract_instance_addresses USING btree (contract_instance_id) WHERE (deactivated_at IS NULL);


--
-- Name: contract_instance_addresses_latest_instance_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX contract_instance_addresses_latest_instance_idx ON public.contract_instance_addresses USING btree (contract_instance_id, ((deactivated_at IS NULL)) DESC, admitted_at DESC) INCLUDE (chain_id, address);


--
-- Name: contract_instance_addresses_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX contract_instance_addresses_lookup_idx ON public.contract_instance_addresses USING btree (chain_id, address, admitted_at DESC);


--
-- Name: contract_instances_chain_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX contract_instances_chain_kind_idx ON public.contract_instances USING btree (chain_id, contract_kind);


--
-- Name: discovery_edges_active_source_from_endpoint_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX discovery_edges_active_source_from_endpoint_idx ON public.discovery_edges USING btree (source_manifest_id, from_contract_instance_id) WHERE ((deactivated_at IS NULL) AND (edge_kind <> 'migration'::text));


--
-- Name: discovery_edges_active_source_to_endpoint_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX discovery_edges_active_source_to_endpoint_idx ON public.discovery_edges USING btree (source_manifest_id, to_contract_instance_id) WHERE ((deactivated_at IS NULL) AND (edge_kind <> 'migration'::text));


--
-- Name: discovery_edges_active_target_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX discovery_edges_active_target_idx ON public.discovery_edges USING btree (chain_id, to_contract_instance_id, edge_kind) WHERE (deactivated_at IS NULL);


--
-- Name: discovery_edges_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX discovery_edges_lookup_idx ON public.discovery_edges USING btree (chain_id, from_contract_instance_id, edge_kind);


--
-- Name: discovery_edges_observation_point_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX discovery_edges_observation_point_idx ON public.discovery_edges USING btree (discovery_source, edge_kind, active_from_block_number, active_from_block_hash, ((provenance ->> 'observation_key'::text)));


--
-- Name: discovery_edges_observation_point_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX discovery_edges_observation_point_lookup_idx ON public.discovery_edges USING btree (discovery_source, ((provenance ->> 'observation_key'::text)), active_from_block_number, active_from_block_hash, edge_kind);


--
-- Name: execution_cache_outcomes_execution_trace_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX execution_cache_outcomes_execution_trace_idx ON public.execution_cache_outcomes USING btree (execution_trace_id);


--
-- Name: manifest_alert_observations_contract_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_alert_observations_contract_idx ON public.manifest_alert_observations USING btree (contract_instance_id, observation_kind, manifest_alert_observation_id DESC) WHERE (contract_instance_id IS NOT NULL);


--
-- Name: manifest_alert_observations_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_alert_observations_lookup_idx ON public.manifest_alert_observations USING btree (observation_kind, lifecycle_status, namespace, source_family, manifest_version, manifest_alert_observation_id DESC);


--
-- Name: manifest_alert_observations_manifest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_alert_observations_manifest_idx ON public.manifest_alert_observations USING btree (source_manifest_id, observation_kind, manifest_alert_observation_id DESC) WHERE (source_manifest_id IS NOT NULL);


--
-- Name: manifest_alert_observations_proxy_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_alert_observations_proxy_idx ON public.manifest_alert_observations USING btree (proxy_contract_instance_id, manifest_alert_observation_id DESC) WHERE (proxy_contract_instance_id IS NOT NULL);


--
-- Name: manifest_contract_instances_instance_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_contract_instances_instance_idx ON public.manifest_contract_instances USING btree (contract_instance_id);


--
-- Name: manifest_contract_instances_manifest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_contract_instances_manifest_idx ON public.manifest_contract_instances USING btree (manifest_id, declaration_kind, declaration_name);


--
-- Name: manifest_discovery_rules_manifest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_discovery_rules_manifest_idx ON public.manifest_discovery_rules USING btree (manifest_id);


--
-- Name: manifest_versions_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_versions_lookup_idx ON public.manifest_versions USING btree (namespace, source_family, chain, rollout_status);


--
-- Name: manifest_versions_rollout_manifest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX manifest_versions_rollout_manifest_idx ON public.manifest_versions USING btree (rollout_status, manifest_id);


--
-- Name: name_surfaces_lower_namehash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX name_surfaces_lower_namehash_idx ON public.name_surfaces USING btree (lower(namehash)) WHERE ((labelhashes[1] IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));

--
-- Name: name_surfaces_lower_labelhash_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX name_surfaces_lower_labelhash_replay_idx ON public.name_surfaces USING btree (lower(labelhashes[1]), logical_name_id) WHERE ((labelhashes[1] IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));


--
-- Name: normalized_events_chain_position_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_chain_position_idx ON public.normalized_events USING btree (chain_id, block_number DESC, normalized_event_id DESC) WHERE (block_number IS NOT NULL);


--
-- Name: normalized_events_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_kind_idx ON public.normalized_events USING btree (event_kind, normalized_event_id DESC);


--
-- Name: normalized_events_manifest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_manifest_idx ON public.normalized_events USING btree (source_manifest_id, event_kind, normalized_event_id DESC) WHERE (source_manifest_id IS NOT NULL);


--
-- Name: normalized_events_name_projection_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_name_projection_replay_idx ON public.normalized_events USING btree (logical_name_id, block_number DESC NULLS LAST, chain_id, block_hash DESC NULLS LAST, transaction_hash DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((logical_name_id IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));


--
-- Name: normalized_events_name_relevant_projection_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_name_relevant_projection_idx ON public.normalized_events USING btree (logical_name_id, block_number NULLS FIRST, log_index, event_identity) WHERE ((logical_name_id IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));


--
-- Name: normalized_events_namespace_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_namespace_idx ON public.normalized_events USING btree (namespace, normalized_event_id DESC);


--
-- Name: normalized_events_record_inventory_resource_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_record_inventory_resource_replay_idx ON public.normalized_events USING btree (resource_id, block_number, log_index NULLS FIRST, normalized_event_id) WHERE ((resource_id IS NOT NULL) AND (logical_name_id IS NOT NULL) AND (chain_id IS NOT NULL) AND (block_number IS NOT NULL) AND (block_hash IS NOT NULL) AND (derivation_kind = ANY (ARRAY['ens_v1_unwrapped_authority'::text, 'ens_v2_resolver'::text])) AND (event_kind = ANY (ARRAY['RecordChanged'::text, 'RecordVersionChanged'::text, 'ResolverChanged'::text])) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));


--
-- Name: normalized_events_resource_projection_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_resource_projection_replay_idx ON public.normalized_events USING btree (resource_id, block_number DESC NULLS LAST, chain_id, block_hash DESC NULLS LAST, transaction_hash DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC) WHERE ((resource_id IS NOT NULL) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])));


--
-- Name: normalized_events_reverse_claim_source_lookup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_events_reverse_claim_source_lookup_idx ON public.normalized_events USING btree (chain_id, lower((after_state ->> 'reverse_node'::text)), block_number DESC NULLS LAST, log_index DESC NULLS LAST, normalized_event_id DESC) WHERE ((event_kind = 'ReverseChanged'::text) AND (derivation_kind = 'ens_v1_reverse_claim'::text) AND (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state])) AND ((after_state ->> 'reverse_node'::text) IS NOT NULL) AND ((after_state ->> 'reverse_node'::text) <> ''::text) AND ((after_state ->> 'address'::text) IS NOT NULL) AND ((after_state ->> 'address'::text) <> ''::text) AND ((after_state ->> 'coin_type'::text) IS NOT NULL) AND ((after_state ->> 'coin_type'::text) <> ''::text) AND ((after_state ->> 'reverse_name'::text) IS NOT NULL) AND ((after_state ->> 'reverse_name'::text) <> ''::text));


--
-- Name: normalized_replay_cursors_progress_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX normalized_replay_cursors_progress_idx ON public.normalized_replay_cursors USING btree (deployment_profile, chain_id, cursor_kind, next_block_number, target_block_number);


--
-- Name: raw_call_snapshots_by_request_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_call_snapshots_by_request_hash_idx ON public.raw_call_snapshots USING btree (chain_id, request_hash, block_number DESC);


--
-- Name: raw_call_snapshots_by_response_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_call_snapshots_by_response_hash_idx ON public.raw_call_snapshots USING btree (chain_id, response_hash, block_number DESC);


--
-- Name: raw_call_snapshots_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_call_snapshots_by_state_idx ON public.raw_call_snapshots USING btree (chain_id, canonicality_state, block_number DESC, request_hash);


--
-- Name: raw_code_hashes_by_contract_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_code_hashes_by_contract_idx ON public.raw_code_hashes USING btree (chain_id, contract_address, block_number DESC);


--
-- Name: raw_code_hashes_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_code_hashes_by_state_idx ON public.raw_code_hashes USING btree (chain_id, canonicality_state, block_number DESC, contract_address);


--
-- Name: raw_logs_by_emitter_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_by_emitter_idx ON public.raw_logs USING btree (chain_id, emitting_address, block_number DESC, log_index DESC);


--
-- Name: raw_logs_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_by_state_idx ON public.raw_logs USING btree (chain_id, canonicality_state, block_number DESC, log_index DESC);


--
-- Name: raw_logs_by_tx_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_by_tx_idx ON public.raw_logs USING btree (chain_id, transaction_hash, log_index);


--
-- Name: raw_logs_canonical_emitter_block_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_canonical_emitter_block_idx ON public.raw_logs USING btree (chain_id, lower(emitting_address), block_number, transaction_index, log_index) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_logs_canonical_emitter_topic_block_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_canonical_emitter_topic_block_idx ON public.raw_logs USING btree (chain_id, emitting_address, (topics[1]), block_number, log_index) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_logs_canonical_replay_position_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_canonical_replay_position_idx ON public.raw_logs USING btree (chain_id, block_number, block_hash, transaction_index, log_index, raw_log_id) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_logs_canonical_rewind_observed_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_canonical_rewind_observed_idx ON public.raw_logs USING btree (chain_id, observed_at, block_number, block_hash) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_logs_canonical_topic_block_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_canonical_topic_block_idx ON public.raw_logs USING btree (chain_id, lower(topics[1]), block_number, transaction_index, log_index) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_logs_canonical_topic_node_emitter_block_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_canonical_topic_node_emitter_block_idx ON public.raw_logs USING btree (chain_id, (topics[1]), lower((topics[2])), emitting_address, block_number DESC, transaction_index DESC, log_index DESC, raw_log_id DESC) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_logs_noncanonical_replay_guard_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_logs_noncanonical_replay_guard_idx ON public.raw_logs USING btree (chain_id, block_hash) WHERE (canonicality_state <> ALL (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: raw_payload_cache_metadata_by_block_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_payload_cache_metadata_by_block_idx ON public.raw_payload_cache_metadata USING btree (chain_id, block_hash, payload_kind);


--
-- Name: raw_payload_cache_metadata_by_retained_digest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_payload_cache_metadata_by_retained_digest_idx ON public.raw_payload_cache_metadata USING btree (digest_algorithm, retained_digest) WHERE (retained_digest IS NOT NULL);


--
-- Name: raw_payload_cache_metadata_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_payload_cache_metadata_by_state_idx ON public.raw_payload_cache_metadata USING btree (chain_id, canonicality_state, block_number DESC, block_hash);


--
-- Name: raw_payload_cache_metadata_identity_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX raw_payload_cache_metadata_identity_idx ON public.raw_payload_cache_metadata USING btree (chain_id, block_hash, payload_kind, COALESCE(digest_algorithm, ''::text), COALESCE(retained_digest, ''::text));


--
-- Name: raw_receipts_by_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_receipts_by_hash_idx ON public.raw_receipts USING btree (chain_id, transaction_hash);


--
-- Name: raw_receipts_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_receipts_by_state_idx ON public.raw_receipts USING btree (chain_id, canonicality_state, block_number DESC, transaction_index DESC);


--
-- Name: raw_transactions_by_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_transactions_by_hash_idx ON public.raw_transactions USING btree (chain_id, transaction_hash);


--
-- Name: raw_transactions_by_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_transactions_by_state_idx ON public.raw_transactions USING btree (chain_id, canonicality_state, block_number DESC, transaction_index DESC);


--
-- Name: surface_bindings_logical_name_projection_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX surface_bindings_logical_name_projection_replay_idx ON public.surface_bindings USING btree (logical_name_id, active_from, active_to, surface_binding_id) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: surface_bindings_resource_projection_replay_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX surface_bindings_resource_projection_replay_idx ON public.surface_bindings USING btree (resource_id, active_from, active_to, logical_name_id, surface_binding_id) WHERE (canonicality_state = ANY (ARRAY['canonical'::public.canonicality_state, 'safe'::public.canonicality_state, 'finalized'::public.canonicality_state]));


--
-- Name: address_names_current address_names_current_logical_name_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.address_names_current
    ADD CONSTRAINT address_names_current_logical_name_id_fkey FOREIGN KEY (logical_name_id) REFERENCES public.name_surfaces(logical_name_id);


--
-- Name: address_names_current address_names_current_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.address_names_current
    ADD CONSTRAINT address_names_current_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(resource_id);


--
-- Name: address_names_current address_names_current_surface_binding_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.address_names_current
    ADD CONSTRAINT address_names_current_surface_binding_id_fkey FOREIGN KEY (surface_binding_id) REFERENCES public.surface_bindings(surface_binding_id);


--
-- Name: address_names_current address_names_current_token_lineage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.address_names_current
    ADD CONSTRAINT address_names_current_token_lineage_id_fkey FOREIGN KEY (token_lineage_id) REFERENCES public.token_lineages(token_lineage_id);


--
-- Name: backfill_ranges backfill_ranges_backfill_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backfill_ranges
    ADD CONSTRAINT backfill_ranges_backfill_job_id_fkey FOREIGN KEY (backfill_job_id) REFERENCES public.backfill_jobs(backfill_job_id) ON DELETE CASCADE;


--
-- Name: chain_header_audit chain_header_audit_chain_id_block_hash_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chain_header_audit
    ADD CONSTRAINT chain_header_audit_chain_id_block_hash_fkey FOREIGN KEY (chain_id, block_hash) REFERENCES public.chain_lineage(chain_id, block_hash) ON DELETE CASCADE;


--
-- Name: children_current children_current_child_logical_name_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.children_current
    ADD CONSTRAINT children_current_child_logical_name_id_fkey FOREIGN KEY (child_logical_name_id) REFERENCES public.name_surfaces(logical_name_id);


--
-- Name: children_current children_current_parent_logical_name_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.children_current
    ADD CONSTRAINT children_current_parent_logical_name_id_fkey FOREIGN KEY (parent_logical_name_id) REFERENCES public.name_surfaces(logical_name_id);


--
-- Name: contract_instance_addresses contract_instance_addresses_contract_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contract_instance_addresses
    ADD CONSTRAINT contract_instance_addresses_contract_instance_id_fkey FOREIGN KEY (contract_instance_id) REFERENCES public.contract_instances(contract_instance_id) ON DELETE CASCADE;


--
-- Name: contract_instance_addresses contract_instance_addresses_source_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contract_instance_addresses
    ADD CONSTRAINT contract_instance_addresses_source_manifest_id_fkey FOREIGN KEY (source_manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE SET NULL;


--
-- Name: discovery_edges discovery_edges_from_contract_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.discovery_edges
    ADD CONSTRAINT discovery_edges_from_contract_instance_id_fkey FOREIGN KEY (from_contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: discovery_edges discovery_edges_source_manifest_id_fkey1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.discovery_edges
    ADD CONSTRAINT discovery_edges_source_manifest_id_fkey1 FOREIGN KEY (source_manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE SET NULL;


--
-- Name: discovery_edges discovery_edges_to_contract_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.discovery_edges
    ADD CONSTRAINT discovery_edges_to_contract_instance_id_fkey FOREIGN KEY (to_contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: execution_cache_outcomes execution_cache_outcomes_execution_trace_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_cache_outcomes
    ADD CONSTRAINT execution_cache_outcomes_execution_trace_id_fkey FOREIGN KEY (execution_trace_id) REFERENCES public.execution_traces(execution_trace_id) ON DELETE CASCADE;


--
-- Name: execution_steps execution_steps_execution_trace_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_steps
    ADD CONSTRAINT execution_steps_execution_trace_id_fkey FOREIGN KEY (execution_trace_id) REFERENCES public.execution_traces(execution_trace_id) ON DELETE CASCADE;


--
-- Name: manifest_alert_observations manifest_alert_observations_contract_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_contract_instance_id_fkey FOREIGN KEY (contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: manifest_alert_observations manifest_alert_observations_discovery_edge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_discovery_edge_id_fkey FOREIGN KEY (discovery_edge_id) REFERENCES public.discovery_edges(discovery_edge_id) ON DELETE SET NULL;


--
-- Name: manifest_alert_observations manifest_alert_observations_expected_implementation_contra_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_expected_implementation_contra_fkey FOREIGN KEY (expected_implementation_contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: manifest_alert_observations manifest_alert_observations_observed_implementation_contra_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_observed_implementation_contra_fkey FOREIGN KEY (observed_implementation_contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: manifest_alert_observations manifest_alert_observations_proxy_contract_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_proxy_contract_instance_id_fkey FOREIGN KEY (proxy_contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: manifest_alert_observations manifest_alert_observations_source_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_alert_observations
    ADD CONSTRAINT manifest_alert_observations_source_manifest_id_fkey FOREIGN KEY (source_manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE SET NULL;


--
-- Name: manifest_capability_flags manifest_capability_flags_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_capability_flags
    ADD CONSTRAINT manifest_capability_flags_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE CASCADE;


--
-- Name: manifest_contract_instances manifest_contract_instances_contract_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_contract_instances
    ADD CONSTRAINT manifest_contract_instances_contract_instance_id_fkey FOREIGN KEY (contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: manifest_contract_instances manifest_contract_instances_implementation_contract_instan_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_contract_instances
    ADD CONSTRAINT manifest_contract_instances_implementation_contract_instan_fkey FOREIGN KEY (implementation_contract_instance_id) REFERENCES public.contract_instances(contract_instance_id);


--
-- Name: manifest_contract_instances manifest_contract_instances_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_contract_instances
    ADD CONSTRAINT manifest_contract_instances_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE CASCADE;


--
-- Name: manifest_discovery_rules manifest_discovery_rules_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.manifest_discovery_rules
    ADD CONSTRAINT manifest_discovery_rules_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE CASCADE;


--
-- Name: name_current name_current_logical_name_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.name_current
    ADD CONSTRAINT name_current_logical_name_id_fkey FOREIGN KEY (logical_name_id) REFERENCES public.name_surfaces(logical_name_id);


--
-- Name: name_current name_current_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.name_current
    ADD CONSTRAINT name_current_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(resource_id);


--
-- Name: name_current name_current_surface_binding_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.name_current
    ADD CONSTRAINT name_current_surface_binding_id_fkey FOREIGN KEY (surface_binding_id) REFERENCES public.surface_bindings(surface_binding_id);


--
-- Name: name_current name_current_token_lineage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.name_current
    ADD CONSTRAINT name_current_token_lineage_id_fkey FOREIGN KEY (token_lineage_id) REFERENCES public.token_lineages(token_lineage_id);


--
-- Name: normalized_events normalized_events_source_manifest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.normalized_events
    ADD CONSTRAINT normalized_events_source_manifest_id_fkey FOREIGN KEY (source_manifest_id) REFERENCES public.manifest_versions(manifest_id) ON DELETE SET NULL;


--
-- Name: permissions_current permissions_current_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.permissions_current
    ADD CONSTRAINT permissions_current_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(resource_id);


--
-- Name: record_inventory_current record_inventory_current_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.record_inventory_current
    ADD CONSTRAINT record_inventory_current_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(resource_id);


--
-- Name: resources resources_token_lineage_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_token_lineage_id_fkey FOREIGN KEY (token_lineage_id) REFERENCES public.token_lineages(token_lineage_id);


--
-- Name: surface_bindings surface_bindings_logical_name_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.surface_bindings
    ADD CONSTRAINT surface_bindings_logical_name_id_fkey FOREIGN KEY (logical_name_id) REFERENCES public.name_surfaces(logical_name_id);


--
-- Name: surface_bindings surface_bindings_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.surface_bindings
    ADD CONSTRAINT surface_bindings_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(resource_id);


--
-- PostgreSQL database dump complete
--
