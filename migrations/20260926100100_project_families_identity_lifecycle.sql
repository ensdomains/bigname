-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_name_state, project_binding_candidate, project_lifecycle_key_state,
-- project_lifecycle_triple_summary, project_lifecycle_association,
-- project_lifecycle_event, project_child_registration_state,
-- project_wrapper_state, project_registry_node_state,
-- project_registry_binding_observation (TYR-36 step 2). The tables are
-- additive and unread by every served path; the family loop fills them block
-- by block after each Project batch commits. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same tables.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_name_state (
    namespace text NOT NULL,
    logical_name_id text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    migration_path text,
    migration_evidence jsonb,
    migration_position jsonb,
    migrated_at timestamptz,
    authority_start_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (namespace, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_name_state IS
    'Project-owned name facts of family F1: the latest MigrationApplied of a name and the latest authority epoch start per authority arm (docs/projections.md, Owned key families). Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.namespace IS
    'This value is the name''s namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.logical_name_id IS
    'This value identifies the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.chain_id IS
    'This value is the chain whose events wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.migration_path IS
    'This value is the migration_path of the name''s latest MigrationApplied, as children.rs reads it; served as history, never a gate.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.migration_evidence IS
    'This value is that event''s evidence array.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.migration_position IS
    'This value is that event''s position as a JSON object of the four position fields.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.migrated_at IS
    'This value is that event''s block timestamp.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.authority_start_positions IS
    'This value maps each authority arm to the position of the name''s latest AuthorityEpochChanged in that arm.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_binding_candidate (
    surface_binding_id uuid NOT NULL,
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    authority_arm text NOT NULL,
    resource_id uuid NOT NULL,
    binding_kind text NOT NULL,
    canonicality_state text NOT NULL,
    active_from timestamptz,
    surface_namehash text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    state_derived boolean,
    authority_kind text,
    registry_only boolean NOT NULL DEFAULT false,
    predecessor_resource_id uuid,
    predecessor_position jsonb,
    lease_resource_id uuid,
    lease_position jsonb,
    wrapped_registrar_resource_id uuid,
    node text,
    transaction_hash text,
    emitting_address text,
    surface_bound_position jsonb,
    PRIMARY KEY (surface_binding_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_binding_candidate IS
    'Project-owned binding candidates of family F1: every surface binding of a name, selected or not, with the registry-only handoff facts and the wrapper facts the authority admission reads at publication. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.surface_binding_id IS
    'This value identifies the surface binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.logical_name_id IS
    'This value is the bound name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.namespace IS
    'This value is the name''s namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.chain_id IS
    'This value is the binding''s chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.authority_arm IS
    'This value is the binding''s authority arm.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.resource_id IS
    'This value is the bound resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.binding_kind IS
    'This value is the binding kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.canonicality_state IS
    'This value is the binding row''s canonicality when the block applied it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.active_from IS
    'This value is the binding''s active_from.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.surface_namehash IS
    'This value is the lower-cased namehash of the bound surface, which the direct-binding pass compares with a registrar event''s namehash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.block_number IS
    'This value is the block number of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.transaction_index IS
    'This value is the transaction index of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.log_index IS
    'This value is the log index of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.event_identity IS
    'This value is the event identity of the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.normalized_event_id IS
    'This value names the binding row: its block and the transaction and log index of its provenance; event_identity is the surface binding id, the tiebreak stage.rs uses in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.state_derived IS
    'This value is the state_derived flag of the latest SurfaceBound for this name and resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.authority_kind IS
    'This value is the authority_kind of that SurfaceBound.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.registry_only IS
    'This value is true when an AuthorityEpochChanged registry_only was seen on this name and resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.predecessor_resource_id IS
    'This value is the resource of the latest same-arm candidate positioned before a registry-only binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.predecessor_position IS
    'This value is that predecessor candidate''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.lease_resource_id IS
    'This value is the successor registrar lease a registry-only binding recorded.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.lease_position IS
    'This value is the position of the grant that created that lease.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.wrapped_registrar_resource_id IS
    'This value is the registrar lease a wrapper SurfaceBound for this name and resource recorded.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.node IS
    'This value is the lower-cased node of that wrapper SurfaceBound.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.transaction_hash IS
    'This value is that wrapper SurfaceBound''s transaction hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.emitting_address IS
    'This value is the lower-cased address that emitted that wrapper SurfaceBound.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.surface_bound_position IS
    'This value is the position of the latest SurfaceBound that set the flags and wrapper facts above.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_lifecycle_key_state (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    logical_name_id text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    last_grant jsonb,
    last_reservation jsonb,
    last_active jsonb,
    last_release_any jsonb,
    last_path_expiry jsonb,
    last_explicit_release jsonb,
    last_renewal jsonb,
    last_revival jsonb,
    last_expiry_changed jsonb,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_lifecycle_key_state IS
    'Project-owned lifecycle state of family F2a per resource: membership-only maxima over the resource''s own lifecycle events in the canonical event order. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.resource_id IS
    'This value is the lifecycle key, a resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.logical_name_id IS
    'This value is the name of the resource''s latest named lifecycle event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind, status and the registered_at source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_reservation IS
    'This value holds the latest RegistrationReserved: position, registrant, expiry and status.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_active IS
    'This value holds the kind and position of the later of last_grant and last_reservation.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_release_any IS
    'This value holds the position of the latest RegistrationReleased of any kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_path_expiry IS
    'This value holds the latest path-expiry release (RegistryPathExpired, interpreter_state, registry_name_binding_expired): position, released_at, expiry, source_event, derived_from, terminal_reason.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_explicit_release IS
    'This value holds the latest release that is not a path expiry: position and released_at; witnessing is computed at read.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_renewal IS
    'This value holds the latest RegistrationRenewed: position, expiry and revived_from_expiry.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_revival IS
    'This value holds the position of the latest RegistrationRenewed with revived_from_expiry applied after this key''s own path-expiry release; raw-resource domain, never merged.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_expiry_changed IS
    'This value holds the position of the latest ExpiryChanged; it feeds the five-kind selection only.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_lifecycle_triple_summary (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    registry_identifier text NOT NULL,
    token_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    last_grant jsonb,
    last_reservation jsonb,
    last_active jsonb,
    last_release_any jsonb,
    last_path_expiry jsonb,
    last_explicit_release jsonb,
    last_renewal jsonb,
    last_expiry_changed jsonb,
    PRIMARY KEY (chain_id, logical_name_id, registry_identifier, token_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_lifecycle_triple_summary IS
    'Project-owned lifecycle state of family F2a per (name, registry, token) triple: the same maxima over the triple''s null-resource ENSv2 lifecycle events only; a read merges it into the resource its association row targets. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.logical_name_id IS
    'This value is the name of the triple.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.registry_identifier IS
    'This value is COALESCE(registry_contract_instance_id, emitting address, registry) of the triple''s events.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.token_id IS
    'This value is the triple''s token id; empty text when the events carry none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind, status and the registered_at source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_reservation IS
    'This value holds the latest RegistrationReserved: position, registrant, expiry and status.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_active IS
    'This value holds the kind and position of the later of last_grant and last_reservation.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_release_any IS
    'This value holds the position of the latest RegistrationReleased of any kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_path_expiry IS
    'This value holds the latest path-expiry release (RegistryPathExpired, interpreter_state, registry_name_binding_expired): position, released_at, expiry, source_event, derived_from, terminal_reason.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_explicit_release IS
    'This value holds the latest release that is not a path expiry: position and released_at; witnessing is computed at read.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_renewal IS
    'This value holds the latest RegistrationRenewed: position, expiry and revived_from_expiry.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_expiry_changed IS
    'This value holds the position of the latest ExpiryChanged; it feeds the five-kind selection only.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_lifecycle_association (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    registry_identifier text NOT NULL,
    token_id text NOT NULL,
    target_resource_id uuid NOT NULL,
    event_kind text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    PRIMARY KEY (chain_id, logical_name_id, registry_identifier, token_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_lifecycle_association IS
    'Project-owned lifecycle association of family F2a: per triple, the resource of the latest resource-bearing RegistrationGranted or RegistrationReserved in the canonical event order. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.logical_name_id IS
    'This value is the name of the triple.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.registry_identifier IS
    'This value is COALESCE(registry_contract_instance_id, emitting address, registry) of the triple''s events.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.token_id IS
    'This value is the triple''s token id; empty text when the events carry none.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.target_resource_id IS
    'This value is the resource the triple''s null-resource events currently belong to.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.event_kind IS
    'This value is the kind of the winning grant or reservation.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.block_number IS
    'This value is the block number of the winning grant or reservation.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.transaction_index IS
    'This value is the transaction index of the winning grant or reservation; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.log_index IS
    'This value is the log index of the winning grant or reservation; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.event_identity IS
    'This value is the event identity of the winning grant or reservation, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_association.normalized_event_id IS
    'This value names the winning grant or reservation in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_lifecycle_event (
    chain_id text NOT NULL,
    state_kind text NOT NULL,
    state_key text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    original_logical_name_id text,
    decoded_logical_name_id text,
    resource_id uuid,
    source_family text NOT NULL,
    authority_kind text,
    transaction_hash text,
    to_address text,
    namehash text,
    registrant text,
    before_registrant text,
    expiry jsonb,
    expiry_seconds bigint,
    status text,
    released_at jsonb,
    source_event text,
    derived_from text,
    terminal_reason text,
    revived_from_expiry boolean,
    state_derived boolean,
    surface_materialization boolean,
    registrar_surface_snapshot boolean,
    original_registered_at bigint,
    owner_getter text,
    owner_word_unmasked boolean,
    registry_owner text,
    PRIMARY KEY (chain_id, state_kind, state_key, event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (state_kind IN ('resource', 'triple'))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_lifecycle_event IS
    'Project-owned retained lifecycle events of family F2a: every RegistrationGranted, RegistrationRenewed, RegistrationReleased, RegistrationReserved, ExpiryChanged and TokenControlTransferred of a resource or triple, keyed by position, with the reader fields and admission evidence the authority-admitted readers consume. Unpruned; a row leaves only when undo removes its block. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.state_kind IS
    'This value is resource for an event of a resource key and triple for a null-resource event of a triple.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.state_key IS
    'This value is the resource id, or the triple as a JSON array of name, registry identifier and token id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.event_kind IS
    'This value is the event kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.original_logical_name_id IS
    'This value is the logical name the adapter emitted, null when it emitted the event unnamed; it is never rewritten.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.decoded_logical_name_id IS
    'This value is the name the two staging passes attached at write time; informational, the read recomputes it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.resource_id IS
    'This value is the event''s resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.source_family IS
    'This value is the event''s source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.authority_kind IS
    'This value is COALESCE(NULLIF(after_state authority_kind, ''''), ''registrar'').'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.transaction_hash IS
    'This value is the event''s transaction hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.to_address IS
    'This value is the lower-cased recipient of a TokenControlTransferred.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.namehash IS
    'This value is the lower-cased namehash the event carries.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.registrant IS
    'This value is the lower-cased after-state registrant.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.before_registrant IS
    'This value is the lower-cased before-state registrant of a RegistrationReleased.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.expiry IS
    'This value is the after-state expiry as the event carries it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.expiry_seconds IS
    'This value is that expiry as seconds when it is an integral JSON number within the served range, else null.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.status IS
    'This value is the after-state status.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.released_at IS
    'This value is the after-state released_at.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.source_event IS
    'This value is the after-state source_event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.derived_from IS
    'This value is the after-state derived_from.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.terminal_reason IS
    'This value is the after-state terminal_reason.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.revived_from_expiry IS
    'This value is the after-state revived_from_expiry.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.state_derived IS
    'This value is the after-state state_derived.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.surface_materialization IS
    'This value is the after-state surface_materialization.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.registrar_surface_snapshot IS
    'This value is the after-state registrar_surface_snapshot.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.original_registered_at IS
    'This value is the after-state original_registered_at in seconds.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.owner_getter IS
    'This value is the lower-cased after-state owner_getter.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.owner_word_unmasked IS
    'This value is the after-state owner_word_unmasked.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.registry_owner IS
    'This value is the lower-cased after-state registry_owner.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_child_registration_state (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    registry_contract_instance_id text NOT NULL,
    event_kind text,
    registrant text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    exists boolean NOT NULL DEFAULT false,
    PRIMARY KEY (chain_id, logical_name_id, registry_contract_instance_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_child_registration_state IS
    'Project-owned per-registry child registration row of family F2a: the latest RegistrationGranted, RegistrationRenewed or RegistrationReleased of a name for one registry contract instance, and whether any reservation, grant or renewal exists there. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.logical_name_id IS
    'This value is the child name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.registry_contract_instance_id IS
    'This value is the registry contract instance the events carry.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.event_kind IS
    'This value is the kind of the latest granted, renewed or released event; null when only a reservation exists.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.registrant IS
    'This value is that event''s lower-cased registrant.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.block_number IS
    'This value is the block number of the latest event of the three kinds, or the first reservation when none exists.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.transaction_index IS
    'This value is the transaction index of the latest event of the three kinds, or the first reservation when none exists; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.log_index IS
    'This value is the log index of the latest event of the three kinds, or the first reservation when none exists; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.event_identity IS
    'This value is the event identity of the latest event of the three kinds, or the first reservation when none exists, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.normalized_event_id IS
    'This value names the latest event of the three kinds, or the first reservation when none exists in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_registration_state.exists IS
    'This value is true once any reservation, grant or renewal carries this registry.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_wrapper_state (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    logical_name_id text,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    wrapper_state text,
    fuses bigint,
    wrapper_state_position jsonb,
    expiry_seconds numeric,
    expiry_position jsonb,
    owner_word_unmasked boolean,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_wrapper_state IS
    'Project-owned wrapper state of family F2b per wrapper resource: the latest wrapper_state and fuses and the latest wrapper expiry, unmasked; masks are applied at read against the block clock. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.resource_id IS
    'This value is the wrapper resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.logical_name_id IS
    'This value is the wrapped name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.wrapper_state IS
    'This value is wrapped, emancipated or locked from the latest PermissionScopeChanged; null for any other value.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.fuses IS
    'This value is that event''s fuses when a JSON number from 0 to 4294967295.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.wrapper_state_position IS
    'This value is that PermissionScopeChanged''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.expiry_seconds IS
    'This value is the latest wrapper expiry: a JSON number from 0 to 18446744073709551615.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.expiry_position IS
    'This value is that ExpiryChanged''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.owner_word_unmasked IS
    'This value is the latest owner_word_unmasked flag the wrapper events carried.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_registry_node_state (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    owner text,
    owner_getter text,
    owner_getter_reason text,
    owner_word_unmasked boolean,
    registry_owner text,
    emitter_role text,
    registry_contract text,
    has_old_record boolean NOT NULL DEFAULT false,
    first_current_record_block bigint,
    PRIMARY KEY (chain_id, namespace, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_node_state IS
    'Project-owned registry ownership of family F2c per ENSv1 or Basenames registry node: the latest owner with the zero-owner override facts, and the registry generation facts. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.node IS
    'This value is the lower-cased node the event addresses: child_node, else node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner IS
    'This value is the lower-cased after-state owner.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_getter IS
    'This value is the after-state owner_getter as written.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_getter_reason IS
    'This value is the after-state owner_getter_reason.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_word_unmasked IS
    'This value is the after-state owner_word_unmasked.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.registry_owner IS
    'This value is the lower-cased after-state registry_owner.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.emitter_role IS
    'This value is the after-state emitter_role of the latest event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.registry_contract IS
    'This value is the lower-cased emitting address, else the after-state registry_contract.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.has_old_record IS
    'This value is true once any event with emitter_role registry_old addressed the node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.first_current_record_block IS
    'This value is the first block with an emitter_role registry event for the node.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_registry_binding_observation (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    attributed_via text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    registry_owner text,
    registry_contract text,
    provenance jsonb,
    applicable boolean NOT NULL,
    clear_event_identity text,
    PRIMARY KEY (chain_id, resource_id, attributed_via),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (attributed_via IN ('own', 'name'))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_binding_observation IS
    'Project-owned registry binding observations of family F2c: per resource and attribution, the latest AuthorityTransferred, SubregistryChanged, SurfaceBound or SurfaceUnbound observation; a read takes the latest of the two attributions. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.resource_id IS
    'This value is the observed resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.attributed_via IS
    'This value is own when the event''s resource is the resource and name when the event reached it through the name''s current resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.event_kind IS
    'This value is the observation''s event kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.registry_owner IS
    'This value is the lower-cased owner_getter; null after SurfaceUnbound.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.registry_contract IS
    'This value is the lower-cased registry contract the observation names.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.provenance IS
    'This value is the observation''s source family, event kind and namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.applicable IS
    'This value is true when owner and contract are well-formed addresses and the owner is not zero.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.clear_event_identity IS
    'This value is the event identity when the observation is not applicable.'
$ddl$;
END
$migration$;
