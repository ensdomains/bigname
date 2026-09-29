CREATE SEQUENCE IF NOT EXISTS reverse_hydration_attempt_ordinal_seq AS bigint;

COMMENT ON SEQUENCE reverse_hydration_attempt_ordinal_seq IS
    'This sequence assigns durable order to reverse-name polling batches; its values are not serving data.';


CREATE TABLE IF NOT EXISTS child_registration_events (
    parent_logical_name_id text NOT NULL,
    event_identity text NOT NULL,
    child_logical_name_id text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_order_key bigint NOT NULL,
    log_order_key bigint NOT NULL,
    event_kind text NOT NULL,
    manifest_version bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    target_block_number bigint NOT NULL,
    target_block_hash text NOT NULL,
    PRIMARY KEY (parent_logical_name_id, event_identity),
    CHECK (parent_logical_name_id <> child_logical_name_id),
    CHECK (btrim(namespace) <> ''),
    CONSTRAINT child_registration_events_same_namespace_check
        CHECK (
            starts_with(parent_logical_name_id, namespace || ':')
            AND starts_with(child_logical_name_id, namespace || ':')
        ),
    CHECK (btrim(event_identity) <> ''),
    CHECK (btrim(chain_id) <> ''),
    CHECK (block_number >= 0),
    CHECK (btrim(block_hash) <> ''),
    CHECK (transaction_order_key >= -1),
    CHECK (log_order_key >= -1),
    CHECK (event_kind IN ('RegistrationGranted', 'LabelRegistered')),
    CHECK (manifest_version >= 0),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (target_block_number >= block_number),
    CHECK (btrim(target_block_hash) <> '')
);

CREATE INDEX IF NOT EXISTS child_registration_events_parent_history_idx
    ON child_registration_events (
        parent_logical_name_id,
        chain_id,
        block_number,
        block_hash,
        transaction_order_key,
        log_order_key,
        event_identity
    );

CREATE INDEX IF NOT EXISTS child_registration_events_chain_block_idx
    ON child_registration_events (chain_id, block_number);

COMMENT ON TABLE child_registration_events IS
    'Project-owned historical membership of direct child registration events: one row per parent name and registration event of a name exactly one label below it. Rebuilt from canonical normalized events and name surfaces; event payloads stay in normalized_events.';
COMMENT ON COLUMN child_registration_events.parent_logical_name_id IS
    'This value identifies the parent name: the event namespace and the namehash of the child surface labels without its first label.';
COMMENT ON COLUMN child_registration_events.event_identity IS
    'This value identifies the registration event in normalized_events; name history joins the event by it.';
COMMENT ON COLUMN child_registration_events.child_logical_name_id IS
    'This value identifies the child name the event carried when it happened.';
COMMENT ON COLUMN child_registration_events.namespace IS
    'This value identifies the name system shared by the parent and the child.';
COMMENT ON COLUMN child_registration_events.chain_id IS
    'This value identifies the chain of the event and of the child surface.';
COMMENT ON COLUMN child_registration_events.block_number IS
    'This value is the event block height, the first history order key.';
COMMENT ON COLUMN child_registration_events.block_hash IS
    'This value is the event block hash, a history order key and the readable-lineage check.';
COMMENT ON COLUMN child_registration_events.transaction_order_key IS
    'This value is the event transaction index in its block, or -1 when the event has none, so it orders as history orders a missing index.';
COMMENT ON COLUMN child_registration_events.log_order_key IS
    'This value is the event log index, or -1 when the event has none, so it orders as history orders a missing index.';
COMMENT ON COLUMN child_registration_events.event_kind IS
    'This value is the stored registration kind of the event.';
COMMENT ON COLUMN child_registration_events.manifest_version IS
    'This value records the manifest version that admitted the event.';
COMMENT ON COLUMN child_registration_events.provenance IS
    'This object cites the normalized event row and source family the membership was derived from.';
COMMENT ON COLUMN child_registration_events.target_block_number IS
    'This value identifies the Project target height of the publication that wrote the row.';
COMMENT ON COLUMN child_registration_events.target_block_hash IS
    'This value identifies the Project target hash of the publication that wrote the row.';
COMMENT ON INDEX child_registration_events_parent_history_idx IS
    'This bounded index serves one parent''s child registrations in history order on one chain, in both directions. Every key is a bounded identifier, hash or number.';
COMMENT ON INDEX child_registration_events_chain_block_idx IS
    'This bounded index lets Project replace one chain''s rows by block range.';

-- Owned key families: the Project-owned tables the API serves, written block by block.
-- docs/projections.md, "Owned key families".

CREATE TABLE IF NOT EXISTS project_family_marker (
    chain_id text NOT NULL,
    current_block_number bigint,
    current_block_hash text,
    block_timestamp timestamptz,
    input_content_hash text,
    sequence bigint NOT NULL DEFAULT 0,
    interpret_input_content_hash text,
    interpret_redo_attempt bigint,
    state text NOT NULL,
    interpret_redo_in_progress boolean,
    project_redo_attempt bigint,
    project_redo_mode text,
    project_redo_from bigint,
    project_redo_to bigint,
    admission_manifests text,
    PRIMARY KEY (chain_id),
    CHECK ((current_block_number IS NULL) = (current_block_hash IS NULL)),
    CHECK (state IN ('live', 'bootstrap_pending')),
    CHECK (sequence >= 0)
);
COMMENT ON TABLE project_family_marker IS
    'Project-owned marker of the owned key families: the last block the family loop applied on each chain, the generation every family block and family undo advances, and the input revision it read. Readers serve the publication it names; chain_phase_state keeps the Project phase progress.';
COMMENT ON COLUMN project_family_marker.chain_id IS
    'This value is the chain the marker belongs to.';
COMMENT ON COLUMN project_family_marker.current_block_number IS
    'This value is the last block whose facts the families hold; null before the first block.';
COMMENT ON COLUMN project_family_marker.current_block_hash IS
    'This value is the readable hash that block had when it was applied.';
COMMENT ON COLUMN project_family_marker.block_timestamp IS
    'This value is that block''s timestamp from chain_lineage, the block clock the family reads will use.';
COMMENT ON COLUMN project_family_marker.input_content_hash IS
    'This value is the interpreter content hash of the binary that applied the block.';
COMMENT ON COLUMN project_family_marker.sequence IS
    'This value counts every family block and every family undo applied on the chain; it only grows. It is the explicit publication generation of the design, named so because schema-v2 reserves generation for authorised columns.';
COMMENT ON COLUMN project_family_marker.interpret_input_content_hash IS
    'This value is the Interpret row''s input_content_hash the last block read inside its own transaction, the first half of the input revision.';
COMMENT ON COLUMN project_family_marker.interpret_redo_attempt IS
    'This value is the Interpret row''s redo_attempt_generation the last block read inside its own transaction, the second half of the input revision.';
COMMENT ON COLUMN project_family_marker.state IS
    'This value is live when the families hold a complete publication and bootstrap_pending while a rebuild is populating the families.';
COMMENT ON COLUMN project_family_marker.interpret_redo_in_progress IS
    'This value is the Interpret row''s redo_in_progress the last block read; always false after a block, since no block applies while Interpret is in redo, and null on a reset marker.';
COMMENT ON COLUMN project_family_marker.project_redo_attempt IS
    'This value is the Project row''s redo_attempt_generation the last block read inside its own transaction.';
COMMENT ON COLUMN project_family_marker.project_redo_mode IS
    'This value is the Project row''s redo_mode the last block read, null when no redo was open.';
COMMENT ON COLUMN project_family_marker.project_redo_from IS
    'This value is the Project row''s redo_from_block_number the last block read.';
COMMENT ON COLUMN project_family_marker.project_redo_to IS
    'This value is the Project row''s redo_to_block_number the last block read.';
COMMENT ON COLUMN project_family_marker.admission_manifests IS
    'This value is the key of the active manifest set the last block classified under: manifest_id:event_id of the latest SourceManifestUpdated event of every manifest the chain reads, at or below the block or with no block. A family run reads the manifest updates once, so an update written during a run applies from the next run; a block that sees another key classifies every stored resolver again. An update with no block applies to every block, so it is not tied to the block it was written at.';

CREATE TABLE IF NOT EXISTS project_family_undo (
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    family text NOT NULL,
    key text NOT NULL,
    before_image jsonb,
    PRIMARY KEY (chain_id, block_number, family, key),
    CHECK (btrim(block_hash) <> '')
);
COMMENT ON TABLE project_family_undo IS
    'Project-owned undo record of the owned key families: per applied block, the image each family row had before the block first changed it, plus the prior marker under family marker. Undoing a block restores these images. Rows are kept back to the lowest of 256 blocks below the marker, the finalized block, the safe block and an active repair''s floor; with no finalized or safe head nothing is pruned, so the journal grows by every block until the heads appear and is then pruned in one delete.';
COMMENT ON COLUMN project_family_undo.chain_id IS
    'This value is the chain of the block.';
COMMENT ON COLUMN project_family_undo.block_number IS
    'This value is the block whose change the row undoes.';
COMMENT ON COLUMN project_family_undo.block_hash IS
    'This value is the readable hash the block had when it was applied.';
COMMENT ON COLUMN project_family_undo.family IS
    'This value names the family table of the row, or marker for the prior family marker.';
COMMENT ON COLUMN project_family_undo.key IS
    'This value is the row''s primary key as a JSON array in key column order, or the chain id for the marker.';
COMMENT ON COLUMN project_family_undo.before_image IS
    'This value is to_jsonb of the row before the block, or null when the row did not exist.';

CREATE TABLE IF NOT EXISTS project_repair_record (
    chain_id text NOT NULL,
    attempt bigint NOT NULL,
    reason text NOT NULL,
    trusted_base_number bigint,
    trusted_base_hash text,
    replay_target_number bigint NOT NULL,
    replay_target_hash text NOT NULL,
    state text NOT NULL,
    prefix_interpret_input_content_hash text,
    prefix_interpret_redo_attempt bigint,
    invalidation_from bigint,
    pending_undo_target bigint,
    completed_sequence bigint,
    completed_marker_number bigint,
    completed_marker_hash text,
    completed_input_hash text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    prefix_recorded boolean NOT NULL DEFAULT false,
    reset_sequence bigint,
    PRIMARY KEY (chain_id),
    CHECK (reason IN ('required_redo_range', 'orphaned_lineage', 'content_hash_rebuild', 'operator_redo')),
    CHECK (state IN ('undoing', 'replaying', 'rebuilding', 'complete')),
    CHECK ((state = 'complete') = (completed_sequence IS NOT NULL AND completed_marker_number IS NOT NULL AND completed_marker_hash IS NOT NULL AND completed_input_hash IS NOT NULL)),
    CHECK (state = 'complete' OR (completed_sequence IS NULL AND completed_marker_number IS NULL AND completed_marker_hash IS NULL AND completed_input_hash IS NULL)),
    CHECK (state <> 'undoing' OR (prefix_interpret_input_content_hash IS NULL AND prefix_interpret_redo_attempt IS NULL)),
    CHECK ((trusted_base_number IS NULL) = (trusted_base_hash IS NULL)),
    CHECK (state <> 'rebuilding' OR trusted_base_number IS NULL),
    CONSTRAINT project_repair_record_prefix_recorded_check
        CHECK (state <> 'undoing' OR NOT prefix_recorded)
);
COMMENT ON TABLE project_repair_record IS
    'Project-owned repair record: the durable description of the latest family undo-then-replay or rebuild of a chain, its attempt, reason, trusted base, replay target, state, input revision and completion identity. Undo never rewrites it.';
COMMENT ON COLUMN project_repair_record.chain_id IS
    'This value is the chain under repair.';
COMMENT ON COLUMN project_repair_record.attempt IS
    'This value is the Project row''s redo_attempt_generation when the repair began.';
COMMENT ON COLUMN project_repair_record.reason IS
    'This value says why the families are repaired: a required redo range, an orphaned lineage, a content-hash rebuild or an operator redo.';
COMMENT ON COLUMN project_repair_record.trusted_base_number IS
    'This value is the block below the repaired range whose facts stay; null for a rebuild.';
COMMENT ON COLUMN project_repair_record.trusted_base_hash IS
    'This value is the trusted base''s readable hash.';
COMMENT ON COLUMN project_repair_record.replay_target_number IS
    'This value is the block the replay must reach, captured before the first undo.';
COMMENT ON COLUMN project_repair_record.replay_target_hash IS
    'This value is the replay target''s hash when captured.';
COMMENT ON COLUMN project_repair_record.state IS
    'This value is undoing, replaying, rebuilding or complete.';
COMMENT ON COLUMN project_repair_record.prefix_interpret_input_content_hash IS
    'This value is the Interpret input_content_hash of the input revision the replay started from; null while undoing.';
COMMENT ON COLUMN project_repair_record.prefix_interpret_redo_attempt IS
    'This value is the Interpret redo_attempt_generation of that input revision; null while undoing.';
COMMENT ON COLUMN project_repair_record.invalidation_from IS
    'This value is the lowest block a stamp invalidated while the repair was active; step 2 never sets it.';
COMMENT ON COLUMN project_repair_record.pending_undo_target IS
    'This value is the block the undo must reach before replay may start; null once replay starts.';
COMMENT ON COLUMN project_repair_record.completed_sequence IS
    'This value is the family marker sequence the completing block produced; null until complete.';
COMMENT ON COLUMN project_repair_record.completed_marker_number IS
    'This value is the family marker block when the repair completed; null until complete.';
COMMENT ON COLUMN project_repair_record.completed_marker_hash IS
    'This value is the family marker hash when the repair completed; null until complete.';
COMMENT ON COLUMN project_repair_record.completed_input_hash IS
    'This value is the interpreter content hash the completing loop ran under; null until complete.';
COMMENT ON COLUMN project_repair_record.updated_at IS
    'This value is when the record last changed.';
COMMENT ON COLUMN project_repair_record.reset_sequence IS
    'This value is the family marker generation the rebuild''s reset wrote; null for an undo-then-replay.';
COMMENT ON COLUMN project_repair_record.prefix_recorded IS
    'This value is true once the replay or rebuild captured its input revision in prefix_interpret_input_content_hash and prefix_interpret_redo_attempt, which may both be null when the chain has no Interpret row; false while undoing.';

CREATE TABLE IF NOT EXISTS project_name_state (
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
    PRIMARY KEY (chain_id, namespace, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_name_state IS
    'Project-owned name facts of family F1: the latest MigrationApplied of a name and the latest authority epoch start per authority arm (docs/projections.md, Owned key families).';
COMMENT ON COLUMN project_name_state.namespace IS
    'This value is the name''s namespace.';
COMMENT ON COLUMN project_name_state.logical_name_id IS
    'This value identifies the name.';
COMMENT ON COLUMN project_name_state.chain_id IS
    'This value is the chain whose events wrote the row; each chain keeps its own row for a name.';
COMMENT ON COLUMN project_name_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_name_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_name_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_name_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_name_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_name_state.migration_path IS
    'This value is the migration_path of the name''s latest MigrationApplied, as children.rs reads it; served as history, never a gate.';
COMMENT ON COLUMN project_name_state.migration_evidence IS
    'This value is that event''s evidence array.';
COMMENT ON COLUMN project_name_state.migration_position IS
    'This value is that event''s position as a JSON object of the four position fields.';
COMMENT ON COLUMN project_name_state.migrated_at IS
    'This value is that event''s block timestamp.';
COMMENT ON COLUMN project_name_state.authority_start_positions IS
    'This value maps each authority arm to the position of the name''s latest AuthorityEpochChanged in that arm, with its authority_kind, authority_key, resource and the owner it reports to the served control block.';

CREATE TABLE IF NOT EXISTS project_binding_candidate (
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
    authority_key text,
    predecessor_wrapped_registrar_resource_id uuid,
    predecessor_node text,
    bound_owner text,
    PRIMARY KEY (surface_binding_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_binding_candidate IS
    'Project-owned binding candidates of family F1: every surface binding of a name, selected or not, with the registry-only handoff facts and the wrapper facts the authority admission reads at publication.';
COMMENT ON COLUMN project_binding_candidate.surface_binding_id IS
    'This value identifies the surface binding. It orders candidates only after the whole position: two bindings of one name at the same position with no transaction or log (synthesised) order by event_identity and then this id, where the served selection orders equal (block, transaction, log) by surface_binding_id descending without the identity.';
COMMENT ON COLUMN project_binding_candidate.logical_name_id IS
    'This value is the bound name.';
COMMENT ON COLUMN project_binding_candidate.namespace IS
    'This value is the name''s namespace.';
COMMENT ON COLUMN project_binding_candidate.chain_id IS
    'This value is the binding''s chain.';
COMMENT ON COLUMN project_binding_candidate.authority_arm IS
    'This value is the binding''s authority arm.';
COMMENT ON COLUMN project_binding_candidate.resource_id IS
    'This value is the bound resource.';
COMMENT ON COLUMN project_binding_candidate.binding_kind IS
    'This value is the binding kind.';
COMMENT ON COLUMN project_binding_candidate.canonicality_state IS
    'This value is the binding row''s canonicality when the block applied it.';
COMMENT ON COLUMN project_binding_candidate.active_from IS
    'This value is the binding''s active_from.';
COMMENT ON COLUMN project_binding_candidate.surface_namehash IS
    'This value is the lower-cased namehash of the bound surface, which the direct-binding pass compares with a registrar event''s namehash.';
COMMENT ON COLUMN project_binding_candidate.block_number IS
    'This value is the block number of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>.';
COMMENT ON COLUMN project_binding_candidate.transaction_index IS
    'This value is the transaction index of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. Null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_binding_candidate.log_index IS
    'This value is the log index of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. Null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_binding_candidate.event_identity IS
    'This value is the event identity of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. It is the final tiebreak of the canonical event order, compared as bytes; two bindings one event opened are ordered by surface_binding_id. The adapter materializes one raw log''s events and bindings together (adapters schema_v2/session.rs:490 and :512) and stamps each log-sourced binding with that log''s provenance (schema_v2/identity.rs:229 and :329); a block-boundary binding and its SurfaceBound come from one block with no transaction or log (identity/boundary.rs:137). The families assume, as an adapter precondition, that an identity binding:<surface_binding_id> means the adapter''s reconcile dropped the SurfaceBound (schema_v2/protocol/v1/reconcile_support.rs:42-43), not that the SurfaceBound sits at another position; the cited lines show that a binding and its SurfaceBound share provenance, not that every binding has an opener. If the precondition fails, the family positions the binding at its own block and provenance index under the identity binding:<surface_binding_id>, with no error and no anomaly count.';
COMMENT ON COLUMN project_binding_candidate.normalized_event_id IS
    'This value names the SurfaceBound that opened the binding in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_binding_candidate.state_derived IS
    'This value is the state_derived flag of the SurfaceBound that opened the binding.';
COMMENT ON COLUMN project_binding_candidate.authority_kind IS
    'This value is the authority_kind of the SurfaceBound that opened the binding.';
COMMENT ON COLUMN project_binding_candidate.registry_only IS
    'This value is true once an AuthorityEpochChanged registry_only was seen on this name and resource, in the binding''s block or later. An epoch at an earlier block than the binding does not set it, where the served REGISTRY_ONLY_HANDOFFS (name_authority/stage.rs:127-134) takes an epoch on the name and resource at any position.';
COMMENT ON COLUMN project_binding_candidate.predecessor_resource_id IS
    'This value is the resource of the latest candidate of the same name and arm positioned before a registry-only binding.';
COMMENT ON COLUMN project_binding_candidate.predecessor_position IS
    'This value is that predecessor candidate''s position.';
COMMENT ON COLUMN project_binding_candidate.lease_resource_id IS
    'This value is the lease a registry-only handoff stands for (stage.rs:47-135): the latest ens_v1 registrar grant of the name after the binding, on another resource, with a registrar release of the predecessor''s resource before it; else the predecessor''s resource.';
COMMENT ON COLUMN project_binding_candidate.lease_position IS
    'This value is the position of that successor grant, else the predecessor''s position.';
COMMENT ON COLUMN project_binding_candidate.wrapped_registrar_resource_id IS
    'This value is the registrar lease the NameWrapper SurfaceBound that opened the binding recorded.';
COMMENT ON COLUMN project_binding_candidate.node IS
    'This value is the lower-cased node of that NameWrapper SurfaceBound.';
COMMENT ON COLUMN project_binding_candidate.transaction_hash IS
    'This value is that NameWrapper SurfaceBound''s transaction hash.';
COMMENT ON COLUMN project_binding_candidate.emitting_address IS
    'This value is the lower-cased address that emitted that NameWrapper SurfaceBound.';
COMMENT ON COLUMN project_binding_candidate.surface_bound_position IS
    'This value is the position of the SurfaceBound that opened the binding, null when none did.';
COMMENT ON COLUMN project_binding_candidate.authority_key IS
    'This value is the authority_key of the SurfaceBound that opened the binding.';
COMMENT ON COLUMN project_binding_candidate.predecessor_wrapped_registrar_resource_id IS
    'This value is the registrar lease the handoff''s predecessor recorded when the predecessor is a NameWrapper binding, the lease authority_events.sql:164-186 admits registrar grants and releases of.';
COMMENT ON COLUMN project_binding_candidate.predecessor_node IS
    'This value is the lower-cased node the handoff''s predecessor recorded when it is a NameWrapper binding.';
COMMENT ON COLUMN project_binding_candidate.bound_owner IS
    'This value is the owner the SurfaceBound that opened the binding reports to the served control block (name_current/build.sql:650-671): null when its owner word is unmasked, else its registry_owner, else its owner, lower-cased; its position is surface_bound_position.';
CREATE INDEX IF NOT EXISTS project_binding_candidate_wrapped_lease_idx
    ON project_binding_candidate (chain_id, wrapped_registrar_resource_id)
    WHERE wrapped_registrar_resource_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_binding_candidate_resource_idx
    ON project_binding_candidate (chain_id, resource_id);
CREATE INDEX IF NOT EXISTS project_binding_candidate_name_idx
    ON project_binding_candidate (chain_id, logical_name_id);
-- The name summary writer's work list (crates/project families/derived/summary.rs).
CREATE INDEX IF NOT EXISTS project_binding_candidate_predecessor_idx
    ON project_binding_candidate (chain_id, predecessor_resource_id)
    WHERE predecessor_resource_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_binding_candidate_lease_idx
    ON project_binding_candidate (chain_id, lease_resource_id)
    WHERE lease_resource_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS project_lifecycle_key_state (
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
);
COMMENT ON TABLE project_lifecycle_key_state IS
    'Project-owned lifecycle state of family F2a per resource: membership-only maxima over the resource''s own lifecycle events in the canonical event order.';
COMMENT ON COLUMN project_lifecycle_key_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_key_state.resource_id IS
    'This value is the lifecycle key, a resource.';
COMMENT ON COLUMN project_lifecycle_key_state.logical_name_id IS
    'This value is the name of the resource''s latest named lifecycle event.';
COMMENT ON COLUMN project_lifecycle_key_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_lifecycle_key_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_key_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_key_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_key_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_lifecycle_key_state.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind and authority_key as the payload has them (null when absent), status and the registered_at source.';
COMMENT ON COLUMN project_lifecycle_key_state.last_reservation IS
    'This value holds the latest RegistrationReserved: position, registrant, expiry and status.';
COMMENT ON COLUMN project_lifecycle_key_state.last_active IS
    'This value holds the kind and position of the later of last_grant and last_reservation.';
COMMENT ON COLUMN project_lifecycle_key_state.last_release_any IS
    'This value holds the position of the latest RegistrationReleased of any kind.';
COMMENT ON COLUMN project_lifecycle_key_state.last_path_expiry IS
    'This value holds the latest path-expiry release (RegistryPathExpired, interpreter_state, registry_name_binding_expired): position, released_at, expiry, source_event, derived_from, terminal_reason.';
COMMENT ON COLUMN project_lifecycle_key_state.last_explicit_release IS
    'This value holds the latest release that is not a path expiry: position and released_at; witnessing is computed at read.';
COMMENT ON COLUMN project_lifecycle_key_state.last_renewal IS
    'This value holds the latest RegistrationRenewed: position, expiry and revived_from_expiry.';
COMMENT ON COLUMN project_lifecycle_key_state.last_revival IS
    'This value holds the position of the latest RegistrationRenewed with revived_from_expiry applied after this key''s own path-expiry release; raw-resource domain, never merged.';
COMMENT ON COLUMN project_lifecycle_key_state.last_expiry_changed IS
    'This value holds the position of the latest ExpiryChanged; it feeds the five-kind selection only.';

CREATE TABLE IF NOT EXISTS project_lifecycle_triple_summary (
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
);
COMMENT ON TABLE project_lifecycle_triple_summary IS
    'Project-owned lifecycle state of family F2a per (name, registry, token) triple: the same maxima over the triple''s null-resource ENSv2 lifecycle events only; a read merges it into the resource its association row targets.';
COMMENT ON COLUMN project_lifecycle_triple_summary.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_triple_summary.logical_name_id IS
    'This value is the name of the triple.';
COMMENT ON COLUMN project_lifecycle_triple_summary.registry_identifier IS
    'This value is COALESCE(registry_contract_instance_id, emitting address, registry) of the triple''s events.';
COMMENT ON COLUMN project_lifecycle_triple_summary.token_id IS
    'This value is the triple''s token id; empty text when the events carry none.';
COMMENT ON COLUMN project_lifecycle_triple_summary.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_lifecycle_triple_summary.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_triple_summary.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_triple_summary.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_triple_summary.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind and authority_key as the payload has them (null when absent), status and the registered_at source.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_reservation IS
    'This value holds the latest RegistrationReserved: position, registrant, expiry and status.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_active IS
    'This value holds the kind and position of the later of last_grant and last_reservation.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_release_any IS
    'This value holds the position of the latest RegistrationReleased of any kind.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_path_expiry IS
    'This value holds the latest path-expiry release (RegistryPathExpired, interpreter_state, registry_name_binding_expired): position, released_at, expiry, source_event, derived_from, terminal_reason.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_explicit_release IS
    'This value holds the latest release that is not a path expiry: position and released_at; witnessing is computed at read.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_renewal IS
    'This value holds the latest RegistrationRenewed: position, expiry and revived_from_expiry.';
COMMENT ON COLUMN project_lifecycle_triple_summary.last_expiry_changed IS
    'This value holds the position of the latest ExpiryChanged; it feeds the five-kind selection only.';

CREATE TABLE IF NOT EXISTS project_lifecycle_association (
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
);
COMMENT ON TABLE project_lifecycle_association IS
    'Project-owned lifecycle association of family F2a: per triple, the resource of the latest resource-bearing RegistrationGranted or RegistrationReserved in the canonical event order.';
COMMENT ON COLUMN project_lifecycle_association.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_association.logical_name_id IS
    'This value is the name of the triple.';
COMMENT ON COLUMN project_lifecycle_association.registry_identifier IS
    'This value is COALESCE(registry_contract_instance_id, emitting address, registry) of the triple''s events.';
COMMENT ON COLUMN project_lifecycle_association.token_id IS
    'This value is the triple''s token id; empty text when the events carry none.';
COMMENT ON COLUMN project_lifecycle_association.target_resource_id IS
    'This value is the resource the triple''s null-resource events currently belong to.';
COMMENT ON COLUMN project_lifecycle_association.event_kind IS
    'This value is the kind of the winning grant or reservation.';
COMMENT ON COLUMN project_lifecycle_association.block_number IS
    'This value is the block number of the winning grant or reservation.';
COMMENT ON COLUMN project_lifecycle_association.transaction_index IS
    'This value is the transaction index of the winning grant or reservation; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_association.log_index IS
    'This value is the log index of the winning grant or reservation; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_association.event_identity IS
    'This value is the event identity of the winning grant or reservation, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_association.normalized_event_id IS
    'This value names the winning grant or reservation in normalized_events as attribution only; it never takes part in ordering.';

CREATE TABLE IF NOT EXISTS project_lifecycle_event (
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
    authority_key text,
    PRIMARY KEY (chain_id, state_kind, state_key, event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (state_kind IN ('resource', 'triple'))
);
COMMENT ON TABLE project_lifecycle_event IS
    'Project-owned retained lifecycle events of family F2a: every RegistrationGranted, RegistrationRenewed, RegistrationReleased, RegistrationReserved, ExpiryChanged and TokenControlTransferred of a resource or triple, keyed by position, with the reader fields and admission evidence the authority-admitted readers consume. Unpruned; a row leaves only when undo removes its block.';
COMMENT ON COLUMN project_lifecycle_event.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_lifecycle_event.state_kind IS
    'This value is resource for an event of a resource key and triple for a null-resource event of a triple.';
COMMENT ON COLUMN project_lifecycle_event.state_key IS
    'This value is the resource id, or the triple as a JSON array of name, registry identifier and token id.';
COMMENT ON COLUMN project_lifecycle_event.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_lifecycle_event.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_lifecycle_event.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_lifecycle_event.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_lifecycle_event.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_lifecycle_event.event_kind IS
    'This value is the event kind.';
COMMENT ON COLUMN project_lifecycle_event.original_logical_name_id IS
    'This value is the logical name the adapter emitted, null when it emitted the event unnamed; it is never rewritten.';
COMMENT ON COLUMN project_lifecycle_event.decoded_logical_name_id IS
    'This value is the name the two staging passes attached at write time; informational, the read recomputes it.';
COMMENT ON COLUMN project_lifecycle_event.resource_id IS
    'This value is the event''s resource.';
COMMENT ON COLUMN project_lifecycle_event.source_family IS
    'This value is the event''s source family.';
COMMENT ON COLUMN project_lifecycle_event.authority_kind IS
    'This value is the after-state authority_kind as ->> reads it, null when absent; the admission reads default it to registrar (COALESCE(NULLIF(authority_kind, ''''), ''registrar'')) and the served name block reports it as it is (name_current/build.sql:30).';
COMMENT ON COLUMN project_lifecycle_event.transaction_hash IS
    'This value is the event''s transaction hash.';
COMMENT ON COLUMN project_lifecycle_event.to_address IS
    'This value is the lower-cased recipient of a TokenControlTransferred.';
COMMENT ON COLUMN project_lifecycle_event.namehash IS
    'This value is the lower-cased namehash the event carries.';
COMMENT ON COLUMN project_lifecycle_event.registrant IS
    'This value is the lower-cased after-state registrant.';
COMMENT ON COLUMN project_lifecycle_event.before_registrant IS
    'This value is the lower-cased before-state registrant of a RegistrationReleased.';
COMMENT ON COLUMN project_lifecycle_event.expiry IS
    'This value is the after-state expiry as the event carries it.';
COMMENT ON COLUMN project_lifecycle_event.expiry_seconds IS
    'This value is that expiry as seconds when it is an integral JSON number within the served range, else null.';
COMMENT ON COLUMN project_lifecycle_event.status IS
    'This value is the after-state status.';
COMMENT ON COLUMN project_lifecycle_event.released_at IS
    'This value is the after-state released_at.';
COMMENT ON COLUMN project_lifecycle_event.source_event IS
    'This value is the after-state source_event.';
COMMENT ON COLUMN project_lifecycle_event.derived_from IS
    'This value is the after-state derived_from.';
COMMENT ON COLUMN project_lifecycle_event.terminal_reason IS
    'This value is the after-state terminal_reason.';
COMMENT ON COLUMN project_lifecycle_event.revived_from_expiry IS
    'This value is the after-state revived_from_expiry.';
COMMENT ON COLUMN project_lifecycle_event.state_derived IS
    'This value is the after-state state_derived.';
COMMENT ON COLUMN project_lifecycle_event.surface_materialization IS
    'This value is the after-state surface_materialization.';
COMMENT ON COLUMN project_lifecycle_event.registrar_surface_snapshot IS
    'This value is the after-state registrar_surface_snapshot.';
COMMENT ON COLUMN project_lifecycle_event.original_registered_at IS
    'This value is the after-state original_registered_at in seconds.';
COMMENT ON COLUMN project_lifecycle_event.owner_getter IS
    'This value is the lower-cased after-state owner_getter.';
COMMENT ON COLUMN project_lifecycle_event.owner_word_unmasked IS
    'This value is the after-state owner_word_unmasked.';
COMMENT ON COLUMN project_lifecycle_event.registry_owner IS
    'This value is the lower-cased after-state registry_owner.';
COMMENT ON COLUMN project_lifecycle_event.authority_key IS
    'This value is the event''s after_state authority_key as ->> reads it, which the served authority context reports with the authority kind (name_current/build.sql:393-395).';
CREATE INDEX IF NOT EXISTS project_lifecycle_event_unnamed_lease_idx
    ON project_lifecycle_event (chain_id, state_key)
    WHERE state_kind = 'resource' AND source_family = 'ens_v1_registrar_l1'
      AND original_logical_name_id IS NULL AND decoded_logical_name_id IS NULL;
CREATE INDEX IF NOT EXISTS project_lifecycle_event_decoded_name_idx
    ON project_lifecycle_event (chain_id, decoded_logical_name_id);

-- The name summary writer's work list (crates/project families/derived/summary.rs).
CREATE INDEX IF NOT EXISTS project_lifecycle_association_target_idx
    ON project_lifecycle_association (chain_id, target_resource_id)
    WHERE target_resource_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS project_child_registration_state (
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
);
COMMENT ON TABLE project_child_registration_state IS
    'Project-owned per-registry child registration row of family F2a: the latest RegistrationGranted, RegistrationRenewed or RegistrationReleased of a name for one registry contract instance, and whether any reservation, grant or renewal exists there.';
COMMENT ON COLUMN project_child_registration_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_child_registration_state.logical_name_id IS
    'This value is the child name.';
COMMENT ON COLUMN project_child_registration_state.registry_contract_instance_id IS
    'This value is the registry contract instance the events carry.';
COMMENT ON COLUMN project_child_registration_state.event_kind IS
    'This value is the kind of the latest granted, renewed or released event; null when only a reservation exists.';
COMMENT ON COLUMN project_child_registration_state.registrant IS
    'This value is that event''s lower-cased registrant.';
COMMENT ON COLUMN project_child_registration_state.block_number IS
    'This value is the block number of the latest event of the three kinds, or the first reservation when none exists.';
COMMENT ON COLUMN project_child_registration_state.transaction_index IS
    'This value is the transaction index of the latest event of the three kinds, or the first reservation when none exists; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_child_registration_state.log_index IS
    'This value is the log index of the latest event of the three kinds, or the first reservation when none exists; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_child_registration_state.event_identity IS
    'This value is the event identity of the latest event of the three kinds, or the first reservation when none exists, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_child_registration_state.normalized_event_id IS
    'This value names the latest event of the three kinds, or the first reservation when none exists in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_child_registration_state.exists IS
    'This value is true once any reservation, grant or renewal carries this registry.';

CREATE TABLE IF NOT EXISTS project_wrapper_state (
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
    lifecycle_source text,
    lifecycle_unwrapped boolean,
    lifecycle_position jsonb,
    unwrapped_position jsonb,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_wrapper_state IS
    'Project-owned wrapper state of family F2b per wrapper resource: the latest wrapper_state and fuses, the latest wrapper expiry, and the newest wrapper lifecycle event with the latest unwrap, unmasked; masks are applied at read against the block clock.';
COMMENT ON COLUMN project_wrapper_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_wrapper_state.resource_id IS
    'This value is the wrapper resource.';
COMMENT ON COLUMN project_wrapper_state.logical_name_id IS
    'This value is the wrapped name.';
COMMENT ON COLUMN project_wrapper_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_wrapper_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_wrapper_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_wrapper_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_wrapper_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_wrapper_state.wrapper_state IS
    'This value is wrapped, emancipated or locked from the latest PermissionScopeChanged; null for any other value.';
COMMENT ON COLUMN project_wrapper_state.fuses IS
    'This value is the fuses of the latest PermissionScopeChanged when a JSON number whose value is an integer from 0 to 9223372036854775807, the range builders/permissions.rs modifiers and address_names.rs scope_modifiers read before casting to bigint; null otherwise. The served children and name blocks read a narrower range, 0 to 4294967295 (children.rs:146-148, name_current/build.sql:541-544), so a publisher for those two readers must reapply it; the NameWrapper emits fuses as uint32 (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27-L37 @ ens_v1@91c966f), so the ranges differ only for a value the contract never emits. A non-integral spelling such as 1.0 fails the served bigint cast and the Project batch, so it never reaches a served row.';
COMMENT ON COLUMN project_wrapper_state.wrapper_state_position IS
    'This value is that PermissionScopeChanged''s position.';
COMMENT ON COLUMN project_wrapper_state.expiry_seconds IS
    'This value is the latest wrapper expiry when a JSON integer from 0 to 18446744073709551615, the range the served numeric read keeps (address_names.rs wrapper_expiries, children.rs latest_wrapper_expiries); null otherwise. A decimal spelling such as 1.0 or 1.5, which the served read keeps as that numeric, is null here: the Project reads event payloads without arbitrary precision, so a decimal can arrive rounded (9007199254740991.0 as 9007199254740990). The adapter writes the expiry as a JSON integer (adapters schema_v2/protocol/v1/wrapper.rs decodes a uint64), so only a hand-written payload reaches the difference.';
COMMENT ON COLUMN project_wrapper_state.expiry_position IS
    'This value is that ExpiryChanged''s position.';
COMMENT ON COLUMN project_wrapper_state.owner_word_unmasked IS
    'This value is the latest owner_word_unmasked flag the wrapper events carried.';
COMMENT ON COLUMN project_wrapper_state.lifecycle_source IS
    'This value is the source of the newest wrapper lifecycle event of the resource: NameWrapped, NameUnwrapped, holder_grant or holder_revoke (resource_summary.rs wrapper_lifecycles). NameWrapped is the mint: the pinned NameWrapper emits it only from _wrap, right after minting the token of the node (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f).';
COMMENT ON COLUMN project_wrapper_state.lifecycle_unwrapped IS
    'This value is true when the newest wrapper lifecycle event leaves the resource unwrapped: a NameUnwrapped or a holder revoke with no powers. The pinned NameWrapper emits NameUnwrapped when _unwrap burns the token (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f) and when a mint burns a still-held token first (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f); its upgrade, which no manifest admits, burns without NameUnwrapped, so there only the holder revoke leaves the resource unwrapped (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f). The served wrapper restrictions are served only while it is false.';
COMMENT ON COLUMN project_wrapper_state.lifecycle_position IS
    'This value is the canonical position of the newest wrapper lifecycle event.';
COMMENT ON COLUMN project_wrapper_state.unwrapped_position IS
    'This value is the canonical position of the latest NameUnwrapped of the resource, kept when a later mint or holder grant becomes the newest lifecycle event; a re-wrap over a still-held token emits NameUnwrapped to the zero address before its NameWrapped (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f).';

CREATE TABLE IF NOT EXISTS project_registry_node_state (
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
    owner_event_kind text,
    owner_position jsonb,
    owner_resource_id uuid,
    PRIMARY KEY (chain_id, namespace, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_registry_node_state IS
    'Project-owned registry ownership of family F2c per ENSv1 or Basenames registry node: the latest owner with the zero-owner override facts, and the registry generation facts.';
COMMENT ON COLUMN project_registry_node_state.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_node_state.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_registry_node_state.node IS
    'This value is the lower-cased node the event addresses: child_node, else node.';
COMMENT ON COLUMN project_registry_node_state.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_registry_node_state.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_node_state.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_node_state.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_node_state.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_node_state.owner IS
    'This value is the lower-cased owner of the latest AuthorityTransferred or SubregistryChanged for the node.';
COMMENT ON COLUMN project_registry_node_state.owner_getter IS
    'This value is the lower-cased owner_getter of that event.';
COMMENT ON COLUMN project_registry_node_state.owner_getter_reason IS
    'This value is the owner_getter_reason of that event.';
COMMENT ON COLUMN project_registry_node_state.owner_word_unmasked IS
    'This value is the owner_word_unmasked of that event.';
COMMENT ON COLUMN project_registry_node_state.registry_owner IS
    'This value is the lower-cased registry_owner of that event.';
COMMENT ON COLUMN project_registry_node_state.emitter_role IS
    'This value is the after-state emitter_role of the latest event.';
COMMENT ON COLUMN project_registry_node_state.registry_contract IS
    'This value is the lower-cased emitting address, else the after-state registry_contract.';
COMMENT ON COLUMN project_registry_node_state.has_old_record IS
    'This value is true once any event with emitter_role registry_old addressed the node.';
COMMENT ON COLUMN project_registry_node_state.first_current_record_block IS
    'This value is the first block with an emitter_role registry event for the node.';
COMMENT ON COLUMN project_registry_node_state.owner_event_kind IS
    'This value is the kind of the registry event that last set the owner group: AuthorityTransferred or SubregistryChanged, both of which report the owner (name_authority/stage.rs:200-261). Either overwrites the group, so a SubregistryChanged after an AuthorityTransferred whose getter was zero replaces the owner; the served ownerless verdict, which reads AuthorityTransferred only, cannot be recovered from this row, and project_registry_owner_event keeps every owner-setting event for it.';
COMMENT ON COLUMN project_registry_node_state.owner_position IS
    'This value is the position of that event, apart from the row''s last-write position.';
COMMENT ON COLUMN project_registry_node_state.owner_resource_id IS
    'This value is that event''s resource.';

CREATE TABLE IF NOT EXISTS project_registry_owner_event (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    transaction_hash text,
    logical_name_id text,
    resource_id uuid,
    event_kind text NOT NULL,
    source_family text NOT NULL,
    authority_kind text,
    owner text,
    owner_getter text,
    owner_getter_reason text,
    registry_owner text,
    owner_word_unmasked boolean,
    PRIMARY KEY (chain_id, namespace, node, event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_registry_owner_event IS
    'Project-owned owner-setting registry events of family F2c: every AuthorityTransferred and SubregistryChanged an ENSv1 or Basenames registry reported for a node, and every AuthorityTransferred an ENSv2 registry reported for a named node, keyed by position, with the name, resource, authority kind and owner facts each carried. The node row keeps only the latest owner group, which a SubregistryChanged after a zero-getter transfer replaces; the served ownerless verdict and owner history are recovered from these rows. Unpruned; a row leaves only when undo removes its block.';
COMMENT ON COLUMN project_registry_owner_event.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_owner_event.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_registry_owner_event.node IS
    'This value is the lower-cased node the event addresses: child_node, else node; for an ENSv2 registry event, the namehash of its name.';
COMMENT ON COLUMN project_registry_owner_event.block_number IS
    'This value is the event''s block number.';
COMMENT ON COLUMN project_registry_owner_event.transaction_index IS
    'This value is the event''s transaction index; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_owner_event.log_index IS
    'This value is the event''s log index; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_owner_event.event_identity IS
    'This value is the event identity, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_owner_event.normalized_event_id IS
    'This value names the event in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_owner_event.transaction_hash IS
    'This value is the event''s transaction hash, null for a synthesised event.';
COMMENT ON COLUMN project_registry_owner_event.logical_name_id IS
    'This value is the event''s name, null when it carried none.';
COMMENT ON COLUMN project_registry_owner_event.resource_id IS
    'This value is the event''s resource.';
COMMENT ON COLUMN project_registry_owner_event.event_kind IS
    'This value is AuthorityTransferred or SubregistryChanged.';
COMMENT ON COLUMN project_registry_owner_event.source_family IS
    'This value is the registry source family.';
COMMENT ON COLUMN project_registry_owner_event.authority_kind IS
    'This value is the after-state authority_kind of the event.';
COMMENT ON COLUMN project_registry_owner_event.owner IS
    'This value is the lower-cased owner the event reported.';
COMMENT ON COLUMN project_registry_owner_event.owner_getter IS
    'This value is the lower-cased owner_getter of the event.';
COMMENT ON COLUMN project_registry_owner_event.owner_getter_reason IS
    'This value is the owner_getter_reason of the event.';
COMMENT ON COLUMN project_registry_owner_event.registry_owner IS
    'This value is the lower-cased registry_owner of the event, as the node row keeps it for its latest event.';
COMMENT ON COLUMN project_registry_owner_event.owner_word_unmasked IS
    'This value is the owner_word_unmasked flag of the event, as the node row keeps it for its latest event.';

-- The name summary writer's work list (crates/project families/derived/summary.rs) and its
-- zero-owner attribution (crates/storage families/name/summary.rs).
CREATE INDEX IF NOT EXISTS project_registry_owner_event_name_idx
    ON project_registry_owner_event (chain_id, logical_name_id)
    WHERE logical_name_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_registry_owner_event_resource_idx
    ON project_registry_owner_event (chain_id, resource_id)
    WHERE resource_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS project_registry_binding_observation (
    chain_id text NOT NULL,
    observation_identity text NOT NULL,
    logical_name_id text,
    resource_id uuid NOT NULL,
    attributed_via text NOT NULL,
    target_resource_id uuid NOT NULL,
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
    PRIMARY KEY (chain_id, observation_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (attributed_via IN ('own', 'name'))
);
COMMENT ON TABLE project_registry_binding_observation IS
    'Project-owned registry binding observations of family F2c: per observation identity (the name, else the resource; permission_resources.rs:10-11), the latest AuthorityTransferred, SubregistryChanged, SurfaceBound or SurfaceUnbound observation with the resource it reaches. The resource summary takes, per target resource, the latest row that reaches it.';
COMMENT ON COLUMN project_registry_binding_observation.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_binding_observation.observation_identity IS
    'This value is COALESCE(logical_name_id, resource_id) of the observation, its DISTINCT ON key.';
COMMENT ON COLUMN project_registry_binding_observation.logical_name_id IS
    'This value is the event''s name, null for an unnamed observation.';
COMMENT ON COLUMN project_registry_binding_observation.resource_id IS
    'This value is the event''s own resource.';
COMMENT ON COLUMN project_registry_binding_observation.attributed_via IS
    'This value is name for a named AuthorityTransferred or SubregistryChanged, which reaches the name''s current resource, and own for every other observation, which reaches its own resource.';
COMMENT ON COLUMN project_registry_binding_observation.target_resource_id IS
    'This value is the resource the observation reaches after the block: for attributed_via name the name''s ENSv1 or Basenames binding active at the block, else resource_id; a block that moves the name''s current binding moves it. A reader whose authority selection differs re-resolves it from logical_name_id.';
COMMENT ON COLUMN project_registry_binding_observation.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_registry_binding_observation.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_binding_observation.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_binding_observation.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_binding_observation.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_binding_observation.event_kind IS
    'This value is the observation''s event kind.';
COMMENT ON COLUMN project_registry_binding_observation.registry_owner IS
    'This value is the lower-cased owner_getter; null after SurfaceUnbound.';
COMMENT ON COLUMN project_registry_binding_observation.registry_contract IS
    'This value is the lower-cased registry contract the observation names.';
COMMENT ON COLUMN project_registry_binding_observation.provenance IS
    'This value is the observation''s raw fact reference and name.';
COMMENT ON COLUMN project_registry_binding_observation.applicable IS
    'This value is true when owner and contract are well-formed addresses and the owner is not zero.';
COMMENT ON COLUMN project_registry_binding_observation.clear_event_identity IS
    'This value is the event identity when the observation is not applicable.';
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_target_idx
    ON project_registry_binding_observation (chain_id, target_resource_id);
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_resource_idx
    ON project_registry_binding_observation (chain_id, resource_id);
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_owner_idx
    ON project_registry_binding_observation (chain_id, registry_contract, registry_owner);

CREATE TABLE IF NOT EXISTS project_resolver_classification (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    classification jsonb,
    support_status text NOT NULL,
    unsupported_reason text,
    manifest_id bigint,
    manifest_event_id bigint,
    admission_namespace text,
    summary_version text,
    observed_families jsonb NOT NULL DEFAULT '{}'::jsonb,
    pointer_families jsonb NOT NULL DEFAULT '{}'::jsonb,
    upgrades jsonb NOT NULL DEFAULT '{}'::jsonb,
    admission_manifests text,
    PRIMARY KEY (chain_id, resolver_address),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (support_status IN ('supported', 'unsupported'))
);
COMMENT ON TABLE project_resolver_classification IS
    'Project-owned resolver classification of family F3, pinned to the block that last classified it: resolver_current without its sampled sections, from the candidate accumulators the row keeps and the discovery edges, declarations and manifests active at that block. A resolver is classified again when an event names it, a pointer moves to or from it, a resolver edge, its address or a declaration of it starts or stops, and when the active manifest set changes. Edge and address activity also honours deactivated_at, a wall-clock time as in the served build, so a classification can differ from a later rebuild once an edge is deactivated.';
COMMENT ON COLUMN project_resolver_classification.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resolver_classification.resolver_address IS
    'This value is the lower-cased resolver address.';
COMMENT ON COLUMN project_resolver_classification.block_number IS
    'This value is the block number of the latest event that named the resolver, or of the activation block for a row written by a resolver edge, address or declaration activation.';
COMMENT ON COLUMN project_resolver_classification.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resolver_classification.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resolver_classification.event_identity IS
    'This value is the event identity of that event, or activation:<block> for an activation; the final tiebreak of the canonical event order, compared as bytes. An epoch change reclassifies the row without moving it.';
COMMENT ON COLUMN project_resolver_classification.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resolver_classification.classification IS
    'This value is the source family, role, basis, implementation, read features, mirror and latest upgrade of the classifying candidate.';
COMMENT ON COLUMN project_resolver_classification.support_status IS
    'This value is supported or unsupported.';
COMMENT ON COLUMN project_resolver_classification.unsupported_reason IS
    'This value is the reason when unsupported: resolver_not_declared, resolver_implementation_unknown, resolver_implementation_not_declared, or resolver_manifest_not_active for a resolver with candidates but no active manifest of its family, which the served build leaves out (one such row per resolver).';
COMMENT ON COLUMN project_resolver_classification.manifest_id IS
    'This value is the declaring manifest.';
COMMENT ON COLUMN project_resolver_classification.manifest_event_id IS
    'This value is the SourceManifestUpdated event of that manifest.';
COMMENT ON COLUMN project_resolver_classification.admission_namespace IS
    'This value is the namespace of the declaring manifest.';
COMMENT ON COLUMN project_resolver_classification.summary_version IS
    'This value is the classification summary version.';
COMMENT ON COLUMN project_resolver_classification.observed_families IS
    'This value maps each resolver family an event proposed the resolver under to its best priority: 3 for an ENSv2 Upgraded proxy, an AliasChanged and either side of a ResolverChanged, 4 for either side of a PermissionChanged scope (resolver/build.sql:5-86).';
COMMENT ON COLUMN project_resolver_classification.pointer_families IS
    'This value maps each resolver family to the number of F4 and F5 pointer rows pointing at the resolver now, standing for the priority 2 name pointers. It approximates the served candidates: an unnamed ENSv2 pointer row counts here though the served build has no candidate for it, so a resolver with an ENSv1 event proposal and such a pointer can classify under ens_v2_resolver_l1 here and ens_v1_resolver_l1 served.';
COMMENT ON COLUMN project_resolver_classification.upgrades IS
    'This value maps each family to the latest Upgraded of the proxy: its position, implementation and normalized event id.';
COMMENT ON COLUMN project_resolver_classification.admission_manifests IS
    'This value is the key of the active manifest set the classification was made under (project_family_marker.admission_manifests).';

CREATE TABLE IF NOT EXISTS project_registry_pointer (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text NOT NULL,
    resource_id uuid,
    source_family text NOT NULL,
    PRIMARY KEY (chain_id, namespace, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
CREATE INDEX IF NOT EXISTS project_registry_pointer_resolver_idx ON project_registry_pointer (chain_id, resolver_address);
COMMENT ON TABLE project_registry_pointer IS
    'Project-owned ENSv1 registry-node resolver pointer of family F4: the latest ResolverChanged per node, clears included, from the ENSv1 registry, registrar and wrapper families only (record_inventory/mirror.rs:100). A ResolverChanged of another family with no resource, such as a Basenames reverse node pointer, lands in neither F4 nor F5, where the served reverse-claim resolver (builders/primary_names.rs:103-113) reads the latest ResolverChanged at the node from any family.';
COMMENT ON COLUMN project_registry_pointer.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_registry_pointer.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_registry_pointer.node IS
    'This value is lower(COALESCE(child_node, namehash, node)) of the event.';
COMMENT ON COLUMN project_registry_pointer.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_registry_pointer.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_registry_pointer.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_registry_pointer.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_registry_pointer.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_registry_pointer.resolver_address IS
    'This value is the lower-cased resolver, the zero address for a clear.';
COMMENT ON COLUMN project_registry_pointer.resource_id IS
    'This value is the event''s resource when it names one.';
COMMENT ON COLUMN project_registry_pointer.source_family IS
    'This value is the event''s source family.';

CREATE TABLE IF NOT EXISTS project_resource_pointer (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text,
    pointer_position jsonb,
    namespace text,
    source_family text,
    namehash text,
    nonzero_resolver_address text,
    nonzero_position jsonb,
    boundary_kind text,
    boundary_position jsonb,
    boundary_block_timestamp timestamptz,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
CREATE INDEX IF NOT EXISTS project_resource_pointer_resolver_idx ON project_resource_pointer (chain_id, resolver_address, resource_id);
CREATE INDEX IF NOT EXISTS project_resource_pointer_root_node_idx
    ON project_resource_pointer (chain_id, namespace, namehash)
    WHERE source_family = 'ens_v2_root_l1';
COMMENT ON TABLE project_resource_pointer IS
    'Project-owned resource resolver pointer of family F5: the current pointer with clears, the latest non-zero pointer and the record version boundary of a resource.';
COMMENT ON COLUMN project_resource_pointer.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resource_pointer.resource_id IS
    'This value is the resource.';
COMMENT ON COLUMN project_resource_pointer.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resource_pointer.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resource_pointer.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resource_pointer.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resource_pointer.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver of the latest ResolverChanged on the resource, named or not, clears included. At an ENSv2 root-registry TLD expiry the interpreter emits the resolver clear with no logical name (adapters schema_v2/protocol/v2_registry/expiry.rs); this row keeps that clear, where the served pointer read takes named ResolverChanged only (builders/linked_records.rs, project_record_pointer_latest) and never sees it, so the served inventory keeps a row the name no longer reaches. The pinned registry returns the zero address from getResolver once the token has expired (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258, L628-L630 @ ens_v2@a971bd64), which this row matches.';
COMMENT ON COLUMN project_resource_pointer.pointer_position IS
    'This value is that ResolverChanged''s position.';
COMMENT ON COLUMN project_resource_pointer.namespace IS
    'This value is that event''s namespace.';
COMMENT ON COLUMN project_resource_pointer.source_family IS
    'This value is that event''s source family.';
COMMENT ON COLUMN project_resource_pointer.namehash IS
    'This value is the namehash of the pointer''s name when it is named, else the node the event addresses (child_node, namehash or node).';
COMMENT ON COLUMN project_resource_pointer.nonzero_resolver_address IS
    'This value is the latest pointer whose resolver is a non-empty, non-zero address.';
COMMENT ON COLUMN project_resource_pointer.nonzero_position IS
    'This value is that event''s position.';
COMMENT ON COLUMN project_resource_pointer.boundary_kind IS
    'This value is the kind of the latest RecordVersionChanged or ResolverChanged on the resource.';
COMMENT ON COLUMN project_resource_pointer.boundary_position IS
    'This value is that boundary event''s position.';
COMMENT ON COLUMN project_resource_pointer.boundary_block_timestamp IS
    'This value is that boundary event''s block timestamp.';

CREATE TABLE IF NOT EXISTS project_named_resource_pointer (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text,
    source_family text NOT NULL,
    PRIMARY KEY (chain_id, resource_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
CREATE INDEX IF NOT EXISTS project_named_resource_pointer_resolver_idx
    ON project_named_resource_pointer (chain_id, resolver_address, logical_name_id, resource_id);
COMMENT ON TABLE project_named_resource_pointer IS
    'Project-owned named resource resolver pointer of family F5: the latest named ResolverChanged per resource and logical name in canonical event order, including clears. An unnamed pointer or an event naming another name leaves this row unchanged. The composed name reader loads exact resource/name pairs; bound-name discovery walks retained pointer keys by resolver instead of normalized event history. Released names keep their pointer facts and are filtered by the composed binding admission.';
COMMENT ON COLUMN project_named_resource_pointer.chain_id IS
    'This value is the chain whose events wrote the row.';
COMMENT ON COLUMN project_named_resource_pointer.resource_id IS
    'This value is the resource named by the event.';
COMMENT ON COLUMN project_named_resource_pointer.logical_name_id IS
    'This value is the logical name named by the event.';
COMMENT ON COLUMN project_named_resource_pointer.block_number IS
    'This value is the block number of the latest named ResolverChanged for the key.';
COMMENT ON COLUMN project_named_resource_pointer.transaction_index IS
    'This value is the transaction index of that event; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_named_resource_pointer.log_index IS
    'This value is the log index of that event; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_named_resource_pointer.event_identity IS
    'This value is that event identity, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_named_resource_pointer.normalized_event_id IS
    'This value names that event in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_named_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver from that event, including null, empty and zero-address clears.';
COMMENT ON COLUMN project_named_resource_pointer.source_family IS
    'This value is that event''s source family.';

CREATE TABLE IF NOT EXISTS project_node_record_partition (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    arm text NOT NULL,
    arm_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    node text,
    logical_name_id text,
    source_family text NOT NULL,
    namespace text NOT NULL,
    source_manifest_id bigint,
    version_position jsonb,
    PRIMARY KEY (chain_id, resolver_address, arm, arm_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (arm IN ('named', 'native', 'guarded'))
);
COMMENT ON TABLE project_node_record_partition IS
    'Project-owned node record partitions of family F6: per resolver, attribution arm and arm identity, the latest record version event.';
COMMENT ON COLUMN project_node_record_partition.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_node_record_partition.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_node_record_partition.arm IS
    'This value is named, native or guarded.';
COMMENT ON COLUMN project_node_record_partition.arm_identity IS
    'This value is the logical name for named; node and source family for native; node, source family, namespace and manifest for guarded, joined by a vertical bar.';
COMMENT ON COLUMN project_node_record_partition.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_node_record_partition.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_node_record_partition.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_node_record_partition.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_node_record_partition.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_node_record_partition.node IS
    'This value is the lower-cased node.';
COMMENT ON COLUMN project_node_record_partition.logical_name_id IS
    'This value is the name the events carry.';
COMMENT ON COLUMN project_node_record_partition.source_family IS
    'This value is the events'' source family.';
COMMENT ON COLUMN project_node_record_partition.namespace IS
    'This value is the events'' namespace.';
COMMENT ON COLUMN project_node_record_partition.source_manifest_id IS
    'This value is the events'' source manifest.';
COMMENT ON COLUMN project_node_record_partition.version_position IS
    'This value is the position of the partition''s latest RecordVersionChanged.';

CREATE TABLE IF NOT EXISTS project_node_record_value (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    arm text NOT NULL,
    arm_identity text NOT NULL,
    record_key text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    value jsonb,
    record_family text,
    selector_key text,
    contenthash_hex text,
    address_bytes_hex text,
    source_event text,
    storage_model text,
    sibling_value jsonb,
    sibling_position jsonb,
    node text,
    logical_name_id text,
    resource_id uuid,
    source_family text NOT NULL,
    namespace text NOT NULL,
    source_manifest_id bigint,
    hydrated_value jsonb,
    hydrated_at_block bigint,
    sibling_status text,
    sibling_address_bytes_hex text,
    raw_name jsonb,
    raw_name_bytes jsonb,
    PRIMARY KEY (chain_id, resolver_address, arm, arm_identity, record_key),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (arm IN ('named', 'native', 'guarded'))
);
COMMENT ON TABLE project_node_record_value IS
    'Project-owned node record values of family F6: per partition and record key, the latest record in the canonical event order, with its coin-60 compatibility sibling.';
COMMENT ON COLUMN project_node_record_value.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_node_record_value.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_node_record_value.arm IS
    'This value is the partition''s arm.';
COMMENT ON COLUMN project_node_record_value.arm_identity IS
    'This value is the partition''s arm identity.';
COMMENT ON COLUMN project_node_record_value.record_key IS
    'This value is the record key.';
COMMENT ON COLUMN project_node_record_value.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_node_record_value.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_node_record_value.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_node_record_value.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_node_record_value.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_node_record_value.status IS
    'This value is success, not_found or unsupported, as the inventory builder classifies the value.';
COMMENT ON COLUMN project_node_record_value.value IS
    'This value is the record value as the event carries it.';
COMMENT ON COLUMN project_node_record_value.record_family IS
    'This value is the after-state record_family.';
COMMENT ON COLUMN project_node_record_value.selector_key IS
    'This value is the after-state selector_key.';
COMMENT ON COLUMN project_node_record_value.contenthash_hex IS
    'This value is the after-state contenthash_hex.';
COMMENT ON COLUMN project_node_record_value.address_bytes_hex IS
    'This value is the after-state address_bytes_hex.';
COMMENT ON COLUMN project_node_record_value.source_event IS
    'This value is the after-state source_event.';
COMMENT ON COLUMN project_node_record_value.storage_model IS
    'This value is the after-state storage_model.';
COMMENT ON COLUMN project_node_record_value.sibling_value IS
    'This value is the AddressChanged half''s value when this record is the AddrChanged half of a coin-60 pair.';
COMMENT ON COLUMN project_node_record_value.sibling_position IS
    'This value is that AddressChanged half''s own position.';
COMMENT ON COLUMN project_node_record_value.node IS
    'This value is the lower-cased node.';
COMMENT ON COLUMN project_node_record_value.logical_name_id IS
    'This value is the name the record carries.';
COMMENT ON COLUMN project_node_record_value.resource_id IS
    'This value is the resource the record carries.';
COMMENT ON COLUMN project_node_record_value.source_family IS
    'This value is the record''s source family.';
COMMENT ON COLUMN project_node_record_value.namespace IS
    'This value is the record''s namespace.';
COMMENT ON COLUMN project_node_record_value.source_manifest_id IS
    'This value is the record''s source manifest.';
COMMENT ON COLUMN project_node_record_value.hydrated_value IS
    'This value is the hydrated text value; null until hydration moves into the block.';
COMMENT ON COLUMN project_node_record_value.hydrated_at_block IS
    'This value is the block the hydrated value was read at.';
COMMENT ON COLUMN project_node_record_value.sibling_status IS
    'This value is the status of the AddressChanged half of a coin-60 pair, the half the served inventory keeps.';
COMMENT ON COLUMN project_node_record_value.sibling_address_bytes_hex IS
    'This value is the address_bytes_hex of that AddressChanged half.';
COMMENT ON COLUMN project_node_record_value.raw_name IS
    'This value is the after-state raw_name of a name record, the claim input a reverse claim reads.';
COMMENT ON COLUMN project_node_record_value.raw_name_bytes IS
    'This value is the after-state raw_name_bytes of a name record.';
CREATE INDEX IF NOT EXISTS project_node_record_value_node_idx
    ON project_node_record_value (chain_id, resolver_address, node);

CREATE TABLE IF NOT EXISTS project_record_id_value (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    record_id text NOT NULL,
    record_key text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    value jsonb,
    record_family text,
    selector_key text,
    contenthash_hex text,
    address_bytes_hex text,
    source_event text,
    storage_model text,
    source_family text NOT NULL,
    namespace text NOT NULL,
    source_manifest_id bigint,
    raw_name jsonb,
    raw_name_bytes jsonb,
    PRIMARY KEY (chain_id, resolver_address, record_id, record_key),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_record_id_value IS
    'Project-owned record-id values of family F7: per resolver, record id and record key, the latest RecordChanged with storage model resolver_record_id.';
COMMENT ON COLUMN project_record_id_value.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_record_id_value.resolver_address IS
    'This value is the lower-cased resolver.';
COMMENT ON COLUMN project_record_id_value.record_id IS
    'This value is the resolver record id.';
COMMENT ON COLUMN project_record_id_value.record_key IS
    'This value is the record key.';
COMMENT ON COLUMN project_record_id_value.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_record_id_value.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_record_id_value.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_record_id_value.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_record_id_value.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_record_id_value.status IS
    'This value is success, not_found or unsupported, as the inventory builder classifies the value.';
COMMENT ON COLUMN project_record_id_value.value IS
    'This value is the record value as the event carries it.';
COMMENT ON COLUMN project_record_id_value.record_family IS
    'This value is the after-state record_family.';
COMMENT ON COLUMN project_record_id_value.selector_key IS
    'This value is the after-state selector_key.';
COMMENT ON COLUMN project_record_id_value.contenthash_hex IS
    'This value is the after-state contenthash_hex.';
COMMENT ON COLUMN project_record_id_value.address_bytes_hex IS
    'This value is the after-state address_bytes_hex.';
COMMENT ON COLUMN project_record_id_value.source_event IS
    'This value is the after-state source_event.';
COMMENT ON COLUMN project_record_id_value.storage_model IS
    'This value is the after-state storage_model.';
COMMENT ON COLUMN project_record_id_value.source_family IS
    'This value is the record''s source family.';
COMMENT ON COLUMN project_record_id_value.namespace IS
    'This value is the record''s namespace.';
COMMENT ON COLUMN project_record_id_value.source_manifest_id IS
    'This value is the record''s source manifest.';
COMMENT ON COLUMN project_record_id_value.raw_name IS
    'This value is the after-state raw_name of a name record.';
COMMENT ON COLUMN project_record_id_value.raw_name_bytes IS
    'This value is the after-state raw_name_bytes of a name record.';

CREATE TABLE IF NOT EXISTS project_resolver_link (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    node text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    record_id text NOT NULL,
    storage_model text,
    PRIMARY KEY (chain_id, resolver_address, node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_resolver_link IS
    'Project-owned resolver links of family F7: per resolver and node, the latest ResolverRecordLinked; record id 0 is an explicit clear. A link whose payload carries no resolver is kept, where the served links.sql requires the payload resolver to be present and equal to the emitter.';
COMMENT ON COLUMN project_resolver_link.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resolver_link.resolver_address IS
    'This value is the lower-cased resolver that emitted the ResolverRecordLinked; a link whose payload names another resolver is not kept (resolvers/collections/links.sql:16-17).';
COMMENT ON COLUMN project_resolver_link.node IS
    'This value is the lower-cased node; 32 zero bytes is the default link.';
COMMENT ON COLUMN project_resolver_link.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resolver_link.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resolver_link.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resolver_link.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resolver_link.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resolver_link.record_id IS
    'This value is the linked record id, 0 for an unlink.';
COMMENT ON COLUMN project_resolver_link.storage_model IS
    'This value is the after-state storage_model.';

CREATE TABLE IF NOT EXISTS project_grant (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    subject text NOT NULL,
    scope text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    event_kind text NOT NULL,
    scope_kind text,
    scope_detail jsonb,
    effective_powers jsonb NOT NULL,
    grant_source jsonb,
    revocation_source jsonb,
    inheritance_path jsonb,
    transfer_behavior jsonb,
    revoked boolean NOT NULL,
    registration_position jsonb,
    PRIMARY KEY (chain_id, resource_id, subject, scope),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
CREATE INDEX IF NOT EXISTS project_grant_subject_idx ON project_grant (subject);
CREATE INDEX IF NOT EXISTS project_grant_scope_idx ON project_grant (chain_id, scope);
COMMENT ON TABLE project_grant IS
    'Project-owned raw grants of family F8: per resource, subject and scope, the latest PermissionChanged or RootPermissionChanged, unmasked; wrapper masks, grace and expiry retirement apply at read.';
COMMENT ON COLUMN project_grant.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_grant.resource_id IS
    'This value is the resource.';
COMMENT ON COLUMN project_grant.subject IS
    'This value is the lower-cased subject.';
COMMENT ON COLUMN project_grant.scope IS
    'This value is the scope key as permissions.rs builds it.';
COMMENT ON COLUMN project_grant.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_grant.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_grant.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_grant.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_grant.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_grant.event_kind IS
    'This value is PermissionChanged or RootPermissionChanged.';
COMMENT ON COLUMN project_grant.scope_kind IS
    'This value is the scope kind with registry_root folded into root.';
COMMENT ON COLUMN project_grant.scope_detail IS
    'This value is the after-state scope object.';
COMMENT ON COLUMN project_grant.effective_powers IS
    'This value is the after-state effective_powers array, unmasked.';
COMMENT ON COLUMN project_grant.grant_source IS
    'This value is the after-state grant_source.';
COMMENT ON COLUMN project_grant.revocation_source IS
    'This value is the after-state revocation_source.';
COMMENT ON COLUMN project_grant.inheritance_path IS
    'This value is the after-state inheritance_path.';
COMMENT ON COLUMN project_grant.transfer_behavior IS
    'This value is the after-state transfer_behavior.';
COMMENT ON COLUMN project_grant.revoked IS
    'This value is true when the effective powers are empty; the row stays as a clear.';
COMMENT ON COLUMN project_grant.registration_position IS
    'This value is the position of the resource''s latest RegistrationGranted or RegistrationReserved before the grant, counting earlier events of the grant''s own block: the registration the grant was written under, by the rule F2a keeps as last_active. It is new state for the per-block publisher, not a copy of a served value: the served permissions read has no per-grant registration and masks by the resource''s current registration (builders/permissions.rs v2_registration_current). Null when the resource has no earlier grant or reservation.';

CREATE TABLE IF NOT EXISTS project_resource_admin_aggregate (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    admin_powers jsonb NOT NULL,
    PRIMARY KEY (chain_id, resource_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_resource_admin_aggregate IS
    'Project-owned admin aggregate of family F8: per resource, the admin powers any subject holds through a registry or root scope grant.';
COMMENT ON COLUMN project_resource_admin_aggregate.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resource_admin_aggregate.resource_id IS
    'This value is the resource.';
COMMENT ON COLUMN project_resource_admin_aggregate.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resource_admin_aggregate.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resource_admin_aggregate.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resource_admin_aggregate.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resource_admin_aggregate.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resource_admin_aggregate.admin_powers IS
    'This value is the sorted distinct admin powers.';

CREATE TABLE IF NOT EXISTS project_account_approval (
    chain_id text NOT NULL,
    authority_kind text NOT NULL,
    authority_contract text NOT NULL,
    owner text NOT NULL,
    subject text NOT NULL,
    relation_kind text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    authority_contract_instance_id text,
    approved boolean NOT NULL,
    effective_powers jsonb,
    grant_source jsonb,
    revocation_source jsonb,
    inheritance_path jsonb,
    transfer_behavior jsonb,
    PRIMARY KEY (chain_id, authority_kind, authority_contract, owner, subject, relation_kind),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
CREATE INDEX IF NOT EXISTS project_account_approval_subject_idx
    ON project_account_approval (subject, authority_kind);
COMMENT ON TABLE project_account_approval IS
    'Project-owned account approvals of family F9: the latest AccountPermissionChanged per authority contract, owner, subject and relation; an explicit false stays as a row.';
COMMENT ON COLUMN project_account_approval.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_account_approval.authority_kind IS
    'This value is registry or wrapper.';
COMMENT ON COLUMN project_account_approval.authority_contract IS
    'This value is the lower-cased authority contract.';
COMMENT ON COLUMN project_account_approval.owner IS
    'This value is the lower-cased owner.';
COMMENT ON COLUMN project_account_approval.subject IS
    'This value is the lower-cased approved operator.';
COMMENT ON COLUMN project_account_approval.relation_kind IS
    'This value is the relation kind.';
COMMENT ON COLUMN project_account_approval.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_account_approval.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_account_approval.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_account_approval.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_account_approval.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_account_approval.authority_contract_instance_id IS
    'This value is the authority contract instance.';
COMMENT ON COLUMN project_account_approval.approved IS
    'This value is the after-state approved flag.';
COMMENT ON COLUMN project_account_approval.effective_powers IS
    'This value is the after-state effective_powers.';
COMMENT ON COLUMN project_account_approval.grant_source IS
    'This value is the after-state grant_source.';
COMMENT ON COLUMN project_account_approval.revocation_source IS
    'This value is the after-state revocation_source.';
COMMENT ON COLUMN project_account_approval.inheritance_path IS
    'This value is the after-state inheritance_path.';
COMMENT ON COLUMN project_account_approval.transfer_behavior IS
    'This value is the after-state transfer_behavior.';

CREATE TABLE IF NOT EXISTS project_name_alias (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    active boolean NOT NULL,
    alias_state text,
    to_logical_name_id text,
    to_name text,
    to_resource_id text,
    to_normalized_name text,
    to_canonical_display_name text,
    to_namehash text,
    resolver_address text,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_name_alias IS
    'Project-owned name aliases of family F10: per source name, the latest AliasChanged with its event-carried target.';
COMMENT ON COLUMN project_name_alias.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_name_alias.logical_name_id IS
    'This value is the source name.';
COMMENT ON COLUMN project_name_alias.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_name_alias.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_name_alias.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_name_alias.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_name_alias.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_name_alias.active IS
    'This value is the after-state active flag, true when absent.';
COMMENT ON COLUMN project_name_alias.alias_state IS
    'This value is the after-state alias_state.';
COMMENT ON COLUMN project_name_alias.to_logical_name_id IS
    'This value is the after-state to_logical_name_id.';
COMMENT ON COLUMN project_name_alias.to_name IS
    'This value is the after-state to_name.';
COMMENT ON COLUMN project_name_alias.to_resource_id IS
    'This value is the after-state to_resource_id.';
COMMENT ON COLUMN project_name_alias.to_normalized_name IS
    'This value is the after-state to_normalized_name.';
COMMENT ON COLUMN project_name_alias.to_canonical_display_name IS
    'This value is the after-state to_canonical_display_name.';
COMMENT ON COLUMN project_name_alias.to_namehash IS
    'This value is the after-state to_namehash.';
COMMENT ON COLUMN project_name_alias.resolver_address IS
    'This value is the lower-cased resolver the alias was written at.';

CREATE TABLE IF NOT EXISTS project_resolver_alias (
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    alias_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    active boolean NOT NULL,
    alias_state text,
    from_dns_encoded_name text,
    to_dns_encoded_name text,
    from_name text,
    to_logical_name_id text,
    to_name text,
    to_resource_id text,
    logical_name_id text,
    PRIMARY KEY (chain_id, resolver_address, alias_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_resolver_alias IS
    'Project-owned per-resolver alias state of family F10: per resolver and alias identity, the latest AliasChanged.';
COMMENT ON COLUMN project_resolver_alias.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_resolver_alias.resolver_address IS
    'This value is lower(COALESCE(after resolver, before resolver, emitting address)).';
COMMENT ON COLUMN project_resolver_alias.alias_identity IS
    'This value is COALESCE(logical_name_id, from_logical_name_id, from_namehash, from_dns_encoded_name, from_name, event_identity).';
COMMENT ON COLUMN project_resolver_alias.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_resolver_alias.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_resolver_alias.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_resolver_alias.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_resolver_alias.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_resolver_alias.active IS
    'This value is the after-state active flag, true when absent.';
COMMENT ON COLUMN project_resolver_alias.alias_state IS
    'This value is the after-state alias_state, active when absent.';
COMMENT ON COLUMN project_resolver_alias.from_dns_encoded_name IS
    'This value is the from_dns_encoded_name.';
COMMENT ON COLUMN project_resolver_alias.to_dns_encoded_name IS
    'This value is the to_dns_encoded_name.';
COMMENT ON COLUMN project_resolver_alias.from_name IS
    'This value is the from_name.';
COMMENT ON COLUMN project_resolver_alias.to_logical_name_id IS
    'This value is the after-state to_logical_name_id.';
COMMENT ON COLUMN project_resolver_alias.to_name IS
    'This value is the after-state to_name.';
COMMENT ON COLUMN project_resolver_alias.to_resource_id IS
    'This value is the after-state to_resource_id.';
COMMENT ON COLUMN project_resolver_alias.logical_name_id IS
    'This value is the event''s name.';

CREATE TABLE IF NOT EXISTS project_child_edge_candidate (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    parent_node text NOT NULL,
    child_node text NOT NULL,
    authority_arm text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    owner text,
    owner_getter text,
    labelhash text,
    source_family text NOT NULL,
    PRIMARY KEY (chain_id, namespace, parent_node, child_node, authority_arm),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_child_edge_candidate IS
    'Project-owned ENSv1 and Basenames child edge candidates of family F11: the latest SubregistryChanged per parent, child and arm, kept while ineligible. Candidates are retained per parent: a later edge for the child under another parent adds a row and leaves the earlier parent''s row in place, so the reader selects the latest per child and arm.';
COMMENT ON COLUMN project_child_edge_candidate.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_child_edge_candidate.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_child_edge_candidate.parent_node IS
    'This value is the lower-cased parent node.';
COMMENT ON COLUMN project_child_edge_candidate.child_node IS
    'This value is the lower-cased child node.';
COMMENT ON COLUMN project_child_edge_candidate.authority_arm IS
    'This value is the canonical authority arm of the edge''s registry: basenames for basenames_base_registry, ens_v1 for ens_v1_registry_l1 (children.rs:271-273).';
COMMENT ON COLUMN project_child_edge_candidate.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_child_edge_candidate.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_child_edge_candidate.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_child_edge_candidate.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_child_edge_candidate.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_child_edge_candidate.owner IS
    'This value is the lower-cased edge owner.';
COMMENT ON COLUMN project_child_edge_candidate.owner_getter IS
    'This value is the lower-cased edge owner_getter.';
COMMENT ON COLUMN project_child_edge_candidate.labelhash IS
    'This value is the lower-cased labelhash.';
COMMENT ON COLUMN project_child_edge_candidate.source_family IS
    'This value is the event''s source family.';

CREATE TABLE IF NOT EXISTS project_parent_subregistry (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    subregistry_address text NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_parent_subregistry IS
    'Project-owned ENSv2 parent subregistry of family F11: per parent name, the latest SubregistryChanged address, clears included.';
COMMENT ON COLUMN project_parent_subregistry.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_parent_subregistry.logical_name_id IS
    'This value is the parent name.';
COMMENT ON COLUMN project_parent_subregistry.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_parent_subregistry.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_parent_subregistry.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_parent_subregistry.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_parent_subregistry.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_parent_subregistry.subregistry_address IS
    'This value is the lower-cased subregistry, empty or zero for a clear.';

CREATE TABLE IF NOT EXISTS project_reverse_tuple (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    reverse_node text,
    source_event text,
    claim_provenance jsonb,
    reverse_position jsonb,
    raw_name jsonb,
    raw_name_bytes jsonb,
    claim_event_identity text,
    claim_position jsonb,
    hydrated_name text,
    attempt_block bigint,
    attempt_hash text,
    attempt_ordinal bigint,
    baseline jsonb,
    PRIMARY KEY (address, coin_type, namespace),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_reverse_tuple IS
    'Project-owned reverse tuples of family F12: per address, coin type and namespace, the latest ReverseChanged and the latest direct claim, with the hydration result once hydration moves into the block.';
COMMENT ON COLUMN project_reverse_tuple.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_reverse_tuple.coin_type IS
    'This value is the coin type.';
COMMENT ON COLUMN project_reverse_tuple.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_reverse_tuple.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_reverse_tuple.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_reverse_tuple.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_reverse_tuple.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_reverse_tuple.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_reverse_tuple.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_reverse_tuple.reverse_node IS
    'This value is the lower-cased reverse node of the latest ReverseChanged.';
COMMENT ON COLUMN project_reverse_tuple.source_event IS
    'This value is that event''s source_event.';
COMMENT ON COLUMN project_reverse_tuple.claim_provenance IS
    'This value is that event''s claim_provenance.';
COMMENT ON COLUMN project_reverse_tuple.reverse_position IS
    'This value is that ReverseChanged''s position.';
COMMENT ON COLUMN project_reverse_tuple.raw_name IS
    'This value is the raw_name of the latest direct claim.';
COMMENT ON COLUMN project_reverse_tuple.raw_name_bytes IS
    'This value is the raw_name_bytes of that claim.';
COMMENT ON COLUMN project_reverse_tuple.claim_event_identity IS
    'This value is that claim''s event identity.';
COMMENT ON COLUMN project_reverse_tuple.claim_position IS
    'This value is that claim''s position.';
COMMENT ON COLUMN project_reverse_tuple.hydrated_name IS
    'This value is the hydrated reverse name; null until hydration moves into the block.';
COMMENT ON COLUMN project_reverse_tuple.attempt_block IS
    'This value is the hydration attempt block.';
COMMENT ON COLUMN project_reverse_tuple.attempt_hash IS
    'This value is the hydration attempt block hash.';
COMMENT ON COLUMN project_reverse_tuple.attempt_ordinal IS
    'This value is the hydration attempt ordinal.';
COMMENT ON COLUMN project_reverse_tuple.baseline IS
    'This value is the pre-hydration baseline.';

CREATE TABLE IF NOT EXISTS project_reverse_node_claim (
    namespace text NOT NULL,
    reverse_node text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text NOT NULL,
    raw_name jsonb,
    raw_name_bytes jsonb,
    PRIMARY KEY (namespace, reverse_node, resolver_address),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_reverse_node_claim IS
    'Project-owned node-selected claim facts of family F12: per node and resolver, the latest name record or version change, the claim a ReverseClaimed tuple selects through the node''s current resolver.';
COMMENT ON COLUMN project_reverse_node_claim.namespace IS
    'This value is the namespace.';
COMMENT ON COLUMN project_reverse_node_claim.reverse_node IS
    'This value is the lower-cased node.';
COMMENT ON COLUMN project_reverse_node_claim.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_reverse_node_claim.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_reverse_node_claim.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_reverse_node_claim.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_reverse_node_claim.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_reverse_node_claim.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_reverse_node_claim.resolver_address IS
    'This value is the lower-cased after-state resolver of the name record or version change; a read follows the node''s resolver pointer to one row.';
COMMENT ON COLUMN project_reverse_node_claim.raw_name IS
    'This value is the record''s raw_name, null when the latest event is a version change.';
COMMENT ON COLUMN project_reverse_node_claim.raw_name_bytes IS
    'This value is the record''s raw_name_bytes.';

CREATE TABLE IF NOT EXISTS project_claim_normalization (
    chain_id text NOT NULL,
    claim_event_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    normalized_name text,
    reason text,
    raw_name jsonb,
    raw_name_bytes jsonb,
    PRIMARY KEY (chain_id, claim_event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_claim_normalization IS
    'Project-owned claim normalization of family F12: the normalization result of each claim event, stored once.';
COMMENT ON COLUMN project_claim_normalization.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_claim_normalization.claim_event_identity IS
    'This value is the claim event.';
COMMENT ON COLUMN project_claim_normalization.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_claim_normalization.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_claim_normalization.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_claim_normalization.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_claim_normalization.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_claim_normalization.status IS
    'This value is success, not_found, invalid_name or unsupported.';
COMMENT ON COLUMN project_claim_normalization.normalized_name IS
    'This value is the normalized name on success.';
COMMENT ON COLUMN project_claim_normalization.reason IS
    'This value is the reason when not successful.';
COMMENT ON COLUMN project_claim_normalization.raw_name IS
    'This value is the claim event''s after-state raw_name, the original claim input.';
COMMENT ON COLUMN project_claim_normalization.raw_name_bytes IS
    'This value is the claim event''s after-state raw_name_bytes.';

CREATE TABLE IF NOT EXISTS project_address_name_fold (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    controller text,
    controller_action text,
    controller_subject text,
    controller_position jsonb,
    token_holder text,
    token_holder_position jsonb,
    registrant text,
    registrant_position jsonb,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_address_name_fold IS
    'Project-owned per-name address fold of family F13: the ordered controller fold, the token holder and the registrant read from the name''s retained F2a rows, unmasked.';
COMMENT ON COLUMN project_address_name_fold.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_name_fold.logical_name_id IS
    'This value is the name.';
COMMENT ON COLUMN project_address_name_fold.block_number IS
    'This value is the block number of the event that last wrote the row.';
COMMENT ON COLUMN project_address_name_fold.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_address_name_fold.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_address_name_fold.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_address_name_fold.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_address_name_fold.controller IS
    'This value is the controller the fold holds after the latest event.';
COMMENT ON COLUMN project_address_name_fold.controller_action IS
    'This value is set or revoke, the latest controller action.';
COMMENT ON COLUMN project_address_name_fold.controller_subject IS
    'This value is that action''s lower-cased subject.';
COMMENT ON COLUMN project_address_name_fold.controller_position IS
    'This value is that action''s position.';
COMMENT ON COLUMN project_address_name_fold.token_holder IS
    'This value is the lower-cased recipient of the latest TokenControlTransferred.';
COMMENT ON COLUMN project_address_name_fold.token_holder_position IS
    'This value is that transfer''s position.';
COMMENT ON COLUMN project_address_name_fold.registrant IS
    'This value is the lower-cased registrant of the latest retained F2a row of the name that names one (a grant''s registrant, a release''s prior registrant, a transfer''s recipient; name_current/build.sql:440-491 unmasked).';
COMMENT ON COLUMN project_address_name_fold.registrant_position IS
    'This value is that row''s position.';

CREATE TABLE IF NOT EXISTS project_address_controller_candidate (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    event_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    normalized_event_id bigint,
    resource_id uuid,
    event_kind text NOT NULL,
    source_family text NOT NULL,
    action text NOT NULL,
    subject text,
    PRIMARY KEY (chain_id, logical_name_id, event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL)),
    CHECK (action IN ('set', 'revoke'))
);
COMMENT ON TABLE project_address_controller_candidate IS
    'Project-owned controller candidates of family F13: every named controller event (AuthorityTransferred, state-derived registry-only SurfaceBound, resource-scoped PermissionChanged) with its resource and position, never pruned, so a read folds the candidates the served admission keeps (address_names.rs:115-283).';
COMMENT ON COLUMN project_address_controller_candidate.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_controller_candidate.logical_name_id IS
    'This value is the event''s name.';
COMMENT ON COLUMN project_address_controller_candidate.event_identity IS
    'This value is the event identity, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_address_controller_candidate.block_number IS
    'This value is the event''s block number.';
COMMENT ON COLUMN project_address_controller_candidate.transaction_index IS
    'This value is the event''s transaction index; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_address_controller_candidate.log_index IS
    'This value is the event''s log index; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_address_controller_candidate.normalized_event_id IS
    'This value names the event in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_address_controller_candidate.resource_id IS
    'This value is the event''s resource, which the admission compares with the selected resource and the registry-only predecessor window.';
COMMENT ON COLUMN project_address_controller_candidate.event_kind IS
    'This value is the event kind.';
COMMENT ON COLUMN project_address_controller_candidate.source_family IS
    'This value is the event''s source family.';
COMMENT ON COLUMN project_address_controller_candidate.action IS
    'This value is set for an AuthorityTransferred, a SurfaceBound and a PermissionChanged whose effective powers hold resource_control, and revoke for any other resource-scoped PermissionChanged, before any read-time mask.';
COMMENT ON COLUMN project_address_controller_candidate.subject IS
    'This value is the lower-cased controller the event names: the registry owner (the zero address for a masked owner word), the SurfaceBound owner or the permission subject.';

CREATE TABLE IF NOT EXISTS project_address_name_index (
    address text NOT NULL,
    logical_name_id text NOT NULL,
    relation text NOT NULL,
    chain_id text NOT NULL,
    PRIMARY KEY (address, logical_name_id, relation)
);
COMMENT ON TABLE project_address_name_index IS
    'Project-owned address-to-name index of family F13, re-derived from the controller candidates, the fold''s token holder and the retained F2a rows of each touched name and never journalled. It holds every address a relation can take under some admission and mask, so reads only remove rows.';
COMMENT ON COLUMN project_address_name_index.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_address_name_index.logical_name_id IS
    'This value is the name.';
COMMENT ON COLUMN project_address_name_index.relation IS
    'This value is registrant, token_holder or effective_controller.';
COMMENT ON COLUMN project_address_name_index.chain_id IS
    'This value is the chain.';
CREATE INDEX IF NOT EXISTS project_address_name_index_name_idx
    ON project_address_name_index (chain_id, logical_name_id);

CREATE TABLE IF NOT EXISTS project_address_record_node_index (
    address text NOT NULL,
    coin_type text NOT NULL,
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    node text NOT NULL,
    logical_name_id text NOT NULL DEFAULT '',
    PRIMARY KEY (address, coin_type, chain_id, resolver_address, node, logical_name_id)
);
COMMENT ON TABLE project_address_record_node_index IS
    'Project-owned inverse address record index of family F14 for node-keyed values, re-derived from project_node_record_value and never journalled. It holds every successful EVM-shaped addr value whatever its partition''s version, with the name it was written under; readers apply the version and link boundary.';
COMMENT ON COLUMN project_address_record_node_index.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_address_record_node_index.coin_type IS
    'This value is the coin type.';
COMMENT ON COLUMN project_address_record_node_index.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_record_node_index.resolver_address IS
    'This value is the resolver.';
COMMENT ON COLUMN project_address_record_node_index.node IS
    'This value is the node.';
COMMENT ON COLUMN project_address_record_node_index.logical_name_id IS
    'This value is the name the value was written under, empty for a value written with no name; a named write whose node is not the name''s namehash is found by it.';
CREATE INDEX IF NOT EXISTS project_address_record_node_index_node_idx
    ON project_address_record_node_index (chain_id, resolver_address, node);
CREATE INDEX IF NOT EXISTS project_address_record_node_index_name_idx
    ON project_address_record_node_index (chain_id, logical_name_id)
    WHERE logical_name_id <> '';

CREATE TABLE IF NOT EXISTS project_address_record_id_index (
    address text NOT NULL,
    coin_type text NOT NULL,
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    record_id text NOT NULL,
    PRIMARY KEY (address, coin_type, chain_id, resolver_address, record_id)
);
COMMENT ON TABLE project_address_record_id_index IS
    'Project-owned inverse address record index of family F14 for record-id values, re-derived from project_record_id_value and never journalled.';
COMMENT ON COLUMN project_address_record_id_index.address IS
    'This value is the lower-cased address.';
COMMENT ON COLUMN project_address_record_id_index.coin_type IS
    'This value is the coin type.';
COMMENT ON COLUMN project_address_record_id_index.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN project_address_record_id_index.resolver_address IS
    'This value is the resolver.';
COMMENT ON COLUMN project_address_record_id_index.record_id IS
    'This value is the record id.';
CREATE INDEX IF NOT EXISTS project_address_record_id_index_record_idx
    ON project_address_record_id_index (chain_id, resolver_address, record_id);

CREATE TABLE IF NOT EXISTS project_name_history (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    first_block_number bigint NOT NULL,
    created_at timestamptz NOT NULL,
    has_ens_v2_events boolean NOT NULL,
    event_arms jsonb NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
);
COMMENT ON TABLE project_name_history IS
    'Project-owned whole-history facts of family F1: per name, the block and time of the first readable event naming it, whether ENSv2 events name it, and the authority arms its authority events vote. Written once when the first event naming the name is applied and changed only when a later event adds a fact; a row leaves only when undo removes the block that created it. The composed name reader reads it for created_at, the coverage of a name with no selected arm, and the arm of a name with no open binding.';
COMMENT ON COLUMN project_name_history.chain_id IS
    'This value is the chain whose events wrote the row; each chain keeps its own row for a name.';
COMMENT ON COLUMN project_name_history.logical_name_id IS
    'This value is the name.';
COMMENT ON COLUMN project_name_history.namespace IS
    'This value is the namespace of the first event naming the name.';
COMMENT ON COLUMN project_name_history.block_number IS
    'This value is the block number of the event that last changed the row.';
COMMENT ON COLUMN project_name_history.transaction_index IS
    'This value is the transaction index of the event that last changed the row; null with log_index for a synthesised event, which sorts before every transaction of its block.';
COMMENT ON COLUMN project_name_history.log_index IS
    'This value is the log index of the event that last changed the row; null with transaction_index for a synthesised event.';
COMMENT ON COLUMN project_name_history.event_identity IS
    'This value is the event identity of the event that last changed the row, the final tiebreak of the canonical event order, compared as bytes.';
COMMENT ON COLUMN project_name_history.normalized_event_id IS
    'This value names the event that last changed the row in normalized_events as attribution only; it never takes part in ordering.';
COMMENT ON COLUMN project_name_history.first_block_number IS
    'This value is the block of the first readable event naming the name.';
COMMENT ON COLUMN project_name_history.created_at IS
    'This value is the block time of the first readable event naming the name, which the name row reports as registration.created_at (name_current/build.sql, the created lateral).';
COMMENT ON COLUMN project_name_history.has_ens_v2_events IS
    'This value is whether any event naming the name came from the ENSv2 root, registry or registrar families (name_current/build.sql, the corpus lateral).';
COMMENT ON COLUMN project_name_history.event_arms IS
    'This value is the sorted array of authority arms (ens_v1, ens_v2, basenames) voted by the name''s registration, renewal, release, expiry change, authority transfer, token transfer and authority epoch events (name_authority/build.sql, event_arms), without the ENSv2 root and registry expiry changes, which never vote, and releases, which the reader decides against the binding candidates.';

CREATE TABLE IF NOT EXISTS project_name_summary (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    authority_arm text,
    serving boolean NOT NULL,
    registration_status text,
    expires_at timestamptz,
    registered_at timestamptz,
    zero_owner boolean NOT NULL,
    recompose_at bigint,
    owner text,
    PRIMARY KEY (chain_id, logical_name_id)
);
COMMENT ON TABLE project_name_summary IS
    'Project-owned name summary: fields the child and label lists filter, sort and count inside one statement. The family writer refreshes touched names from the shared name composition and journals every change for undo. Every name surface has a row. A name without a composed row has no serving resource or registration, while its selected authority arm and next clock boundary can remain. No composed name row is persisted.';
COMMENT ON COLUMN project_name_summary.chain_id IS
    'This value is the chain of the name''s surface.';
COMMENT ON COLUMN project_name_summary.logical_name_id IS
    'This value is the name.';
COMMENT ON COLUMN project_name_summary.namespace IS
    'This value is the namespace of the name.';
COMMENT ON COLUMN project_name_summary.authority_arm IS
    'This value is the selected authority arm (ens_v1, ens_v2 or basenames) of provenance.authority_selection, null when no single arm is selected; the child lists take a child''s arm from it.';
COMMENT ON COLUMN project_name_summary.serving IS
    'This value is whether the name has a serving resource (provenance.read_reachability.serving_resource_id), which admits an ownerless registry child.';
COMMENT ON COLUMN project_name_summary.registration_status IS
    'This value is declared_summary.registration.status; the subnames expiry fence drops a released child.';
COMMENT ON COLUMN project_name_summary.expires_at IS
    'This value is the expiry the subnames expiry sort and fence read: the first timestamp of the registration and control expiry fields, as address_names/query.rs reads it.';
COMMENT ON COLUMN project_name_summary.registered_at IS
    'This value is the registration time the subnames registration sort reads: registration.registered_at, else registration.registration_date.';
COMMENT ON COLUMN project_name_summary.zero_owner IS
    'This value is whether the latest ENSv1 or Basenames registry Transfer attributed to the name names the zero owner, which zeroes a registry child''s owner. A Transfer is attributed as the served child build does: by the name it carries, else the latest named registry event of any kind of its resource and family, else an active, readable surface at its node.';
COMMENT ON COLUMN project_name_summary.recompose_at IS
    'This value is the first second, in Unix seconds, after the block the row was composed at at which the composition can change with no fact changing: a binding interval opening or closing, or a NameWrapper expiry or grace boundary, kept whether or not the name composes a row. The family step composes the name again at the first block whose time reaches it; null when no such second exists. It is a count of seconds, not a timestamp, because a NameWrapper expiry can be any 64-bit word, past the last instant a timestamp holds.';
COMMENT ON COLUMN project_name_summary.owner IS
    'This value is the owner the composed name row serves: declared_summary.control.owner, else control.registry_owner, lower-cased; null when the first present one is blank or the name composes no row. The registry labels'' owner and exclude_owner filters read it.';
CREATE INDEX IF NOT EXISTS project_name_summary_recompose_idx
    ON project_name_summary (chain_id, recompose_at)
    WHERE recompose_at IS NOT NULL;

-- The composed expiring listing's candidate indexes.
CREATE INDEX IF NOT EXISTS project_lifecycle_event_expiry_idx
    ON project_lifecycle_event (expiry_seconds)
    WHERE expiry_seconds IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_lifecycle_event_inexact_expiry_idx
    ON project_lifecycle_event (chain_id)
    WHERE expiry_seconds IS NULL AND jsonb_typeof(expiry) = 'number';
CREATE INDEX IF NOT EXISTS project_wrapper_state_expiry_idx
    ON project_wrapper_state (expiry_seconds)
    WHERE expiry_seconds IS NOT NULL;

CREATE INDEX IF NOT EXISTS project_grant_subject_resource_idx
    ON project_grant (subject COLLATE "C", resource_id, scope COLLATE "C");

CREATE INDEX IF NOT EXISTS project_registry_binding_observation_owner_target_idx
    ON project_registry_binding_observation (chain_id, registry_contract, registry_owner, target_resource_id);

CREATE INDEX IF NOT EXISTS project_registry_binding_observation_owner_resource_idx
    ON project_registry_binding_observation (chain_id, registry_contract, registry_owner, resource_id);

CREATE INDEX IF NOT EXISTS project_grant_resource_subject_idx
    ON project_grant (resource_id, subject COLLATE "C", scope COLLATE "C");