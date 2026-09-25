-- Existing schema-v2 databases gain the owned key family columns, tables and
-- access paths the TYR-36 step 2 review asked for (F1, F2a, F2c, F3, F6, F12,
-- F13 and the derived indexes), and two family tables change key: the F2c
-- registry binding observation is keyed by its observation identity and the
-- F12 node claim by node and resolver. A database whose family tables have
-- the step 2 shape
-- before this review is reset: every family row, the undo journal, the family
-- marker and the repair record are deleted, and the next Project batch
-- rebuilds the families from the chain. Every served path leaves these tables
-- unread. An empty schema-migration database has no phase baseline yet, so
-- this migration is a no-op there and phase-runner init-schema installs the
-- same schema.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_binding_candidate
    ADD COLUMN IF NOT EXISTS authority_key text,
    ADD COLUMN IF NOT EXISTS predecessor_wrapped_registrar_resource_id uuid,
    ADD COLUMN IF NOT EXISTS predecessor_node text,
    ADD COLUMN IF NOT EXISTS bound_owner text
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.authority_key IS
    'This value is the authority_key of the SurfaceBound that opened the binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.predecessor_wrapped_registrar_resource_id IS
    'This value is the registrar lease the handoff''s predecessor recorded when the predecessor is a NameWrapper binding, the lease authority_events.sql:164-186 admits registrar grants and releases of.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.predecessor_node IS
    'This value is the lower-cased node the handoff''s predecessor recorded when it is a NameWrapper binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.bound_owner IS
    'This value is the owner the SurfaceBound that opened the binding reports to the served control block (name_current/build.sql:650-671): null when its owner word is unmasked, else its registry_owner, else its owner, lower-cased; its position is surface_bound_position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.block_number IS
    'This value is the block number of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.transaction_index IS
    'This value is the transaction index of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. Null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.log_index IS
    'This value is the log index of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. Null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.event_identity IS
    'This value is the event identity of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. It is the final tiebreak of the canonical event order, compared as bytes; two bindings one event opened are ordered by surface_binding_id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.normalized_event_id IS
    'This value names the SurfaceBound that opened the binding in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.state_derived IS
    'This value is the state_derived flag of the SurfaceBound that opened the binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.authority_kind IS
    'This value is the authority_kind of the SurfaceBound that opened the binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.registry_only IS
    'This value is true once an AuthorityEpochChanged registry_only was seen on this name and resource, in the binding''s block or later.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.predecessor_resource_id IS
    'This value is the resource of the latest candidate of the same name and arm positioned before a registry-only binding.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.predecessor_position IS
    'This value is that predecessor candidate''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.lease_resource_id IS
    'This value is the lease a registry-only handoff stands for (stage.rs:47-135): the latest ens_v1 registrar grant of the name after the binding, on another resource, with a registrar release of the predecessor''s resource before it; else the predecessor''s resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.lease_position IS
    'This value is the position of that successor grant, else the predecessor''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.wrapped_registrar_resource_id IS
    'This value is the registrar lease the NameWrapper SurfaceBound that opened the binding recorded.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.node IS
    'This value is the lower-cased node of that NameWrapper SurfaceBound.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.transaction_hash IS
    'This value is that NameWrapper SurfaceBound''s transaction hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.emitting_address IS
    'This value is the lower-cased address that emitted that NameWrapper SurfaceBound.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.surface_bound_position IS
    'This value is the position of the SurfaceBound that opened the binding, null when none did.'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_binding_candidate_name_idx
    ON bigname_phase.project_binding_candidate (chain_id, logical_name_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_binding_candidate_resource_idx
    ON bigname_phase.project_binding_candidate (chain_id, resource_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_binding_candidate_wrapped_lease_idx
    ON bigname_phase.project_binding_candidate (chain_id, wrapped_registrar_resource_id)
    WHERE wrapped_registrar_resource_id IS NOT NULL
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_lifecycle_event
    ADD COLUMN IF NOT EXISTS authority_key text
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.authority_key IS
    'This value is the event''s after_state authority_key as ->> reads it, which the served authority context reports with the authority kind (name_current/build.sql:393-395).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_event.authority_kind IS
    'This value is the after-state authority_kind as ->> reads it, null when absent; the admission reads default it to registrar (COALESCE(NULLIF(authority_kind, ''''), ''registrar'')) and the served name block reports it as it is (name_current/build.sql:30).'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_lifecycle_event_decoded_name_idx
    ON bigname_phase.project_lifecycle_event (chain_id, decoded_logical_name_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_lifecycle_event_unnamed_lease_idx
    ON bigname_phase.project_lifecycle_event (chain_id, state_key)
    WHERE state_kind = 'resource' AND source_family = 'ens_v1_registrar_l1'
      AND original_logical_name_id IS NULL AND decoded_logical_name_id IS NULL
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_state.authority_start_positions IS
    'This value maps each authority arm to the position of the name''s latest AuthorityEpochChanged in that arm, with its authority_kind, authority_key, resource and the owner it reports to the served control block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_key_state.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind and authority_key as the payload has them (null when absent), status and the registered_at source.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_lifecycle_triple_summary.last_grant IS
    'This value holds the latest RegistrationGranted: position, registrant, expiry, authority_kind and authority_key as the payload has them (null when absent), status and the registered_at source.'
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_registry_node_state
    ADD COLUMN IF NOT EXISTS owner_event_kind text,
    ADD COLUMN IF NOT EXISTS owner_position jsonb,
    ADD COLUMN IF NOT EXISTS owner_resource_id uuid
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_event_kind IS
    'This value is the kind of the registry event that last set the owner group: AuthorityTransferred or SubregistryChanged, both of which report the owner (name_authority/stage.rs:200-261).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_position IS
    'This value is the position of that event, apart from the row''s last-write position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_resource_id IS
    'This value is that event''s resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner IS
    'This value is the lower-cased owner of the latest AuthorityTransferred or SubregistryChanged for the node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_getter IS
    'This value is the lower-cased owner_getter of that event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_getter_reason IS
    'This value is the owner_getter_reason of that event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.owner_word_unmasked IS
    'This value is the owner_word_unmasked of that event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_node_state.registry_owner IS
    'This value is the lower-cased registry_owner of that event.'
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_resolver_classification
    ADD COLUMN IF NOT EXISTS observed_families jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS pointer_families jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS upgrades jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS admission_epoch text
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_resolver_classification IS
    'Project-owned resolver classification of family F3, pinned to the block that last classified it: resolver_current without its sampled sections, from the candidate accumulators the row keeps and the discovery edges, declarations and manifests active at that block. A resolver is classified again when an event names it, a pointer moves to or from it, a resolver edge, its address or a declaration of it starts or stops, and when the admission epoch changes. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.observed_families IS
    'This value maps each resolver family an event proposed the resolver under to its best priority: 3 for an ENSv2 Upgraded proxy, an AliasChanged and either side of a ResolverChanged, 4 for either side of a PermissionChanged scope (resolver/build.sql:5-86).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.pointer_families IS
    'This value maps each resolver family to the number of F4 and F5 pointer rows pointing at the resolver now, standing for the priority 2 name pointers.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.upgrades IS
    'This value maps each family to the latest Upgraded of the proxy: its position, implementation and normalized event id.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.admission_epoch IS
    'This value is the admission epoch the classification was made under (project_family_marker.admission_epoch).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.block_number IS
    'This value is the block number of the latest event that named the resolver, or of the activation block for a row written by a resolver edge, address or declaration activation.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.event_identity IS
    'This value is the event identity of that event, or activation:<block> for an activation; the final tiebreak of the canonical event order, compared as bytes. An epoch change reclassifies the row without moving it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.classification IS
    'This value is the source family, role, basis, implementation, read features, mirror and latest upgrade of the classifying candidate.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_classification.unsupported_reason IS
    'This value is the reason when unsupported: resolver_not_declared, resolver_implementation_unknown, resolver_implementation_not_declared, or resolver_manifest_not_active for a resolver with candidates but no active manifest of its family, which the served build leaves out.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.resolver_address IS
    'This value is the lower-cased resolver of the latest ResolverChanged on the resource, named or not, clears included.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.namehash IS
    'This value is the namehash of the pointer''s name when it is named, else the node the event addresses (child_node, namehash or node).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resource_pointer.nonzero_resolver_address IS
    'This value is the latest pointer whose resolver is a non-empty, non-zero address.'
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_node_record_value
    ADD COLUMN IF NOT EXISTS sibling_status text,
    ADD COLUMN IF NOT EXISTS sibling_address_bytes_hex text,
    ADD COLUMN IF NOT EXISTS raw_name jsonb,
    ADD COLUMN IF NOT EXISTS raw_name_bytes jsonb
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.sibling_status IS
    'This value is the status of the AddressChanged half of a coin-60 pair, the half the served inventory keeps.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.sibling_address_bytes_hex IS
    'This value is the address_bytes_hex of that AddressChanged half.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.raw_name IS
    'This value is the after-state raw_name of a name record, the claim input a reverse claim reads.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_node_record_value.raw_name_bytes IS
    'This value is the after-state raw_name_bytes of a name record.'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_node_record_value_node_idx
    ON bigname_phase.project_node_record_value (chain_id, resolver_address, node)
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_record_id_value
    ADD COLUMN IF NOT EXISTS raw_name jsonb,
    ADD COLUMN IF NOT EXISTS raw_name_bytes jsonb
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.raw_name IS
    'This value is the after-state raw_name of a name record.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_record_id_value.raw_name_bytes IS
    'This value is the after-state raw_name_bytes of a name record.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_resolver_link.resolver_address IS
    'This value is the lower-cased resolver that emitted the ResolverRecordLinked; a link whose payload names another resolver is not kept (resolvers/collections/links.sql:16-17).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_child_edge_candidate.authority_arm IS
    'This value is the canonical authority arm of the edge''s registry: basenames for basenames_base_registry, ens_v1 for ens_v1_registry_l1 (children.rs:271-273).'
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.project_claim_normalization
    ADD COLUMN IF NOT EXISTS raw_name jsonb,
    ADD COLUMN IF NOT EXISTS raw_name_bytes jsonb
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.raw_name IS
    'This value is the claim event''s after-state raw_name, the original claim input.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.raw_name_bytes IS
    'This value is the claim event''s after-state raw_name_bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_name_fold IS
    'Project-owned per-name address fold of family F13: the ordered controller fold, the token holder and the registrant read from the name''s retained F2a rows, unmasked. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.registrant IS
    'This value is the lower-cased registrant of the latest retained F2a row of the name that names one (a grant''s registrant, a release''s prior registrant, a transfer''s recipient; name_current/build.sql:440-491 unmasked).'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.registrant_position IS
    'This value is that row''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_name_index IS
    'Project-owned address-to-name index of family F13, re-derived from the controller candidates, the fold''s token holder and the retained F2a rows of each touched name and never journalled. It holds every address a relation can take under some admission and mask, so reads only remove rows. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_index.relation IS
    'This value is registrant, token_holder or effective_controller.'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_address_name_index_name_idx
    ON bigname_phase.project_address_name_index (chain_id, logical_name_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_address_record_node_index_node_idx
    ON bigname_phase.project_address_record_node_index (chain_id, resolver_address, node)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_address_record_id_index_record_idx
    ON bigname_phase.project_address_record_id_index (chain_id, resolver_address, record_id)
$ddl$;
-- The step 2 shape before this review: reset the families and rebuild the reshaped tables.
IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'bigname_phase' AND table_name = 'project_registry_binding_observation'
      AND column_name = 'observation_identity'
) THEN
    EXECUTE $ddl$DELETE FROM bigname_phase.project_family_marker$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_family_undo$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_repair_record$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_name_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_binding_candidate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_key_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_triple_summary$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_association$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_lifecycle_event$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_child_registration_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_wrapper_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_node_state$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resolver_classification$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_pointer$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resource_pointer$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_node_record_partition$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_node_record_value$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_record_id_value$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resolver_link$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_grant$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resource_admin_aggregate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_account_approval$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_name_alias$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_resolver_alias$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_child_edge_candidate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_parent_subregistry$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_reverse_tuple$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_claim_normalization$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_name_fold$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_name_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_record_node_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_record_id_index$ddl$;
    EXECUTE $ddl$DROP TABLE IF EXISTS bigname_phase.project_registry_binding_observation$ddl$;
    EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_registry_binding_observation (
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
)
$ddl$;
    EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_binding_observation IS
    'Project-owned registry binding observations of family F2c: per observation identity (the name, else the resource; permission_resources.rs:10-11), the latest AuthorityTransferred, SubregistryChanged, SurfaceBound or SurfaceUnbound observation with the resource it reaches. The resource summary takes, per target resource, the latest row that reaches it. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.chain_id IS
    'This value is the chain.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.observation_identity IS
    'This value is COALESCE(logical_name_id, resource_id) of the observation, its DISTINCT ON key.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.logical_name_id IS
    'This value is the event''s name, null for an unnamed observation.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.resource_id IS
    'This value is the event''s own resource.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.attributed_via IS
    'This value is name for a named AuthorityTransferred or SubregistryChanged, which reaches the name''s current resource, and own for every other observation, which reaches its own resource.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.target_resource_id IS
    'This value is the resource the observation reaches after the block: for attributed_via name the name''s ENSv1 or Basenames binding active at the block, else resource_id; a block that moves the name''s current binding moves it. A reader whose authority selection differs re-resolves it from logical_name_id.'
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
    'This value is the observation''s raw fact reference and name.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.applicable IS
    'This value is true when owner and contract are well-formed addresses and the owner is not zero.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.clear_event_identity IS
    'This value is the event identity when the observation is not applicable.'
$ddl$;
    EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_binding_observation_target_idx
    ON bigname_phase.project_registry_binding_observation (chain_id, target_resource_id)
$ddl$;
    EXECUTE $ddl$DROP TABLE IF EXISTS bigname_phase.project_reverse_node_claim$ddl$;
    EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_reverse_node_claim (
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
)
$ddl$;
    EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_reverse_node_claim IS
    'Project-owned node-selected claim facts of family F12: per node and resolver, the latest name record or version change, the claim a ReverseClaimed tuple selects through the node''s current resolver. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.namespace IS
    'This value is the namespace.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.reverse_node IS
    'This value is the lower-cased node.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.chain_id IS
    'This value is the chain.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.resolver_address IS
    'This value is the lower-cased after-state resolver of the name record or version change; a read follows the node''s resolver pointer to one row.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.raw_name IS
    'This value is the record''s raw_name, null when the latest event is a version change.'
$ddl$;
    EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.raw_name_bytes IS
    'This value is the record''s raw_name_bytes.'
$ddl$;
END IF;
-- An older family migration re-comments these tables on a baseline that already
-- has the new shape; restate the comments.
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_registry_binding_observation IS
    'Project-owned registry binding observations of family F2c: per observation identity (the name, else the resource; permission_resources.rs:10-11), the latest AuthorityTransferred, SubregistryChanged, SurfaceBound or SurfaceUnbound observation with the resource it reaches. The resource summary takes, per target resource, the latest row that reaches it. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.observation_identity IS
    'This value is COALESCE(logical_name_id, resource_id) of the observation, its DISTINCT ON key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.logical_name_id IS
    'This value is the event''s name, null for an unnamed observation.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.resource_id IS
    'This value is the event''s own resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.attributed_via IS
    'This value is name for a named AuthorityTransferred or SubregistryChanged, which reaches the name''s current resource, and own for every other observation, which reaches its own resource.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.target_resource_id IS
    'This value is the resource the observation reaches after the block: for attributed_via name the name''s ENSv1 or Basenames binding active at the block, else resource_id; a block that moves the name''s current binding moves it. A reader whose authority selection differs re-resolves it from logical_name_id.'
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
    'This value is the observation''s raw fact reference and name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.applicable IS
    'This value is true when owner and contract are well-formed addresses and the owner is not zero.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_registry_binding_observation.clear_event_identity IS
    'This value is the event identity when the observation is not applicable.'
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_reverse_node_claim IS
    'Project-owned node-selected claim facts of family F12: per node and resolver, the latest name record or version change, the claim a ReverseClaimed tuple selects through the node''s current resolver. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.reverse_node IS
    'This value is the lower-cased node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.resolver_address IS
    'This value is the lower-cased after-state resolver of the name record or version change; a read follows the node''s resolver pointer to one row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.raw_name IS
    'This value is the record''s raw_name, null when the latest event is a version change.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.raw_name_bytes IS
    'This value is the record''s raw_name_bytes.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_address_controller_candidate (
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
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_controller_candidate IS
    'Project-owned controller candidates of family F13: every named controller event (AuthorityTransferred, state-derived registry-only SurfaceBound, resource-scoped PermissionChanged) with its resource and position, never pruned, so a read folds the candidates the served admission keeps (address_names.rs:115-283). Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.logical_name_id IS
    'This value is the event''s name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.event_identity IS
    'This value is the event identity, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.block_number IS
    'This value is the event''s block number.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.transaction_index IS
    'This value is the event''s transaction index; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.log_index IS
    'This value is the event''s log index; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.normalized_event_id IS
    'This value names the event in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.resource_id IS
    'This value is the event''s resource, which the admission compares with the selected resource and the registry-only predecessor window.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.event_kind IS
    'This value is the event kind.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.source_family IS
    'This value is the event''s source family.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.action IS
    'This value is set for an AuthorityTransferred, a SurfaceBound and a PermissionChanged whose effective powers hold resource_control, and revoke for any other resource-scoped PermissionChanged, before any read-time mask.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_controller_candidate.subject IS
    'This value is the lower-cased controller the event names: the registry owner (the zero address for a masked owner word), the SurfaceBound owner or the permission subject.'
$ddl$;
END
$migration$;
