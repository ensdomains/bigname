-- Install before the matching full Interpret re-derivation and Project rebuild.
-- Empty legacy schema-migration databases wait for init-schema to install the phase baseline.
-- Existing Project summaries are reset under their publication lock; identity text is rebuilt
-- by the actual Interpret/import writer, never stamped ready by a SQL backfill.
DO $migration$
DECLARE family text;
BEGIN
IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN RETURN; END IF;
IF to_regclass('bigname_phase.project_family_marker') IS NOT NULL THEN
    LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
END IF;

-- Current spelling and lexical membership are derived atomically by Interpret and verified imports.
CREATE TABLE IF NOT EXISTS bigname_phase.name_search_documents (
    search_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_name_id text NOT NULL UNIQUE REFERENCES bigname_phase.name_surfaces(logical_name_id) ON DELETE CASCADE,
    chain_id text NOT NULL,
    namespace text NOT NULL,
    namehash text NOT NULL,
    name text NOT NULL CHECK (name <> ''),
    display_name_override text,
    spelling_class smallint NOT NULL,
    CONSTRAINT name_search_documents_class_check CHECK (
        (spelling_class IN (0, 1) AND octet_length(name) <= 2000)
        OR (spelling_class = 2 AND octet_length(name) > 2000)),
    CONSTRAINT name_search_documents_display_check CHECK (
        display_name_override IS NULL OR spelling_class = 0)
);
COMMENT ON TABLE bigname_phase.name_search_documents IS
    'Identity-derived current search spelling. Class 0 is raw-backed, 1 structural through 2000 bytes, 2 longer structural. Only raw-backed display may differ. Interpret and verified imports update this atomically with postings; Project owns no text.';
CREATE INDEX IF NOT EXISTS name_search_documents_structural_order_idx
    ON bigname_phase.name_search_documents (name, namespace, namehash, logical_name_id)
    WHERE spelling_class = 1;
CREATE INDEX IF NOT EXISTS name_search_documents_class_idx
    ON bigname_phase.name_search_documents (namespace, spelling_class, search_id);
CREATE TABLE IF NOT EXISTS bigname_phase.name_search_postings (
    namespace text NOT NULL,
    spelling_class smallint NOT NULL CHECK (spelling_class BETWEEN 0 AND 2),
    token_kind smallint NOT NULL CHECK (token_kind IN (1, 2)),
    token_length smallint NOT NULL CHECK (token_length BETWEEN 1 AND 3),
    token_bytes bytea NOT NULL CHECK (octet_length(token_bytes) BETWEEN 1 AND 12),
    search_id bigint NOT NULL REFERENCES bigname_phase.name_search_documents(search_id) ON DELETE CASCADE,
    PRIMARY KEY (namespace, spelling_class, token_kind, token_length, token_bytes, search_id)
);
COMMENT ON TABLE bigname_phase.name_search_postings IS
    'Distinct Unicode-scalar contains (kind 1) and anchored-prefix (kind 2) tokens of lengths 1 through 3. The leading key supports a bounded ordered probe; search_id supports atomic exact membership replacement.';
CREATE INDEX IF NOT EXISTS name_search_postings_document_idx ON bigname_phase.name_search_postings (search_id);
CREATE INDEX IF NOT EXISTS name_surfaces_search_labelhashes_idx
    ON bigname_phase.name_surfaces USING gin (labelhashes) WHERE raw_name IS NULL;

IF to_regclass('bigname_phase.project_name_summary') IS NULL THEN RETURN; END IF;
IF (SELECT count(*) FROM pg_catalog.pg_attribute
    WHERE attrelid=to_regclass('bigname_phase.project_name_summary')
      AND attname IN ('search_supported','search_fields','search_creation_transport_resource_id')
      AND attnum>0 AND NOT attisdropped) < 3 THEN
    FOREACH family IN ARRAY ARRAY[
        'project_family_marker', 'project_family_undo', 'project_repair_record',
        'child_registration_events', 'project_name_state', 'project_binding_candidate',
        'project_lifecycle_key_state',
        'project_lifecycle_triple_summary', 'project_lifecycle_association',
        'project_lifecycle_event', 'project_child_registration_state', 'project_wrapper_state',
        'project_registry_node_state', 'project_registry_owner_event',
        'project_registry_binding_observation', 'project_resolver_classification',
        'project_universal_resolver_proxy', 'project_registry_pointer',
        'project_resource_pointer', 'project_named_resource_pointer',
        'project_node_record_partition', 'project_node_record_value', 'project_record_id_value',
        'project_resolver_link', 'project_grant', 'project_resource_admin_aggregate',
        'project_account_approval', 'project_ens_v2_entry_owner',
        'project_ens_v2_registry_parent',
        'project_child_edge_candidate', 'project_parent_subregistry', 'project_reverse_tuple',
        'project_reverse_node_claim', 'project_claim_normalization', 'project_address_name_fold',
        'project_address_controller_candidate', 'project_text_hydration_work',
        'project_reverse_hydration_work', 'project_address_name_index',
        'project_address_record_node_index', 'project_address_record_id_index',
        'project_name_history', 'project_name_summary',
        'project_address_history_anchor', 'project_history_source_edge',
        'project_history_source', 'project_history_catalogue_marker'
    ] LOOP
        IF to_regclass('bigname_phase.' || family) IS NOT NULL THEN
            EXECUTE format('DELETE FROM bigname_phase.%I', family);
        END IF;
    END LOOP;
    ALTER TABLE bigname_phase.project_name_summary
        ADD COLUMN IF NOT EXISTS search_supported boolean NOT NULL,
        ADD COLUMN IF NOT EXISTS search_fields jsonb,
        ADD COLUMN IF NOT EXISTS search_creation_transport_resource_id uuid;
    ALTER TABLE bigname_phase.project_name_summary ADD CONSTRAINT project_name_summary_search_check CHECK (
        (search_supported AND search_fields IS NOT NULL AND jsonb_typeof(search_fields) = 'object'
         AND search_fields ? 'registration_status')
        OR (NOT search_supported AND search_fields IS NULL AND search_creation_transport_resource_id IS NULL));
END IF;
COMMENT ON COLUMN bigname_phase.project_name_summary.search_supported IS
    'Whether the shared composition yields a supported search row. A supported ownerless or unregistered row is distinct from an absent or unsupported composition.';
COMMENT ON COLUMN bigname_phase.project_name_summary.search_fields IS
    'Shared public registration and ENSv1 fields with omission/null preserved, finished wrapper expiry and declared creation only. No identity spelling, fallback clock or deferred wrapper marker.';
COMMENT ON COLUMN bigname_phase.project_name_summary.search_creation_transport_resource_id IS
    'Optional selected Basenames resource whose live pointer and Ethereum context may add a creation timestamp; no stored cross-chain eligibility decision.';
END
$migration$;
