-- Existing schema-v2 databases gain the Project-owned historical membership of
-- direct child registration events that name history reads for
-- include=child_registrations. The table is additive and filled by the full
-- Project rebuild that the matching interpreter content hash rotation requires;
-- an empty schema-migration database has no phase baseline yet, so this
-- migration is a no-op there and phase-runner init-schema installs the same table.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.child_registration_events (
    parent_logical_name_id text NOT NULL,
    event_identity text NOT NULL,
    child_logical_name_id text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    transaction_order_key text NOT NULL,
    log_order_key bigint NOT NULL,
    event_kind text NOT NULL,
    manifest_version bigint NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    target_block_number bigint NOT NULL,
    target_block_hash text NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
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
    CHECK (log_order_key >= -1),
    CHECK (event_kind IN ('RegistrationGranted', 'LabelRegistered')),
    CHECK (manifest_version >= 0),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (target_block_number >= block_number),
    CHECK (btrim(target_block_hash) <> '')
)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS child_registration_events_parent_history_idx
    ON bigname_phase.child_registration_events (
        parent_logical_name_id,
        chain_id,
        block_number,
        block_hash,
        transaction_order_key,
        log_order_key,
        event_identity
    )
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS child_registration_events_chain_block_idx
    ON bigname_phase.child_registration_events (chain_id, block_number)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.child_registration_events IS
    'Project-owned historical membership of direct child registration events: one row per parent name and registration event of a name exactly one label below it. Rebuilt from canonical normalized events and name surfaces; event payloads stay in normalized_events.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.parent_logical_name_id IS
    'This value identifies the parent name: the event namespace and the namehash of the child surface labels without its first label.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.event_identity IS
    'This value identifies the registration event in normalized_events; name history joins the event by it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.child_logical_name_id IS
    'This value identifies the child name the event carried when it happened.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.namespace IS
    'This value identifies the name system shared by the parent and the child.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.chain_id IS
    'This value identifies the chain of the event and of the child surface.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.block_number IS
    'This value is the event block height, the first history order key.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.block_hash IS
    'This value is the event block hash, a history order key and the readable-lineage check.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.transaction_order_key IS
    'This value is the event transaction hash, or an empty string when the event has none, so it orders as history orders a missing hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.log_order_key IS
    'This value is the event log index, or -1 when the event has none, so it orders as history orders a missing index.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.event_kind IS
    'This value is the stored registration kind of the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.manifest_version IS
    'This value records the manifest version that admitted the event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.provenance IS
    'This object cites the normalized event row and source family the membership was derived from.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.target_block_number IS
    'This value identifies the Project target height of the publication that wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.target_block_hash IS
    'This value identifies the Project target hash of the publication that wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.last_recomputed_at IS
    'This Project-owned maintenance time records the latest rebuild of the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.inserted_at IS
    'This Project-owned maintenance time records the first insertion of the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON INDEX bigname_phase.child_registration_events_parent_history_idx IS
    'This bounded index serves one parent''s child registrations in history order on one chain, in both directions. Every key is a bounded identifier or hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON INDEX bigname_phase.child_registration_events_chain_block_idx IS
    'This bounded index lets Project replace one chain''s rows by block range.'
$ddl$;
END
$migration$;
