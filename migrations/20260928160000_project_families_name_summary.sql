-- Existing schema-v2 databases gain project_name_summary (TYR-36 step 7b slice
-- 2b): the per-name fields the child and label lists read inside one statement,
-- which the family step writes for the names each block touches. A database
-- without the table has families built without it, so it resets every owned
-- key family and the next family run rebuilds them, writing every name's
-- summary. It also adds the indexes the writer's work list reads. An empty
-- schema-migration database has no phase baseline yet, so this migration is a
-- no-op there and phase-runner init-schema installs the same table.
--
-- Apply it before starting a release that writes project_name_summary, whatever
-- BIGNAME_SERVE_FROM_FAMILIES says: that release's family step writes the table
-- on every block and fails until it exists. Apply it before the switch is ever
-- turned on as well: the reset deletes the family marker, so with the switch on
-- every fenced route answers 409 stale until the family rebuild finishes. It
-- takes no marker lock, so a family run in flight when it applies fails once on
-- the missing marker and the next run rebuilds the families.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

IF to_regclass('bigname_phase.project_name_summary') IS NULL THEN
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
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_owner_event$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_registry_binding_observation$ddl$;
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
    EXECUTE $ddl$DELETE FROM bigname_phase.project_reverse_node_claim$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_claim_normalization$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_name_fold$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_controller_candidate$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_name_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_record_node_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_address_record_id_index$ddl$;
    EXECUTE $ddl$DELETE FROM bigname_phase.project_name_history$ddl$;
    -- An earlier deployment of the preceding slice may already have named pointer facts.
    -- Reset them with the rest of the family state when installing this summary family.
    IF to_regclass('bigname_phase.project_named_resource_pointer') IS NOT NULL THEN
        EXECUTE $ddl$DELETE FROM bigname_phase.project_named_resource_pointer$ddl$;
    END IF;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_name_summary (
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
    PRIMARY KEY (chain_id, logical_name_id)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_name_summary IS
    'Project-owned name summary family (TYR-36 step 7b slice 2b): per name, the fields the child and label lists filter, sort and count by inside one statement, which they cannot compose at read for every child of a parent. The family step writes the row for every name a block touches, from the same composition as the composed name row (bigname_storage::families::name), and journals it like every other family. Every name with a surface has a row; one the composed reader serves no row for has no serving arm, resource or registration fields, but retains the next recomposition deadline. Each column but zero_owner is the value the served lists read from the name''s name_current row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.chain_id IS
    'This value is the chain of the name''s surface.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.logical_name_id IS
    'This value is the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.namespace IS
    'This value is the namespace of the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.authority_arm IS
    'This value is the selected authority arm (ens_v1, ens_v2 or basenames) of provenance.authority_selection, null when no single arm is selected; the child lists take a child''s arm from it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.serving IS
    'This value is whether the name has a serving resource (provenance.read_reachability.serving_resource_id), which admits an ownerless registry child.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.registration_status IS
    'This value is declared_summary.registration.status; the subnames expiry fence drops a released child.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.expires_at IS
    'This value is the expiry the subnames expiry sort and fence read: the first timestamp of the registration and control expiry fields, as address_names/query.rs reads it.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.registered_at IS
    'This value is the registration time the subnames registration sort reads: registration.registered_at, else registration.registration_date.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.zero_owner IS
    'This value is whether the latest ENSv1 or Basenames registry Transfer attributed to the name names the zero owner, which zeroes a registry child''s owner. A Transfer is attributed as the served child build does: by the name it carries, else the latest named registry event of any kind of its resource and family, else an active, readable surface at its node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_name_summary.recompose_at IS
    'This value is the first second, in Unix seconds, after the block the row was composed at at which the composition can change with no fact changing: a binding interval opening or closing, or a NameWrapper expiry or grace boundary, kept whether or not the name composes a row. The family step composes the name again at the first block whose time reaches it; null when no such second exists. It is a count of seconds, not a timestamp, because a NameWrapper expiry can be any 64-bit word, past the last instant a timestamp holds.'
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_name_summary_recompose_idx
    ON bigname_phase.project_name_summary (chain_id, recompose_at)
    WHERE recompose_at IS NOT NULL
$ddl$;
-- The work list of the summary writer finds the names that read a changed
-- resource through these (crates/project families/derived/summary.rs), and
-- the summary's zero-owner attribution reads a name's registry owner events
-- by name and by resource (crates/storage families/name/summary.rs).
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_binding_candidate_predecessor_idx
    ON bigname_phase.project_binding_candidate (chain_id, predecessor_resource_id)
    WHERE predecessor_resource_id IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_binding_candidate_lease_idx
    ON bigname_phase.project_binding_candidate (chain_id, lease_resource_id)
    WHERE lease_resource_id IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_lifecycle_association_target_idx
    ON bigname_phase.project_lifecycle_association (chain_id, target_resource_id)
    WHERE target_resource_id IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_owner_event_name_idx
    ON bigname_phase.project_registry_owner_event (chain_id, logical_name_id)
    WHERE logical_name_id IS NOT NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_owner_event_resource_idx
    ON bigname_phase.project_registry_owner_event (chain_id, resource_id)
    WHERE resource_id IS NOT NULL
$ddl$;
END
$migration$;
