-- Describe the Project-owned address-record projection on initialized databases.
-- A database without the phase baseline receives these comments during init-schema.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.address_records_current') IS NULL THEN
        RETURN;
    END IF;

COMMENT ON TABLE bigname_phase.address_records_current IS
    'Project-owned reverse index over current addr:<coin_type> resolver records: one row per address a record resolves to, coin type, and current bound name. Rebuilt from record_inventory_current; not serving truth for forward record values.';
COMMENT ON COLUMN bigname_phase.address_records_current.address IS
    'Lowercase EVM address stored by the selected address record.';
COMMENT ON COLUMN bigname_phase.address_records_current.coin_type IS
    'Decimal coin type selected for reverse address-record membership.';
COMMENT ON COLUMN bigname_phase.address_records_current.logical_name_id IS
    'Logical name identity selected by Project for this reverse membership.';
COMMENT ON COLUMN bigname_phase.address_records_current.namespace IS
    'Namespace of the selected logical name.';
COMMENT ON COLUMN bigname_phase.address_records_current.raw_name IS
    'Selected name text used for reverse address-record ordering.';
COMMENT ON COLUMN bigname_phase.address_records_current.namehash IS
    'Namehash of the selected logical name.';
COMMENT ON COLUMN bigname_phase.address_records_current.surface_binding_id IS
    'Binding selected by Project for the current name.';
COMMENT ON COLUMN bigname_phase.address_records_current.resource_id IS
    'Registration resource referenced by the selected name binding.';
COMMENT ON COLUMN bigname_phase.address_records_current.record_resource_id IS
    'Resource whose resolver record inventory supplies the address value.';
COMMENT ON COLUMN bigname_phase.address_records_current.binding_kind IS
    'Kind of the selected name binding.';
COMMENT ON COLUMN bigname_phase.address_records_current.record_key IS
    'Address record inventory key, including the default EVM key when used as a fallback.';
COMMENT ON COLUMN bigname_phase.address_records_current.support_status IS
    'Whether the selected address record is supported for serving.';
COMMENT ON COLUMN bigname_phase.address_records_current.unsupported_reason IS
    'Reason the selected address record is unsupported, or null for supported rows.';
COMMENT ON COLUMN bigname_phase.address_records_current.provenance IS
    'Evidence for the selected name, resolver, and address record.';
COMMENT ON COLUMN bigname_phase.address_records_current.chain_positions IS
    'Chain positions used by Project to rebuild this membership.';
COMMENT ON COLUMN bigname_phase.address_records_current.canonicality_summary IS
    'Canonicality summary for the projected membership.';
COMMENT ON COLUMN bigname_phase.address_records_current.manifest_version IS
    'Manifest version used to derive the membership.';
COMMENT ON COLUMN bigname_phase.address_records_current.last_recomputed_at IS
    'Database timestamp when Project last rebuilt the membership.';
COMMENT ON COLUMN bigname_phase.address_records_current.inserted_at IS
    'Database timestamp when this projection row was inserted.';
END
$migration$;
