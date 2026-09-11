-- Existing schema-v2 databases gain the Project-owned reverse index over current
-- `addr:<coin_type>` resolver records (names that resolve to an address). The
-- table is additive and rebuilt by Project; an empty schema-migration database
-- has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same table.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.address_names_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.address_records_current (
    address text NOT NULL,
    coin_type text NOT NULL,
    logical_name_id text NOT NULL
        REFERENCES bigname_phase.name_surfaces (logical_name_id),
    namespace text NOT NULL,
    raw_name text NOT NULL,
    namehash text NOT NULL,
    surface_binding_id uuid NOT NULL
        REFERENCES bigname_phase.surface_bindings (surface_binding_id),
    resource_id uuid NOT NULL
        REFERENCES bigname_phase.resources (resource_id),
    record_resource_id uuid NOT NULL
        REFERENCES bigname_phase.resources (resource_id),
    binding_kind text NOT NULL,
    record_key text NOT NULL,
    support_status text NOT NULL,
    unsupported_reason text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    chain_positions jsonb NOT NULL DEFAULT '{}'::jsonb,
    canonicality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamptz NOT NULL DEFAULT now(),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (address, coin_type, logical_name_id),
    CHECK (address = lower(address) AND address ~ '^0x[0-9a-f]{40}$'),
    CHECK (coin_type ~ '^[0-9]+$'),
    CHECK (btrim(namespace) <> ''),
    CHECK (btrim(namehash) <> ''),
    CONSTRAINT address_records_current_logical_identity_check
        CHECK (logical_name_id = namespace || ':' || namehash),
    CHECK (record_key = 'addr:' || coin_type OR record_key = 'addr:2147483648'),
    CHECK (support_status IN ('supported', 'unsupported')),
    CHECK (
        (support_status = 'supported' AND unsupported_reason IS NULL)
        OR (
            support_status = 'unsupported'
            AND unsupported_reason IS NOT NULL
            AND btrim(unsupported_reason) <> ''
        )
    ),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(chain_positions) = 'object'),
    CHECK (jsonb_typeof(canonicality_summary) = 'object'),
    CHECK (manifest_version > 0)
)
$ddl$;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS address_records_current_address_sort_idx
    ON bigname_phase.address_records_current (
        address, coin_type, namespace, raw_name, logical_name_id
    )
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS address_records_current_name_idx
    ON bigname_phase.address_records_current (logical_name_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS address_records_current_resource_idx
    ON bigname_phase.address_records_current (resource_id)
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS address_records_current_record_resource_idx
    ON bigname_phase.address_records_current (record_resource_id)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.address_records_current IS
    'Project-owned reverse index over current addr:<coin_type> resolver records: one row per address a record resolves to, coin type, and current bound name. Rebuilt from record_inventory_current; not serving truth for forward record values.'
$ddl$;
END
$migration$;
