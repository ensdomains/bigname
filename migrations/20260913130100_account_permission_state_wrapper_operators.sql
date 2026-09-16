-- NameWrapper `setApprovalForAll` operators now land in the Project-owned
-- `account_permission_state_current` table beside registry operators, carrying
-- `authority_kind = 'wrapper'` and the `wrapper_control` power that Project fans out to the
-- holder's registrations. The original table admitted only registry operators; this widens the
-- two closed checks and leaves every other constraint in place. Anonymous check constraints are
-- located by definition because the creating migration did not name them. An empty
-- schema-migration database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the widened checks.
DO $migration$
DECLARE
    constraint_name text;
BEGIN
IF to_regclass('bigname_phase.account_permission_state_current') IS NULL THEN
    RETURN;
END IF;

FOR constraint_name IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'bigname_phase.account_permission_state_current'::regclass
      AND contype = 'c'
      AND (
          pg_get_constraintdef(oid) LIKE '%authority_kind = ''registry''%'
          OR pg_get_constraintdef(oid) LIKE '%registry_control%'
      )
LOOP
    EXECUTE format(
        'ALTER TABLE bigname_phase.account_permission_state_current DROP CONSTRAINT %I',
        constraint_name
    );
END LOOP;

-- A database initialized from the current phase baseline already carries the named checks.
EXECUTE $ddl$
ALTER TABLE bigname_phase.account_permission_state_current
    DROP CONSTRAINT IF EXISTS account_permission_state_current_authority_kind_check
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.account_permission_state_current
    DROP CONSTRAINT IF EXISTS account_permission_state_current_effective_powers_check
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.account_permission_state_current
    ADD CONSTRAINT account_permission_state_current_authority_kind_check
    CHECK (authority_kind IN ('registry', 'wrapper'))
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.account_permission_state_current
    ADD CONSTRAINT account_permission_state_current_effective_powers_check
    CHECK (
        (approved AND authority_kind = 'registry'
            AND effective_powers = '["registry_control"]'::jsonb)
        OR (approved AND authority_kind = 'wrapper'
            AND effective_powers = '["wrapper_control"]'::jsonb)
        OR (NOT approved AND effective_powers = '[]'::jsonb)
    )
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.account_permission_state_current.authority_kind IS
    'The authority class: registry (ENSv1/Basenames registry operators) or wrapper (NameWrapper operators).'
$ddl$;
END
$migration$;
