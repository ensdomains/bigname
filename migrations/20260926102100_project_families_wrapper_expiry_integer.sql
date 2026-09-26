-- Existing schema-v2 databases gain the project_wrapper_state.expiry_seconds
-- comment (TYR-36 step 2) that states the family keeps only an integral expiry,
-- in place of the 101900 statement that a decimal counts by value.
-- Comments only; no column, index or row changes. An empty schema-migration
-- database has no phase baseline yet, so this migration is a no-op there and
-- phase-runner init-schema installs the same comment.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_wrapper_state.expiry_seconds IS
    'This value is the latest wrapper expiry when a JSON integer from 0 to 18446744073709551615, the range the served numeric read keeps (address_names.rs wrapper_expiries, children.rs latest_wrapper_expiries); null otherwise. A decimal spelling such as 1.0 or 1.5, which the served read keeps as that numeric, is null here: the Project reads event payloads without arbitrary precision, so a decimal can arrive rounded (9007199254740991.0 as 9007199254740990). The adapter writes the expiry as a JSON integer (adapters schema_v2/protocol/v1/wrapper.rs decodes a uint64), so only a hand-written payload reaches the difference.'
$ddl$;
END
$migration$;
