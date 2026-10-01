-- TYR-129: the ENSv2 resolver alias path is no longer interpreted (docs/upstream.md:L62-L68).
-- No admitted resolver emits `AliasChanged`, so both alias family tables are expected to be
-- empty. The official Sepolia PermissionedResolver declares no such event: its own event list
-- holds only ResourceArgument
-- (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L9-L17 @ ens_v2_sepolia_20260916@366de741),
-- and the record events it inherits are ResolverCreated, Linked and Cleared
-- (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/interfaces/IRecordResolver.sol:L26-L42 @ ens_v2_sepolia_20260916@366de741).
-- The admitted manifest manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v1.toml declares
-- `Linked` (L60-L63) among its ABI events (L54-L177) and no `AliasChanged`; no other manifest
-- declares it.
-- Current admission does not prove what an older database built from retained June-generation
-- events holds, so the migration refuses to drop a table that still has rows. Each step is
-- guarded on its own table, so an empty phase schema stays empty.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_name_alias') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM bigname_phase.project_name_alias LIMIT 1) THEN
        RAISE EXCEPTION 'bigname_phase.project_name_alias holds rows: the alias tables hold rows, census them before retiring';
    END IF;
END IF;
IF to_regclass('bigname_phase.project_resolver_alias') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM bigname_phase.project_resolver_alias LIMIT 1) THEN
        RAISE EXCEPTION 'bigname_phase.project_resolver_alias holds rows: the alias tables hold rows, census them before retiring';
    END IF;
END IF;
DROP TABLE IF EXISTS bigname_phase.project_name_alias;
DROP TABLE IF EXISTS bigname_phase.project_resolver_alias;
END
$migration$;
