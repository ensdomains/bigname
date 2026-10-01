-- TYR-129: the ENSv2 resolver alias path is no longer interpreted (docs/upstream.md:L62-L68).
-- No admitted resolver emits `AliasChanged`, so both alias family tables are empty; dropping them
-- changes no served data. The official Sepolia PermissionedResolver declares no such event: its
-- own event list holds only ResourceArgument
-- (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L9-L17 @ ens_v2_sepolia_20260916@366de741),
-- and the record events it inherits are ResolverCreated, Linked and Cleared
-- (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/interfaces/IRecordResolver.sol:L26-L42 @ ens_v2_sepolia_20260916@366de741).
-- The admitted manifest manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v1.toml declares
-- `Linked` (L60-L63) among its ABI events (L54-L177) and no `AliasChanged`; no other manifest
-- declares it.
-- Each drop is guarded on its own table, so an empty phase schema stays empty.
DROP TABLE IF EXISTS bigname_phase.project_name_alias;
DROP TABLE IF EXISTS bigname_phase.project_resolver_alias;
