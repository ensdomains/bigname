-- TYR-129: the ENSv2 resolver alias path is no longer interpreted. No admitted resolver emits
-- `AliasChanged`, so both alias family tables are empty; dropping them changes no served data.
-- Each drop is guarded on its own table, so an empty phase schema stays empty.
DROP TABLE IF EXISTS bigname_phase.project_name_alias;
DROP TABLE IF EXISTS bigname_phase.project_resolver_alias;
