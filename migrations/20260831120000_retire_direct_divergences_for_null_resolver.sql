-- Fresh installs receive this trigger from the phase baseline. Existing phase
-- schemas receive it additively so durable divergence observations are kept.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.name_current') IS NULL
        OR to_regclass('bigname_phase.resolution_divergences') IS NULL
    THEN
        RETURN;
    END IF;

    EXECUTE $function$
        CREATE OR REPLACE FUNCTION
            bigname_phase.retire_direct_divergences_for_null_resolver()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, bigname_phase, pg_temp
        AS $body$
        DECLARE
            surface_on_ethereum_mainnet boolean;
        BEGIN
            -- Rust lookup admission excludes resolver.status = 'unsupported', but
            -- retirement intentionally does not: once the exact resolver is null,
            -- prior direct-resolver observations are stale regardless of that status.
            IF NEW.namespace = 'ens'
                AND NEW.declared_summary -> 'resolver' ? 'chain_id'
                AND NEW.declared_summary -> 'resolver' ? 'address'
                AND NEW.declared_summary -> 'resolver' -> 'chain_id' = 'null'::jsonb
                AND NEW.declared_summary -> 'resolver' -> 'address' = 'null'::jsonb
            THEN
                EXECUTE format(
                    'SELECT EXISTS (
                        SELECT 1 FROM %I.name_surfaces
                        WHERE logical_name_id = $1
                          AND chain_id = ''ethereum-mainnet''
                    )',
                    TG_TABLE_SCHEMA
                )
                INTO surface_on_ethereum_mainnet
                USING NEW.logical_name_id;

                IF surface_on_ethereum_mainnet THEN
                    EXECUTE format(
                        'UPDATE %I.resolution_divergences
                         SET cleared_at = GREATEST(
                             statement_timestamp(), last_observed_at
                         )
                         WHERE logical_name_id = $1
                           AND resolver_chain_id = ''ethereum-mainnet''
                           AND cleared_at IS NULL',
                        TG_TABLE_SCHEMA
                    )
                    USING NEW.logical_name_id;
                END IF;
            END IF;
            RETURN NEW;
        END
        $body$
    $function$;

    REVOKE ALL ON FUNCTION
        bigname_phase.retire_direct_divergences_for_null_resolver()
        FROM PUBLIC;
    DROP TRIGGER IF EXISTS name_current_retire_null_resolver_divergences
        ON bigname_phase.name_current;
    CREATE TRIGGER name_current_retire_null_resolver_divergences
    AFTER INSERT OR UPDATE OF declared_summary
    ON bigname_phase.name_current
    FOR EACH ROW
    EXECUTE FUNCTION
        bigname_phase.retire_direct_divergences_for_null_resolver();

    UPDATE bigname_phase.resolution_divergences AS divergence
    SET cleared_at = GREATEST(statement_timestamp(), divergence.last_observed_at)
    FROM bigname_phase.name_current AS name
    JOIN bigname_phase.name_surfaces AS surface
      ON surface.logical_name_id = name.logical_name_id
    WHERE divergence.logical_name_id = name.logical_name_id
      AND divergence.resolver_chain_id = 'ethereum-mainnet'
      AND divergence.cleared_at IS NULL
      AND name.namespace = 'ens'
      AND surface.chain_id = 'ethereum-mainnet'
      AND name.declared_summary -> 'resolver' ? 'chain_id'
      AND name.declared_summary -> 'resolver' ? 'address'
      AND name.declared_summary -> 'resolver' -> 'chain_id' = 'null'::jsonb
      AND name.declared_summary -> 'resolver' -> 'address' = 'null'::jsonb;

    COMMENT ON FUNCTION
        bigname_phase.retire_direct_divergences_for_null_resolver()
    IS
        'Retires active direct-resolver observations during projection publication when an ENS Mainnet exact resolver becomes null; it performs no live/indexed comparison.';
END
$migration$;
