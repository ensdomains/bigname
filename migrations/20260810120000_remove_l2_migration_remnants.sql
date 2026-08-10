-- Narrow an initialized phase schema to the same active binding and permission
-- vocabularies installed by the current schema-v2 baseline. The deployment
-- boundary performs the required full-history interpretation and projection
-- walk after this schema-migration.

DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM bigname_phase.normalized_events
            WHERE after_state -> 'scope' ->> 'kind' IN (
                'migration_derived',
                'transport_derived'
            )
        ) THEN
            RAISE EXCEPTION
                'cannot remove permission scopes: normalized events still use removed values';
        END IF;
    END IF;

    IF to_regclass('bigname_phase.surface_bindings') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM bigname_phase.surface_bindings
            WHERE binding_kind = 'migration_rebind'
        ) THEN
            RAISE EXCEPTION
                'cannot remove migration_rebind: surface bindings still use it';
        END IF;

        ALTER TABLE bigname_phase.surface_bindings
            DROP CONSTRAINT IF EXISTS surface_bindings_binding_kind_check,
            ADD CONSTRAINT surface_bindings_binding_kind_check
                CHECK (
                    binding_kind IN (
                        'declared_registry_path',
                        'linked_subregistry_path',
                        'resolver_alias_path',
                        'observed_wildcard_path',
                        'observed_only'
                    )
                );
    END IF;

    IF to_regclass('bigname_phase.name_current') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM bigname_phase.name_current
            WHERE binding_kind = 'migration_rebind'
        ) THEN
            RAISE EXCEPTION
                'cannot remove migration_rebind: current names still use it';
        END IF;

        ALTER TABLE bigname_phase.name_current
            DROP CONSTRAINT IF EXISTS name_current_binding_kind_check,
            ADD CONSTRAINT name_current_binding_kind_check
                CHECK (
                    binding_kind IS NULL
                    OR binding_kind IN (
                        'declared_registry_path',
                        'linked_subregistry_path',
                        'resolver_alias_path',
                        'observed_wildcard_path',
                        'observed_only'
                    )
                );
    END IF;

    IF to_regclass('bigname_phase.address_names_current') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM bigname_phase.address_names_current
            WHERE binding_kind = 'migration_rebind'
        ) THEN
            RAISE EXCEPTION
                'cannot remove migration_rebind: current address-name rows still use it';
        END IF;

        ALTER TABLE bigname_phase.address_names_current
            DROP CONSTRAINT IF EXISTS address_names_current_binding_kind_check,
            ADD CONSTRAINT address_names_current_binding_kind_check
                CHECK (
                    binding_kind IN (
                        'declared_registry_path',
                        'linked_subregistry_path',
                        'resolver_alias_path',
                        'observed_wildcard_path',
                        'observed_only'
                    )
                );
    END IF;

    IF to_regclass('bigname_phase.permissions_current') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM bigname_phase.permissions_current
            WHERE scope_kind IN ('migration_derived', 'transport_derived')
        ) THEN
            RAISE EXCEPTION
                'cannot remove permission scopes: current rows still use removed values';
        END IF;

        ALTER TABLE bigname_phase.permissions_current
            DROP CONSTRAINT IF EXISTS permissions_current_scope_kind_check,
            ADD CONSTRAINT permissions_current_scope_kind_check
                CHECK (
                    scope_kind IN (
                        'root',
                        'registry',
                        'resource',
                        'resolver',
                        'record_manager'
                    )
                );
    END IF;
END
$migration$;
