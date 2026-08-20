-- Admit the block-boundary PreimageObserved derivation emitted by the schema-v2 adapter.
-- Add the replacement without scanning historical rows under the later metadata-swap lock.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_derivation_kind_check_raw_block'
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
            ADD CONSTRAINT normalized_events_derivation_kind_check_raw_block CHECK (
                derivation_kind IN (
                    'ens_v1_reverse_claim',
                    'ens_v1_unwrapped_authority',
                    'ens_v2_migration',
                    'ens_v2_permissions',
                    'ens_v2_registrar',
                    'ens_v2_registry_resource_surface',
                    'ens_v2_resolver',
                    'manifest_sync',
                    'proxy_upgrade',
                    'raw_block_preimage_observation',
                    'raw_log_preimage_observation'
                )
            ) NOT VALID;
    END IF;
END
$migration$;
