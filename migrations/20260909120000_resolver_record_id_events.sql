-- Admit record-ID resolver links and permission-argument observations.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bigname_phase.normalized_events'::regclass
          AND conname = 'normalized_events_event_kind_check_record_id'
    ) THEN
        ALTER TABLE bigname_phase.normalized_events
        ADD CONSTRAINT normalized_events_event_kind_check_record_id CHECK (event_kind IN (
            'AccountPermissionChanged', 'AliasChanged', 'AuthorityEpochChanged',
            'AuthorityTransferred', 'ContractDiscovered', 'ExpiryChanged',
            'MigrationApplied', 'ParentChanged', 'PermissionChanged',
            'PermissionScopeChanged', 'PreimageObserved', 'RecordChanged',
            'RecordVersionChanged', 'RegistrarNameRegistered', 'RegistrationGranted',
            'RegistrationReleased', 'RegistrationRenewed', 'RegistrationReserved',
            'RegistryCreated', 'ResolverChanged', 'ResolverPermissionArgument',
            'ResolverRecordLinked', 'ReverseChanged', 'RootPermissionChanged',
            'SourceManifestUpdated', 'SubregistryChanged', 'SurfaceBound',
            'SurfaceUnbound', 'TokenControlTransferred', 'TokenRegenerated',
            'TokenResourceLinked', 'Upgraded'
        )) NOT VALID;
    END IF;
END
$migration$;
