ALTER TABLE public.current_projection_full_replay_input_revision
    ADD COLUMN projection_replay_version_floor INTEGER DEFAULT 1 NOT NULL,
    ADD COLUMN projection_replay_version_fence_active BOOLEAN DEFAULT false NOT NULL,
    ADD CONSTRAINT current_projection_replay_version_floor_check CHECK (
        projection_replay_version_floor > 0
    );

WITH persisted_versions AS (
    SELECT replay_version
    FROM public.current_projection_replay_status
    UNION ALL
    SELECT replay_version
    FROM public.current_projection_replay_attempt
    UNION ALL
    SELECT replay_version
    FROM public.current_projection_staging_checkpoints
)
UPDATE public.current_projection_full_replay_input_revision
SET projection_replay_version_floor = GREATEST(
    projection_replay_version_floor,
    COALESCE((SELECT MAX(replay_version) FROM persisted_versions), 1)
)
WHERE singleton;

-- See docs/glossary.md#projection-replay-version-fence for the terms used by
-- this database boundary. Every new binary stamps its compiled replay version
-- on every database connection and again inside explicit projection/replay
-- write transactions.
-- A connection without that stamp predates this fence. Keeping the fence
-- inactive until the first fence-aware replay owner arrives lets the migration
-- serialize with an already-running writer: all protected statements take the
-- shared row lock below, while activation takes the existing exclusive lock.
CREATE FUNCTION public.enforce_current_projection_replay_version_fence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    fence_active BOOLEAN;
    persisted_replay_version INTEGER;
    process_replay_version_setting TEXT;
    process_replay_version INTEGER;
BEGIN
    process_replay_version_setting := current_setting(
        'bigname.projection_replay_version',
        true
    );

    BEGIN
        -- PostgreSQL acquires the target table lock before a statement trigger
        -- runs. Do not wait here: a replay owner already holding the singleton
        -- lock may need that table lock to publish, especially for TRUNCATE or
        -- primary-name trigger maintenance. Fence-aware writers take the
        -- singleton lock before reaching this trigger, so lock contention here
        -- identifies an unfenced writer crossing replay admission.
        --
        -- The invalidation queue is also an indexer/storage handoff. Its
        -- ordinary INSERT/UPDATE/DELETE table lock is compatible with replay's
        -- queue DML, so a stamped producer may wait without reversing the table
        -- lock order. This lets matching-version intake continue during replay;
        -- the version comparison still rejects an outdated producer after the
        -- replay owner commits. TRUNCATE keeps the non-waiting path.
        IF TG_TABLE_NAME = 'projection_invalidations'
           AND TG_OP <> 'TRUNCATE'
           AND process_replay_version_setting IS NOT NULL
           AND btrim(process_replay_version_setting) <> '' THEN
            SELECT
                projection_replay_version_fence_active,
                projection_replay_version_floor
            INTO
                fence_active,
                persisted_replay_version
            FROM public.current_projection_full_replay_input_revision
            WHERE singleton
            FOR SHARE;
        ELSE
            SELECT
                projection_replay_version_fence_active,
                projection_replay_version_floor
            INTO
                fence_active,
                persisted_replay_version
            FROM public.current_projection_full_replay_input_revision
            WHERE singleton
            FOR SHARE NOWAIT;
        END IF;
    EXCEPTION
        WHEN lock_not_available THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'fatal projection replay version fence: unfenced writer crossed in-progress replay admission',
                HINT = 'terminate this process';
    END;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'fatal projection replay version fence: singleton state is missing; refusing projection-owned write',
            HINT = 'terminate this process and repair the projection replay fence';
    END IF;

    IF NOT fence_active THEN
        RETURN NULL;
    END IF;

    IF process_replay_version_setting IS NULL
       OR btrim(process_replay_version_setting) = '' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'fatal projection replay version fence: process replay version is unstamped and predates the fence; persisted replay version is %s',
                persisted_replay_version
            ),
            HINT = 'terminate this outdated process';
    END IF;

    BEGIN
        process_replay_version := process_replay_version_setting::INTEGER;
    EXCEPTION
        WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = format(
                    'fatal projection replay version fence: process replay version stamp %L is invalid; persisted replay version is %s',
                    process_replay_version_setting,
                    persisted_replay_version
                ),
                HINT = 'terminate this process';
    END;

    IF process_replay_version <= 0
       OR process_replay_version < persisted_replay_version THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'fatal projection replay version fence: process replay version %s is older than persisted replay version %s; refusing projection-owned write',
                process_replay_version,
                persisted_replay_version
            ),
            HINT = 'terminate this outdated process';
    END IF;

    RETURN NULL;
END;
$$;

-- This is the complete static writer set from replay attempt through
-- projection publication and invalidation completion. Dynamic replay staging
-- tables are covered by the checkpoint mutation in the same transaction.
DO $$
DECLARE
    protected_table TEXT;
BEGIN
    FOREACH protected_table IN ARRAY ARRAY[
        'address_names_current',
        'address_names_current_identity_counts',
        'address_names_current_identity_feed',
        'children_current',
        'name_current',
        'permissions_current',
        'permissions_current_publication',
        'permissions_current_resource_summary',
        'primary_names_current',
        'record_inventory_current',
        'resolver_current',
        'projection_apply_cursors',
        'projection_invalidations',
        'projection_invalidation_dead_letters',
        'current_projection_full_replay_input_revision',
        'current_projection_replay_attempt',
        'current_projection_staging_checkpoints',
        'current_projection_replay_status'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER current_projection_replay_version_fence_before_write
             BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.%I
             FOR EACH STATEMENT
             EXECUTE FUNCTION public.enforce_current_projection_replay_version_fence()',
            protected_table
        );
    END LOOP;
END;
$$;
