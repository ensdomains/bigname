-- Replace the replay-version trigger function without changing the checksum of
-- the migrations that first installed it. The queue's committed-floor exception
-- depends on every later statement seeing a fresh READ COMMITTED snapshot.
CREATE OR REPLACE FUNCTION public.enforce_current_projection_replay_version_fence()
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
        -- The invalidation queue is also an indexer/storage handoff. Producers
        -- can already hold ROW EXCLUSIVE locks on staging input journals before
        -- enqueueing. Waiting for the singleton here would reverse replay's
        -- singleton-then-journal lock order. Read the committed floor without a
        -- row lock instead: the queue row and its retained generation journal
        -- make an enqueue that crosses admission post-replay apply work.
        --
        -- This TOCTOU bound requires READ COMMITTED: after a newer floor commits,
        -- the producer's next statement must see it instead of retaining an old
        -- transaction snapshot. Fail closed for future producers using stronger
        -- isolation rather than silently widening the crossing-write window.
        -- TRUNCATE and unstamped queue writers keep the non-waiting lock path.
        IF TG_TABLE_NAME = 'projection_invalidations'
           AND TG_OP <> 'TRUNCATE'
           AND process_replay_version_setting IS NOT NULL
           AND btrim(process_replay_version_setting) <> '' THEN
            IF current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = format(
                        'fatal projection replay version fence: committed-floor invalidation write requires READ COMMITTED transaction isolation; observed %s',
                        current_setting('transaction_isolation')
                    ),
                    HINT = 'use READ COMMITTED for projection invalidation queue writes';
            END IF;

            SELECT
                projection_replay_version_fence_active,
                projection_replay_version_floor
            INTO
                fence_active,
                persisted_replay_version
            FROM public.current_projection_full_replay_input_revision
            WHERE singleton;
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
            -- A plain read sees the last committed floor without waiting for
            -- the replay owner. It lets the error distinguish a current
            -- fence-aware writer that should retry from an unstamped or
            -- already-outdated process that must terminate.
            SELECT
                projection_replay_version_fence_active,
                projection_replay_version_floor
            INTO
                fence_active,
                persisted_replay_version
            FROM public.current_projection_full_replay_input_revision
            WHERE singleton;

            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = 'fatal projection replay version fence: singleton state is missing; refusing projection-owned write',
                    HINT = 'terminate this process and repair the projection replay fence';
            END IF;

            IF process_replay_version_setting IS NULL
               OR btrim(process_replay_version_setting) = '' THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = 'fatal projection replay version fence: unfenced writer crossed in-progress replay admission',
                    HINT = 'terminate this outdated process';
            END IF;

            BEGIN
                process_replay_version :=
                    process_replay_version_setting::INTEGER;
            EXCEPTION
                WHEN invalid_text_representation
                     OR numeric_value_out_of_range THEN
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

            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'projection replay admission is in progress; retry protected write',
                HINT = 'retry after the replay admission transaction finishes';
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
