-- Commit-ordered raw-log input revisions for incremental stateful adapters.
--
-- raw_log_id is sequence-allocation ordered, not commit ordered.  A transaction
-- can therefore commit a lower raw_log_id after a cache has observed a higher
-- one.  These per-chain revisions serialize at transaction commit, while the
-- per-block-hash rows let an adapter prove that no later mutation touched its
-- cached ancestor path.
CREATE TABLE public.raw_log_staging_input_revisions (
    chain_id text PRIMARY KEY,
    revision bigint NOT NULL,
    retention_generation bigint NOT NULL DEFAULT 0,
    retained_history_complete boolean NOT NULL,
    incomplete_since timestamp with time zone,
    proven_retention_generation bigint,
    proven_discovery_admission_epoch bigint,
    proven_through_block bigint,
    CONSTRAINT raw_log_staging_input_revisions_revision_check CHECK (revision >= 0),
    CONSTRAINT raw_log_staging_input_revisions_retention_generation_check CHECK (
        retention_generation >= 0
    ),
    CONSTRAINT raw_log_staging_input_revisions_completeness_check CHECK (
        retained_history_complete = (incomplete_since IS NULL)
    ),
    CONSTRAINT raw_log_staging_input_revisions_proof_shape_check CHECK (
        (
            retained_history_complete
            AND proven_retention_generation IS NOT NULL
            AND proven_discovery_admission_epoch IS NOT NULL
            AND proven_through_block IS NOT NULL
            AND proven_retention_generation = retention_generation
        )
        OR (
            NOT retained_history_complete
            AND proven_retention_generation IS NULL
            AND proven_discovery_admission_epoch IS NULL
            AND proven_through_block IS NULL
        )
    ),
    CONSTRAINT raw_log_staging_input_revisions_proof_values_check CHECK (
        COALESCE(proven_retention_generation, 0) >= 0
        AND COALESCE(proven_discovery_admission_epoch, 0) >= 0
        AND COALESCE(proven_through_block, 0) >= 0
    )
);

ALTER TABLE public.backfill_jobs
    ADD COLUMN raw_log_retention_generation bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT backfill_jobs_raw_log_retention_generation_check CHECK (
        raw_log_retention_generation >= 0
    );

CREATE TABLE public.raw_log_staging_block_revisions (
    chain_id text NOT NULL REFERENCES public.raw_log_staging_input_revisions(chain_id) ON DELETE CASCADE,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    revision bigint NOT NULL,
    CONSTRAINT raw_log_staging_block_revisions_pkey PRIMARY KEY (chain_id, block_hash),
    CONSTRAINT raw_log_staging_block_revisions_block_number_check CHECK (block_number >= 0),
    CONSTRAINT raw_log_staging_block_revisions_revision_check CHECK (revision > 0)
);

CREATE INDEX raw_log_staging_block_revisions_changed_idx
    ON public.raw_log_staging_block_revisions (chain_id, revision, block_hash);

-- Durable stateful-adapter checkpoints must record the raw-log corpus version
-- from which their private state was derived. A later commit may insert a
-- lower physical position than the checkpoint cursor, so position alone is
-- not a safe resume proof.
ALTER TABLE public.normalized_replay_adapter_checkpoints
    ADD COLUMN raw_log_retention_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN raw_log_input_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT normalized_replay_adapter_checkpoints_raw_log_version_check CHECK (
        raw_log_retention_generation >= 0
        AND raw_log_input_revision >= 0
    );

-- The global automatic replay cursor is also a durable checkpoint. Persisting
-- the same commit-ordered input version lets catch-up rewind for a raw-log
-- commit that lands below its cursor even when transaction-start timestamps
-- make that row appear older than the replay pass.
ALTER TABLE public.normalized_replay_cursors
    ADD COLUMN raw_log_retention_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN raw_log_input_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT normalized_replay_cursors_raw_log_version_check CHECK (
        raw_log_retention_generation >= 0
        AND raw_log_input_revision >= 0
    );

-- Drain pre-migration writers before snapshotting the retained corpus, then
-- exclude new INSERT/UPDATE/DELETE statements until the revision triggers are
-- installed at the end of this transaction. Otherwise an old writer can
-- commit after the snapshots while CREATE TRIGGER waits for its table lock,
-- leaving a raw log with no revision or per-block evidence.
LOCK TABLE public.raw_logs IN SHARE ROW EXCLUSIVE MODE;

-- Existing databases cannot prove that their retained staging corpus has
-- never been compacted. Start them in a distinct unknown generation so legacy
-- completed jobs (which receive generation zero above) cannot accidentally
-- authorize absence-based reconciliation after this migration. A fresh,
-- generation-bound backfill must establish the first proof tuple.
WITH known_chains AS (
    SELECT chain_id FROM public.chain_lineage
    UNION
    SELECT chain AS chain_id FROM public.manifest_versions
    UNION
    SELECT chain_id FROM public.discovery_edges
    UNION
    SELECT chain_id FROM public.normalized_events WHERE chain_id IS NOT NULL
    UNION
    SELECT chain_id FROM public.backfill_jobs
    UNION
    SELECT chain_id FROM public.raw_logs
)
INSERT INTO public.raw_log_staging_input_revisions (
    chain_id,
    revision,
    retention_generation,
    retained_history_complete,
    incomplete_since,
    proven_retention_generation,
    proven_discovery_admission_epoch,
    proven_through_block
)
SELECT
    known.chain_id,
    CASE WHEN EXISTS (
        SELECT 1
        FROM public.raw_logs raw
        WHERE raw.chain_id = known.chain_id
    ) THEN 1 ELSE 0 END::bigint,
    1,
    false,
    clock_timestamp(),
    NULL,
    NULL,
    NULL
FROM known_chains known;

INSERT INTO public.raw_log_staging_block_revisions (
    chain_id,
    block_hash,
    block_number,
    revision
)
SELECT chain_id, block_hash, block_number, 1
FROM public.raw_logs
GROUP BY chain_id, block_hash, block_number;

CREATE FUNCTION public.bump_raw_log_staging_revision_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
BEGIN
    FOR affected_chain IN
        SELECT DISTINCT chain_id FROM inserted_rows ORDER BY chain_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('raw_log_staging:' || affected_chain, 0)
        );
        INSERT INTO public.raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES (affected_chain, 1, 0, false, clock_timestamp(), NULL, NULL, NULL)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.raw_log_staging_input_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        SELECT affected_chain, block_hash, block_number, next_revision
        FROM inserted_rows
        WHERE chain_id = affected_chain
        GROUP BY block_hash, block_number
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET
            block_number = EXCLUDED.block_number,
            revision = EXCLUDED.revision;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.bump_raw_log_staging_revision_after_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    destructive_change boolean;
    next_revision bigint;
BEGIN
    FOR affected_chain, destructive_change IN
        WITH changed_rows AS (
            SELECT
                inserted.chain_id AS inserted_chain_id,
                deleted.chain_id AS deleted_chain_id,
                inserted.raw_log_id IS NULL
                    OR deleted.raw_log_id IS NULL
                    OR ROW(
                        inserted.chain_id,
                        inserted.block_hash,
                        inserted.block_number,
                        inserted.transaction_hash,
                        inserted.transaction_index,
                        inserted.log_index,
                        inserted.emitting_address,
                        inserted.topics,
                        inserted.data,
                        inserted.canonicality_state
                    ) IS DISTINCT FROM ROW(
                        deleted.chain_id,
                        deleted.block_hash,
                        deleted.block_number,
                        deleted.transaction_hash,
                        deleted.transaction_index,
                        deleted.log_index,
                        deleted.emitting_address,
                        deleted.topics,
                        deleted.data,
                        deleted.canonicality_state
                    ) AS semantic_change,
                inserted.raw_log_id IS NULL
                    OR deleted.raw_log_id IS NULL
                    OR ROW(
                        inserted.chain_id,
                        inserted.block_hash,
                        inserted.block_number,
                        inserted.transaction_hash,
                        inserted.transaction_index,
                        inserted.log_index,
                        inserted.emitting_address,
                        inserted.topics,
                        inserted.data
                    ) IS DISTINCT FROM ROW(
                        deleted.chain_id,
                        deleted.block_hash,
                        deleted.block_number,
                        deleted.transaction_hash,
                        deleted.transaction_index,
                        deleted.log_index,
                        deleted.emitting_address,
                        deleted.topics,
                        deleted.data
                    ) AS destructive_change
            FROM inserted_rows inserted
            FULL JOIN deleted_rows deleted USING (raw_log_id)
        ),
        changed_chains AS (
            SELECT
                affected.chain_id,
                BOOL_OR(changed.destructive_change) AS destructive_change
            FROM changed_rows changed
            CROSS JOIN LATERAL UNNEST(
                ARRAY[changed.inserted_chain_id, changed.deleted_chain_id]
            ) AS affected(chain_id)
            WHERE changed.semantic_change
              AND affected.chain_id IS NOT NULL
            GROUP BY affected.chain_id
        )
        SELECT changed_chains.chain_id, changed_chains.destructive_change
        FROM changed_chains
        ORDER BY changed_chains.chain_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('raw_log_staging:' || affected_chain, 0)
        );
        INSERT INTO public.raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES (
            affected_chain,
            1,
            CASE WHEN destructive_change THEN 1 ELSE 0 END,
            false,
            clock_timestamp(),
            NULL,
            NULL,
            NULL
        )
        ON CONFLICT (chain_id) DO UPDATE
        SET
            revision = public.raw_log_staging_input_revisions.revision + 1,
            retention_generation =
                public.raw_log_staging_input_revisions.retention_generation
                + CASE WHEN destructive_change THEN 1 ELSE 0 END,
            retained_history_complete = CASE
                WHEN destructive_change THEN false
                ELSE public.raw_log_staging_input_revisions.retained_history_complete
            END,
            incomplete_since = CASE
                WHEN destructive_change THEN clock_timestamp()
                ELSE public.raw_log_staging_input_revisions.incomplete_since
            END,
            proven_retention_generation = CASE
                WHEN destructive_change THEN NULL
                ELSE public.raw_log_staging_input_revisions.proven_retention_generation
            END,
            proven_discovery_admission_epoch = CASE
                WHEN destructive_change THEN NULL
                ELSE public.raw_log_staging_input_revisions.proven_discovery_admission_epoch
            END,
            proven_through_block = CASE
                WHEN destructive_change THEN NULL
                ELSE public.raw_log_staging_input_revisions.proven_through_block
            END
        RETURNING revision INTO next_revision;

        INSERT INTO public.raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        SELECT DISTINCT ON (changed.block_hash)
            affected_chain,
            changed.block_hash,
            changed.block_number,
            next_revision
        FROM (
            SELECT
                inserted.raw_log_id,
                inserted.chain_id,
                inserted.block_hash,
                inserted.block_number,
                true AS is_current
            FROM inserted_rows inserted
            FULL JOIN deleted_rows deleted USING (raw_log_id)
            WHERE inserted.raw_log_id IS NULL
               OR deleted.raw_log_id IS NULL
               OR ROW(
                    inserted.chain_id,
                    inserted.block_hash,
                    inserted.block_number,
                    inserted.transaction_hash,
                    inserted.transaction_index,
                    inserted.log_index,
                    inserted.emitting_address,
                    inserted.topics,
                    inserted.data,
                    inserted.canonicality_state
               ) IS DISTINCT FROM ROW(
                    deleted.chain_id,
                    deleted.block_hash,
                    deleted.block_number,
                    deleted.transaction_hash,
                    deleted.transaction_index,
                    deleted.log_index,
                    deleted.emitting_address,
                    deleted.topics,
                    deleted.data,
                    deleted.canonicality_state
               )

            UNION ALL

            SELECT
                deleted.raw_log_id,
                deleted.chain_id,
                deleted.block_hash,
                deleted.block_number,
                false AS is_current
            FROM inserted_rows inserted
            FULL JOIN deleted_rows deleted USING (raw_log_id)
            WHERE inserted.raw_log_id IS NULL
               OR deleted.raw_log_id IS NULL
               OR ROW(
                    inserted.chain_id,
                    inserted.block_hash,
                    inserted.block_number,
                    inserted.transaction_hash,
                    inserted.transaction_index,
                    inserted.log_index,
                    inserted.emitting_address,
                    inserted.topics,
                    inserted.data,
                    inserted.canonicality_state
               ) IS DISTINCT FROM ROW(
                    deleted.chain_id,
                    deleted.block_hash,
                    deleted.block_number,
                    deleted.transaction_hash,
                    deleted.transaction_index,
                    deleted.log_index,
                    deleted.emitting_address,
                    deleted.topics,
                    deleted.data,
                    deleted.canonicality_state
               )
        ) changed
        WHERE changed.chain_id = affected_chain
          AND changed.block_hash IS NOT NULL
          AND changed.block_number IS NOT NULL
        -- The revision row is hash-identified. If an UPDATE retains a hash but
        -- corrects its height, collapse OLD and NEW to one conflict key and
        -- retain the current (NEW) block number. raw_log_id only makes a
        -- malformed multi-row correction deterministic.
        ORDER BY changed.block_hash, changed.is_current DESC, changed.raw_log_id DESC
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET
            block_number = EXCLUDED.block_number,
            revision = EXCLUDED.revision;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.bump_raw_log_staging_revision_after_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
BEGIN
    FOR affected_chain IN
        SELECT DISTINCT chain_id FROM deleted_rows ORDER BY chain_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('raw_log_staging:' || affected_chain, 0)
        );
        INSERT INTO public.raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES (affected_chain, 1, 1, false, clock_timestamp(), NULL, NULL, NULL)
        ON CONFLICT (chain_id) DO UPDATE
        SET
            revision = public.raw_log_staging_input_revisions.revision + 1,
            retention_generation = public.raw_log_staging_input_revisions.retention_generation + 1,
            retained_history_complete = false,
            incomplete_since = clock_timestamp(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        RETURNING revision INTO next_revision;

        INSERT INTO public.raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        SELECT affected_chain, block_hash, block_number, next_revision
        FROM deleted_rows
        WHERE chain_id = affected_chain
        GROUP BY block_hash, block_number
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET
            block_number = EXCLUDED.block_number,
            revision = EXCLUDED.revision;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.mark_raw_log_staging_incomplete_after_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.raw_log_staging_input_revisions
    SET
        revision = revision + 1,
        retention_generation = retention_generation + 1,
        retained_history_complete = false,
        incomplete_since = clock_timestamp(),
        proven_retention_generation = NULL,
        proven_discovery_admission_epoch = NULL,
        proven_through_block = NULL;
    RETURN NULL;
END;
$$;

CREATE TRIGGER raw_logs_staging_revision_insert
AFTER INSERT ON public.raw_logs
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.bump_raw_log_staging_revision_after_insert();

CREATE TRIGGER raw_logs_staging_revision_update
AFTER UPDATE ON public.raw_logs
REFERENCING OLD TABLE AS deleted_rows NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.bump_raw_log_staging_revision_after_update();

CREATE TRIGGER raw_logs_staging_revision_delete
AFTER DELETE ON public.raw_logs
REFERENCING OLD TABLE AS deleted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.bump_raw_log_staging_revision_after_delete();

CREATE TRIGGER raw_logs_staging_revision_truncate
AFTER TRUNCATE ON public.raw_logs
FOR EACH STATEMENT
EXECUTE FUNCTION public.mark_raw_log_staging_incomplete_after_truncate();
