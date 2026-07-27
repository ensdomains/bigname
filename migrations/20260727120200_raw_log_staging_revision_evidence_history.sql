-- Preserve one raw-log block witness per input revision. The original
-- (chain_id, block_hash) key compacted repeated mutations of the same block,
-- which made an otherwise complete revision sequence appear to have gaps.
LOCK TABLE public.raw_logs IN SHARE ROW EXCLUSIVE MODE;

-- The legacy key retained only the latest witness for a block hash, so an
-- upgraded database cannot prove which earlier revisions are still complete.
-- Record the current revision as a conservative history floor before changing
-- the key. Checkpoints older than this floor must restart; checkpoints at the
-- floor need evidence only for post-migration revisions.
ALTER TABLE public.raw_log_staging_input_revisions
    ADD COLUMN block_revision_evidence_floor bigint NOT NULL DEFAULT 0;

UPDATE public.raw_log_staging_input_revisions
SET block_revision_evidence_floor = revision;

ALTER TABLE public.raw_log_staging_input_revisions
    ADD CONSTRAINT raw_log_staging_input_revisions_evidence_floor_check CHECK (
        block_revision_evidence_floor >= 0
        AND block_revision_evidence_floor <= revision
    );

ALTER TABLE public.raw_log_staging_block_revisions
    DROP CONSTRAINT raw_log_staging_block_revisions_pkey,
    ADD CONSTRAINT raw_log_staging_block_revisions_pkey
        PRIMARY KEY (chain_id, revision, block_hash);

DROP INDEX public.raw_log_staging_block_revisions_changed_idx;

CREATE OR REPLACE FUNCTION public.bump_raw_log_staging_revision_after_insert()
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
        ON CONFLICT (chain_id, revision, block_hash) DO UPDATE
        SET block_number = EXCLUDED.block_number;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.bump_raw_log_staging_revision_after_update()
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
        -- A revision row is keyed by revision and hash. If an UPDATE retains a
        -- hash but corrects its height, collapse OLD and NEW within that one
        -- revision and retain the current (NEW) block number. raw_log_id only
        -- makes a malformed multi-row correction deterministic.
        ORDER BY changed.block_hash, changed.is_current DESC, changed.raw_log_id DESC
        ON CONFLICT (chain_id, revision, block_hash) DO UPDATE
        SET block_number = EXCLUDED.block_number;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.bump_raw_log_staging_revision_after_delete()
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
        ON CONFLICT (chain_id, revision, block_hash) DO UPDATE
        SET block_number = EXCLUDED.block_number;
    END LOOP;
    RETURN NULL;
END;
$$;
