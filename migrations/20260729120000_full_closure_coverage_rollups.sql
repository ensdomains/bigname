-- Rebuildable, fact-derived coverage aggregates for full-closure replay.
--
-- Existing rows are intentionally not backfilled by this migration. The first
-- upgraded proof builds one current-generation snapshot from authoritative
-- backfill facts; later proofs consume the commit-ordered change journal.
CREATE TABLE public.full_closure_coverage_input_revisions (
    chain_id text PRIMARY KEY,
    revision bigint NOT NULL,
    CONSTRAINT full_closure_coverage_input_revisions_revision_check
        CHECK (revision >= 0)
);

CREATE TABLE public.full_closure_coverage_input_changes (
    change_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id text NOT NULL,
    revision bigint NOT NULL,
    change_kind text NOT NULL,
    backfill_coverage_fact_id bigint,
    source_family text NOT NULL,
    scope text NOT NULL,
    address text,
    CONSTRAINT full_closure_coverage_input_changes_revision_check
        CHECK (revision > 0),
    CONSTRAINT full_closure_coverage_input_changes_kind_check
        CHECK (change_kind = ANY (ARRAY['append'::text, 'rebuild'::text])),
    CONSTRAINT full_closure_coverage_input_changes_fact_shape_check
        CHECK (
            (change_kind = 'append'::text)
            = (backfill_coverage_fact_id IS NOT NULL)
        ),
    CONSTRAINT full_closure_coverage_input_changes_scope_check
        CHECK (scope = ANY (ARRAY['address'::text, 'family'::text])),
    CONSTRAINT full_closure_coverage_input_changes_address_scope_check
        CHECK ((scope = 'address'::text) = (address IS NOT NULL))
);

CREATE INDEX full_closure_coverage_input_changes_revision_idx
    ON public.full_closure_coverage_input_changes (chain_id, revision, change_id);

CREATE TABLE public.full_closure_coverage_rollups (
    full_closure_coverage_rollup_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id text NOT NULL,
    raw_log_retention_generation bigint NOT NULL,
    source_family text NOT NULL,
    scope text NOT NULL,
    address text,
    covered_blocks int8multirange NOT NULL,
    CONSTRAINT full_closure_coverage_rollups_tuple_key
        UNIQUE NULLS NOT DISTINCT (
            chain_id,
            raw_log_retention_generation,
            source_family,
            scope,
            address
        ),
    CONSTRAINT full_closure_coverage_rollups_generation_check
        CHECK (raw_log_retention_generation >= 0),
    CONSTRAINT full_closure_coverage_rollups_scope_check
        CHECK (scope = ANY (ARRAY['address'::text, 'family'::text])),
    CONSTRAINT full_closure_coverage_rollups_address_scope_check
        CHECK ((scope = 'address'::text) = (address IS NOT NULL)),
    CONSTRAINT full_closure_coverage_rollups_nonempty_check
        CHECK (covered_blocks <> '{}'::int8multirange)
);

CREATE TABLE public.full_closure_coverage_rollup_states (
    chain_id text PRIMARY KEY,
    proof_format_version text NOT NULL,
    coverage_input_revision bigint NOT NULL,
    raw_log_input_revision bigint NOT NULL,
    raw_log_retention_generation bigint NOT NULL,
    discovery_admission_epoch bigint NOT NULL,
    topic0s_by_family jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT full_closure_coverage_rollup_states_version_check
        CHECK (proof_format_version <> ''),
    CONSTRAINT full_closure_coverage_rollup_states_revisions_check
        CHECK (
            coverage_input_revision >= 0
            AND raw_log_input_revision >= 0
        ),
    CONSTRAINT full_closure_coverage_rollup_states_authority_check
        CHECK (
            raw_log_retention_generation >= 0
            AND discovery_admission_epoch >= 0
        ),
    CONSTRAINT full_closure_coverage_rollup_states_topics_check
        CHECK (jsonb_typeof(topic0s_by_family) = 'object')
);

-- Coverage fact identity rows are append-only in production. Once a saved
-- aggregate exists, inserts can extend it without revisiting prior facts.
-- Before then, changes advance only the compact per-chain input revision: the
-- first proof must rebuild from all facts, so per-fact journal rows would be
-- unused generation-zero/upgrade write amplification. Unexpected updates or
-- deletes conservatively mark both the old and new aggregate keys for a
-- source-of-truth rebuild when saved state exists.
CREATE FUNCTION public.journal_full_closure_coverage_fact_inserts()
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
            hashtextextended('full_closure_coverage:' || affected_chain, 0)
        );
        INSERT INTO public.full_closure_coverage_input_revisions (
            chain_id,
            revision
        )
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.full_closure_coverage_input_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.full_closure_coverage_input_changes (
            chain_id,
            revision,
            change_kind,
            backfill_coverage_fact_id,
            source_family,
            scope,
            address
        )
        SELECT
            affected_chain,
            next_revision,
            'append',
            backfill_coverage_fact_id,
            source_family,
            scope,
            address
        FROM inserted_rows
        WHERE chain_id = affected_chain
          AND EXISTS (
              SELECT 1
              FROM public.full_closure_coverage_rollup_states state
              WHERE state.chain_id = affected_chain
          );
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.journal_full_closure_coverage_fact_updates()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
BEGIN
    FOR affected_chain IN
        SELECT chain_id
        FROM (
            SELECT chain_id FROM deleted_rows
            UNION
            SELECT chain_id FROM inserted_rows
        ) affected_chains
        ORDER BY chain_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('full_closure_coverage:' || affected_chain, 0)
        );
        INSERT INTO public.full_closure_coverage_input_revisions (
            chain_id,
            revision
        )
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.full_closure_coverage_input_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.full_closure_coverage_input_changes (
            chain_id,
            revision,
            change_kind,
            backfill_coverage_fact_id,
            source_family,
            scope,
            address
        )
        SELECT DISTINCT
            affected_chain,
            next_revision,
            'rebuild',
            NULL::bigint,
            changed.source_family,
            changed.scope,
            changed.address
        FROM (
            SELECT chain_id, source_family, scope, address FROM deleted_rows
            UNION
            SELECT chain_id, source_family, scope, address FROM inserted_rows
        ) changed
        WHERE changed.chain_id = affected_chain
          AND EXISTS (
              SELECT 1
              FROM public.full_closure_coverage_rollup_states state
              WHERE state.chain_id = affected_chain
          );
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.journal_full_closure_coverage_fact_deletes()
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
            hashtextextended('full_closure_coverage:' || affected_chain, 0)
        );
        INSERT INTO public.full_closure_coverage_input_revisions (
            chain_id,
            revision
        )
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.full_closure_coverage_input_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.full_closure_coverage_input_changes (
            chain_id,
            revision,
            change_kind,
            backfill_coverage_fact_id,
            source_family,
            scope,
            address
        )
        SELECT DISTINCT
            affected_chain,
            next_revision,
            'rebuild',
            NULL::bigint,
            source_family,
            scope,
            address
        FROM deleted_rows
        WHERE chain_id = affected_chain
          AND EXISTS (
              SELECT 1
              FROM public.full_closure_coverage_rollup_states state
              WHERE state.chain_id = affected_chain
          );
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.journal_full_closure_coverage_fact_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
BEGIN
    FOR affected_chain IN
        SELECT DISTINCT chain_id
        FROM public.full_closure_coverage_rollups
        ORDER BY chain_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('full_closure_coverage:' || affected_chain, 0)
        );
        INSERT INTO public.full_closure_coverage_input_revisions (
            chain_id,
            revision
        )
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.full_closure_coverage_input_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.full_closure_coverage_input_changes (
            chain_id,
            revision,
            change_kind,
            backfill_coverage_fact_id,
            source_family,
            scope,
            address
        )
        SELECT DISTINCT
            affected_chain,
            next_revision,
            'rebuild',
            NULL::bigint,
            source_family,
            scope,
            address
        FROM public.full_closure_coverage_rollups
        WHERE chain_id = affected_chain
          AND EXISTS (
              SELECT 1
              FROM public.full_closure_coverage_rollup_states state
              WHERE state.chain_id = affected_chain
          );
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER journal_full_closure_coverage_fact_inserts
AFTER INSERT ON public.backfill_coverage_facts
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.journal_full_closure_coverage_fact_inserts();

CREATE TRIGGER journal_full_closure_coverage_fact_updates
AFTER UPDATE ON public.backfill_coverage_facts
REFERENCING OLD TABLE AS deleted_rows NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.journal_full_closure_coverage_fact_updates();

CREATE TRIGGER journal_full_closure_coverage_fact_deletes
AFTER DELETE ON public.backfill_coverage_facts
REFERENCING OLD TABLE AS deleted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.journal_full_closure_coverage_fact_deletes();

CREATE TRIGGER journal_full_closure_coverage_fact_truncate
AFTER TRUNCATE ON public.backfill_coverage_facts
FOR EACH STATEMENT
EXECUTE FUNCTION public.journal_full_closure_coverage_fact_truncate();

-- Parent job authority is part of every fact's validity. A relevant job
-- mutation retires or reshapes all of that job's fact contributions and must
-- rebuild their aggregate keys. Routine progress/accounting updates do not.
CREATE FUNCTION public.journal_full_closure_coverage_job_authority_updates()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
BEGIN
    FOR affected_chain IN
        WITH changed_jobs AS (
            SELECT deleted.backfill_job_id
            FROM deleted_jobs deleted
            JOIN inserted_jobs inserted USING (backfill_job_id)
            WHERE ROW(
                deleted.chain_id,
                deleted.status,
                deleted.range_start_block_number,
                deleted.range_end_block_number,
                deleted.raw_log_retention_generation,
                deleted.stored_verification_raw_log_input_revision,
                deleted.stored_verification_from_block,
                deleted.stored_verification_to_block,
                deleted.source_identity
            ) IS DISTINCT FROM ROW(
                inserted.chain_id,
                inserted.status,
                inserted.range_start_block_number,
                inserted.range_end_block_number,
                inserted.raw_log_retention_generation,
                inserted.stored_verification_raw_log_input_revision,
                inserted.stored_verification_from_block,
                inserted.stored_verification_to_block,
                inserted.source_identity
            )
        )
        SELECT DISTINCT fact.chain_id
        FROM public.backfill_coverage_facts fact
        JOIN changed_jobs changed
          ON changed.backfill_job_id = fact.backfill_job_id
        ORDER BY fact.chain_id
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('full_closure_coverage:' || affected_chain, 0)
        );
        INSERT INTO public.full_closure_coverage_input_revisions (
            chain_id,
            revision
        )
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.full_closure_coverage_input_revisions.revision + 1
        RETURNING revision INTO next_revision;

        WITH changed_jobs AS (
            SELECT deleted.backfill_job_id
            FROM deleted_jobs deleted
            JOIN inserted_jobs inserted USING (backfill_job_id)
            WHERE ROW(
                deleted.chain_id,
                deleted.status,
                deleted.range_start_block_number,
                deleted.range_end_block_number,
                deleted.raw_log_retention_generation,
                deleted.stored_verification_raw_log_input_revision,
                deleted.stored_verification_from_block,
                deleted.stored_verification_to_block,
                deleted.source_identity
            ) IS DISTINCT FROM ROW(
                inserted.chain_id,
                inserted.status,
                inserted.range_start_block_number,
                inserted.range_end_block_number,
                inserted.raw_log_retention_generation,
                inserted.stored_verification_raw_log_input_revision,
                inserted.stored_verification_from_block,
                inserted.stored_verification_to_block,
                inserted.source_identity
            )
        )
        INSERT INTO public.full_closure_coverage_input_changes (
            chain_id,
            revision,
            change_kind,
            backfill_coverage_fact_id,
            source_family,
            scope,
            address
        )
        SELECT DISTINCT
            affected_chain,
            next_revision,
            'rebuild',
            NULL::bigint,
            fact.source_family,
            fact.scope,
            fact.address
        FROM public.backfill_coverage_facts fact
        JOIN changed_jobs changed
          ON changed.backfill_job_id = fact.backfill_job_id
        WHERE fact.chain_id = affected_chain
          AND EXISTS (
              SELECT 1
              FROM public.full_closure_coverage_rollup_states state
              WHERE state.chain_id = affected_chain
          );
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER journal_full_closure_coverage_job_authority_updates
AFTER UPDATE ON public.backfill_jobs
REFERENCING OLD TABLE AS deleted_jobs NEW TABLE AS inserted_jobs
FOR EACH STATEMENT
EXECUTE FUNCTION public.journal_full_closure_coverage_job_authority_updates();

-- This non-concurrent index briefly blocks backfill_jobs writes while a
-- pre-applied migration builds it.
CREATE INDEX backfill_jobs_completed_generation_coverage_idx
    ON public.backfill_jobs (
        chain_id,
        raw_log_retention_generation,
        backfill_job_id
    )
    WHERE status = 'completed'::backfill_lifecycle_status;
