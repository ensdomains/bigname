-- Completed startup adapter rows survive successful boots and are reusable
-- only by the adapter derivation and applied schema state that produced them.
-- Existing rows intentionally remain NULL so the first upgraded boot fails
-- closed and rebuilds each family before publishing a versioned completion.
ALTER TABLE public.normalized_replay_adapter_checkpoints
    ADD COLUMN adapter_semantic_version bigint,
    ADD COLUMN schema_migration_count bigint,
    ADD COLUMN schema_migration_max_version bigint,
    ADD CONSTRAINT normalized_replay_adapter_checkpoints_adapter_version_check CHECK (
        adapter_semantic_version IS NULL
        OR adapter_semantic_version > 0
    ),
    ADD CONSTRAINT normalized_replay_adapter_checkpoints_schema_version_check CHECK (
        (
            schema_migration_count IS NULL
            AND schema_migration_max_version IS NULL
        )
        OR (
            schema_migration_count > 0
            AND schema_migration_max_version > 0
        )
    );

-- Commit-ordered lineage mutations for startup adapter lineage-prefix keys.
-- Header-anchor backfills routinely add rows below the current head without
-- touching raw logs, so the head identity alone cannot describe the retained
-- lineage corpus consumed by stateful ENSv1 adapters.
CREATE TABLE public.chain_lineage_mutation_revisions (
    chain_id text PRIMARY KEY,
    revision bigint NOT NULL,
    CONSTRAINT chain_lineage_mutation_revisions_revision_check CHECK (revision >= 0)
);

-- One row per committed lineage-revision bump records the lowest affected
-- block. Startup checkpoint reuse can therefore distinguish live tail growth
-- above a completed scan from a mutation inside the consumed prefix. The
-- composite primary key also makes a missing revision in an expected interval
-- detectable instead of treating incomplete evidence as proof.
CREATE TABLE public.chain_lineage_mutation_revision_evidence (
    chain_id text NOT NULL,
    revision bigint NOT NULL,
    min_affected_block_number bigint NOT NULL,
    CONSTRAINT chain_lineage_mutation_revision_evidence_pkey
        PRIMARY KEY (chain_id, revision),
    CONSTRAINT chain_lineage_mutation_revision_evidence_revision_check
        CHECK (revision > 0),
    CONSTRAINT chain_lineage_mutation_revision_evidence_block_number_check
        CHECK (min_affected_block_number >= 0)
);

-- Drain pre-migration lineage writers before snapshotting existing chains and
-- keep new writes out until the statement triggers are installed.
LOCK TABLE public.chain_lineage IN SHARE ROW EXCLUSIVE MODE;

INSERT INTO public.chain_lineage_mutation_revisions (chain_id, revision)
SELECT DISTINCT chain_id, 0
FROM public.chain_lineage;

CREATE FUNCTION public.bump_chain_lineage_mutation_revision_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
    min_affected_block_number bigint;
BEGIN
    FOR affected_chain IN
        SELECT DISTINCT chain_id FROM inserted_rows ORDER BY chain_id
    LOOP
        SELECT MIN(block_number)
        INTO STRICT min_affected_block_number
        FROM inserted_rows
        WHERE chain_id = affected_chain;

        INSERT INTO public.chain_lineage_mutation_revisions (chain_id, revision)
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.chain_lineage_mutation_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.chain_lineage_mutation_revision_evidence (
            chain_id,
            revision,
            min_affected_block_number
        )
        VALUES (affected_chain, next_revision, min_affected_block_number);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.bump_chain_lineage_mutation_revision_after_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
    min_affected_block_number bigint;
BEGIN
    FOR affected_chain IN
        SELECT chain_id
        FROM (
            SELECT chain_id FROM deleted_rows
            UNION
            SELECT chain_id FROM inserted_rows
        ) AS affected_chains
        ORDER BY chain_id
    LOOP
        SELECT MIN(block_number)
        INTO STRICT min_affected_block_number
        FROM (
            SELECT block_number
            FROM deleted_rows
            WHERE chain_id = affected_chain
            UNION ALL
            SELECT block_number
            FROM inserted_rows
            WHERE chain_id = affected_chain
        ) AS affected_rows;

        INSERT INTO public.chain_lineage_mutation_revisions (chain_id, revision)
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.chain_lineage_mutation_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.chain_lineage_mutation_revision_evidence (
            chain_id,
            revision,
            min_affected_block_number
        )
        VALUES (affected_chain, next_revision, min_affected_block_number);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.bump_chain_lineage_mutation_revision_after_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_chain text;
    next_revision bigint;
    min_affected_block_number bigint;
BEGIN
    FOR affected_chain IN
        SELECT DISTINCT chain_id FROM deleted_rows ORDER BY chain_id
    LOOP
        SELECT MIN(block_number)
        INTO STRICT min_affected_block_number
        FROM deleted_rows
        WHERE chain_id = affected_chain;

        INSERT INTO public.chain_lineage_mutation_revisions (chain_id, revision)
        VALUES (affected_chain, 1)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = public.chain_lineage_mutation_revisions.revision + 1
        RETURNING revision INTO next_revision;

        INSERT INTO public.chain_lineage_mutation_revision_evidence (
            chain_id,
            revision,
            min_affected_block_number
        )
        VALUES (affected_chain, next_revision, min_affected_block_number);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER chain_lineage_mutation_revision_insert
AFTER INSERT ON public.chain_lineage
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.bump_chain_lineage_mutation_revision_after_insert();

CREATE TRIGGER chain_lineage_mutation_revision_update
AFTER UPDATE ON public.chain_lineage
REFERENCING OLD TABLE AS deleted_rows NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.bump_chain_lineage_mutation_revision_after_update();

CREATE TRIGGER chain_lineage_mutation_revision_delete
AFTER DELETE ON public.chain_lineage
REFERENCING OLD TABLE AS deleted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.bump_chain_lineage_mutation_revision_after_delete();
