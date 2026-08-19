-- Existing schema-v2 databases gain the append-only projection-generation
-- failure audit table. An empty schema-migration database has no phase baseline
-- yet, so this step must be a no-op there; phase-runner init-schema installs the
-- same table afterward.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NOT NULL THEN
        EXECUTE $ddl$
            CREATE TABLE IF NOT EXISTS bigname_phase.project_generation_failures (
                chain_id text NOT NULL,
                target_block_number bigint NOT NULL,
                target_block_hash text NOT NULL,
                interpreter_content_hash text NOT NULL,
                failure_kind text NOT NULL,
                failure_fingerprint text NOT NULL,
                logical_name_id text NOT NULL,
                evidence jsonb NOT NULL,
                detected_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (
                    chain_id, target_block_number, target_block_hash,
                    interpreter_content_hash, failure_kind, failure_fingerprint
                ),
                CHECK (btrim(chain_id) <> ''),
                CHECK (target_block_number >= 0),
                CHECK (btrim(target_block_hash) <> ''),
                CHECK (btrim(interpreter_content_hash) <> ''),
                CHECK (failure_kind = 'dual_current_exact_name_authority'),
                CHECK (failure_fingerprint ~ '^[0-9a-f]{64}$'),
                CHECK (btrim(logical_name_id) <> ''),
                CHECK (jsonb_typeof(evidence) = 'object')
            )
        $ddl$;
        EXECUTE $ddl$
            CREATE INDEX IF NOT EXISTS project_generation_failures_name_idx
                ON bigname_phase.project_generation_failures
                   (chain_id, logical_name_id, target_block_number)
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON TABLE bigname_phase.project_generation_failures IS
                'This append-only table records each projection-blocking invariant failure that aborted a projection generation; its rows are operator diagnostics and never product projections.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.chain_id IS
                'This value identifies the chain whose projection generation failed.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.target_block_number IS
                'This value is the block number of the target the aborted projection generation was publishing.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.target_block_hash IS
                'This value is the block hash of that target, resolvable through lineage as canonical or orphaned after a later reorganization.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.interpreter_content_hash IS
                'This value identifies the interpreter build whose derived input produced the failure.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.failure_kind IS
                'This value names the projection-blocking invariant that aborted the projection generation.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.failure_fingerprint IS
                'This value deterministically fingerprints the semantic conflict so a retried projection generation records no duplicate.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.logical_name_id IS
                'This value identifies the logical name whose conflicting authority blocked publication.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.evidence IS
                'This payload carries the conflicting binding and resource identities, the activated boundary event identity, each block, transaction, and log position, and the canonicality observed at failure.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.project_generation_failures.detected_at IS
                'This time records when the phase runner appended the row, after the projection generation transaction rolled back.'
        $ddl$;
    END IF;
END
$migration$;
