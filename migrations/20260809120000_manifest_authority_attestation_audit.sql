-- Existing schema-v2 databases gain the append-only attestation audit table.
-- An empty schema-migration database has no phase baseline yet, so this step must be
-- a no-op there; phase-runner init-schema installs the same table afterward.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.chain_phase_state') IS NOT NULL THEN
        -- Preserve outstanding fences created by the pre-generation marker
        -- format. Rows on one chain that still carry the same legacy marker
        -- receive the same database-minted token. Each chain-phase marker then
        -- carries the token of the invalidation that last writes it; a compound
        -- sync can leave Interpret and Project with different tokens.
        EXECUTE $ddl$
            WITH legacy_generations AS MATERIALIZED (
                SELECT chain_id,
                       input_content_hash AS marker,
                       gen_random_uuid()::text AS generation_token
                FROM bigname_phase.chain_phase_state
                WHERE input_content_hash ~ '^manifest-authority:[0-9a-f]{64}$'
                GROUP BY chain_id, input_content_hash
            )
            UPDATE bigname_phase.chain_phase_state phase
            SET input_content_hash = legacy.marker || ':' || legacy.generation_token,
                updated_at = now()
            FROM legacy_generations legacy
            WHERE phase.chain_id = legacy.chain_id
              AND phase.input_content_hash = legacy.marker
        $ddl$;
        EXECUTE $ddl$
            CREATE TABLE IF NOT EXISTS bigname_phase.manifest_authority_attestations (
                chain_id text NOT NULL,
                phase_name text NOT NULL,
                generation_token text NOT NULL,
                authority_fingerprint text NOT NULL,
                redo_from_block_number bigint NOT NULL,
                redo_to_block_number bigint NOT NULL,
                attested_by text NOT NULL,
                attested_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (chain_id, phase_name, generation_token),
                CHECK (btrim(chain_id) <> ''),
                CHECK (phase_name = 'interpret'),
                CHECK (btrim(generation_token) <> ''),
                CHECK (authority_fingerprint ~ '^[0-9a-f]{64}$'),
                CHECK (redo_from_block_number >= 0),
                CHECK (redo_to_block_number >= redo_from_block_number),
                CHECK (btrim(attested_by) <> '')
            )
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON TABLE bigname_phase.manifest_authority_attestations IS
                'This append-only table records each operator-attested manifest-authority discharge.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.chain_id IS
                'This value identifies the chain.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.phase_name IS
                'This value identifies the attested derived phase.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.generation_token IS
                'This value uniquely identifies the manifest-authority invalidation being discharged.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.authority_fingerprint IS
                'This value fingerprints the desired manifest authority for the invalidation.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.redo_from_block_number IS
                'This value is the first block in the attested redo range.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.redo_to_block_number IS
                'This value is the last block in the attested redo range.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.attested_by IS
                'This value identifies the phase-runner command context that supplied the attestation.'
        $ddl$;
        EXECUTE $ddl$
            COMMENT ON COLUMN bigname_phase.manifest_authority_attestations.attested_at IS
                'This time records the transaction that began the attested redo.'
        $ddl$;
    END IF;
END
$migration$;
