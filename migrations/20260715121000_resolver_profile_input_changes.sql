CREATE TABLE public.resolver_profile_input_changes (
    chain_id TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1,
    processed_generation BIGINT NOT NULL DEFAULT 0,
    previous_code_hash TEXT,
    current_code_hash TEXT,
    force_reconciliation BOOLEAN NOT NULL DEFAULT FALSE,
    last_notification_txid BIGINT NOT NULL DEFAULT txid_current(),
    first_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (chain_id, contract_address),
    CONSTRAINT resolver_profile_input_changes_address_lower_check CHECK (
        contract_address = lower(contract_address)
    ),
    CONSTRAINT resolver_profile_input_changes_generation_check CHECK (
        generation > 0
        AND processed_generation >= 0
        AND processed_generation <= generation
    )
);

CREATE INDEX resolver_profile_input_changes_pending_idx
    ON public.resolver_profile_input_changes (
        last_changed_at,
        chain_id,
        contract_address
    )
    WHERE processed_generation < generation;

CREATE FUNCTION public.record_resolver_profile_input_changes(changes JSONB)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    changed_count BIGINT;
BEGIN
    WITH decoded AS (
        SELECT DISTINCT ON (chain_id, contract_address)
            entry ->> 'chain_id' AS chain_id,
            lower(entry ->> 'contract_address') AS contract_address,
            NULLIF(lower(entry ->> 'previous_code_hash'), '') AS previous_code_hash,
            NULLIF(lower(entry ->> 'current_code_hash'), '') AS current_code_hash,
            COALESCE((entry ->> 'force_reconciliation')::BOOLEAN, FALSE)
                AS force_reconciliation
        FROM jsonb_array_elements(COALESCE(changes, '[]'::jsonb)) AS input(entry)
        WHERE entry ->> 'chain_id' IS NOT NULL
          AND btrim(entry ->> 'chain_id') <> ''
          AND entry ->> 'contract_address' IS NOT NULL
          AND btrim(entry ->> 'contract_address') <> ''
        ORDER BY chain_id, contract_address
    ),
    recorded AS (
        INSERT INTO public.resolver_profile_input_changes (
            chain_id,
            contract_address,
            generation,
            processed_generation,
            previous_code_hash,
            current_code_hash,
            force_reconciliation,
            last_notification_txid,
            first_changed_at,
            last_changed_at,
            processed_at
        )
        SELECT
            chain_id,
            contract_address,
            1,
            0,
            previous_code_hash,
            current_code_hash,
            force_reconciliation,
            txid_current(),
            now(),
            now(),
            NULL
        FROM decoded
        ON CONFLICT (chain_id, contract_address)
        DO UPDATE SET
            generation = resolver_profile_input_changes.generation + 1,
            previous_code_hash = CASE
                WHEN resolver_profile_input_changes.processed_generation
                    = resolver_profile_input_changes.generation
                THEN EXCLUDED.previous_code_hash
                ELSE resolver_profile_input_changes.previous_code_hash
            END,
            current_code_hash = EXCLUDED.current_code_hash,
            force_reconciliation = (
                resolver_profile_input_changes.force_reconciliation
                OR EXCLUDED.force_reconciliation
            ),
            last_notification_txid = txid_current(),
            last_changed_at = EXCLUDED.last_changed_at,
            processed_at = NULL
        -- PostgreSQL fires statement-level INSERT and UPDATE triggers for the
        -- two arms of INSERT ... ON CONFLICT DO UPDATE. Suppress a duplicate
        -- notification when both arms observe the same final effective hash
        -- in one transaction. Never deduplicate across transactions: two raw
        -- writers may compute latest under different statement snapshots.
        -- Explicit manifest/discovery kicks also bypass this suppression.
        WHERE EXCLUDED.force_reconciliation
           OR resolver_profile_input_changes.current_code_hash
                IS DISTINCT FROM EXCLUDED.current_code_hash
           OR resolver_profile_input_changes.last_notification_txid
                <> txid_current()
        RETURNING 1
    )
    SELECT COUNT(*)::BIGINT INTO changed_count FROM recorded;

    RETURN changed_count;
END;
$$;

CREATE FUNCTION public.queue_resolver_profile_input_changes_after_raw_code_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    changes JSONB;
BEGIN
    WITH affected_pairs AS (
        SELECT DISTINCT
            inserted.chain_id,
            inserted.contract_address
        FROM inserted_raw_code_hashes inserted
    ),
    before_latest AS (
        SELECT DISTINCT ON (stored.chain_id, stored.contract_address)
            stored.chain_id,
            stored.contract_address,
            lower(stored.code_hash) AS code_hash
        FROM raw_code_hashes stored
        JOIN affected_pairs affected
          ON affected.chain_id = stored.chain_id
         AND affected.contract_address = stored.contract_address
        WHERE stored.canonicality_state <> 'orphaned'::canonicality_state
          AND NOT EXISTS (
              SELECT 1
              FROM inserted_raw_code_hashes inserted
              WHERE inserted.raw_code_hash_id = stored.raw_code_hash_id
          )
        ORDER BY
            stored.chain_id,
            stored.contract_address,
            stored.block_number DESC,
            CASE stored.canonicality_state
                WHEN 'finalized'::canonicality_state THEN 4
                WHEN 'safe'::canonicality_state THEN 3
                WHEN 'canonical'::canonicality_state THEN 2
                WHEN 'observed'::canonicality_state THEN 1
                ELSE 0
            END DESC,
            stored.raw_code_hash_id DESC
    ),
    after_latest AS (
        SELECT DISTINCT ON (stored.chain_id, stored.contract_address)
            stored.chain_id,
            stored.contract_address,
            lower(stored.code_hash) AS code_hash
        FROM raw_code_hashes stored
        JOIN affected_pairs affected
          ON affected.chain_id = stored.chain_id
         AND affected.contract_address = stored.contract_address
        WHERE stored.canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY
            stored.chain_id,
            stored.contract_address,
            stored.block_number DESC,
            CASE stored.canonicality_state
                WHEN 'finalized'::canonicality_state THEN 4
                WHEN 'safe'::canonicality_state THEN 3
                WHEN 'canonical'::canonicality_state THEN 2
                WHEN 'observed'::canonicality_state THEN 1
                ELSE 0
            END DESC,
            stored.raw_code_hash_id DESC
    ),
    effective_changes AS (
        SELECT
            affected.chain_id,
            affected.contract_address,
            before_latest.code_hash AS previous_code_hash,
            after_latest.code_hash AS current_code_hash
        FROM affected_pairs affected
        LEFT JOIN before_latest
          USING (chain_id, contract_address)
        LEFT JOIN after_latest
          USING (chain_id, contract_address)
        WHERE before_latest.code_hash IS DISTINCT FROM after_latest.code_hash
    )
    SELECT jsonb_agg(jsonb_build_object(
        'chain_id', chain_id,
        'contract_address', contract_address,
        'previous_code_hash', previous_code_hash,
        'current_code_hash', current_code_hash
    ))
    INTO changes
    FROM effective_changes;

    IF changes IS NOT NULL THEN
        PERFORM public.record_resolver_profile_input_changes(changes);
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.queue_resolver_profile_input_changes_after_raw_code_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    changes JSONB;
BEGIN
    WITH affected_pairs AS (
        SELECT old_row.chain_id, old_row.contract_address
        FROM old_raw_code_hashes old_row
        UNION
        SELECT new_row.chain_id, new_row.contract_address
        FROM new_raw_code_hashes new_row
    ),
    before_candidates AS (
        SELECT
            stored.raw_code_hash_id,
            stored.chain_id,
            stored.contract_address,
            stored.block_number,
            lower(stored.code_hash) AS code_hash,
            stored.canonicality_state
        FROM raw_code_hashes stored
        JOIN affected_pairs affected
          ON affected.chain_id = stored.chain_id
         AND affected.contract_address = stored.contract_address
        WHERE stored.canonicality_state <> 'orphaned'::canonicality_state
          AND NOT EXISTS (
              SELECT 1
              FROM new_raw_code_hashes new_row
              WHERE new_row.raw_code_hash_id = stored.raw_code_hash_id
          )

        UNION ALL

        SELECT
            old_row.raw_code_hash_id,
            old_row.chain_id,
            old_row.contract_address,
            old_row.block_number,
            lower(old_row.code_hash) AS code_hash,
            old_row.canonicality_state
        FROM old_raw_code_hashes old_row
        WHERE old_row.canonicality_state <> 'orphaned'::canonicality_state
    ),
    before_latest AS (
        SELECT DISTINCT ON (candidate.chain_id, candidate.contract_address)
            candidate.chain_id,
            candidate.contract_address,
            candidate.code_hash
        FROM before_candidates candidate
        ORDER BY
            candidate.chain_id,
            candidate.contract_address,
            candidate.block_number DESC,
            CASE candidate.canonicality_state
                WHEN 'finalized'::canonicality_state THEN 4
                WHEN 'safe'::canonicality_state THEN 3
                WHEN 'canonical'::canonicality_state THEN 2
                WHEN 'observed'::canonicality_state THEN 1
                ELSE 0
            END DESC,
            candidate.raw_code_hash_id DESC
    ),
    after_latest AS (
        SELECT DISTINCT ON (stored.chain_id, stored.contract_address)
            stored.chain_id,
            stored.contract_address,
            lower(stored.code_hash) AS code_hash
        FROM raw_code_hashes stored
        JOIN affected_pairs affected
          ON affected.chain_id = stored.chain_id
         AND affected.contract_address = stored.contract_address
        WHERE stored.canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY
            stored.chain_id,
            stored.contract_address,
            stored.block_number DESC,
            CASE stored.canonicality_state
                WHEN 'finalized'::canonicality_state THEN 4
                WHEN 'safe'::canonicality_state THEN 3
                WHEN 'canonical'::canonicality_state THEN 2
                WHEN 'observed'::canonicality_state THEN 1
                ELSE 0
            END DESC,
            stored.raw_code_hash_id DESC
    ),
    effective_changes AS (
        SELECT
            affected.chain_id,
            affected.contract_address,
            before_latest.code_hash AS previous_code_hash,
            after_latest.code_hash AS current_code_hash
        FROM affected_pairs affected
        LEFT JOIN before_latest
          USING (chain_id, contract_address)
        LEFT JOIN after_latest
          USING (chain_id, contract_address)
        WHERE before_latest.code_hash IS DISTINCT FROM after_latest.code_hash
    )
    SELECT jsonb_agg(jsonb_build_object(
        'chain_id', chain_id,
        'contract_address', contract_address,
        'previous_code_hash', previous_code_hash,
        'current_code_hash', current_code_hash
    ))
    INTO changes
    FROM effective_changes;

    IF changes IS NOT NULL THEN
        PERFORM public.record_resolver_profile_input_changes(changes);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER raw_code_hashes_resolver_profile_input_insert_trigger
AFTER INSERT ON public.raw_code_hashes
REFERENCING NEW TABLE AS inserted_raw_code_hashes
FOR EACH STATEMENT
EXECUTE FUNCTION public.queue_resolver_profile_input_changes_after_raw_code_insert();

CREATE TRIGGER raw_code_hashes_resolver_profile_input_update_trigger
AFTER UPDATE ON public.raw_code_hashes
REFERENCING OLD TABLE AS old_raw_code_hashes NEW TABLE AS new_raw_code_hashes
FOR EACH STATEMENT
EXECUTE FUNCTION public.queue_resolver_profile_input_changes_after_raw_code_update();

-- A generation-zero corpus has never crossed a destructive retention
-- boundary, so existing resolver-local facts can be repaired from its retained
-- history. Upgraded databases are deliberately seeded into generation one by
-- the raw-log retention migration because their historical completeness is
-- unknown; do not manufacture absence-aware work that such a corpus cannot
-- authorize. The authority journal baselines current authority on first use
-- and queues only later changes for those upgraded databases.
WITH profile_targets AS (
    SELECT DISTINCT
        mv.chain AS chain_id,
        lower(cia.address) AS contract_address
    FROM manifest_versions mv
    JOIN manifest_contract_instances mci
      ON mci.manifest_id = mv.manifest_id
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = mci.contract_instance_id
     AND cia.chain_id = mv.chain
    WHERE mv.source_family IN (
          'ens_v1_resolver_l1',
          'basenames_base_resolver'
      )
      AND mci.declaration_kind = 'contract'

    UNION

    SELECT DISTINCT
        edge.chain_id,
        lower(cia.address) AS contract_address
    FROM discovery_edges edge
    JOIN manifest_versions source_manifest
      ON source_manifest.manifest_id = edge.source_manifest_id
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = edge.to_contract_instance_id
     AND cia.chain_id = edge.chain_id
    WHERE edge.edge_kind = 'resolver'
      AND source_manifest.source_family IN (
          'ens_v1_registry_l1',
          'basenames_base_registry'
      )
),
seeded AS (
    SELECT
        target.chain_id,
        target.contract_address,
        latest.code_hash
    FROM profile_targets target
    JOIN raw_log_staging_input_revisions retention
      ON retention.chain_id = target.chain_id
     AND retention.retention_generation = 0
    LEFT JOIN LATERAL (
        SELECT lower(code_hash.code_hash) AS code_hash
        FROM raw_code_hashes code_hash
        WHERE code_hash.chain_id = target.chain_id
          AND code_hash.contract_address = target.contract_address
          AND code_hash.canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY
            code_hash.block_number DESC,
            CASE code_hash.canonicality_state
                WHEN 'finalized'::canonicality_state THEN 4
                WHEN 'safe'::canonicality_state THEN 3
                WHEN 'canonical'::canonicality_state THEN 2
                WHEN 'observed'::canonicality_state THEN 1
                ELSE 0
            END DESC,
            code_hash.raw_code_hash_id DESC
        LIMIT 1
    ) latest ON TRUE
)
INSERT INTO public.resolver_profile_input_changes (
    chain_id,
    contract_address,
    generation,
    processed_generation,
    previous_code_hash,
    current_code_hash,
    force_reconciliation
)
SELECT
    chain_id,
    contract_address,
    1,
    0,
    code_hash,
    code_hash,
    TRUE
FROM seeded
ON CONFLICT (chain_id, contract_address) DO NOTHING;
