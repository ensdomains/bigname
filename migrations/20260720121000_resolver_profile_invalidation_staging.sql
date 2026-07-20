CREATE TABLE public.resolver_profile_reconciliation_invalidation_keys (
    chain_id TEXT NOT NULL,
    projection TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    key_payload JSONB NOT NULL,
    PRIMARY KEY (chain_id, projection, projection_key),
    CONSTRAINT resolver_profile_invalidation_chain_check CHECK (chain_id <> ''),
    CONSTRAINT resolver_profile_invalidation_projection_check CHECK (
        projection IN ('resolver_current', 'record_inventory_current')
    ),
    CONSTRAINT resolver_profile_invalidation_payload_check CHECK (
        jsonb_typeof(key_payload) = 'object'
    )
);

COMMENT ON TABLE public.resolver_profile_reconciliation_invalidation_keys IS
    'Indexer-owned crash-safe projection invalidation keys streamed before one chain-context resolver-profile replay and published in bounded pages only after that replay is durable.';
