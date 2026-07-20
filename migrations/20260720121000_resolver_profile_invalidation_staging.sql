CREATE TABLE public.resolver_profile_reconciliation_invalidation_keys (
    run_id UUID NOT NULL,
    projection TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    key_payload JSONB NOT NULL,
    PRIMARY KEY (run_id, projection, projection_key),
    CONSTRAINT resolver_profile_invalidation_run_fk
        FOREIGN KEY (run_id)
        REFERENCES public.resolver_profile_reconciliation_runs (run_id)
        ON DELETE CASCADE,
    CONSTRAINT resolver_profile_invalidation_projection_check CHECK (
        projection IN ('resolver_current', 'record_inventory_current')
    ),
    CONSTRAINT resolver_profile_invalidation_payload_check CHECK (
        jsonb_typeof(key_payload) = 'object'
    )
);

COMMENT ON TABLE public.resolver_profile_reconciliation_invalidation_keys IS
    'Indexer-owned projection invalidation keys captured in bounded target pages before one chain-context resolver-profile replay and published only after that replay is durable.';
