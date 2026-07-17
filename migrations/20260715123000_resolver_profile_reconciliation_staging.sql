CREATE TABLE public.resolver_profile_reconciliation_runs (
    run_id UUID PRIMARY KEY,
    chain_id TEXT NOT NULL UNIQUE,
    first_block_number BIGINT NOT NULL,
    last_block_number BIGINT NOT NULL,
    resolver_address_count BIGINT NOT NULL,
    resolver_address_set_digest TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT resolver_profile_reconciliation_runs_range_check CHECK (
        first_block_number >= 0
        AND last_block_number >= first_block_number
    ),
    CONSTRAINT resolver_profile_reconciliation_runs_address_count_check CHECK (
        resolver_address_count > 0
    ),
    CONSTRAINT resolver_profile_reconciliation_runs_address_digest_check CHECK (
        resolver_address_set_digest <> ''
    ),
    CONSTRAINT resolver_profile_reconciliation_runs_status_check CHECK (
        status IN ('running', 'replay_complete')
    )
);

CREATE TABLE public.resolver_profile_reconciliation_targets (
    run_id UUID NOT NULL,
    resolver_address TEXT NOT NULL,
    CONSTRAINT resolver_profile_reconciliation_targets_pkey PRIMARY KEY (
        run_id,
        resolver_address
    ),
    CONSTRAINT resolver_profile_reconciliation_targets_run_fkey FOREIGN KEY (
        run_id
    ) REFERENCES public.resolver_profile_reconciliation_runs (run_id) ON DELETE CASCADE,
    CONSTRAINT resolver_profile_reconciliation_targets_address_check CHECK (
        resolver_address <> '' AND resolver_address = lower(resolver_address)
    )
);

CREATE TABLE public.resolver_profile_reconciliation_state_items (
    run_id UUID NOT NULL,
    item_kind TEXT NOT NULL,
    item_key TEXT NOT NULL,
    item_payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT resolver_profile_reconciliation_state_items_pkey PRIMARY KEY (
        run_id,
        item_kind,
        item_key
    ),
    CONSTRAINT resolver_profile_reconciliation_state_items_run_fkey FOREIGN KEY (
        run_id
    ) REFERENCES public.resolver_profile_reconciliation_runs (run_id) ON DELETE CASCADE,
    CONSTRAINT resolver_profile_reconciliation_state_items_kind_check CHECK (item_kind <> ''),
    CONSTRAINT resolver_profile_reconciliation_state_items_key_check CHECK (item_key <> '')
);

COMMENT ON TABLE public.resolver_profile_reconciliation_runs IS
    'Transient adapter-owned run markers for absence-aware resolver-profile replay. A replay_complete row authorizes only its matching final anti-join transaction; it is never a normalized replay cursor or completion checkpoint.';

COMMENT ON TABLE public.resolver_profile_reconciliation_targets IS
    'The exact normalized resolver-emitter address set for one transient profile reconciliation run. Streaming and absence repair join this table instead of carrying the full seed fanout in replay cursors or page-local memory.';

COMMENT ON TABLE public.resolver_profile_reconciliation_state_items IS
    'Page-evicted private ENSv1/Basenames authority state and staged resolver events for one resolver-profile reconciliation run. It is operational scratch state, not manifest authority, normalized replay authority, or an API-visible snapshot.';
