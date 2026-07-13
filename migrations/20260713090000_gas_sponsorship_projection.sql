-- Gas-sponsorship accounting read model (docs/architecture.md § Normalized
-- event taxonomy — Gas sponsorship; docs/api-v1-routes.md
-- § GET /v1/gas-sponsorship/{namespace}/{name}), plus the durable
-- transaction-input fact family for requires_transaction_input source
-- families (docs/storage.md § Raw-log retention modes).

CREATE TABLE public.gas_sponsorship_current (
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    normalized_name text NOT NULL,
    namehash text NOT NULL,
    lease_start_at timestamp with time zone,
    registered_seconds_total bigint DEFAULT 0 NOT NULL,
    earned_updates bigint DEFAULT 0 NOT NULL,
    spent_updates bigint DEFAULT 0 NOT NULL,
    last_sponsored_write_at timestamp with time zone,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT gas_sponsorship_current_pkey PRIMARY KEY (logical_name_id),
    CONSTRAINT gas_sponsorship_current_check CHECK (
        logical_name_id = ((namespace || ':'::text) || normalized_name)
    ),
    CONSTRAINT gas_sponsorship_current_counts_check CHECK (
        registered_seconds_total >= 0
        AND earned_updates >= 0
        AND spent_updates >= 0
    )
);

CREATE INDEX gas_sponsorship_current_lower_namehash_idx
    ON public.gas_sponsorship_current (lower(namehash));

CREATE TABLE public.gas_sponsorship_global_current (
    namespace text NOT NULL,
    sponsored_op_count bigint DEFAULT 0 NOT NULL,
    attributed_op_count bigint DEFAULT 0 NOT NULL,
    failed_op_count bigint DEFAULT 0 NOT NULL,
    gas_wei_total numeric(78,0) DEFAULT 0 NOT NULL,
    failed_gas_wei_total numeric(78,0) DEFAULT 0 NOT NULL,
    usd_e8_total numeric(78,0) DEFAULT 0 NOT NULL,
    unpriced_wei_total numeric(78,0) DEFAULT 0 NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonicality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT gas_sponsorship_global_current_pkey PRIMARY KEY (namespace),
    CONSTRAINT gas_sponsorship_global_current_counts_check CHECK (
        sponsored_op_count >= 0
        AND attributed_op_count >= 0
        AND failed_op_count >= 0
        AND attributed_op_count <= sponsored_op_count
        AND failed_op_count <= sponsored_op_count
        AND gas_wei_total >= 0
        AND failed_gas_wei_total >= 0
        AND usd_e8_total >= 0
        AND unpriced_wei_total >= 0
    )
);

-- Durable input calldata for transactions carrying a matched log of a
-- requires_transaction_input source family. Replay-required raw facts, not
-- compactable staging; the authorized adapter input for calldata-derived
-- normalized events.
CREATE TABLE public.raw_transaction_inputs (
    chain_id text NOT NULL,
    block_hash text NOT NULL,
    block_number bigint NOT NULL,
    transaction_hash text NOT NULL,
    input bytea NOT NULL,
    canonicality_state public.canonicality_state DEFAULT 'observed'::public.canonicality_state NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT raw_transaction_inputs_pkey PRIMARY KEY (chain_id, block_hash, transaction_hash),
    CONSTRAINT raw_transaction_inputs_block_number_check CHECK (block_number >= 0)
);
