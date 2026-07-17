CREATE TABLE public.resolver_profile_authority_journal (
    journal_key TEXT PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0,
    authority_snapshot JSONB NOT NULL DEFAULT '{"entries": []}'::JSONB,
    discovery_epoch_snapshot JSONB NOT NULL DEFAULT '{}'::JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT resolver_profile_authority_journal_singleton_check CHECK (
        journal_key = 'active_resolver_profiles'
    ),
    CONSTRAINT resolver_profile_authority_journal_revision_check CHECK (
        revision >= 0
    ),
    CONSTRAINT resolver_profile_authority_journal_snapshot_check CHECK (
        jsonb_typeof(authority_snapshot) = 'object'
        AND jsonb_typeof(authority_snapshot -> 'entries') = 'array'
    ),
    CONSTRAINT resolver_profile_authority_journal_epoch_snapshot_check CHECK (
        jsonb_typeof(discovery_epoch_snapshot) = 'object'
    )
);

INSERT INTO public.resolver_profile_authority_journal (
    journal_key,
    revision,
    authority_snapshot,
    discovery_epoch_snapshot
) VALUES (
    'active_resolver_profiles',
    0,
    '{"entries": []}'::JSONB,
    '{}'::JSONB
);
