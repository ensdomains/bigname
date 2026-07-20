CREATE TABLE public.resolver_profile_authority_journal_entries (
    journal_key TEXT NOT NULL,
    entry_key TEXT COLLATE "C" NOT NULL,
    entry_payload JSONB NOT NULL,
    PRIMARY KEY (journal_key, entry_key),
    CONSTRAINT resolver_profile_authority_journal_entries_journal_fk
        FOREIGN KEY (journal_key)
        REFERENCES public.resolver_profile_authority_journal (journal_key)
        ON DELETE CASCADE,
    CONSTRAINT resolver_profile_authority_journal_entry_payload_check CHECK (
        jsonb_typeof(entry_payload) = 'object'
    )
);

CREATE INDEX resolver_profile_authority_journal_entries_family_idx
    ON public.resolver_profile_authority_journal_entries (
        journal_key,
        (entry_payload ->> 'chain'),
        (entry_payload ->> 'source_family')
    );

INSERT INTO public.resolver_profile_authority_journal_entries (
    journal_key,
    entry_key,
    entry_payload
)
SELECT
    journal.journal_key,
    jsonb_build_array(
        entry -> 'chain',
        entry -> 'source_family',
        entry -> 'address',
        entry -> 'contract_instance_id',
        entry -> 'source',
        entry -> 'source_manifest_id',
        entry -> 'active_from_block_number',
        entry -> 'active_to_block_number'
    )::TEXT,
    entry
FROM public.resolver_profile_authority_journal journal
CROSS JOIN LATERAL jsonb_array_elements(
    journal.authority_snapshot -> 'entries'
) AS snapshot(entry);

ALTER TABLE public.resolver_profile_authority_journal
    DROP CONSTRAINT resolver_profile_authority_journal_snapshot_check;

ALTER TABLE public.resolver_profile_authority_journal
    DROP COLUMN authority_snapshot;
