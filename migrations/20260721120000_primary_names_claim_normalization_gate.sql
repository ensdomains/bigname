ALTER TABLE public.primary_names_current
    ADD COLUMN claim_name_is_normalized boolean DEFAULT false NOT NULL;

ALTER TABLE public.primary_names_current
    ADD CONSTRAINT primary_names_current_claim_name_is_normalized_check
    CHECK (
        NOT claim_name_is_normalized
        OR (
            claim_status = 'success'
            AND normalized_claim_name IS NOT NULL
        )
    );
