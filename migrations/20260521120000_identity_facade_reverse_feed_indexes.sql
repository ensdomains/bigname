-- no-transaction

-- Reverse identity feed rendering usually asks for the first name only
-- (`page_size=1`, no cursor) for many addresses at once. Keep a role-ranked
-- address/name order available without sorting every high-cardinality address.
CREATE INDEX IF NOT EXISTS address_names_current_identity_feed_sort_idx
    ON public.address_names_current (
        address,
        (
            CASE
                WHEN relation IN ('registrant', 'token_holder') THEN 0
                ELSE 1
            END
        ),
        normalized_name,
        namespace,
        namehash,
        logical_name_id
    )
    INCLUDE (
        relation,
        resource_id,
        surface_binding_id,
        token_lineage_id
    );

-- Primary-name promotion starts from `primary_names_current` and then checks
-- that the claimed surface is a readable address-name row for the requested
-- address. This index makes that join name-led instead of scanning all names
-- for high-cardinality addresses.
CREATE INDEX IF NOT EXISTS address_names_current_identity_claim_name_idx
    ON public.address_names_current (
        address,
        namespace,
        normalized_name,
        relation
    )
    INCLUDE (
        logical_name_id,
        namehash,
        resource_id,
        surface_binding_id,
        token_lineage_id
    );
