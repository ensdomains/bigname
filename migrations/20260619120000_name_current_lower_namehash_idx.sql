-- Back the `domain(id:)` namehash fallback (and any direct namehash lookup) with a functional
-- index on lower(namehash), so it does not sequential-scan name_current. The primary `domain(id:)`
-- path resolves by normalized_name (already covered by the (namespace, normalized_name) index);
-- this index covers the namehash-on-miss branch in load_name_current_list_row_by_namehash, whose
-- predicate is `LOWER(namehash) = LOWER($1)` against name_current.namehash.
CREATE INDEX IF NOT EXISTS name_current_lower_namehash_idx
    ON public.name_current (lower(namehash));
