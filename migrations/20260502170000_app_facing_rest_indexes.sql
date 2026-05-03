-- no-transaction

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

-- Keep this migration intentionally lightweight. App-facing routes must work
-- without these indexes; the mandatory migration only adds narrow lookup
-- helpers that fit large local replay databases without a large transient
-- disk requirement.

CREATE INDEX IF NOT EXISTS name_current_app_namespace_name_idx
    ON public.name_current (
        namespace,
        normalized_name
    );

CREATE INDEX IF NOT EXISTS name_current_app_global_name_idx
    ON public.name_current (
        normalized_name,
        namespace
    );

CREATE INDEX IF NOT EXISTS address_names_current_app_relation_filter_idx
    ON public.address_names_current (
        address,
        relation,
        namespace,
        logical_name_id
    );

CREATE INDEX IF NOT EXISTS permissions_current_app_subject_resource_idx
    ON public.permissions_current (
        subject,
        resource_id,
        scope
    );

CREATE INDEX IF NOT EXISTS permissions_current_app_resolver_scope_idx
    ON public.permissions_current (
        scope
    )
    WHERE scope_kind = 'resolver';
