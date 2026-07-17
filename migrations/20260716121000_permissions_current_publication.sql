CREATE TABLE public.permissions_current_publication (
    projection TEXT PRIMARY KEY,
    publication_version INTEGER NOT NULL,
    data_revision BIGINT NOT NULL DEFAULT 1,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT permissions_current_publication_projection_check
        CHECK (projection = 'permissions_current'),
    CONSTRAINT permissions_current_publication_version_check
        CHECK (publication_version > 0),
    CONSTRAINT permissions_current_publication_data_revision_check
        CHECK (data_revision > 0)
);
