CREATE TABLE tyr228_publication_http_20261005_r1.search_fields (
    logical_name_id text PRIMARY KEY,
    chain_id text NOT NULL,
    search_supported boolean NOT NULL,
    owner text,
    public_authority text,
    search_fields jsonb,
    search_creation_transport_resource_id uuid,
    display_name_override text,
    CHECK (search_supported = (search_fields IS NOT NULL)),
    CHECK (search_fields IS NULL OR (jsonb_typeof(search_fields)='object' AND search_fields ? 'registration_status'))
);
CREATE TABLE tyr228_publication_http_20261005_r1.generation (
    chain_id text NOT NULL,
    namespace text NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    input_hash text NOT NULL,
    PRIMARY KEY(chain_id,namespace)
);
