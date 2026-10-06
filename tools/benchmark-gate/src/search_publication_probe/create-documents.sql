CREATE TABLE tyr228_publication_http_20261005_r1.tyr228_documents AS
SELECT 0::bigint AS search_id,s.logical_name_id,s.raw_name AS name,s.namespace,s.namehash,0::smallint AS spelling_class FROM bigname_phase.name_surfaces s WITH NO DATA;
ALTER TABLE tyr228_publication_http_20261005_r1.tyr228_documents ADD PRIMARY KEY(search_id),ADD UNIQUE(logical_name_id);
CREATE TABLE tyr228_publication_http_20261005_r1.tyr228_postings(namespace text NOT NULL,spelling_class smallint NOT NULL,token_kind smallint NOT NULL,token_length smallint NOT NULL,token_bytes bytea NOT NULL,search_id bigint NOT NULL);
