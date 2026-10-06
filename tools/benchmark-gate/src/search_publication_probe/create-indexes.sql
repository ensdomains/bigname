ALTER TABLE tyr228_publication_http_20261005_r1.tyr228_spelling_probe ADD PRIMARY KEY (logical_name_id);
CREATE INDEX tyr228_spelling_probe_order_idx
    ON tyr228_publication_http_20261005_r1.tyr228_spelling_probe (name,namespace,namehash,logical_name_id)
    WHERE octet_length(name) <= 2000;
ANALYZE tyr228_publication_http_20261005_r1.tyr228_spelling_probe;
