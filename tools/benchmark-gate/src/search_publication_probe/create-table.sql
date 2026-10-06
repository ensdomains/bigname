CREATE TABLE tyr228_publication_http_20261005_r1.tyr228_spelling_probe AS
SELECT surface.logical_name_id, rendered.name, surface.namespace, surface.namehash
FROM bigname_phase.name_surfaces surface
CROSS JOIN LATERAL (SELECT COALESCE(surface.raw_name, (
            SELECT string_agg(
                       CASE WHEN preimage.decoded_label IS NOT NULL
                                 AND preimage.normalized_under_version
                            THEN preimage.decoded_label
                            ELSE '[' || substring(lower(path.labelhash) FROM 3) || ']' END,
                       '.' ORDER BY path.position)
            FROM unnest(surface.labelhashes) WITH ORDINALITY AS path(labelhash, position)
            LEFT JOIN bigname_phase.label_preimages preimage
              ON preimage.labelhash = lower(path.labelhash))) AS name OFFSET 0) rendered
WHERE surface.raw_name IS NULL AND hash_array_extended(surface.raw_labels, 0) IS NULL WITH NO DATA;
ALTER TABLE tyr228_publication_http_20261005_r1.tyr228_spelling_probe ALTER COLUMN logical_name_id SET NOT NULL,
    ALTER COLUMN name SET NOT NULL, ALTER COLUMN namespace SET NOT NULL,
    ALTER COLUMN namehash SET NOT NULL;
