SELECT jsonb_build_object(
 'source_rows',(SELECT count(*) FROM (SELECT surface.logical_name_id, rendered.name, surface.namespace, surface.namehash
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
WHERE surface.raw_name IS NULL AND hash_array_extended(surface.raw_labels, 0) IS NULL) source),
 'spelling_rows',(SELECT count(*) FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe),
 'missing_or_extra',(SELECT count(*) FROM (
    (SELECT * FROM (SELECT surface.logical_name_id, rendered.name, surface.namespace, surface.namehash
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
WHERE surface.raw_name IS NULL AND hash_array_extended(surface.raw_labels, 0) IS NULL) source EXCEPT SELECT * FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe)
    UNION ALL
    (SELECT * FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe EXCEPT SELECT * FROM (SELECT surface.logical_name_id, rendered.name, surface.namespace, surface.namehash
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
WHERE surface.raw_name IS NULL AND hash_array_extended(surface.raw_labels, 0) IS NULL) source)
 ) delta),
 'long_rows',(SELECT count(*) FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe WHERE octet_length(name)>2000),
 'max_spelling_bytes',(SELECT max(octet_length(name)) FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe),
 'text_bytes',(SELECT sum(octet_length(name)) FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe));
