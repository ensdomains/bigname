INSERT INTO tyr228_publication_http_20261005_r1.tyr228_documents
SELECT row_number() OVER (ORDER BY logical_name_id),logical_name_id,name,namespace,namehash,spelling_class FROM (
 SELECT logical_name_id,raw_name AS name,namespace,namehash,0::smallint AS spelling_class FROM bigname_phase.name_surfaces WHERE raw_name<>'' AND octet_length(raw_name)<=2000
 UNION ALL
 SELECT logical_name_id,name,namespace,namehash,(CASE WHEN octet_length(name)<=2000 THEN 1 ELSE 2 END)::smallint FROM tyr228_publication_http_20261005_r1.tyr228_spelling_probe) all_text;
ANALYZE tyr228_publication_http_20261005_r1.tyr228_documents;
