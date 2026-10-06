INSERT INTO tyr228_publication_http_20261005_r1.tyr228_postings
SELECT document.namespace,document.spelling_class,token.kind,token.n,convert_to(token.fragment,'UTF8'),document.search_id
FROM tyr228_publication_http_20261005_r1.tyr228_documents document
CROSS JOIN LATERAL (
 SELECT DISTINCT 1::smallint AS kind,n::smallint,substring(document.name FROM position FOR n) AS fragment
 FROM generate_series(1,3) n CROSS JOIN LATERAL generate_series(1,char_length(document.name)-n+1) position
 UNION ALL
 SELECT 2::smallint,n::smallint,substring(document.name FROM 1 FOR n) FROM generate_series(1,least(3,char_length(document.name))) n
) token;
