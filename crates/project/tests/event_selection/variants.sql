
INSERT INTO project_declared_resolver_addresses VALUES
('ens','0xmirror','ens_v2_resolver_l1','ensv1_mirror_resolver',2),
('ens','0xshared','ens_v1_resolver_l1','public_resolver',3),
('basenames','0xshared','basenames_base_resolver','public_resolver',4);
CREATE TEMP TABLE variants AS
SELECT row_number() OVER () AS variant, family, visibility, canonicality, chain, height
FROM (VALUES ('ens_v1_resolver_l1'), ('ens_v2_resolver_l1'), ('basenames_base_resolver'),
             ('ens_v1_registry_l1'), ('ens_v1_registrar_l1'), ('ens_v1_wrapper_l1'), ('unsupported')) f(family)
CROSS JOIN (VALUES ('activated'),('candidate')) v(visibility)
CROSS JOIN (VALUES ('canonical'),('safe'),('finalized'),('orphaned')) c(canonicality)
CROSS JOIN (VALUES ('bench'),('other')) ch(chain)
CROSS JOIN (VALUES (9),(10),(11)) h(height);
INSERT INTO normalized_events
SELECT 300000+variant,'variant-'||variant,chain,CASE WHEN family LIKE 'basenames%' THEN 'basenames' ELSE 'ens' END,
NULL,NULL,CASE WHEN family IN ('ens_v1_registry_l1','ens_v1_registrar_l1','ens_v1_wrapper_l1') THEN 'ResolverChanged' ELSE 'RecordChanged' END,
family,CASE WHEN variant%3=0 THEN 99 ELSE 1 END,1,height,
CASE WHEN variant%5=0 THEN 'missing' ELSE 'block' END,0,0,canonicality,visibility,
jsonb_build_object('node',CASE WHEN variant%7=0 THEN NULL ELSE 'NODE-1' END,
'resolver',CASE WHEN variant%11=0 THEN '' ELSE '0xSHARED' END),'{}',jsonb_build_object('emitting_address','0xShared')
FROM variants;
-- Same lookup through different resources, pointer source families, namespaces and resolver histories.
INSERT INTO normalized_events
SELECT 400000+i,'extra-pointer-'||i,'bench',CASE WHEN i=6 THEN 'basenames' ELSE 'ens' END,
md5('1')::uuid,'ens:node-1','ResolverChanged',family,1,1,10,'block',0,0,'canonical','activated',
jsonb_build_object('resolver',resolver),'{}','{}'
FROM (VALUES (1,'ens_v1_registry_l1','0xshared'),(2,'ens_v1_registrar_l1','0xshared'),
(3,'ens_v1_wrapper_l1','0xshared'),(4,'ens_v2_registry_l1','0xmirror'),(5,'ens_v2_root_l1','0xshared'),
(6,'basenames_base_registry','0xshared'),(7,'ens_v2_root_l1',''),(8,'ens_v2_root_l1','0x0000000000000000000000000000000000000000'),
(9,'ens_v2_registry_l1',NULL),(10,'ens_v2_registry_l1','0xundeclared')) p(i,family,resolver);
INSERT INTO normalized_events
SELECT 500000+i,'reverse-history-'||i,'bench','ens',NULL,NULL,'ReverseChanged','reverse',1,1,10,'block',0,0,'canonical',
CASE WHEN i%2=0 THEN 'candidate' ELSE 'activated' END,
jsonb_build_object('address','0xOwner','coin_type','60','namespace','ens','reverse_node','NODE-1'),'{}','{}'
FROM generate_series(1,100)i;
INSERT INTO project_changed_events SELECT * FROM normalized_events;
INSERT INTO project_scope_resolver_candidate_events VALUES(300001,NULL);
INSERT INTO project_scope_ancestors VALUES('ens:node-2');
INSERT INTO children_current VALUES('ens:node-1','ens:node-2','{"chain_id":"bench"}');
ANALYZE normalized_events;

INSERT INTO project_scope_account_permissions VALUES('bench','resolver','0xshared','0xowner','0xsubject','operator');
INSERT INTO normalized_events
SELECT 600000+i,'scope-arm-'||i,'bench','ens',NULL,NULL,kind,'ens_v1_registry_l1',1,1,
CASE WHEN i=1 THEN NULL ELSE 10 END,CASE WHEN i=1 THEN NULL ELSE 'block' END,0,0,'canonical','candidate',after_value::jsonb,before_value::jsonb,'{}'
FROM (VALUES
(1,'SourceManifestUpdated','{}','{}'),
(2,'AliasChanged','{"to_resource_id":"c4ca4238-a0b9-2382-0dcc-509a6f75849b","resolver":"0xshared"}','{}'),
(3,'AliasChanged','{}','{"to_logical_name_id":"ens:node-1","resolver":"0xshared"}'),
(4,'SubregistryChanged','{"node":"NODE-1"}','{}'),
(5,'AuthorityTransferred','{"child_node":"NODE-1"}','{}'),
(6,'Upgraded','{"proxy_address":"0xSHARED"}','{}'),
(7,'AccountPermissionChanged','{"scope":{"authority_kind":"resolver","authority_contract":"0xShared","owner":"0xOwner"},"subject":"0xSubject","relation_kind":"operator"}','{}'),
(8,'PermissionChanged','{"scope":{"kind":"resolver","resolver_address":"0xshared"}}','{}'),
(9,'RecordVersionChanged','{"node":"NODE-1"}','{}'),
(10,'RecordChanged','{"node":"NODE-1","source_event":"NameChanged"}','{}')
) v(i,kind,after_value,before_value);
INSERT INTO project_changed_events SELECT * FROM normalized_events WHERE normalized_event_id>=600000;
