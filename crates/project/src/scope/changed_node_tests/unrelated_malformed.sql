
SET LOCAL statement_timeout='12s';
SET LOCAL jit=off;
CREATE TEMP TABLE project_changed_events (chain_id text,namespace text,source_manifest_id bigint,source_family text,event_kind text,logical_name_id text,after_state jsonb,raw_fact_ref jsonb);
CREATE TEMP TABLE name_surfaces (chain_id text,namehash text,logical_name_id text,canonicality_state text);
CREATE INDEX ON name_surfaces (chain_id,namehash) WHERE canonicality_state IN ('canonical','safe','finalized');
CREATE TEMP TABLE normalized_events (normalized_event_id bigint PRIMARY KEY,chain_id text,logical_name_id text,resource_id uuid,event_kind text,source_family text,namespace text,canonicality_state text,after_state jsonb);
CREATE INDEX ON normalized_events(chain_id,logical_name_id) WHERE canonicality_state IN ('canonical','safe','finalized');
CREATE INDEX ON normalized_events(resource_id) WHERE canonicality_state IN ('canonical','safe','finalized');
CREATE TEMP TABLE record_inventory_current(resource_id uuid PRIMARY KEY,provenance jsonb,support_status text);
CREATE TEMP TABLE resolver_current(chain_id text,resolver_address text,support_status text,declared_summary jsonb,provenance jsonb);
CREATE TEMP TABLE project_declared_resolver_addresses(manifest_id bigint,namespace text,resolver_address text);
INSERT INTO resolver_current VALUES ('bench','0xab','supported','{"classification":{"source_family":"ens_v1_resolver_l1","basis":"manifest_declared_address"}}','{"manifest_id":1}');
INSERT INTO project_declared_resolver_addresses VALUES (1,'ens','0xab');

INSERT INTO name_surfaces SELECT 'bench','node-'||n,'name-'||n,'canonical' FROM generate_series(1,10) n;
INSERT INTO normalized_events SELECT (n-1)*4+h,'bench','name-'||n,md5(n::text)::uuid,'ResolverChanged','ens_v2_registry_l1','ens','canonical','{"resolver":"0xAB"}' FROM generate_series(1,10) n CROSS JOIN generate_series(1,4) h;
INSERT INTO record_inventory_current SELECT md5(n::text)::uuid,jsonb_build_object('chain_id','bench','resolver_pointer_event_id',(n-1)*4+1),'supported' FROM generate_series(1,10) n;
INSERT INTO project_changed_events SELECT 'bench','ens',1,'ens_v1_resolver_l1','RecordChanged',null,jsonb_build_object('node','node-'||n),'{"emitting_address":"0xab"}' FROM generate_series(1,1) n CROSS JOIN generate_series(1,1) r;
ANALYZE name_surfaces; ANALYZE normalized_events; ANALYZE record_inventory_current; ANALYZE resolver_current; ANALYZE project_declared_resolver_addresses;
CREATE TEMP TABLE project_scope_names(logical_name_id text PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE project_scope_resources(resource_id uuid PRIMARY KEY) ON COMMIT DROP;
UPDATE record_inventory_current SET provenance=jsonb_set(provenance,'{resolver_pointer_event_id}','"not-a-bigint"') WHERE resource_id=md5('10')::uuid;
