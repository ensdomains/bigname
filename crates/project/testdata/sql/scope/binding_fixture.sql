CREATE TEMP TABLE chain_lineage(chain_id text, block_hash text, block_number bigint,
    canonicality_state text, block_timestamp timestamptz);
INSERT INTO chain_lineage VALUES ('bench','canonical',10,'canonical','2026-09-20'),
    ('bench','orphan',9,'orphaned','2026-09-19'), ('other','canonical',10,'canonical','2026-09-20');
CREATE TEMP TABLE name_surfaces(logical_name_id text PRIMARY KEY, namehash text);
CREATE TEMP TABLE resources(resource_id uuid PRIMARY KEY, chain_id text);
CREATE TEMP TABLE surface_bindings(surface_binding_id uuid, resource_id uuid,
    logical_name_id text, chain_id text, block_hash text, block_number bigint,
    canonicality_state text, authority_arm text, active_from timestamptz,
    active_to timestamptz, provenance jsonb);
CREATE TEMP TABLE normalized_events(normalized_event_id bigint PRIMARY KEY,
    resource_id uuid, logical_name_id text, namespace text, chain_id text,
    block_hash text, block_number bigint, source_family text, event_kind text,
    consumer_visibility text, canonicality_state text, after_state jsonb);
CREATE INDEX ON surface_bindings(logical_name_id);
CREATE INDEX ON surface_bindings(resource_id);
CREATE INDEX ON normalized_events(resource_id);
CREATE INDEX ON normalized_events(logical_name_id);
CREATE TEMP TABLE project_scope_names(logical_name_id text PRIMARY KEY);
CREATE TEMP TABLE project_scope_resources(resource_id uuid PRIMARY KEY);
CREATE TEMP TABLE project_binding_seen_names(operator text, logical_name_id text,
    PRIMARY KEY(operator, logical_name_id));
CREATE TEMP TABLE project_binding_seen_resources(operator text, resource_id uuid,
    PRIMARY KEY(operator, resource_id));
INSERT INTO name_surfaces SELECT 'ens:node-'||i, 'node-'||i FROM generate_series(1,20)i;
INSERT INTO resources
SELECT md5(kind||i)::uuid,'bench' FROM generate_series(1,20)i
CROSS JOIN unnest(ARRAY['lease-','registry-','wrapper-']) kind;
-- Closed lease bindings intentionally survive through the unnamed-history operators.
INSERT INTO surface_bindings
SELECT md5('binding-'||kind||i)::uuid,md5(kind||i)::uuid,'ens:node-'||i,
    'bench','canonical',10,'canonical','ens_v1','2026-09-01',
    CASE WHEN kind='lease-' THEN '2026-09-02'::timestamptz END,'{}'
FROM generate_series(1,20)i CROSS JOIN unnest(ARRAY['lease-','registry-','wrapper-'])kind;
INSERT INTO normalized_events
SELECT i,md5('lease-'||i)::uuid,NULL,'ens','bench','canonical',10,
    'ens_v1_registrar_l1','RegistrationGranted','activated','canonical',
    jsonb_build_object('namehash','NODE-'||i)
FROM generate_series(1,20)i;
INSERT INTO normalized_events
SELECT 100+i,md5('wrapper-'||i)::uuid,'ens:node-'||i,'ens','bench','canonical',10,
    'ens_v1_wrapper_l1','SurfaceBound','activated','canonical',
    jsonb_build_object('wrapped_registrar_resource_id',md5('lease-'||i)::uuid)
FROM generate_series(1,20)i;
INSERT INTO normalized_events
SELECT 200+i,md5('registry-'||i)::uuid,'ens:node-'||i,'ens','bench','canonical',10,
    'ens_v1_registry_l1','ResolverChanged','activated','canonical','{}'
FROM generate_series(1,20)i;
-- Shared historical resource and a wrapper edge create cycles and cross-name closure.
INSERT INTO surface_bindings
SELECT md5('cross-'||i)::uuid,md5('lease-'||(i+1))::uuid,'ens:node-'||i,
    'bench','canonical',10,'canonical','ens_v1','2026-09-01',NULL,'{}'
FROM generate_series(1,19)i;
-- Future, nonactivated, orphaned and foreign-chain facts must not add their resources.
INSERT INTO resources SELECT md5('excluded-'||i)::uuid,'bench' FROM generate_series(1,4)i;
INSERT INTO normalized_events
SELECT 300+i,md5('wrapper-1')::uuid,'ens:node-1','ens',
    CASE WHEN i=4 THEN 'other' ELSE 'bench' END,
    CASE WHEN i=3 THEN 'orphan' ELSE 'canonical' END,
    CASE WHEN i=1 THEN 11 WHEN i=3 THEN 9 ELSE 10 END,
    'ens_v1_wrapper_l1','SurfaceBound',CASE WHEN i=2 THEN 'latent' ELSE 'activated' END,
    'canonical',jsonb_build_object('wrapped_registrar_resource_id',md5('excluded-'||i)::uuid)
FROM generate_series(1,4)i;
ANALYZE normalized_events;
ANALYZE surface_bindings;

CREATE TEMP TABLE project_binding_frontier_names(logical_name_id text PRIMARY KEY);
CREATE TEMP TABLE project_binding_frontier_resources(resource_id uuid PRIMARY KEY);
