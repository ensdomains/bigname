CREATE TEMP TABLE normalized_events(normalized_event_id bigint PRIMARY KEY,event_identity text,chain_id text,namespace text,resource_id uuid,logical_name_id text,event_kind text,source_family text,source_manifest_id bigint,manifest_version bigint,block_number bigint,block_hash text,transaction_index int,log_index int,canonicality_state text,consumer_visibility text,after_state jsonb,before_state jsonb,raw_fact_ref jsonb);
CREATE TEMP TABLE name_surfaces(logical_name_id text PRIMARY KEY,namespace text,namehash text,chain_id text,block_number bigint,block_hash text,canonicality_state text);
CREATE TEMP TABLE chain_lineage(chain_id text,block_number bigint,block_hash text,canonicality_state text,PRIMARY KEY(chain_id,block_hash));
CREATE TEMP TABLE project_scope_names(logical_name_id text PRIMARY KEY);
CREATE TEMP TABLE project_scope_children(logical_name_id text PRIMARY KEY);
CREATE TEMP TABLE project_scope_resources(resource_id uuid PRIMARY KEY);
CREATE TEMP TABLE project_declared_resolver_addresses(namespace text,resolver_address text,source_family text,classification_role text,manifest_id bigint);
CREATE INDEX IF NOT EXISTS normalized_events_name_history_idx
    ON normalized_events (
        logical_name_id,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE logical_name_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX IF NOT EXISTS normalized_events_resource_history_idx
    ON normalized_events (
        resource_id,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE resource_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX IF NOT EXISTS normalized_events_chain_block_number_idx
    ON normalized_events (chain_id, block_number);
CREATE INDEX IF NOT EXISTS normalized_events_ens_v1_record_node_resolver_idx
    ON normalized_events (
        chain_id,
        lower(after_state ->> 'node'),
        lower(COALESCE(
            NULLIF(after_state ->> 'resolver', ''),
            NULLIF(raw_fact_ref ->> 'emitting_address', '')
        )),
        block_number,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE logical_name_id IS NULL
      AND source_family = 'ens_v1_resolver_l1'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX IF NOT EXISTS normalized_events_basenames_record_node_resolver_idx
    ON normalized_events (
        chain_id,
        lower(after_state ->> 'node'),
        lower(COALESCE(
            NULLIF(after_state ->> 'resolver', ''),
            NULLIF(raw_fact_ref ->> 'emitting_address', '')
        )),
        block_number,
        transaction_index,
        log_index,
        normalized_event_id
    )
    WHERE logical_name_id IS NULL
      AND source_family = 'basenames_base_resolver'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX IF NOT EXISTS normalized_events_pointer_after_resolver_history_idx
    ON normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        block_number,
        block_hash
    ) INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX IF NOT EXISTS normalized_events_projection_idx
    ON normalized_events (
        event_kind,
        canonicality_state,
        chain_id,
        block_number,
        normalized_event_id
    );

ALTER TABLE name_surfaces ADD COLUMN raw_labels text[];
CREATE TEMP TABLE project_changed_events (LIKE normalized_events);
INSERT INTO chain_lineage VALUES('bench',10,'block','canonical'),('bench',11,'future','canonical'),('bench',9,'orphan','orphaned'),('other',10,'block','canonical');
INSERT INTO project_declared_resolver_addresses VALUES('ens','0xmirror','mirror','ensv1_mirror_resolver',2),('other','0xmirror','mirror','ensv1_mirror_resolver',3),('ens','0xordinary','v2','public_resolver_v2',2);
INSERT INTO name_surfaces SELECT 'name-'||i,'ens','node-'||i,'bench',10,'block','canonical',ARRAY['label-'||i,'eth'] FROM generate_series(1,32)i;
INSERT INTO name_surfaces VALUES('eth','ens','eth','bench',10,'block','canonical',ARRAY['eth']),('root','ens','root','bench',10,'block','canonical',ARRAY[]::text[]);
INSERT INTO normalized_events SELECT i,'v1-'||i,'bench','ens',CASE WHEN i%5=0 THEN NULL ELSE md5(('v1-'||i)::text)::uuid END,NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated',jsonb_build_object('node','NODE-'||i,'resolver','0xshared'),'{}','{}' FROM generate_series(1,32)i;
INSERT INTO normalized_events SELECT 100+i,'mirror-'||i,'bench','ens',md5(('mirror-'||i)::text)::uuid,'name-'||i,'ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated',jsonb_build_object('node','node-'||i,'resolver','0xMIRROR'),'{}','{}' FROM generate_series(1,32)i;
INSERT INTO normalized_events VALUES(200,'ancestor','bench','ens',NULL,NULL,'ResolverChanged','ens_v1_registry_l1',1,1,10,'block',0,0,'canonical','activated','{"node":"ETH"}','{}','{}');
-- Independent negative cases, and every admitted family/state variant.
UPDATE normalized_events SET chain_id='other' WHERE normalized_event_id IN(1,102);
UPDATE normalized_events SET block_number=11,block_hash='future' WHERE normalized_event_id IN(3,104);
UPDATE normalized_events SET block_hash='missing' WHERE normalized_event_id IN(5,106);
UPDATE normalized_events SET block_number=9,block_hash='orphan' WHERE normalized_event_id IN(7,108);
UPDATE normalized_events SET canonicality_state='orphaned' WHERE normalized_event_id IN(9,110);
UPDATE normalized_events SET consumer_visibility='candidate' WHERE normalized_event_id IN(11,112);
UPDATE normalized_events SET source_family='other' WHERE normalized_event_id IN(13,114);
UPDATE normalized_events SET event_kind='other' WHERE normalized_event_id IN(15,116);
UPDATE normalized_events SET after_state='{}' WHERE normalized_event_id IN(17,118);
UPDATE normalized_events SET resource_id=NULL WHERE normalized_event_id=120;
UPDATE normalized_events SET logical_name_id=NULL WHERE normalized_event_id=122;
UPDATE normalized_events SET after_state='{"resolver":"0xordinary"}' WHERE normalized_event_id=124;
UPDATE normalized_events SET namespace='other' WHERE normalized_event_id=25;
UPDATE normalized_events SET source_family='ens_v1_registrar_l1',canonicality_state='safe' WHERE normalized_event_id=26;
UPDATE normalized_events SET source_family='ens_v1_wrapper_l1',canonicality_state='finalized' WHERE normalized_event_id=27;
UPDATE normalized_events SET source_family='ens_v2_root_l1',canonicality_state='safe' WHERE normalized_event_id=126;
UPDATE name_surfaces SET chain_id='other' WHERE logical_name_id='name-28';
UPDATE name_surfaces SET block_number=11,block_hash='future' WHERE logical_name_id='name-29';
UPDATE name_surfaces SET block_hash='missing' WHERE logical_name_id='name-30';
UPDATE name_surfaces SET canonicality_state='orphaned' WHERE logical_name_id='name-31';
UPDATE name_surfaces SET namespace='other' WHERE logical_name_id='name-32';
-- Repeated pointers, multiple resources per node, and mixed case history must preserve all edges.
INSERT INTO normalized_events SELECT normalized_event_id+1000,event_identity||'-duplicate',chain_id,namespace,CASE WHEN normalized_event_id<100 THEN md5((resource_id::text||'-other'))::uuid ELSE resource_id END,logical_name_id,event_kind,source_family,source_manifest_id,manifest_version,block_number,block_hash,transaction_index,log_index,canonicality_state,consumer_visibility,after_state,before_state,raw_fact_ref FROM normalized_events;
INSERT INTO project_changed_events SELECT * FROM normalized_events WHERE normalized_event_id IN(5,9,11,15,17,21,1026);
ANALYZE normalized_events; ANALYZE name_surfaces; ANALYZE chain_lineage; ANALYZE project_changed_events; ANALYZE project_declared_resolver_addresses;
