SET LOCAL statement_timeout='35s'; SET LOCAL work_mem='16MB'; SET LOCAL jit=off;
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
CREATE INDEX IF NOT EXISTS normalized_events_project_node_history_idx
    ON normalized_events (chain_id, lower(after_state ->> 'node'), block_number)
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));

INSERT INTO chain_lineage VALUES('bench',10,'block','canonical');
INSERT INTO project_declared_resolver_addresses VALUES('ens','0xshared','ens_v2_resolver_l1','public_resolver_v2',1);
INSERT INTO name_surfaces SELECT 'ens:node-'||i,'ens','node-'||i,'bench',10,'block','canonical' FROM generate_series(1,__NAMES__)i;
INSERT INTO project_scope_names SELECT logical_name_id FROM name_surfaces;
INSERT INTO project_scope_resources SELECT md5(i::text)::uuid FROM generate_series(1,__NAMES__)i;
INSERT INTO normalized_events SELECT i,'pointer-'||i,'bench','ens',md5(i::text)::uuid,'ens:node-'||i,'ResolverChanged','ens_v2_registry_l1',1,1,10,'block',0,0,'canonical','activated',jsonb_build_object('resolver','0xShared','node','node-'||i),'{}','{}' FROM generate_series(1,__NAMES__)i;
INSERT INTO normalized_events SELECT 100000+i,'record-'||i,'bench','ens',NULL,NULL,'RecordChanged','ens_v2_resolver_l1',1,1,10,'block',0,0,'canonical','activated',jsonb_build_object('node','node-'||i,'resolver','0xShared'),'{}','{}' FROM generate_series(1,__RECORDS__)i;
ANALYZE normalized_events; ANALYZE name_surfaces; ANALYZE chain_lineage; ANALYZE project_scope_names; ANALYZE project_scope_children; ANALYZE project_scope_resources; ANALYZE project_declared_resolver_addresses;

CREATE TEMP TABLE project_scope_ancestors(logical_name_id text PRIMARY KEY);
CREATE TEMP TABLE project_scope_account_permissions(chain_id text,authority_kind text,authority_contract text,owner text,subject text,relation_kind text);
CREATE TEMP TABLE project_scope_resolvers(resolver_address text PRIMARY KEY);
CREATE TEMP TABLE project_scope_resolver_passthrough(resolver_address text PRIMARY KEY);
CREATE TEMP TABLE project_scope_resolver_candidate_events(normalized_event_id bigint PRIMARY KEY,resource_id uuid);
CREATE TEMP TABLE project_scope_primary(address text,coin_type text,namespace text);
CREATE TEMP TABLE children_current(parent_logical_name_id text,child_logical_name_id text,provenance jsonb);
CREATE TEMP TABLE project_changed_events (LIKE normalized_events);
INSERT INTO project_scope_children SELECT logical_name_id FROM project_scope_names;
INSERT INTO project_scope_resolvers VALUES('0xshared');
INSERT INTO project_scope_primary VALUES('0xowner','60','ens');
INSERT INTO normalized_events VALUES(200001,'reverse','bench','ens',NULL,NULL,'ReverseChanged','ens_v1_reverse_registrar',1,1,10,'block',0,0,'canonical','activated',
'{"address":"0xOwner","coin_type":"60","namespace":"ens","reverse_node":"NODE-1"}','{}','{}');
