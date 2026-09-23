TRUNCATE project_changed_events,name_surfaces,normalized_events,record_inventory_current,resolver_current,project_declared_resolver_addresses,project_scope_names,project_scope_resources;
INSERT INTO resolver_current VALUES ('bench','0xab','supported','{"classification":{"source_family":"ens_v1_resolver_l1","basis":"manifest_declared_address"}}','{"manifest_id":1}');
INSERT INTO project_declared_resolver_addresses VALUES (1,'ens','0xab');
INSERT INTO name_surfaces(chain_id,namehash,logical_name_id,canonicality_state) VALUES ('bench','node-1','name-1','canonical');
INSERT INTO normalized_events(normalized_event_id,chain_id,logical_name_id,resource_id,event_kind,source_family,namespace,canonicality_state,after_state) VALUES (1,'bench','name-1',md5('1')::uuid,'ResolverChanged','ens_v2_registry_l1','ens','canonical','{"resolver":"0xAB"}'),(2,'bench','name-1',md5('1')::uuid,'ResolverChanged','ens_v2_registry_l1','ens','canonical','{"resolver":"0xff"}');
INSERT INTO record_inventory_current VALUES (md5('1')::uuid,'{"chain_id":"bench","resolver_pointer_event_id":1}','supported');
INSERT INTO project_changed_events VALUES ('bench','different',NULL,'ens_v1_resolver_l1','RecordChanged',NULL,'{"node":"NODE-1"}','{"emitting_address":"0xAB"}');
