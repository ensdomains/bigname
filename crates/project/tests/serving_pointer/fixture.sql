CREATE TEMP TABLE project_events (normalized_event_id bigint, logical_name_id text, resource_id uuid, event_kind text, source_family text, chain_id text, block_number bigint, transaction_index bigint, log_index bigint, event_identity text, after_state jsonb);
CREATE INDEX project_events_name_idx ON project_events(logical_name_id,normalized_event_id);
CREATE INDEX project_events_resource_idx ON project_events(resource_id,normalized_event_id);
CREATE INDEX project_events_kind_idx ON project_events(event_kind,normalized_event_id);
CREATE INDEX project_events_kind_chain_idx ON project_events(event_kind,chain_id,normalized_event_id);
CREATE TEMP TABLE project_name_authority(logical_name_id text PRIMARY KEY, selected_binding_id uuid, unsupported_reason text, known_ownerless_registry bool DEFAULT false, ownerless_registry_resource_id uuid, owner_getter_reason text);
CREATE TEMP TABLE project_resources(resource_id uuid PRIMARY KEY, token_lineage_id uuid);
INSERT INTO project_name_authority (logical_name_id,selected_binding_id,unsupported_reason) VALUES ('target:absent',NULL,'current_authority_not_projected'),
('target:direct',NULL,'current_authority_not_projected'),
('target:nameless_later',NULL,'current_authority_not_projected'),
('target:null_clear',NULL,'current_authority_not_projected'),
('target:zero_clear',NULL,'current_authority_not_projected'),
('target:older_clear',NULL,'current_authority_not_projected'),
('target:released_later',NULL,'current_authority_not_projected'),
('target:released_same',NULL,'current_authority_not_projected'),
('target:released_before',NULL,'current_authority_not_projected'),
('target:duplicate_links',NULL,'current_authority_not_projected'),
('target:wrong_family',NULL,'current_authority_not_projected'),
('target:null_resource',NULL,'current_authority_not_projected'),
('target:bound','bde14145-a3d8-55cf-8ec8-c78bb24e833d','current_authority_not_projected'),
('target:other_reason',NULL,'other'),
('target:multiple_resources',NULL,'current_authority_not_projected'),
('target:foreign_nameless',NULL,'current_authority_not_projected'),
('target:other_name_pointer',NULL,'current_authority_not_projected'),
('target:tie_order',NULL,'current_authority_not_projected');
INSERT INTO project_events VALUES ('1','target:direct','65f3291c-5bbb-5e8c-8ee7-f57d11353d1e','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','direct-1','{"resolver": "0x123"}'),
('2','target:nameless_later','ce696747-6410-5242-82b1-b196962af18f','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','nameless_later-2','{"resolver": "0x123"}'),
('3',NULL,'ce696747-6410-5242-82b1-b196962af18f','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','11','0','0','nameless_later-3','{"resolver": "0x123"}'),
('4','target:null_clear','11da5759-ba20-5c9f-9926-2d8447a86d00','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','null_clear-4','{"resolver": "0x123"}'),
('5',NULL,'11da5759-ba20-5c9f-9926-2d8447a86d00','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','11','0','0','null_clear-5','{"resolver": null}'),
('6','target:zero_clear','15a00a61-9d3e-5038-b6ab-1feeaaa318f0','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','zero_clear-6','{"resolver": "0x123"}'),
('7',NULL,'15a00a61-9d3e-5038-b6ab-1feeaaa318f0','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','11','0','0','zero_clear-7','{"resolver": "0x0000000000000000000000000000000000000000"}'),
('8',NULL,'4e562bbd-79c3-52dc-bc97-c74cb40af78a','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','9','0','0','older_clear-8','{"resolver": null}'),
('9','target:older_clear','4e562bbd-79c3-52dc-bc97-c74cb40af78a','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','older_clear-9','{"resolver": "0x123"}'),
('10','target:released_later','960eb253-b846-5746-b0f2-2435c3fc2b89','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','released_later-10','{"resolver": "0x123"}'),
('11',NULL,'960eb253-b846-5746-b0f2-2435c3fc2b89','RegistrationReleased','ens_v2_root_l1','ethereum-sepolia','11','0','0','released_later-11','{"resolver": "0x123"}'),
('12','target:released_same','82fd214e-5751-528d-87d5-cfbe94597f8a','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','released_same-12','{"resolver": "0x123"}'),
('13',NULL,'82fd214e-5751-528d-87d5-cfbe94597f8a','RegistrationReleased','ens_v2_root_l1','ethereum-sepolia','10','0','0','released_same-13','{"resolver": "0x123"}'),
('14',NULL,'d7f0101f-912c-5db4-9032-efa0bb70be1f','RegistrationReleased','ens_v2_root_l1','ethereum-sepolia','9','0','0','released_before-14','{"resolver": "0x123"}'),
('15','target:released_before','d7f0101f-912c-5db4-9032-efa0bb70be1f','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','released_before-15','{"resolver": "0x123"}'),
('16','target:duplicate_links','66c65309-3b26-54b4-b0b6-80592d7dd82c','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','8','0','0','duplicate_links-16','{"resolver": "0x123"}'),
('17','target:duplicate_links','66c65309-3b26-54b4-b0b6-80592d7dd82c','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','9','0','0','duplicate_links-17','{"resolver": "0x123"}'),
('18',NULL,'66c65309-3b26-54b4-b0b6-80592d7dd82c','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','duplicate_links-18','{"resolver": "0x123"}'),
('19','target:wrong_family','ef888d04-56a7-5890-ae42-c5c5f42cea9f','ResolverChanged','ens_v2_registry_l1','ethereum-sepolia','9','0','0','wrong_family-19','{"resolver": "0x123"}'),
('20',NULL,'ef888d04-56a7-5890-ae42-c5c5f42cea9f','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','wrong_family-20','{"resolver": "0x123"}'),
('21','target:null_resource',NULL,'ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','null_resource-21','{"resolver": "0x123"}'),
('22','target:bound','bde14145-a3d8-55cf-8ec8-c78bb24e833d','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','bound-22','{"resolver": "0x123"}'),
('23','target:other_reason','5e493aff-cf26-59e6-9990-29e1d4bf96c8','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','other_reason-23','{"resolver": "0x123"}'),
('24','target:multiple_resources','e1407479-3136-56c0-9908-bb02fb0339e2','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','8','0','0','multiple_resources-24','{"resolver": "0x123"}'),
('25','target:multiple_resources','3c480084-0bec-5ee3-a530-e96a1170273a','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','9','0','0','multiple_resources-25','{"resolver": "0x123"}'),
('26',NULL,'e1407479-3136-56c0-9908-bb02fb0339e2','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','multiple_resources-26','{"resolver": "0x123"}'),
('27','target:foreign_nameless','c5f95a0d-e75b-5b75-9863-04bfea86ed09','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','foreign_nameless-27','{"resolver": "0x123"}'),
('28',NULL,'11cf49be-4f6b-54c7-8588-cb9aeb8341ea','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','11','0','0','foreign_nameless-28','{"resolver": "0x123"}'),
('29','target:other_name_pointer','74d4ace7-c15c-5606-860c-167780dfcf15','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','other_name_pointer-29','{"resolver": "0x123"}'),
('30','target:othername','74d4ace7-c15c-5606-860c-167780dfcf15','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','11','0','0','othername-30','{"resolver": "0x123"}'),
('31','target:tie_order','a970cad2-3775-553c-b115-8ffa40fe7624','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','tie_order-31','{"resolver": "0x123"}'),
('32','target:tie_order','a970cad2-3775-553c-b115-8ffa40fe7624','ResolverChanged','ens_v2_root_l1','ethereum-sepolia','10','0','0','tie_order-32','{"resolver": "0x123"}');
CREATE TEMP TABLE expected(logical_name_id text,event_identity text);
INSERT INTO expected VALUES ('target:direct','direct-1'),
('target:nameless_later','nameless_later-3'),
('target:older_clear','older_clear-9'),
('target:released_before','released_before-15'),
('target:duplicate_links','duplicate_links-18'),
('target:multiple_resources','multiple_resources-26'),
('target:foreign_nameless','foreign_nameless-27'),
('target:other_name_pointer','other_name_pointer-29'),
('target:tie_order','tie_order-32');
-- Keep the unchanged ownerless-registry arm represented in full-statement equivalence.
INSERT INTO project_name_authority
(logical_name_id, known_ownerless_registry, ownerless_registry_resource_id, owner_getter_reason)
VALUES ('target:ownerless', true, '00000000-0000-0000-0000-000000000001', 'fixture_ownerless');
INSERT INTO project_resources VALUES ('00000000-0000-0000-0000-000000000001', NULL);
INSERT INTO project_events VALUES
(33,'target:ownerless','00000000-0000-0000-0000-000000000001','ResolverChanged','ens_v1_registry_l1','ethereum-sepolia',10,0,0,'ownerless-33','{"resolver":"0xABC"}');
INSERT INTO expected VALUES ('target:ownerless','ownerless-33');
