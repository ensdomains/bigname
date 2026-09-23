CREATE TEMP TABLE project_scope_resolvers(resolver_address text PRIMARY KEY);
CREATE TEMP TABLE project_scope_resolver_passthrough(resolver_address text PRIMARY KEY);
CREATE TEMP TABLE chain_lineage(chain_id text,block_number bigint,block_hash text,canonicality_state text);
INSERT INTO chain_lineage VALUES('ethereum-sepolia',1,'canonical','canonical');
CREATE TEMP TABLE project_manifests(manifest_id bigint,namespace text,source_family text);
INSERT INTO project_manifests VALUES(1,'ens','ens_v1_registry_l1'),(2,'ens','ens_v2_registry_l1'),(3,'basenames','basenames_base_registry');
CREATE TEMP TABLE contract_instance_addresses AS
SELECT i AS contract_instance_id,'ethereum-sepolia'::text AS chain_id,upper('address-'||i) AS address,
       1::bigint AS active_from_block_number,'canonical'::text AS active_from_block_hash,
       NULL::bigint AS active_to_block_number,NULL::timestamptz AS deactivated_at
FROM generate_series(1,2000)i;
CREATE TEMP TABLE discovery_edges AS
SELECT i AS to_contract_instance_id,'ethereum-sepolia'::text AS chain_id,'resolver'::text AS edge_kind,
       manifest_id AS source_manifest_id,'canonical'::text AS canonicality_state,
       1::bigint AS active_from_block_number,'canonical'::text AS active_from_block_hash,
       NULL::bigint AS active_to_block_number,NULL::timestamptz AS deactivated_at
FROM generate_series(1,2000)i CROSS JOIN project_manifests;
CREATE TEMP TABLE project_declared_resolver_addresses AS
SELECT lower('address-'||i) AS resolver_address,namespace,source_family,
       'role-'||manifest_id AS classification_role,manifest_id,
       1::bigint AS declaration_start_block,1::bigint AS classification_declaration_ordinality
FROM generate_series(1,2000)i CROSS JOIN project_manifests WHERE manifest_id IN(1,3);
CREATE INDEX ON contract_instance_addresses(chain_id,lower(address)) WHERE deactivated_at IS NULL;
CREATE INDEX ON discovery_edges(chain_id,to_contract_instance_id,edge_kind) WHERE deactivated_at IS NULL;
ANALYZE contract_instance_addresses;
ANALYZE discovery_edges;
