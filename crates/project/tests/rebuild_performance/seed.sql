-- A mixed database for full-rebuild measurements and for the statement equality tests.
-- `__NAMES__` is the number of names and `__CHAIN__` the chain; every population scales with it.
--
-- Populations, by `i % 10`:
--   0-3  ENSv1 `.eth` registrar names whose registrar rows carry the name
--   4    ENSv1 `.eth` registrar names whose registrar rows were written before the label was known
--   5    ENSv1 names registered through the NameWrapper (the registrar rows carry no name and are
--        reached only through the lease the NameWrapped row recorded)
--   6-8  ENSv2 registry names (label rows without a resource, token rows with one, state-derived
--        expiry releases without a name, role grants with and without an admin holder)
--   9    ENSv1 registry-only subnames, half of them with a zero owner and a retained resolver
-- On top: `.eth` leases whose label is never learned, reverse claims with a primary name for every
-- second name, and node-keyed resolver records (coin-60 `AddressChanged` with and without its
-- `AddrChanged` sibling, other coins, text records, version bumps) for the ENSv1 names.
INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT '__CHAIN__', '0x' || lpad(to_hex(block), 64, '0'), block,
       to_timestamp(1700000000 + block * 12), 'canonical'::canonicality_state
FROM generate_series(1, 300) block;

CREATE TEMP TABLE seed AS
SELECT i,
       '0x' || md5('n' || i) || md5('m' || i) AS namehash,
       'ens:0x' || md5('n' || i) || md5('m' || i) AS logical_name_id,
       md5('r' || i)::uuid AS registrar,
       md5('w' || i)::uuid AS wrapper,
       md5('v' || i)::uuid AS token,
       md5('g' || i)::uuid AS registry_node,
       md5('l' || i)::uuid AS lineage,
       '0x' || substr(md5('o' || i), 1, 40) AS owner,
       '0x' || substr(md5('p' || i), 1, 40) AS later_owner,
       '0x' || lpad(to_hex(i), 64, '0') AS token_id,
       1 + (i % 250) AS block,
       '0x' || lpad(to_hex(1 + (i % 250)), 64, '0') AS block_hash,
       CASE WHEN i % 10 <= 3 THEN 'v1_named'
            WHEN i % 10 = 4 THEN 'v1_nameless'
            WHEN i % 10 = 5 THEN 'wrapped'
            WHEN i % 10 <= 8 THEN 'v2'
            ELSE 'registry_only' END AS shape,
       -- Most names share the public resolver; the rest split over two others.
       CASE WHEN i % 20 = 0 THEN '0x00000000000000000000000000000000000000b2'
            WHEN i % 20 = 1 THEN '0x00000000000000000000000000000000000000b3'
            ELSE '0x00000000000000000000000000000000000000b1' END AS resolver,
       i <= __NAMES__ AS known
FROM generate_series(1, __NAMES__ + __NAMES__ / 5) i;
CREATE INDEX ON seed (i);
ANALYZE seed;

INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
    deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
VALUES (1, 'ens', 'ens_v1_resolver_l1', '__CHAIN__', 'fixture', 'active', 'fixture',
        'fixture/rebuild-performance.toml', '{"deployment_epoch":"fixture","contracts":[
            {"role":"resolver","address":"0x00000000000000000000000000000000000000b1","proxy_kind":"none","start_block":0},
            {"role":"resolver","address":"0x00000000000000000000000000000000000000b2","proxy_kind":"none","start_block":0}]}');
INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, source_manifest_id, chain_id, derivation_kind, canonicality_state, after_state)
SELECT 'seed:manifest', 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1, manifest_id,
       '__CHAIN__', 'manifest_sync', 'canonical'::canonicality_state,
       jsonb_build_object('rollout_status', 'active', 'normalizer_version', 'fixture',
           'manifest_payload', manifest_payload)
FROM manifest_versions;

INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
SELECT lineage, '__CHAIN__', block_hash, block, 'canonical'::canonicality_state FROM seed
WHERE shape IN ('v1_named', 'v1_nameless', 'wrapped', 'v2');

-- The ENSv2 registry root: the resource every registration falls back to for admin roles.
INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number,
    provenance, canonicality_state)
VALUES ('00000000-0000-0000-0000-0000000000f0', NULL, '__CHAIN__',
        '0x' || lpad('1', 64, '0'), 1,
        '{"adapter":"ens_v2_permissions","source_family":"ens_v2_registry_l1","registry_contract_instance_id":"00000000-0000-0000-0000-000000000001","upstream_resource":"0x0000000000000000000000000000000000000000000000000000000000000000"}',
        'canonical'::canonicality_state);
INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number,
    provenance, canonicality_state)
SELECT registrar, lineage, '__CHAIN__', block_hash, block,
       '{"authority_kind":"registrar","source_family":"ens_v1_registrar_l1"}'::jsonb, 'canonical'::canonicality_state
FROM seed WHERE shape IN ('v1_named', 'v1_nameless', 'wrapped')
UNION ALL
SELECT wrapper, NULL, '__CHAIN__', block_hash, block,
       '{"authority_kind":"name_wrapper","source_family":"ens_v1_wrapper_l1"}'::jsonb, 'canonical'::canonicality_state
FROM seed WHERE known AND shape = 'wrapped'
UNION ALL
SELECT token, lineage, '__CHAIN__', block_hash, block,
       jsonb_build_object('adapter', 'ens_v2_permissions', 'source_family', 'ens_v2_registry_l1',
           'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001',
           'upstream_resource', token_id),
       'canonical'::canonicality_state
FROM seed WHERE known AND shape = 'v2'
UNION ALL
SELECT registry_node, NULL, '__CHAIN__', block_hash, block,
       '{"authority_kind":"registry_only","source_family":"ens_v1_registry_l1"}'::jsonb, 'canonical'::canonicality_state
FROM seed WHERE known AND shape = 'registry_only';

INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
    namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
    block_number, canonicality_state)
SELECT logical_name_id, 'ens',
       CASE WHEN shape = 'registry_only' THEN 'sub' || i || '.parent.eth' ELSE 'n' || i || '.eth' END,
       CASE WHEN shape = 'registry_only' THEN ARRAY['sub' || i, 'parent', 'eth']
            ELSE ARRAY['n' || i, 'eth'] END,
       '\x00', namehash,
       CASE WHEN shape = 'registry_only'
            THEN ARRAY['0x' || md5('a' || i) || md5('b' || i), '0x' || lpad('3', 64, '0'),
                       '0x' || lpad('2', 64, '0')]
            ELSE ARRAY['0x' || md5('a' || i) || md5('b' || i), '0x' || lpad('2', 64, '0')] END,
       'fixture', 'active', '__CHAIN__', block_hash, block, 'canonical'::canonicality_state
FROM seed WHERE known;

INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind,
    authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state, provenance)
SELECT md5('b' || i)::uuid, logical_name_id,
       CASE shape WHEN 'wrapped' THEN wrapper WHEN 'v2' THEN token
                  WHEN 'registry_only' THEN registry_node ELSE registrar END,
       'declared_registry_path', CASE WHEN shape = 'v2' THEN 'ens_v2' ELSE 'ens_v1' END,
       to_timestamp(1700000000 + block * 12 - 1), '__CHAIN__', block_hash, block, 'canonical'::canonicality_state,
       '{"transaction_index":0,"log_index":1}'
FROM seed WHERE known;

-- `.eth` registrar lifecycle: grant, expiry, a later renewal and a token transfer. Rows carry the
-- name only for the `v1_named` shape.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state, raw_fact_ref)
SELECT 'seed:registrar:' || kind || ':' || i, 'ens',
       CASE WHEN known AND shape = 'v1_named' THEN logical_name_id END, registrar, kind,
       'ens_v1_registrar_l1', 1, '__CHAIN__', block + later,
       '0x' || lpad(to_hex(block + later), 64, '0'),
       '0x' || md5('t' || i || ':' || later), 0, log, 'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       CASE kind
           WHEN 'TokenControlTransferred' THEN jsonb_build_object('source_event', 'Transfer',
               'authority_kind', 'registrar', 'from', owner, 'to', later_owner,
               'namehash', namehash)
           ELSE jsonb_build_object('source_event',
               CASE kind WHEN 'RegistrationRenewed' THEN 'NameRenewed' ELSE 'NameRegistered' END,
               'authority_kind', 'registrar', 'registrant', owner,
               'expiry', 1900000000 + i + later, 'namehash', namehash)
       END,
       '{"emitting_address":"0x00000000000000000000000000000000000000a1"}'
FROM seed
CROSS JOIN (VALUES ('RegistrationGranted', 0, 2), ('ExpiryChanged', 0, 3),
    ('RegistrationRenewed', 20, 2), ('TokenControlTransferred', 30, 4)) kinds(kind, later, log)
WHERE shape IN ('v1_named', 'v1_nameless', 'wrapped')
  AND (kind <> 'TokenControlTransferred' OR i % 3 = 0);

-- NameWrapper rows: the wrap records the registrar lease it took over.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state, raw_fact_ref)
SELECT 'seed:wrapper:' || kind || ':' || i, 'ens', logical_name_id, wrapper, kind,
       'ens_v1_wrapper_l1', 1, '__CHAIN__', block, block_hash, '0x' || md5('t' || i || ':0'), 0,
       log, 'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       jsonb_build_object('source_event', 'NameWrapped', 'node', namehash,
           'authority_kind', 'wrapper', 'wrapper_state', 'wrapped', 'fuses', 0, 'owner', owner,
           'to', owner, 'expiry', 1900000000 + i, 'wrapped_registrar_resource_id', registrar),
       '{"emitting_address":"0x00000000000000000000000000000000000000a2"}'
FROM seed
CROSS JOIN (VALUES ('SurfaceBound', 5), ('PermissionScopeChanged', 6), ('ExpiryChanged', 7),
    ('TokenControlTransferred', 8)) kinds(kind, log)
WHERE known AND shape = 'wrapped';

-- ENSv1 registry rows: ownership and the resolver pointer of every ENSv1 name.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state, raw_fact_ref)
SELECT 'seed:registry:' || kind || ':' || i, 'ens', logical_name_id,
       CASE shape WHEN 'wrapped' THEN wrapper WHEN 'registry_only' THEN registry_node
                  ELSE registrar END,
       kind, 'ens_v1_registry_l1', 1, '__CHAIN__', block, block_hash,
       '0x' || md5('t' || i || ':0'), 0, log, 'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       CASE kind
           WHEN 'ResolverChanged' THEN jsonb_build_object('node', namehash, 'resolver', resolver)
           WHEN 'AuthorityEpochChanged' THEN jsonb_build_object('node', namehash,
               'authority_kind', 'registry_only', 'owner', owner)
           ELSE jsonb_build_object('source_event', 'NewOwner', 'node', namehash, 'owner', owner,
               'owner_getter', CASE WHEN shape = 'registry_only' AND i % 20 = 9
                   THEN '0x0000000000000000000000000000000000000000' ELSE owner END,
               'owner_getter_reason', CASE WHEN shape = 'registry_only' AND i % 20 = 9
                   THEN 'zero_owner' END)
       END,
       '{"emitting_address":"0x00000000000000000000000000000000000000a3"}'
FROM seed
CROSS JOIN (VALUES ('AuthorityTransferred', 9), ('ResolverChanged', 10),
    ('AuthorityEpochChanged', 11)) kinds(kind, log)
WHERE known AND shape <> 'v2' AND (kind <> 'AuthorityEpochChanged' OR shape = 'registry_only');

-- ENSv2 registry rows. The label row names the registration but no resource; the token row names
-- both. Every third registration is later released by a state-derived path expiry that names the
-- resource but no name, and every fourth of those is registered again. Every seventh registration
-- is unregistered at the end.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state, raw_fact_ref)
SELECT 'seed:v2:' || tag || ':' || i, 'ens',
       CASE WHEN tag = 'expired' THEN NULL ELSE logical_name_id END,
       CASE WHEN tag = 'label' THEN NULL ELSE token END,
       kind, 'ens_v2_registry_l1', 1, '__CHAIN__', block + later,
       '0x' || lpad(to_hex(block + later), 64, '0'),
       CASE WHEN tag = 'expired' THEN NULL ELSE '0x' || md5('t' || i || ':' || later) END,
       CASE WHEN tag = 'expired' THEN NULL ELSE 0 END,
       CASE WHEN tag = 'expired' THEN NULL ELSE log END,
       'ens_v2_registry_resource_surface', 'canonical'::canonicality_state,
       CASE tag
           WHEN 'resolver' THEN jsonb_build_object('source_event', 'ResolverUpdated',
               'resolver', resolver)
           WHEN 'transfer' THEN jsonb_build_object('source_event', 'TransferSingle',
               'from', owner, 'to', later_owner, 'token_id', token_id,
               'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001')
           WHEN 'unregistered' THEN jsonb_build_object('source_event', 'LabelUnregistered',
               'status', 'released', 'sender', later_owner, 'token_id', token_id,
               'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001')
           WHEN 'expired' THEN jsonb_build_object('source_event', 'RegistryPathExpired',
               'derived_from', 'interpreter_state',
               'terminal_reason', 'registry_name_binding_expired', 'token_id', token_id,
               'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001',
               'expiry', 1700000000 + i, 'status', 'released', 'released_at', 1700000000 + i)
           ELSE jsonb_build_object('source_event',
               CASE tag WHEN 'label' THEN 'LabelRegistered' WHEN 'renewed' THEN 'ExpiryUpdated'
                        ELSE 'TokenResource' END,
               'status', 'registered', 'authority_kind', 'ens_v2_registry', 'registrant', owner,
               'expiry', 1900000000 + i + later, 'token_id', token_id,
               'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001')
       END,
       '{"emitting_address":"0x00000000000000000000000000000000000000a4"}'
FROM seed
CROSS JOIN (VALUES ('label', 'RegistrationGranted', 0, 0), ('token', 'RegistrationGranted', 0, 1),
    ('resolver', 'ResolverChanged', 0, 3), ('renewed', 'RegistrationRenewed', 10, 2),
    ('transfer', 'TokenControlTransferred', 15, 4), ('expired', 'RegistrationReleased', 20, 0),
    ('again', 'RegistrationGranted', 25, 1), ('unregistered', 'RegistrationReleased', 30, 5)
) kinds(tag, kind, later, log)
WHERE known AND shape = 'v2'
  AND (tag NOT IN ('transfer') OR i % 2 = 0)
  AND (tag NOT IN ('expired', 'again') OR i % 3 = 0)
  AND (tag <> 'again' OR i % 4 = 0)
  AND (tag <> 'unregistered' OR i % 7 = 0);

-- ENSv2 role grants: a holder role on every registration, an admin role on every second one, and
-- one admin role on the registry root.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    before_state, after_state)
SELECT 'seed:v2:role:' || role || ':' || i, 'ens', NULL, token, 'PermissionChanged',
       'ens_v2_registry_l1', 1, '__CHAIN__', block, block_hash, '0x' || md5('t' || i || ':0'), 0,
       log, 'ens_v2_permissions', 'canonical'::canonicality_state,
       jsonb_build_object('subject', subject, 'effective_powers', '[]'::jsonb),
       jsonb_build_object('subject', subject,
           'scope', jsonb_build_object('kind', 'registry', 'chain_id', '__CHAIN__',
               'registry_address', '0x00000000000000000000000000000000000000a4'),
           'effective_powers', powers,
           'grant_source', jsonb_build_object('kind', 'raw_log',
               'source_event', 'EACRolesChanged', 'upstream_resource', token_id,
               'root_resource', false, 'changed_powers', powers,
               'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001'),
           'revocation_source', NULL, 'inheritance_path', '[]'::jsonb,
           'transfer_behavior', '{}'::jsonb, 'source_event', 'EACRolesChanged',
           'upstream_resource', token_id, 'resource', token_id, 'root_resource', false,
           'registry_contract_instance_id', '00000000-0000-0000-0000-000000000001')
FROM seed
CROSS JOIN LATERAL (VALUES
    ('holder', 5, owner, '["unregister","set_resolver"]'::jsonb),
    ('admin', 6, later_owner, '["admin_set_resolver","can_transfer_admin"]'::jsonb)
) roles(role, log, subject, powers)
WHERE known AND shape = 'v2' AND (role = 'holder' OR i % 2 = 0);
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    before_state, after_state)
VALUES ('seed:v2:role:root', 'ens', NULL, '00000000-0000-0000-0000-0000000000f0',
    'RootPermissionChanged', 'ens_v2_registry_l1', 1, '__CHAIN__', 1, '0x' || lpad('1', 64, '0'),
    '0x' || md5('root'), 0, 0, 'ens_v2_permissions', 'canonical'::canonicality_state,
    '{"subject":"0x00000000000000000000000000000000000000c1","effective_powers":[]}',
    '{"subject":"0x00000000000000000000000000000000000000c1",
      "scope":{"kind":"registry_root","chain_id":"__CHAIN__","registry_address":"0x00000000000000000000000000000000000000a4"},
      "effective_powers":["admin_renew"],
      "grant_source":{"kind":"raw_log","source_event":"EACRolesChanged","upstream_resource":"0x0000000000000000000000000000000000000000000000000000000000000000","root_resource":true,"changed_powers":["admin_renew"],"registry_contract_instance_id":"00000000-0000-0000-0000-000000000001"},
      "revocation_source":null,
      "inheritance_path":[{"kind":"registry_root_fallback","chain_id":"__CHAIN__","registry_address":"0x00000000000000000000000000000000000000a4","upstream_resource":"0x0000000000000000000000000000000000000000000000000000000000000000"}],
      "transfer_behavior":{},"source_event":"EACRolesChanged",
      "upstream_resource":"0x0000000000000000000000000000000000000000000000000000000000000000",
      "resource":"0x0000000000000000000000000000000000000000000000000000000000000000",
      "root_resource":true,"registry_contract_instance_id":"00000000-0000-0000-0000-000000000001"}');

-- Node-keyed resolver records for the ENSv1 names; none carries a name or a resource. Every name
-- sets coin 60 through `AddressChanged` with its `AddrChanged` sibling at the next log index;
-- every second name sets it again later without a sibling, plus another coin and a text record;
-- every seventh bumps the record version in between.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, source_manifest_id, chain_id, block_number,
    block_hash, transaction_hash, transaction_index, log_index, derivation_kind,
    canonicality_state, after_state, raw_fact_ref)
SELECT 'seed:record:' || tag || ':' || i, 'ens', NULL, NULL,
       CASE WHEN tag = 'version' THEN 'RecordVersionChanged' ELSE 'RecordChanged' END,
       'ens_v1_resolver_l1', 1, (SELECT manifest_id FROM manifest_versions), '__CHAIN__',
       block + later, '0x' || lpad(to_hex(block + later), 64, '0'),
       '0x' || md5('t' || i || ':' || later), 0, log, 'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       CASE tag
           WHEN 'version' THEN jsonb_build_object('source_event', 'VersionChanged',
               'node', namehash, 'resolver', resolver, 'record_version', 1)
           WHEN 'text' THEN jsonb_build_object('source_event', 'TextChanged', 'node', namehash,
               'resolver', resolver, 'record_key', 'text:url', 'record_family', 'text',
               'selector_key', 'url', 'value', 'https://example.test/' || i)
           ELSE jsonb_build_object('source_event',
               CASE WHEN tag = 'sibling' THEN 'AddrChanged' ELSE 'AddressChanged' END,
               'node', namehash, 'resolver', resolver,
               'record_key', 'addr:' || coin, 'record_family', 'addr', 'selector_key', coin,
               'value', owner)
       END,
       jsonb_build_object('emitting_address', resolver)
FROM seed
CROSS JOIN (VALUES ('coin60', '60', 1, 20), ('sibling', '60', 1, 21), ('version', NULL, 2, 5),
    ('coin60_again', '60', 3, 20), ('coin0', '0', 3, 22), ('text', NULL, 3, 23)
) kinds(tag, coin, later, log)
WHERE known AND shape <> 'v2'
  AND (tag NOT IN ('coin60_again', 'coin0', 'text') OR i % 2 = 0)
  AND (tag <> 'version' OR i % 7 = 0);

-- Reverse claims: every second name's owner claims its reverse node, points it at the public
-- resolver, and sets a primary name there. Every fifth claimant renames once.
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state, raw_fact_ref)
SELECT 'seed:reverse:' || tag || ':' || i, 'ens', NULL, NULL, kind, family, 1, '__CHAIN__',
       block + later, '0x' || lpad(to_hex(block + later), 64, '0'),
       '0x' || md5('t' || i || ':' || later), 0, log, 'ens_v1_unwrapped_authority', 'canonical'::canonicality_state,
       CASE tag
           WHEN 'claim' THEN jsonb_build_object('source_event', 'ReverseClaimed', 'address', owner,
               'coin_type', '60', 'namespace', 'ens',
               'reverse_node', '0x' || md5('x' || i) || md5('y' || i))
           WHEN 'pointer' THEN jsonb_build_object('node', '0x' || md5('x' || i) || md5('y' || i),
               'resolver', '0x00000000000000000000000000000000000000b1')
           ELSE jsonb_build_object('source_event', 'NameChanged',
               'node', '0x' || md5('x' || i) || md5('y' || i),
               'resolver', '0x00000000000000000000000000000000000000b1', 'record_key', 'name',
               'record_family', 'name',
               'raw_name', CASE WHEN tag = 'renamed' THEN 'renamed' || i || '.eth'
                                ELSE 'n' || i || '.eth' END)
       END,
       '{"emitting_address":"0x00000000000000000000000000000000000000b1"}'
FROM seed
CROSS JOIN (VALUES ('claim', 'ReverseChanged', 'ens_v1_reverse_l1', 4, 30),
    ('pointer', 'ResolverChanged', 'ens_v1_registry_l1', 4, 31),
    ('name', 'RecordChanged', 'ens_v1_resolver_l1', 4, 32),
    ('renamed', 'RecordChanged', 'ens_v1_resolver_l1', 6, 30)) kinds(tag, kind, family, later, log)
WHERE known AND i % 2 = 0 AND (tag <> 'renamed' OR i % 5 = 0);

DROP TABLE seed;
ANALYZE;
