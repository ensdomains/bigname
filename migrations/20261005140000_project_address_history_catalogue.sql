-- New semantic Project family: deploy matching binaries only after the full-history
-- Interpret redo and its Project redo. Merely installing the tables publishes no catalogue.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN RETURN; END IF;
    LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
-- Compact address-history catalogue. See docs/storage.md table ownership.

CREATE TABLE IF NOT EXISTS bigname_phase.project_address_history_anchor (
    chain_id text NOT NULL,
    address text NOT NULL,
    namespace text NOT NULL,
    anchor_kind smallint NOT NULL,
    anchor_id text NOT NULL,
    current_mask smallint NOT NULL DEFAULT 0,
    historical_mask smallint NOT NULL DEFAULT 0,
    current_resource_id uuid,
    first_bucket bigint,
    last_bucket bigint,
    bucket_range int8range NOT NULL DEFAULT 'empty'::int8range,
    event_mask bigint NOT NULL DEFAULT 0,
    key_bloom bit(256) NOT NULL DEFAULT B'0'::bit(256),
    PRIMARY KEY (chain_id, anchor_kind, anchor_id, address),
    CHECK (anchor_kind IN (0, 1)),
    CHECK (current_mask BETWEEN 0 AND 7 AND historical_mask BETWEEN 0 AND 3),
    CHECK (anchor_kind = 0 OR current_resource_id IS NULL),
    CHECK ((first_bucket IS NULL) = (last_bucket IS NULL)),
    CHECK (first_bucket IS NULL OR (first_bucket >= -1 AND last_bucket >= first_bucket)),
    CHECK (bucket_range = CASE WHEN first_bucket IS NULL THEN 'empty'::int8range
        ELSE int8range(first_bucket, last_bucket + 1, '[)') END),
    CHECK (event_mask >= 0)
);

COMMENT ON TABLE bigname_phase.project_address_history_anchor IS
    'Project-owned exact current/historical address membership by logical name or resource. Semantic and pruning fields are journalled together by their owning chain; undo restores both atomically. No event IDs or payloads are copied per address.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.chain_id IS
    'The owning chain of this catalogue fact.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.address IS
    'The lower-case related address.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.namespace IS
    'The namespace of the qualifying membership evidence.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.anchor_kind IS
    'Anchor encoding: 0 logical name, 1 resource.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.anchor_id IS
    'The stable logical-name ID or resource UUID text, according to anchor_kind.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.current_mask IS
    'Exact current relation bits: owner 1, effective controller 2, role holder 4.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.historical_mask IS
    'Exact historical relation bits: owner 1, effective controller 2, independent of current reasons.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.current_resource_id IS
    'For a logical-name row, its selected current resource; these name rows are the provenance of resource current membership.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.first_bucket IS
    'The earliest conservative candidate bucket, block divided by 256 or -1 for unpositioned events; null only for an empty envelope.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.last_bucket IS
    'The latest conservative candidate bucket; null only for an empty envelope.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.bucket_range IS
    'The inclusive first/last candidate buckets encoded as half-open int8range; empty when no candidate source is known.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.event_mask IS
    'Frozen event-kind bit union; unknown kinds are all-matching. A missing bit proves absence only.';

COMMENT ON COLUMN bigname_phase.project_address_history_anchor.key_bloom IS
    '256-bit negative record-key summary: exact UTF-8 key MD5 bytes 0,5,10,15 select bits; resets and unknown kinds match every key.';

CREATE TABLE IF NOT EXISTS bigname_phase.project_history_source (
    chain_id text NOT NULL,
    source_kind smallint NOT NULL,
    source_key text NOT NULL,
    resolver_address text NOT NULL DEFAULT '',
    first_bucket bigint,
    last_bucket bigint,
    bucket_range int8range NOT NULL DEFAULT 'empty'::int8range,
    event_mask bigint NOT NULL DEFAULT 0,
    key_bloom bit(256) NOT NULL DEFAULT B'0'::bit(256),
    PRIMARY KEY (chain_id, source_kind, source_key, resolver_address),
    CHECK (source_kind BETWEEN 0 AND 3),
    CHECK (source_kind = 3 OR resolver_address = ''),
    CHECK ((first_bucket IS NULL) = (last_bucket IS NULL)),
    CHECK (first_bucket IS NULL OR (first_bucket >= -1 AND last_bucket >= first_bucket)),
    CHECK (bucket_range = CASE WHEN first_bucket IS NULL THEN 'empty'::int8range
        ELSE int8range(first_bucket, last_bucket + 1, '[)') END),
    CHECK (event_mask >= 0)
);

COMMENT ON TABLE bigname_phase.project_history_source IS
    'Project-owned shared event-source bounds and negative filter summaries, one row per chain/name, resource, node, or resolver/record ID. Journalled by the source chain; empty sources retain a reachable key without event rows.';

COMMENT ON COLUMN bigname_phase.project_history_source.chain_id IS
    'The owning chain of this catalogue fact.';

COMMENT ON COLUMN bigname_phase.project_history_source.source_kind IS
    'Source encoding: 0 logical name, 1 resource, 2 node records, 3 resolver record-ID writes.';

COMMENT ON COLUMN bigname_phase.project_history_source.source_key IS
    'Stable name ID, resource UUID text, lower-case node, or record ID according to source_kind.';

COMMENT ON COLUMN bigname_phase.project_history_source.resolver_address IS
    'Lower-case resolver for a record-ID source, empty for other source kinds.';

COMMENT ON COLUMN bigname_phase.project_history_source.first_bucket IS
    'The earliest conservative candidate bucket, block divided by 256 or -1 for unpositioned events; null only for an empty envelope.';

COMMENT ON COLUMN bigname_phase.project_history_source.last_bucket IS
    'The latest conservative candidate bucket; null only for an empty envelope.';

COMMENT ON COLUMN bigname_phase.project_history_source.bucket_range IS
    'The inclusive first/last candidate buckets encoded as half-open int8range; empty when no candidate source is known.';

COMMENT ON COLUMN bigname_phase.project_history_source.event_mask IS
    'Frozen event-kind bit union; unknown kinds are all-matching. A missing bit proves absence only.';

COMMENT ON COLUMN bigname_phase.project_history_source.key_bloom IS
    '256-bit negative record-key summary: exact UTF-8 key MD5 bytes 0,5,10,15 select bits; resets and unknown kinds match every key.';

CREATE TABLE IF NOT EXISTS bigname_phase.project_history_source_edge (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    source_kind smallint NOT NULL,
    source_key text NOT NULL,
    source_resolver text NOT NULL DEFAULT '',
    pointer_event_identity text NOT NULL,
    link_event_identity text NOT NULL DEFAULT '',
    pointer_resolver text NOT NULL,
    node text NOT NULL,
    pointer_block_number bigint,
    link_block_number bigint,
    first_bucket bigint,
    last_bucket bigint,
    bucket_range int8range NOT NULL DEFAULT 'empty'::int8range,
    event_mask bigint NOT NULL DEFAULT 0,
    key_bloom bit(256) NOT NULL DEFAULT B'0'::bit(256),
    PRIMARY KEY (chain_id, resource_id, source_kind, source_key, source_resolver,
        pointer_event_identity, link_event_identity),
    CHECK (source_kind IN (2, 3)),
    CHECK ((source_kind = 2 AND source_resolver = '' AND link_event_identity = '')
        OR (source_kind = 3 AND source_resolver <> '' AND link_event_identity <> '')),
    CHECK ((first_bucket IS NULL) = (last_bucket IS NULL)),
    CHECK (first_bucket IS NULL OR (first_bucket >= -1 AND last_bucket >= first_bucket)),
    CHECK (bucket_range = CASE WHEN first_bucket IS NULL THEN 'empty'::int8range
        ELSE int8range(first_bucket, last_bucket + 1, '[)') END),
    CHECK (event_mask >= 0)
);

COMMENT ON TABLE bigname_phase.project_history_source_edge IS
    'Project-owned conservative resolver-source reachability per resource and pointer/link evidence. Edges discover candidates; exact history attribution decides membership. No pointer-start lower bound is implied.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.chain_id IS
    'The owning chain of this catalogue fact.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.resource_id IS
    'The owning-chain resource whose history may reach this source.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.source_kind IS
    'Source encoding: 0 logical name, 1 resource, 2 node records, 3 resolver record-ID writes.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.source_key IS
    'Stable name ID, resource UUID text, lower-case node, or record ID according to source_kind.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.source_resolver IS
    'The reached source resolver key; empty for a node source.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.pointer_event_identity IS
    'The stable ResolverChanged identity that supplies this conservative reachability reason.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.link_event_identity IS
    'The stable ResolverRecordLinked identity for a record-ID source, empty for a node source.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.pointer_resolver IS
    'The pointer resolver address, also used for reverse link propagation.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.node IS
    'The pointer surface node; both this node and zero-node links can supply record-ID sources.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.pointer_block_number IS
    'The pointer evidence block, null when unpositioned; it never imposes a source start bound.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.link_block_number IS
    'The link evidence block, null for an absent or unpositioned link.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.first_bucket IS
    'The earliest conservative candidate bucket, block divided by 256 or -1 for unpositioned events; null only for an empty envelope.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.last_bucket IS
    'The latest conservative candidate bucket; null only for an empty envelope.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.bucket_range IS
    'The inclusive first/last candidate buckets encoded as half-open int8range; empty when no candidate source is known.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.event_mask IS
    'Frozen event-kind bit union; unknown kinds are all-matching. A missing bit proves absence only.';

COMMENT ON COLUMN bigname_phase.project_history_source_edge.key_bloom IS
    '256-bit negative record-key summary: exact UTF-8 key MD5 bytes 0,5,10,15 select bits; resets and unknown kinds match every key.';

CREATE TABLE IF NOT EXISTS bigname_phase.project_history_catalogue_marker (
    chain_id text PRIMARY KEY,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    publication_sequence bigint NOT NULL,
    input_content_hash text NOT NULL,
    catalogue_version smallint NOT NULL,
    CHECK (block_number >= 0 AND publication_sequence >= 0),
    CHECK (catalogue_version = 1)
);

COMMENT ON TABLE bigname_phase.project_history_catalogue_marker IS
    'Project-owned completeness stamp published atomically with the family marker and catalogue. A reader requires the captured family publication and matching catalogue version, sequence, position and input hash.';

COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.chain_id IS
    'The owning chain of this catalogue fact.';

COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.block_number IS
    'The exact family publication block whose catalogue is complete.';

COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.block_hash IS
    'The exact family publication block hash.';

COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.publication_sequence IS
    'The current family sequence, including undo/replay generations at the same block.';

COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.input_content_hash IS
    'The semantic input hash of the family publication.';

COMMENT ON COLUMN bigname_phase.project_history_catalogue_marker.catalogue_version IS
    'The frozen catalogue layout/encoding version; currently 1.';

CREATE INDEX IF NOT EXISTS project_address_history_first_idx ON bigname_phase.project_address_history_anchor (address, namespace, first_bucket);

CREATE INDEX IF NOT EXISTS project_address_history_last_idx ON bigname_phase.project_address_history_anchor (address, namespace, last_bucket);

CREATE INDEX IF NOT EXISTS project_address_history_any_first_idx ON bigname_phase.project_address_history_anchor (address, first_bucket);

CREATE INDEX IF NOT EXISTS project_address_history_any_last_idx ON bigname_phase.project_address_history_anchor (address, last_bucket);

CREATE INDEX IF NOT EXISTS project_address_history_overlap_idx ON bigname_phase.project_address_history_anchor USING gist (address, bucket_range);

CREATE INDEX IF NOT EXISTS project_address_history_historical_names_idx ON bigname_phase.project_address_history_anchor (address, anchor_id) WHERE anchor_kind = 0 AND historical_mask <> 0;

CREATE INDEX IF NOT EXISTS project_address_history_current_resource_idx ON bigname_phase.project_address_history_anchor (chain_id, current_resource_id) WHERE current_resource_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS project_history_edge_source_idx ON bigname_phase.project_history_source_edge (chain_id, source_kind, source_key, source_resolver);

CREATE INDEX IF NOT EXISTS project_history_edge_resolver_node_idx ON bigname_phase.project_history_source_edge (chain_id, pointer_resolver, node);


-- Preserve the existing source equality prefixes and replace their order suffixes.
DROP INDEX IF EXISTS bigname_phase.normalized_events_name_history_idx;

CREATE INDEX IF NOT EXISTS normalized_events_name_history_idx
    ON bigname_phase.normalized_events (
        logical_name_id,
        block_number DESC NULLS LAST,
        chain_id ASC NULLS LAST,
        block_hash DESC NULLS LAST,
        transaction_index DESC NULLS LAST,
        log_index DESC NULLS LAST,
        event_identity DESC
    )
    WHERE logical_name_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

DROP INDEX IF EXISTS bigname_phase.normalized_events_resource_history_idx;

CREATE INDEX IF NOT EXISTS normalized_events_resource_history_idx
    ON bigname_phase.normalized_events (
        resource_id,
        block_number DESC NULLS LAST,
        chain_id ASC NULLS LAST,
        block_hash DESC NULLS LAST,
        transaction_index DESC NULLS LAST,
        log_index DESC NULLS LAST,
        event_identity DESC
    )
    WHERE resource_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');

DROP INDEX IF EXISTS bigname_phase.normalized_events_project_node_history_idx;

CREATE INDEX IF NOT EXISTS normalized_events_project_node_history_idx
    ON bigname_phase.normalized_events (
        chain_id, lower(after_state ->> 'node'),
        block_number DESC NULLS LAST,
        block_hash DESC NULLS LAST,
        transaction_index DESC NULLS LAST,
        log_index DESC NULLS LAST,
        event_identity DESC
    )
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));

DROP INDEX IF EXISTS bigname_phase.normalized_events_record_id_write_idx;

CREATE INDEX IF NOT EXISTS normalized_events_record_id_write_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        (after_state ->> 'resolver_record_id'),
        block_number DESC NULLS LAST,
        block_hash DESC NULLS LAST,
        transaction_index DESC NULLS LAST,
        log_index DESC NULLS LAST,
        event_identity DESC
    )
    WHERE event_kind = 'RecordChanged'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
-- Complement readable serving indexes without duplicating canonical entries.
CREATE INDEX IF NOT EXISTS normalized_events_history_discovery_name_idx
    ON bigname_phase.normalized_events (chain_id, logical_name_id, block_number)
    WHERE logical_name_id IS NOT NULL AND resource_id IS NOT NULL
      AND canonicality_state NOT IN (
          'canonical'::bigname_phase.canonicality_state,
          'safe'::bigname_phase.canonicality_state,
          'finalized'::bigname_phase.canonicality_state);

CREATE INDEX IF NOT EXISTS normalized_events_history_discovery_resource_idx
    ON bigname_phase.normalized_events (chain_id, resource_id, block_number)
    WHERE logical_name_id IS NOT NULL AND resource_id IS NOT NULL
      AND canonicality_state NOT IN (
          'canonical'::bigname_phase.canonicality_state,
          'safe'::bigname_phase.canonicality_state,
          'finalized'::bigname_phase.canonicality_state);
END
$migration$;
