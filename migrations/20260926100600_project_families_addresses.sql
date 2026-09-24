-- Existing schema-v2 databases gain the Project-owned owned key family tables
-- project_reverse_tuple, project_reverse_node_claim,
-- project_claim_normalization, project_address_name_fold,
-- project_address_name_index, project_address_record_node_index,
-- project_address_record_id_index (TYR-36 step 2). The tables are additive
-- and unread by every served path; the family loop fills them block by block
-- after each Project batch commits. An empty schema-migration database has no
-- phase baseline yet, so this migration is a no-op there and phase-runner
-- init-schema installs the same tables.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_reverse_tuple (
    address text NOT NULL,
    coin_type text NOT NULL,
    namespace text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    reverse_node text,
    source_event text,
    claim_provenance jsonb,
    reverse_position jsonb,
    raw_name jsonb,
    raw_name_bytes jsonb,
    claim_event_identity text,
    claim_position jsonb,
    hydrated_name text,
    attempt_block bigint,
    attempt_hash text,
    attempt_ordinal bigint,
    baseline jsonb,
    PRIMARY KEY (address, coin_type, namespace),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_reverse_tuple IS
    'Project-owned reverse tuples of family F12: per address, coin type and namespace, the latest ReverseChanged and the latest direct claim, with the hydration result once hydration moves into the block. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.address IS
    'This value is the lower-cased address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.coin_type IS
    'This value is the coin type.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.reverse_node IS
    'This value is the lower-cased reverse node of the latest ReverseChanged.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.source_event IS
    'This value is that event''s source_event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.claim_provenance IS
    'This value is that event''s claim_provenance.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.reverse_position IS
    'This value is that ReverseChanged''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.raw_name IS
    'This value is the raw_name of the latest direct claim.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.raw_name_bytes IS
    'This value is the raw_name_bytes of that claim.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.claim_event_identity IS
    'This value is that claim''s event identity.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.claim_position IS
    'This value is that claim''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.hydrated_name IS
    'This value is the hydrated reverse name; null until hydration moves into the block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.attempt_block IS
    'This value is the hydration attempt block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.attempt_hash IS
    'This value is the hydration attempt block hash.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.attempt_ordinal IS
    'This value is the hydration attempt ordinal.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_tuple.baseline IS
    'This value is the pre-hydration baseline.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_reverse_node_claim (
    namespace text NOT NULL,
    reverse_node text NOT NULL,
    chain_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    resolver_address text,
    raw_name jsonb,
    raw_name_bytes jsonb,
    PRIMARY KEY (namespace, reverse_node),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_reverse_node_claim IS
    'Project-owned node-selected claim facts of family F12: per node, the latest name record, the claim a ReverseClaimed tuple selects through the node''s resolver. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.namespace IS
    'This value is the namespace.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.reverse_node IS
    'This value is the lower-cased node.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.resolver_address IS
    'This value is the lower-cased resolver the name record was written at.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.raw_name IS
    'This value is the record''s raw_name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_reverse_node_claim.raw_name_bytes IS
    'This value is the record''s raw_name_bytes.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_claim_normalization (
    chain_id text NOT NULL,
    claim_event_identity text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    status text NOT NULL,
    normalized_name text,
    reason text,
    PRIMARY KEY (chain_id, claim_event_identity),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_claim_normalization IS
    'Project-owned claim normalization of family F12: the normalization result of each claim event, stored once. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.claim_event_identity IS
    'This value is the claim event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.status IS
    'This value is success, not_found, invalid_name or unsupported.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.normalized_name IS
    'This value is the normalized name on success.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_claim_normalization.reason IS
    'This value is the reason when not successful.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_address_name_fold (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    block_number bigint NOT NULL,
    transaction_index bigint,
    log_index bigint,
    event_identity text NOT NULL,
    normalized_event_id bigint,
    controller text,
    controller_action text,
    controller_subject text,
    controller_position jsonb,
    token_holder text,
    token_holder_position jsonb,
    registrant text,
    registrant_position jsonb,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK ((transaction_index IS NULL) = (log_index IS NULL))
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_name_fold IS
    'Project-owned per-name address fold of family F13: the ordered controller fold, the token holder and the registrant, unmasked. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.logical_name_id IS
    'This value is the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.block_number IS
    'This value is the block number of the event that last wrote the row.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.transaction_index IS
    'This value is the transaction index of the event that last wrote the row; null with log_index for a synthesised event, which sorts before every transaction of its block.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.log_index IS
    'This value is the log index of the event that last wrote the row; null with transaction_index for a synthesised event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.event_identity IS
    'This value is the event identity of the event that last wrote the row, the final tiebreak of the canonical event order, compared as bytes.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.normalized_event_id IS
    'This value names the event that last wrote the row in normalized_events as attribution only; it never takes part in ordering.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.controller IS
    'This value is the controller the fold holds after the latest event.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.controller_action IS
    'This value is set or revoke, the latest controller action.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.controller_subject IS
    'This value is that action''s lower-cased subject.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.controller_position IS
    'This value is that action''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.token_holder IS
    'This value is the lower-cased recipient of the latest TokenControlTransferred.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.token_holder_position IS
    'This value is that transfer''s position.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.registrant IS
    'This value is the lower-cased registrant of the latest grant naming one.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_fold.registrant_position IS
    'This value is that grant''s position.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_address_name_index (
    address text NOT NULL,
    logical_name_id text NOT NULL,
    relation text NOT NULL,
    chain_id text NOT NULL,
    PRIMARY KEY (address, logical_name_id, relation)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_name_index IS
    'Project-owned address-to-name index of family F13, re-derived from project_address_name_fold and never journalled. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_index.address IS
    'This value is the lower-cased address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_index.logical_name_id IS
    'This value is the name.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_index.relation IS
    'This value is controller, token_holder or registrant.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_name_index.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_address_record_node_index (
    address text NOT NULL,
    coin_type text NOT NULL,
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    node text NOT NULL,
    PRIMARY KEY (address, coin_type, chain_id, resolver_address, node)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_record_node_index IS
    'Project-owned inverse address record index of family F14 for node-keyed values, re-derived from project_node_record_value and never journalled. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_node_index.address IS
    'This value is the lower-cased address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_node_index.coin_type IS
    'This value is the coin type.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_node_index.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_node_index.resolver_address IS
    'This value is the resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_node_index.node IS
    'This value is the node.'
$ddl$;
EXECUTE $ddl$
CREATE TABLE IF NOT EXISTS bigname_phase.project_address_record_id_index (
    address text NOT NULL,
    coin_type text NOT NULL,
    chain_id text NOT NULL,
    resolver_address text NOT NULL,
    record_id text NOT NULL,
    PRIMARY KEY (address, coin_type, chain_id, resolver_address, record_id)
)
$ddl$;
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_address_record_id_index IS
    'Project-owned inverse address record index of family F14 for record-id values, re-derived from project_record_id_value and never journalled. Step 2 of TYR-36 writes it block by block beside the served tables and nothing reads it yet.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_id_index.address IS
    'This value is the lower-cased address.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_id_index.coin_type IS
    'This value is the coin type.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_id_index.chain_id IS
    'This value is the chain.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_id_index.resolver_address IS
    'This value is the resolver.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_address_record_id_index.record_id IS
    'This value is the record id.'
$ddl$;
END
$migration$;
