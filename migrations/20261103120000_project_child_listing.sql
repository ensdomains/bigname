-- The child listing family F17: project_child_listing, one row per child a parent lists at the
-- publication, with its six read indexes, and project_child_listing_count, the per-parent and
-- per-registry child counts the family step keeps from the same diff, and
-- project_child_preimage_watermark, the highest block of every other chain whose preimage
-- observations each chain has read. An index on an existing family table serves the step's work
-- list: project_parent_subregistry by subregistry address.
--
-- The listing tables start empty and the watermarks start at each other chain's highest
-- observation or finality floor (below), so nothing needs a reset. The release that adds them rotates the
-- projection content hash, so the runner refuses a normal Project or Live start until a
-- full-range Project redo, which composes every row at its composition pass; until then the
-- API serves no family publication of the new build.
--
-- The index on the populated table is built in this transaction, not concurrently, on
-- the redo's maintenance window. The family marker table is taken in EXCLUSIVE mode first, as
-- 20261014120000_project_name_summary_bound_names.sql does, so no family run writes meanwhile.
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there and phase-runner init-schema installs the same tables and indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN
    RETURN;
END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
CREATE TABLE IF NOT EXISTS bigname_phase.project_child_listing (
    chain_id text NOT NULL,
    parent_logical_name_id text NOT NULL,
    child_logical_name_id text COLLATE "C" NOT NULL,
    namespace text NOT NULL,
    display_key text COLLATE "C" NOT NULL,
    display_name text NOT NULL,
    namehash text NOT NULL,
    labelhash text,
    owner text,
    registrant text,
    registry_authority text,
    lifecycle_shadow boolean NOT NULL,
    wrapper_held boolean NOT NULL,
    released_lease boolean NOT NULL,
    token_holder text,
    registry_address text,
    summary_owner text,
    registration_status text,
    expires_key numeric NOT NULL,
    registered_key numeric NOT NULL,
    recompose_at numeric,
    unhealed_labelhashes text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (chain_id, child_logical_name_id)
);
COMMENT ON TABLE bigname_phase.project_child_listing IS
    'Project-owned child listing of family F17: one row per child a parent lists at the publication, composed by the children relation (crates/storage families/topology/children.rs) for the block Project publishes and journalled for undo. The subnames page, the registry labels page and the child counts read it through its indexes.';
COMMENT ON COLUMN bigname_phase.project_child_listing.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN bigname_phase.project_child_listing.parent_logical_name_id IS
    'This value is the parent that lists the child.';
COMMENT ON COLUMN bigname_phase.project_child_listing.child_logical_name_id IS
    'This value is the child; a child is listed by at most one parent.';
COMMENT ON COLUMN bigname_phase.project_child_listing.namespace IS
    'This value is the child''s namespace.';
COMMENT ON COLUMN bigname_phase.project_child_listing.display_key IS
    'This value is the sort and cursor key of the name sort: display_name, cut to its first 500 characters when it is longer than 2000 bytes, compared bytewise.';
COMMENT ON COLUMN bigname_phase.project_child_listing.display_name IS
    'This value is the child name spelled from chain-observed label preimages only; the served name also applies imported preimages at read.';
COMMENT ON COLUMN bigname_phase.project_child_listing.namehash IS
    'This value is the child node.';
COMMENT ON COLUMN bigname_phase.project_child_listing.labelhash IS
    'This value is the child''s labelhash.';
COMMENT ON COLUMN bigname_phase.project_child_listing.owner IS
    'This value is the owner the child row serves.';
COMMENT ON COLUMN bigname_phase.project_child_listing.registrant IS
    'This value is the ENSv2 registrant of the child''s registration.';
COMMENT ON COLUMN bigname_phase.project_child_listing.registry_authority IS
    'This value is the authority a child with no name row serves (ens_v1 or ens_v0).';
COMMENT ON COLUMN bigname_phase.project_child_listing.lifecycle_shadow IS
    'This value is whether the child''s only surface is a shadow one a NameWrapper or ENSv1 registrar event observed.';
COMMENT ON COLUMN bigname_phase.project_child_listing.wrapper_held IS
    'This value is whether that lifecycle shadow''s registry owner is the NameWrapper that observed it.';
COMMENT ON COLUMN bigname_phase.project_child_listing.released_lease IS
    'This value is whether the child has no name surface and its registrar lease has been released.';
COMMENT ON COLUMN bigname_phase.project_child_listing.token_holder IS
    'This value is the holder of the child''s registrar lease when it has no name surface.';
COMMENT ON COLUMN bigname_phase.project_child_listing.registry_address IS
    'This value is the ENSv2 registry whose label the child is: its registration''s emitter while that is the parent''s current subregistry; null otherwise.';
COMMENT ON COLUMN bigname_phase.project_child_listing.summary_owner IS
    'This value is project_name_summary.owner of the child, which the registry labels'' owner filters read.';
COMMENT ON COLUMN bigname_phase.project_child_listing.registration_status IS
    'This value is project_name_summary.registration_status of the child, which the subnames expiry fence reads.';
COMMENT ON COLUMN bigname_phase.project_child_listing.expires_key IS
    'This value is project_name_summary.expires_at of the child in Unix seconds, -1 when it has none, the expires_at sort key.';
COMMENT ON COLUMN bigname_phase.project_child_listing.registered_key IS
    'This value is project_name_summary.registered_at of the child in Unix seconds, -1 when it has none, the registered_at sort key.';
COMMENT ON COLUMN bigname_phase.project_child_listing.recompose_at IS
    'This value is the first second, in Unix seconds, at which the ENSv1→ENSv2 migration gate stops keeping the child, the second after its NameWrapper expiry; the family step derives the child again at the first block whose time reaches it. Null when the gate does not apply.';
CREATE INDEX IF NOT EXISTS project_child_listing_name_idx
    ON bigname_phase.project_child_listing
        (chain_id, parent_logical_name_id, display_key, child_logical_name_id)
    INCLUDE (registration_status, released_lease, expires_key);
CREATE INDEX IF NOT EXISTS project_child_listing_expires_idx
    ON bigname_phase.project_child_listing
        (chain_id, parent_logical_name_id, expires_key, display_key, child_logical_name_id);
CREATE INDEX IF NOT EXISTS project_child_listing_registered_idx
    ON bigname_phase.project_child_listing
        (chain_id, parent_logical_name_id, registered_key, display_key, child_logical_name_id);
CREATE INDEX IF NOT EXISTS project_child_listing_labels_idx
    ON bigname_phase.project_child_listing
        (chain_id, parent_logical_name_id, registry_address, display_key, child_logical_name_id)
    WHERE registry_address IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_child_listing_label_owners_idx
    ON bigname_phase.project_child_listing
        (chain_id, parent_logical_name_id, registry_address, summary_owner, display_key,
         child_logical_name_id)
    WHERE registry_address IS NOT NULL;
COMMENT ON COLUMN bigname_phase.project_child_listing.unhealed_labelhashes IS
    'This value is the label hashes display_name still spells as [labelhash], the child''s own label first then its parent''s in order, empty when the name is fully spelled; the family step derives again the rows that hold a label a preimage observation names.';
CREATE INDEX IF NOT EXISTS project_child_listing_recompose_idx
    ON bigname_phase.project_child_listing (chain_id, recompose_at)
    WHERE recompose_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS project_child_listing_unhealed_idx
    ON bigname_phase.project_child_listing USING gin (unhealed_labelhashes)
    WHERE unhealed_labelhashes <> '{}';
CREATE TABLE IF NOT EXISTS bigname_phase.project_child_listing_count (
    chain_id text NOT NULL,
    parent_logical_name_id text NOT NULL,
    registry_address text NOT NULL,
    child_count bigint NOT NULL,
    PRIMARY KEY (chain_id, parent_logical_name_id, registry_address),
    CHECK (child_count > 0)
);
COMMENT ON TABLE bigname_phase.project_child_listing_count IS
    'Project-owned child counts of family F17: per parent, the number of its project_child_listing rows, and per parent and registry, the number of that registry''s labels. The family step moves them by its own diff in the same block transaction and journals them for undo; a count of zero has no row.';
COMMENT ON COLUMN bigname_phase.project_child_listing_count.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN bigname_phase.project_child_listing_count.parent_logical_name_id IS
    'This value is the parent.';
COMMENT ON COLUMN bigname_phase.project_child_listing_count.registry_address IS
    'This value is the registry whose labels the row counts, or the empty string for every child of the parent.';
COMMENT ON COLUMN bigname_phase.project_child_listing_count.child_count IS
    'This value is the number of listed rows the key covers.';
CREATE TABLE IF NOT EXISTS bigname_phase.project_child_preimage_watermark (
    chain_id text NOT NULL,
    source_chain_id text NOT NULL,
    source_block_number bigint NOT NULL,
    PRIMARY KEY (chain_id, source_chain_id),
    CHECK (source_chain_id <> chain_id),
    CHECK (source_block_number >= 0)
);
COMMENT ON TABLE bigname_phase.project_child_preimage_watermark IS
    'Project-owned watermarks of family F17: per chain and other chain, the highest block of the other chain, at or below its finalized (else safe) block, whose preimage observations the chain''s family step has read, so its stored child names heal from labels first observed there. The family step advances it in its own block transaction and journals it for undo; a reset clears it, so a rebuild reads every other chain from block 0.';
COMMENT ON COLUMN bigname_phase.project_child_preimage_watermark.chain_id IS
    'This value is the chain whose child listing the watermark heals.';
COMMENT ON COLUMN bigname_phase.project_child_preimage_watermark.source_chain_id IS
    'This value is the other chain whose preimage observations are read.';
COMMENT ON COLUMN bigname_phase.project_child_preimage_watermark.source_block_number IS
    'This value is the highest block of the source chain, at or below its finalized (else safe) block, whose preimage observations have been read.';
-- Each chain with a family marker starts its watermark for every other chain at that chain's
-- highest preimage observation, or its finality floor (finalized, else safe) if lower, so the
-- first family step after this schema-migration reads only observations that follow it and the
-- ones a reorg can still replace. An other chain with neither marker is not seeded. A rebuild
-- clears the rows and reads every other chain from 0.
INSERT INTO bigname_phase.project_child_preimage_watermark
    (chain_id, source_chain_id, source_block_number)
SELECT marker.chain_id, head.chain_id,
       LEAST(observed.block_number,
             COALESCE(head.finalized_block_number, head.safe_block_number))
FROM bigname_phase.project_family_marker marker
JOIN bigname_phase.chain_heads head
  ON head.chain_id <> marker.chain_id
 AND COALESCE(head.finalized_block_number, head.safe_block_number) IS NOT NULL
CROSS JOIN LATERAL (
    SELECT GREATEST(
        (SELECT max(event.block_number) FROM bigname_phase.normalized_events event
         WHERE event.event_kind = 'PreimageObserved' AND event.canonicality_state = 'canonical'
           AND event.chain_id = head.chain_id AND event.consumer_visibility = 'activated'),
        (SELECT max(event.block_number) FROM bigname_phase.normalized_events event
         WHERE event.event_kind = 'PreimageObserved' AND event.canonicality_state = 'safe'
           AND event.chain_id = head.chain_id AND event.consumer_visibility = 'activated'),
        (SELECT max(event.block_number) FROM bigname_phase.normalized_events event
         WHERE event.event_kind = 'PreimageObserved' AND event.canonicality_state = 'finalized'
           AND event.chain_id = head.chain_id AND event.consumer_visibility = 'activated')
    ) AS block_number
) observed
WHERE observed.block_number IS NOT NULL
ON CONFLICT (chain_id, source_chain_id) DO NOTHING;
CREATE INDEX IF NOT EXISTS project_parent_subregistry_address_idx
    ON bigname_phase.project_parent_subregistry (chain_id, subregistry_address);
END
$migration$;
