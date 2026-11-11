-- The address listing family F18: project_address_listing, one row per (chain, address, name)
-- that GET /v1/addresses/{address}/names lists for the ownership relations or as a former
-- owner, with one read index per offered sort and filter, and the indexes the family step's
-- own reads use. The family step derives the rows from the same relation fold the route
-- composes at read, so a page can walk an index instead of composing every candidate name.
--
-- The preimage watermark table of the child listing now also heals this family's stored names:
-- its comment says so.
--
-- The new table starts empty and needs no reset. The release that adds it rotates the
-- projection content hash, so the runner refuses a normal Project or Live start until a
-- full-range Project redo, which composes every row at its composition pass; until then the
-- API serves no family publication of the new build.
--
-- The family marker table is taken in EXCLUSIVE mode first, as
-- 20261014120000_project_name_summary_bound_names.sql does, so no family run writes meanwhile.
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there and phase-runner init-schema installs the same table and indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN
    RETURN;
END IF;
LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
CREATE TABLE IF NOT EXISTS bigname_phase.project_address_listing (
    chain_id text NOT NULL,
    address text NOT NULL,
    logical_name_id text COLLATE "C" NOT NULL,
    namespace text NOT NULL,
    display_key text COLLATE "C" NOT NULL,
    display_name text NOT NULL,
    normalized_name text COLLATE "C" NOT NULL,
    parent_normalized_name text COLLATE "C",
    namehash text NOT NULL,
    resource_id uuid,
    token_holder boolean NOT NULL,
    effective_controller boolean NOT NULL,
    role_holder boolean NOT NULL,
    listed boolean NOT NULL,
    former_owner boolean NOT NULL,
    registry_child boolean NOT NULL,
    registry_parent_logical_name_id text COLLATE "C",
    registration_lead boolean NOT NULL,
    public_authority text,
    migrated boolean NOT NULL,
    expires_key numeric NOT NULL,
    registered_key numeric NOT NULL,
    created_key numeric NOT NULL,
    former_expires_key numeric NOT NULL,
    token_holder_block bigint,
    effective_controller_block bigint,
    role_holder_block bigint,
    unhealed_labelhashes text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (chain_id, address, logical_name_id),
    CHECK (listed = (token_holder OR effective_controller OR role_holder)),
    CHECK (listed OR former_owner),
    CHECK (listed OR NOT registration_lead),
    CHECK (registry_child = (registry_parent_logical_name_id IS NOT NULL))
);
COMMENT ON TABLE bigname_phase.project_address_listing IS
    'Project-owned address listing of family F18: one row per (chain, address, name) that GET /v1/addresses/{address}/names lists for the token holder, effective controller or registry role holder relations, or as the former owner of a lapsed registration, derived at the publication by the relation fold the route composes with (crates/storage families/records/address_relations.rs) and journalled for undo.';
COMMENT ON COLUMN bigname_phase.project_address_listing.chain_id IS
    'This value is the chain.';
COMMENT ON COLUMN bigname_phase.project_address_listing.address IS
    'This value is the lower-cased address the row lists the name for.';
COMMENT ON COLUMN bigname_phase.project_address_listing.logical_name_id IS
    'This value is the name, or the <namespace>:<node> id of a surface-less ENSv1 registry child.';
COMMENT ON COLUMN bigname_phase.project_address_listing.namespace IS
    'This value is the name''s namespace.';
COMMENT ON COLUMN bigname_phase.project_address_listing.display_key IS
    'This value is the sort and cursor key of the name sort: display_name, cut to its first 500 characters when it is longer than 2000 bytes, compared bytewise.';
COMMENT ON COLUMN bigname_phase.project_address_listing.display_name IS
    'This value is the name spelled from chain-observed label preimages only; the served name also applies imported preimages at read.';
COMMENT ON COLUMN bigname_phase.project_address_listing.normalized_name IS
    'This value is the normalized name under the same spelling, which the q filter matches.';
COMMENT ON COLUMN bigname_phase.project_address_listing.parent_normalized_name IS
    'This value is normalized_name without its first label, null for a top-level name, which the parent filter matches.';
COMMENT ON COLUMN bigname_phase.project_address_listing.namehash IS
    'This value is the name''s node.';
COMMENT ON COLUMN bigname_phase.project_address_listing.resource_id IS
    'This value is the resource the name''s rows stand on: its selected binding''s, an ownerless registry name''s implied lease, or a registry child''s registry-only resource; the registration dedupe groups by it.';
COMMENT ON COLUMN bigname_phase.project_address_listing.token_holder IS
    'This value is whether the address holds the token_holder relation (served as owner).';
COMMENT ON COLUMN bigname_phase.project_address_listing.effective_controller IS
    'This value is whether the address holds the effective_controller relation (served as manager).';
COMMENT ON COLUMN bigname_phase.project_address_listing.role_holder IS
    'This value is whether the address holds an ENSv2 registry role on the name''s selected resource.';
COMMENT ON COLUMN bigname_phase.project_address_listing.listed IS
    'This value is whether the address holds any of the three relations; a row that is not listed is a former-owner row only.';
COMMENT ON COLUMN bigname_phase.project_address_listing.former_owner IS
    'This value is whether the address is the owner the name''s lapsed registration had when it ended.';
COMMENT ON COLUMN bigname_phase.project_address_listing.registry_child IS
    'This value is whether the row is a surface-less ENSv1 registry child, listed as its parent''s subnames serve it.';
COMMENT ON COLUMN bigname_phase.project_address_listing.registry_parent_logical_name_id IS
    'This value is the parent that lists a registry child row, null on every other row; the family step derives a parent''s registry children again by it.';
COMMENT ON COLUMN bigname_phase.project_address_listing.registration_lead IS
    'This value is whether the row is the smallest (display_key, logical_name_id) listed row of its address on its resource, the row the registration dedupe keeps when no filter removes it.';
COMMENT ON COLUMN bigname_phase.project_address_listing.public_authority IS
    'This value is the public authority the name serves (ens_v0, ens_v1 or ens_v2), a registry child''s registry authority, or null.';
COMMENT ON COLUMN bigname_phase.project_address_listing.migrated IS
    'This value is whether the name''s ENSv2 authority was selected by an ENSv1 to ENSv2 migration proof, which the is_migrated filter reads.';
COMMENT ON COLUMN bigname_phase.project_address_listing.expires_key IS
    'This value is the expires_at sort value of the composed name in Unix seconds, -1 when it has none.';
COMMENT ON COLUMN bigname_phase.project_address_listing.registered_key IS
    'This value is the registered_at sort value of the composed name in Unix seconds, -1 when it has none.';
COMMENT ON COLUMN bigname_phase.project_address_listing.created_key IS
    'This value is the created_at sort value of the composed name in Unix seconds, -1 when it has none.';
COMMENT ON COLUMN bigname_phase.project_address_listing.former_expires_key IS
    'This value is the served registration expiry the former-owner page sorts by, in Unix seconds, -1 when it has none.';
COMMENT ON COLUMN bigname_phase.project_address_listing.token_holder_block IS
    'This value is the block of the event that gave the address its token_holder relation, null when the relation has no cited event at or below the publication.';
COMMENT ON COLUMN bigname_phase.project_address_listing.effective_controller_block IS
    'This value is the block of the event that gave the address its effective_controller relation, null when the relation has no cited event at or below the publication.';
COMMENT ON COLUMN bigname_phase.project_address_listing.role_holder_block IS
    'This value is the block of the grant that gave the address its role_holder relation, null when the relation has no cited event at or below the publication.';
COMMENT ON COLUMN bigname_phase.project_address_listing.unhealed_labelhashes IS
    'This value is the label hashes display_name still spells as [labelhash], in name order, empty when the name is fully spelled; the family step spells again the rows that hold a label a preimage observation names.';
CREATE INDEX IF NOT EXISTS project_address_listing_name_idx
    ON bigname_phase.project_address_listing (address, display_key, logical_name_id, resource_id)
    INCLUDE (token_holder, effective_controller, role_holder, registration_lead)
    WHERE listed;
CREATE INDEX IF NOT EXISTS project_address_listing_expires_idx
    ON bigname_phase.project_address_listing (address, expires_key, logical_name_id, resource_id)
    INCLUDE (token_holder, effective_controller, role_holder, registration_lead)
    WHERE listed;
CREATE INDEX IF NOT EXISTS project_address_listing_registered_idx
    ON bigname_phase.project_address_listing
        (address, registered_key, logical_name_id, resource_id)
    INCLUDE (token_holder, effective_controller, role_holder, registration_lead)
    WHERE listed;
CREATE INDEX IF NOT EXISTS project_address_listing_created_idx
    ON bigname_phase.project_address_listing (address, created_key, logical_name_id, resource_id)
    INCLUDE (token_holder, effective_controller, role_holder, registration_lead)
    WHERE listed;
CREATE INDEX IF NOT EXISTS project_address_listing_q_idx
    ON bigname_phase.project_address_listing
        (address, left(normalized_name, 100), display_key, logical_name_id, resource_id)
    WHERE listed;
CREATE INDEX IF NOT EXISTS project_address_listing_parent_idx
    ON bigname_phase.project_address_listing
        (address, md5(parent_normalized_name), display_key, logical_name_id, resource_id)
    WHERE listed;
CREATE INDEX IF NOT EXISTS project_address_listing_former_owner_idx
    ON bigname_phase.project_address_listing
        (address, former_expires_key, namespace,
         (CASE WHEN octet_length(normalized_name) <= 2000 THEN normalized_name
               ELSE left(normalized_name, 500) END),
         namehash)
    WHERE former_owner;
CREATE INDEX IF NOT EXISTS project_address_listing_name_work_idx
    ON bigname_phase.project_address_listing (chain_id, logical_name_id);
CREATE INDEX IF NOT EXISTS project_address_listing_resource_idx
    ON bigname_phase.project_address_listing (chain_id, resource_id, address);
CREATE INDEX IF NOT EXISTS project_address_listing_registry_parent_idx
    ON bigname_phase.project_address_listing (chain_id, registry_parent_logical_name_id)
    WHERE registry_child;
CREATE INDEX IF NOT EXISTS project_address_listing_unhealed_idx
    ON bigname_phase.project_address_listing USING gin (unhealed_labelhashes)
    WHERE unhealed_labelhashes <> '{}';
COMMENT ON INDEX bigname_phase.project_address_listing_q_idx IS
    'This index serves the q prefix filter under the name sort. It keys the first 100 characters of normalized_name, so every key fits a btree row however long the name; a reader matches q on that cut as a range and checks the full normalized_name against the whole q, so a q longer than 100 characters filters inside the range of its first 100.';
COMMENT ON INDEX bigname_phase.project_address_listing_parent_idx IS
    'This index serves the parent filter under the name sort. It keys md5(parent_normalized_name), an equality key of bounded width, because the row carries no parent node and the route matches the parent as normalized text; a reader checks parent_normalized_name itself.';
COMMENT ON INDEX bigname_phase.project_address_listing_former_owner_idx IS
    'This index serves the former-owner page: expiry, namespace, name, namehash. The name key is normalized_name cut as display_key is cut, to its first 500 characters when it is longer than 2000 bytes, so every key fits a btree row. Two names with the same expiry and namespace whose cut keys are equal, which needs the same first 500 characters, order by namehash.';
COMMENT ON TABLE bigname_phase.project_child_preimage_watermark IS
    'Project-owned watermarks of families F17 and F18: per chain and other chain, the highest block of the other chain, at or below its finalized (else safe) block, whose preimage observations the chain''s family step has read, so its stored child names and address listing names heal from labels first observed there. The family step advances it in its own block transaction and journals it for undo; a reset clears it, and a rebuild''s composition pass starts it again.';
COMMENT ON COLUMN bigname_phase.project_child_preimage_watermark.chain_id IS
    'This value is the chain whose child listing and address listing the watermark heals.';
END
$migration$;
