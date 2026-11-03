-- The stored event counts, family F20: project_event_count, one row per chain, anchor kind,
-- anchor, namespace and event kind with the number of product-visible events at or below the
-- family publication that the anchor's event read admits.
--
-- The new table starts empty and needs no reset. The release that adds it rotates the projection
-- content hash, so the runner refuses a normal Project or Live start until a full-range Project
-- redo, whose composition pass counts every catalogue window; until then the API serves no family
-- publication of the new build.
--
-- An empty schema-migration database has no phase baseline yet, so this schema-migration is a
-- no-op there and phase-runner init-schema installs the same table.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN
    RETURN;
END IF;
CREATE TABLE IF NOT EXISTS bigname_phase.project_event_count (
    chain_id text NOT NULL,
    anchor_kind smallint NOT NULL,
    anchor text NOT NULL,
    namespace text NOT NULL,
    event_kind text NOT NULL,
    event_count bigint NOT NULL,
    PRIMARY KEY (chain_id, anchor_kind, anchor, namespace, event_kind),
    CHECK (anchor_kind BETWEEN 0 AND 2),
    CHECK (event_count > 0)
);
COMMENT ON TABLE bigname_phase.project_event_count IS
    'Project-owned stored event counts of family F20: per chain, anchor, namespace and event kind, the number of product-visible normalized events with a block at or below the family publication that the anchor''s event read admits. Each block adds its range''s counts in the family transaction and journals them for undo; a count of zero has no row.';
COMMENT ON COLUMN bigname_phase.project_event_count.chain_id IS
    'This value is the chain every counted event lies on.';
COMMENT ON COLUMN bigname_phase.project_event_count.anchor_kind IS
    'This value is how the anchor reads the event: 0 the anchor emitted it, 1 a ResolverChanged another contract emitted sets the pointer to the anchor, 2 a ResolverChanged another contract emitted clears the pointer from the anchor to another resolver. A resolver''s events are the sum of its three kinds.';
COMMENT ON COLUMN bigname_phase.project_event_count.anchor IS
    'This value is the lower-case contract address the anchor kind names.';
COMMENT ON COLUMN bigname_phase.project_event_count.namespace IS
    'This value is the namespace of the counted events.';
COMMENT ON COLUMN bigname_phase.project_event_count.event_kind IS
    'This value is the event kind of the counted events, product kind or not.';
COMMENT ON COLUMN bigname_phase.project_event_count.event_count IS
    'This value is the number of events the key covers.';
END
$migration$;
