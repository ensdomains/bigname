//! The address listing refresh's statements (`address_listing.rs`).

/// The refresh's temporary tables, emptied for this step, one statement each.
pub(super) const PREPARE: [&str; 5] = [
    "/* project:families.derived.address_listing_prepare_fresh */
     CREATE TEMP TABLE IF NOT EXISTS bigname_address_listing_fresh
         (LIKE project_address_listing) ON COMMIT DROP",
    "/* project:families.derived.address_listing_prepare_scope */
     CREATE TEMP TABLE IF NOT EXISTS bigname_address_listing_scope
         (kind smallint NOT NULL, id text COLLATE \"C\" NOT NULL) ON COMMIT DROP",
    "/* project:families.derived.address_listing_prepare_change */
     CREATE TEMP TABLE IF NOT EXISTS bigname_address_listing_change
         (address text NOT NULL, logical_name_id text COLLATE \"C\" NOT NULL,
          before_image jsonb, after_image jsonb) ON COMMIT DROP",
    "/* project:families.derived.address_listing_prepare_group */
     CREATE TEMP TABLE IF NOT EXISTS bigname_address_listing_group
         (address text NOT NULL, resource_id uuid) ON COMMIT DROP",
    "/* project:families.derived.address_listing_prepare_truncate */
     TRUNCATE pg_temp.bigname_address_listing_fresh, pg_temp.bigname_address_listing_scope,
              pg_temp.bigname_address_listing_change, pg_temp.bigname_address_listing_group",
];

/// Scope kind of a name derived whole: every stored row of the id is replaced by its fresh rows.
pub(super) const NAME: i16 = 0;
/// Scope kind of a registry child derived again: the id's stored registry-child rows only.
pub(super) const REGISTRY_CHILD: i16 = 1;
/// Scope kind of a parent whose registry children are derived whole.
pub(super) const REGISTRY_PARENT: i16 = 2;
/// Not a scope: a staged name that composed a name row, so it is no registry child.
pub(super) const COMPOSED: i16 = 3;

pub(super) const SCOPE: &str = "/* project:families.derived.address_listing_scope */
    INSERT INTO pg_temp.bigname_address_listing_scope (kind, id)
    SELECT $1, id FROM unnest($2::text[]) scoped (id)";

/// The names the block's lookup refresh composed (`lookup/work.rs`: the summary work list, the
/// address fold and controller candidate keys, the named resource pointers, the resolution path
/// work and the admission sweep) and the ids its index refresh touched, `$1`, that the summary
/// refresh did not stage.
pub(super) const EXTRA: &str = "/* project:families.derived.address_listing_extra */
    SELECT DISTINCT extra.id
    FROM (SELECT work.logical_name_id AS id FROM pg_temp.bigname_lookup_name_work work
          UNION ALL SELECT touched.id FROM unnest($1::text[]) touched (id)) extra
    WHERE extra.id IS NOT NULL AND extra.id <> ''
      AND NOT EXISTS (SELECT 1 FROM pg_temp.bigname_address_listing_scope scope
                      WHERE scope.kind = 0 AND scope.id = extra.id)
    ORDER BY 1";

/// The staged names that composed no name row: each can be a surface-less registry child, whose
/// rows the children relation derives.
pub(super) const UNCOMPOSED: &str = "/* project:families.derived.address_listing_uncomposed */
    SELECT DISTINCT scope.id FROM pg_temp.bigname_address_listing_scope scope
    WHERE scope.kind = 0 AND scope.id LIKE '%:%'
      AND NOT EXISTS (SELECT 1 FROM pg_temp.bigname_address_listing_scope composed
                      WHERE composed.kind = 3 AND composed.id = scope.id)
    ORDER BY 1";

/// The labels chain `$1` observed in blocks `$3` (exclusive) to `$2`: the labels of each observed
/// name's surface, and the label hash an observation names directly, as the child listing's work
/// list reads them (`children/work.rs`). Each state is probed on its own so the projection index
/// bounds the read to the blocks.
pub(super) const OBSERVED_LABELS: &str =
    "/* project:families.derived.address_listing_observed_labels */
    WITH revealed AS MATERIALIZED (
        SELECT revealed_event.logical_name_id, revealed_event.labelhash
        FROM unnest('{canonical,safe,finalized}'::canonicality_state[]) AS state(canonicality_state)
        CROSS JOIN LATERAL (
            SELECT observed.logical_name_id, observed.after_state ->> 'labelhash' AS labelhash
            FROM normalized_events observed
            WHERE observed.event_kind = 'PreimageObserved'
              AND observed.canonicality_state = state.canonicality_state
              AND observed.chain_id = $1
              AND observed.block_number > $3 AND observed.block_number <= $2
            OFFSET 0
        ) revealed_event
    )
    SELECT COALESCE(array_agg(DISTINCT label.labelhash), '{}'::text[])
    FROM (
        SELECT lower(surface_label.labelhash) AS labelhash
        FROM revealed
        CROSS JOIN LATERAL (
            SELECT surface.labelhashes FROM name_surfaces surface
            WHERE surface.logical_name_id = revealed.logical_name_id
            OFFSET 0
        ) surface
        CROSS JOIN LATERAL unnest(surface.labelhashes) surface_label (labelhash)
        UNION ALL
        SELECT lower(revealed.labelhash) FROM revealed WHERE revealed.labelhash IS NOT NULL
    ) label";

/// The registry-child rows of chain `$1` whose stored name still spells one of the labels `$2` as
/// `[labelhash]` (`project_address_listing_unhealed_idx`): the children relation derives them
/// again. The other holders are spelled again from their surface (`RESPELL`).
pub(super) const REGISTRY_HOLDERS: &str =
    "/* project:families.derived.address_listing_registry_holders */
    SELECT DISTINCT listing.logical_name_id FROM project_address_listing listing
    WHERE listing.chain_id = $1 AND listing.unhealed_labelhashes <> '{}'
      AND listing.unhealed_labelhashes && $2::text[] AND listing.registry_child
    ORDER BY 1";

/// The rows the scope's stored rows differ in from the fresh ones, keyed by (address, name), with
/// each side's image (null for none). The fresh image keeps the stored `registration_lead`, which
/// the lead step decides after the write: the comparison leaves it out. A row the children
/// relation derived under two arms has one fresh row. Each stored row is probed by its key.
pub(super) const CHANGES: &str = "/* project:families.derived.address_listing_changes */
    INSERT INTO pg_temp.bigname_address_listing_change
    WITH fresh AS (
        SELECT DISTINCT ON (fresh.address, fresh.logical_name_id) fresh.*
        FROM pg_temp.bigname_address_listing_fresh fresh
        ORDER BY fresh.address, fresh.logical_name_id, fresh.registry_parent_logical_name_id
    ), keys AS (
        SELECT listing.address, listing.logical_name_id
        FROM pg_temp.bigname_address_listing_scope scope
        CROSS JOIN LATERAL (
            SELECT listing.address, listing.logical_name_id, listing.registry_child
            FROM project_address_listing listing
            WHERE listing.chain_id = $1 AND listing.logical_name_id = scope.id
            OFFSET 0
        ) listing
        WHERE scope.kind = 0 OR (scope.kind = 1 AND listing.registry_child)
        UNION
        SELECT listing.address, listing.logical_name_id
        FROM pg_temp.bigname_address_listing_scope scope
        CROSS JOIN LATERAL (
            SELECT listing.address, listing.logical_name_id
            FROM project_address_listing listing
            WHERE listing.chain_id = $1 AND listing.registry_parent_logical_name_id = scope.id
              AND listing.registry_child
            OFFSET 0
        ) listing
        WHERE scope.kind = 2
        UNION
        SELECT fresh.address, fresh.logical_name_id FROM fresh
    ), stored AS (
        SELECT listing.*
        FROM keys
        CROSS JOIN LATERAL (
            SELECT listing.* FROM project_address_listing listing
            WHERE listing.chain_id = $1 AND listing.address = keys.address
              AND listing.logical_name_id = keys.logical_name_id
            OFFSET 0
        ) listing
    )
    SELECT keys.address, keys.logical_name_id, to_jsonb(stored),
           to_jsonb(fresh) || jsonb_build_object('registration_lead',
               fresh.listed AND COALESCE(stored.registration_lead, FALSE))
    FROM keys
    LEFT JOIN stored
      ON stored.address = keys.address AND stored.logical_name_id = keys.logical_name_id
    LEFT JOIN fresh
      ON fresh.address = keys.address AND fresh.logical_name_id = keys.logical_name_id
    WHERE (to_jsonb(stored) - 'registration_lead')
          IS DISTINCT FROM (to_jsonb(fresh) - 'registration_lead')";

/// An undo's derivation keeps only the rows it spells further: the same row with fewer labels left
/// as `[labelhash]` and nothing else changed but the name and the keys spelled from it.
pub(super) const HEALS_ONLY: &str = "/* project:families.derived.address_listing_heals_only */
    DELETE FROM pg_temp.bigname_address_listing_change change
    WHERE NOT COALESCE(
        (change.before_image - '{display_key,display_name,normalized_name,
                                 parent_normalized_name,unhealed_labelhashes}'::text[])
            = (change.after_image - '{display_key,display_name,normalized_name,
                                      parent_normalized_name,unhealed_labelhashes}'::text[])
        AND (change.before_image -> 'unhealed_labelhashes')
            @> (change.after_image -> 'unhealed_labelhashes')
        AND (change.before_image -> 'unhealed_labelhashes')
            <> (change.after_image -> 'unhealed_labelhashes'),
        FALSE)";

/// The journal row of each change, keyed as `store::key_text` keys it. A row an earlier step of
/// the same block changed keeps that step's image, the row as it stood before the block.
pub(super) const JOURNAL: &str = "/* project:families.derived.address_listing_journal */
    INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key, before_image)
    SELECT $1, $2, $3, 'project_address_listing',
           '[' || to_json($1::text)::text || ',' || to_json(change.address)::text || ','
               || to_json(change.logical_name_id::text)::text || ']',
           change.before_image
    FROM pg_temp.bigname_address_listing_change change
    ON CONFLICT (chain_id, block_number, family, key) DO NOTHING";

pub(super) const DELETE: &str = "/* project:families.derived.address_listing_delete */
    DELETE FROM project_address_listing listing
    USING pg_temp.bigname_address_listing_change change
    WHERE listing.chain_id = $1 AND listing.address = change.address
      AND listing.logical_name_id = change.logical_name_id";

pub(super) const INSERT: &str = "/* project:families.derived.address_listing_insert */
    INSERT INTO project_address_listing
    SELECT (jsonb_populate_record(NULL::project_address_listing, change.after_image)).*
    FROM pg_temp.bigname_address_listing_change change
    WHERE change.after_image IS NOT NULL";

/// The (address, resource) groups the changes touched, before and after, for the lead step.
pub(super) const GROUPS: &str = "/* project:families.derived.address_listing_groups */
    INSERT INTO pg_temp.bigname_address_listing_group (address, resource_id)
    SELECT DISTINCT image.row ->> 'address', (image.row ->> 'resource_id')::uuid
    FROM pg_temp.bigname_address_listing_change change
    CROSS JOIN LATERAL (VALUES (change.before_image), (change.after_image)) image (row)
    WHERE image.row ->> 'resource_id' IS NOT NULL";

pub(super) const CLEAR_CHANGES: &str = "/* project:families.derived.address_listing_clear */
    TRUNCATE pg_temp.bigname_address_listing_change";

/// The stored rows of chain `$1` that hold one of the labels `$2` and are spelled from their
/// surface, as `target` rows for `RESPELL`.
pub(super) const LABEL_TARGET: &str = "/* project:families.derived.address_listing_label_target */
    SELECT listing.* FROM project_address_listing listing
    WHERE listing.chain_id = $1 AND listing.unhealed_labelhashes <> '{}'
      AND listing.unhealed_labelhashes && $2::text[] AND NOT listing.registry_child";

/// The stored rows of chain `$1` keyed by `$2` (addresses) and `$3` (names) that still spell a
/// label as `[labelhash]` and are spelled from their surface, as `target` rows for `RESPELL`.
pub(super) const KEY_TARGET: &str = "/* project:families.derived.address_listing_key_target */
    SELECT listing.*
    FROM unnest($2::text[], $3::text[]) keyed (address, logical_name_id)
    CROSS JOIN LATERAL (
        SELECT listing.* FROM project_address_listing listing
        WHERE listing.chain_id = $1 AND listing.address = keyed.address
          AND listing.logical_name_id = keyed.logical_name_id
        OFFSET 0
    ) listing
    WHERE listing.unhealed_labelhashes <> '{}' AND NOT listing.registry_child";

/// The rows the lead step changes: in each touched (address, resource) group, the smallest
/// (display_key, logical_name_id) listed row is the lead and no other row is. A group's rows are
/// probed by the resource. A row with no resource is never listed, so it leads no group.
pub(super) const LEADS: &str = "/* project:families.derived.address_listing_leads */
    INSERT INTO pg_temp.bigname_address_listing_change
    WITH groups AS (
        SELECT DISTINCT address, resource_id FROM pg_temp.bigname_address_listing_group
    ), members AS (
        SELECT listing.*
        FROM groups
        CROSS JOIN LATERAL (
            SELECT listing.* FROM project_address_listing listing
            WHERE listing.chain_id = $1 AND listing.resource_id = groups.resource_id
              AND listing.address = groups.address
            OFFSET 0
        ) listing
    ), ranked AS (
        SELECT members.*,
               members.listed AND row_number() OVER (
                   PARTITION BY members.address, members.resource_id
                   ORDER BY members.listed DESC, members.display_key, members.logical_name_id
               ) = 1 AS lead
        FROM members
    )
    SELECT ranked.address, ranked.logical_name_id,
           to_jsonb(ranked) - 'lead',
           (to_jsonb(ranked) - 'lead') || jsonb_build_object('registration_lead', ranked.lead)
    FROM ranked
    WHERE ranked.lead <> ranked.registration_lead";
