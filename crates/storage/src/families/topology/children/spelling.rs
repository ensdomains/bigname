//! How the children relation spells a name: the parent's stored spelling and its unhealed
//! labels ([`Relation::parent_spelling`]), and the child's display name and the labels it leaves
//! as `[labelhash]` ([`CHILD_DISPLAY_NAME`], [`CHILD_UNHEALED_LABELHASHES`]), from the same
//! expressions, so the listing family's stored unhealed labels are those of its stored name.
use super::Relation;
use crate::families::name::rendered::rendered_name_sql;

impl Relation<'_> {
    /// The parent's spelling: [`rendered_name_sql`] under the same preimage rule. A stored
    /// parent with no raw bytes is spelled by [`Self::parent_spelling`].
    pub(super) fn parent_name(self, surface: &str) -> String {
        match self {
            Self::Served => rendered_name_sql(surface),
            Self::Stored(_) => format!("COALESCE({surface}.raw_name, spelling.name)"),
        }
    }

    /// The labels the parent's stored spelling leaves as `[labelhash]`, in order: none for a
    /// parent with raw bytes, and none for the served name, which stores nothing.
    pub(super) fn parent_unhealed(self, surface: &str) -> String {
        match self {
            Self::Served => "'{}'::text[]".to_owned(),
            Self::Stored(_) => format!(
                "CASE WHEN {surface}.raw_name IS NULL THEN spelling.unhealed
                      ELSE '{{}}'::text[] END"
            ),
        }
    }

    /// The join that spells a stored parent from its label hashes: each label from its
    /// chain-observed preimage when that is decoded and normalized, else `[labelhash]`. The
    /// spelling and its unhealed labels come from the same `known` test of each label.
    pub(super) fn parent_spelling(self, surface: &str) -> String {
        match self {
            Self::Served => String::new(),
            Self::Stored(_) => stored_spelling(surface, "spelling"),
        }
    }
}

/// The join, aliased `alias`, that spells the surface `surface` from its label hashes: each label
/// from its chain-observed preimage when that is decoded and normalized, else `[labelhash]`, as
/// `name`, with the labels it leaves as `[labelhash]` as `unhealed`, from the same `known` test of
/// each label. A null surface spells a null name and no unhealed label. The child listing spells a
/// stored parent with it, and the address listing family a stored name with no raw bytes.
pub(crate) fn stored_spelling(surface: &str, alias: &str) -> String {
    format!(
        "
            LEFT JOIN LATERAL (
                SELECT string_agg(CASE WHEN label.known THEN label.decoded_label
                                       ELSE '[' || substring(label.labelhash FROM 3) || ']' END,
                                  '.' ORDER BY label.position) AS name,
                       COALESCE(array_agg(label.labelhash ORDER BY label.position)
                                    FILTER (WHERE NOT label.known),
                                '{{}}'::text[]) AS unhealed
                FROM (
                    SELECT path.position, lower(path.labelhash) AS labelhash,
                           preimage.decoded_label,
                           COALESCE(preimage.decoded_label IS NOT NULL
                                    AND preimage.normalized_under_version, FALSE) AS known
                    FROM unnest({surface}.labelhashes) WITH ORDINALITY AS path(labelhash, position)
                    LEFT JOIN bigname_phase.label_preimages preimage
                      ON preimage.labelhash = lower(path.labelhash)
                     AND preimage.source_kind <> 'ens_rainbow_import'
                ) label
            ) {alias} ON TRUE"
    )
}

/// The child's raw name from its label preimage: null for no preimage and for a decoded label
/// that fails normalization; the label alone under the root.
pub(super) fn label_raw_name(under_root: &str) -> String {
    format!(
        "CASE WHEN preimage.raw_label IS NULL THEN NULL
              WHEN preimage.decoded_label IS NOT NULL
                   AND NOT preimage.normalized_under_version THEN NULL
              WHEN {under_root} THEN preimage.raw_label
              ELSE preimage.raw_label || decode('2e', 'hex') || convert_to(parent.raw_name, 'UTF8')
         END"
    )
}

pub(super) fn label_decoded_name(under_root: &str) -> String {
    format!(
        "CASE WHEN preimage.decoded_label IS NULL THEN NULL
              WHEN NOT preimage.normalized_under_version THEN NULL
              WHEN {under_root} THEN preimage.decoded_label
              ELSE preimage.decoded_label || '.' || parent.raw_name
         END"
    )
}

/// The child's name from its own label: decoded, else the escaped raw bytes, else null for a
/// label it cannot spell. A child whose surface at the clock stores no raw bytes has a name row,
/// which never serves the escaped form (`name::rendered`), so this one skips it too and the two
/// routes name the child alike.
macro_rules! child_spelled_name {
    () => {
        "COALESCE(
    selected.decoded_name,
    CASE WHEN child_surface.logical_name_id IS NOT NULL AND child_surface.raw_name IS NULL
              AND child_surface.block_number <= clock.block_number THEN NULL
         ELSE encode(selected.raw_name, 'escape') END)"
    };
}

/// The served child name: `child_spelled_name`, else the labelhash placeholder under the
/// parent's spelling.
pub(in crate::families::topology) const CHILD_DISPLAY_NAME: &str = concat!(
    "COALESCE(",
    child_spelled_name!(),
    ",
    '[' || substring(lower(selected.labelhash) FROM 3) || '].' || parent.raw_name
)"
);

/// The labels [`CHILD_DISPLAY_NAME`] spells as `[labelhash]`: the child's own when it takes the
/// placeholder, then the parent's (`parent.unhealed_labelhashes`), which its name ends in. Empty
/// when the name is fully spelled. The listing family stores it so a preimage observation derives
/// again only the rows it can spell.
pub(super) const CHILD_UNHEALED_LABELHASHES: &str = concat!(
    "CASE WHEN ",
    child_spelled_name!(),
    " IS NULL AND selected.labelhash IS NOT NULL
      THEN ARRAY[lower(selected.labelhash)] ELSE '{}'::text[] END
    || parent.unhealed_labelhashes"
);
