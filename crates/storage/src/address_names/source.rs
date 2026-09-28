//! Where an address-names or resolves-to page reads its rows from: the served tables
//! (`address_names_current`, `address_records_current`, `name_current`), or rows the owned key
//! families composed at read (TYR-36 step 7b, `families::records::address_names` and
//! `families::records::resolves_to`), bound as JSON record sets in the same shapes. Both run
//! through the same statements, so the grouping, dedupe, filters, sorts, cursors and totals are
//! one piece of SQL whichever side the rows come from; only the `FROM` of the relation rows and
//! the name rows the filters and sorts read change.
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};

/// The columns of a composed `address_names_current` row set.
const ADDRESS_NAMES_COLUMNS: &str = "anc(address text, logical_name_id text, relation text,
    namespace text, raw_name text, namehash text, surface_binding_id uuid, resource_id uuid,
    token_lineage_id uuid, binding_kind text, support_status text, unsupported_reason text,
    provenance jsonb, chain_positions jsonb, canonicality_summary jsonb, manifest_version bigint,
    last_recomputed_at timestamptz)";

/// The columns of a composed `address_records_current` row set.
const ADDRESS_RECORDS_COLUMNS: &str = "arc(address text, coin_type text, logical_name_id text,
    namespace text, raw_name text, namehash text, surface_binding_id uuid, resource_id uuid,
    record_resource_id uuid, binding_kind text, record_key text, support_status text,
    unsupported_reason text, provenance jsonb, chain_positions jsonb,
    canonicality_summary jsonb, manifest_version bigint, last_recomputed_at timestamptz)";

/// The source of one page's rows.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RowSource<'a> {
    /// The served tables.
    Served,
    /// Composed rows: `rows` in the served relation table's shape, and `names` the composed name
    /// rows (`logical_name_id`, `declared_summary`, `provenance`) the authority and migration
    /// filters and the timestamp sorts read in place of `name_current`.
    Composed { rows: &'a Value, names: &'a Value },
}

impl<'a> RowSource<'a> {
    /// `WITH`, and for composed rows the `composed_names` CTE the name reads use.
    pub(super) fn push_with(self, builder: &mut QueryBuilder<'a, Postgres>) {
        builder.push("\n        WITH ");
        if let Self::Composed { names, .. } = self {
            builder.push("composed_names AS (SELECT * FROM JSONB_TO_RECORDSET(");
            builder.push_bind(names);
            builder.push(
                ") AS nc(logical_name_id text, declared_summary jsonb, provenance jsonb)),\n        ",
            );
        }
    }

    /// The relation holding the name rows: `name_current`, or the composed `composed_names`.
    pub(super) fn names(self) -> &'static str {
        match self {
            Self::Served => "bigname_phase.name_current",
            Self::Composed { .. } => "composed_names",
        }
    }

    /// The `address_names_current` rows, aliased `anc`.
    pub(super) fn push_address_names(self, builder: &mut QueryBuilder<'a, Postgres>) {
        self.push_rows(
            builder,
            "bigname_phase.address_names_current anc",
            ADDRESS_NAMES_COLUMNS,
        );
    }

    /// The `address_records_current` rows, aliased `arc`.
    pub(super) fn push_address_records(self, builder: &mut QueryBuilder<'a, Postgres>) {
        self.push_rows(
            builder,
            "bigname_phase.address_records_current arc",
            ADDRESS_RECORDS_COLUMNS,
        );
    }

    fn push_rows(
        self,
        builder: &mut QueryBuilder<'a, Postgres>,
        served: &str,
        columns: &'static str,
    ) {
        match self {
            Self::Served => {
                builder.push(served);
            }
            Self::Composed { rows, .. } => {
                builder.push("JSONB_TO_RECORDSET(");
                builder.push_bind(rows);
                builder.push(") AS ");
                builder.push(columns);
            }
        }
    }
}
