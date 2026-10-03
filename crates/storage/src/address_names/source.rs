//! Where an address-names or resolves-to page reads its rows from: the rows the owned key
//! families composed at read (`families::records::address_names` and
//! `families::records::resolves_to`), bound as JSON record sets. The page statements group,
//! dedupe, filter, sort, page and count over those record sets.
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};

use super::{
    ADDRESS_NAMES_PUBLICATION_READ_FILTER, DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS,
    DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
};

/// The columns of a composed address-name row set. `registry_child` marks a surface-less ENSv1
/// registry child's row (`families::records::registry_children`), which has no surface binding
/// and carries its `served_owner`, `served_authority` and `served_lifecycle_shadow`; other rows
/// leave all four null.
const ADDRESS_NAMES_COLUMNS: &str = "anc(address text, logical_name_id text, relation text,
    namespace text, raw_name text, normalized_name text, namehash text, surface_binding_id uuid, resource_id uuid,
    token_lineage_id uuid, binding_kind text, support_status text, unsupported_reason text,
    provenance jsonb, chain_positions jsonb, canonicality_summary jsonb, manifest_version bigint,
    last_recomputed_at timestamptz, registry_child boolean, served_owner text,
    served_authority text, served_lifecycle_shadow boolean)";

/// The columns of a composed address-record row set.
const ADDRESS_RECORDS_COLUMNS: &str = "arc(address text, coin_type text, logical_name_id text,
    namespace text, raw_name text, normalized_name text, namehash text, surface_binding_id uuid, resource_id uuid,
    record_resource_id uuid, binding_kind text, record_key text, support_status text,
    unsupported_reason text, provenance jsonb, chain_positions jsonb,
    canonicality_summary jsonb, manifest_version bigint, last_recomputed_at timestamptz)";

/// The source of one page's rows.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RowSource<'a> {
    /// Composed rows: `rows` the relation rows, and `names` the composed name rows
    /// (`logical_name_id`, `declared_summary`, `provenance`) the authority and migration filters
    /// and the timestamp sorts read. With `parent`, a normalized name, only the relation rows
    /// whose name is exactly one label below it.
    Composed {
        rows: &'a Value,
        names: &'a Value,
        parent: Option<&'a str>,
    },
}

impl<'a> RowSource<'a> {
    /// `WITH` and the `composed_names` CTE the name reads use.
    pub(super) fn push_with(self, builder: &mut QueryBuilder<'a, Postgres>) {
        builder.push("\n        WITH ");
        let Self::Composed { names, .. } = self;
        {
            builder.push("composed_names AS (SELECT * FROM JSONB_TO_RECORDSET(");
            builder.push_bind(names);
            builder.push(
                ") AS nc(logical_name_id text, declared_summary jsonb, provenance jsonb)),\n        ",
            );
        }
    }

    /// The relation holding the name rows, `composed_names`.
    pub(super) fn names(self) -> &'static str {
        match self {
            Self::Composed { .. } => "composed_names",
        }
    }

    /// The address-name relation rows, aliased `anc`.
    pub(super) fn push_address_names(self, builder: &mut QueryBuilder<'a, Postgres>) {
        self.push_rows(builder, ADDRESS_NAMES_COLUMNS);
    }

    /// The `served_rows` CTE definition, followed by a comma: the address-name rows the read
    /// filter admits, a row with a surface binding through its identity rows and a surface-less
    /// registry child's through its publication alone.
    pub(super) fn push_served_address_names(self, builder: &mut QueryBuilder<'a, Postgres>) {
        builder.push("served_rows AS (SELECT anc.* FROM ");
        self.push_address_names(builder);
        builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS);
        builder.push(" WHERE anc.registry_child IS NOT TRUE");
        builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER);
        builder.push(" UNION ALL SELECT anc.* FROM ");
        self.push_address_names(builder);
        builder.push(" WHERE anc.registry_child");
        builder.push(ADDRESS_NAMES_PUBLICATION_READ_FILTER);
        builder.push("),\n        ");
    }

    /// The address-record rows, aliased `arc`.
    pub(super) fn push_address_records(self, builder: &mut QueryBuilder<'a, Postgres>) {
        self.push_rows(builder, ADDRESS_RECORDS_COLUMNS);
    }

    fn push_rows(self, builder: &mut QueryBuilder<'a, Postgres>, columns: &'static str) {
        let Self::Composed { rows, parent, .. } = self;
        let Some(parent) = parent else {
            builder.push("JSONB_TO_RECORDSET(");
            builder.push_bind(rows);
            builder.push(") AS ");
            builder.push(columns);
            return;
        };
        let alias = &columns[..columns.find('(').unwrap_or(columns.len())];
        builder.push("(SELECT * FROM JSONB_TO_RECORDSET(");
        builder.push_bind(rows);
        builder.push(") AS ");
        builder.push(columns);
        builder.push(" WHERE TRUE");
        crate::name_current::push_parent_predicate(
            builder,
            &format!("{alias}.normalized_name"),
            parent,
        );
        builder.push(format!(") AS {alias}"));
    }
}
