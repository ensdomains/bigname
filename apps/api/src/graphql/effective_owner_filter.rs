use sqlx::{Postgres, QueryBuilder};

use bigname_storage::{
    DEFAULT_ADDRESS_NAMES_MEMBERSHIP_JOINS, DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER,
    NameCurrentAddressFilter,
};

use super::generated_filter_ops::{GeneratedDomainFilter, StringFilter};

const TARGETS: &str = r#"JSONB_AGG(
    owner_witness.chain_positions || JSONB_BUILD_OBJECT(
        'chain_id', owner_witness.provenance ->> 'chain_id'
    ) ORDER BY owner_witness.address, owner_witness.relation
)"#;

pub(crate) fn owner_filter_is_active(filter: &StringFilter) -> bool {
    filter != &StringFilter::default()
}

pub(crate) fn bounded_effective_owner_membership(filter: &StringFilter) -> bool {
    filter.eq.as_ref().is_some_and(Option::is_some) || filter.in_values.is_some()
}

pub(crate) fn active_owner_filter(filter: Option<&GeneratedDomainFilter>) -> Option<&StringFilter> {
    filter
        .map(|filter| &filter.owner)
        .filter(|filter| owner_filter_is_active(filter))
}

pub(crate) fn legacy_address_filter<'a>(
    owner_filter: Option<&StringFilter>,
    address_filter: Option<&'a NameCurrentAddressFilter>,
) -> Option<&'a NameCurrentAddressFilter> {
    owner_filter.is_none().then_some(address_filter).flatten()
}

pub(crate) fn push_effective_owner_cte_predicates<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: Option<&'a StringFilter>,
    chain_ids: Option<&'a [String]>,
) {
    let Some(filter) = filter else { return };
    builder.push(
        "eligible_effective_owner AS NOT MATERIALIZED (\
         SELECT anc.logical_name_id, anc.address, anc.relation, \
                anc.chain_positions, anc.provenance \
         FROM bigname_phase.address_names_current anc ",
    );
    builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_JOINS);
    builder.push(
        " WHERE anc.support_status = 'supported' \
           AND anc.relation = 'effective_controller'",
    );
    if let Some(chain_ids) = chain_ids {
        builder
            .push(" AND anc.provenance ->> 'chain_id' = ANY(")
            .push_bind(chain_ids)
            .push(")");
    }
    builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER);
    builder.push("), ");
    if bounded_effective_owner_membership(filter) {
        builder
            .push("effective_owner_membership AS (SELECT owner_witness.logical_name_id, ")
            .push(TARGETS)
            .push(" AS membership_targets FROM eligible_effective_owner owner_witness WHERE TRUE");
        push_owner_predicates(builder, filter);
        builder.push(" GROUP BY owner_witness.logical_name_id), ");
    }
}

pub(crate) fn push_effective_owner_membership_targets(
    builder: &mut QueryBuilder<'_, Postgres>,
    filter: Option<&StringFilter>,
    legacy_address: bool,
) {
    builder.push(if filter.is_some_and(bounded_effective_owner_membership) {
        " effective_owner_membership.membership_targets"
    } else if filter.is_some() {
        " effective_owner_probe.membership_targets"
    } else if legacy_address {
        " address_membership.membership_targets"
    } else {
        " '[]'::JSONB AS membership_targets"
    });
}

pub(crate) fn push_effective_owner_lateral_join<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: Option<&'a StringFilter>,
) {
    let Some(filter) = filter else { return };
    if bounded_effective_owner_membership(filter) {
        builder.push(
            " JOIN effective_owner_membership \
               ON effective_owner_membership.logical_name_id = nc.logical_name_id",
        );
        return;
    }
    builder.push(" JOIN LATERAL (SELECT ").push(TARGETS).push(
        " AS membership_targets FROM eligible_effective_owner owner_witness \
             WHERE owner_witness.logical_name_id = nc.logical_name_id",
    );
    push_owner_predicates(builder, filter);
    builder.push(" HAVING COUNT(*) > 0) effective_owner_probe ON TRUE");
}

fn push_owner_predicates<'a>(builder: &mut QueryBuilder<'a, Postgres>, filter: &'a StringFilter) {
    if let Some(value) = filter.eq.as_ref() {
        match value {
            Some(value) => push_scalar(builder, "owner_witness.address", " = ", value),
            None => {
                builder.push(" AND FALSE");
            }
        }
    }
    if let Some(value) = filter.not.as_ref() {
        match value {
            Some(value) => push_negative(builder, "rejected_owner.address", " = ", value),
            None => {
                builder.push(" AND FALSE");
            }
        }
    }
    for (value, operator) in [
        (filter.gt.as_ref(), " > "),
        (filter.gte.as_ref(), " >= "),
        (filter.lt.as_ref(), " < "),
        (filter.lte.as_ref(), " <= "),
    ] {
        if let Some(value) = value {
            push_scalar(
                builder,
                "(owner_witness.address COLLATE \"C\")",
                operator,
                value,
            );
        }
    }
    push_membership(builder, filter.in_values.as_deref(), false);
    push_membership(builder, filter.not_in_values.as_deref(), true);
    for (value, nocase, negative, pattern) in [
        (filter.contains.as_deref(), false, false, Pattern::Contains),
        (
            filter.contains_nocase.as_deref(),
            true,
            false,
            Pattern::Contains,
        ),
        (
            filter.not_contains.as_deref(),
            false,
            true,
            Pattern::Contains,
        ),
        (
            filter.not_contains_nocase.as_deref(),
            true,
            true,
            Pattern::Contains,
        ),
        (filter.starts_with.as_deref(), false, false, Pattern::Starts),
        (
            filter.starts_with_nocase.as_deref(),
            true,
            false,
            Pattern::Starts,
        ),
        (
            filter.not_starts_with.as_deref(),
            false,
            true,
            Pattern::Starts,
        ),
        (
            filter.not_starts_with_nocase.as_deref(),
            true,
            true,
            Pattern::Starts,
        ),
        (filter.ends_with.as_deref(), false, false, Pattern::Ends),
        (
            filter.ends_with_nocase.as_deref(),
            true,
            false,
            Pattern::Ends,
        ),
        (filter.not_ends_with.as_deref(), false, true, Pattern::Ends),
        (
            filter.not_ends_with_nocase.as_deref(),
            true,
            true,
            Pattern::Ends,
        ),
    ] {
        if let Some(value) = value {
            push_pattern(builder, pattern.apply(value), nocase, negative);
        }
    }
}

fn push_membership<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    values: Option<&'a [String]>,
    negative: bool,
) {
    let Some(values) = values else { return };
    if values.is_empty() {
        builder.push(" AND FALSE");
    } else if negative {
        push_negative_list(builder, values);
    } else {
        builder
            .push(" AND owner_witness.address = ANY(")
            .push_bind(values)
            .push("::text[])");
    }
}

fn push_scalar<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    operator: &str,
    value: &'a String,
) {
    builder
        .push(" AND ")
        .push(column)
        .push(operator)
        .push_bind(value);
}

fn push_negative<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    operator: &str,
    value: &'a String,
) {
    builder
        .push(
            " AND NOT EXISTS (SELECT 1 FROM eligible_effective_owner rejected_owner \
               WHERE rejected_owner.logical_name_id = owner_witness.logical_name_id AND ",
        )
        .push(column)
        .push(operator)
        .push_bind(value)
        .push(")");
}

fn push_negative_list<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [String]) {
    builder
        .push(
            " AND NOT EXISTS (SELECT 1 FROM eligible_effective_owner rejected_owner \
               WHERE rejected_owner.logical_name_id = owner_witness.logical_name_id \
                 AND rejected_owner.address = ANY(",
        )
        .push_bind(values)
        .push("::text[]))");
}

fn push_pattern<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    pattern: String,
    nocase: bool,
    negative: bool,
) {
    let operator = if nocase { " ILIKE " } else { " LIKE " };
    if negative {
        builder
            .push(
                " AND NOT EXISTS (SELECT 1 FROM eligible_effective_owner rejected_owner \
                   WHERE rejected_owner.logical_name_id = owner_witness.logical_name_id \
                     AND (rejected_owner.address COLLATE \"C\")",
            )
            .push(operator)
            .push_bind(pattern)
            .push(")");
    } else {
        builder
            .push(" AND (owner_witness.address COLLATE \"C\")")
            .push(operator)
            .push_bind(pattern);
    }
}

#[derive(Clone, Copy)]
enum Pattern {
    Contains,
    Starts,
    Ends,
}

impl Pattern {
    fn apply(self, value: &str) -> String {
        match self {
            Self::Contains if value.starts_with('%') || value.ends_with('%') => value.to_owned(),
            Self::Contains => format!("%{value}%"),
            Self::Starts => format!("{value}%"),
            Self::Ends => format!("%{value}"),
        }
    }
}
