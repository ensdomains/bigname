use super::generated_filter_ops::StringFilter;

pub(crate) fn owner_filter_is_active(filter: &StringFilter) -> bool {
    filter != &StringFilter::default()
}

pub(crate) fn bounded_effective_owner_membership(filter: &StringFilter) -> bool {
    filter.eq.as_ref().is_some_and(Option::is_some) || filter.in_values.is_some()
}
