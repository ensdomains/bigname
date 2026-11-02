//! A family's declared contracts read by role, address and block. A singleton role names one
//! address. A repeatable role, such as a registry implementation, can name several, each with
//! its own admitted range.
use super::{AddressAdmissionInput, Catalog};

impl Catalog {
    fn role_admissions<'s>(
        &'s self,
        source_family: &str,
        role: &str,
    ) -> impl Iterator<Item = &'s AddressAdmissionInput> {
        self.admissions.for_role(role).filter(move |admission| {
            admission.role.as_deref() == Some(role)
                && admission
                    .source_manifest_id
                    .and_then(|manifest_id| self.source(manifest_id))
                    .is_some_and(|source| source.source_family == source_family)
        })
    }

    /// The distinct addresses the family declares under `role`, in any range. One address
    /// closed and declared again is one address.
    pub(in crate::schema_v2) fn declared_addresses(
        &self,
        source_family: &str,
        role: &str,
    ) -> Vec<String> {
        distinct(self.role_admissions(source_family, role))
    }

    /// The distinct addresses the family declares under `role` with no end block: the live
    /// declarations. A retired range has an end block, so it is not counted.
    pub(in crate::schema_v2) fn live_addresses(
        &self,
        source_family: &str,
        role: &str,
    ) -> Vec<String> {
        distinct(
            self.role_admissions(source_family, role)
                .filter(|admission| admission.active_to_block.is_none()),
        )
    }

    /// The distinct addresses whose declared range under `role` holds `block`.
    pub(in crate::schema_v2) fn declared_addresses_at(
        &self,
        source_family: &str,
        role: &str,
        block: i64,
    ) -> Vec<String> {
        distinct(
            self.role_admissions(source_family, role)
                .filter(|admission| holds(admission, block)),
        )
    }

    /// The one address the family declares under `role`, at any block. `None` when the role is
    /// undeclared or names several addresses.
    pub(in crate::schema_v2) fn declared_address_for_role(
        &self,
        source_family: &str,
        role: &str,
    ) -> Option<&str> {
        one_address(self.role_admissions(source_family, role))
    }

    /// The first block of the role's one declared address.
    pub(in crate::schema_v2) fn declared_start_block_for_role(
        &self,
        source_family: &str,
        role: &str,
    ) -> Option<i64> {
        self.declared_address_for_role(source_family, role)?;
        self.role_admissions(source_family, role)
            .filter_map(|admission| admission.active_from_block)
            .min()
    }

    /// Whether some declaration of `role` at `address` holds `block`.
    pub(in crate::schema_v2) fn declares_at(
        &self,
        source_family: &str,
        role: &str,
        address: &str,
        block: i64,
    ) -> bool {
        self.role_admissions(source_family, role).any(|admission| {
            admission.address.eq_ignore_ascii_case(address) && holds(admission, block)
        })
    }
}

fn one_address<'a>(
    mut admissions: impl Iterator<Item = &'a AddressAdmissionInput>,
) -> Option<&'a str> {
    let first = admissions.next()?.address.as_str();
    admissions
        .all(|admission| admission.address.eq_ignore_ascii_case(first))
        .then_some(first)
}

/// The distinct lowercase addresses of `admissions`, sorted.
fn distinct<'a>(admissions: impl Iterator<Item = &'a AddressAdmissionInput>) -> Vec<String> {
    let mut addresses: Vec<String> = admissions
        .map(|admission| admission.address.to_ascii_lowercase())
        .collect();
    addresses.sort();
    addresses.dedup();
    addresses
}

/// The inclusive admitted range, as event admission reads it.
fn holds(admission: &AddressAdmissionInput, block: i64) -> bool {
    admission.active_from_block.is_none_or(|from| block >= from)
        && admission.active_to_block.is_none_or(|to| block <= to)
}
