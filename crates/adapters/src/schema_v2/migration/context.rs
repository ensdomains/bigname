/// What a BaseRegistrar log's interpretation reads from outside the log: the ENSv1→ENSv2
/// migration launch and the other logs of its transaction.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::schema_v2) struct RegistrarContext<'a> {
    pub(in crate::schema_v2) matched_wrapper_completion: bool,
    pub(in crate::schema_v2) wrapper_mint_completion: Option<&'a crate::schema_v2::RawLogInput>,
    pub(in crate::schema_v2) migration_enabled: bool,
    pub(in crate::schema_v2) graveyard_cleanup: bool,
    pub(in crate::schema_v2) transaction_has_registry_setup: bool,
    pub(in crate::schema_v2) registration_follows: bool,
    /// An admitted controller event later in the transaction owns this numeric BaseRegistrar
    /// registration or renewal.
    pub(in crate::schema_v2) controller_lifecycle_follows: bool,
    pub(in crate::schema_v2) registry_graveyard: Option<alloy_primitives::Address>,
    pub(in crate::schema_v2) wrapper_custody: crate::schema_v2::protocol::v1::WrapperCustody,
}
