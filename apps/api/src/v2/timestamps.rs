use sqlx::types::time::OffsetDateTime;

pub(crate) use bigname_storage::public_name_fields::ExpiryTimestamp;

pub(super) fn clock_input(value: &str) -> Option<OffsetDateTime> {
    value
        .parse::<bigname_storage::UnixSeconds>()
        .ok()?
        .to_datetime()
}
