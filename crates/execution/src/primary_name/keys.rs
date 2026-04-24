use crate::validation::normalize_address;

pub(crate) fn normalized_verified_primary_name_request_key(
    namespace: &str,
    normalized_address: &str,
    coin_type: &str,
) -> String {
    format!(
        "{namespace}:{}:{coin_type}",
        normalize_address(normalized_address)
    )
}
