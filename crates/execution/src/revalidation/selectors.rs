use bigname_storage::SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey;

pub(crate) fn selector_family_and_key(
    record_key: &str,
    selector: &SupportedVerifiedRecordKey,
) -> (String, Option<String>) {
    match selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            ("addr".to_owned(), Some(coin_type.clone()))
        }
        SupportedVerifiedRecordKey::Avatar => ("avatar".to_owned(), None),
        SupportedVerifiedRecordKey::Contenthash => ("contenthash".to_owned(), None),
        SupportedVerifiedRecordKey::Text => (
            "text".to_owned(),
            record_key.strip_prefix("text:").map(str::to_owned),
        ),
    }
}
