use std::collections::BTreeMap;

use crate::v2::Status;
use crate::v2::name_record::value_to_string;
use crate::v2::name_records::RecordAnswer;
use crate::v2::support::ResolutionRecordKey;

/// The flat name-detail values carried by successful (`ok`) verified answers. Every other status,
/// and an `ok` answer without a value, contributes nothing; `unsupported_fields` and the dictionary
/// omission rules decide what an absent value means.
#[derive(Debug, Default, PartialEq)]
pub(super) struct VerifiedRecordValues {
    pub(super) addresses: BTreeMap<String, String>,
    pub(super) text_records: BTreeMap<String, String>,
    pub(super) content_hash: Option<String>,
}

impl VerifiedRecordValues {
    pub(super) fn from_answers(
        records: &[ResolutionRecordKey],
        answers: &BTreeMap<String, RecordAnswer>,
    ) -> Self {
        let mut values = Self::default();
        for record in records {
            let Some(value) = answers
                .get(&record.record_key)
                .filter(|answer| answer.status == Status::Ok)
                .and_then(|answer| answer.value.as_ref())
                .and_then(value_to_string)
            else {
                continue;
            };

            match record.record_family.as_str() {
                "addr" => {
                    if let Some(coin_type) = record.selector_key.clone() {
                        values.addresses.insert(coin_type, value);
                    }
                }
                "text" => {
                    if let Some(key) = record.selector_key.clone() {
                        values.text_records.insert(key, value);
                    }
                }
                "avatar" => {
                    values.text_records.insert("avatar".to_owned(), value);
                }
                "contenthash" => {
                    values.content_hash = Some(value);
                }
                _ => {}
            }
        }
        values
    }
}

#[cfg(test)]
mod tests {
    use crate::v2::name_records::VERIFIED_NOT_SUPPORTED_REASON;
    use crate::v2::support::parse_resolution_record_key;

    use super::*;

    fn answer(status: Status, value: Option<&str>) -> RecordAnswer {
        RecordAnswer {
            status,
            value: value.map(|value| serde_json::Value::String(value.to_owned())),
            unsupported_reason: (status == Status::Unsupported)
                .then(|| VERIFIED_NOT_SUPPORTED_REASON.to_owned()),
            failure_reason: None,
            meta: None,
        }
    }

    #[test]
    fn verified_values_keep_only_ok_answers_by_record_family() {
        let records = [
            "addr:60",
            "addr:0",
            "addr:2",
            "avatar",
            "contenthash",
            "text:description",
            "text:url",
            "text:email",
        ]
        .map(|key| parse_resolution_record_key(key).expect("test record key"));
        let answers = BTreeMap::from([
            ("addr:60".to_owned(), answer(Status::Ok, Some("0x0e0e"))),
            ("addr:0".to_owned(), answer(Status::Ok, Some("0x001122"))),
            ("addr:2".to_owned(), answer(Status::NotFound, None)),
            (
                "avatar".to_owned(),
                answer(Status::Ok, Some("eip155:1/erc721:0x1/1")),
            ),
            ("contenthash".to_owned(), answer(Status::Unsupported, None)),
            (
                "text:description".to_owned(),
                answer(Status::Ok, Some("verified")),
            ),
            ("text:url".to_owned(), answer(Status::Failed, None)),
            // An `ok` answer without a value contributes nothing.
            ("text:email".to_owned(), answer(Status::Ok, None)),
        ]);

        let values = VerifiedRecordValues::from_answers(&records, &answers);

        assert_eq!(
            values,
            VerifiedRecordValues {
                addresses: BTreeMap::from([
                    ("0".to_owned(), "0x001122".to_owned()),
                    ("60".to_owned(), "0x0e0e".to_owned()),
                ]),
                text_records: BTreeMap::from([
                    ("avatar".to_owned(), "eip155:1/erc721:0x1/1".to_owned()),
                    ("description".to_owned(), "verified".to_owned()),
                ]),
                content_hash: None,
            }
        );
    }

    #[test]
    fn verified_values_ignore_answers_for_keys_not_requested() {
        let records = [parse_resolution_record_key("contenthash").expect("test record key")];
        let answers = BTreeMap::from([
            ("contenthash".to_owned(), answer(Status::Ok, Some("0xe301"))),
            ("addr:60".to_owned(), answer(Status::Ok, Some("0x0e0e"))),
        ]);

        let values = VerifiedRecordValues::from_answers(&records, &answers);

        assert!(values.addresses.is_empty());
        assert_eq!(values.content_hash.as_deref(), Some("0xe301"));
    }
}
