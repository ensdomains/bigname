use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::error::{V2Error, V2Result};

pub(crate) const V2_CURSOR_VERSION: u8 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Payload {
    pub(crate) version: u8,
    pub(crate) sort: String,
    pub(crate) filters: BTreeMap<String, String>,
    pub(crate) last_item: BTreeMap<String, String>,
    pub(crate) snapshot: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) evaluated_at: Option<String>,
    /// What the continuation is bound to; absent on cursors issued before position-bound
    /// pagination and on routes that still bind a publication token in `snapshot`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) binding: Option<Binding>,
}

/// The continuation contract a cursor follows (docs/api-v2-routes.md, "Shared Route Rules").
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum BindingPolicy {
    /// A history walk bound to one block per chain.
    #[serde(rename = "history-bound-v1")]
    HistoryBound,
    /// A position in a sort order over live current state.
    #[serde(rename = "keyset-v1")]
    Keyset,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub(crate) policy: BindingPolicy,
    /// `0x`-prefixed keccak256 of the manifest revisions the request scope was read under.
    pub(crate) manifests: String,
    /// The block bound and redo counters of every chain in the request scope; present exactly
    /// for [`BindingPolicy::HistoryBound`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) chains: Option<BTreeMap<String, BoundChain>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundChain {
    pub(crate) block_number: i64,
    pub(crate) block_hash: String,
    pub(crate) interpret_generation: i64,
    pub(crate) project_generation: i64,
}

impl Binding {
    fn is_well_formed(&self) -> bool {
        let digest = self.manifests.strip_prefix("0x").is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        });
        let chains = match (self.policy, self.chains.as_ref()) {
            (BindingPolicy::HistoryBound, Some(chains)) => {
                !chains.is_empty()
                    && chains.iter().all(|(chain, bound)| {
                        !chain.trim().is_empty()
                            && !bound.block_hash.trim().is_empty()
                            && bound.block_number >= 0
                            && bound.interpret_generation >= 0
                            && bound.project_generation >= 0
                    })
            }
            (BindingPolicy::Keyset, None) => true,
            _ => false,
        };
        digest && chains
    }
}

impl Payload {
    pub(crate) fn new(
        sort: impl Into<String>,
        filters: BTreeMap<String, String>,
        last_item: BTreeMap<String, String>,
        snapshot: Option<String>,
    ) -> Self {
        Self {
            version: V2_CURSOR_VERSION,
            sort: sort.into(),
            filters,
            last_item,
            snapshot,
            evaluated_at: None,
            binding: None,
        }
    }
}

pub(crate) fn encode(payload: &Payload) -> String {
    hex::encode(serde_json::to_vec(payload).expect("v2 cursor payload must serialize"))
}

pub(crate) fn decode(cursor: &str) -> V2Result<Payload> {
    let decoded = hex::decode(cursor).map_err(|_| invalid_cursor_error())?;
    let payload: Payload = serde_json::from_slice(&decoded).map_err(|_| invalid_cursor_error())?;

    if payload.version != V2_CURSOR_VERSION
        || payload
            .binding
            .as_ref()
            .is_some_and(|binding| !binding.is_well_formed())
    {
        return Err(invalid_cursor_error());
    }

    Ok(payload)
}

pub(crate) fn cursor_value(
    payload: &Payload,
    key: &str,
    invalid_cursor_error: impl Fn() -> V2Error,
) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_cursor_error)
}

pub(crate) fn invalid_cursor_error() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::v2::error::ErrorCode;

    fn sample_payload() -> Payload {
        let filters = BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("order".to_owned(), "asc".to_owned()),
        ]);
        let last_item = BTreeMap::from([
            ("name".to_owned(), "nick.eth".to_owned()),
            ("registration_id".to_owned(), "reg-1".to_owned()),
        ]);

        Payload::new("name", filters, last_item, Some("snapshot-1".to_owned()))
    }

    #[test]
    fn cursor_round_trips_encoded_payload() {
        let payload = sample_payload();
        let encoded = encode(&payload);

        assert!(encoded.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(encoded, encoded.to_ascii_lowercase());
        assert_eq!(decode(&encoded).expect("cursor must decode"), payload);
    }

    #[test]
    fn cursor_decode_rejects_version_mismatch_as_invalid_input() {
        let mut payload = sample_payload();
        payload.version = V2_CURSOR_VERSION + 1;
        let encoded = hex::encode(serde_json::to_vec(&payload).expect("payload must serialize"));

        let error = decode(&encoded).expect_err("version mismatch must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn cursor_decode_rejects_malformed_token_as_invalid_input() {
        let error = decode("not-a-hex-cursor").expect_err("malformed cursor must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn cursor_decode_checks_the_binding_shape() {
        let mut payload = sample_payload();
        payload.snapshot = None;
        payload.binding = Some(Binding {
            policy: BindingPolicy::HistoryBound,
            manifests: format!("0x{}", "ab".repeat(32)),
            chains: Some(BTreeMap::from([(
                "ethereum-mainnet".to_owned(),
                BoundChain {
                    block_number: 100,
                    block_hash: "0x64".to_owned(),
                    interpret_generation: 3,
                    project_generation: 4,
                },
            )])),
        });
        assert_eq!(decode(&encode(&payload)).expect("well formed"), payload);

        let malformed = |edit: fn(&mut Binding)| {
            let mut payload = payload.clone();
            edit(payload.binding.as_mut().expect("binding"));
            decode(&encode(&payload))
                .expect_err("malformed binding")
                .code()
        };
        assert_eq!(
            malformed(|b| b.manifests = "0xAB".to_owned()),
            ErrorCode::InvalidInput
        );
        assert_eq!(
            malformed(|b| b.policy = BindingPolicy::Keyset),
            ErrorCode::InvalidInput
        );
        assert_eq!(malformed(|b| b.chains = None), ErrorCode::InvalidInput);
        assert_eq!(
            malformed(|b| b.chains = Some(BTreeMap::new())),
            ErrorCode::InvalidInput
        );
        assert_eq!(
            malformed(|b| {
                let chain = b
                    .chains
                    .as_mut()
                    .unwrap()
                    .get_mut("ethereum-mainnet")
                    .unwrap();
                chain.interpret_generation = -1;
            }),
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn cursor_payload_has_no_route_field() {
        let payload = sample_payload();
        let serialized = serde_json::to_value(payload).expect("payload must serialize");
        let Value::Object(object) = serialized else {
            panic!("payload must serialize as an object");
        };

        assert!(!object.contains_key("route"));
        assert!(object.contains_key("snapshot"));
    }
}
