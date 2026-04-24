use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::{ProviderRawPayloadCacheMetadata, decode::keccak256_hex};

const RAW_PAYLOAD_DIGEST_ALGORITHM: &str = "keccak256";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JsonRpcResultPayload {
    pub(super) result: Option<Value>,
    pub(super) fingerprint: JsonRpcPayloadFingerprint,
}

impl JsonRpcResultPayload {
    pub(super) fn with_cache_metadata(
        self,
        payload_kind: &str,
        method: &str,
        fetch_mode: &str,
    ) -> JsonRpcResultWithCacheMetadata {
        JsonRpcResultWithCacheMetadata {
            result: self.result,
            cache_metadata: self
                .fingerprint
                .cache_metadata(payload_kind, method, fetch_mode),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JsonRpcResultWithCacheMetadata {
    pub(super) result: Option<Value>,
    pub(super) cache_metadata: ProviderRawPayloadCacheMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JsonRpcPayloadFingerprint {
    digest_algorithm: String,
    retained_digest: String,
    payload_size_bytes: i64,
}

impl JsonRpcPayloadFingerprint {
    pub(super) fn for_body(body: &[u8]) -> Result<Self> {
        let payload_size_bytes =
            i64::try_from(body.len()).context("JSON-RPC payload size does not fit in i64")?;

        Ok(Self {
            digest_algorithm: RAW_PAYLOAD_DIGEST_ALGORITHM.to_owned(),
            retained_digest: keccak256_hex(body),
            payload_size_bytes,
        })
    }

    fn cache_metadata(
        self,
        payload_kind: &str,
        method: &str,
        fetch_mode: &str,
    ) -> ProviderRawPayloadCacheMetadata {
        ProviderRawPayloadCacheMetadata {
            payload_kind: payload_kind.to_owned(),
            digest_algorithm: self.digest_algorithm,
            retained_digest: self.retained_digest,
            payload_size_bytes: self.payload_size_bytes,
            cache_metadata: json!({
                "source": "json-rpc",
                "method": method,
                "fetch_mode": fetch_mode,
                "digest_scope": "json_rpc_response_body",
            }),
        }
    }
}
