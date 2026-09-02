use std::{fmt, str::FromStr};

use alloy_primitives::{Address, B256, LogData, U256, hex, keccak256};
use alloy_sol_types::{SolEvent, TopicList};
use anyhow::{Context, Result, bail};

const ABI_WORD_BYTES: usize = 32;

#[derive(Debug)]
struct MalformedEventLog(&'static str);

impl fmt::Display for MalformedEventLog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

pub(crate) fn decode_event_log<E>(
    topics: &[String],
    data: &[u8],
    context: &'static str,
) -> Result<E>
where
    E: SolEvent,
{
    let log_data = alloy_log_data(topics, data).context(MalformedEventLog(context))?;
    validate_event_topic_count::<E>(&log_data, context)?;
    E::decode_log_data_validate(&log_data).context(MalformedEventLog(context))
}

pub(crate) fn decode_event_log_data_as<E>(
    topics: &[String],
    data: &[u8],
    expected_topic0: &str,
    context: &'static str,
) -> Result<E>
where
    E: SolEvent,
{
    let log_data = alloy_log_data(topics, data).context(MalformedEventLog(context))?;
    validate_event_topic_count::<E>(&log_data, context)?;
    let actual_topic0 = log_data
        .topics()
        .first()
        .context(MalformedEventLog(context))?;
    let expected_topic0 =
        B256::from_str(&normalize_hex_32(expected_topic0)?).context(MalformedEventLog(context))?;
    if *actual_topic0 != expected_topic0 {
        return Err(anyhow::anyhow!(MalformedEventLog(context)));
    }
    let decoded_topics = E::decode_topics(log_data.topics()).context(MalformedEventLog(context))?;
    let decoded_data =
        E::abi_decode_data_validate(&log_data.data).context(MalformedEventLog(context))?;
    Ok(E::new(decoded_topics, decoded_data))
}

fn validate_event_topic_count<E>(log_data: &LogData, context: &'static str) -> Result<()>
where
    E: SolEvent,
{
    if log_data.topics().len() != <E::TopicList as TopicList>::COUNT {
        return Err(anyhow::anyhow!(MalformedEventLog(context)));
    }
    Ok(())
}

/// A tolerantly decoded event plus its provenance: `unmasked_word` carries the original 32-byte
/// data word when the strict decode rejected the log and the masked retry accepted it, and is
/// `None` when the strict decode accepted the log unchanged. Callers that treat the decoded
/// value as more than a read-equivalent display need the flag to tell a masked word apart from a
/// genuinely typed value.
pub(crate) struct TolerantEvent<E> {
    pub(crate) event: E,
    pub(crate) unmasked_word: Option<[u8; ABI_WORD_BYTES]>,
}

/// Decodes like `decode_event_log`, except that a data payload of exactly one 32-byte word whose
/// upper 12 bytes are nonzero decodes as the word's low 20 bytes. Only valid for events whose
/// data payload is a single address word: the 2017 ENSv1 registry stored and logged argument
/// words without masking them to the declared address type, so its `NewOwner`/`NewResolver`/
/// `Transfer` logs can carry a full 32-byte word in the address slot (#361, docs/architecture.md
/// § Source families); reference indexers decode such a word as its low 20 bytes
/// (upstream: .refs/graph_node/graph/src/abi/event_ext.rs:L17 @ graph_node@aefe173). The retry is
/// attempted only for exactly-32-byte data; any other input keeps the strict decoder's result.
pub(crate) fn decode_event_log_tolerant_address_word<E>(
    topics: &[String],
    data: &[u8],
    context: &'static str,
) -> Result<TolerantEvent<E>>
where
    E: SolEvent,
{
    decode_event_log_tolerant_word::<E>(topics, data, context, 12)
}

/// Decodes like `decode_event_log`, except that a data payload of exactly one 32-byte word whose
/// upper 24 bytes are nonzero decodes as the word's low 8 bytes. Only valid for events whose data
/// payload is a single uint64 word: the 2017 ENSv1 registry's `NewTTL` logs can carry an unmasked
/// word in the uint64 slot (one mainnet instance, block 4,003,999; #361, docs/architecture.md
/// § Source families). The retry is attempted only for exactly-32-byte data; any other input
/// keeps the strict decoder's result.
pub(crate) fn decode_event_log_tolerant_uint64_word<E>(
    topics: &[String],
    data: &[u8],
    context: &'static str,
) -> Result<TolerantEvent<E>>
where
    E: SolEvent,
{
    decode_event_log_tolerant_word::<E>(topics, data, context, 24)
}

fn decode_event_log_tolerant_word<E>(
    topics: &[String],
    data: &[u8],
    context: &'static str,
    mask_bytes: usize,
) -> Result<TolerantEvent<E>>
where
    E: SolEvent,
{
    match decode_event_log::<E>(topics, data, context) {
        Err(error) if is_malformed_event_log(&error) && data.len() == ABI_WORD_BYTES => {
            let mut masked = data.to_vec();
            masked[..mask_bytes].fill(0);
            let event = decode_event_log::<E>(topics, &masked, context)?;
            let unmasked_word = exact_word(data)?.to_owned();
            Ok(TolerantEvent {
                event,
                unmasked_word: Some(unmasked_word),
            })
        }
        result => result.map(|event| TolerantEvent {
            event,
            unmasked_word: None,
        }),
    }
}

pub(crate) fn is_malformed_event_log(error: &anyhow::Error) -> bool {
    error.downcast_ref::<MalformedEventLog>().is_some()
}

pub(crate) fn address_hex(address: Address) -> String {
    hex_string(address.as_slice())
}

pub(crate) fn address_hex_from_word(word: &[u8]) -> Result<String> {
    let word = exact_word(word)?;
    let address = Address::from_slice(&word[12..]);
    Ok(format!("0x{}", hex::encode(address.as_slice())))
}

pub(crate) fn topic_address_hex(value: &str) -> Result<String> {
    address_hex_from_word(&hex_32(value)?)
}

pub(crate) fn u256_decimal(value: U256) -> String {
    value.to_string()
}

pub(crate) fn u256_i64(value: U256, label: &str) -> Result<i64> {
    let value = u64::try_from(value).with_context(|| format!("{label} exceeds u64"))?;
    i64::try_from(value).with_context(|| format!("{label} exceeds i64"))
}

pub(crate) fn saturating_u256_i64(value: U256) -> i64 {
    u64::try_from(value)
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or(i64::MAX)
}

/// Saturating `u64` -> `i64` for non-date second counts (e.g. registration durations). Decode stays
/// faithful to the on-chain value; this only guards against a pathological `> i64::MAX` duration
/// aborting the decode on a strict `try_from`. Timestamp/expiry fields are NOT converted here — they
/// carry the raw `u64` (incl. the `type(uint64).max` "no expiry" sentinel) into the normalized event,
/// and the name_current projection interprets it.
pub(crate) fn saturating_seconds_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn u256_word_hex(value: U256) -> String {
    hex_string(value.to_be_bytes::<ABI_WORD_BYTES>())
}

pub(crate) fn hex_32(value: &str) -> Result<[u8; ABI_WORD_BYTES]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; ABI_WORD_BYTES];
    hex::decode_to_slice(&normalized[2..], &mut output)
        .with_context(|| format!("invalid 32-byte hex value {normalized}"))?;
    Ok(output)
}

pub(crate) fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

pub(crate) fn keccak_signature_hex(signature: &str) -> String {
    keccak256_hex(signature.as_bytes())
}

pub(crate) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(keccak256_bytes(bytes))
}

pub(crate) fn keccak256_bytes(bytes: &[u8]) -> [u8; ABI_WORD_BYTES] {
    let digest = keccak256(bytes);
    let mut output = [0u8; ABI_WORD_BYTES];
    output.copy_from_slice(digest.as_slice());
    output
}

pub(crate) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    hex_string(namehash_bytes(labels))
}

pub(crate) fn child_namehash_hex(parent_node: &str, labelhash: &str) -> Result<String> {
    let mut bytes = [0u8; ABI_WORD_BYTES * 2];
    bytes[..ABI_WORD_BYTES].copy_from_slice(&hex_32(parent_node)?);
    bytes[ABI_WORD_BYTES..].copy_from_slice(&hex_32(labelhash)?);
    Ok(keccak256_hex(&bytes))
}

pub(crate) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex_string_without_prefix(bytes))
}

pub(crate) fn hex_string_without_prefix(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(bytes)
}

fn alloy_log_data(topics: &[String], data: &[u8]) -> Result<LogData> {
    let topics = topics
        .iter()
        .map(|topic| {
            let normalized = normalize_hex_32(topic)?;
            B256::from_str(&normalized).with_context(|| format!("invalid EVM log topic {topic}"))
        })
        .collect::<Result<Vec<_>>>()?;
    LogData::new(topics, data.to_vec().into()).context("EVM log has more than four topics")
}

fn exact_word(word: &[u8]) -> Result<&[u8; ABI_WORD_BYTES]> {
    if word.len() != ABI_WORD_BYTES {
        bail!("ABI word must be exactly 32 bytes");
    }
    word.try_into().context("ABI word must be exactly 32 bytes")
}

pub(crate) fn namehash_bytes(labels: &[Vec<u8>]) -> [u8; ABI_WORD_BYTES] {
    let mut node = [0u8; ABI_WORD_BYTES];
    for label in labels.iter().rev() {
        let mut combined = [0u8; ABI_WORD_BYTES * 2];
        combined[..ABI_WORD_BYTES].copy_from_slice(&node);
        combined[ABI_WORD_BYTES..].copy_from_slice(&keccak256_bytes(label));
        node = keccak256_bytes(&combined);
    }
    node
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;
    use alloy_sol_types::{SolEvent, sol};

    use super::{
        ABI_WORD_BYTES, decode_event_log, decode_event_log_data_as,
        decode_event_log_tolerant_address_word, decode_event_log_tolerant_uint64_word,
        is_malformed_event_log, saturating_seconds_i64,
    };

    sol! {
        event SingleAddress(bytes32 indexed node, address who);
        event SingleUint64(bytes32 indexed node, uint64 ttl);
    }

    const CONTEXT: &str = "SingleAddress log is malformed";

    fn single_address_topics(node: B256) -> Vec<String> {
        vec![
            format!("{:#x}", SingleAddress::SIGNATURE_HASH),
            format!("{node:#x}"),
        ]
    }

    #[test]
    fn saturating_seconds_i64_clamps_durations_without_panicking() {
        assert_eq!(saturating_seconds_i64(0), 0);
        assert_eq!(saturating_seconds_i64(31_536_000), 31_536_000);
        assert_eq!(saturating_seconds_i64(u64::MAX), i64::MAX);
    }

    #[test]
    fn tolerant_address_word_matches_strict_decode_for_masked_words() {
        let node = B256::repeat_byte(0x42);
        let who = alloy_primitives::Address::repeat_byte(0x24);
        let encoded = SingleAddress { node, who }.encode_log_data();
        let decoded = decode_event_log_tolerant_address_word::<SingleAddress>(
            &encoded
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect::<Vec<_>>(),
            &encoded.data,
            CONTEXT,
        )
        .expect("masked address word decodes");
        assert_eq!(decoded.event.node, node);
        assert_eq!(decoded.event.who, who);
        assert_eq!(decoded.unmasked_word, None);
    }

    #[test]
    fn strict_decode_rejects_an_extra_topic() {
        let node = B256::repeat_byte(0x42);
        let who = alloy_primitives::Address::repeat_byte(0x24);
        let encoded = SingleAddress { node, who }.encode_log_data();
        let mut topics = encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        topics.push(format!("{:#x}", B256::repeat_byte(0xff)));

        let error = decode_event_log::<SingleAddress>(&topics, &encoded.data, CONTEXT)
            .map(|_| ())
            .expect_err("an extra topic must make the strict event decode malformed");
        assert!(is_malformed_event_log(&error));
    }

    #[test]
    fn strict_data_as_decode_rejects_an_extra_topic() {
        let node = B256::repeat_byte(0x42);
        let who = alloy_primitives::Address::repeat_byte(0x24);
        let encoded = SingleAddress { node, who }.encode_log_data();
        let mut topics = encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        topics.push(format!("{:#x}", B256::repeat_byte(0xff)));

        let error = decode_event_log_data_as::<SingleAddress>(
            &topics,
            &encoded.data,
            &format!("{:#x}", SingleAddress::SIGNATURE_HASH),
            CONTEXT,
        )
        .map(|_| ())
        .expect_err("an extra topic must make the strict data-as decode malformed");
        assert!(is_malformed_event_log(&error));
    }

    #[test]
    fn strict_decoders_reject_a_missing_topic() {
        let node = B256::repeat_byte(0x42);
        let who = alloy_primitives::Address::repeat_byte(0x24);
        let encoded = SingleAddress { node, who }.encode_log_data();
        let topics = encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();

        let direct = decode_event_log::<SingleAddress>(&topics[..1], &encoded.data, CONTEXT)
            .map(|_| ())
            .expect_err("a missing topic must make the strict event decode malformed");
        assert!(is_malformed_event_log(&direct));
        let data_as = decode_event_log_data_as::<SingleAddress>(
            &topics[..1],
            &encoded.data,
            &format!("{:#x}", SingleAddress::SIGNATURE_HASH),
            CONTEXT,
        )
        .map(|_| ())
        .expect_err("a missing topic must make the strict data-as decode malformed");
        assert!(is_malformed_event_log(&data_as));
    }

    #[test]
    fn tolerant_address_word_decodes_unmasked_word_as_its_low_20_bytes() {
        let node = B256::repeat_byte(0x93);
        let mut data = [0u8; ABI_WORD_BYTES];
        data[..12].fill(0xff);
        data[12..].copy_from_slice(&[0xab; 20]);
        let decoded = decode_event_log_tolerant_address_word::<SingleAddress>(
            &single_address_topics(node),
            &data,
            CONTEXT,
        )
        .expect("unmasked address word decodes as its low 20 bytes");
        assert_eq!(decoded.event.node, node);
        assert_eq!(
            decoded.event.who,
            alloy_primitives::Address::repeat_byte(0xab)
        );
        assert_eq!(decoded.unmasked_word, Some(data));
    }

    // The strict decoder validates slot contents but not buffer exhaustion, so a clean word with
    // trailing bytes passes strict decode without ever reaching the retry; that acceptance is
    // adapter-wide and tracked as issue #367. Pinned so an alloy upgrade that starts rejecting
    // trailing bytes trips this test deliberately instead of narrowing acceptance silently.
    #[test]
    fn strict_decode_accepts_a_clean_word_with_trailing_bytes() {
        let node = B256::repeat_byte(0x42);
        let who = alloy_primitives::Address::repeat_byte(0x24);
        let encoded = SingleAddress { node, who }.encode_log_data();
        let topics = encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        let mut data = encoded.data.to_vec();
        data.extend_from_slice(&[0xff; ABI_WORD_BYTES]);
        let strict = decode_event_log::<SingleAddress>(&topics, &data, CONTEXT)
            .expect("strict decode accepts a clean word followed by trailing bytes");
        assert_eq!(strict.who, who);
        let tolerant =
            decode_event_log_tolerant_address_word::<SingleAddress>(&topics, &data, CONTEXT)
                .expect("tolerant decode defers to the strict result");
        assert_eq!(tolerant.event.who, who);
        assert_eq!(tolerant.unmasked_word, None);
    }

    #[test]
    fn tolerant_address_word_stays_malformed_for_unmasked_word_at_non_word_lengths() {
        let node = B256::repeat_byte(0x93);
        let mut data = [0u8; ABI_WORD_BYTES];
        data[..12].fill(0xff);
        for bad_data in [&data[..31], &[data.as_slice(), &[0]].concat()] {
            let error = decode_event_log_tolerant_address_word::<SingleAddress>(
                &single_address_topics(node),
                bad_data,
                CONTEXT,
            )
            .map(|_| ())
            .expect_err("an unmasked word at a non-word length is never retried");
            assert!(is_malformed_event_log(&error));
        }
    }

    #[test]
    fn tolerant_uint64_word_matches_strict_decode_for_masked_words() {
        let node = B256::repeat_byte(0x42);
        let encoded = SingleUint64 { node, ttl: 3_600 }.encode_log_data();
        let decoded = decode_event_log_tolerant_uint64_word::<SingleUint64>(
            &encoded
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect::<Vec<_>>(),
            &encoded.data,
            CONTEXT,
        )
        .expect("masked uint64 word decodes");
        assert_eq!(decoded.event.node, node);
        assert_eq!(decoded.event.ttl, 3_600);
        assert_eq!(decoded.unmasked_word, None);
    }

    #[test]
    fn tolerant_uint64_word_decodes_unmasked_word_as_its_low_8_bytes() {
        let node = B256::repeat_byte(0x93);
        let mut data = [0u8; ABI_WORD_BYTES];
        data[..24].fill(0xff);
        data[24..].copy_from_slice(&[0x5a; 8]);
        let decoded = decode_event_log_tolerant_uint64_word::<SingleUint64>(
            &[
                format!("{:#x}", SingleUint64::SIGNATURE_HASH),
                format!("{node:#x}"),
            ],
            &data,
            CONTEXT,
        )
        .expect("unmasked uint64 word decodes as its low 8 bytes");
        assert_eq!(decoded.event.node, node);
        assert_eq!(decoded.event.ttl, 0x5a5a_5a5a_5a5a_5a5a);
        assert_eq!(decoded.unmasked_word, Some(data));
    }

    #[test]
    fn tolerant_uint64_word_stays_malformed_for_unmasked_word_at_non_word_lengths() {
        let node = B256::repeat_byte(0x93);
        let mut data = [0u8; ABI_WORD_BYTES];
        data[..24].fill(0xff);
        for bad_data in [&data[..31], &[data.as_slice(), &[0]].concat()] {
            let error = decode_event_log_tolerant_uint64_word::<SingleUint64>(
                &[
                    format!("{:#x}", SingleUint64::SIGNATURE_HASH),
                    format!("{node:#x}"),
                ],
                bad_data,
                CONTEXT,
            )
            .map(|_| ())
            .expect_err("an unmasked word at a non-word length is never retried");
            assert!(is_malformed_event_log(&error));
        }
    }

    #[test]
    fn tolerant_address_word_stays_malformed_for_bad_topics() {
        let node = B256::repeat_byte(0x93);
        let mut data = [0u8; ABI_WORD_BYTES];
        data[..12].fill(0xff);
        let error = decode_event_log_tolerant_address_word::<SingleAddress>(
            &[
                format!("{:#x}", B256::repeat_byte(0xde)),
                format!("{node:#x}"),
            ],
            &data,
            CONTEXT,
        )
        .map(|_| ())
        .expect_err("a wrong event signature stays terminal");
        assert!(is_malformed_event_log(&error));
        let error = decode_event_log_tolerant_address_word::<SingleAddress>(
            &single_address_topics(node)[..1],
            &data,
            CONTEXT,
        )
        .map(|_| ())
        .expect_err("a missing indexed topic stays terminal");
        assert!(is_malformed_event_log(&error));
    }
}
