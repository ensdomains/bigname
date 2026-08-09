//! `ens_v1_registry_l1`'s 2017 LLL-era emitter stored and logged argument words without masking
//! them to the declared slot width (#361), so a `NewOwner`/`Transfer` data word can carry a value
//! no `address` type represents. The tolerant decode reads such a word as its low 20 bytes — the
//! value fallback-registry readers see — but with the upper bytes nonzero no caller can ever
//! authenticate as the stored owner: on-chain the write grants authority to no one and locks out
//! the previous owner. The event body therefore records the read-equivalent value with explicit
//! markers, while interpreter state records no owner, activates no authority, and grants no
//! permission for it (docs/architecture.md § Source families).

use alloy_sol_types::SolEvent;
use serde_json::{Value, json};

use crate::evm_abi::{TolerantEvent, decode_event_log, hex_string};
use crate::schema_v2::state::{State, V1NameState};

/// The family's tolerant decoder: strict-first, masked-retry, with provenance.
pub(super) type TolerantDecode<E> =
    fn(&[String], &[u8], &'static str) -> anyhow::Result<TolerantEvent<E>>;

/// Routes the registry decode: strict unless the source family tolerates unmasked words, in
/// which case the family's tolerant decoder runs strict-first and retries masked.
pub(super) fn decode_registry_event<E>(
    tolerate_unmasked_word: bool,
    topics: &[String],
    data: &[u8],
    context: &'static str,
    tolerant: TolerantDecode<E>,
) -> anyhow::Result<TolerantEvent<E>>
where
    E: SolEvent,
{
    if tolerate_unmasked_word {
        tolerant(topics, data, context)
    } else {
        decode_event_log::<E>(topics, data, context).map(|event| TolerantEvent {
            event,
            unmasked_word: None,
        })
    }
}

/// Adds the `<field>_word_unmasked` / `<field>_word_raw` marker pair so consumers can tell a
/// masked low-20 value apart from a genuinely typed address. Absent on clean decodes.
pub(super) fn mark_unmasked_word(body: &mut Value, field: &str, word: &[u8; 32]) {
    let object = body.as_object_mut().expect("registry state is an object");
    object.insert(format!("{field}_word_unmasked"), Value::Bool(true));
    object.insert(format!("{field}_word_raw"), json!(hex_string(word)));
}

/// Whether a retained `NewOwner`/`Transfer` body carried an unmasked owner word. State restore
/// keys on this marker to replay the same forget-the-owner transition the interpreter ran.
pub(in crate::schema_v2) fn body_has_unmasked_owner_word(after_state: &Value) -> bool {
    after_state
        .get("owner_word_unmasked")
        .and_then(Value::as_bool)
        == Some(true)
}

/// State transition for a `NewOwner`/`Transfer` whose owner word was unmasked: like the
/// zero-owner arm, a prior registry-direct authority closes and no new authority holder replaces
/// it. The registry-owner state forgets the node entirely — owner of record and any remembered
/// registry-direct authority — because the word's low 20 bytes are an address no caller can
/// authenticate as and the raw word is not an address. A later clean `NewOwner` then reports an
/// empty `explicit_before`, matching the no-authority-holder after-state this write publishes,
/// and a registrar expiry cannot resurrect the dead authority. The raw word survives in the
/// body's `owner_word_raw` marker.
pub(super) fn close_authority_for_unmasked_owner(
    state: &mut State,
    namespace: &str,
    namehash: &str,
    previous: Option<&V1NameState>,
) -> Option<V1NameState> {
    state.forget_v1_registry_owner(namespace, namehash);
    if previous.is_some_and(|authority| authority.token_lineage_id.is_none()) {
        state.activate_v1_authority(namespace, namehash, None);
        None
    } else {
        previous.cloned()
    }
}
