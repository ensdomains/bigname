use crate::schema_v2::model::RawLogInput;

use super::{State, v1_key};

/// The registrar's own `NameRegistered` / `NameRenewed` are held until the end of
/// their block. An admitted controller event for the same label in the same
/// transaction takes the fact over and drops the held log; whatever is still
/// held when the block ends is interpreted then, as the fallback source.
impl State {
    pub(in crate::schema_v2) fn hold_v1_registrar_log(
        &mut self,
        namespace: &str,
        namehash: &str,
        raw: &RawLogInput,
    ) {
        self.v1_pending_registrar_logs.insert(
            (raw.transaction_hash.clone(), v1_key(namespace, namehash)),
            raw.clone(),
        );
    }

    pub(in crate::schema_v2) fn release_v1_registrar_log(
        &mut self,
        namespace: &str,
        namehash: &str,
        raw: &RawLogInput,
    ) {
        self.v1_pending_registrar_logs
            .remove(&(raw.transaction_hash.clone(), v1_key(namespace, namehash)));
    }

    /// The held logs in their original order; the hold is empty afterwards.
    pub(in crate::schema_v2) fn take_v1_pending_registrar_logs(&mut self) -> Vec<RawLogInput> {
        let mut held = std::mem::take(&mut self.v1_pending_registrar_logs)
            .into_iter()
            .map(|(_, raw)| raw)
            .collect::<Vec<_>>();
        held.sort_by_key(|raw| (raw.block_number, raw.log_index));
        held
    }
}
