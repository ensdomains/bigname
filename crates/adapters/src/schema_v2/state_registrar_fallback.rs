use crate::schema_v2::model::RawLogInput;

use super::{State, v1_key};

/// The registrar's own `NameRegistered` / `NameRenewed` are held while their
/// transaction is open. An admitted controller event for the same label in the
/// same transaction takes the fact over and drops every held log for it;
/// whatever is still held when the transaction is complete is interpreted
/// then, as the fallback source, before any later transaction's logs.
impl State {
    pub(in crate::schema_v2) fn hold_v1_registrar_log(
        &mut self,
        namespace: &str,
        namehash: &str,
        raw: &RawLogInput,
    ) {
        self.v1_pending_registrar_logs.insert(
            (
                raw.transaction_hash.clone(),
                v1_key(namespace, namehash),
                raw.log_index,
            ),
            raw.clone(),
        );
    }

    pub(in crate::schema_v2) fn release_v1_registrar_log(
        &mut self,
        namespace: &str,
        namehash: &str,
        raw: &RawLogInput,
    ) {
        let key = v1_key(namespace, namehash);
        let held = self
            .v1_pending_registrar_logs
            .keys()
            .filter(|(transaction, name, _)| *transaction == raw.transaction_hash && *name == key)
            .cloned()
            .collect::<Vec<_>>();
        for key in held {
            self.v1_pending_registrar_logs.remove(&key);
        }
    }

    /// Every held log in its original order; the hold is empty afterwards.
    pub(in crate::schema_v2) fn take_v1_pending_registrar_logs(&mut self) -> Vec<RawLogInput> {
        let mut held = std::mem::take(&mut self.v1_pending_registrar_logs)
            .into_iter()
            .map(|(_, raw)| raw)
            .collect::<Vec<_>>();
        held.sort_by_key(|raw| (raw.block_number, raw.log_index));
        held
    }
}
