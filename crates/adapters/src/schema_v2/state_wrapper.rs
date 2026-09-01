use super::{State, V1NameState, V1WrapperData, v1_key};
use crate::schema_v2::model::RawLogInput;

impl State {
    pub(in crate::schema_v2) fn note_v1_unwrap(
        &mut self,
        namespace: &str,
        namehash: &str,
        wrapper: &str,
        raw: &RawLogInput,
    ) {
        self.v1_pending_unwraps.insert(
            v1_key(namespace, namehash),
            (unwrap_transaction(wrapper, raw), raw.log_index),
        );
    }

    pub(in crate::schema_v2) fn matching_v1_unwrap_time(
        &self,
        namespace: &str,
        namehash: &str,
        from: &str,
        raw: &RawLogInput,
    ) -> Option<time::OffsetDateTime> {
        self.v1_pending_unwraps
            .get(&v1_key(namespace, namehash))
            .and_then(|(transaction, unwrapped_log)| {
                (transaction == &unwrap_transaction(from, raw) && *unwrapped_log < raw.log_index)
                    .then(|| super::super::seam::event_time(raw.block_timestamp, *unwrapped_log))
            })
    }

    pub(in crate::schema_v2) fn set_v1_registrar_controller(
        &mut self,
        controller: &str,
        approved: bool,
        raw: &RawLogInput,
    ) {
        self.begin_v1_registrar_controller_transaction(raw);
        let controller = controller.to_ascii_lowercase();
        if approved {
            self.v1_registrar_controllers.insert(controller);
        } else {
            let completed = self
                .v1_pending_wrapper_sync_expiries
                .iter()
                .filter_map(|(key, (expected_controller, expiry))| {
                    expected_controller
                        .eq_ignore_ascii_case(&controller)
                        .then(|| (key.clone(), *expiry))
                })
                .collect::<Vec<_>>();
            for (key, expiry) in completed {
                let expiry = self
                    .v1_correlated_wrapper_expiries
                    .get(&key)
                    .copied()
                    .map_or(expiry, |current| current.max(expiry));
                self.v1_correlated_wrapper_expiries
                    .insert(key.clone(), expiry);
                self.v1_pending_wrapper_sync_expiries.remove(&key);
            }
            self.v1_registrar_controllers.remove(&controller);
        }
    }

    // Candidate ENSv1→ENSv2 migration evidence may describe the wrapper expiry derived during
    // syncWrapper, but it must not advance the independently admitted NameWrapper state.
    // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L104-L111 @ ens_v2_sepolia_20260629@ccaeb58)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
    pub(in crate::schema_v2) fn correlated_v1_wrapper_expiry(
        &mut self,
        namespace: &str,
        namehash: &str,
        registrar_expiry: u64,
        raw: &RawLogInput,
    ) -> Option<u64> {
        self.begin_v1_registrar_controller_transaction(raw);
        let key = v1_key(namespace, namehash);
        let registry_owner = self.v1_registry_owners.get(&key)?.clone();
        if !self.v1_registrar_controllers.contains(&registry_owner)
            || self
                .v1_names
                .get(&key)
                .is_none_or(|name| name.authority_source_family != "ens_v1_wrapper_l1")
            || !self.v1_wrapper_data.contains_key(&key)
        {
            return None;
        }
        let wrapper_expiry = registrar_expiry.checked_add(super::ENS_GRACE_PERIOD_SECS as u64)?;
        self.v1_pending_wrapper_sync_expiries
            .insert(key, (registry_owner, wrapper_expiry));
        Some(wrapper_expiry)
    }

    fn begin_v1_registrar_controller_transaction(&mut self, raw: &RawLogInput) {
        let transaction = format!("{}:{}", raw.block_hash, raw.transaction_hash);
        if self.v1_registrar_controller_transaction.as_deref() != Some(transaction.as_str()) {
            self.v1_registrar_controller_transaction = Some(transaction);
            self.v1_registrar_controllers.clear();
            self.v1_pending_wrapper_sync_expiries.clear();
        }
    }

    pub(in crate::schema_v2) fn restore_v1_correlated_wrapper_expiry(
        &mut self,
        namespace: &str,
        namehash: &str,
        expiry: u64,
    ) {
        let key = v1_key(namespace, namehash);
        let expiry = self
            .v1_correlated_wrapper_expiries
            .get(&key)
            .copied()
            .map_or(expiry, |current| current.max(expiry));
        self.v1_correlated_wrapper_expiries.insert(key, expiry);
    }

    pub(in crate::schema_v2) fn retained_v1_correlated_wrapper_expiry(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<u64> {
        self.v1_correlated_wrapper_expiries
            .get(&v1_key(namespace, namehash))
            .copied()
    }

    // ENSv1 stores the wrapped .eth expiry with the registrar grace period added. A completed
    // ENSv1→ENSv2 syncWrapper envelope can update that expiry without an ordinary NameWrapper
    // event, so fallback identity materialization uses the later of those two retained facts.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L270-L277 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297-L303 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L104-L111 @ ens_v2_sepolia_20260629@ccaeb58)
    pub(in crate::schema_v2) fn v1_registrar_expiry_from_wrapper(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<i64> {
        let key = v1_key(namespace, namehash);
        let wrapper_expiry = self.v1_wrapper_data.get(&key)?.expiry;
        let expiry = self
            .v1_correlated_wrapper_expiries
            .get(&key)
            .copied()
            .map_or(wrapper_expiry, |correlated| correlated.max(wrapper_expiry));
        i64::try_from(expiry)
            .ok()?
            .checked_sub(super::ENS_GRACE_PERIOD_SECS)
    }

    pub(in crate::schema_v2) fn wrap_v1_name(
        &mut self,
        namespace: &str,
        namehash: &str,
        fuses: u32,
        expiry: u64,
        at_unix_timestamp: i64,
    ) -> V1WrapperData {
        let retained = self
            .v1_wrapper_data
            .get(&v1_key(namespace, namehash))
            .copied();
        let expiry = retained.map_or(expiry, |data| data.expiry.max(expiry));
        let fuses = retained
            .filter(|data| u64::try_from(at_unix_timestamp).is_ok_and(|now| data.expiry >= now))
            .map_or(fuses, |data| fuses | (data.fuses & 0xffff_0000));
        let data = V1WrapperData { fuses, expiry };
        self.v1_wrapper_data
            .insert(v1_key(namespace, namehash), data);
        data
    }

    pub(in crate::schema_v2) fn restore_v1_wrapper_data(
        &mut self,
        namespace: &str,
        namehash: &str,
        fuses: u32,
        expiry: u64,
    ) {
        let expiry = self
            .v1_wrapper_data
            .get(&v1_key(namespace, namehash))
            .map_or(expiry, |data| data.expiry.max(expiry));
        self.v1_wrapper_data
            .insert(v1_key(namespace, namehash), V1WrapperData { fuses, expiry });
        if let Some(state) = self.v1_names.get_mut(&v1_key(namespace, namehash))
            && state.authority_source_family == "ens_v1_wrapper_l1"
        {
            state.expiry = Some(i64::try_from(expiry).unwrap_or(i64::MAX));
        }
    }

    pub(in crate::schema_v2) fn set_v1_wrapper_fuses(
        &mut self,
        namespace: &str,
        namehash: &str,
        fuses: u32,
    ) -> Option<(V1WrapperData, V1WrapperData)> {
        let data = self.v1_wrapper_data.get_mut(&v1_key(namespace, namehash))?;
        let previous = *data;
        data.fuses = fuses;
        Some((previous, *data))
    }

    pub(in crate::schema_v2) fn update_v1_wrapper_expiry(
        &mut self,
        namespace: &str,
        namehash: &str,
        expiry: u64,
    ) -> Option<(u64, V1NameState)> {
        let key = v1_key(namespace, namehash);
        let data = self.v1_wrapper_data.get_mut(&key)?;
        let previous = data.expiry;
        data.expiry = data.expiry.max(expiry);
        let state = self.v1_names.get_mut(&key)?;
        if state.authority_source_family != "ens_v1_wrapper_l1" {
            return None;
        }
        state.expiry = Some(i64::try_from(data.expiry).unwrap_or(i64::MAX));
        Some((previous, state.clone()))
    }
}

fn unwrap_transaction(address: &str, raw: &RawLogInput) -> String {
    let address = address.to_ascii_lowercase();
    format!("{address}:{}:{}", raw.block_hash, raw.transaction_hash)
}
