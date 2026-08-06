use super::{State, V1NameState, V1WrapperData, v1_key};

impl State {
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
