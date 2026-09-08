use super::*;

impl State {
    pub(in crate::schema_v2) fn settle_v1_releases(
        &mut self,
        at_unix_timestamp: i64,
    ) -> Vec<V1Release> {
        let mut due = Vec::new();
        // Not an inverted guard: `v1_registration_is_live` rejects an expiry whose
        // grace boundary overflows, and such entries sort to the maximum end where
        // the ascending drain below can never reach them. This drain keeps the two
        // in agreement; the property test pins it with `i64::MAX` expiries.
        while let Some((expiry, _)) = self.v1_expiries.get_max() {
            if expiry.checked_add(ENS_GRACE_PERIOD_SECS).is_some() {
                break;
            }
            due.push(self.v1_expiries.remove_max().unwrap().1);
        }
        while let Some((expiry, _)) = self.v1_expiries.get_min() {
            if v1_registration_is_live(Some(*expiry), at_unix_timestamp) {
                break;
            }
            due.push(self.v1_expiries.remove_min().unwrap().1);
        }
        // Preserve the prior OrdMap registrar-key order for deterministic, output-identical releases.
        due.sort();
        let mut releases = Vec::new();
        for key in due {
            let Some(registrar) = self.v1_registrars.remove(&key) else {
                continue;
            };
            let label_less = self.v1_label_less_registrars.remove(&key).is_some();
            let previous_authority = self.v1_names.get(&key).cloned();
            let release_is_active = previous_authority.as_ref().is_some_and(|active| {
                active.resource_id == registrar.resource_id
                    || active.authority_source_family == "ens_v1_wrapper_l1"
            });
            let Some((namespace, namehash)) = key.split_once(':') else {
                continue;
            };
            let next_authority = if release_is_active {
                let next = self.v1_registry_authority_if_authentic(&key);
                self.activate_v1_authority(namespace, namehash, next.clone());
                next
            } else {
                previous_authority.clone()
            };
            releases.push(V1Release {
                namehash: namehash.to_owned(),
                resolver: self.v1_resolvers.get(&key).cloned(),
                registrar,
                label_less,
                release_was_active: release_is_active,
                previous_authority,
                next_authority,
            });
        }
        releases
    }
}
