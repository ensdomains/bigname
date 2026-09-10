use super::{State, V1NameState, V1Release, v1_key};

impl State {
    pub(in crate::schema_v2) fn remember_v2_migration(&mut self, name: &str, timestamp: i64) {
        let first = self
            .v2_migration_times
            .get(name)
            .copied()
            .unwrap_or(timestamp);
        self.v2_migration_times
            .insert(name.to_owned(), first.min(timestamp));
    }

    pub(in crate::schema_v2) fn restore_v2_migration_boundary(
        &mut self,
        event: &crate::schema_v2::PriorEventInput,
    ) {
        if event.source_family == "ens_v2_migration_l1"
            && event.event_kind == "MigrationApplied"
            && event.after_state["consumer_visibility"] == "activated"
            && event.after_state["predecessor_binding"]["authority_epoch"] == "ens_v1"
            && event.after_state["successor_binding"]["authority_epoch"] == "ens_v2"
            && let (Some(name), Some(timestamp)) = (&event.logical_name_id, event.block_timestamp)
        {
            self.remember_v2_migration(name, timestamp.unix_timestamp());
        }
    }

    fn v1_expiry_is_after_migration(&self, name: &str, timestamp: i64) -> bool {
        self.v2_migration_times
            .get(name)
            .is_some_and(|boundary| *boundary < timestamp)
    }

    pub(in crate::schema_v2) fn release_v1_name(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) -> Option<V1NameState> {
        let released = self.v1_names.remove(&v1_key(namespace, namehash));
        if let Some(released) = released.as_ref()
            && self.active_resources.get(&released.logical_name_id) == Some(&released.resource_id)
        {
            self.active_resources.remove(&released.logical_name_id);
        }
        released
    }

    pub(in crate::schema_v2) fn restore_v1_registration_release(
        &mut self,
        namespace: &str,
        namehash: &str,
        timestamp: i64,
    ) {
        let key = v1_key(namespace, namehash);
        let registrar = self.v1_registrars.remove(&key);
        self.update_v1_expiry_index(
            &key,
            registrar.as_ref().and_then(|state| state.expiry),
            None,
        );
        let should_release_active = self.v1_names.get(&key).is_some_and(|active| {
            registrar
                .as_ref()
                .is_some_and(|registrar| active.logical_name_id == registrar.logical_name_id)
                || matches!(
                    active.authority_source_family.as_str(),
                    "ens_v1_registrar_l1" | "basenames_base_registrar" | "ens_v1_wrapper_l1"
                )
        });
        if should_release_active {
            let next_authority = if self.v1_expiry_is_after_migration(&key, timestamp) {
                None
            } else {
                self.v1_registry_authority_if_authentic(&key)
            };
            self.activate_v1_authority(namespace, namehash, next_authority);
        }
    }

    pub(in crate::schema_v2) fn settle_v1_releases(
        &mut self,
        at_unix_timestamp: i64,
    ) -> Vec<V1Release> {
        let mut due = Vec::new();
        while let Some((expiry, _)) = self.v1_expiries.get_max() {
            if expiry.checked_add(ENS_GRACE_PERIOD_SECS).is_some() {
                break;
            }
            self.v1_expiries.remove_max();
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
            if self.v1_expiry_is_after_migration(&key, at_unix_timestamp) {
                // The old lease still expires, but its retained registry owner
                // cannot reopen the ENSv1 binding closed by a proven migration.
                let (namespace, namehash) = key.split_once(':').expect("registrar key");
                self.release_v1_name(namespace, namehash);
                releases.push(V1Release {
                    namehash: namehash.to_owned(),
                    registrar,
                    release_was_active: false,
                    previous_authority: None,
                    next_authority: None,
                    resolver: None,
                });
                continue;
            }
            let previous_authority = self.v1_names.get(&key).cloned();
            let release_is_active = previous_authority.as_ref().is_some_and(|active| {
                active.resource_id == registrar.resource_id
                    || active.authority_source_family == "ens_v1_wrapper_l1"
            });
            let Some((namespace, namehash)) = key.split_once(':') else {
                continue;
            };
            let next_authority = if release_is_active {
                // A NameWrapper holds the registry node only on behalf of the lease it wrapped
                // (wrapETH2LD reclaims the node for the wrapper). Once that lease lapses past
                // grace there is no ENSv1 authority left to fall back to: reviving the
                // remembered registry-only custody would keep serving the name as registered.
                let lapsed_wrapper_custody = previous_authority
                    .as_ref()
                    .is_some_and(|active| active.authority_source_family == "ens_v1_wrapper_l1");
                let next = if lapsed_wrapper_custody {
                    None
                } else {
                    self.v1_registry_authority_if_authentic(&key)
                };
                self.activate_v1_authority(namespace, namehash, next);
                self.v1_name(namespace, namehash)
            } else {
                previous_authority.clone()
            };
            releases.push(V1Release {
                namehash: namehash.to_owned(),
                resolver: self.v1_resolvers.get(&key).cloned(),
                registrar,
                release_was_active: release_is_active,
                previous_authority,
                next_authority,
            });
        }
        releases
    }
    pub(super) fn update_v1_expiry_index(
        &mut self,
        registrar_key: &str,
        previous: Option<i64>,
        current: Option<i64>,
    ) {
        if previous == current {
            return;
        }
        if let Some(previous) = previous {
            self.v1_expiries
                .remove(&(previous, registrar_key.to_owned()));
        }
        if let Some(current) = current {
            self.v1_expiries.insert((current, registrar_key.to_owned()));
        }
    }
}

pub(in crate::schema_v2) const ENS_GRACE_PERIOD_SECS: i64 = 90 * 24 * 60 * 60;

pub(in crate::schema_v2) fn v1_registration_is_live(
    expiry: Option<i64>,
    at_unix_timestamp: i64,
) -> bool {
    expiry.is_none_or(|expiry| {
        expiry
            .checked_add(ENS_GRACE_PERIOD_SECS)
            .is_none_or(|release| at_unix_timestamp <= release)
    })
}
