//! ENSv2 state maps. Every keyed read or write reports its key to the lookahead loader's
//! loaded-keys check (lookahead/coverage.rs), so a lookahead restore or batch cannot read
//! ENSv2 state whose history was not loaded. Whole-map reads are named for the reason they
//! need no report.
use std::{borrow::Borrow, marker::PhantomData};

use imbl::{ordmap::OrdMap, ordset::OrdSet};
use uuid::Uuid;

use crate::{
    evm_abi::hex_string,
    schema_v2::lookahead::{
        observe_name, observe_v2, observe_v2_expiry_window, observe_v2_registry, restoring,
    },
};

pub(in crate::schema_v2) trait Cover<Q: ?Sized> {
    fn report(key: &Q);
}

/// `registry:token_id`, as built by `v2_key`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct TokenKey;
/// `(address, id)`: a registry and an observation or upstream resource id, or a resolver and
/// an upstream resource id.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct AddressId;
/// `(registry, raw label)`: the token a registry holds for a label.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct RegistryLabel;
/// A registry's own parent claim.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct RegistryClaim;
/// A logical name: its current holders.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct Name;
/// `(token_id, logical name)`: the tokens with that id holding the name, in any registry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct TokenName;
/// `(resolver contract instance, upstream resource)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct InstanceResource;

impl Cover<str> for TokenKey {
    fn report(key: &str) {
        if let Some((registry, token_id)) = key.rsplit_once(':') {
            observe_v2(registry, token_id);
        }
    }
}
impl Cover<String> for TokenKey {
    fn report(key: &String) {
        <Self as Cover<str>>::report(key);
    }
}
impl Cover<(String, String)> for AddressId {
    fn report((address, id): &(String, String)) {
        observe_v2(address, id);
    }
}
impl Cover<(String, Vec<u8>)> for RegistryLabel {
    fn report((registry, label): &(String, Vec<u8>)) {
        observe_v2(registry, &hex_string(alloy_primitives::keccak256(label)));
    }
}
impl Cover<str> for RegistryClaim {
    fn report(registry: &str) {
        observe_v2(registry, "-");
    }
}
impl Cover<String> for RegistryClaim {
    fn report(registry: &String) {
        observe_v2(registry, "-");
    }
}
impl Cover<str> for Name {
    fn report(name: &str) {
        observe_name(name);
    }
}
impl Cover<String> for Name {
    fn report(name: &String) {
        observe_name(name);
    }
}
impl Cover<(String, String)> for TokenName {
    fn report((_, name): &(String, String)) {
        observe_name(name);
    }
}
impl Cover<(Uuid, String)> for InstanceResource {
    fn report((instance, resource): &(Uuid, String)) {
        observe_v2(&instance.to_string(), resource);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) struct Covered<C, K: Ord + Clone, V: Clone> {
    map: OrdMap<K, V>,
    cover: PhantomData<C>,
}

impl<C, K: Ord + Clone, V: Clone> Default for Covered<C, K, V> {
    fn default() -> Self {
        Self {
            map: OrdMap::new(),
            cover: PhantomData,
        }
    }
}

impl<C, K: Ord + Clone, V: Clone> Covered<C, K, V> {
    pub(in crate::schema_v2) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        C: Cover<Q>,
    {
        C::report(key);
        self.map.get(key)
    }

    pub(in crate::schema_v2) fn get_mut<Q: Ord + ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        C: Cover<Q>,
    {
        C::report(key);
        self.map.get_mut(key)
    }

    pub(in crate::schema_v2) fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        C: Cover<K>,
    {
        C::report(&key);
        self.map.insert(key, value)
    }

    pub(in crate::schema_v2) fn remove<Q: Ord + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        C: Cover<Q>,
    {
        C::report(key);
        self.map.remove(key)
    }

    pub(in crate::schema_v2) fn entry(&mut self, key: K) -> CoveredEntry<'_, K, V>
    where
        C: Cover<K>,
    {
        C::report(&key);
        CoveredEntry {
            map: &mut self.map,
            key,
        }
    }

    #[cfg(test)]
    pub(in crate::schema_v2) fn contains_key<Q: Ord + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.map.contains_key(key)
    }

    #[cfg(test)]
    pub(in crate::schema_v2) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[cfg(test)]
    pub(in crate::schema_v2) fn as_map(&self) -> &OrdMap<K, V> {
        &self.map
    }

    /// Emptied before a rebuild from the token map; reads no key.
    pub(in crate::schema_v2) fn clear(&mut self) {
        self.map.clear();
    }

    /// Every loaded entry. Only restore finish and the rebuilds it drives call this: they
    /// re-derive each loaded token's state through reporting reads.
    pub(in crate::schema_v2) fn loaded(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }

    /// Whether any token is loaded. Only used together with `latest_v2_timestamp`, which the
    /// loader supplies for the whole chain: a chain with an ENSv2 token before the batch has
    /// a topology timestamp.
    pub(in crate::schema_v2) fn is_empty_loaded(&self) -> bool {
        self.map.is_empty()
    }
}

pub(in crate::schema_v2) struct CoveredEntry<'a, K: Ord + Clone, V: Clone> {
    map: &'a mut OrdMap<K, V>,
    key: K,
}

impl<'a, K: Ord + Clone, V: Clone + Default> CoveredEntry<'a, K, V> {
    pub(in crate::schema_v2) fn or_default(self) -> &'a mut V {
        self.map.entry(self.key).or_default()
    }
}

impl<V: Clone> Covered<TokenKey, String, V> {
    /// The keys of every token `registry` holds, or during a lookahead restore every loaded
    /// one (see lookahead/coverage.rs).
    pub(in crate::schema_v2) fn registry_keys(&self, registry: &str) -> Vec<String> {
        if !restoring() {
            observe_v2_registry(registry);
        }
        let prefix = format!("{registry}:");
        self.map
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(key, _)| key.clone())
            .collect()
    }
}

/// `(expiry, token key)` for every loaded token with an expiry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct V2Expiries(OrdSet<(u64, String)>);

impl V2Expiries {
    pub(in crate::schema_v2) fn insert(&mut self, expiry: u64, token_key: String) {
        TokenKey::report(token_key.as_str());
        self.0.insert((expiry, token_key));
    }

    pub(in crate::schema_v2) fn remove(&mut self, expiry: u64, token_key: &str) {
        TokenKey::report(token_key);
        self.0.remove(&(expiry, token_key.to_owned()));
    }

    #[cfg(test)]
    pub(in crate::schema_v2) fn contains(&self, entry: &(u64, String)) -> bool {
        self.0.contains(entry)
    }

    #[cfg(test)]
    pub(in crate::schema_v2) fn as_set(&self) -> &OrdSet<(u64, String)> {
        &self.0
    }

    /// The tokens whose expiry lies in `(previous, current]`.
    pub(in crate::schema_v2) fn crossed(&self, previous: i64, current: i64) -> OrdSet<String> {
        observe_v2_expiry_window(previous, current);
        let first = u64::try_from(previous.saturating_add(1)).unwrap_or_default();
        let last = u64::try_from(current).expect("non-negative timestamp");
        self.0
            .range((first, String::new())..)
            .take_while(|(expiry, _)| *expiry <= last)
            .map(|(_, token_key)| token_key.clone())
            .collect()
    }
}
