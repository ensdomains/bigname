use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

struct Registration<V> {
    id: u64,
    hook: V,
}

/// A process-local registry for hooks installed by one test scope.
///
/// Keys must include every isolation boundary needed by the caller. Database-backed
/// tests should normally include [`crate::current_test_database`] in the key.
pub struct ScopedTestHookRegistry<K: Ord + Clone + 'static, V: 'static> {
    hooks: Mutex<BTreeMap<K, Registration<V>>>,
    next_registration_id: AtomicU64,
}

impl<K: Ord + Clone + 'static, V: 'static> ScopedTestHookRegistry<K, V> {
    pub const fn new() -> Self {
        Self {
            hooks: Mutex::new(BTreeMap::new()),
            next_registration_id: AtomicU64::new(1),
        }
    }

    /// Install one hook until the returned guard is dropped or the hook is consumed.
    ///
    /// Installing a second live hook for the same key is a test bug and panics instead
    /// of silently redirecting or blocking one of the tests.
    #[must_use = "dropping the guard immediately uninstalls the test hook"]
    pub fn install(&'static self, key: K, hook: V) -> ScopedTestHookGuard<K, V> {
        let registration_id = self.next_registration_id.fetch_add(1, Ordering::Relaxed);
        let mut hooks = self
            .hooks
            .lock()
            .expect("scoped test hook registry mutex poisoned");
        if hooks.contains_key(&key) {
            drop(hooks);
            panic!("a scoped test hook is already installed for this key");
        }
        hooks.insert(
            key.clone(),
            Registration {
                id: registration_id,
                hook,
            },
        );
        drop(hooks);
        ScopedTestHookGuard {
            registry: self,
            key,
            registration_id,
        }
    }

    pub fn get_cloned(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.hooks
            .lock()
            .expect("scoped test hook registry mutex poisoned")
            .get(key)
            .map(|registration| registration.hook.clone())
    }

    /// Consume the hook for `key`. Dropping its original guard afterward is harmless.
    pub fn take(&self, key: &K) -> Option<V> {
        self.hooks
            .lock()
            .expect("scoped test hook registry mutex poisoned")
            .remove(key)
            .map(|registration| registration.hook)
    }

    fn remove_registration(&self, key: &K, registration_id: u64) {
        let mut hooks = self
            .hooks
            .lock()
            .expect("scoped test hook registry mutex poisoned");
        if hooks
            .get(key)
            .is_some_and(|registration| registration.id == registration_id)
        {
            hooks.remove(key);
        }
    }
}

impl<K: Ord + Clone + 'static, V: 'static> Default for ScopedTestHookRegistry<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Removes one exact test-hook registration when its test scope exits.
#[must_use = "dropping the guard uninstalls the test hook"]
pub struct ScopedTestHookGuard<K: Ord + Clone + 'static, V: 'static> {
    registry: &'static ScopedTestHookRegistry<K, V>,
    key: K,
    registration_id: u64,
}

impl<K: Ord + Clone + 'static, V: 'static> Drop for ScopedTestHookGuard<K, V> {
    fn drop(&mut self) {
        self.registry
            .remove_registration(&self.key, self.registration_id);
    }
}

#[cfg(test)]
mod tests {
    use super::ScopedTestHookRegistry;

    static HOOKS: ScopedTestHookRegistry<&str, usize> = ScopedTestHookRegistry::new();

    #[test]
    fn guard_drop_cleans_an_unconsumed_hook() {
        let guard = HOOKS.install("dropped", 1);
        assert_eq!(HOOKS.get_cloned(&"dropped"), Some(1));

        drop(guard);

        assert_eq!(HOOKS.get_cloned(&"dropped"), None);
    }

    #[test]
    fn keys_isolate_parallel_hooks() {
        let first = HOOKS.install("first", 1);
        let second = HOOKS.install("second", 2);

        assert_eq!(HOOKS.take(&"first"), Some(1));
        assert_eq!(HOOKS.get_cloned(&"second"), Some(2));

        drop(first);
        drop(second);
        assert_eq!(HOOKS.get_cloned(&"second"), None);
    }

    #[test]
    fn consumed_guard_cannot_remove_a_new_registration() {
        let consumed_guard = HOOKS.install("reused", 1);
        assert_eq!(HOOKS.take(&"reused"), Some(1));

        let replacement_guard = HOOKS.install("reused", 2);
        drop(consumed_guard);

        assert_eq!(HOOKS.get_cloned(&"reused"), Some(2));
        drop(replacement_guard);
    }

    #[test]
    #[should_panic(expected = "already installed")]
    fn duplicate_live_key_is_rejected() {
        let _guard = HOOKS.install("duplicate", 1);
        let _duplicate = HOOKS.install("duplicate", 2);
    }
}
