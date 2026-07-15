use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use sqlx::PgPool;

use super::RegistryReplayState;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct CachedLiveRegistryReplayState {
    pub(super) through_block_number: i64,
    pub(super) through_block_hash: String,
    pub(super) raw_log_input_revision: i64,
    pub(super) raw_log_retention_generation: i64,
    pub(super) discovery_admission_epoch: i64,
    pub(super) replay_state: RegistryReplayState,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LiveRegistryReplayStateKey {
    host: String,
    port: u16,
    socket: Option<String>,
    username: String,
    database: Option<String>,
    deployment_profile: String,
    chain: String,
}

static LIVE_REGISTRY_REPLAY_STATES: OnceLock<
    Mutex<HashMap<LiveRegistryReplayStateKey, CachedLiveRegistryReplayState>>,
> = OnceLock::new();
const MAX_LIVE_REGISTRY_REPLAY_STATES: usize = 32;
pub(super) const MAX_LIVE_REGISTRY_REPLAY_STATE_WEIGHT: usize = 32 * 1024 * 1024;

fn live_registry_replay_states()
-> &'static Mutex<HashMap<LiveRegistryReplayStateKey, CachedLiveRegistryReplayState>> {
    LIVE_REGISTRY_REPLAY_STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn live_registry_replay_state_key(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> LiveRegistryReplayStateKey {
    let options = pool.connect_options();
    LiveRegistryReplayStateKey {
        host: options.get_host().to_owned(),
        port: options.get_port(),
        socket: options
            .get_socket()
            .map(|path| path.to_string_lossy().into_owned()),
        username: options.get_username().to_owned(),
        database: options.get_database().map(str::to_owned),
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
    }
}

pub(super) fn take_live_registry_replay_state(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Option<CachedLiveRegistryReplayState> {
    live_registry_replay_states()
        .lock()
        .expect("ENSv2 live registry replay-state cache lock must not be poisoned")
        .remove(&live_registry_replay_state_key(
            pool,
            deployment_profile,
            chain,
        ))
}

pub(super) fn store_live_registry_replay_state(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    state: CachedLiveRegistryReplayState,
) {
    let key = live_registry_replay_state_key(pool, deployment_profile, chain);
    let mut states = live_registry_replay_states()
        .lock()
        .expect("ENSv2 live registry replay-state cache lock must not be poisoned");
    if states.len() >= MAX_LIVE_REGISTRY_REPLAY_STATES
        && !states.contains_key(&key)
        && let Some(evicted_key) = states.keys().next().cloned()
    {
        states.remove(&evicted_key);
    }
    states.insert(key, state);
}

pub(crate) fn invalidate_live_registry_replay_state(pool: &PgPool, chain: &str) {
    let probe = live_registry_replay_state_key(pool, "", chain);
    live_registry_replay_states()
        .lock()
        .expect("ENSv2 live registry replay-state cache lock must not be poisoned")
        .retain(|key, _| {
            key.host != probe.host
                || key.port != probe.port
                || key.socket != probe.socket
                || key.username != probe.username
                || key.database != probe.database
                || key.chain != probe.chain
        });
}

pub(super) fn replay_state_fits_process_cache(
    state: &RegistryReplayState,
    max_weight: usize,
) -> bool {
    replay_state_weight(state) <= max_weight
}

fn replay_state_weight(state: &RegistryReplayState) -> usize {
    let suffix_weight = state
        .registry_suffix_by_address
        .iter()
        .map(|(address, suffix)| address.len() + suffix.len() + 32)
        .sum::<usize>();
    let contract_weight = state
        .registry_contract_by_address
        .keys()
        .map(|address| address.len() + 32)
        .sum::<usize>();
    let alias_weight = state
        .token_aliases
        .iter()
        .map(|((registry, token), (target_registry, target_token))| {
            registry.len() + token.len() + target_registry.len() + target_token.len() + 64
        })
        .sum::<usize>();
    let state_weight = state
        .states_by_registry_token
        .iter()
        .map(|((registry, token), value)| {
            registry.len()
                + token.len()
                + value.token_id.len()
                + value.labelhash.len()
                + value.label.len()
                + value.full_name.len()
                + value.name.logical_name_id.len()
                + value.name.input_name.len()
                + value.name.canonical_display_name.len()
                + value.name.normalized_name.len()
                + value.name.dns_encoded_name.len()
                + value
                    .name
                    .labelhashes
                    .iter()
                    .map(String::len)
                    .sum::<usize>()
                + value.owner.as_ref().map_or(0, String::len)
                + value.resolver.as_ref().map_or(0, String::len)
                + value.subregistry.as_ref().map_or(0, String::len)
                + 512
        })
        .sum::<usize>();
    suffix_weight + contract_weight + alias_weight + state_weight
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_state_budget_rejects_unbounded_suffix_state() {
        let mut state = RegistryReplayState::default();
        state.registry_suffix_by_address.insert(
            "0x0000000000000000000000000000000000000001".to_owned(),
            "x".repeat(MAX_LIVE_REGISTRY_REPLAY_STATE_WEIGHT),
        );
        assert!(replay_state_weight(&state) > MAX_LIVE_REGISTRY_REPLAY_STATE_WEIGHT);
    }
}
