use std::{collections::BTreeSet, sync::Mutex};

use anyhow::Result;

static POST_DISCOVERY_MUTATION_FAILURE_POOLS: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());

pub(crate) fn install_post_discovery_mutation_failure(pool: &sqlx::PgPool) {
    POST_DISCOVERY_MUTATION_FAILURE_POOLS
        .lock()
        .expect("post-discovery mutation failure hook must not be poisoned")
        .insert(pool as *const sqlx::PgPool as usize);
}

pub(super) fn fail_after_discovery_mutation(pool: &sqlx::PgPool) -> Result<()> {
    if POST_DISCOVERY_MUTATION_FAILURE_POOLS
        .lock()
        .expect("post-discovery mutation failure hook must not be poisoned")
        .remove(&(pool as *const sqlx::PgPool as usize))
    {
        anyhow::bail!("injected failure after committed discovery mutation");
    }
    Ok(())
}
