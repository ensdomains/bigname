use anyhow::{Context, Result, bail};
use bigname_lookup::{ChainRpcUrls, RpcChainCheckError, verify_chain_rpc_url};
use bigname_storage::walk_index_set::{IndexState, load_drop_list_state};
use sqlx::PgPool;

use crate::cli::WalkIndexSetCheck;

pub(crate) async fn ensure_verified_lookup_ddl_available(pool: &PgPool) -> Result<()> {
    let phase_schema_exists = bigname_storage::phase_schema_exists(pool)
        .await
        .context("API verified-lookup DDL preflight could not inspect the phase schema")?;
    if !phase_schema_exists {
        return Ok(());
    }

    let missing_ddl = bigname_storage::load_missing_api_lookup_ddl(pool)
        .await
        .context("API verified-lookup DDL preflight could not inspect required lookup DDL")?;
    if !missing_ddl.is_empty() {
        let diagnostics = missing_ddl
            .iter()
            .map(|object| format!("{}: {}", object.kind.as_str(), object.identity))
            .collect::<Vec<_>>()
            .join("\n");
        bail!(
            "API verified-lookup DDL preflight failed: required lookup objects are missing or serving relations are unreadable\n{diagnostics}"
        );
    }

    Ok(())
}

/// Refuses to serve, unless `check` is `warn`, when an index `ops/walk-index-set/drop.sql` drops is
/// not a valid, ready index on `normalized_events` in the database the API reads, which on a
/// physical standby is the primary's catalog as replayed. It reads the catalog only. Returns how
/// many are not, which the `walk_index_set_unusable` gauge and the boot line report. Without
/// the table there is nothing to check.
pub(crate) async fn ensure_walk_index_set(
    pool: &PgPool,
    check: WalkIndexSetCheck,
) -> Result<usize> {
    let mut connection = pool
        .acquire()
        .await
        .context("API walk index set check could not read the catalog")?;
    let statuses = load_drop_list_state(&mut connection)
        .await
        .context("API walk index set check could not read the catalog")?;
    drop(connection);
    let Some(statuses) = statuses else {
        crate::metrics::record_walk_index_set_unusable(0);
        return Ok(0);
    };
    for status in &statuses {
        if let Some(found) = status.other_definition() {
            tracing::warn!(
                service = "api",
                index = status.index.name,
                found,
                expected = status.index.definition,
                "a normalized_events index the walk index set scripts drop has another \
                 definition than this build expects, and the API reads it as found because a \
                 changed definition takes a new name (docs/storage.md, Walk index set)"
            );
        }
    }
    let unusable = statuses
        .iter()
        .filter_map(|status| {
            let name = status.index.name;
            match &status.state {
                IndexState::Present { .. } => None,
                IndexState::Missing => Some(format!("{name} (missing)")),
                IndexState::Invalid => Some(format!("{name} (not valid and ready)")),
                IndexState::Foreign { held_by } => Some(format!("{name} (held by {held_by})")),
            }
        })
        .collect::<Vec<_>>();
    crate::metrics::record_walk_index_set_unusable(unusable.len());
    if unusable.is_empty() {
        tracing::info!(
            service = "api",
            indexes = statuses.len(),
            walk_index_set_check = check.as_str(),
            "every normalized_events index the walk index set scripts drop is in place"
        );
        if check == WalkIndexSetCheck::Warn {
            tracing::warn!(
                service = "api",
                "BIGNAME_API_WALK_INDEX_SET_CHECK=warn is set, so a start without the indexes \
                 the walk index set scripts drop would serve instead of refusing"
            );
        }
        return Ok(0);
    }
    let listed = unusable.join(", ");
    match check {
        WalkIndexSetCheck::Refuse => {
            tracing::error!(
                service = "api",
                unusable = %listed,
                "API walk index set check refused to start"
            );
            bail!(
                "API walk index set check refused to start: {} of the {} normalized_events \
                 indexes the walk index set scripts drop are not in place: {listed}. The phase \
                 runner rebuilds them before Project, and ops/walk-index-set/install.sql \
                 rebuilds them by hand. BIGNAME_API_WALK_INDEX_SET_CHECK=warn serves without \
                 them.",
                unusable.len(),
                statuses.len()
            )
        }
        WalkIndexSetCheck::Warn => {
            tracing::warn!(
                service = "api",
                unusable = %listed,
                "the API serves without normalized_events indexes the walk index set scripts \
                 drop (BIGNAME_API_WALK_INDEX_SET_CHECK=warn), and reads that need them may \
                 exceed BIGNAME_API_DB_STATEMENT_TIMEOUT_MS"
            );
            Ok(unusable.len())
        }
    }
}

/// Warns, naming the table, when the API role cannot read the stored event counts
/// (`docs/deployment.md`, Serving the stored event counts). It never refuses: without the
/// privilege every emitter-anchored event count and the registry overview's `counts.events`
/// count rows, exact up to 10,000. A check that fails only warns too. Returns whether the
/// counts are readable.
pub(crate) async fn warn_on_unreadable_stored_event_counts(pool: &PgPool) -> bool {
    match bigname_storage::phase_schema_exists(pool).await {
        Ok(true) => {}
        Ok(false) => return false,
        Err(error) => {
            tracing::warn!(
                service = "api",
                error = %error,
                "the API could not check the phase schema for the stored event counts and \
                 serves event counts from the rows"
            );
            return false;
        }
    }
    match bigname_storage::stored_event_counts_readable(pool).await {
        Ok(true) => true,
        Ok(false) => {
            tracing::warn!(
                service = "api",
                table = "bigname_phase.project_event_count",
                "the API role cannot read the stored event counts, so it serves event counts \
                 from the rows, exact up to 10,000 and null above; grant SELECT on the table \
                 to serve them exactly"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                service = "api",
                table = "bigname_phase.project_event_count",
                error = %error,
                "the API could not check its privilege on the stored event counts and serves \
                 event counts from the rows until a read finds the privilege"
            );
            false
        }
    }
}

/// Refuses to serve when a configured RPC endpoint answers for another chain. An endpoint that
/// cannot be reached only warns, as lookups on that chain already fail on their own until it
/// answers.
pub(crate) async fn ensure_rpc_chains(urls: &ChainRpcUrls, check_genesis: bool) -> Result<()> {
    for (chain, _) in urls.iter() {
        let Some(expected_chain_id) = crate::v2::slug_to_numeric(chain) else {
            bail!(
                "API RPC chain check refused to start: chain {chain} has no known EIP-155 chain \
                 id, so its RPC endpoint cannot be checked"
            );
        };
        match verify_chain_rpc_url(urls, chain, expected_chain_id, check_genesis).await {
            Ok(()) => {}
            Err(RpcChainCheckError::Refused(message)) => {
                bail!("API RPC chain check refused to start: {message}")
            }
            Err(RpcChainCheckError::Unreachable(message)) => tracing::warn!(
                service = "api",
                chain,
                error = %message,
                "API RPC chain check could not reach the endpoint; lookups on this chain fail until it answers"
            ),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "startup_preflight_tests.rs"]
mod tests;
