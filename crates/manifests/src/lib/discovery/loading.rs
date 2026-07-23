use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgConnection};
use uuid::Uuid;

use super::admission::DiscoveryAdmissionState;
use super::provenance::TRANSITIVE_DISCOVERY_EDGE_KIND;
use super::types::{
    DiscoveryObservation, StoredActiveContract, StoredActiveRoot, StoredDiscoveryRule,
};
use crate::{
    ManifestRuntimeProgress, PROPAGATED_ROLE_PROVENANCE_FIELD, REACHABLE_FROM_ROOT_ADMISSION,
    ZERO_ADDRESS, normalize_address,
};

#[path = "loading/progress.rs"]
mod progress;

use super::reconciliation::DiscoveryObservationPageSource;
use progress::{
    AdmissionLoadProgress, AdmissionStateProgress, AdmissionStateProgressFuture,
    load_active_discovered_parent_rows_with_progress,
    load_known_contract_instance_addresses_with_progress,
};

struct PageSourceAdmissionProgress<'a, Source>(&'a Source);

impl<Source> AdmissionStateProgress for PageSourceAdmissionProgress<'_, Source>
where
    Source: DiscoveryObservationPageSource + Sync,
{
    fn record(&mut self) -> AdmissionStateProgressFuture<'_> {
        Box::pin(async move { self.0.record_progress().await })
    }
}

struct DiscoveryAdmissionScope {
    active_contract_chains: Vec<String>,
    active_contract_addresses: Vec<String>,
    known_address_chains: Vec<String>,
    known_address_addresses: Vec<String>,
}

/// How `known_contract_instances_by_address` is materialized on the loaded
/// admission state.
enum KnownAddressLoad {
    /// Every `contract_instance_addresses` row (historical behavior of the
    /// unscoped loaders).
    All,
    /// Only the scoped observation target addresses.
    Scoped,
    /// Left empty: the caller resolves known addresses per batch via
    /// `load_known_contract_instance_addresses` instead of holding the whole
    /// table in memory.
    Skip,
}

pub async fn load_discovery_admission_state(pool: &PgPool) -> Result<DiscoveryAdmissionState> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire connection for discovery admission state loading")?;
    load_discovery_admission_state_inner(&mut connection, None, None, KnownAddressLoad::All, None)
        .await
}

pub async fn load_discovery_admission_state_with_progress(
    pool: &PgPool,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<DiscoveryAdmissionState> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire connection for discovery admission state loading")?;
    let mut progress = AdmissionLoadProgress::new(pool, progress);
    load_discovery_admission_state_inner(
        &mut connection,
        None,
        None,
        KnownAddressLoad::All,
        Some(&mut progress),
    )
    .await
}

pub(super) async fn load_discovery_admission_state_with_excluded_source(
    executor: &mut PgConnection,
    excluded_discovery_source: Option<&str>,
) -> Result<DiscoveryAdmissionState> {
    load_discovery_admission_state_inner(
        executor,
        excluded_discovery_source,
        None,
        KnownAddressLoad::All,
        None,
    )
    .await
}

/// Admission state for the streamed full-source reconcile: roots, contracts,
/// rules, and transitive discovered parents load unscoped exactly as for the
/// in-memory full reconcile, but the known-address map stays empty — the
/// streamed walk resolves target addresses per observation page.
///
/// The unscoped discovered-parent load stays in memory deliberately: it is
/// filtered to active, non-orphaned, role-propagating (`subregistry` +
/// `reachable_from_root` + `propagated_role`) edges of OTHER discovery
/// sources, so for a per-chain registry source it is bounded by the other
/// chains'/families' derived registries — orders of magnitude below the
/// excluded source's own per-node edge count, and identical to what the
/// in-memory full reconcile already loads today.
pub(super) async fn load_streamed_discovery_admission_state_with_excluded_source(
    executor: &mut PgConnection,
    excluded_discovery_source: Option<&str>,
    source: &(impl DiscoveryObservationPageSource + Sync),
) -> Result<DiscoveryAdmissionState> {
    let mut progress = PageSourceAdmissionProgress(source);
    load_discovery_admission_state_inner(
        executor,
        excluded_discovery_source,
        None,
        KnownAddressLoad::Skip,
        Some(&mut progress),
    )
    .await
}

pub(super) async fn load_scoped_discovery_admission_state_with_excluded_source(
    executor: &mut PgConnection,
    excluded_discovery_source: Option<&str>,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryAdmissionState> {
    let (active_contract_chains, active_contract_addresses) =
        scoped_address_key_vectors(observations.iter().map(|observation| {
            (
                observation.chain.clone(),
                normalize_address(&observation.from_address),
            )
        }));
    let (known_address_chains, known_address_addresses) =
        scoped_address_key_vectors(observations.iter().filter_map(|observation| {
            let address = normalize_address(&observation.to_address);
            if address == ZERO_ADDRESS {
                None
            } else {
                Some((observation.chain.clone(), address))
            }
        }));

    load_discovery_admission_state_inner(
        executor,
        excluded_discovery_source,
        Some(DiscoveryAdmissionScope {
            active_contract_chains,
            active_contract_addresses,
            known_address_chains,
            known_address_addresses,
        }),
        KnownAddressLoad::Scoped,
        None,
    )
    .await
}

async fn load_discovery_admission_state_inner(
    executor: &mut PgConnection,
    excluded_discovery_source: Option<&str>,
    scope: Option<DiscoveryAdmissionScope>,
    known_address_load: KnownAddressLoad,
    mut progress: Option<&mut dyn AdmissionStateProgress>,
) -> Result<DiscoveryAdmissionState> {
    let (
        scoped,
        active_contract_scope_chains,
        active_contract_scope_addresses,
        known_address_scope_chains,
        known_address_scope_addresses,
    ) = match scope {
        Some(scope) => (
            true,
            scope.active_contract_chains,
            scope.active_contract_addresses,
            scope.known_address_chains,
            scope.known_address_addresses,
        ),
        None => (false, Vec::new(), Vec::new(), Vec::new(), Vec::new()),
    };

    let active_manifest_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE rollout_status = 'active'",
    )
    .fetch_one(&mut *executor)
    .await
    .context("failed to count active manifest versions")? as usize;
    if let Some(progress) = progress.as_deref_mut() {
        progress.record().await?;
    }

    let active_root_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mci.contract_instance_id, cia.address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'root'
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active manifest roots")?;
    if let Some(progress) = progress.as_deref_mut() {
        progress.record().await?;
    }

    let active_contract_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mci.role, mci.contract_instance_id, cia.address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active manifest contracts")?;
    if let Some(progress) = progress.as_deref_mut() {
        progress.record().await?;
    }

    // Scoped ordered replay may use earlier edges from the same source as
    // ancestry, including edges inserted earlier in the current transaction.
    // An edge whose start is already orphaned is never authority, even while
    // its superseding assignment is still waiting to be replayed.
    let active_discovered_parent_rows = if scoped {
        sqlx::query(
            r#"
            WITH scoped_addresses AS (
                SELECT DISTINCT chain_id, address
                FROM UNNEST($5::TEXT[], $6::TEXT[]) AS scope(chain_id, address)
            )
            SELECT
                mv.manifest_id,
                mv.chain,
                de.provenance ->> 'propagated_role' AS role,
                de.to_contract_instance_id AS contract_instance_id,
                cia.address AS address
            FROM scoped_addresses scope
            JOIN contract_instance_addresses cia
              ON cia.chain_id = scope.chain_id
             AND cia.address = scope.address
             AND cia.deactivated_at IS NULL
            JOIN discovery_edges de
              ON de.to_contract_instance_id = cia.contract_instance_id
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind = $4
              AND de.admission = $1
              AND de.provenance ? $2
              AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
              AND NOT EXISTS (
                  SELECT 1
                  FROM chain_lineage start_block
                  WHERE start_block.chain_id = de.chain_id
                    AND start_block.block_hash = de.active_from_block_hash
                    AND start_block.canonicality_state = 'orphaned'::canonicality_state
              )
            "#,
        )
        .bind(REACHABLE_FROM_ROOT_ADMISSION)
        .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
        .bind(excluded_discovery_source)
        .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
        .bind(&active_contract_scope_chains)
        .bind(&active_contract_scope_addresses)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load active transitive discovery parents")?
    } else if let Some(progress) = progress.as_deref_mut() {
        load_active_discovered_parent_rows_with_progress(
            executor,
            excluded_discovery_source,
            progress,
        )
        .await
        .context("failed to load active transitive discovery parents")?
    } else {
        sqlx::query(
            r#"
            SELECT
                mv.manifest_id,
                mv.chain,
                de.provenance ->> 'propagated_role' AS role,
                de.to_contract_instance_id AS contract_instance_id,
                cia.address AS address
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind = $4
              AND de.admission = $1
              AND de.provenance ? $2
              AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
              AND NOT EXISTS (
                  SELECT 1
                  FROM chain_lineage start_block
                  WHERE start_block.chain_id = de.chain_id
                    AND start_block.block_hash = de.active_from_block_hash
                    AND start_block.canonicality_state = 'orphaned'::canonicality_state
              )
            "#,
        )
        .bind(REACHABLE_FROM_ROOT_ADMISSION)
        .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
        .bind(excluded_discovery_source)
        .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load active transitive discovery parents")?
    };

    let active_rule_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mdr.edge_kind, mdr.from_role, mdr.admission
        FROM manifest_versions mv
        JOIN manifest_discovery_rules mdr ON mdr.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active discovery rules")?;
    if let Some(progress) = progress.as_deref_mut() {
        progress.record().await?;
    }

    let known_contract_instances_by_address = match known_address_load {
        KnownAddressLoad::All if progress.is_some() => {
            load_known_contract_instance_addresses_with_progress(
                executor,
                progress.as_deref_mut().expect("progress checked above"),
            )
            .await?
        }
        KnownAddressLoad::All => {
            // The contract_instance_id tiebreak keeps the per-key winner
            // deterministic when an address has only deactivated rows with
            // equal admitted_at, so per-batch resolution (the streamed
            // reconcile) and this full load always agree.
            let known_address_rows = sqlx::query(
                r#"
                SELECT chain_id, address, contract_instance_id
                FROM contract_instance_addresses
                ORDER BY chain_id, address, (deactivated_at IS NULL) DESC, admitted_at DESC,
                         contract_instance_id
                "#,
            )
            .fetch_all(&mut *executor)
            .await
            .context("failed to load known contract-instance addresses")?;
            fold_known_contract_instance_addresses(known_address_rows)?
        }
        KnownAddressLoad::Scoped => {
            load_known_contract_instance_addresses(
                executor,
                &known_address_scope_chains,
                &known_address_scope_addresses,
            )
            .await?
        }
        KnownAddressLoad::Skip => HashMap::new(),
    };

    let mut active_roots = Vec::with_capacity(active_root_rows.len());
    for row in active_root_rows {
        active_roots.push(StoredActiveRoot {
            manifest_id: row
                .try_get("manifest_id")
                .context("failed to read active root manifest_id")?,
            chain: row
                .try_get("chain")
                .context("failed to read active root chain")?,
            _contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read active root contract_instance_id")?,
            address: normalize_address(
                &row.try_get::<String, _>("address")
                    .context("failed to read active root address")?,
            ),
        });
        if active_roots
            .len()
            .is_multiple_of(progress::ADMISSION_LOAD_ROWS)
            && let Some(progress) = progress.as_deref_mut()
        {
            progress.record().await?;
        }
    }
    let active_root_manifest_ids = active_roots.iter().map(|root| root.manifest_id).collect();

    let mut active_contract_set = HashSet::new();
    let mut examined_contract_rows = 0usize;
    for row in active_contract_rows
        .into_iter()
        .chain(active_discovered_parent_rows)
    {
        active_contract_set.insert(StoredActiveContract {
            manifest_id: row
                .try_get("manifest_id")
                .context("failed to read active contract manifest_id")?,
            chain: row
                .try_get("chain")
                .context("failed to read active contract chain")?,
            role: row
                .try_get("role")
                .context("failed to read active contract role")?,
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read active contract contract_instance_id")?,
            address: normalize_address(
                &row.try_get::<String, _>("address")
                    .context("failed to read active contract address")?,
            ),
        });
        examined_contract_rows += 1;
        if examined_contract_rows.is_multiple_of(progress::ADMISSION_LOAD_ROWS)
            && let Some(progress) = progress.as_deref_mut()
        {
            progress.record().await?;
        }
    }
    let mut active_contracts = Vec::with_capacity(active_contract_set.len());
    for contract in active_contract_set {
        active_contracts.push(contract);
        if active_contracts
            .len()
            .is_multiple_of(progress::ADMISSION_LOAD_ROWS)
            && let Some(progress) = progress.as_deref_mut()
        {
            progress.record().await?;
        }
    }

    let mut rules_by_manifest_id: HashMap<i64, Vec<StoredDiscoveryRule>> = HashMap::new();
    let mut loaded_rule_count = 0usize;
    for row in active_rule_rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read active rule manifest_id")?;
        let rule = StoredDiscoveryRule {
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read active rule edge_kind")?,
            from_role: row
                .try_get("from_role")
                .context("failed to read active rule from_role")?,
            admission: row
                .try_get("admission")
                .context("failed to read active rule admission")?,
        };
        rules_by_manifest_id
            .entry(manifest_id)
            .or_default()
            .push(rule);
        loaded_rule_count += 1;
        if loaded_rule_count.is_multiple_of(progress::ADMISSION_LOAD_ROWS)
            && let Some(progress) = progress.as_deref_mut()
        {
            progress.record().await?;
        }
    }

    let active_rule_count = rules_by_manifest_id.values().map(Vec::len).sum();

    Ok(DiscoveryAdmissionState {
        active_manifest_count,
        active_root_count: active_roots.len(),
        active_contract_count: active_contracts.len(),
        active_rule_count,
        active_roots,
        active_root_manifest_ids,
        active_contracts,
        known_contract_instances_by_address,
        rules_by_manifest_id,
    })
}

/// Resolve `contract_instance_addresses` for one batch of
/// `(chain, normalized address)` keys, preferring the active and most
/// recently admitted row per key exactly like the full known-address load.
pub(super) async fn load_known_contract_instance_addresses(
    executor: &mut PgConnection,
    chains: &[String],
    addresses: &[String],
) -> Result<HashMap<(String, String), Uuid>> {
    if chains.is_empty() {
        return Ok(HashMap::new());
    }

    let known_address_rows = sqlx::query(
        r#"
        WITH scoped_addresses AS (
            SELECT DISTINCT chain_id, address
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS scope(chain_id, address)
        )
        SELECT cia.chain_id, cia.address, cia.contract_instance_id
        FROM scoped_addresses scope
        JOIN contract_instance_addresses cia
          ON cia.chain_id = scope.chain_id
         AND cia.address = scope.address
        ORDER BY cia.chain_id, cia.address, (cia.deactivated_at IS NULL) DESC, cia.admitted_at DESC,
                 cia.contract_instance_id
        "#,
    )
    .bind(chains)
    .bind(addresses)
    .fetch_all(&mut *executor)
    .await
    .context("failed to load known contract-instance addresses")?;

    fold_known_contract_instance_addresses(known_address_rows)
}

fn fold_known_contract_instance_addresses(
    known_address_rows: Vec<sqlx::postgres::PgRow>,
) -> Result<HashMap<(String, String), Uuid>> {
    let mut known_contract_instances_by_address = HashMap::new();
    for row in known_address_rows {
        let chain = row
            .try_get::<String, _>("chain_id")
            .context("failed to read known address chain_id")?;
        let address = normalize_address(
            &row.try_get::<String, _>("address")
                .context("failed to read known address")?,
        );
        known_contract_instances_by_address
            .entry((chain, address))
            .or_insert(
                row.try_get("contract_instance_id")
                    .context("failed to read known address contract_instance_id")?,
            );
    }
    Ok(known_contract_instances_by_address)
}

pub(super) fn scoped_address_key_vectors(
    keys: impl Iterator<Item = (String, String)>,
) -> (Vec<String>, Vec<String>) {
    let mut keys = keys.collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter().unzip()
}
