use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    IdentityNameRecordRow, IdentityPrimaryNameSnapshot, PrimaryNameClaimStatus,
    READABLE_REVERSE_IDENTITY_CTES, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityRoles, ReverseIdentityStorageInput,
};
use sqlx::{PgPool, Row};

mod page;

#[cfg(test)]
pub(crate) mod relation_page_test_hooks {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    #[derive(Clone)]
    pub(crate) struct RelationPageHook {
        calls: Arc<AtomicUsize>,
        paused: Arc<AtomicBool>,
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct RelationPageControl {
        calls: Arc<AtomicUsize>,
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl RelationPageControl {
        pub(crate) fn page_loader_call_count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, RelationPageHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, RelationPageHook>,
        RelationPageControl,
    )> {
        let database = current_test_database(pool).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            RelationPageHook {
                calls: Arc::clone(&calls),
                paused: Arc::new(AtomicBool::new(false)),
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((
            guard,
            RelationPageControl {
                calls,
                reached,
                resume,
            },
        ))
    }

    pub(super) async fn record_page_load(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.get_cloned(&database) {
            hook.calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(crate) async fn pause_before_additional_scan(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.get_cloned(&database)
            && hook
                .paused
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;

    static COUNT_CALLS: ScopedTestHookRegistry<String, Arc<AtomicUsize>> =
        ScopedTestHookRegistry::new();

    pub(crate) struct CountCallControl(Arc<AtomicUsize>);

    impl CountCallControl {
        pub(crate) fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, Arc<AtomicUsize>>,
        CountCallControl,
    )> {
        let database = current_test_database(pool).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = COUNT_CALLS.install(database, Arc::clone(&calls));
        Ok((guard, CountCallControl(calls)))
    }

    pub(super) async fn record(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(calls) = COUNT_CALLS.get_cloned(&database) {
            calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod primary_coherence_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::{Barrier, Notify};

    #[derive(Clone)]
    pub(crate) struct PrimaryCoherenceHook {
        candidate_read: Arc<Notify>,
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct PrimaryCoherenceControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl PrimaryCoherenceControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, PrimaryCoherenceHook> =
        ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, PrimaryCoherenceHook>,
        PrimaryCoherenceControl,
    )> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            PrimaryCoherenceHook {
                candidate_read: Arc::new(Notify::new()),
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, PrimaryCoherenceControl { reached, resume }))
    }

    pub(crate) async fn uninstall(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        HOOKS.take(&database);
        Ok(())
    }

    pub(super) async fn candidate_read_complete(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.get_cloned(&database) {
            hook.candidate_read.notify_one();
        }
        Ok(())
    }

    pub(super) async fn pause_after_candidate_read(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.get_cloned(&database) {
            hook.candidate_read.notified().await;
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ReverseIdentityPageRow {
    input_index: usize,
    logical_name_id: String,
    primary_name: Option<IdentityPrimaryNameSnapshot>,
}

pub(crate) async fn load_reverse_identity_records_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
) -> Result<Vec<ReverseIdentityGroup>> {
    load_reverse_identity_records_live_with_count_mode(
        pool,
        inputs,
        public_namespaces,
        ReverseCountMode::Include,
    )
    .await
}

pub(crate) async fn load_reverse_identity_records_page_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
) -> Result<Vec<ReverseIdentityGroup>> {
    #[cfg(test)]
    relation_page_test_hooks::record_page_load(pool).await?;
    load_reverse_identity_records_live_with_count_mode(
        pool,
        inputs,
        public_namespaces,
        ReverseCountMode::Omit,
    )
    .await
}

pub(crate) async fn prepare_reverse_identity_additional_scan(
    _pool: &PgPool,
) -> crate::v2::V2Result<()> {
    #[cfg(test)]
    relation_page_test_hooks::pause_before_additional_scan(_pool)
        .await
        .map_err(|_| crate::v2::V2Error::internal_error("failed to run reverse-page test hook"))?;
    Ok(())
}

#[derive(Clone, Copy)]
enum ReverseCountMode {
    Include,
    Omit,
}

async fn load_reverse_identity_records_live_with_count_mode(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
    count_mode: ReverseCountMode,
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let first_page_feed = inputs
        .iter()
        .all(|input| input.page_size == 1 && input.cursor.is_none());
    let page_records_future = async {
        let page_rows =
            page::load_reverse_identity_page_rows(pool, inputs, public_namespaces).await?;
        let logical_name_ids =
            dedupe_in_order(page_rows.iter().map(|row| row.logical_name_id.clone()));
        let name_records =
            bigname_storage::load_phase_identity_records_by_ids(pool, &logical_name_ids)
                .await?
                .into_iter()
                .map(|record| (record.row.logical_name_id.clone(), record))
                .collect::<BTreeMap<_, _>>();
        Result::<_>::Ok((page_rows, name_records))
    };

    let total_counts_future = async {
        match count_mode {
            ReverseCountMode::Include => {
                load_reverse_identity_total_counts_live(pool, inputs, public_namespaces)
                    .await
                    .map(Some)
            }
            ReverseCountMode::Omit => Ok(None),
        }
    };
    let ((page_rows, name_records), total_counts) =
        tokio::try_join!(page_records_future, total_counts_future)?;

    let rows_by_input = page_rows.into_iter().fold(
        BTreeMap::<usize, Vec<ReverseIdentityPageRow>>::new(),
        |mut grouped, row| {
            grouped.entry(row.input_index).or_default().push(row);
            grouped
        },
    );

    Ok(inputs
        .iter()
        .enumerate()
        .map(|(input_index, input)| {
            let mut entries = rows_by_input
                .get(&input_index)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|row| reverse_identity_record(&name_records, input, row))
                .collect::<Vec<_>>();
            let total_count = total_counts.as_ref().map(|counts| {
                *counts
                    .get(&(input.address.clone(), input.roles))
                    .unwrap_or(&0)
            });
            let has_more = match (first_page_feed, total_count) {
                (true, Some(total_count)) => {
                    total_count > input.page_size.max(0) as u64 && !entries.is_empty()
                }
                _ => entries.len() as i64 > input.page_size,
            };
            entries.truncate(input.page_size.max(0) as usize);

            ReverseIdentityGroup {
                input: input.clone(),
                entries,
                total_count,
                has_more,
            }
        })
        .collect())
}

fn reverse_identity_record(
    name_records: &BTreeMap<String, IdentityNameRecordRow>,
    input: &ReverseIdentityStorageInput,
    row: ReverseIdentityPageRow,
) -> Option<ReverseIdentityRecordRow> {
    let name_record = name_records.get(&row.logical_name_id)?.clone();
    let primary_name = row.primary_name;
    let mut relation_facets = name_record
        .relations
        .iter()
        .filter(|relation| {
            relation.address == input.address && input.roles.includes(relation.relation)
        })
        .map(|relation| relation.relation)
        .collect::<Vec<_>>();
    relation_facets.sort();
    relation_facets.dedup();

    Some(ReverseIdentityRecordRow {
        name_record,
        relation_facets,
        primary_chain_positions: primary_name
            .as_ref()
            .and_then(|primary| primary.chain_positions.clone()),
        primary_name,
        requested_coin_type: input.coin_type.clone(),
    })
}

async fn load_reverse_identity_total_counts_live(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
    public_namespaces: &[String],
) -> Result<BTreeMap<(String, ReverseIdentityRoles), u64>> {
    #[cfg(test)]
    test_hooks::record(pool).await?;

    let requests = inputs
        .iter()
        .map(|input| (input.address.clone(), input.roles))
        .collect::<BTreeSet<_>>();
    let addresses = requests
        .iter()
        .map(|(address, _)| address.clone())
        .collect::<Vec<_>>();
    let roles = requests
        .iter()
        .map(|(_, roles)| roles_storage_value(*roles).to_owned())
        .collect::<Vec<_>>();

    let query = format!(
        r#"
        WITH {READABLE_REVERSE_IDENTITY_CTES}, requested AS (
            SELECT *
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS requested(address, roles)
        )
        SELECT
            requested.address,
            requested.roles,
            COUNT(DISTINCT anc.logical_name_id)::BIGINT AS total_count
        FROM requested
        LEFT JOIN readable_relations anc
          ON anc.address = requested.address
         AND anc.namespace = ANY($3::TEXT[])
         AND (
             requested.roles = 'both'
             OR (requested.roles = 'owned' AND anc.relation IN ('registrant', 'token_holder'))
             OR (requested.roles = 'managed' AND anc.relation = 'effective_controller')
         )
        GROUP BY requested.address, requested.roles
        ORDER BY requested.address, requested.roles
        "#
    );
    let rows = sqlx::query(&query)
        .bind(&addresses)
        .bind(&roles)
        .bind(public_namespaces)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to live-count reverse identity rows for {} inputs",
                inputs.len()
            )
        })?;

    rows.into_iter()
        .map(|row| {
            let address = row.try_get::<String, _>("address")?;
            let roles = parse_roles(&row.try_get::<String, _>("roles")?)?;
            let total_count = row.try_get::<i64, _>("total_count")?;
            Ok(((address, roles), u64::try_from(total_count).unwrap_or(0)))
        })
        .collect()
}

pub(super) fn parse_primary_name_claim_status(value: &str) -> Result<PrimaryNameClaimStatus> {
    match value {
        "success" => Ok(PrimaryNameClaimStatus::Success),
        "not_found" => Ok(PrimaryNameClaimStatus::NotFound),
        "unsupported" => Ok(PrimaryNameClaimStatus::Unsupported),
        "invalid_name" => Ok(PrimaryNameClaimStatus::InvalidName),
        _ => bail!("unknown identity primary-name status {value}"),
    }
}

fn roles_storage_value(roles: ReverseIdentityRoles) -> &'static str {
    match roles {
        ReverseIdentityRoles::Owned => "owned",
        ReverseIdentityRoles::Managed => "managed",
        ReverseIdentityRoles::Both => "both",
    }
}

fn parse_roles(value: &str) -> Result<ReverseIdentityRoles> {
    match value {
        "owned" => Ok(ReverseIdentityRoles::Owned),
        "managed" => Ok(ReverseIdentityRoles::Managed),
        "both" => Ok(ReverseIdentityRoles::Both),
        _ => bail!("unknown reverse identity roles {value}"),
    }
}

fn dedupe_in_order(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}
