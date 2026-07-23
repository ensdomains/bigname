use super::*;
use bigname_storage::sql_row;
use sqlx::postgres::PgRow;

use crate::checkpoint_context::StartupAdapterProgress;

const REVERSE_CLAIM_PROGRESS_ROWS: i64 = 1_000;

pub(super) async fn load_reverse_claim_sources(
    pool: &PgPool,
    chain: &str,
) -> Result<HashMap<String, ReverseClaimSource>> {
    load_reverse_claim_sources_internal(pool, chain, None).await
}

pub(super) async fn load_reverse_claim_sources_with_progress(
    pool: &PgPool,
    chain: &str,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<HashMap<String, ReverseClaimSource>> {
    let mut sources = HashMap::new();
    let mut after_reverse_node = None::<String>;
    loop {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT ON (LOWER(ne.after_state->>'reverse_node'))
                LOWER(ne.after_state->>'reverse_node') AS reverse_node,
                LOWER(ne.after_state->>'address') AS address,
                COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
                ne.after_state->>'coin_type' AS coin_type,
                ne.after_state->>'reverse_name' AS reverse_name,
                COALESCE(
                    ne.after_state->'claim_provenance'->>'source_family',
                    ne.source_family
                ) AS claim_source_family,
                COALESCE(
                    ne.after_state->'claim_provenance'->>'contract_role',
                    $4
                ) AS claim_contract_role,
                ne.after_state->'claim_provenance'->>'contract_instance_id' AS claim_contract_instance_id,
                COALESCE(
                    ne.after_state->'claim_provenance'->>'emitting_address',
                    ne.raw_fact_ref->>'emitting_address'
                ) AS claim_emitting_address
            FROM normalized_events ne
            WHERE ne.chain_id = $1
              AND COALESCE(ne.after_state->>'namespace', ne.namespace) IN ($2, $3)
              AND ne.event_kind = $5
              AND ne.derivation_kind = $6
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.after_state->>'reverse_node' IS NOT NULL
              AND ne.after_state->>'reverse_node' <> ''
              AND ne.after_state->>'address' IS NOT NULL
              AND ne.after_state->>'address' <> ''
              AND ne.after_state->>'coin_type' IS NOT NULL
              AND ne.after_state->>'coin_type' <> ''
              AND ne.after_state->>'reverse_name' IS NOT NULL
              AND ne.after_state->>'reverse_name' <> ''
              AND (
                  $7::TEXT IS NULL
                  OR LOWER(ne.after_state->>'reverse_node') > $7::TEXT
              )
            ORDER BY
                LOWER(ne.after_state->>'reverse_node'),
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
            LIMIT $8
            "#,
        )
        .bind(chain)
        .bind("ens")
        .bind("basenames")
        .bind(CONTRACT_ROLE_REVERSE_REGISTRAR)
        .bind(EVENT_KIND_REVERSE_CHANGED)
        .bind(DERIVATION_KIND_ENS_V1_REVERSE_CLAIM)
        .bind(after_reverse_node.as_deref())
        .bind(REVERSE_CLAIM_PROGRESS_ROWS)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to page reverse claim sources for chain {chain}"))?;
        if rows.is_empty() {
            break;
        }
        let page_len = rows.len();
        for row in rows {
            let (reverse_node, source) = reverse_claim_source_from_row(&row)?;
            after_reverse_node = Some(reverse_node.clone());
            sources.insert(reverse_node, source);
        }
        progress.record(pool).await?;
        if page_len < usize::try_from(REVERSE_CLAIM_PROGRESS_ROWS).expect("page limit fits usize") {
            break;
        }
    }
    Ok(sources)
}

pub(super) async fn load_reverse_claim_sources_for_nodes(
    pool: &PgPool,
    chain: &str,
    reverse_nodes: &[String],
) -> Result<HashMap<String, ReverseClaimSource>> {
    if reverse_nodes.is_empty() {
        return Ok(HashMap::new());
    }

    load_reverse_claim_sources_internal(pool, chain, Some(reverse_nodes)).await
}

async fn load_reverse_claim_sources_internal(
    pool: &PgPool,
    chain: &str,
    reverse_nodes: Option<&[String]>,
) -> Result<HashMap<String, ReverseClaimSource>> {
    let rows = if let Some(reverse_nodes) = reverse_nodes {
        sqlx::query(
            r#"
        SELECT DISTINCT ON (LOWER(ne.after_state->>'reverse_node'))
            LOWER(ne.after_state->>'reverse_node') AS reverse_node,
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            ne.after_state->>'reverse_name' AS reverse_name,
            COALESCE(
                ne.after_state->'claim_provenance'->>'source_family',
                ne.source_family
            ) AS claim_source_family,
            COALESCE(
                ne.after_state->'claim_provenance'->>'contract_role',
                $4
            ) AS claim_contract_role,
            ne.after_state->'claim_provenance'->>'contract_instance_id' AS claim_contract_instance_id,
            COALESCE(
                ne.after_state->'claim_provenance'->>'emitting_address',
                ne.raw_fact_ref->>'emitting_address'
            ) AS claim_emitting_address
        FROM normalized_events ne
        WHERE ne.chain_id = $1
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) IN ($2, $3)
          AND ne.event_kind = $5
          AND ne.derivation_kind = $6
          AND LOWER(ne.after_state->>'reverse_node') = ANY($7::TEXT[])
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND ne.after_state->>'reverse_node' IS NOT NULL
          AND ne.after_state->>'reverse_node' <> ''
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
          AND ne.after_state->>'reverse_name' IS NOT NULL
          AND ne.after_state->>'reverse_name' <> ''
        ORDER BY
            LOWER(ne.after_state->>'reverse_node'),
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
        )
        .bind(chain)
        .bind("ens")
        .bind("basenames")
        .bind(CONTRACT_ROLE_REVERSE_REGISTRAR)
        .bind(EVENT_KIND_REVERSE_CHANGED)
        .bind(DERIVATION_KIND_ENS_V1_REVERSE_CLAIM)
        .bind(reverse_nodes)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load scoped reverse claim sources for chain {chain}")
        })?
    } else {
        sqlx::query(
        r#"
        SELECT DISTINCT ON (LOWER(ne.after_state->>'reverse_node'))
            LOWER(ne.after_state->>'reverse_node') AS reverse_node,
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            ne.after_state->>'reverse_name' AS reverse_name,
            COALESCE(
                ne.after_state->'claim_provenance'->>'source_family',
                ne.source_family
            ) AS claim_source_family,
            COALESCE(
                ne.after_state->'claim_provenance'->>'contract_role',
                $4
            ) AS claim_contract_role,
            ne.after_state->'claim_provenance'->>'contract_instance_id' AS claim_contract_instance_id,
            COALESCE(
                ne.after_state->'claim_provenance'->>'emitting_address',
                ne.raw_fact_ref->>'emitting_address'
            ) AS claim_emitting_address
        FROM normalized_events ne
        WHERE ne.chain_id = $1
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) IN ($2, $3)
          AND ne.event_kind = $5
          AND ne.derivation_kind = $6
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND ne.after_state->>'reverse_node' IS NOT NULL
          AND ne.after_state->>'reverse_node' <> ''
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
          AND ne.after_state->>'reverse_name' IS NOT NULL
          AND ne.after_state->>'reverse_name' <> ''
        ORDER BY
            LOWER(ne.after_state->>'reverse_node'),
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
        )
        .bind(chain)
        .bind("ens")
        .bind("basenames")
        .bind(CONTRACT_ROLE_REVERSE_REGISTRAR)
        .bind(EVENT_KIND_REVERSE_CHANGED)
        .bind(DERIVATION_KIND_ENS_V1_REVERSE_CLAIM)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load reverse claim sources for chain {chain}"))?
    };

    rows.iter().map(reverse_claim_source_from_row).collect()
}

fn reverse_claim_source_from_row(row: &PgRow) -> Result<(String, ReverseClaimSource)> {
    let reverse_node = sql_row::get::<String>(row, "reverse_node")?;
    let address = row
        .try_get::<String, _>("address")
        .context("missing reverse claim address")?;
    let namespace = row
        .try_get::<String, _>("namespace")
        .context("missing reverse claim namespace")?;
    let coin_type = row
        .try_get::<String, _>("coin_type")
        .context("missing reverse claim coin_type")?;
    let reverse_name = row
        .try_get::<String, _>("reverse_name")
        .context("missing reverse claim reverse_name")?;

    Ok((
        reverse_node.clone(),
        ReverseClaimSource {
            address,
            namespace,
            coin_type,
            reverse_name,
            reverse_node,
            claim_provenance: ReverseClaimProvenance {
                source_family: row
                    .try_get::<String, _>("claim_source_family")
                    .context("missing reverse claim source_family")?,
                contract_role: row
                    .try_get::<String, _>("claim_contract_role")
                    .context("missing reverse claim contract_role")?,
                contract_instance_id: row
                    .try_get("claim_contract_instance_id")
                    .context("missing reverse claim contract_instance_id column")?,
                emitting_address: row
                    .try_get("claim_emitting_address")
                    .context("missing reverse claim emitting_address column")?,
            },
        },
    ))
}

pub(super) fn apply_reverse_claim_source_observation(
    history: &mut ReverseClaimSourceHistory,
    observation: AuthorityObservation,
    resolver_fact_profile_status: Option<ResolverFactProfileStatus>,
) -> Result<()> {
    match observation {
        AuthorityObservation::ResolverChanged(event) => {
            let before_resolver = history.current_resolver.clone();
            let before_normalized_resolver = nonzero_address(before_resolver.as_deref());
            let after_normalized_resolver = nonzero_address(Some(event.resolver.as_str()));
            if before_normalized_resolver != after_normalized_resolver {
                history.current_record_version = None;
            }
            history.current_resolver = Some(event.resolver.clone());
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RESOLVER_CHANGED,
                json!({
                    "resolver": before_resolver,
                }),
                resolver_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "resolver:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordChanged(event) => {
            if !current_reverse_source_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            if event.selector.record_key != "name" {
                return Ok(());
            }
            // Generic ENSv1 NameChanged intake retains one reverse-source
            // observation across profile reclassification, but only a
            // supported name profile may turn it into declared primary-name
            // identity. Pending evidence keeps the enrichment-compatible
            // identity. Explicitly unsupported evidence uses a distinct
            // identity so reconciliation can orphan a prior claim without
            // rewriting its durable provenance.
            let (claim_source, identity_kind) = match resolver_fact_profile_status
                .context("reverse-name record observation is missing resolver profile status")?
            {
                ResolverFactProfileStatus::Supported => {
                    (Some(&history.claim_source), "record-change")
                }
                ResolverFactProfileStatus::Pending => (None, "record-change"),
                ResolverFactProfileStatus::Unsupported => (None, "record-change-unsupported"),
            };
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                record_changed_after_state(&event, claim_source),
                format!(
                    "{identity_kind}:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordVersionChanged(event) => {
            if !current_reverse_source_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let before_version = history.current_record_version;
            history.current_record_version = Some(event.record_version);
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RECORD_VERSION_CHANGED,
                json!({
                    "record_version": before_version,
                }),
                record_version_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "record-version:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RegistrationGranted(_)
        | AuthorityObservation::RegistrationRenewed(_)
        | AuthorityObservation::TokenTransferred(_)
        | AuthorityObservation::RegistryOwnerChanged(_)
        | AuthorityObservation::WrapperNameWrapped(_)
        | AuthorityObservation::WrapperNameUnwrapped(_)
        | AuthorityObservation::WrapperFusesSet(_)
        | AuthorityObservation::WrapperExpiryExtended(_)
        | AuthorityObservation::WrapperTokenTransferred(_) => {}
    }

    Ok(())
}

fn current_reverse_source_resolver_matches(
    history: &ReverseClaimSourceHistory,
    observed_resolver: &str,
) -> bool {
    match (
        nonzero_address(history.current_resolver.as_deref()),
        nonzero_address(Some(observed_resolver)),
    ) {
        (Some(current), Some(observed)) => current == observed,
        _ => false,
    }
}
