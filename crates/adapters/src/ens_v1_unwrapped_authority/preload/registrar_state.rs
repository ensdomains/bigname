use super::registrar_history::registrar_expiry_from_provenance_or_binding_end;
use super::support::*;
use super::*;

pub(super) async fn block_index_with_preloaded_registrar_release_boundaries(
    pool: &PgPool,
    chain: &str,
    rows: &[sqlx::postgres::PgRow],
    registrar_state: &HashMap<String, PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<CanonicalBlockIndex> {
    let Some(replay_head) = block_index.blocks.last() else {
        return Ok(block_index.clone());
    };
    let mut release_timestamps = Vec::new();
    let mut release_namespaces = Vec::new();

    for row in rows {
        let resource_provenance: Value = sql_row::get(&row, "resource_provenance")?;
        if resource_provenance
            .get("authority_kind")
            .and_then(Value::as_str)
            != Some("registrar")
        {
            continue;
        }

        let logical_name_id: String = sql_row::get(&row, "logical_name_id")?;
        let expiry = if let Some(expiry) = registrar_state
            .get(&logical_name_id)
            .and_then(|state| state.expiry)
        {
            expiry
        } else {
            let active_to = row
                .try_get("active_to")
                .context("missing binding active_to")?;
            let expiry =
                registrar_expiry_from_provenance_or_binding_end(&resource_provenance, active_to)?;
            OffsetDateTime::from_unix_timestamp(expiry)
                .context("preloaded registrar expiry is not a valid unix timestamp")?
        };
        let release_timestamp = release_after_grace(expiry)?;
        if release_timestamp <= replay_head.block_timestamp {
            release_timestamps.push(release_timestamp);
            release_namespaces.push(sql_row::get(&row, "namespace")?);
        }
    }

    if release_timestamps.is_empty() {
        return Ok(block_index.clone());
    }

    let release_blocks = load_release_boundary_blocks_for_timestamps(
        pool,
        chain,
        &release_timestamps,
        &release_namespaces,
        replay_head,
    )
    .await?;
    if release_blocks.is_empty() {
        return Ok(block_index.clone());
    }

    let mut blocks = block_index.blocks.clone();
    blocks.extend(release_blocks);
    sort_and_dedup_blocks(&mut blocks);
    Ok(CanonicalBlockIndex { blocks })
}

async fn load_release_boundary_blocks_for_timestamps(
    pool: &PgPool,
    chain: &str,
    release_timestamps: &[OffsetDateTime],
    release_namespaces: &[String],
    replay_head: &RawBlockSnapshot,
) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (requested.release_timestamp, requested.namespace)
            rb.chain_id,
            rb.block_hash,
            rb.block_number,
            rb.block_timestamp,
            rb.canonicality_state::TEXT AS canonicality_state
        FROM unnest($2::TIMESTAMPTZ[], $3::TEXT[]) AS requested(
            release_timestamp,
            namespace
        )
        JOIN LATERAL (
            SELECT
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_timestamp >= requested.release_timestamp
              AND block_timestamp <= $4
              AND block_number <= $5
              AND canonicality_state {CANONICALITY_STATE_FILTER}
            ORDER BY block_timestamp, block_number
            LIMIT 1
        ) rb ON TRUE
        ORDER BY requested.release_timestamp, requested.namespace, rb.block_timestamp, rb.block_number
        "#
    ))
    .bind(chain)
    .bind(release_timestamps)
    .bind(release_namespaces)
    .bind(replay_head.block_timestamp)
    .bind(replay_head.block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load ENSv1 preloaded registrar release boundary blocks for chain {chain}"
        )
    })?;

    rows.into_iter().map(raw_block_snapshot_from_row).collect()
}

pub(super) async fn load_latest_registrar_state_before_block(
    pool: &PgPool,
    scopes: &[RegistrarStateScope],
    boundary_block: i64,
) -> Result<HashMap<String, PreloadedRegistrarState>> {
    if scopes.is_empty() {
        return Ok(HashMap::new());
    }
    let logical_name_ids = scopes
        .iter()
        .map(|scope| scope.logical_name_id.clone())
        .collect::<Vec<_>>();
    let lower_block_numbers = scopes
        .iter()
        .map(|scope| scope.lower_block_number)
        .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"
        WITH scope AS (
            SELECT *
            FROM UNNEST($1::TEXT[], $2::BIGINT[]) AS scope(logical_name_id, lower_block_number)
        ),
        candidates AS (
            SELECT
                event.logical_name_id,
                CASE
                    WHEN event.event_kind IN ($4, $5, $6)
                    THEN (event.after_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                CASE
                    WHEN event.event_kind = $4 THEN event.after_state->>'registrant'
                    WHEN event.event_kind = $7 THEN event.after_state->>'to'
                    ELSE NULL
                END AS registrant,
                event.block_number,
                COALESCE(event.log_index, -1) AS log_index,
                event.normalized_event_id
            FROM normalized_events event
            JOIN scope
              ON scope.logical_name_id = event.logical_name_id
            WHERE event.block_number >= scope.lower_block_number
              AND event.block_number < $3
              AND event.event_kind IN ($4, $5, $6, $7)
              AND event.canonicality_state {CANONICALITY_STATE_FILTER}
        )
        SELECT
            logical_name_id,
            (
                ARRAY_AGG(
                    expiry
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (WHERE expiry IS NOT NULL)
            )[1] AS expiry,
            (
                ARRAY_AGG(
                    registrant
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (WHERE registrant IS NOT NULL)
            )[1] AS registrant
        FROM candidates
        GROUP BY logical_name_id
        "#
    ))
    .bind(logical_name_ids)
    .bind(lower_block_numbers)
    .bind(boundary_block)
    .bind(EVENT_KIND_REGISTRATION_GRANTED)
    .bind(EVENT_KIND_REGISTRATION_RENEWED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .fetch_all(pool)
    .await
    .context("failed to preload latest registrar state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let expiry = row
            .try_get::<Option<i64>, _>("expiry")?
            .map(|value| {
                OffsetDateTime::from_unix_timestamp(value)
                    .context("preloaded registrar expiry is not a valid unix timestamp")
            })
            .transpose()?;
        let registrant = row.try_get("registrant")?;
        state.insert(
            row.try_get("logical_name_id")?,
            PreloadedRegistrarState {
                expiry,
                registrant,
                authority_key: None,
                labelhash: None,
                start_ref: None,
            },
        );
    }
    Ok(state)
}

pub(super) async fn load_selected_registrar_state_before_replay(
    pool: &PgPool,
    _logical_name_ids: &[String],
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<HashMap<String, PreloadedRegistrarState>> {
    if raw_logs.is_empty() {
        return Ok(HashMap::new());
    }

    let event_identities = selected_registrar_event_identities(raw_logs, event_topics)?;
    if event_identities.is_empty() {
        return Ok(HashMap::new());
    }
    let block_timestamps = raw_logs
        .iter()
        .map(|raw_log| (raw_log.block_hash.clone(), raw_log.block_timestamp))
        .collect::<HashMap<_, _>>();
    let rows = sqlx::query(&format!(
        r#"
        WITH candidates AS (
            SELECT
                event.logical_name_id,
                CASE
                    WHEN event.event_kind IN ($2, $3)
                    THEN (event.before_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                CASE
                    WHEN event.event_kind = $4 THEN event.before_state->>'from'
                    ELSE NULL
                END AS registrant,
                CASE
                    WHEN resource.provenance->>'authority_kind' = 'registrar'
                    THEN resource.provenance->>'authority_key'
                    ELSE NULL
                END AS authority_key,
                CASE
                    WHEN resource.provenance->>'authority_kind' = 'registrar'
                    THEN COALESCE(resource.provenance->>'labelhash', event.after_state->>'labelhash')
                    ELSE NULL
                END AS labelhash,
                event.chain_id,
                event.block_hash,
                event.block_number,
                event.transaction_hash,
                event.log_index,
                event.namespace,
                event.source_manifest_id,
                event.source_family,
                event.manifest_version,
                event.canonicality_state::TEXT AS canonicality_state,
                event.normalized_event_id
            FROM normalized_events event
            LEFT JOIN resources resource
              ON resource.resource_id = event.resource_id
            WHERE event.event_identity = ANY($1::TEXT[])
              AND event.event_kind IN ($2, $3, $4)
              AND event.canonicality_state {CANONICALITY_STATE_FILTER}
        )
        SELECT
            logical_name_id,
            (
                ARRAY_AGG(
                    expiry
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE expiry IS NOT NULL)
            )[1] AS expiry,
            (
                ARRAY_AGG(
                    registrant
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE registrant IS NOT NULL)
            )[1] AS registrant
            ,
            (
                ARRAY_AGG(
                    authority_key
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS authority_key,
            (
                ARRAY_AGG(
                    labelhash
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS labelhash,
            (
                ARRAY_AGG(
                    jsonb_build_object(
                        'chain_id', chain_id,
                        'block_hash', block_hash,
                        'block_number', block_number,
                        'transaction_hash', transaction_hash,
                        'log_index', log_index,
                        'canonicality_state', canonicality_state,
                        'namespace', namespace,
                        'source_manifest_id', source_manifest_id,
                        'source_family', source_family,
                        'manifest_version', manifest_version
                    )
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference
        FROM candidates
        GROUP BY logical_name_id
        "#
    ))
    .bind(&event_identities)
    .bind(EVENT_KIND_REGISTRATION_RENEWED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .fetch_all(pool)
    .await
    .context("failed to preload selected registrar state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let expiry = row
            .try_get::<Option<i64>, _>("expiry")?
            .map(|value| {
                OffsetDateTime::from_unix_timestamp(value)
                    .context("preloaded selected registrar expiry is not a valid unix timestamp")
            })
            .transpose()?;
        let registrant = row.try_get("registrant")?;
        let authority_key: Option<String> = row.try_get("authority_key")?;
        let labelhash: Option<String> = row.try_get("labelhash")?;
        let reference: Option<Value> = row.try_get("reference")?;
        let start_ref = match (authority_key.as_ref(), reference) {
            (Some(_), Some(reference)) => {
                Some(selected_registrar_start_ref(&reference, &block_timestamps)?)
            }
            (Some(_), None) => {
                bail!("selected registrar replay state is missing reference")
            }
            _ => None,
        };
        state.insert(
            row.try_get("logical_name_id")?,
            PreloadedRegistrarState {
                expiry,
                registrant,
                authority_key,
                labelhash,
                start_ref,
            },
        );
    }
    Ok(state)
}

fn selected_registrar_event_identities(
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<String>> {
    let mut identities = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        let Some(observation) = build_authority_observation(raw_log, event_topics)? else {
            continue;
        };
        match observation {
            AuthorityObservation::RegistrationRenewed(_) => {
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_REGISTRATION_RENEWED,
                    "renewal",
                ));
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_EXPIRY_CHANGED,
                    "expiry",
                ));
            }
            AuthorityObservation::TokenTransferred(_) => {
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
                    "token-transfer",
                ));
            }
            _ => {}
        }
    }
    Ok(identities.into_iter().collect())
}
