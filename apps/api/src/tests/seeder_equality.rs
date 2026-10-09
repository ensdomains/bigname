//! The chunked identity seeders in `support.rs` write one statement per chunk of rows. These
//! tests seed one fixture twice, once through the chunked seeders and once through the
//! one-statement-per-row seeders they replaced, kept below as the reference, and compare every
//! table of the two databases.
use std::collections::BTreeMap;

use super::*;

/// Every row of every phase table, as JSON text sorted per table. Columns filled from the clock
/// and generated keys differ between two seedings and are left out. A search posting names its
/// document by the document's logical name id instead of its generated key.
async fn phase_rows(pool: &PgPool) -> Result<BTreeMap<String, Vec<String>>> {
    let tables: Vec<(String, Vec<String>)> = sqlx::query_as(
        "SELECT table_name::text,
                coalesce(array_agg(column_name::text) FILTER (
                    WHERE column_default ILIKE '%now()%' OR is_identity = 'YES'
                       OR column_name IN ('updated_at', 'started_at', 'finished_at')
                ), '{}')
         FROM information_schema.columns
         WHERE table_schema = 'bigname_phase'
           AND table_name IN (SELECT table_name FROM information_schema.tables
                              WHERE table_schema = 'bigname_phase' AND table_type = 'BASE TABLE')
         GROUP BY table_name ORDER BY table_name",
    )
    .fetch_all(pool)
    .await?;
    let mut rows = BTreeMap::new();
    for (table, excluded) in tables {
        let query = if table == "name_search_postings" {
            "SELECT ((to_jsonb(posting) - $1::text[])
                     || jsonb_build_object('logical_name_id', document.logical_name_id))::text
             FROM bigname_phase.name_search_postings posting
             JOIN bigname_phase.name_search_documents document USING (search_id)
             ORDER BY 1"
                .to_owned()
        } else {
            format!(
                "SELECT (to_jsonb(stored) - $1::text[])::text
                 FROM bigname_phase.{table} stored ORDER BY 1"
            )
        };
        let mut excluded = excluded;
        if table == "name_search_postings" {
            excluded.push("search_id".to_owned());
        }
        let table_rows: Vec<String> = sqlx::query_scalar(&query)
            .bind(excluded)
            .fetch_all(pool)
            .await?;
        if !table_rows.is_empty() {
            rows.insert(table, table_rows);
        }
    }
    Ok(rows)
}

async fn assert_same_phase_rows(
    per_row: &TestDatabase,
    chunked: &TestDatabase,
    seeded: &[&str],
) -> Result<()> {
    let (per_row, chunked) = (
        phase_rows(&per_row.pool).await?,
        phase_rows(&chunked.pool).await?,
    );
    for table in seeded {
        assert!(
            per_row.contains_key(*table),
            "the fixture seeds no {table} row"
        );
    }
    let tables = per_row
        .keys()
        .chain(chunked.keys())
        .collect::<std::collections::BTreeSet<_>>();
    let mut differences = Vec::new();
    for table in tables {
        let (expected, actual) = (
            per_row.get(table).cloned().unwrap_or_default(),
            chunked.get(table).cloned().unwrap_or_default(),
        );
        if expected != actual {
            let missing = expected
                .iter()
                .filter(|row| !actual.contains(row))
                .take(3)
                .collect::<Vec<_>>();
            let extra = actual
                .iter()
                .filter(|row| !expected.contains(row))
                .take(3)
                .collect::<Vec<_>>();
            differences.push(format!(
                "{table}: {} rows per row, {} chunked\n  only per row: {missing:#?}\n  only chunked: {extra:#?}",
                expected.len(),
                actual.len()
            ));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

/// Seed one fixture into two fresh databases, through each path, and compare their rows.
/// `seeded` names tables the fixture must write, so the comparison cannot pass empty.
async fn assert_paths_agree<P, C>(seeded: &[&str], seed_per_row: P, seed_chunked: C) -> Result<()>
where
    P: AsyncFnOnce(&TestDatabase) -> Result<()>,
    C: AsyncFnOnce(&TestDatabase) -> Result<()>,
{
    let per_row = TestDatabase::new_migrated().await?;
    let chunked = TestDatabase::new_migrated().await?;
    seed_per_row(&per_row).await?;
    seed_chunked(&chunked).await?;
    let compared = assert_same_phase_rows(&per_row, &chunked, seeded).await;
    per_row.cleanup().await?;
    chunked.cleanup().await?;
    compared
}

const EQUALITY_CHAIN: &str = "ethereum-mainnet";

const IDENTITY_TABLES: [&str; 7] = [
    "chain_lineage",
    "token_lineages",
    "resources",
    "name_surfaces",
    "surface_bindings",
    "name_search_documents",
    "name_search_postings",
];

fn lineage_row(
    id: u128,
    hash: &str,
    block: i64,
    state: CanonicalityState,
    seed: &str,
) -> TokenLineage {
    TokenLineage {
        token_lineage_id: Uuid::from_u128(id),
        chain_id: EQUALITY_CHAIN.into(),
        block_hash: hash.into(),
        block_number: block,
        provenance: json!({ "seed": seed }),
        canonicality_state: state,
    }
}

fn resource_row(id: u128, token: Option<u128>, hash: &str, block: i64, seed: &str) -> Resource {
    Resource {
        resource_id: Uuid::from_u128(id),
        token_lineage_id: token.map(Uuid::from_u128),
        chain_id: EQUALITY_CHAIN.into(),
        block_hash: hash.into(),
        block_number: block,
        provenance: json!({ "seed": seed }),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn surface_row(
    name: &str,
    hash: &str,
    block: i64,
    state: CanonicalityState,
    seed: &str,
) -> Result<NameSurface> {
    let (logical_name_id, namehash) = phase_logical_identity("ens", name)?;
    Ok(NameSurface {
        logical_name_id,
        namespace: "ens".into(),
        input_name: name.into(),
        canonical_display_name: name.into(),
        normalized_name: name.into(),
        dns_encoded_name: Some(name.as_bytes().to_vec()),
        namehash,
        labelhashes: Vec::new(),
        normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.into(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: EQUALITY_CHAIN.into(),
        block_hash: hash.into(),
        block_number: block,
        provenance: json!({ "seed": seed }),
        canonicality_state: state,
    })
}

fn binding_row(
    id: u128,
    name: &str,
    resource: u128,
    active_to: Option<i64>,
    seed: &str,
) -> Result<SurfaceBinding> {
    Ok(SurfaceBinding {
        surface_binding_id: Uuid::from_u128(id),
        logical_name_id: format!("ens:{name}"),
        resource_id: Uuid::from_u128(resource),
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        authority_arm: "ens_v1".into(),
        active_from: OffsetDateTime::from_unix_timestamp(1_700_000_000)?,
        active_to: active_to
            .map(OffsetDateTime::from_unix_timestamp)
            .transpose()?,
        chain_id: EQUALITY_CHAIN.into(),
        block_hash: "0xeq-a".into(),
        block_number: 5,
        provenance: json!({ "seed": seed }),
        canonicality_state: CanonicalityState::Canonical,
    })
}

/// One call of each identity upsert helper.
type UpsertCall = (
    Vec<TokenLineage>,
    Vec<Resource>,
    Vec<NameSurface>,
    Vec<SurfaceBinding>,
);

/// Two calls of each helper: readable and observed rows, two readable rows at one height (the
/// second anchors to the first's block), keys repeated inside a call and across calls (the
/// conflict-updated columns come from the last).
fn upsert_fixture() -> Result<[UpsertCall; 2]> {
    use CanonicalityState::{Canonical, Observed, Safe};
    Ok([
        (
            vec![
                lineage_row(0x10, "0xeq-a", 5, Canonical, "first"),
                lineage_row(0x11, "0xeq-b", 6, Observed, "first"),
                lineage_row(0x10, "0xeq-a", 5, Safe, "repeated"),
                lineage_row(0x12, "0xeq-c", 5, Canonical, "same height"),
            ],
            vec![
                resource_row(0x20, Some(0x10), "0xeq-a", 5, "first"),
                resource_row(0x21, Some(0x11), "0xeq-d", 7, "first"),
                resource_row(0x20, None, "0xeq-c", 5, "repeated"),
            ],
            vec![
                surface_row("alpha.eth", "0xeq-a", 5, Canonical, "first")?,
                surface_row("beta.eth", "0xeq-e", 8, Observed, "first")?,
                surface_row("alpha.eth", "0xeq-c", 5, Safe, "repeated")?,
            ],
            vec![
                binding_row(0x30, "alpha.eth", 0x20, None, "first")?,
                binding_row(0x31, "beta.eth", 0x21, None, "first")?,
                binding_row(0x30, "alpha.eth", 0x20, Some(1_800_000_000), "repeated")?,
            ],
        ),
        (
            vec![lineage_row(0x11, "0xeq-f", 9, Canonical, "second call")],
            vec![resource_row(0x21, Some(0x10), "0xeq-a", 5, "second call")],
            vec![surface_row(
                "beta.eth",
                "0xeq-a",
                5,
                Canonical,
                "second call",
            )?],
            vec![binding_row(
                0x31,
                "beta.eth",
                0x21,
                Some(1_900_000_000),
                "second call",
            )?],
        ),
    ])
}

#[tokio::test]
async fn chunked_identity_upserts_leave_the_rows_of_one_statement_per_row() -> Result<()> {
    assert_paths_agree(
        &IDENTITY_TABLES,
        async |database: &TestDatabase| {
            for (tokens, resources, surfaces, bindings) in upsert_fixture()? {
                for row in &tokens {
                    per_row_token_lineages(&database.pool, std::slice::from_ref(row)).await?;
                }
                for row in &resources {
                    per_row_resources(&database.pool, std::slice::from_ref(row)).await?;
                }
                for row in &surfaces {
                    per_row_name_surfaces(&database.pool, std::slice::from_ref(row)).await?;
                }
                for row in &bindings {
                    per_row_surface_bindings(&database.pool, std::slice::from_ref(row)).await?;
                }
            }
            Ok(())
        },
        async |database: &TestDatabase| {
            for (tokens, resources, surfaces, bindings) in upsert_fixture()? {
                upsert_test_token_lineages(&database.pool, &tokens).await?;
                upsert_test_resources(&database.pool, &resources).await?;
                upsert_test_name_surfaces(&database.pool, &surfaces).await?;
                upsert_test_surface_bindings(&database.pool, &bindings).await?;
            }
            Ok(())
        },
    )
    .await
}

const FAMILY_EQUALITY_NAMES: [(&str, u128, &str); 4] = [
    ("alpha.eth", 0x9100, "ens_v1"),
    ("beta.alpha.eth", 0x9110, "ens_v2"),
    ("gamma.alpha.eth", 0x9120, "ens_v2"),
    ("beta.alpha.eth", 0x9110, "ens_v1"),
];

#[tokio::test]
async fn seed_family_names_leaves_the_rows_of_seed_family_name_per_name() -> Result<()> {
    assert_paths_agree(
        &IDENTITY_TABLES,
        async |database: &TestDatabase| {
            for (name, seed, arm) in FAMILY_EQUALITY_NAMES {
                let rows =
                    family_name_rows(name, seed, arm, "ens", FAMILY_CHAIN, FAMILY_FIRST_BLOCK)?;
                per_row_name_surfaces(&database.pool, &[rows.surface]).await?;
                per_row_token_lineages(&database.pool, &[rows.token]).await?;
                per_row_resources(&database.pool, &[rows.resource]).await?;
                per_row_surface_bindings(&database.pool, &[rows.binding]).await?;
            }
            Ok(())
        },
        async |database: &TestDatabase| {
            let seeded = seed_family_names(database, &FAMILY_EQUALITY_NAMES).await?;
            assert_eq!(seeded.len(), FAMILY_EQUALITY_NAMES.len());
            Ok(())
        },
    )
    .await
}

/// A surface stored by its label hashes alone, which a preimage of one of its labels re-spells.
async fn seed_structural_surface(database: &TestDatabase) -> Result<()> {
    let (logical_name_id, namehash) = phase_logical_identity("ens", "gamma.eth")?;
    let labelhashes = ["gamma", "eth"]
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())));
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         VALUES ($1, '0xeq-structural', 4, '2024-01-01T00:00:00Z', 'canonical')",
    )
    .bind(EQUALITY_CHAIN)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, 'ens', $2, $3, $4, 'active', $5, '0xeq-structural', 4, 'canonical')",
    )
    .bind(logical_name_id)
    .bind(namehash)
    .bind(labelhashes.to_vec())
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(EQUALITY_CHAIN)
    .execute(&database.pool)
    .await?;
    Ok(())
}

const PREIMAGE_EQUALITY_LABELS: [&[u8]; 6] = [
    b"gamma",
    b"Beta",
    b"\xff\xfe",
    b"nul\0label",
    b"eth",
    b"gamma",
];

#[tokio::test]
async fn label_preimages_in_one_chunk_leave_the_rows_of_one_per_label() -> Result<()> {
    assert_paths_agree(
        &[
            "label_preimages",
            "name_search_documents",
            "name_search_postings",
        ],
        async |database: &TestDatabase| {
            seed_structural_surface(database).await?;
            for label in PREIMAGE_EQUALITY_LABELS {
                per_row_label_preimage(&database.pool, label).await?;
            }
            Ok(())
        },
        async |database: &TestDatabase| {
            seed_structural_surface(database).await?;
            let hashes =
                insert_family_label_preimages(&database.pool, &PREIMAGE_EQUALITY_LABELS).await?;
            assert_eq!(hashes.len(), PREIMAGE_EQUALITY_LABELS.len());
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn address_name_identities_in_chunks_leave_the_rows_of_one_per_spec() -> Result<()> {
    let specs = v2_address_name_specs();
    let mut seeded = IDENTITY_TABLES.to_vec();
    seeded.extend(["chain_heads", "chain_phase_state"]);
    assert_paths_agree(
        &seeded,
        async |database: &TestDatabase| per_spec_address_name_identities(database, &specs).await,
        async |database: &TestDatabase| seed_v2_address_name_identities(database, &specs).await,
    )
    .await
}

// The one-statement-per-row seeders the chunked ones replaced, unchanged but for their names.

async fn per_row_readable_lineage_anchors<'a>(
    pool: &PgPool,
    anchors: impl IntoIterator<Item = (&'a str, &'a str, i64, CanonicalityState)>,
) -> Result<()> {
    for (chain_id, block_hash, block_number, canonicality_state) in anchors {
        if !matches!(
            canonicality_state,
            CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
        ) {
            continue;
        }

        let block_timestamp = parse_rfc3339_utc_timestamp(&format!(
            "2026-04-17T00:00:{:02}Z",
            block_number.rem_euclid(60)
        ))
        .map_err(|error| anyhow::anyhow!(error))?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.chain_lineage (
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5::bigname_phase.canonicality_state)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(block_timestamp)
        .bind(canonicality_state.as_str())
        .execute(pool)
        .await
        .with_context(|| {
            format!("failed to seed readable lineage for {chain_id} block {block_hash}")
        })?;
    }

    Ok(())
}

async fn per_row_readable_lineage_anchor(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> Result<(String, i64)> {
    per_row_readable_lineage_anchors(
        pool,
        [(chain_id, block_hash, block_number, canonicality_state)],
    )
    .await?;
    sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT block_hash, block_number
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_one(pool)
    .await
    .context("readable test lineage anchor must exist")
}

async fn per_row_identity_lineage_anchor(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<(String, i64)> {
    let block_timestamp = parse_rfc3339_utc_timestamp(&format!(
        "2026-04-17T00:00:{:02}Z",
        block_number.rem_euclid(60)
    ))
    .map_err(|error| anyhow::anyhow!(error))?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, $4, 'observed'::bigname_phase.canonicality_state)
        ON CONFLICT (chain_id, block_hash) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .bind(block_timestamp)
    .execute(pool)
    .await?;
    Ok((block_hash.to_owned(), block_number))
}

async fn per_row_lineage_anchor_for_state(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> Result<(String, i64)> {
    if matches!(
        canonicality_state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    ) {
        per_row_readable_lineage_anchor(
            pool,
            chain_id,
            block_hash,
            block_number,
            canonicality_state,
        )
        .await
    } else {
        per_row_identity_lineage_anchor(pool, chain_id, block_hash, block_number).await
    }
}

async fn per_row_token_lineages(
    pool: &PgPool,
    token_lineages: &[TokenLineage],
) -> Result<Vec<TokenLineage>> {
    per_row_readable_lineage_anchors(
        pool,
        token_lineages.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in token_lineages {
        let (block_hash, block_number) = per_row_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.token_lineages (
                token_lineage_id, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6::bigname_phase.canonicality_state)
            ON CONFLICT (token_lineage_id) DO UPDATE SET
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(row.token_lineage_id)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(token_lineages.to_vec())
}

async fn per_row_resources(pool: &PgPool, resources: &[Resource]) -> Result<Vec<Resource>> {
    per_row_readable_lineage_anchors(
        pool,
        resources.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in resources {
        let (block_hash, block_number) = per_row_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.resources (
                resource_id, token_lineage_id, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::bigname_phase.canonicality_state)
            ON CONFLICT (resource_id) DO UPDATE SET
                token_lineage_id = EXCLUDED.token_lineage_id,
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(row.resource_id)
        .bind(row.token_lineage_id)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(resources.to_vec())
}

async fn per_row_name_surfaces(
    pool: &PgPool,
    name_surfaces: &[NameSurface],
) -> Result<Vec<NameSurface>> {
    per_row_readable_lineage_anchors(
        pool,
        name_surfaces.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in name_surfaces {
        let (block_hash, block_number) = per_row_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        let (logical_name_id, namehash) =
            phase_logical_identity(&row.namespace, &row.normalized_name)?;
        let raw_labels = row
            .normalized_name
            .split('.')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let labelhashes = raw_labels
            .iter()
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
            .collect::<Vec<_>>();
        let mut transaction = pool.begin().await?;
        bigname_storage::identity_search::prepare(
            &mut transaction,
            &[],
            std::slice::from_ref(&labelhashes),
            std::slice::from_ref(&logical_name_id),
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.name_surfaces (
                logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                namehash, labelhashes, normalizer_version, visibility_state,
                normalization_errors, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9, $10, $11, $12, $13,
                    $14::bigname_phase.canonicality_state)
            ON CONFLICT (logical_name_id) DO UPDATE SET
                raw_name = EXCLUDED.raw_name,
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(&logical_name_id)
        .bind(&row.namespace)
        .bind(&row.normalized_name)
        .bind(raw_labels)
        .bind(&row.dns_encoded_name)
        .bind(namehash)
        .bind(labelhashes)
        .bind(&row.normalizer_version)
        .bind(&row.normalization_errors)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(&mut *transaction)
        .await?;
        bigname_storage::identity_search::refresh(
            &mut transaction,
            std::slice::from_ref(&logical_name_id),
            &[],
        )
        .await?;
        transaction.commit().await?;
    }
    Ok(name_surfaces.to_vec())
}

async fn per_row_surface_bindings(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<Vec<SurfaceBinding>> {
    per_row_readable_lineage_anchors(
        pool,
        bindings.iter().map(|row| {
            (
                row.chain_id.as_str(),
                row.block_hash.as_str(),
                row.block_number,
                row.canonicality_state,
            )
        }),
    )
    .await?;
    for row in bindings {
        let (block_hash, block_number) = per_row_lineage_anchor_for_state(
            pool,
            &row.chain_id,
            &row.block_hash,
            row.block_number,
            row.canonicality_state,
        )
        .await?;
        let (namespace, name) = row
            .logical_name_id
            .split_once(':')
            .context("test surface binding logical_name_id must include namespace")?;
        let (logical_name_id, _) = phase_logical_identity(namespace, name)?;
        sqlx::query(
            r#"
            INSERT INTO bigname_phase.surface_bindings (
                surface_binding_id, logical_name_id, resource_id, binding_kind,
                authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::bigname_phase.canonicality_state)
            ON CONFLICT (surface_binding_id) DO UPDATE SET
                active_to = EXCLUDED.active_to,
                provenance = EXCLUDED.provenance,
                canonicality_state = EXCLUDED.canonicality_state
            "#,
        )
        .bind(row.surface_binding_id)
        .bind(logical_name_id)
        .bind(row.resource_id)
        .bind(row.binding_kind.as_str())
        .bind(&row.authority_arm)
        .bind(row.active_from)
        .bind(row.active_to)
        .bind(&row.chain_id)
        .bind(block_hash)
        .bind(block_number)
        .bind(&row.provenance)
        .bind(row.canonicality_state.as_str())
        .execute(pool)
        .await?;
    }
    Ok(bindings.to_vec())
}

#[allow(clippy::too_many_arguments)]
async fn per_row_family_identity_inputs(
    pool: &PgPool,
    namespace: &str,
    name: &str,
    chain: &str,
    block: i64,
    hash: &str,
    resource: Uuid,
    token: Uuid,
    binding: Uuid,
    arm: &str,
) -> Result<String> {
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let (logical, namehash) = phase_logical_identity(namespace, &normalized.normalized_name)?;
    let at: OffsetDateTime = sqlx::query_scalar(
        "SELECT block_timestamp FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2 AND block_number = $3"
    ).bind(chain).bind(hash).bind(block).fetch_one(pool).await?;
    per_row_token_lineages(
        pool,
        &[TokenLineage {
            token_lineage_id: token,
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    per_row_resources(
        pool,
        &[Resource {
            resource_id: resource,
            token_lineage_id: Some(token),
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    per_row_name_surfaces(
        pool,
        &[NameSurface {
            logical_name_id: logical.clone(),
            namespace: namespace.into(),
            input_name: name.into(),
            canonical_display_name: normalized.canonical_display_name,
            normalized_name: normalized.normalized_name,
            dns_encoded_name: Some(normalized.dns_encoded_name),
            namehash,
            labelhashes: vec![],
            normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.into(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    per_row_surface_bindings(
        pool,
        &[SurfaceBinding {
            surface_binding_id: binding,
            logical_name_id: format!("{namespace}:{name}"),
            resource_id: resource,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: arm.into(),
            active_from: at,
            active_to: None,
            chain_id: chain.into(),
            block_number: block,
            block_hash: hash.into(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    Ok(logical)
}

async fn per_row_label_preimage(pool: &PgPool, raw_label: &[u8]) -> Result<String> {
    let hash = format!("{:#x}", alloy_primitives::keccak256(raw_label));
    let decoded = std::str::from_utf8(raw_label)
        .ok()
        .filter(|label| !label.contains('\0'));
    let normalization_error = match decoded {
        Some(label) => {
            match bigname_domain::normalization::normalize_label_under_suffix(label, &[]) {
                Ok(name) if name.normalized_name == label => None,
                Ok(_) => Some("raw label is not byte-identical to its normalized form".to_owned()),
                Err(error) => Some(error.to_string()),
            }
        }
        None => Some("raw label has no PostgreSQL-safe UTF-8 decoding".to_owned()),
    };
    let mut transaction = pool.begin().await?;
    bigname_storage::identity_search::prepare(
        &mut transaction,
        std::slice::from_ref(&hash),
        &[],
        &[],
    )
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
        normalized_under_version, normalization_error, source_kind, source_priority)
        VALUES ($1, $2, $3, $4, $5, $6, 'fixture', 0) ON CONFLICT DO NOTHING",
    )
    .bind(&hash)
    .bind(raw_label)
    .bind(decoded)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(normalization_error.is_none())
    .bind(normalization_error)
    .execute(&mut *transaction)
    .await?;
    bigname_storage::identity_search::refresh(&mut transaction, &[], std::slice::from_ref(&hash))
        .await?;
    transaction.commit().await?;
    Ok(hash)
}

async fn per_spec_address_name_identities(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"base":{
            "chain_id":"base-mainnet", "block_number":1, "block_hash":"0xcount-base-empty",
            "timestamp":"2024-01-01T00:00:00Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 1, "0xcount-base-empty").await?;
    for spec in specs {
        for at in [spec.created_at, spec.registered_at] {
            let (block, hash) = address_fixture_time_block(at)?;
            sqlx::query(
                "INSERT INTO chain_lineage
                    (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
                 VALUES ('ethereum-mainnet', $1, $2, $3::timestamptz, 'canonical')
                 ON CONFLICT (chain_id, block_hash) DO NOTHING",
            )
            .bind(&hash)
            .bind(block)
            .bind(at)
            .execute(&database.pool)
            .await?;
        }
    }
    for spec in specs {
        let (block, hash) = address_fixture_time_block(spec.created_at)?;
        per_row_family_identity_inputs(
            &database.pool,
            "ens",
            spec.name,
            "ethereum-mainnet",
            block,
            &hash,
            spec.resource_id,
            spec.token_lineage_id,
            spec.surface_binding_id,
            "ens_v1",
        )
        .await?;
        database
            .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
                "chain_id":"ethereum-mainnet", "block_number":spec.block_number,
                "block_hash":spec.block_hash, "timestamp":"2024-05-31T18:26:47Z"
            }}))
            .await?;
    }
    if specs.is_empty() {
        rebuild_address_fixture(database).await?;
    }
    Ok(())
}
