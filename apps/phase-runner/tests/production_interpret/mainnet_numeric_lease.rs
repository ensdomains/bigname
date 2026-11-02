//! TYR-449: a 2020 migration mint on the checked-in mainnet profile, through Interpret and
//! Project. The numeric BaseRegistrar `NameRegistered` is the lease, so a migrated name whose
//! label bigname never learned serves its lease like a name a controller registered.
use super::*;

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const LEGACY_CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";
const MIGRATOR: &str = "0x00000000000000000000000000000000000000c1";
const HOLDER: &str = "0x00000000000000000000000000000000000000c2";
const MANAGER: &str = "0x00000000000000000000000000000000000000c3";
const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000c4";
const SUBNAME_OWNER: &str = "0x00000000000000000000000000000000000000c5";
const DAY: i64 = 86_400;
const FIRST: i64 = 9_400_000;
/// The mints, one transaction per name.
const MINT: i64 = 9_412_290;
/// The registry writes after the mint: the ownerless clear and the registry-only handoff.
const HANDOFF: i64 = 9_412_300;
const LIVE: i64 = 9_412_310;
/// From this block the lease is past its expiry and in grace.
const GRACE: i64 = 9_412_320;
/// From this block the lease is past its grace period.
const RELEASED: i64 = 9_412_330;
/// A controller registers the released name again.
const AGAIN: i64 = 9_412_331;
const HEAD: i64 = 9_412_340;
/// Every lease's expiry: live below `GRACE`.
const EXPIRY: i64 = 9_500_000;

mod events {
    alloy_sol_types::sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }
}

mod registry_events {
    alloy_sol_types::sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event Transfer(bytes32 indexed node, address owner);
    }
}

fn timestamp_sql() -> String {
    format!(
        "CASE WHEN height >= {RELEASED} THEN {EXPIRY} + 90 * {DAY} + 1000 + height - {RELEASED}
              WHEN height >= {GRACE} THEN {EXPIRY} + 30 * {DAY} + height - {GRACE}
              ELSE height END"
    )
}

/// The canonical chain from `FIRST` to `HEAD`, with an intake cursor at the head.
async fn seed(pool: &PgPool) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         )
         SELECT $1, $1 || '-block-' || height::text,
                CASE WHEN height > $2 THEN $1 || '-block-' || (height - 1)::text END,
                height, to_timestamp({}), 'canonical'::canonicality_state
         FROM generate_series($2::bigint, $3::bigint) AS height",
        timestamp_sql()
    ))
    .bind(CHAIN)
    .bind(FIRST)
    .bind(HEAD)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, HEAD))
    .bind(HEAD)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'intake', 'drpc', 'ethereum_head', 0, $2 + 1, $2, $2, $3)",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(CHAIN, HEAD))
    .execute(pool)
    .await?;
    PhaseStore::new(pool.clone())
        .initialize_chain(CHAIN)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', started_at = now(), finished_at = now(),
             current_block_number = $2,
             current_block_hash = $3,
             input_content_hash = CASE phase_name WHEN 'project' THEN $5
                 WHEN 'interpret' THEN $4 END
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(block_hash(CHAIN, HEAD))
    .bind(INTERPRETER_CONTENT_HASH)
    .bind(PROJECTION_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

/// Logs by block and transaction, log indexes block-wide.
#[derive(Default)]
struct Facts(Vec<(i64, i64, &'static str, alloy_primitives::LogData)>);

impl Facts {
    fn push(
        &mut self,
        block: i64,
        transaction: i64,
        emitter: &'static str,
        log: alloy_primitives::LogData,
    ) {
        self.0.push((block, transaction, emitter, log));
    }

    async fn insert(&self, pool: &PgPool) -> Result<()> {
        let mut facts = self.0.iter().collect::<Vec<_>>();
        facts.sort_by_key(|(block, transaction, _, _)| (*block, *transaction));
        let mut transactions = std::collections::BTreeSet::new();
        let mut log_index = std::collections::BTreeMap::<i64, i64>::new();
        for (block, transaction, emitter, log) in facts {
            let hash = format!("{CHAIN}-transaction-{block}-{transaction}");
            if transactions.insert(hash.clone()) {
                sqlx::query(
                    "INSERT INTO raw_transactions (
                         chain_id, block_hash, block_number, transaction_hash, transaction_index,
                         from_address, to_address
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(CHAIN)
                .bind(block_hash(CHAIN, *block))
                .bind(block)
                .bind(&hash)
                .bind(transaction)
                .bind(MIGRATOR)
                .bind(*emitter)
                .execute(pool)
                .await?;
            }
            let index = log_index.entry(*block).or_default();
            sqlx::query(
                "INSERT INTO raw_logs (
                     chain_id, block_hash, block_number, transaction_hash, transaction_index,
                     log_index, emitting_address, topics, data
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            )
            .bind(CHAIN)
            .bind(block_hash(CHAIN, *block))
            .bind(block)
            .bind(&hash)
            .bind(transaction)
            .bind(*index)
            .bind(*emitter)
            .bind(
                log.topics()
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect::<Vec<_>>(),
            )
            .bind(log.data.to_vec())
            .execute(pool)
            .await?;
            *index += 1;
        }
        Ok(())
    }
}

fn labelhash(label: &str) -> B256 {
    keccak256(label.as_bytes())
}

fn token(label: &str) -> U256 {
    U256::from_be_bytes(labelhash(label).0)
}

fn node(label: &str) -> B256 {
    raw_namehash(&[label.as_bytes(), b"eth"])
}

fn name(label: &str) -> String {
    format!("ens:{:#x}", node(label))
}

fn eth() -> B256 {
    raw_namehash(&[b"eth"])
}

fn at(address: &str) -> Address {
    address.parse().expect("an address")
}

fn new_owner(parent: B256, label: &str, owner: &str) -> alloy_primitives::LogData {
    registry_events::NewOwner {
        node: parent,
        label: labelhash(label),
        owner: at(owner),
    }
    .encode_log_data()
}

fn record_owner(label: &str, owner: &str) -> alloy_primitives::LogData {
    registry_events::Transfer {
        node: node(label),
        owner: at(owner),
    }
    .encode_log_data()
}

fn numeric_registration(label: &str, owner: &str) -> alloy_primitives::LogData {
    events::NameRegistered {
        id: token(label),
        owner: at(owner),
        expires: U256::from(EXPIRY),
    }
    .encode_log_data()
}

fn token_transfer(label: &str, from: &str, to: &str) -> alloy_primitives::LogData {
    events::Transfer {
        from: at(from),
        to: at(to),
        tokenId: token(label),
    }
    .encode_log_data()
}

/// The 2020 migration mint of `label` for HOLDER: the migrator registers the token to itself,
/// writes the registry record to HOLDER and hands HOLDER the token
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f).
fn migration_mint(facts: &mut Facts, transaction: i64, label: &str) {
    facts.push(
        MINT,
        transaction,
        BASE_REGISTRAR,
        token_transfer(
            label,
            "0x0000000000000000000000000000000000000000",
            MIGRATOR,
        ),
    );
    facts.push(
        MINT,
        transaction,
        REGISTRY,
        new_owner(eth(), label, MIGRATOR),
    );
    facts.push(
        MINT,
        transaction,
        BASE_REGISTRAR,
        numeric_registration(label, MIGRATOR),
    );
    facts.push(MINT, transaction, REGISTRY, record_owner(label, HOLDER));
    facts.push(
        MINT,
        transaction,
        BASE_REGISTRAR,
        token_transfer(label, MIGRATOR, HOLDER),
    );
}

/// A legacy controller registration of `label` for `owner` at `block`, in the same shape: the
/// controller registers to itself, reclaims the record for the owner, hands over the token and
/// emits its label-bearing event
/// (upstream: .refs/ens_v1/deployments/archive/ETHRegistrarController_mainnet_9380471.sol/ETHRegistrarController_mainnet_9380471.json:L35 @ ens_v1@91c966f).
fn controller_mint(
    facts: &mut Facts,
    block: i64,
    transaction: i64,
    label: &str,
    owner: &'static str,
    expiry: i64,
) -> Result<()> {
    facts.push(
        block,
        transaction,
        BASE_REGISTRAR,
        token_transfer(
            label,
            "0x0000000000000000000000000000000000000000",
            LEGACY_CONTROLLER,
        ),
    );
    facts.push(
        block,
        transaction,
        REGISTRY,
        new_owner(eth(), label, LEGACY_CONTROLLER),
    );
    facts.push(
        block,
        transaction,
        BASE_REGISTRAR,
        events::NameRegistered {
            id: token(label),
            owner: at(LEGACY_CONTROLLER),
            expires: U256::from(u64::try_from(expiry)?),
        }
        .encode_log_data(),
    );
    facts.push(block, transaction, REGISTRY, new_owner(eth(), label, owner));
    facts.push(
        block,
        transaction,
        BASE_REGISTRAR,
        token_transfer(label, LEGACY_CONTROLLER, owner),
    );
    facts.push(
        block,
        transaction,
        LEGACY_CONTROLLER,
        legacy_registrar_controller_events::NameRegistered {
            name: label.to_owned(),
            label: labelhash(label),
            owner: at(owner),
            cost: U256::from(1_u64),
            expires: U256::from(u64::try_from(expiry)?),
        }
        .encode_log_data(),
    );
    Ok(())
}

/// The pairs of row R and row 1: each name's numeric mint and its controller twin.
const TWINS: [(&str, &str); 3] = [
    ("migrated-plain", "controlled-plain"),
    ("migrated-ownerless", "controlled-ownerless"),
    ("migrated-handoff", "controlled-handoff"),
];

fn facts() -> Result<Facts> {
    let mut facts = Facts::default();
    facts.push(
        FIRST,
        0,
        REGISTRY,
        new_owner(B256::ZERO, "eth", BASE_REGISTRAR),
    );
    let mut transaction = 0;
    for (migrated, controlled) in TWINS {
        migration_mint(&mut facts, transaction, migrated);
        controller_mint(
            &mut facts,
            MINT,
            transaction + 1,
            controlled,
            HOLDER,
            EXPIRY,
        )?;
        transaction += 2;
    }
    for label in ["migrated-parent", "migrated-again"] {
        migration_mint(&mut facts, transaction, label);
        transaction += 1;
    }
    // R-a: the holder clears the record. R-b: the holder moves only the record.
    for (label, owner) in [
        (
            "migrated-ownerless",
            "0x0000000000000000000000000000000000000000",
        ),
        (
            "controlled-ownerless",
            "0x0000000000000000000000000000000000000000",
        ),
        ("migrated-handoff", MANAGER),
        ("controlled-handoff", MANAGER),
    ] {
        facts.push(HANDOFF, transaction, REGISTRY, record_owner(label, owner));
        transaction += 1;
    }
    facts.push(
        HANDOFF,
        transaction,
        REGISTRY,
        new_owner(node("migrated-parent"), "sub", SUBNAME_OWNER),
    );
    controller_mint(
        &mut facts,
        AGAIN,
        0,
        "migrated-again",
        NEXT_HOLDER,
        EXPIRY + 365 * DAY * 3,
    )?;
    Ok(facts)
}

/// `value` with each `(from, to)` text replacement made and then every UUID and 32-byte hex
/// string replaced by its order of first appearance, so two names that differ only in identity
/// and label text compare equal.
fn canonical(value: &Value, replacements: &[(String, String)], ids: &mut Vec<String>) -> Value {
    match value {
        Value::String(text) => {
            let mut text = text.clone();
            for (from, to) in replacements {
                text = text.replace(from.as_str(), to);
            }
            let identity =
                Uuid::parse_str(&text).is_ok() || (text.len() == 66 && text.starts_with("0x"));
            if identity {
                let position = ids.iter().position(|id| *id == text).unwrap_or_else(|| {
                    ids.push(text.clone());
                    ids.len() - 1
                });
                Value::String(format!("<id {position}>"))
            } else {
                Value::String(text)
            }
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| canonical(item, replacements, ids))
                .collect(),
        ),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, item)| (key.clone(), canonical(item, replacements, ids)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `summary` without the fields that record where in its transaction a fact landed: the event
/// heads and latest event kinds, and the authority key, which embeds the granting log's position.
/// A controller emits its event after the registry and token writes the migration mint makes
/// after its numeric event, so these differ between twins that serve the same lease.
fn positionless(mut summary: Value) -> Value {
    fn strip(value: &mut Value) {
        match value {
            Value::Object(fields) => {
                fields.remove("history");
                fields.remove("latest_event_kind");
                if let Some(Value::String(key)) = fields.get_mut("authority_key") {
                    *key = key.split(':').next().unwrap_or_default().to_owned();
                }
                fields.values_mut().for_each(strip);
            }
            Value::Array(items) => items.iter_mut().for_each(strip),
            _ => {}
        }
    }
    strip(&mut summary);
    summary
}

/// What a client reads for `label`.eth: the composed row's summary, coverage and binding shape,
/// its `project_name_summary` row, and the relations of every party to it. Identities and the
/// label's text are canonicalized.
async fn served(pool: &PgPool, label: &str) -> Result<Value> {
    let logical_name_id = name(label);
    let row = project_name(pool, &logical_name_id).await?;
    let summary: Option<Value> = sqlx::query_scalar(
        // The surface's raw bytes and their sort key exist only for a learned label, so the
        // migrated twin serves none: they are label knowledge, not composition.
        "SELECT to_jsonb(summary) - 'logical_name_id' - 'raw_name' - 'order_name' - 'namehash'
         FROM project_name_summary summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_optional(pool)
    .await?;
    let mut relations = serde_json::Map::new();
    for address in [MIGRATOR, HOLDER, MANAGER, LEGACY_CONTROLLER] {
        let mut held = bigname_storage::load_address_names_current(pool, address, None, None)
            .await?
            .into_iter()
            .filter(|relation| relation.logical_name_id == logical_name_id)
            .map(|relation| relation.relation.as_str().to_owned())
            .collect::<Vec<_>>();
        held.sort();
        relations.insert(address.to_owned(), json!(held));
    }
    // A name whose labels bigname never learned renders each label as its hash, `eth` included.
    let placeholder =
        |label: &str| format!("[{}]", alloy_primitives::hex::encode(labelhash(label)));
    let replacements = [
        (placeholder(label), "<label>".to_owned()),
        (placeholder("eth"), "eth".to_owned()),
        (label.to_owned(), "<label>".to_owned()),
    ];
    let mut ids = Vec::new();
    Ok(canonical(
        &json!({
            "declared_summary": positionless(row.declared_summary),
            "coverage": row.coverage,
            "binding_kind": row.binding_kind.map(|kind| kind.as_str()),
            "has_binding": row.surface_binding_id.is_some(),
            "has_resource": row.resource_id.is_some(),
            "has_serving_resource": row.serving_resource_id.is_some(),
            "has_token_lineage": row.token_lineage_id.is_some(),
            "summary": summary,
            "relations": relations,
        }),
        &replacements,
        &mut ids,
    ))
}

fn field<'a>(served: &'a Value, pointer: &str) -> &'a Value {
    served.pointer(pointer).unwrap_or(&Value::Null)
}

async fn through(pool: &PgPool, from: i64, to: i64, mode: FamilyMode) -> Result<()> {
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(std::num::NonZeroU32::new(200_000).expect("non-zero"));
    let mut resume_current = None;
    loop {
        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: from,
                to_block: to,
                resume_current,
                mode: InterpretRunMode::Normal,
            })
            .await?;
        if outcome.complete {
            break;
        }
        resume_current = Some(outcome.current);
    }
    publish_families(pool, CHAIN, to, mode).await
}

async fn corpus(name: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(name).await?;
    let pool = scratch.pool();
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet");
    sync_schema_v2_repository(pool, &load_repository(profile)?).await?;
    seed(pool).await?;
    facts()?.insert(pool).await?;
    Ok(scratch)
}

/// Rows 1, 3, 4, R and 11. Each migrated name, the plain mint, the mint whose holder cleared the
/// registry record (the ownerless profile) and the mint whose holder moved only the record (a
/// registry-only handoff), serves exactly what its controller twin serves, live, in grace and
/// released. The plain mint serves its lease: active, owner and manager HOLDER, the lease's
/// expiry and the mint's block as its registration time. A subname under a migrated parent keeps
/// its owner when the parent's lease is released.
#[tokio::test]
async fn a_numeric_lease_composes_like_a_controller_lease() -> Result<()> {
    let scratch = corpus("production_interpret_mainnet_numeric_lease").await?;
    let pool = scratch.pool();
    let subname = format!(
        "ens:{:#x}",
        raw_namehash(&[b"sub", b"migrated-parent", b"eth"])
    );
    let mut subname_served = Vec::new();
    for (from, to, mode, phase) in [
        (FIRST, LIVE, FamilyMode::Rebuild, "live"),
        (LIVE + 1, GRACE + 1, FamilyMode::Normal, "grace"),
        (GRACE + 2, RELEASED + 1, FamilyMode::Normal, "released"),
    ] {
        through(pool, from, to, mode).await?;
        for (migrated, controlled) in TWINS {
            let numeric = served(pool, migrated).await?;
            let controller = served(pool, controlled).await?;
            anyhow::ensure!(
                numeric == controller,
                "{phase}: {migrated} serves {numeric:#}\nbut {controlled} serves {controller:#}"
            );
        }
        let plain = served(pool, "migrated-plain").await?;
        let status = field(&plain, "/summary/registration_status");
        anyhow::ensure!(
            *status
                == json!(match phase {
                    "live" => "active",
                    "grace" => "expired",
                    _ => "released",
                }),
            "{phase}: {plain:#}"
        );
        if phase == "live" {
            anyhow::ensure!(
                *field(&plain, "/declared_summary/control/owner") == json!(HOLDER)
                    && *field(
                        &plain,
                        "/relations/0x00000000000000000000000000000000000000c2"
                    ) != json!([]),
                "live: {plain:#}"
            );
        }
        if phase == "released" {
            anyhow::ensure!(
                *field(
                    &plain,
                    "/relations/0x00000000000000000000000000000000000000c2"
                ) == json!([]),
                "released: {plain:#}"
            );
        }
        let child = project_name(pool, &subname).await?;
        subname_served.push(json!({
            "control": child.declared_summary["control"],
            "status": child.declared_summary.pointer("/registration/status"),
        }));
    }
    anyhow::ensure!(
        subname_served
            .iter()
            .all(|served| *served == subname_served[0])
            && subname_served[0]["control"]
                .to_string()
                .contains(SUBNAME_OWNER),
        "the subname follows its parent's release: {subname_served:?}"
    );
    scratch.cleanup().await
}

/// Rows 5b and 9. A label preimage imported after the fact renames the migrated name on read and
/// changes no stored row. A controller registration after the release starts a new lease for its
/// registrant.
#[tokio::test]
async fn a_migrated_name_is_renamed_on_read_and_registered_again() -> Result<()> {
    let scratch = corpus("production_interpret_mainnet_numeric_lease_again").await?;
    let pool = scratch.pool();
    through(pool, FIRST, LIVE, FamilyMode::Rebuild).await?;
    let stored = |pool: PgPool| async move {
        let events: Value = sqlx::query_scalar(
            "SELECT coalesce(jsonb_agg(to_jsonb(event) - 'observed_at' ORDER BY event_identity), '[]')
             FROM normalized_events event WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(&pool)
        .await?;
        anyhow::Ok(json!({"events": events, "families": family_state(&pool, CHAIN).await?}))
    };
    let before = stored(pool.clone()).await?;
    let rendered = project_name(pool, &name("migrated-plain"))
        .await?
        .normalized_name;
    anyhow::ensure!(
        rendered.starts_with('['),
        "an unlearned label renders as its hash: {rendered}"
    );
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version, normalized_under_version,
             source_kind, source_priority
         ) VALUES ($1, convert_to($2, 'UTF8'), $2, $3, true, 'import', 0)",
    )
    .bind(format!("{:#x}", labelhash("migrated-plain")))
    .bind("migrated-plain")
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .execute(pool)
    .await?;
    let renamed = project_name(pool, &name("migrated-plain"))
        .await?
        .normalized_name;
    anyhow::ensure!(
        renamed.starts_with("migrated-plain."),
        "the import renames it: {renamed}"
    );
    anyhow::ensure!(
        stored(pool.clone()).await? == before,
        "the import changed a stored row"
    );

    through(pool, LIVE + 1, HEAD, FamilyMode::Normal).await?;
    let again = project_name(pool, &name("migrated-again")).await?;
    let (registered_at, at_again): (Option<i64>, i64) = sqlx::query_as(
        "SELECT extract(epoch FROM summary.registered_at)::bigint,
                extract(epoch FROM lineage.block_timestamp)::bigint
         FROM project_name_summary summary, chain_lineage lineage
         WHERE summary.chain_id = $1 AND summary.logical_name_id = $2
           AND lineage.chain_id = $1 AND lineage.block_number = $3",
    )
    .bind(CHAIN)
    .bind(name("migrated-again"))
    .bind(AGAIN)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        again.declared_summary.pointer("/registration/status") == Some(&json!("active"))
            && again.declared_summary.pointer("/control/owner") == Some(&json!(NEXT_HOLDER))
            && registered_at == Some(at_again),
        "registered again at {at_again}, {registered_at:?}: {:#}",
        again.declared_summary
    );
    scratch.cleanup().await
}
