//! ABI content types are read from the record writes Project already selects for a name
//! (`provenance.record_event_ids`), so these cases run the production Project engine and then the
//! shared storage read the records and lookup routes use. They pin that Project's resolver
//! selection, version resets, and ENSv1 mirror rules decide which ABI writes count.
use anyhow::{Context, Result};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_storage::{
    AbiContentTypes, AbiContentTypesInput, AbiContentTypesUnavailable,
    load_record_inventory_abi_content_types,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const V1_REGISTRY: &str = "0x4444444444444444444444444444444444444401";
const RESOLVER_A: &str = "0x1111111111111111111111111111111111111111";
const RESOLVER_B: &str = "0x3333333333333333333333333333333333333333";
const MIRROR: &str = "0x1010101010101010101010101010101010101010";
const V1_RESOURCE: &str = "69200000-0000-0000-0000-000000000001";
const V2_RESOURCE: &str = "69200000-0000-0000-0000-000000000002";
const ROOT_INSTANCE: &str = "69200000-0000-0000-0000-000000000201";
const MIRROR_INSTANCE: &str = "69200000-0000-0000-0000-000000000202";
const NAME: &str = "abi.fixture";
const FIRST_BLOCK: i64 = 10;
const LAST_BLOCK: i64 = 14;

fn observed(types: &[&str]) -> AbiContentTypes {
    AbiContentTypes::Observed(types.iter().map(|value| (*value).to_owned()).collect())
}

/// ABI records are not inventory entries, so an inventory whose only write is an ABI write has
/// empty selectors and entries and still lists the content type. The write precedes the pointer:
/// ENSv1 reads the selected resolver's node-keyed storage whenever it was written.
#[tokio::test]
async fn abi_only_inventory_lists_a_write_made_before_the_resolver_was_selected() -> Result<()> {
    let fixture = Fixture::new("abi_pre_pointer").await?;
    fixture.abi("abi-one", RESOLVER_A, 10, 1, "1").await?;
    fixture.v1_pointer("pointer-a", 11, 0, RESOLVER_A).await?;
    fixture.run(11, None, RunMode::Normal).await?;
    let row = fixture.inventory(V1_RESOURCE).await?;
    assert_eq!(row["support_status"], "supported", "{row}");
    assert_eq!(row["selectors"], json!([]), "{row}");
    assert_eq!(row["entries"], json!([]), "{row}");
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["1"])
    );
    fixture.cleanup().await
}

/// Switching away hides the earlier resolver's writes and switching back restores them.
#[tokio::test]
async fn switching_resolvers_away_and_back_follows_the_selected_storage() -> Result<()> {
    let fixture = Fixture::new("abi_switch").await?;
    fixture.v1_pointer("pointer-a", 10, 0, RESOLVER_A).await?;
    fixture.abi("abi-two", RESOLVER_A, 10, 1, "2").await?;
    fixture.text("text-b", RESOLVER_B, 10, 2).await?;
    fixture.v1_pointer("pointer-b", 11, 0, RESOLVER_B).await?;
    fixture
        .v1_pointer("pointer-a-again", 12, 0, RESOLVER_A)
        .await?;
    fixture.run(10, None, RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["2"])
    );
    fixture.run(11, Some(10), RunMode::Normal).await?;
    // Resolver B is an eligible observation path with no ABI write: an empty list.
    assert_eq!(fixture.abi_content_types(V1_RESOURCE).await?, observed(&[]));
    fixture.run(12, Some(11), RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["2"])
    );
    fixture.run(12, None, RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["2"])
    );
    fixture.cleanup().await
}

/// `VersionChanged` resets the record version, and only writes strictly after it count, ordered
/// by position within the same block.
#[tokio::test]
async fn a_version_reset_drops_earlier_writes_in_the_same_block() -> Result<()> {
    let fixture = Fixture::new("abi_version_reset").await?;
    fixture.v1_pointer("pointer-a", 10, 0, RESOLVER_A).await?;
    fixture.abi("abi-before", RESOLVER_A, 11, 1, "1").await?;
    fixture.version_changed("reset", RESOLVER_A, 11, 2).await?;
    fixture.abi("abi-after", RESOLVER_A, 11, 3, "4").await?;
    fixture
        .abi("abi-wide", RESOLVER_A, 12, 0, &(1_u128 << 100).to_string())
        .await?;
    fixture.run(11, None, RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["4"])
    );
    fixture.run(12, Some(11), RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["4", "1267650600228229401496703205376"])
    );
    fixture.cleanup().await
}

/// A declared ENSv1 mirror serves the ENSv1 resolver's storage for the queried node; without a
/// projected ENSv1 resolver its inventory is unsupported and the list is withheld.
#[tokio::test]
async fn mirrors_list_the_mirrored_storage_or_withhold_an_unsupported_row() -> Result<()> {
    let fixture = Fixture::new("abi_mirror").await?;
    fixture.v2_pointer("v2-pointer", 10, 0, MIRROR).await?;
    fixture.v1_pointer("pointer-a", 10, 1, RESOLVER_A).await?;
    fixture.abi("abi-mirrored", RESOLVER_A, 11, 0, "32").await?;
    fixture.run(11, None, RunMode::Normal).await?;
    let mirrored = fixture.inventory(V2_RESOURCE).await?;
    assert_eq!(mirrored["support_status"], "supported", "{mirrored}");
    assert_eq!(mirrored["provenance"]["resolver_address"], MIRROR);
    assert_eq!(
        fixture.abi_content_types(V2_RESOURCE).await?,
        observed(&["32"])
    );
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["32"])
    );
    fixture.cleanup().await?;

    let fixture = Fixture::new("abi_mirror_unsupported").await?;
    fixture.v2_pointer("v2-pointer", 10, 0, MIRROR).await?;
    fixture
        .abi("abi-unselected", RESOLVER_A, 11, 0, "32")
        .await?;
    fixture.run(11, None, RunMode::Normal).await?;
    let mirrored = fixture.inventory(V2_RESOURCE).await?;
    assert_eq!(mirrored["support_status"], "unsupported", "{mirrored}");
    assert_eq!(
        fixture.abi_content_types(V2_RESOURCE).await?,
        AbiContentTypes::Unavailable(AbiContentTypesUnavailable::InventoryNotAuthoritative)
    );
    fixture.cleanup().await
}

/// Selected evidence that is retracted or removed after Project ran withholds the list instead of
/// shrinking it, and a nonstandard multi-bit write is flagged rather than split.
#[tokio::test]
async fn retracted_evidence_and_multi_bit_writes_withhold_the_list() -> Result<()> {
    let fixture = Fixture::new("abi_integrity").await?;
    fixture.v1_pointer("pointer-a", 10, 0, RESOLVER_A).await?;
    fixture.abi("abi-one", RESOLVER_A, 10, 1, "1").await?;
    fixture.abi("abi-two", RESOLVER_A, 10, 2, "2").await?;
    fixture.run(10, None, RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["1", "2"])
    );
    for statement in [
        "UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE event_identity = 'abi_integrity:abi-two'",
        "DELETE FROM normalized_events WHERE event_identity = 'abi_integrity:abi-two'",
    ] {
        sqlx::query(statement).execute(&fixture.pool).await?;
        assert_eq!(
            fixture.abi_content_types(V1_RESOURCE).await?,
            AbiContentTypes::Unavailable(AbiContentTypesUnavailable::ObservationsStale),
            "{statement}"
        );
    }
    fixture.run(10, Some(10), RunMode::Redo).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["1"])
    );

    fixture.abi("abi-mask", RESOLVER_A, 11, 0, "3").await?;
    fixture.run(11, Some(10), RunMode::Normal).await?;
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        AbiContentTypes::Unavailable(AbiContentTypesUnavailable::ContentTypeNotSingleBit)
    );
    fixture.cleanup().await
}

/// A route loads its inventory row, then Project publishes a newer target that reclassifies the
/// row's resolver (here to the direct PublicResolverV2 classification (role public_resolver_v2),
/// which admits no ABI event) and, as a declaration change does, republishes the inventory rows
/// that point at it. The ABI read for the held row must not combine the older row with the newer
/// classification.
#[tokio::test]
async fn a_resolver_reclassified_after_the_row_was_loaded_is_stale_not_unsupported() -> Result<()> {
    let fixture = Fixture::new("abi_reclassified").await?;
    fixture.v1_pointer("pointer-a", 10, 0, RESOLVER_A).await?;
    fixture.abi("abi-one", RESOLVER_A, 10, 1, "1").await?;
    fixture.run(11, None, RunMode::Normal).await?;
    let held = fixture.held(V1_RESOURCE).await?;
    assert_eq!(fixture.abi_content_types_of(&held).await?, observed(&["1"]));

    // The publish at block 12, in the shape Project writes it.
    sqlx::raw_sql(&format!(
        "UPDATE resolver_current
         SET declared_summary = jsonb_set(declared_summary, '{{classification}}',
                 declared_summary -> 'classification' || jsonb_build_object(
                     'source_family', 'ens_v2_resolver_l1', 'role', 'public_resolver_v2')),
             chain_positions = chain_positions || jsonb_build_object(
                 'target_block_number', 12, 'target_block_hash', '{hash}'),
             canonicality_summary = canonicality_summary || jsonb_build_object(
                 'target_block_number', 12, 'target_block_hash', '{hash}')
         WHERE chain_id = '{CHAIN}' AND resolver_address = '{RESOLVER_A}';
         UPDATE record_inventory_current
         SET chain_positions = chain_positions || jsonb_build_object(
                 'target_block_number', 12, 'target_block_hash', '{hash}'),
             canonicality_summary = canonicality_summary || jsonb_build_object(
                 'target_block_number', 12, 'target_block_hash', '{hash}'),
             last_recomputed_at = now()
         WHERE resource_id = '{V1_RESOURCE}'::uuid",
        hash = block_hash(12),
    ))
    .execute(&fixture.pool)
    .await?;

    assert_eq!(
        fixture.abi_content_types_of(&held).await?,
        AbiContentTypes::Unavailable(AbiContentTypesUnavailable::ObservationsStale)
    );
    // The republished row is read with the classification it was built with.
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        AbiContentTypes::Unavailable(AbiContentTypesUnavailable::ObservationsNotSupported)
    );
    fixture.cleanup().await
}

/// A record write on a shared resolver for another node re-stamps the resolver's row at the new
/// target without republishing this name's inventory row, so a resolver row newer than the
/// inventory row is ordinary and must not make the answer stale.
#[tokio::test]
async fn a_resolver_restamped_by_another_names_write_keeps_the_answer() -> Result<()> {
    let fixture = Fixture::new("abi_restamped").await?;
    fixture.v1_pointer("pointer-a", 10, 0, RESOLVER_A).await?;
    fixture.abi("abi-one", RESOLVER_A, 10, 1, "1").await?;
    fixture.run(11, None, RunMode::Normal).await?;
    let held = fixture.held(V1_RESOURCE).await?;
    let other_node = bigname_lookup::ens_namehash_hex("other.fixture")?;
    fixture
        .insert(
            "other-text",
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(fixture.v1_manifest),
            12,
            0,
            RESOLVER_A,
            json!({
                "source_event": "TextChanged", "resolver": RESOLVER_A, "node": other_node,
                "record_key": "text:url", "record_family": "text", "selector_key": "url",
                "value_retained": true, "value": "https://other.example"
            }),
        )
        .await?;
    fixture.run(12, Some(11), RunMode::Normal).await?;

    assert_eq!(
        fixture
            .target_block("resolver_current", "resolver_address", RESOLVER_A)
            .await?,
        Some(12)
    );
    assert_eq!(
        fixture
            .target_block("record_inventory_current", "resource_id", V1_RESOURCE)
            .await?,
        Some(11)
    );
    assert_eq!(fixture.abi_content_types_of(&held).await?, observed(&["1"]));
    assert_eq!(
        fixture.abi_content_types(V1_RESOURCE).await?,
        observed(&["1"])
    );
    fixture.cleanup().await
}

#[derive(sqlx::FromRow)]
struct HeldInventory {
    resource_id: uuid::Uuid,
    record_version_boundary_key: String,
    support_status: String,
    provenance: Value,
    chain_positions: Value,
    last_recomputed_at: time::OffsetDateTime,
}

struct Fixture {
    id: &'static str,
    database: TestDatabase,
    pool: PgPool,
    v1_manifest: i64,
    root_manifest: i64,
    node: String,
    logical_name_id: String,
}

impl Fixture {
    async fn new(id: &'static str) -> Result<Self> {
        let (database, pool) = database(id).await?;
        for number in FIRST_BLOCK..=LAST_BLOCK {
            sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
                .bind(CHAIN).bind(block_hash(number)).bind(number).bind(1_800_000_000 + number).execute(&pool).await?;
        }
        manifest(
            &pool,
            id,
            "ens_v2_resolver_l1",
            &json!({
                "deployment_epoch": "fixture",
                "correlation_addresses": {"ens_v1_registry": V1_REGISTRY},
                "contracts": [{"role": "ensv1_mirror_resolver", "address": MIRROR,
                               "proxy_kind": "none", "start_block": 0}],
                "capability_flags": {}
            }),
        )
        .await?;
        let v1_manifest = manifest(
            &pool,
            id,
            "ens_v1_resolver_l1",
            &json!({"deployment_epoch": "fixture", "contracts": [
                {"role": "public_resolver", "address": RESOLVER_A, "proxy_kind": "none",
                 "start_block": 0},
                {"role": "public_resolver_b", "address": RESOLVER_B, "proxy_kind": "none",
                 "start_block": 0}
            ]}),
        )
        .await?;
        let root_manifest = manifest(&pool, id, "ens_v2_root_l1", &json!({})).await?;
        for instance in [ROOT_INSTANCE, MIRROR_INSTANCE] {
            sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
                .bind(instance).bind(CHAIN).execute(&pool).await?;
        }
        sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id) VALUES ($1::uuid,$2,$3,$4,$5,$6)")
            .bind(MIRROR_INSTANCE).bind(CHAIN).bind(MIRROR).bind(FIRST_BLOCK)
            .bind(block_hash(FIRST_BLOCK)).bind(root_manifest).execute(&pool).await?;
        sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2::uuid,$3::uuid,'fixture','fixture',$4,$5,$6,'canonical')")
            .bind(CHAIN).bind(ROOT_INSTANCE).bind(MIRROR_INSTANCE).bind(root_manifest)
            .bind(FIRST_BLOCK).bind(block_hash(FIRST_BLOCK)).execute(&pool).await?;

        let node = bigname_lookup::ens_namehash_hex(NAME)?;
        let logical_name_id = format!("ens:{node}");
        let labelhashes: Vec<_> = NAME
            .split('.')
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
            .collect();
        sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens',$2,string_to_array($2,'.'),$3,$4,$5,'fixture','active',$6,$7,$8,'canonical')")
            .bind(&logical_name_id).bind(NAME).bind(b"\x03abi\x07fixture\0".as_slice()).bind(&node)
            .bind(labelhashes).bind(CHAIN).bind(block_hash(FIRST_BLOCK)).bind(FIRST_BLOCK)
            .execute(&pool).await?;
        for (resource, binding, arm) in [
            (
                V1_RESOURCE,
                "69200000-0000-0000-0000-000000000101",
                "ens_v1",
            ),
            (
                V2_RESOURCE,
                "69200000-0000-0000-0000-000000000102",
                "ens_v2",
            ),
        ] {
            sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,$4,'canonical')")
                .bind(resource).bind(CHAIN).bind(block_hash(FIRST_BLOCK)).bind(FIRST_BLOCK)
                .execute(&pool).await?;
            sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,to_timestamp($5),$6,$7,$8,'canonical')")
                .bind(binding).bind(&logical_name_id).bind(resource).bind(arm)
                .bind(1_800_000_000 + FIRST_BLOCK).bind(CHAIN).bind(block_hash(FIRST_BLOCK))
                .bind(FIRST_BLOCK).execute(&pool).await?;
        }
        Ok(Self {
            id,
            database,
            pool,
            v1_manifest,
            root_manifest,
            node,
            logical_name_id,
        })
    }

    async fn v1_pointer(&self, identity: &str, block: i64, log: i64, resolver: &str) -> Result<()> {
        let after = json!({"node": self.node, "resolver": resolver});
        self.insert(
            identity,
            Some(V1_RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            block,
            log,
            V1_REGISTRY,
            after,
        )
        .await
    }

    async fn v2_pointer(&self, identity: &str, block: i64, log: i64, resolver: &str) -> Result<()> {
        let after = json!({"node": self.node, "resolver": resolver});
        self.insert(
            identity,
            Some(V2_RESOURCE),
            "ResolverChanged",
            "ens_v2_root_l1",
            Some(self.root_manifest),
            block,
            log,
            V1_REGISTRY,
            after,
        )
        .await
    }

    /// An ENSv1 `ABIChanged` exactly as the adapter normalizes it, including the content type it
    /// also stores as `value`; the read must use `selector_key` only.
    async fn abi(
        &self,
        identity: &str,
        resolver: &str,
        block: i64,
        log: i64,
        content_type: &str,
    ) -> Result<()> {
        let after = json!({
            "source_event": "ABIChanged", "resolver": resolver, "node": self.node,
            "record_key": format!("abi:{content_type}"), "record_family": "abi",
            "selector_key": content_type, "value_retained": true, "value": "999"
        });
        self.insert(
            identity,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(self.v1_manifest),
            block,
            log,
            resolver,
            after,
        )
        .await
    }

    async fn text(&self, identity: &str, resolver: &str, block: i64, log: i64) -> Result<()> {
        let after = json!({
            "source_event": "TextChanged", "resolver": resolver, "node": self.node,
            "record_key": "text:url", "record_family": "text", "selector_key": "url",
            "value_retained": true, "value": "https://b.example"
        });
        self.insert(
            identity,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(self.v1_manifest),
            block,
            log,
            resolver,
            after,
        )
        .await
    }

    async fn version_changed(
        &self,
        identity: &str,
        resolver: &str,
        block: i64,
        log: i64,
    ) -> Result<()> {
        let after = json!({"source_event": "VersionChanged", "resolver": resolver, "node": self.node, "record_version": 1});
        self.insert(
            identity,
            None,
            "RecordVersionChanged",
            "ens_v1_resolver_l1",
            Some(self.v1_manifest),
            block,
            log,
            resolver,
            after,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert(
        &self,
        identity: &str,
        resource_id: Option<&str>,
        kind: &str,
        source_family: &str,
        manifest_id: Option<i64>,
        block: i64,
        log: i64,
        emitter: &str,
        after_state: Value,
    ) -> Result<()> {
        let logical_name_id = resource_id.map(|_| self.logical_name_id.clone());
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES ($1,'ens',$2,$3::uuid,$4,$5,1,$6,$7,$8,$9,$10,0,$11,'ens_v1_unwrapped_authority','canonical',$12,$13)")
            .bind(format!("{}:{identity}", self.id)).bind(logical_name_id).bind(resource_id)
            .bind(kind).bind(source_family).bind(manifest_id).bind(CHAIN).bind(block)
            .bind(block_hash(block)).bind(format!("0x{:064x}", block * 10 + log)).bind(log)
            .bind(after_state).bind(json!({"emitting_address": emitter}))
            .execute(&self.pool).await?;
        Ok(())
    }

    async fn run(&self, target: i64, previous: Option<i64>, mode: RunMode) -> Result<()> {
        let outcome = Engine::new(self.pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                target_block: target,
                affected_from_block: previous.map_or(0, |previous| (previous + 1).min(target)),
                affected_to_block: target,
                resume_current: previous.map(|number| Marker {
                    number,
                    hash: block_hash(number),
                }),
                mode,
            })
            .await?;
        assert!(outcome.complete);
        Ok(())
    }

    async fn inventory(&self, resource: &str) -> Result<Value> {
        sqlx::query_scalar(
            "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
             FROM record_inventory_current row WHERE resource_id = $1::uuid",
        )
        .bind(resource)
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("no inventory row for {resource}"))
    }

    /// The published row as a route holds it after loading it.
    async fn held(&self, resource: &str) -> Result<HeldInventory> {
        sqlx::query_as(
            "SELECT resource_id, record_version_boundary_key, support_status, provenance, \
                    chain_positions, last_recomputed_at \
             FROM record_inventory_current WHERE resource_id = $1::uuid",
        )
        .bind(resource)
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("no inventory row for {resource}"))
    }

    /// The storage read the records and lookup routes serve, over the published row.
    async fn abi_content_types(&self, resource: &str) -> Result<AbiContentTypes> {
        let held = self.held(resource).await?;
        self.abi_content_types_of(&held).await
    }

    /// The same read over a row the caller loaded earlier.
    async fn abi_content_types_of(&self, held: &HeldInventory) -> Result<AbiContentTypes> {
        let mut answers = load_record_inventory_abi_content_types(
            &self.pool,
            &[AbiContentTypesInput {
                authoritative: held.support_status == "supported",
                resource_id: held.resource_id,
                record_version_boundary_key: &held.record_version_boundary_key,
                provenance: &held.provenance,
                chain_positions: &held.chain_positions,
                last_recomputed_at: held.last_recomputed_at,
            }],
        )
        .await?;
        Ok(answers.remove(0))
    }

    async fn target_block(&self, table: &str, key: &str, value: &str) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT (chain_positions ->> 'target_block_number')::bigint FROM {table} \
             WHERE {key}::text = $1"
        ))
        .bind(value)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn cleanup(self) -> Result<()> {
        self.database.cleanup().await
    }
}

async fn manifest(pool: &PgPool, id: &str, source_family: &str, payload: &Value) -> Result<i64> {
    let manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens',$1,$2,'fixture','active','fixture',$3,$4) RETURNING manifest_id")
        .bind(source_family).bind(CHAIN).bind(format!("fixture/{id}/{source_family}.toml"))
        .bind(payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','SourceManifestUpdated',$2,1,$3,$4,'manifest_sync','canonical',$5)")
        .bind(format!("manifest:{id}:{source_family}")).bind(source_family).bind(manifest_id)
        .bind(CHAIN)
        .bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload}))
        .execute(pool).await?;
    Ok(manifest_id)
}

fn block_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(format!("record_abi_{name}"))).await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    Ok((database, pool))
}
