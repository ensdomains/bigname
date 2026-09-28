//! Corpus policy over real family publications. The shared fixture writes identity and
//! normalized inputs and uses the production Project reducers.
#[path = "../../../../../../crates/project/tests/families_support/mod.rs"]
mod support;

use super::{
    Corpus, load_table_scale, namespace_counts, permissions, readers, table_scale_failures,
};
use crate::budgets::{BudgetProfile, BudgetsFile, GateBudgets};
use anyhow::Result;
use bigname_project::families::FamilyMode;
use serde_json::json;
use support::{CHAIN, Fixture, hash, uuid};

const BASE: &str = "base-sepolia";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
fn node(id: u32) -> String {
    format!("0x{id:064x}")
}
fn name(ns: &str, id: u32) -> String {
    format!("{ns}:{}", node(id))
}

fn budgets() -> GateBudgets {
    let mut value = BudgetsFile::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/release-gate.toml"),
    )
    .unwrap()
    .profile(BudgetProfile::Production)
    .clone();
    value.api_corpus_size = 2;
    value.api_min_specialized_corpus_size = 1;
    value
}

async fn setup(label: &str) -> Result<Fixture> {
    let fixture = Fixture::new(label, 20).await?;
    fixture.lineage(BASE, 20).await?;
    for (ns, chain) in [("ens", CHAIN), ("basenames", BASE)] {
        sqlx::query("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
            VALUES (1,$1,'benchmark_non_resolver',$2,'test','active','ensip15@ens-normalize-0.1.1',$1,'{}')")
            .bind(ns).bind(chain).execute(&fixture.pool).await?;
    }
    Ok(fixture)
}

async fn event(
    f: &Fixture,
    ns: &str,
    id: u32,
    kind: &str,
    family: &str,
    after: serde_json::Value,
) -> Result<()> {
    let chain = if ns == "ens" { CHAIN } else { BASE };
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,logical_name_id,resource_id,after_state)
        VALUES ($1,$2,$3,$4,1,$5,10,$6,'transaction',0,$7,'ens_v1_unwrapped_authority','canonical',$8,$9::uuid,$10) ON CONFLICT (event_identity) DO UPDATE SET after_state=EXCLUDED.after_state")
        .bind(format!("{ns}-{id}-{kind}")).bind(ns).bind(kind).bind(family).bind(chain).bind(hash(10))
        .bind(i64::from(id)).bind(name(ns,id)).bind(uuid(id)).bind(after).execute(&f.pool).await?;
    Ok(())
}

async fn registered(f: &Fixture, ns: &str, id: u32) -> Result<()> {
    let chain = if ns == "ens" { CHAIN } else { BASE };
    let registrar = if ns == "ens" {
        "ens_v1_registrar_l1"
    } else {
        "basenames_base_registrar"
    };
    let registry = if ns == "ens" {
        "ens_v1_registry_l1"
    } else {
        "basenames_base_registry"
    };
    f.surface_on(chain, &name(ns, id), &node(id)).await?;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,0,'canonical')")
        .bind(uuid(id)).bind(chain).bind(hash(0)).execute(&f.pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,provenance,canonicality_state)
        VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,to_timestamp(1800000000+9*12),$5,$6,9,'{\"transaction_index\":0,\"log_index\":0}','canonical')")
        .bind(uuid(id+10000)).bind(name(ns,id)).bind(uuid(id)).bind(if ns=="ens" {"ens_v1"} else {"basenames"})
        .bind(chain).bind(hash(9)).execute(&f.pool).await?;
    event(f,ns,id,"RegistrationGranted",registrar,json!({"authority_kind":"registrar","status":"registered","registrant":OWNER,"expiry":2_000_000_000u64})).await?;
    event(
        f,
        ns,
        id,
        "AuthorityTransferred",
        registry,
        json!({"owner":OWNER}),
    )
    .await?;
    event(f,ns,id,"PermissionChanged",registrar,json!({"subject":OWNER,"scope":{"kind":"resource"},"effective_powers":["resource_control"]})).await?;
    Ok(())
}

async fn seed(f: &Fixture) -> Result<()> {
    for (ns, ids) in [("ens", [1, 2]), ("basenames", [101, 102])] {
        for id in ids {
            registered(f, ns, id).await?;
        }
        let registry = if ns == "ens" {
            "ens_v1_registry_l1"
        } else {
            "basenames_base_registry"
        };
        event(f,ns,ids[1],"SubregistryChanged",registry,json!({"source_event":"NewOwner", "node":node(ids[0]),"child_node":node(ids[1]),"labelhash":node(99),"owner":OWNER})).await?;
    }
    for (ns, id) in [("ens", 1), ("basenames", 101)] {
        event(f,ns,id,"ReverseChanged","ens_v1_reverse_registrar_l1",json!({"address":OWNER,"namespace":ns,"coin_type":"60","source_event":"NameForAddrChanged"})).await?;
        event(f,ns,id,"RecordChanged","ens_v1_resolver_l1",json!({"record_key":"name","source_event":"NameForAddrChanged","raw_name":"alice.eth","primary_claim_source":{"address":OWNER,"namespace":ns,"coin_type":"60"}})).await?;
    }
    publish(f).await
}

async fn publish(f: &Fixture) -> Result<()> {
    f.apply(12, FamilyMode::Rebuild).await?;
    let token = bigname_project::families::input_token(&f.pool, BASE).await?;
    bigname_project::families::apply(
        &f.pool,
        BASE,
        &support::marker(12),
        FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(support::CONTENT_HASH),
    )
    .await?;
    Ok(())
}

#[test]
fn production_scale_rejects_staging_sized_tables() {
    assert!(!table_scale_failures(50_000, 75_000, 3_000_000, 3_000_000).is_empty());
    assert!(table_scale_failures(3_000_000, 3_000_000, 3_000_000, 3_000_000).is_empty());
}

#[tokio::test]
async fn names_addresses_parents_and_scale_use_real_family_admission() -> Result<()> {
    let f = setup("benchmark_family_population").await?;
    seed(&f).await?;
    let names = readers::names(&f.pool, 2, false).await?;
    assert_eq!(
        namespace_counts(&names),
        [("basenames".into(), 1), ("ens".into(), 1)].into()
    );
    assert_eq!(
        namespace_counts(&readers::names(&f.pool, 3, false).await?),
        [("basenames".into(), 2), ("ens".into(), 1)].into()
    );
    let parents = readers::names(&f.pool, 2, true).await?;
    assert_eq!(namespace_counts(&parents), namespace_counts(&names));
    let (relations, addresses) = readers::addresses(&f.pool, 2).await?;
    assert!(relations >= 4);
    assert_eq!(addresses.len(), 2);
    assert_eq!(
        super::stratified::address_namespace_counts(&addresses),
        namespace_counts(&names)
    );
    let primaries = readers::primary_names(&f.pool, 2).await?;
    assert_eq!(
        super::stratified::primary_namespace_counts(&primaries),
        namespace_counts(&names)
    );
    let scale = load_table_scale(&f.pool).await?;
    assert_eq!(scale.name_current_rows, 4);
    assert_eq!(scale.address_names_current_rows, relations);
    assert!(!permissions::load(&f.pool, 4).await?.is_empty());
    f.cleanup().await
}

#[tokio::test]
async fn subregistry_parents_are_sampled_only_while_their_children_are_visible() -> Result<()> {
    let f = setup("benchmark_family_subregistry").await?;
    registered(&f, "ens", 1).await?;
    f.surface(&name("ens", 2), &node(2)).await?;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,0,'canonical')")
        .bind(uuid(2)).bind(CHAIN).bind(hash(0)).execute(&f.pool).await?;
    let parent: String =
        sqlx::query_scalar("SELECT raw_name FROM name_surfaces WHERE logical_name_id=$1")
            .bind(name("ens", 1))
            .fetch_one(&f.pool)
            .await?;
    let child = bigname_domain::normalization::normalize_name(&format!("child.{parent}"))?;
    let labels: Vec<String> = child
        .normalized_labels
        .iter()
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
        .collect();
    sqlx::query("UPDATE name_surfaces SET raw_name=$2,raw_labels=$3,dns_encoded_name=$4,labelhashes=$5 WHERE logical_name_id=$1")
        .bind(name("ens", 2)).bind(child.normalized_name).bind(child.normalized_labels)
        .bind(child.dns_encoded_name).bind(labels).execute(&f.pool).await?;
    sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
        .bind(uuid(90)).bind(CHAIN).execute(&f.pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address) VALUES ($1::uuid,$2,$3)")
        .bind(uuid(90)).bind(CHAIN).bind(OWNER).execute(&f.pool).await?;
    event(
        &f,
        "ens",
        1,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":OWNER}),
    )
    .await?;
    event(&f,"ens",2,"RegistrationGranted","ens_v2_registry_l1",json!({"registry_contract_instance_id":uuid(90),"registrant":OWNER,"expiry":2_000_000_000u64})).await?;
    publish(&f).await?;
    assert_eq!(
        readers::names(&f.pool, 2, true).await?,
        vec![("ens".into(), parent)]
    );
    sqlx::query("UPDATE name_surfaces SET visibility_state='shadow',deactivation_reason='fixture',deactivated_at=now() WHERE logical_name_id=$1")
        .bind(name("ens", 2)).execute(&f.pool).await?;
    assert!(readers::names(&f.pool, 2, true).await?.is_empty());
    f.cleanup().await
}

#[tokio::test]
async fn orphaned_identity_and_inactive_namespaces_do_not_meet_scale_or_sample_floors() -> Result<()>
{
    let f = setup("benchmark_family_visibility").await?;
    seed(&f).await?;
    sqlx::query("UPDATE name_surfaces SET canonicality_state='orphaned' WHERE logical_name_id=$1")
        .bind(name("ens", 1))
        .execute(&f.pool)
        .await?;
    assert_eq!(load_table_scale(&f.pool).await?.name_current_rows, 3);
    sqlx::query(
        "UPDATE manifest_versions SET rollout_status='deprecated' WHERE namespace='basenames'",
    )
    .execute(&f.pool)
    .await?;
    assert_eq!(load_table_scale(&f.pool).await?.name_current_rows, 1);
    assert_eq!(readers::names(&f.pool, 10, false).await?.len(), 1);
    f.cleanup().await
}

#[tokio::test]
async fn corpus_load_keeps_namespace_specialized_and_aggregate_floors_load_bearing() -> Result<()> {
    let f = setup("benchmark_family_floors").await?;
    seed(&f).await?;
    let mut limits = budgets();
    limits.api_corpus_size = 8;
    limits.api_min_specialized_corpus_size = 8;
    let (_, failures) = Corpus::load(&f.pool, &limits).await?;
    let failures = failures.join("; ");
    for expected in [
        "name corpus",
        "address corpus",
        "subname parent corpus",
        "permission subject corpus",
        "successful primary-name corpus",
        "canonical retained registration",
    ] {
        assert!(
            failures.contains(expected),
            "missing {expected}: {failures}"
        );
    }
    f.cleanup().await
}

#[tokio::test]
async fn primary_sampling_excludes_invalid_claims_and_unpublished_families() -> Result<()> {
    let f = setup("benchmark_family_primary_admission").await?;
    seed(&f).await?;
    sqlx::query("UPDATE normalized_events SET after_state=jsonb_set(after_state,'{raw_name}','\"invalid_name.eth\"') WHERE namespace='ens' AND event_kind='RecordChanged'")
        .execute(&f.pool).await?;
    publish(&f).await?;
    let rows = readers::primary_names(&f.pool, 2).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, "basenames");
    sqlx::query("UPDATE project_family_marker SET state='bootstrap_pending' WHERE chain_id=$1")
        .bind(BASE)
        .execute(&f.pool)
        .await?;
    assert!(readers::primary_names(&f.pool, 2).await.is_err());
    assert!(readers::names(&f.pool, 2, false).await.is_err());
    f.cleanup().await
}

#[tokio::test]
async fn permission_corpus_retains_superseded_registration_audit_targets() -> Result<()> {
    let f = setup("benchmark_family_permission_audit").await?;
    registered(&f, "ens", 1).await?;
    sqlx::query("UPDATE surface_bindings SET active_to=to_timestamp(1800000000+11*12) WHERE surface_binding_id=$1::uuid")
        .bind(uuid(10001)).execute(&f.pool).await?;
    f.binding(
        &uuid(20000),
        &name("ens", 1),
        &uuid(333),
        "ens_v1",
        11,
        0,
        None,
    )
    .await?;
    registered(&f, "ens", 2).await?;
    publish(&f).await?;
    let targets = permissions::load(&f.pool, 4).await?;
    assert!(!targets.is_empty());
    assert!(
        targets
            .iter()
            .any(|target| target.retained_registration && target.registration_id == uuid(1)),
        "{targets:?}"
    );
    f.cleanup().await
}
