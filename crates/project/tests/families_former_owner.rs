//! The last holder after ENSv2 path expiry or explicit unregister (TYR-63). Those names serve
//! `lapsed_registration` with the registry token's holder when the registration ended, held
//! through the registry. Both path expiry and explicit unregister retain the scheduled dates;
//! the terminal cause stays distinct. Re-registration drops the former-holder record.
//! This arbitrary registry has no admitted registrar grace policy, so grace ends at expiry.
//! The registry retains expiry and latest owner in its state; unregister burns the owner token.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L353-L362 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L270-L292 @ ens_v2_sepolia_20260916@366de741)
#[path = "families_support/mod.rs"]
mod support;

use alloy_primitives::{B256, keccak256};
use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use support::{
    CHAIN, Event, Fixture,
    address_listing::{FORMER_NAME_KEY, former_read, parent_read, q_read},
    hash, uuid,
};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e6";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const EXPIRY: u64 = 1_800_000_060;
const DAY: u64 = 86_400;

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |parent, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        keccak256(input)
    })
}

async fn surface(fixture: &Fixture, name: &str) -> Result<String> {
    let node = format!("{:#x}", namehash(name));
    let logical_name_id = format!("ens:{node}");
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let labelhashes: Vec<String> = normalized
        .normalized_labels
        .iter()
        .map(|label| format!("{:#x}", keccak256(label.as_bytes())))
        .collect();
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', $5, $6, $7, $2, $8, $9, 'active', $3, $4, 0, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(&node)
    .bind(CHAIN)
    .bind(hash(0))
    .bind(normalized.normalized_name)
    .bind(normalized.normalized_labels)
    .bind(normalized.dns_encoded_name)
    .bind(labelhashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .execute(&fixture.pool)
    .await?;
    Ok(logical_name_id)
}

/// `name` registered in the ENSv2 `eth` registry to ALICE at block 2 on resource `n`, then
/// transferred to BOB at block 3.
async fn registered(fixture: &Fixture, name: &str, n: u32) -> Result<(String, String)> {
    registered_at_log(fixture, name, n, 1).await
}

/// [`registered`], with its events at log `log` of their blocks, so several names register in
/// the same blocks.
async fn registered_at_log(
    fixture: &Fixture,
    name: &str,
    n: u32,
    log: i64,
) -> Result<(String, String)> {
    let logical_name_id = surface(fixture, name).await?;
    let resource = uuid(0x5000 + n);
    fixture
        .binding(
            &uuid(500 + n),
            &logical_name_id,
            &resource,
            "ens_v2",
            2,
            1,
            None,
        )
        .await?;
    let key = json!({"registry_contract_instance_id": "eth", "token_id": format!("{n}")});
    let with = |extra: Value| {
        let mut after = key.clone();
        after
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        after
    };
    fixture
        .write(
            2,
            log,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&logical_name_id),
            Some(&resource),
            with(
                json!({"status": "registered", "registrant": ALICE, "owner": ALICE,
                        "expiry": EXPIRY}),
            ),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            3,
            log,
            "TokenControlTransferred",
            "ens_v2_registry_l1",
            Some(&logical_name_id),
            Some(&resource),
            with(json!({"from": ALICE, "to": BOB})),
            REGISTRY,
        )
        .await?;
    Ok((logical_name_id, resource))
}

/// The stored former-owner rows of `logical_name_id` in the address listing (F18): (address,
/// former expiry key).
async fn former_rows(fixture: &Fixture, logical_name_id: &str) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT address, former_expires_key::text FROM project_address_listing
         WHERE chain_id = $1 AND logical_name_id = $2 AND former_owner ORDER BY address",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_all(&fixture.pool)
    .await?)
}

/// The interpreter's path-expiry release at `block` of the registration of token `n` on
/// `resource`: on the resource, without a name.
async fn expired(fixture: &Fixture, resource: &str, n: u32, block: i64) -> Result<()> {
    let identity = format!("path-expired-{block}-{n}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                n.into(),
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .resource(resource)
            .after(json!({"source_event": "RegistryPathExpired",
                              "registry_contract_instance_id": "eth", "token_id": format!("{n}"),
                              "expiry": EXPIRY, "released_at": EXPIRY}))
            .raw(json!({"emitting_address": REGISTRY}))
            .synthesised(),
        )
        .await?;
    Ok(())
}

/// A name of `labels` labels of 60 characters each under `.eth`, the first label `first`.
fn long_name(first: &str, labels: usize) -> String {
    let mut name = vec![first.to_owned()];
    name.extend((0..labels).map(|i| format!("l{i:02}{}", "a".repeat(57))));
    name.push("eth".to_owned());
    name.join(".")
}

/// A name of `labels` labels of 64 hex characters under `.eth`, the first label `first`: hashes,
/// which PostgreSQL cannot compress, so the stored name and an index key of it keep their width.
fn hex_name(first: &str, labels: usize) -> String {
    let mut name = vec![first.to_owned()];
    name.extend(
        (0..labels)
            .map(|i| format!("{:#x}", keccak256(format!("label {i}").as_bytes()))[2..].to_owned()),
    );
    name.push("eth".to_owned());
    name.join(".")
}

/// `prefix` followed by labels of at most 60 characters and `.eth`, `total` bytes long.
fn padded(prefix: &str, total: usize) -> String {
    let mut name = prefix.to_owned();
    assert!(
        total >= name.len() + ".eth".len() + 2,
        "{prefix} leaves no room for a label"
    );
    loop {
        let left = total - name.len() - ".eth".len();
        if left == 0 {
            break;
        }
        // A label needs its dot and a character, and must not leave a single byte behind.
        let mut label = (left - 1).min(60);
        if left - 1 - label == 1 {
            label -= 1;
        }
        name.push('.');
        name.push_str(&"r".repeat(label));
    }
    name + ".eth"
}

async fn registration(fixture: &Fixture, logical_name_id: &str) -> Result<Value> {
    let row = load_family_name(&fixture.pool, logical_name_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no composed row"))?;
    Ok(row.declared_summary["registration"].clone())
}

#[tokio::test]
async fn an_expired_ens_v2_registration_names_its_last_holder_until_it_is_registered_again()
-> Result<()> {
    let fixture = Fixture::new("families_former_expired", 12).await?;
    let (name, resource) = registered(&fixture, "expired.eth", 1).await?;
    // The interpreter's path-expiry release: on the resource, without a name.
    let identity = "path-expired-6";
    fixture
        .event(
            Event::new(identity, 6, 1, "RegistrationReleased", "ens_v2_registry_l1")
                .resource(&resource)
                .after(json!({"source_event": "RegistryPathExpired",
                              "registry_contract_instance_id": "eth", "token_id": "1",
                              "expiry": EXPIRY, "released_at": EXPIRY}))
                .raw(json!({"emitting_address": REGISTRY}))
                .synthesised(),
        )
        .await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let released = registration(&fixture, &name).await?;
    ensure!(released["status"] == json!("released"), "{released}");
    ensure!(
        released["lapsed_registration"]["owner"] == json!(BOB)
            && released["lapsed_registration"]["held_through"] == json!("registry")
            && released["lapsed_registration"]["release_kind"] == json!("expired"),
        "{released}"
    );
    ensure!(
        released["expiry"] == json!(EXPIRY.to_string())
            && released["grace_ends_at"] == json!(EXPIRY.to_string()),
        "{released}"
    );
    // Rows 15w and 16 of TYR-127: the address listing lists the last holder as the former owner,
    // keyed by the served expiry.
    let former = former_rows(&fixture, &name).await?;
    ensure!(
        former == [(BOB.to_owned(), EXPIRY.to_string())],
        "the listing lists the last holder as former owner: {former:?}"
    );

    // CAROL registers it again: no former holder remains.
    fixture
        .write(
            8,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&name),
            Some(&uuid(0x5100)),
            json!({"registry_contract_instance_id": "eth", "token_id": "2",
                   "status": "registered", "registrant": CAROL, "owner": CAROL,
                   "expiry": EXPIRY + 400 * DAY}),
            REGISTRY,
        )
        .await?;
    fixture.apply(8, FamilyMode::Normal).await?;
    let again = registration(&fixture, &name).await?;
    ensure!(
        again.get("lapsed_registration").is_none_or(Value::is_null),
        "{again}"
    );
    let former = former_rows(&fixture, &name).await?;
    ensure!(
        former.is_empty(),
        "the re-registration drops the former owner row: {former:?}"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn an_unregistered_ens_v2_registration_retains_its_last_holder_and_scheduled_expiry()
-> Result<()> {
    let fixture = Fixture::new("families_former_unregistered", 12).await?;
    let (name, resource) = registered(&fixture, "gone.eth", 3).await?;
    fixture
        .write(
            4,
            1,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            Some(&name),
            Some(&resource),
            json!({"source_event": "LabelUnregistered", "registry_contract_instance_id": "eth",
                   "token_id": "3", "sender": BOB}),
            REGISTRY,
        )
        .await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let released = registration(&fixture, &name).await?;
    ensure!(
        released["status"] == json!("released")
            && released["expiry"] == json!(EXPIRY.to_string())
            && released["grace_ends_at"] == json!(EXPIRY.to_string()),
        "{released}"
    );
    ensure!(
        released["lapsed_registration"]["owner"] == json!(BOB)
            && released["lapsed_registration"]["release_kind"] == json!("unregistered")
            && released["lapsed_registration"]["released_at"] == json!(1_800_000_048),
        "{released}"
    );
    fixture.cleanup().await
}

// Row "long name" of TYR-127: a name whose normalized form is longer than a btree row can hold
// (2704 bytes) still lists. The family step writes its rows, and a reader finds it through each
// cut index: by a q longer than the q key's 100 characters, by its parent, and on its last
// holder's former-owner page.
#[tokio::test]
async fn a_name_past_the_btree_row_limit_lists_and_is_found_by_q_parent_and_former_owner()
-> Result<()> {
    let fixture = Fixture::new("families_former_long_name", 12).await?;
    let long = hex_name("x", 44);
    ensure!(long.len() > 2704, "{} bytes", long.len());
    let (name, resource) = registered(&fixture, &long, 1).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    // The stored name keeps its width: an index keyed by the whole of it could not hold it.
    let stored: i64 = sqlx::query_scalar(
        "SELECT pg_column_size(normalized_name)::bigint FROM project_address_listing
         WHERE chain_id = $1 AND address = $2 AND logical_name_id = $3",
    )
    .bind(CHAIN)
    .bind(BOB)
    .bind(&name)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(stored > 2704, "the stored name takes {stored} bytes");
    let q: String = long.chars().take(150).collect();
    let by_q = q_read(CHAIN, BOB, &q);
    ensure!(
        fixture.listing_read(&by_q).await? == [name.clone()],
        "a q past the cut finds the name"
    );
    fixture
        .ensure_index_serves(
            &by_q,
            "project_address_listing_q_idx",
            "normalized_name, 100)",
        )
        .await?;
    let parent = long.split_once('.').expect("a parent").1;
    let by_parent = parent_read(CHAIN, BOB, parent);
    ensure!(
        fixture.listing_read(&by_parent).await? == [name.clone()],
        "the parent finds the name"
    );
    fixture
        .ensure_index_serves(
            &by_parent,
            "project_address_listing_parent_idx",
            "md5(parent_normalized_name)",
        )
        .await?;

    expired(&fixture, &resource, 1, 6).await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let former = former_read(CHAIN, BOB);
    ensure!(
        fixture.listing_read(&former).await? == [name],
        "the last holder's former-owner page finds the name"
    );
    fixture
        .ensure_index_serves(
            &former,
            "project_address_listing_former_owner_idx",
            "address",
        )
        .await?;
    fixture.assert_rebuild_equal(6).await?;
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}

// Rows "500 boundary" and "former tie" of TYR-127: the former-owner key is the normalized name
// cut as display_key is cut, to its first 500 characters when it is longer than 2000 bytes. A
// name of exactly 500 characters keys whole, and so do the first 500 characters of longer names
// that start with it, one of them 2001 bytes: the four tie on expiry, namespace and key, and
// order by namehash, which is not their name order. A name of exactly 2000 bytes is not cut,
// and follows them.
#[tokio::test]
async fn former_owner_names_sharing_their_first_500_characters_order_by_namehash() -> Result<()> {
    let fixture = Fixture::new("families_former_tie", 12).await?;
    // A label of 8 characters and eight of 60: with `.eth`, 500 characters.
    let head = long_name("p0000000", 8);
    let head = head.strip_suffix(".eth").expect("an eth name");
    let exact = format!("{head}.eth");
    ensure!(exact.chars().count() == 500, "{}", exact.chars().count());
    let longer = |tag: &str, labels: usize| format!("{head}.eth{tag}.{}", long_name("q", labels));
    // Two longer names whose namehash order is not their name order.
    let candidates: Vec<String> = (1..=9).map(|i| longer(&i.to_string(), 30)).collect();
    let (first, second) = candidates
        .iter()
        .flat_map(|a| candidates.iter().map(move |b| (a, b)))
        .find(|(a, b)| a < b && namehash(a) > namehash(b))
        .expect("two names whose namehash order is not their name order");
    let whole = padded(&format!("{head}.eth0"), 2000);
    let cut = padded(&format!("{head}.eth9"), 2001);
    ensure!(whole.len() == 2000 && cut.len() == 2001);
    let tied = [exact, first.clone(), second.clone(), cut];
    let mut ids = Vec::new();
    for (n, name) in tied.iter().chain([&whole]).enumerate() {
        let n = u32::try_from(n + 1)?;
        let (id, resource) = registered_at_log(&fixture, name, n, n.into()).await?;
        expired(&fixture, &resource, n, 6).await?;
        ids.push((name.clone(), id));
    }
    fixture.apply(6, FamilyMode::Normal).await?;

    let mut expected: Vec<&(String, String)> = ids[..4].iter().collect();
    expected.sort_by_key(|(name, _)| namehash(name));
    let mut expected: Vec<String> = expected.into_iter().map(|(_, id)| id.clone()).collect();
    expected.push(ids[4].1.clone());
    let former = former_read(CHAIN, BOB);
    let page = fixture.listing_read(&former).await?;
    ensure!(
        page == expected,
        "the former-owner page: {page:?}, expected {expected:?}"
    );
    let keys: Vec<i64> = sqlx::query_scalar(&format!(
        "SELECT char_length({FORMER_NAME_KEY})::bigint FROM project_address_listing
         WHERE chain_id = $1 AND address = $2 AND former_owner ORDER BY 1"
    ))
    .bind(CHAIN)
    .bind(BOB)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        keys == [500, 500, 500, 500, 2000],
        "the former-owner keys: {keys:?}"
    );
    fixture
        .ensure_index_serves(
            &former,
            "project_address_listing_former_owner_idx",
            "address",
        )
        .await?;
    ensure!(
        fixture.address_listing_checks().0 > 0,
        "the route oracle compared a publication"
    );
    fixture.cleanup().await
}
