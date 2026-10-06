use super::*;

const ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";

// Ordinary ETH root grants and real parent metadata cause ETH to be visited as a selected
// registry and as Wrapper's parent in one address batch. Only the latter lookup has an
// instance. Logs follow authorized registration, ordinary grants, approvals and setParent;
// they are manually encoded emissions, not an EVM execution claim.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L117-L121 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L174-L181 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L448-L514 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L132-L140 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L250-L287 @ ens_v2_sepolia_20261001@07e55a05)
fn mixed_logs(nonempty: bool) -> Logs {
    let mut logs = manual_logs(U256::ZERO, (TIME + 100) as u64);
    logs.roles(0, ROOT, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .register(0, ROOT, "eth", ADMIN, (TIME + 100) as u64, U256::ZERO)
        .roles(0, ETH, U256::ZERO, ADMIN, U256::ZERO, parent_admin())
        .parent(0, ETH, ROOT, "eth", ADMIN)
        .register(0, ETH, LABEL, ALICE, (TIME + 100) as u64, U256::ZERO)
        .roles(1, ETH, U256::ZERO, ALICE, U256::ZERO, bit(0))
        .roles(1, ETH, U256::ZERO, OPERATOR, U256::ZERO, bit(16))
        .approve(4, ETH, ALICE, OPERATOR, true)
        .parent(5, WRAPPER, ETH, LABEL, ADMIN);
    if nonempty {
        // At offset 2 ALICE still has the initialized admin bits through PARENT. At 3
        // manual_logs strips them only after granting the dormant ordinary roles.
        logs.roles(
            2,
            WRAPPER,
            U256::ZERO,
            ETH,
            U256::ZERO,
            bit(0) | bit(8) | bit(16),
        );
    }
    // Ordinary PARENT token grants provide visible cursor rows between the two root ids.
    // The conservative candidate bound below proves page_size=200 composes both roots
    // together; page_size=1 reads at most two candidates at once.
    for index in 0..24 {
        logs.register(
            1,
            PARENT,
            &format!("padding{index}"),
            ALICE,
            (TIME + 100) as u64,
            bit(24),
        );
    }
    logs
}

async fn root_id(database: &TestDatabase, registry: &str) -> Result<Uuid> {
    sqlx::query_scalar(
        "SELECT resource_id FROM project_grant WHERE chain_id = $1 AND scope = 'root'
         AND scope_detail ->> 'registry_address' = $2 LIMIT 1",
    )
    .bind(CHAIN)
    .bind(registry)
    .fetch_one(&database.pool)
    .await
    .map_err(Into::into)
}

// This bounds candidates before filtering/dormancy; emitted row count cannot establish that.
// Address reads have no token-bound root-holder arm. This fixture has no wrapper/registry
// approvals, so those other arms are empty. Every direct row is counted, every ENSv2 approved
// entry is counted even without a live owner grant, and each parent may add an owner key plus
// one operator key per approval. Overcounting these arms is deliberate.
async fn assert_shared_candidate_batch(database: &TestDatabase, subject: &str) -> Result<()> {
    let (direct, entries, parents, approvals, other): (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
          (SELECT count(*) FROM project_grant WHERE chain_id = $1 AND subject = $2),
          (SELECT count(*) FROM project_account_approval approval
           JOIN project_ens_v2_entry_owner entry ON entry.chain_id = approval.chain_id
            AND entry.registry = approval.authority_contract AND entry.owner = approval.owner
           WHERE approval.chain_id = $1 AND approval.subject = $2
            AND approval.authority_kind = 'ens_v2_registry' AND approval.approved
            AND approval.subject <> approval.owner),
          (SELECT count(*) FROM project_ens_v2_registry_parent WHERE chain_id = $1),
          (SELECT count(*) FROM project_account_approval WHERE chain_id = $1 AND subject = $2
            AND authority_kind = 'ens_v2_registry' AND approved AND subject <> owner),
          (SELECT count(*) FROM project_account_approval WHERE chain_id = $1 AND subject = $2
            AND authority_kind <> 'ens_v2_registry')",
    )
    .bind(CHAIN)
    .bind(subject)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(other, 0, "no other approval candidate arms in this fixture");
    assert!(
        direct >= 2,
        "ETH and dormant Wrapper roots are direct candidates"
    );
    let bound = direct + entries + parents * (1 + approvals);
    assert!(
        bound <= 64,
        "all candidate keys must fit the first batch: {bound}"
    );
    Ok(())
}

fn rows_for_root(rows: &[Value], root: Uuid) -> Vec<Value> {
    rows.iter()
        .filter(|row| row["registration_id"] == root.to_string())
        .cloned()
        .collect()
}

#[tokio::test]
async fn mixed_eth_and_wrapper_address_batches_match_registry_reads_and_cursors() -> Result<()> {
    for nonempty in [true, false] {
        let database = setup(&mixed_logs(nonempty)).await?;
        interpret(&database, false, false).await?;
        publish(&database, 5).await?;
        let eth_root = root_id(&database, ETH).await?;
        let wrapper_root = root_id(&database, WRAPPER).await?;
        let parent_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_ens_v2_registry_parent parent
             JOIN project_ens_v2_entry_owner entry ON entry.chain_id = parent.chain_id
              AND entry.registry = parent.parent AND entry.entry_key = parent.parent_entry_key
             WHERE parent.chain_id = $1 AND parent.registry IN ($2, $3)",
        )
        .bind(CHAIN)
        .bind(ETH)
        .bind(WRAPPER)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            parent_rows, 2,
            "both ETH and Wrapper must enter the recognition prepass"
        );
        let bounded = registry_page(&database, WRAPPER).await?;
        for (subject, relation) in [(ALICE, "holder"), (OPERATOR, "operator")] {
            if nonempty {
                assert_derived_powers(
                    &bounded,
                    subject,
                    relation,
                    ETH,
                    ALICE,
                    json!(["registrar", "set_parent", "renew"]),
                );
            } else {
                assert!(for_subject(&bounded, subject).is_empty(), "{bounded:#}");
            }
            assert_shared_candidate_batch(&database, subject).await?;
            let uri = format!("/v1/permissions?address={subject}&namespace=ens");
            let shared = payload(&database, &format!("{uri}&page_size=200")).await?;
            let rows = shared["data"].as_array().unwrap();
            assert_eq!(shared["page"]["has_more"], false);
            assert!(
                rows.iter()
                    .any(|row| row["registration_id"] == eth_root.to_string())
            );
            let expected: Vec<Value> = for_subject(&bounded, subject)
                .into_iter()
                .cloned()
                .collect();
            assert_eq!(rows_for_root(rows, wrapper_root), expected, "{shared:#}");
            // These visible PARENT token rows have no ETH parent. At least two lie between
            // E and W, so the page_size=1 candidate batches cannot contain both root ids.
            let low = eth_root.min(wrapper_root);
            let high = eth_root.max(wrapper_root);
            let between = rows
                .iter()
                .filter(|row| {
                    row["registration_id"]
                        .as_str()
                        .and_then(|id| id.parse::<Uuid>().ok())
                        .is_some_and(|id| low < id && id < high)
                })
                .count();
            assert!(
                between >= 2,
                "fixture must separate E and W in two-key batches: {between}"
            );
            let paged = pages(&database, &format!("{uri}&page_size=1")).await?;
            assert!(paged.len() > 1, "must exercise continuation cursors");
            let walked: Vec<Value> = paged
                .iter()
                .flat_map(|page| page["data"].as_array().unwrap().clone())
                .collect();
            assert_eq!(&walked, rows);
            assert_eq!(rows_for_root(&walked, wrapper_root), expected);
        }
        let dormant: Vec<Value> = sqlx::query_scalar(
            "SELECT effective_powers FROM project_grant WHERE chain_id = $1
             AND resource_id = $2 AND subject IN ($3, $4) ORDER BY subject",
        )
        .bind(CHAIN)
        .bind(wrapper_root)
        .bind(ALICE)
        .bind(OPERATOR)
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(
            dormant,
            vec![json!(["set_subregistry"]), json!(["set_resolver"])]
        );
        database.cleanup().await?;
    }
    Ok(())
}
