//! T16 of the TYR-280 ordering table. A migrated wrapper registry mounted under a second label
//! serves its migrated labels under that path. The walk reads the wrapper registry's entries as
//! it reads any registry's, so it needs no wrapper branch for those labels. A label the wrapper
//! registry leaves to its ENSv1 fallback has no entry: it answers `404 not_found` under the
//! alias (TYR-300) and serves on its canonical path as before.
use super::super::alias_paths::{assert_alias, not_found};
use super::*;

const ALIAS_LABEL: &str = "alias105";
const LEAF: &str = "leaf.envoy1084.eth";
const ALIAS: &str = "leaf.alias105.eth";

#[tokio::test]
async fn an_alias_serves_a_migrated_registrys_token() -> Result<()> {
    let (database, resolver) = wrapped_setup().await?;
    let owner = HOLDER.parse()?;
    let w = REGISTRY.parse()?;
    // The migrated parent's holder registers a native label in W, as in `physical.rs`.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L170-L185 @ ens_v2_sepolia_20261001@07e55a05)
    let mut logs = transaction(
        123,
        0,
        register(
            w,
            "leaf",
            resolver,
            Address::ZERO,
            RESERVATION_EXPIRY + 10,
            owner,
        )?,
    );
    // The BatchRegistrar reserves a second ETH label pointing at W. W's parent stays
    // `envoy1084.eth`, so `leaf.envoy1084.eth` stays canonical.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2_sepolia_20261001@07e55a05)
    let eth: Address = ETH.parse()?;
    let batch: Address = BATCH_REGISTRAR.parse()?;
    logs.extend(transaction(
        123,
        1,
        vec![
            (
                eth,
                LabelReserved {
                    tokenId: label_token(ALIAS_LABEL),
                    labelHash: keccak256(ALIAS_LABEL),
                    label: ALIAS_LABEL.into(),
                    expiry: RESERVATION_EXPIRY + 60,
                    sender: batch,
                }
                .encode_log_data(),
            ),
            (
                eth,
                SubregistryUpdated {
                    tokenId: label_token(ALIAS_LABEL),
                    subregistry: w,
                    sender: batch,
                }
                .encode_log_data(),
            ),
        ],
    ));
    seed_and_run_with_targets(
        &database,
        &logs,
        123,
        123,
        &[(123, 1, DEPLOYER)],
        &[(123, 1, BATCH_REGISTRAR)],
        None,
    )
    .await?;
    let served = assert_alias(&database, ALIAS, LEAF).await?;
    assert_eq!(served["data"]["status"], "active", "{served:#}");
    // `child` is a wrapped ENSv1 subname W serves through its fallback, not a W entry.
    path_get(&database, &format!("/v1/names/{CHILD}")).await?;
    not_found(&database, "/v1/names/child.alias105.eth").await?;
    Ok(())
}
