//! The unwrapped ENSv1→ENSv2 migration of a `.eth` lease whose first observed registrar event
//! was a legacy-controller `NameRenewed`, so its holder is recorded as the zero address. It must
//! reconcile exactly like the same migration of a lease with a known holder when every other
//! clause of the proof holds, and a transfer that is not the migration must still be refused.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use super::super::zero_owner_migration_fixture as real;
use super::*;

const PROOF_WARNING: &str =
    "unwrapped ENSv1→ENSv2 migration boundary derives without its cleanup proof";
const PREDECESSOR_CLAUSE: &str = "incoming transfer from the registrar owner";
const EXPIRY_CLAUSE: &str = "lease live at the migration";
const CONTROLLER_CLAUSE: &str =
    "registrar transfers into the controller and from it to the Graveyard, in order";
const REGISTRY_CLAUSE: &str = "ENSv1 registry reclaim, transfer to the Graveyard and record clears";
/// The controller `unwrapped_input` admits with the `registrar_controller` role.
const CATALOG_CONTROLLER: &str = "0x0000000000000000000000000000000000000098";
/// The ENSv1 registry `unwrapped_input` admits.
const CATALOG_REGISTRY: &str = "0x0000000000000000000000000000000000000099";
const CATALOG_UNWRAPPED_HOLDER: [u8; 20] = [0x51; 20];
const CATALOG_WRAPPED_HOLDER: [u8; 20] = [0x11; 20];
const EARLIER_HOLDER: [u8; 20] = [0x50; 20];
const STRANGER: [u8; 20] = [0x77; 20];
/// The resolver the constructed registry history sets for debut.eth.
const PRIOR_RESOLVER: [u8; 20] = [0x52; 20];
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;

/// Every warning logged on the test's thread, as its recorded fields.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

#[derive(Default)]
struct Fields(BTreeMap<String, String>);

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for Warnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().expect("warning lock").push(fields.0);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

impl Warnings {
    /// The clauses of every missing-proof warning, for every name.
    fn all(&self) -> Vec<String> {
        self.0
            .lock()
            .expect("warning lock")
            .iter()
            .filter(|fields| fields.get("message").map(String::as_str) == Some(PROOF_WARNING))
            .map(|fields| fields.get("clause").cloned().unwrap_or_default())
            .collect()
    }

    /// The clauses of the missing-proof warnings for one name.
    fn clauses(&self, logical_name_id: &str) -> Vec<String> {
        let name = format!("{:?}", Some(logical_name_id));
        self.0
            .lock()
            .expect("warning lock")
            .iter()
            .filter(|fields| {
                fields.get("message").map(String::as_str) == Some(PROOF_WARNING)
                    && fields.get("logical_name_id") == Some(&name)
            })
            .map(|fields| fields.get("clause").cloned().unwrap_or_default())
            .collect()
    }
}

fn interpret_with_warnings(input: BatchInput) -> anyhow::Result<(BatchOutput, Warnings)> {
    let warnings = Warnings::default();
    let output =
        tracing::subscriber::with_default(warnings.clone(), || interpret_test_batch(input))?;
    Ok((output, warnings))
}

fn eth_name(label: &str) -> String {
    let eth = keccak256([[0_u8; 32], keccak256(b"eth").0].concat());
    format!(
        "ens:{:#x}",
        keccak256([eth.0, keccak256(label.as_bytes()).0].concat())
    )
}

/// The resource of the name's ENSv1 registrar lease.
fn lease(output: &BatchOutput, logical_name_id: &str) -> Uuid {
    output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "RegistrationGranted"
                && event.logical_name_id.as_deref() == Some(logical_name_id)
        })
        .and_then(|event| event.resource_id)
        .unwrap_or_else(|| panic!("{logical_name_id}: no registrar lease"))
}

/// The name's lease was granted with no known holder: its first observed registrar event was a
/// controller renewal.
fn assert_no_known_holder(output: &BatchOutput, logical_name_id: &str) {
    let grant = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationGranted"
                && event.source_family == "ens_v1_registrar_l1"
                && event.logical_name_id.as_deref() == Some(logical_name_id)
        })
        .unwrap_or_else(|| panic!("{logical_name_id}: no synthetic grant"));
    assert_eq!(
        grant.after_state["authority_owner"], ZERO_ADDRESS,
        "{logical_name_id}: the grant records no holder"
    );
}

/// The migration in `block` reconciled for this name: one activated boundary, no ENSv1 binding
/// opened or closed inside it, both registrar transfers kept on the lease, the registry cleanup
/// observations moved onto the lease, and no missing-proof warning.
fn assert_reconciled(output: &BatchOutput, warnings: &Warnings, block: i64, logical: &str) {
    assert_eq!(
        output
            .migration_authority_transitions
            .iter()
            .filter(|transition| transition.logical_name_id == logical)
            .count(),
        1,
        "{logical}: one activated boundary"
    );
    let opened = output
        .surface_bindings
        .iter()
        .filter(|binding| {
            binding.logical_name_id == logical
                && binding.authority_arm == "ens_v1"
                && binding.block_number == block
        })
        .collect::<Vec<_>>();
    assert!(
        opened.is_empty(),
        "{logical}: ENSv1 bindings open inside the migration: {opened:?}"
    );
    let closed = output
        .binding_closures
        .iter()
        .filter(|closure| {
            closure.logical_name_id == logical
                && closure.authority_arm == "ens_v1"
                && closure.block_number == block
        })
        .collect::<Vec<_>>();
    assert!(
        closed.is_empty(),
        "{logical}: ENSv1 bindings closed inside the migration: {closed:?}"
    );
    assert_eq!(
        warnings.clauses(logical),
        Vec::<String>::new(),
        "{logical}: the boundary derived without its proof"
    );
    let lease = lease(output, logical);
    let migrated = output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(block))
        .collect::<Vec<_>>();
    assert_eq!(
        migrated
            .iter()
            .filter(|event| {
                event.source_family == "ens_v1_registrar_l1"
                    && event.event_kind == "TokenControlTransferred"
                    && event.resource_id == Some(lease)
            })
            .count(),
        2,
        "{logical}: both registrar transfers stay on the lease"
    );
    assert!(
        migrated
            .iter()
            .filter(|event| {
                event.source_family == "ens_v1_registry_l1"
                    && event.event_kind != "PermissionChanged"
                    && event.logical_name_id.as_deref() == Some(logical)
            })
            .all(|event| event.resource_id == Some(lease)),
        "{logical}: registry cleanup observations sit on the lease"
    );
    assert!(
        !migrated.iter().any(|event| {
            event.source_family.starts_with("ens_v1_")
                && event.logical_name_id.as_deref() == Some(logical)
                && matches!(
                    event.event_kind.as_str(),
                    "SurfaceBound" | "SurfaceUnbound" | "AuthorityEpochChanged"
                )
        }),
        "{logical}: no ENSv1 authority change inside the migration"
    );
}

/// The ENSv1 bindings, the effect a missing proof leaves to ordinary interpretation.
fn v1_bindings(output: &BatchOutput) -> Vec<&SurfaceBinding> {
    output
        .surface_bindings
        .iter()
        .filter(|binding| binding.authority_arm == "ens_v1")
        .collect()
}

/// `input` without the ENSv1→ENSv2 migration family, so with nothing to reconcile.
fn without_migration(input: &BatchInput) -> BatchInput {
    let mut ordinary = input.clone();
    let migration = ordinary
        .manifests
        .iter()
        .filter(|manifest| manifest.source_family == "ens_v2_migration_l1")
        .map(|manifest| manifest.manifest_id)
        .collect::<Vec<_>>();
    ordinary
        .manifests
        .retain(|manifest| !migration.contains(&manifest.manifest_id));
    ordinary.admissions.retain(|admission| {
        admission
            .source_manifest_id
            .is_none_or(|id| !migration.contains(&id))
    });
    ordinary
}

/// The proof refused `input` with `clause` and left its ENSv1 bindings to ordinary
/// interpretation.
fn assert_refused(input: BatchInput, logical: &str, clause: &str) -> anyhow::Result<()> {
    let ordinary = interpret_test_batch(without_migration(&input))?;
    let (output, warnings) = interpret_with_warnings(input)?;
    let clauses = warnings.clauses(logical);
    assert!(
        !clauses.is_empty() && clauses.iter().all(|refused| refused == clause),
        "{logical}: expected the missing-proof warning to name {clause:?}, got {clauses:?}"
    );
    assert_eq!(
        v1_bindings(&output),
        v1_bindings(&ordinary),
        "{logical}: a refused proof must leave ordinary interpretation"
    );
    Ok(())
}

fn replace_log(raw: &mut RawLogInput, log: alloy_primitives::LogData) {
    raw.topics = log
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect();
    raw.data = log.data.to_vec();
}

struct Name {
    label: String,
    logical: String,
    first_log: i64,
    log_count: i64,
}

fn names(receipt: &str) -> anyhow::Result<Vec<Name>> {
    let fixture = real::fixture()?;
    Ok(fixture["names"]
        .as_array()
        .expect("fixture names")
        .iter()
        .filter(|name| name["receipt"] == receipt)
        .map(|name| {
            let label = name["label"].as_str().expect("label").to_owned();
            Name {
                logical: eth_name(&label),
                label,
                first_log: name["first_log"].as_i64().expect("first log"),
                log_count: name["log_count"].as_i64().expect("log count"),
            }
        })
        .collect())
}

fn migration_block(input: &BatchInput) -> i64 {
    input
        .raw_logs
        .iter()
        .map(|raw| raw.block_number)
        .max()
        .expect("migration logs")
}

fn owns(name: &Name, raw: &RawLogInput, block: i64) -> bool {
    raw.block_number == block
        && (name.first_log..name.first_log + name.log_count).contains(&raw.log_index)
}

/// The debut.eth receipt, with its log at `log_index` taken out for `edit`.
fn debut_with(edit: impl FnOnce(&mut RawLogInput), log_index: i64) -> anyhow::Result<BatchInput> {
    let mut input = real::input(&["debut"])?;
    let block = migration_block(&input);
    edit(
        input
            .raw_logs
            .iter_mut()
            .find(|raw| raw.block_number == block && raw.log_index == log_index)
            .expect("captured log"),
    );
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    Ok(input)
}

/// debut.eth's constructed history log.
fn debut_history(input: &mut BatchInput) -> &mut RawLogInput {
    let block = migration_block(input) - 1;
    input
        .raw_logs
        .iter_mut()
        .find(|raw| raw.block_number == block)
        .expect("constructed renewal")
}

fn debut_holder() -> anyhow::Result<Address> {
    Ok(real::fixture()?["names"][0]["holder"]
        .as_str()
        .expect("holder")
        .parse()?)
}

/// debut.eth's history as a legacy-controller registration to `owner` instead of the renewal,
/// so the lease has a known holder.
fn debut_registered_to(owner: Address) -> anyhow::Result<BatchInput> {
    let fixture = real::fixture()?;
    let mut input = real::input(&["debut"])?;
    let history = debut_history(&mut input);
    replace_log(
        history,
        super::super::cost_controller::NameRegistered {
            name: "debut".to_owned(),
            label: keccak256(b"debut"),
            owner,
            cost: U256::ZERO,
            expires: U256::from(fixture["names"][0]["v2_expiry"].as_u64().expect("expiry")),
        }
        .encode_log_data(),
    );
    Ok(input)
}

/// Mainnet block 26,289,806: debut.eth's lease never moved on the BaseRegistrar before the
/// migration, and no holder was ever recorded for it. Its migration block must read exactly as
/// the same migration of a lease whose registration names the holder. The rows compared are each
/// event's family and kind and whether it sits on the lease. States are left out because the
/// two leases' grants record different holders.
#[test]
fn a_lease_with_no_known_holder_migrates_like_one_with_a_holder() -> anyhow::Result<()> {
    let input = real::input(&["debut"])?;
    let block = migration_block(&input);
    let debut = eth_name("debut");
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_no_known_holder(&output, &debut);
    let known = interpret_test_batch(debut_registered_to(debut_holder()?)?)?;
    assert_eq!(
        name_rows(&output, block, &debut),
        name_rows(&known, block, &debut),
        "the migration of a lease with no known holder reads unlike one with a holder"
    );
    assert_reconciled(&output, &warnings, block, &debut);
    assert!(warnings.all().is_empty(), "{:?}", warnings.all());
    Ok(())
}

/// The history a legacy `renew` leaves when the registry records came before the indexed range:
/// the registry's `NewOwner` and `NewResolver` for debut.eth in an earlier block, then the
/// BaseRegistrar `NameRenewed` and the controller `NameRenewed` of one renewal. The captured
/// `NewResolver(node, 0)` of the migration only fires on a value change, so debut.eth had a
/// resolver set. The registry logs and the BaseRegistrar renewal are constructed.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L167 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L179-L182 @ ens_v1@91c966f)
fn debut_with_registry_records() -> anyhow::Result<BatchInput> {
    let mut input = real::input(&["debut"])?;
    let block = migration_block(&input);
    let captured = |log_index: i64| {
        input
            .raw_logs
            .iter()
            .find(|raw| raw.block_number == block && raw.log_index == log_index)
            .expect("captured log")
            .clone()
    };
    let (base, registry) = (captured(470), captured(471));
    let renewal = debut_history(&mut input);
    renewal.log_index = 1;
    let renewal = renewal.clone();
    let expires = U256::from(
        real::fixture()?["names"][0]["v2_expiry"]
            .as_u64()
            .expect("expiry"),
    );
    let mut base_renewal = RawLogInput {
        emitting_address: base.emitting_address,
        log_index: 0,
        ..renewal.clone()
    };
    replace_log(
        &mut base_renewal,
        with_topic0(
            BaseNameRenewed {
                id: U256::from_be_slice(keccak256(b"debut").as_slice()),
                expires,
            }
            .encode_log_data(),
            keccak256(b"NameRenewed(uint256,uint256)"),
        ),
    );
    let records = renewal.block_number - 1;
    let history_block = input
        .blocks
        .iter()
        .find(|history| history.block_number == renewal.block_number)
        .expect("history block")
        .clone();
    input.blocks.push(RawBlockInput {
        block_hash: format!("0x{records:064x}"),
        block_number: records,
        block_timestamp: history_block.block_timestamp - time::Duration::seconds(12),
        ..history_block
    });
    let at_records = |log_index: i64, log: alloy_primitives::LogData| {
        let mut raw = RawLogInput {
            block_hash: format!("0x{records:064x}"),
            block_number: records,
            block_timestamp: renewal.block_timestamp - time::Duration::seconds(12),
            transaction_hash: format!("0x{:064x}", records << 8),
            transaction_index: 0,
            log_index,
            emitting_address: registry.emitting_address.clone(),
            ..renewal.clone()
        };
        replace_log(&mut raw, log);
        raw
    };
    let node: B256 = eth_name("debut")
        .strip_prefix("ens:")
        .expect("namehash")
        .parse()?;
    let registry_logs = [
        at_records(
            0,
            super::super::v1_registry::NewOwner {
                node: super::super::common::namehash(&["eth".to_owned()]).parse()?,
                label: keccak256(b"debut"),
                owner: debut_holder()?,
            }
            .encode_log_data(),
        ),
        at_records(
            1,
            super::super::v1_registry::NewResolver {
                node,
                resolver: Address::from(PRIOR_RESOLVER),
            }
            .encode_log_data(),
        ),
    ];
    input.raw_logs.push(base_renewal);
    input.raw_logs.extend(registry_logs);
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    input.blocks.sort_by_key(|block| block.block_number);
    Ok(input)
}

/// debut.eth with registry records and a resolver from before its renewal, so the predecessor
/// carries ENSv1 resolver state, and still no known holder.
#[test]
fn a_lease_with_no_known_holder_and_a_resolver_reconciles() -> anyhow::Result<()> {
    let input = debut_with_registry_records()?;
    let block = migration_block(&input);
    let debut = eth_name("debut");
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_no_known_holder(&output, &debut);
    assert!(
        output.normalized_events.iter().any(|event| {
            event.source_family == "ens_v1_registry_l1"
                && event.event_kind == "ResolverChanged"
                && event.block_number == Some(block - 2)
        }),
        "the constructed history sets a resolver"
    );
    assert_reconciled(&output, &warnings, block, &debut);
    assert!(warnings.all().is_empty(), "{:?}", warnings.all());
    Ok(())
}

/// The same migration of a lease whose legacy-controller registration names the holder, the
/// shape that already reconciled.
#[test]
fn a_lease_with_a_known_holder_migrates_as_before() -> anyhow::Result<()> {
    let input = debut_registered_to(debut_holder()?)?;
    let block = migration_block(&input);
    let debut = eth_name("debut");
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_reconciled(&output, &warnings, block, &debut);
    Ok(())
}

/// The owner comparison still refuses a known holder that did not send the token.
#[test]
fn a_known_holder_that_did_not_send_the_token_is_still_refused() -> anyhow::Result<()> {
    assert_refused(
        debut_registered_to(Address::from(STRANGER))?,
        &eth_name("debut"),
        PREDECESSOR_CLAUSE,
    )
}

/// A transfer of the holderless lease to anyone but the controller is not the migration, even
/// inside a migration transaction.
#[test]
fn a_transfer_of_a_lease_with_no_known_holder_to_another_address_is_refused() -> anyhow::Result<()>
{
    let stranger = format!("{:#x}", B256::left_padding_from(&STRANGER));
    assert_refused(
        debut_with(|raw| raw.topics[2] = stranger, 470)?,
        &eth_name("debut"),
        CONTROLLER_CLAUSE,
    )
}

#[test]
fn a_lease_with_no_known_holder_that_expired_is_refused() -> anyhow::Result<()> {
    let mut input = real::input(&["debut"])?;
    let migrated_at = input
        .blocks
        .iter()
        .map(|block| block.block_timestamp.unix_timestamp())
        .max()
        .expect("migration block");
    let history = debut_history(&mut input);
    replace_log(
        history,
        super::super::wrapped_controller::NameRenewed {
            name: "debut".to_owned(),
            label: keccak256(b"debut"),
            cost: U256::ZERO,
            expires: U256::from(u64::try_from(migrated_at - 1)?),
        }
        .encode_log_data(),
    );
    assert_refused(input, &eth_name("debut"), EXPIRY_CLAUSE)
}

#[test]
fn a_registry_transfer_not_to_the_graveyard_is_refused() -> anyhow::Result<()> {
    assert_refused(
        debut_with(|raw| raw.data[12..].copy_from_slice(&STRANGER), 472)?,
        &eth_name("debut"),
        REGISTRY_CLAUSE,
    )
}

#[test]
fn a_registry_reclaim_not_to_the_controller_is_refused() -> anyhow::Result<()> {
    assert_refused(
        debut_with(|raw| raw.data[12..].copy_from_slice(&STRANGER), 471)?,
        &eth_name("debut"),
        REGISTRY_CLAUSE,
    )
}

/// The registry transfer to the Graveyard ahead of the reclaim.
#[test]
fn registry_cleanup_out_of_order_is_refused() -> anyhow::Result<()> {
    let mut input = real::input(&["debut"])?;
    let block = migration_block(&input);
    for raw in input
        .raw_logs
        .iter_mut()
        .filter(|raw| raw.block_number == block)
    {
        raw.log_index = match raw.log_index {
            471 => 472,
            472 => 471,
            index => index,
        };
    }
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    assert_refused(input, &eth_name("debut"), REGISTRY_CLAUSE)
}

/// A resolver set before the migration that the transaction does not clear.
#[test]
fn a_resolver_left_set_is_refused() -> anyhow::Result<()> {
    let mut input = real::input(&["debut"])?;
    let block = migration_block(&input);
    let registry = input
        .raw_logs
        .iter()
        .find(|raw| raw.block_number == block && raw.log_index == 471)
        .expect("reclaim")
        .emitting_address
        .clone();
    input
        .raw_logs
        .retain(|raw| raw.block_number != block || raw.log_index != 473);
    let mut resolver = debut_history(&mut input).clone();
    resolver.transaction_index = 1;
    resolver.transaction_hash = format!("{}01", resolver.transaction_hash);
    resolver.emitting_address = registry;
    replace_log(
        &mut resolver,
        super::super::v1_registry::NewResolver {
            node: eth_name("debut")
                .strip_prefix("ens:")
                .expect("namehash")
                .parse()?,
            resolver: Address::from(STRANGER),
        }
        .encode_log_data(),
    );
    input.raw_logs.push(resolver);
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    assert_refused(input, &eth_name("debut"), REGISTRY_CLAUSE)
}

/// Mainnet block 26,294,386: one transaction migrates seven holderless leases of one holder,
/// each in its own run of nine logs.
#[test]
fn seven_holderless_leases_in_one_transaction_each_reconcile() -> anyhow::Result<()> {
    let input = real::input(&["batch"])?;
    let block = migration_block(&input);
    let (output, warnings) = interpret_with_warnings(input)?;
    let names = names("batch")?;
    assert_eq!(names.len(), 7);
    for name in &names {
        assert_no_known_holder(&output, &name.logical);
        assert_reconciled(&output, &warnings, block, &name.logical);
    }
    assert!(warnings.all().is_empty(), "{:?}", warnings.all());
    Ok(())
}

/// Each name's rows in its migration block, by family, kind and whether they sit on its lease.
fn name_rows(output: &BatchOutput, block: i64, logical: &str) -> Vec<(String, String, bool)> {
    let lease = lease(output, logical);
    let resources = output
        .normalized_events
        .iter()
        .filter(|event| event.logical_name_id.as_deref() == Some(logical))
        .filter_map(|event| event.resource_id)
        .collect::<BTreeSet<_>>();
    let mut rows = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(block)
                && (event.logical_name_id.as_deref() == Some(logical)
                    || event
                        .resource_id
                        .is_some_and(|resource| resources.contains(&resource)))
        })
        .map(|event| {
            (
                event.source_family.clone(),
                event.event_kind.clone(),
                event.resource_id == Some(lease),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// The seven-name transaction cut to `kept` names, each name's logs moved by `place`.
fn batch_of(
    kept: usize,
    place: impl Fn(usize, &Name, &mut RawLogInput),
) -> anyhow::Result<(BatchInput, Vec<Name>)> {
    let mut input = real::input(&["batch"])?;
    let block = migration_block(&input);
    let mut names = names("batch")?;
    names.truncate(kept);
    input
        .raw_logs
        .retain(|raw| raw.block_number != block || names.iter().any(|name| owns(name, raw, block)));
    for raw in &mut input.raw_logs {
        if let Some((index, name)) = names
            .iter()
            .enumerate()
            .find(|(_, name)| owns(name, raw, block))
        {
            place(index, name, raw);
        }
    }
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    Ok((input, names))
}

/// Three names' migrations interleaved log by log in one transaction, so each name's window from
/// its first registrar transfer to its cleanup holds the other names' rows. Every name reconciles
/// and keeps exactly the rows it has when it migrates alone. No chain has this shape, since each
/// migration's receiver completes before the next transfer starts. The constructed shape shows
/// each name's window is read per name.
#[test]
fn interleaved_migrations_in_one_transaction_reconcile_per_name() -> anyhow::Result<()> {
    const KEPT: usize = 3;
    let (input, names) = batch_of(KEPT, |index, name, raw| {
        let step = raw.log_index - name.first_log;
        raw.log_index = 313 + step * KEPT as i64 + index as i64;
    })?;
    let block = migration_block(&input);
    let incoming = |name: &Name| {
        input
            .raw_logs
            .iter()
            .filter(|raw| raw.block_number == block)
            .filter(|raw| {
                raw.topics.get(3) == Some(&format!("{:#x}", keccak256(name.label.as_bytes())))
            })
            .map(|raw| raw.log_index)
            .collect::<Vec<_>>()
    };
    let (first, second) = (incoming(&names[0]), incoming(&names[1]));
    assert!(
        first[0] < second[0] && second[0] < first[1],
        "the second name's registrar transfers fall inside the first name's window"
    );
    let (output, warnings) = interpret_with_warnings(input)?;
    for name in &names {
        assert_reconciled(&output, &warnings, block, &name.logical);
        let (mut alone, _) = batch_of(KEPT, |_, _, _| {})?;
        alone.raw_logs.retain(|raw| {
            raw.block_number != block
                || owns(name, raw, block)
                || !names.iter().any(|other| owns(other, raw, block))
        });
        let alone = interpret_test_batch(alone)?;
        assert_eq!(
            name_rows(&output, block, &name.logical),
            name_rows(&alone, block, &name.logical),
            "{}: interleaving changed the name's rows",
            name.logical
        );
    }
    Ok(())
}

/// The seven names in seven transactions of the same block.
#[test]
fn migrations_in_separate_transactions_of_one_block_each_reconcile() -> anyhow::Result<()> {
    let (input, names) = batch_of(7, |index, _, raw| {
        raw.transaction_index += index as i64;
        raw.transaction_hash = format!("0x{:064x}", index + 1);
    })?;
    let block = migration_block(&input);
    let (output, warnings) = interpret_with_warnings(input)?;
    for name in &names {
        assert_reconciled(&output, &warnings, block, &name.logical);
    }
    Ok(())
}

/// One holder migrates a holderless lease in one block and another in the next.
#[test]
fn the_same_holder_migrating_twice_reconciles_both() -> anyhow::Result<()> {
    let (mut input, names) = batch_of(2, |index, _, raw| {
        if index == 1 {
            raw.block_number += 1;
            raw.block_hash = format!("0x{:064x}", raw.block_number);
            raw.transaction_hash = format!("0x{:064x}", (raw.block_number << 8) | 1);
            raw.block_timestamp += time::Duration::seconds(12);
        }
    })?;
    let second = migration_block(&input);
    let first = input
        .blocks
        .iter()
        .find(|block| block.block_number == second - 1)
        .expect("captured block")
        .clone();
    input.blocks.push(RawBlockInput {
        block_hash: format!("0x{second:064x}"),
        block_number: second,
        block_timestamp: first.block_timestamp + time::Duration::seconds(12),
        ..first
    });
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_reconciled(&output, &warnings, second - 1, &names[0].logical);
    assert_reconciled(&output, &warnings, second, &names[1].logical);
    Ok(())
}

/// The catalog fixture's ENSv1 registrar manifest, admitting the legacy-controller renewal that
/// grants a lease with no known holder.
fn admit_controller_renewal(input: &mut BatchInput) -> anyhow::Result<()> {
    let manifest = input
        .manifests
        .iter_mut()
        .find(|manifest| manifest.manifest_id == V1_REGISTRAR_MANIFEST_ID)
        .expect("registrar manifest");
    let mut payload: Value = serde_json::from_str(&manifest.payload_json)?;
    payload["abi"]["events"]
        .as_array_mut()
        .expect("ABI events")
        .push(json!({
            "name": "NameRenewed",
            "fragment": "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
            "emitter_roles": ["registrar_controller"],
            "normalized_events": [
                "RegistrationGranted",
                "RegistrationRenewed",
                "ExpiryChanged",
                "SurfaceUnbound",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "ResolverChanged",
                "PreimageObserved"
            ],
        }));
    manifest.payload_json = payload.to_string();
    Ok(())
}

fn catalog_renewal(scenario: &Value, block: i64) -> RawLogInput {
    let label = scenario["label"].as_str().expect("label");
    raw_at_transaction(
        super::super::NameRenewed {
            name: label.to_owned(),
            label: keccak256(label.as_bytes()),
            expires: U256::from(scenario["stored_expiry"].as_u64().expect("expiry")),
        }
        .encode_log_data(),
        block,
        0,
        0,
        CATALOG_CONTROLLER,
    )
}

fn name_wrapped(
    scenario: &Value,
    owner: Address,
    block: i64,
    log: i64,
) -> anyhow::Result<RawLogInput> {
    let fixture = fixture()?;
    let label = scenario["label"].as_str().expect("label");
    let mut dns_name = vec![u8::try_from(label.len())?];
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.extend_from_slice(b"\x03eth\0");
    Ok(raw_at_transaction(
        super::super::NameWrapped {
            node: scenario["namehash"].as_str().expect("namehash").parse()?,
            name: dns_name.into(),
            owner,
            fuses: 0,
            expiry: scenario["stored_expiry"].as_u64().expect("expiry") + GRACE_PERIOD,
        }
        .encode_log_data(),
        block,
        0,
        log,
        fixture["addresses"]["name_wrapper"]
            .as_str()
            .expect("NameWrapper"),
    ))
}

/// `holder` sends the token into the NameWrapper and the NameWrapper reclaims the registry
/// record, as `wrapETH2LD` does.
fn wrap(scenario: &Value, holder: Address, block: i64) -> anyhow::Result<Vec<RawLogInput>> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let labelhash = keccak256(scenario["label"].as_str().expect("label").as_bytes());
    let wrapper = address(addresses, "name_wrapper")?;
    Ok(vec![
        registrar_transfer_at(
            holder,
            wrapper,
            labelhash,
            block,
            0,
            addresses["base_registrar"].as_str().expect("BaseRegistrar"),
        ),
        raw_at_transaction(
            super::super::v1_registry::NewOwner {
                node: super::super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner: wrapper,
            }
            .encode_log_data(),
            block,
            0,
            1,
            CATALOG_REGISTRY,
        ),
        name_wrapped(scenario, holder, block, 2)?,
    ])
}

/// U-01's unwrapped migration transaction, after a controller renewal four blocks earlier and
/// then `moves`.
fn catalog_unwrapped(moves: Vec<RawLogInput>) -> anyhow::Result<(BatchInput, i64, String)> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["U-01"];
    let block = scenario["migration_block"].as_i64().expect("block");
    let mut input = unwrapped_input(None)?;
    input.raw_logs.retain(|raw| raw.block_number == block);
    input.raw_logs.push(catalog_renewal(scenario, block - 4));
    input.raw_logs.extend(moves);
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    admit_controller_renewal(&mut input)?;
    let logical = format!("ens:{}", scenario["namehash"].as_str().expect("namehash"));
    Ok((input, block, logical))
}

/// W-01's unlocked-wrapped migration transaction, after a controller renewal three blocks
/// earlier and then `moves`.
fn catalog_wrapped(moves: Vec<RawLogInput>) -> anyhow::Result<(BatchInput, String)> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let scenario = &fixture["scenarios"]["W-01"];
    let block = scenario["migration_block"].as_i64().expect("block");
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let label = scenario["label"].as_str().expect("label");
    let mut input = unwrapped_input(None)?;
    input.raw_logs = vec![
        raw_at_transaction(
            super::super::NameUnwrapped {
                node: scenario["namehash"].as_str().expect("namehash").parse()?,
                owner: address(addresses, "graveyard")?,
            }
            .encode_log_data(),
            block,
            0,
            scenario["name_unwrapped_log_index"].as_i64().expect("log"),
            addresses["name_wrapper"].as_str().expect("NameWrapper"),
        ),
        registrar_transfer_at(
            address(addresses, "name_wrapper")?,
            address(addresses, "graveyard")?,
            keccak256(label.as_bytes()),
            block,
            scenario["graveyard_transfer_log_index"]
                .as_i64()
                .expect("log"),
            addresses["base_registrar"].as_str().expect("BaseRegistrar"),
        ),
        raw_at_transaction(
            super::super::v2_registry::LabelRegistered {
                tokenId: v2_token,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner: Address::from(CATALOG_WRAPPED_HOLDER),
                expiry: scenario["stored_expiry"].as_u64().expect("expiry"),
                sender: address(addresses, "unlocked_controller")?,
            }
            .encode_log_data(),
            block,
            0,
            scenario["v2_registration_log_index"].as_i64().expect("log"),
            addresses["eth_registry"].as_str().expect("ETHRegistry"),
        ),
        raw_at_transaction(
            super::super::v2_registry::TokenResource {
                tokenId: v2_token,
                resource: v2_token,
            }
            .encode_log_data(),
            block,
            0,
            scenario["v2_registration_log_index"].as_i64().expect("log") + 2,
            addresses["eth_registry"].as_str().expect("ETHRegistry"),
        ),
        catalog_renewal(scenario, block - 3),
    ];
    input.raw_logs.extend(moves);
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    admit_controller_renewal(&mut input)?;
    let logical = format!("ens:{}", scenario["namehash"].as_str().expect("namehash"));
    Ok((input, logical))
}

fn catalog_scenario(key: &str) -> anyhow::Result<Value> {
    Ok(fixture()?["scenarios"][key].clone())
}

/// The unlocked-wrapped boundary derives and the unwrapped proof is never consulted.
fn assert_wrapped_boundary(input: BatchInput, logical: &str) -> anyhow::Result<()> {
    let (output, warnings) = interpret_with_warnings(input)?;
    let boundaries = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "MigrationApplied"
                && event.logical_name_id.as_deref() == Some(logical)
        })
        .collect::<Vec<_>>();
    assert_eq!(boundaries.len(), 1, "{logical}: one boundary");
    assert_eq!(
        boundaries[0].after_state["migration_path"],
        "unlocked_wrapped"
    );
    assert!(warnings.all().is_empty(), "{:?}", warnings.all());
    Ok(())
}

/// Never moved, migrated unwrapped, so no holder is recorded for the lease.
#[test]
fn catalog_lease_never_moved_migrated_unwrapped_reconciles() -> anyhow::Result<()> {
    let (input, block, logical) = catalog_unwrapped(Vec::new())?;
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_no_known_holder(&output, &logical);
    assert_reconciled(&output, &warnings, block, &logical);
    Ok(())
}

/// Moved between holders on the BaseRegistrar, then migrated unwrapped. The transfer records the
/// holder.
#[test]
fn catalog_lease_moved_unwrapped_migrated_unwrapped_reconciles() -> anyhow::Result<()> {
    let scenario = catalog_scenario("U-01")?;
    let fixture = fixture()?;
    let block = scenario["migration_block"].as_i64().expect("block");
    let (input, block, logical) = catalog_unwrapped(vec![registrar_transfer_at(
        Address::from(EARLIER_HOLDER),
        Address::from(CATALOG_UNWRAPPED_HOLDER),
        keccak256(b"u01"),
        block - 2,
        0,
        fixture["addresses"]["base_registrar"]
            .as_str()
            .expect("BaseRegistrar"),
    )])?;
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_reconciled(&output, &warnings, block, &logical);
    Ok(())
}

/// Wrapped and unwrapped again, then migrated unwrapped. The unwrap's token transfer records the
/// holder.
#[test]
fn catalog_lease_moved_wrapped_migrated_unwrapped_reconciles() -> anyhow::Result<()> {
    let scenario = catalog_scenario("U-01")?;
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let block = scenario["migration_block"].as_i64().expect("block");
    let holder = Address::from(CATALOG_UNWRAPPED_HOLDER);
    let node: B256 = scenario["namehash"].as_str().expect("namehash").parse()?;
    let mut moves = wrap(&scenario, holder, block - 3)?;
    moves.extend([
        raw_at_transaction(
            super::super::v1_registry::Transfer {
                node,
                owner: holder,
            }
            .encode_log_data(),
            block - 2,
            0,
            0,
            CATALOG_REGISTRY,
        ),
        raw_at_transaction(
            super::super::NameUnwrapped {
                node,
                owner: holder,
            }
            .encode_log_data(),
            block - 2,
            0,
            1,
            addresses["name_wrapper"].as_str().expect("NameWrapper"),
        ),
        registrar_transfer_at(
            address(addresses, "name_wrapper")?,
            holder,
            keccak256(b"u01"),
            block - 2,
            2,
            addresses["base_registrar"].as_str().expect("BaseRegistrar"),
        ),
    ]);
    let (input, block, logical) = catalog_unwrapped(moves)?;
    let (output, warnings) = interpret_with_warnings(input)?;
    assert_reconciled(&output, &warnings, block, &logical);
    Ok(())
}

/// Never moved on the BaseRegistrar, migrated wrapped. No chain has this history, since
/// wrapping moves the token into the NameWrapper. The constructed shape shows the proof is not
/// consulted.
#[test]
fn catalog_lease_never_moved_migrated_wrapped_is_not_proved() -> anyhow::Result<()> {
    let scenario = catalog_scenario("W-01")?;
    let block = scenario["migration_block"].as_i64().expect("block");
    let (input, logical) = catalog_wrapped(vec![name_wrapped(
        &scenario,
        Address::from(CATALOG_WRAPPED_HOLDER),
        block - 1,
        0,
    )?])?;
    assert_wrapped_boundary(input, &logical)
}

/// Moved between holders on the BaseRegistrar, migrated wrapped. The proof is not consulted.
#[test]
fn catalog_lease_moved_unwrapped_migrated_wrapped_is_not_proved() -> anyhow::Result<()> {
    let scenario = catalog_scenario("W-01")?;
    let fixture = fixture()?;
    let block = scenario["migration_block"].as_i64().expect("block");
    let (input, logical) = catalog_wrapped(vec![
        registrar_transfer_at(
            Address::from(EARLIER_HOLDER),
            Address::from(CATALOG_WRAPPED_HOLDER),
            keccak256(b"w01"),
            block - 2,
            0,
            fixture["addresses"]["base_registrar"]
                .as_str()
                .expect("BaseRegistrar"),
        ),
        name_wrapped(
            &scenario,
            Address::from(CATALOG_WRAPPED_HOLDER),
            block - 1,
            0,
        )?,
    ])?;
    assert_wrapped_boundary(input, &logical)
}

/// Wrapped with `wrapETH2LD`, migrated wrapped. The proof is not consulted.
#[test]
fn catalog_lease_moved_wrapped_migrated_wrapped_is_not_proved() -> anyhow::Result<()> {
    let scenario = catalog_scenario("W-01")?;
    let block = scenario["migration_block"].as_i64().expect("block");
    let (input, logical) = catalog_wrapped(wrap(
        &scenario,
        Address::from(CATALOG_WRAPPED_HOLDER),
        block - 1,
    )?)?;
    assert_wrapped_boundary(input, &logical)
}
