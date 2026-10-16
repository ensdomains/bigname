//! The input-class table of a node's registry-only resource: every kind of registry owner write,
//! registrar transfer, registration and lease release, from each kind of prior holder. The
//! resource holds `resource_control`, and `resolver_control` at the node's resolver, for one
//! account, the owner the newest registry owner write named, whichever authority is selected.
//! A zero owner, a write a live lease takes over and the admitted Graveyard leave no holder. A
//! write that a registration later in its transaction takes over keeps the holder when it names
//! the current holder, because it makes no row for the lease to take.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
//!
//! Each row runs in one batch and cut after its setup and after each step, resumed warm,
//! restored cold and restored from folded rows. Every row checks the live grants at the end and
//! the one-holder rule after every log, and the rows marked for it check that the cold layouts
//! also ran the lookahead-scoped restore. The rows after a locked ENSv1→ENSv2 migration are in
//! `migration::child::registry_holder`, where the batch takes the locked re-run.

use super::*;

/// A check that a permission row must not pass.
type Forbidden = Box<dyn Fn(&NormalizedEvent) -> bool>;

/// One row of the table.
pub(in crate::schema_v2::tests) struct HolderRow {
    pub(in crate::schema_v2::tests) row: &'static str,
    pub(in crate::schema_v2::tests) prefix: Vec<RawLogInput>,
    pub(in crate::schema_v2::tests) steps: Vec<Vec<RawLogInput>>,
    pub(in crate::schema_v2::tests) resource: uuid::Uuid,
    /// The live grants on the resource after the last step.
    pub(in crate::schema_v2::tests) expected: LiveGrants,
    /// Rows on the resource that no layout may emit.
    pub(in crate::schema_v2::tests) forbidden: Option<Forbidden>,
    /// The one-batch layout takes the locked ENSv1→ENSv2 migration re-run.
    pub(in crate::schema_v2::tests) rerun: bool,
    /// The cold layouts also run the lookahead-scoped restore.
    pub(in crate::schema_v2::tests) lookahead: bool,
}

impl HolderRow {
    pub(in crate::schema_v2::tests) fn new(
        row: &'static str,
        prefix: Vec<RawLogInput>,
        steps: Vec<Vec<RawLogInput>>,
        resource: uuid::Uuid,
        expected: LiveGrants,
    ) -> Self {
        Self {
            row,
            prefix,
            steps,
            resource,
            expected,
            forbidden: None,
            rerun: false,
            lookahead: false,
        }
    }

    pub(in crate::schema_v2::tests) fn forbid(
        mut self,
        forbidden: impl Fn(&NormalizedEvent) -> bool + 'static,
    ) -> Self {
        self.forbidden = Some(Box::new(forbidden));
        self
    }

    pub(in crate::schema_v2::tests) fn rerun(mut self) -> Self {
        self.rerun = true;
        self
    }

    pub(in crate::schema_v2::tests) fn lookahead(mut self) -> Self {
        self.lookahead = true;
        self
    }
}

/// `owner`'s grants on a registry-only resource: `resource_control`, and `resolver_control` at
/// `resolver` when there is one, scoped as in `scoped_grants_on`.
pub(in crate::schema_v2::tests) fn held_by(owner: &str, resolver: Option<&str>) -> LiveGrants {
    let mut grants = LiveGrants::new();
    grants.insert((
        owner.to_ascii_lowercase(),
        "resource".to_owned(),
        vec!["resource_control".to_owned()],
    ));
    if let Some(resolver) = resolver {
        grants.insert((
            owner.to_ascii_lowercase(),
            format!("resolver:{}", resolver.to_ascii_lowercase()),
            vec!["resolver_control".to_owned()],
        ));
    }
    grants
}

/// The registry-only resource of `node`.
pub(in crate::schema_v2::tests) fn registry_only_of(node: &str) -> uuid::Uuid {
    super::super::common::stable_uuid(&format!(
        "resource:registry-only:{CHAIN}:{}",
        node.to_ascii_lowercase()
    ))
}

/// A permission row granting or revoking `subject`.
pub(in crate::schema_v2::tests) fn about(event: &NormalizedEvent, subject: &str) -> bool {
    event.after_state["subject"]
        .as_str()
        .is_some_and(|named| named.eq_ignore_ascii_case(subject))
}

fn is_grant(event: &NormalizedEvent) -> bool {
    event.after_state["effective_powers"]
        .as_array()
        .is_some_and(|powers| !powers.is_empty())
}

/// Where the rows on `resource` break the one-holder rule, read in order after each log: two
/// subjects with `resource_control`, two live resolver scopes, or `resolver_control` with a
/// subject that has no `resource_control`.
fn holder_breaks(outputs: &[&BatchOutput], resource: uuid::Uuid) -> Vec<String> {
    let rows = outputs
        .iter()
        .flat_map(|output| output.normalized_events.iter())
        .filter(|event| {
            event.event_kind == "PermissionChanged" && event.resource_id == Some(resource)
        })
        .collect::<Vec<_>>();
    let mut live = std::collections::BTreeMap::new();
    let mut breaks = Vec::new();
    for (index, event) in rows.iter().enumerate() {
        let state = &event.after_state;
        let scope = match state["scope"]["resolver_address"].as_str() {
            Some(resolver) => format!("resolver:{}", resolver.to_ascii_lowercase()),
            None => state["scope"]["kind"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        };
        let subject = state["subject"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        live.insert((subject, scope), is_grant(event));
        let at = (event.block_number, event.log_index);
        if rows
            .get(index + 1)
            .is_some_and(|next| (next.block_number, next.log_index) == at)
        {
            continue;
        }
        let holders = live
            .iter()
            .filter(|((_, scope), granted)| **granted && scope == "resource")
            .map(|((subject, _), _)| subject.as_str())
            .collect::<Vec<_>>();
        let resolvers = live
            .iter()
            .filter(|((_, scope), granted)| **granted && scope.starts_with("resolver:"))
            .map(|((subject, scope), _)| (subject.as_str(), scope.as_str()))
            .collect::<Vec<_>>();
        if holders.len() > 1
            || resolvers.len() > 1
            || resolvers
                .iter()
                .any(|(subject, _)| !holders.contains(subject))
        {
            breaks.push(format!(
                "after log {at:?}: holders {holders:?}, resolver scopes {resolvers:?}"
            ));
        }
    }
    breaks
}

/// Runs every row of `rows` in every layout of `every_layout`, each batch built by `build`, and
/// returns what each row's layouts found wrong.
pub(in crate::schema_v2::tests) fn holder_row_failures(
    build: &dyn Fn(Vec<RawLogInput>) -> BatchInput,
    rows: Vec<HolderRow>,
) -> anyhow::Result<Vec<String>> {
    let mut failures = Vec::new();
    for row in rows {
        let comparisons = super::super::lookahead::scoped_comparisons();
        for (layout, outputs) in every_layout(build, &row.prefix, &row.steps)? {
            let context = format!("{}, {layout}", row.row);
            let all = outputs.iter().collect::<Vec<_>>();
            let grants = scoped_grants_on(&all, row.resource);
            if grants != row.expected {
                failures.push(format!(
                    "{context}: grants {grants:?}, expected {:?}",
                    row.expected
                ));
            }
            failures.extend(
                holder_breaks(&all, row.resource)
                    .into_iter()
                    .map(|found| format!("{context}: {found}")),
            );
            if let Some(forbidden) = row.forbidden.as_ref() {
                let found = all
                    .iter()
                    .flat_map(|output| output.normalized_events.iter())
                    .filter(|event| {
                        event.event_kind == "PermissionChanged"
                            && event.resource_id == Some(row.resource)
                            && forbidden(event)
                    })
                    .map(|event| event.event_identity.as_str())
                    .collect::<Vec<_>>();
                if !found.is_empty() {
                    failures.push(format!("{context}: forbidden rows {found:?}"));
                }
            }
            if row.rerun
                && layout == "one batch"
                && !all[0].normalized_events.iter().any(|event| {
                    event.event_kind == "MigrationApplied"
                        && event.after_state["migration_path"] == "locked_wrapped"
                        && event.consumer_visibility == "activated"
                })
            {
                failures.push(format!("{context}: the locked re-run did not run"));
            }
        }
        if row.lookahead && super::super::lookahead::scoped_comparisons() == comparisons {
            failures.push(format!("{}: no lookahead-scoped restore ran", row.row));
        }
    }
    Ok(failures)
}

pub(in crate::schema_v2::tests) fn assert_no_failures(failures: Vec<String>) -> anyhow::Result<()> {
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

fn child_hex() -> String {
    format!("{:#x}", child())
}

fn leased() -> B256 {
    node(&["leased", "eth"])
}

fn leased_hex() -> String {
    format!("{:#x}", leased())
}

/// `raw` moved into transaction `index` of its block.
fn in_transaction(mut raw: RawLogInput, index: i64) -> RawLogInput {
    raw.transaction_hash = format!("transaction-{}-{index}", raw.block_number);
    raw.transaction_index = index;
    raw
}

/// A controller's `NameRegistered` of `leased.eth` for `owner`, as `lease_batch` watches it.
fn registered(block: i64, log_index: i64, owner: &str, expires: u64) -> RawLogInput {
    raw_at(
        NameRegistered {
            name: "leased".to_owned(),
            label: keccak256(b"leased"),
            owner: addr(owner),
            expires: U256::from(expires),
        }
        .encode_log_data(),
        block,
        log_index,
        CONTRACT,
    )
}

/// The BaseRegistrar's own `NameRegistered(uint256,address,uint256)` of `leased.eth`.
fn numeric_registered(block: i64, log_index: i64, owner: &str, expires: u64) -> RawLogInput {
    raw_at(
        with_topic0(
            v1_registrar::BaseNameRegistered {
                id: U256::from_be_bytes(keccak256(b"leased").0),
                owner: addr(owner),
                expires: U256::from(expires),
            }
            .encode_log_data(),
            keccak256(b"NameRegistered(uint256,address,uint256)"),
        ),
        block,
        log_index,
        CONTRACT,
    )
}

/// The batch for `logs` with the BaseRegistrar's numeric `NameRegistered` and `Transfer`
/// watched, as on Sepolia.
fn numeric_batch(logs: Vec<RawLogInput>) -> BatchInput {
    let mut input = batch(logs);
    let lifecycle = &[
        "RegistrationGranted",
        "ExpiryChanged",
        "PermissionChanged",
        "SurfaceUnbound",
        "SurfaceBound",
        "AuthorityEpochChanged",
        "ResolverChanged",
        "RegistrationReleased",
        "TokenControlTransferred",
    ];
    input.manifests.push(manifest_with_events(
        REGISTRAR_MANIFEST,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "NameRegistered",
                "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)",
                &["registrar"],
                lifecycle,
            ),
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                lifecycle,
            ),
        ],
    ));
    input
        .admissions
        .push(admission(REGISTRAR_MANIFEST, "registrar"));
    input
}

/// The `.eth` node, and HOLDER's registration of `leased.eth` at block 2 with RESOLVER set at
/// block 3. The registration writes the registry in its own transaction, so the lease takes
/// that write over and the registry-only resource has no holder.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
fn leased_setup() -> Vec<RawLogInput> {
    vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
        new_owner_under(node(&["eth"]), 2, 0, "leased", HOLDER),
        registered(2, 1, HOLDER, 4_000_000_000),
        new_resolver(3, 0, leased(), RESOLVER),
    ]
}

/// `leased_setup`, then HOLDER's registry `setOwner` to AWAY_OWNER at block 4: the registry
/// owner leaves the registrant, so the registry-only authority is selected with AWAY_OWNER as
/// its holder.
fn leased_and_diverged() -> Vec<RawLogInput> {
    let mut logs = leased_setup();
    logs.push(registry_transfer(4, 0, leased(), AWAY_OWNER));
    logs
}

const RELEASED: i64 = 8_000_000;

/// Class A: no prior holder.
#[test]
fn a_write_with_no_prior_holder_grants_its_owner() -> anyhow::Result<()> {
    let resource = registry_only_of(&child_hex());
    let parent_only = unwrapped_child_setup()[..2].to_vec();
    let mut failures = holder_row_failures(
        &batch,
        vec![
            HolderRow::new(
                "A1 first write, then its resolver",
                parent_only.clone(),
                vec![
                    vec![new_owner(2, 0, "child", CHILD_OWNER)],
                    vec![new_resolver(3, 0, child(), RESOLVER)],
                ],
                resource,
                held_by(CHILD_OWNER, Some(RESOLVER)),
            )
            .lookahead(),
            HolderRow::new(
                "A1 reopened after a zero-owner close",
                unwrapped_child_setup(),
                vec![
                    vec![registry_transfer(3, 0, child(), ZERO)],
                    vec![new_owner(4, 0, "child", AWAY_OWNER)],
                ],
                resource,
                held_by(AWAY_OWNER, Some(RESOLVER)),
            ),
            // The registry is watched from the wrap on: the child's own creation is never
            // seen, so `wrap`'s registry write is the first one.
            HolderRow::new(
                "A2 wrap is the first write seen",
                parent_only,
                vec![wrap_child(3)],
                resource,
                held_by(NAME_WRAPPER, None),
            ),
        ],
    )?;
    let graveyard_build = |logs: Vec<RawLogInput>| with_graveyard(batch(logs));
    failures.extend(holder_row_failures(
        &graveyard_build,
        vec![
            HolderRow::new(
                "A4 the parent's write naming the Graveyard after a zero close",
                unwrapped_child_setup(),
                vec![
                    vec![registry_transfer(3, 0, child(), ZERO)],
                    vec![new_owner(4, 0, "child", GRAVEYARD)],
                ],
                resource,
                LiveGrants::new(),
            )
            .forbid(|event| about(event, GRAVEYARD)),
        ],
    )?);
    // HOLDER's registrar token transfer to AWAY_OWNER leaves HOLDER the registry owner, so the
    // registry-only authority is selected again, and HOLDER holds it with its resolver.
    let mut steps = leased_setup();
    let setup = steps.drain(..2).collect::<Vec<_>>();
    failures.extend(holder_row_failures(
        &lease_batch,
        vec![HolderRow::new(
            "A5 a registrar transfer away from the registry owner",
            setup,
            vec![steps, vec![lease_transfer(5, 0, HOLDER, AWAY_OWNER)]],
            registry_only_of(&leased_hex()),
            held_by(HOLDER, Some(RESOLVER)),
        )],
    )?);
    assert_no_failures(failures)
}

/// `registerAndWrapETH2LD` of `leased.eth` for HOLDER at block 2 with an expiry of 10: the
/// registrar mints to the NameWrapper and writes the registry to it, then the NameWrapper mints
/// HOLDER's token. Then the plain re-registration to CHILD_OWNER once the lease is past grace,
/// which burns and mints the registrar token and writes the registry to the new owner.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
fn lapsed_and_registered_again() -> (Vec<RawLogInput>, Vec<RawLogInput>) {
    let wrap = vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
        lease_transfer(2, 0, ZERO, NAME_WRAPPER),
        new_owner_under(node(&["eth"]), 2, 1, "leased", NAME_WRAPPER),
        transfer_single(2, 2, NAME_WRAPPER, ZERO, HOLDER, leased()),
        name_wrapped_with(
            2,
            3,
            leased(),
            b"\x06leased\x03eth\0",
            HOLDER,
            (DOT_ETH_FUSES, 10 + 90 * 86_400),
        ),
        registered(2, 4, NAME_WRAPPER, 10),
    ];
    let again = vec![
        lease_transfer(RELEASED, 0, NAME_WRAPPER, ZERO),
        lease_transfer(RELEASED, 1, ZERO, CHILD_OWNER),
        new_owner_under(node(&["eth"]), RELEASED, 2, "leased", CHILD_OWNER),
        registered(RELEASED, 3, CHILD_OWNER, 9_000_000),
    ];
    (wrap, again)
}

/// Class F: after a lapsed wrapped `.eth` lease. The registration's own registry write is
/// taken over by its lease, so the re-registration grants nothing on the registry-only
/// resource and revokes nothing of the NameWrapper's there. The new registrant's own registry
/// write then gives the resource to the owner it names.
#[test]
fn a_registration_after_a_lapsed_wrapped_lease_leaves_no_holder() -> anyhow::Result<()> {
    let resource = registry_only_of(&leased_hex());
    let (wrap, again) = lapsed_and_registered_again();
    let failures = holder_row_failures(
        &lease_batch,
        vec![
            HolderRow::new(
                "A3/F1 registered again after the lapse",
                wrap.clone(),
                vec![again.clone()],
                resource,
                LiveGrants::new(),
            )
            .forbid(|event| about(event, NAME_WRAPPER) && !is_grant(event))
            .lookahead(),
            HolderRow::new(
                "F3 registered again, then the registrant's registry write",
                wrap,
                vec![
                    again,
                    vec![registry_transfer(RELEASED + 1, 0, leased(), AWAY_OWNER)],
                ],
                resource,
                held_by(AWAY_OWNER, None),
            ),
        ],
    )?;
    assert_no_failures(failures)
}

/// Class B: the NameWrapper holds the resource of a child wrapped by `wrap`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L374 @ ens_v1@91c966f)
#[test]
fn a_write_after_a_wrap_moves_the_name_wrappers_grants() -> anyhow::Result<()> {
    let resource = registry_only_of(&child_hex());
    let mut wrapped = unwrapped_child_setup();
    wrapped.extend(wrap_child(3));
    let row =
        |row, steps, expected| HolderRow::new(row, wrapped.clone(), steps, resource, expected);
    let unwrap_to_zero = vec![
        registry_transfer(4, 0, child(), NAME_WRAPPER),
        transfer_single(4, 1, CHILD_OWNER, CHILD_OWNER, ZERO, child()),
        registry_transfer(4, 2, child(), ZERO),
        name_unwrapped(4, 3, child(), ZERO),
    ];
    let mut failures = holder_row_failures(
        &batch,
        vec![
            row(
                "B1 unwrap to another account, then its resolver change",
                vec![
                    unwrap_child(4, CHILD_OWNER, TRANSFEREE),
                    vec![new_resolver(5, 0, child(), NEXT_RESOLVER)],
                ],
                held_by(TRANSFEREE, Some(NEXT_RESOLVER)),
            )
            .lookahead(),
            row(
                "B2 the parent hands the wrapped record to another account",
                vec![vec![new_owner(4, 0, "child", AWAY_OWNER)]],
                held_by(AWAY_OWNER, Some(RESOLVER)),
            ),
            row(
                "B3 the parent's zero-owner write",
                vec![vec![new_owner(4, 0, "child", ZERO)]],
                LiveGrants::new(),
            ),
            // `setRecord`'s registry rewrite to the NameWrapper itself keeps the holder.
            row(
                "B4 the NameWrapper's setRecord rewrite",
                vec![vec![
                    registry_transfer(4, 0, child(), NAME_WRAPPER),
                    new_resolver(4, 1, child(), NEXT_RESOLVER),
                ]],
                held_by(NAME_WRAPPER, Some(NEXT_RESOLVER)),
            )
            .forbid(|event| event.block_number == Some(4) && event.log_index == Some(0)),
            row(
                "B5 the NameWrapper's setRecord to the zero owner",
                vec![unwrap_to_zero],
                LiveGrants::new(),
            ),
            row(
                "B6 the NameWrapper's setResolver",
                vec![vec![new_resolver(4, 0, child(), NEXT_RESOLVER)]],
                held_by(NAME_WRAPPER, Some(NEXT_RESOLVER)),
            ),
        ],
    )?;
    let graveyard_build = |logs: Vec<RawLogInput>| with_graveyard(batch(logs));
    failures.extend(holder_row_failures(
        &graveyard_build,
        vec![
            row(
                "B7 the holder's unwrap into the Graveyard",
                vec![vec![
                    transfer_single(4, 0, CHILD_OWNER, CHILD_OWNER, ZERO, child()),
                    registry_transfer(4, 1, child(), GRAVEYARD),
                    name_unwrapped(4, 2, child(), GRAVEYARD),
                ]],
                LiveGrants::new(),
            )
            .forbid(|event| about(event, GRAVEYARD)),
            row(
                "B8 the parent's write naming the Graveyard, with no unwrap",
                vec![vec![new_owner(4, 0, "child", GRAVEYARD)]],
                LiveGrants::new(),
            )
            .forbid(|event| about(event, GRAVEYARD)),
        ],
    )?);
    // The record leaves the NameWrapper through the parent and its new owner wraps it again.
    // `_mint` burns the earlier token and emits `NameUnwrapped` with the zero owner first.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L891 @ ens_v1@91c966f)
    failures.extend(holder_row_failures(
        &batch,
        vec![HolderRow::new(
            "B9 away, then wrapped by its new owner",
            custody_setup(),
            vec![
                vec![new_owner(3, 0, "child", AWAY_OWNER)],
                vec![
                    registry_transfer(4, 0, child(), NAME_WRAPPER),
                    transfer_single(4, 1, AWAY_OWNER, CHILD_OWNER, ZERO, child()),
                    name_unwrapped(4, 2, child(), ZERO),
                    transfer_single(4, 3, AWAY_OWNER, ZERO, AWAY_OWNER, child()),
                    name_wrapped(4, 4, child(), b"\x05child\x06parent\x03eth\0", AWAY_OWNER),
                ],
            ],
            resource,
            held_by(NAME_WRAPPER, Some(RESOLVER)),
        )],
    )?);
    assert_no_failures(failures)
}

/// Class C: an ordinary registry owner holds the resource while it is selected.
#[test]
fn a_write_over_a_selected_registry_only_authority_moves_its_holder() -> anyhow::Result<()> {
    let resource = registry_only_of(&child_hex());
    let row = |row, steps, expected| {
        HolderRow::new(row, unwrapped_child_setup(), steps, resource, expected)
    };
    let mut failures = holder_row_failures(
        &batch,
        vec![
            row(
                "C1 setOwner",
                vec![vec![registry_transfer(3, 0, child(), AWAY_OWNER)]],
                held_by(AWAY_OWNER, Some(RESOLVER)),
            )
            .lookahead(),
            row(
                "C2 the owner's zero write",
                vec![vec![registry_transfer(3, 0, child(), ZERO)]],
                LiveGrants::new(),
            ),
            row(
                "C2 the parent's zero write",
                vec![vec![new_owner(3, 0, "child", ZERO)]],
                LiveGrants::new(),
            ),
            row(
                "C3 wrap",
                vec![wrap_child(3)],
                held_by(NAME_WRAPPER, Some(RESOLVER)),
            ),
            row(
                "C8 setResolver",
                vec![vec![new_resolver(3, 0, child(), NEXT_RESOLVER)]],
                held_by(CHILD_OWNER, Some(NEXT_RESOLVER)),
            ),
            // `setRecord` is `setOwner`, then the resolver.
            // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33-L41 @ ens_v1@91c966f)
            row(
                "C9 setRecord",
                vec![vec![
                    registry_transfer(3, 0, child(), AWAY_OWNER),
                    new_resolver(3, 1, child(), NEXT_RESOLVER),
                ]],
                held_by(AWAY_OWNER, Some(NEXT_RESOLVER)),
            ),
        ],
    )?;
    let graveyard_build = |logs: Vec<RawLogInput>| with_graveyard(batch(logs));
    failures.extend(holder_row_failures(
        &graveyard_build,
        vec![
            row(
                "C10 the parent's write naming the Graveyard",
                vec![vec![new_owner(3, 0, "child", GRAVEYARD)]],
                LiveGrants::new(),
            )
            .forbid(|event| about(event, GRAVEYARD)),
        ],
    )?);
    // A wrapped parent's `setSubnodeOwner` creates the child wrapped: the registry write to the
    // NameWrapper, the mint, then `NameWrapped`.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L577-L581 @ ens_v1@91c966f)
    let mut parent = unwrapped_child_setup()[..2].to_vec();
    parent.extend(wrap_parent(2));
    failures.extend(holder_row_failures(
        &batch,
        vec![HolderRow::new(
            "C4 a wrapped parent creates the child wrapped",
            parent,
            vec![vec![
                new_owner(3, 0, "child", NAME_WRAPPER),
                transfer_single(3, 1, HOLDER, ZERO, CHILD_OWNER, child()),
                name_wrapped(3, 2, child(), b"\x05child\x06parent\x03eth\0", CHILD_OWNER),
            ]],
            resource,
            held_by(NAME_WRAPPER, None),
        )],
    )?);
    let leased_resource = registry_only_of(&leased_hex());
    let diverged = |row, steps, expected| {
        HolderRow::new(row, leased_and_diverged(), steps, leased_resource, expected)
    };
    failures.extend(holder_row_failures(
        &lease_batch,
        vec![
            // The registrant's `reclaim` writes the registry back to itself, and the live lease
            // takes that write over.
            // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
            diverged(
                "C5 the registrant's reclaim",
                vec![vec![new_owner_under(
                    node(&["eth"]),
                    5,
                    0,
                    "leased",
                    HOLDER,
                )]],
                LiveGrants::new(),
            ),
            diverged(
                "C6 a registrar transfer the registry owner does not follow",
                vec![vec![lease_transfer(5, 0, HOLDER, TRANSFEREE)]],
                held_by(AWAY_OWNER, Some(RESOLVER)),
            ),
            diverged(
                "C7 a registrar transfer to the registry owner",
                vec![vec![lease_transfer(5, 0, HOLDER, AWAY_OWNER)]],
                LiveGrants::new(),
            ),
        ],
    )?);
    assert_no_failures(failures)
}

/// The old registry's `NewOwner` at `block`, with `data` as its owner word.
fn old_registry_owner(block: i64, parent: B256, labelhash: B256, data: Vec<u8>) -> RawLogInput {
    RawLogInput {
        block_number: block,
        block_hash: format!("block-{block}"),
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block),
        ..lll_old_registry_raw(
            vec![
                LLL_NEW_OWNER_TOPIC0.to_owned(),
                format!("{parent:#x}"),
                format!("{labelhash:#x}"),
            ],
            data,
        )
    }
}

/// C11: an old-registry owner word with dirty high bytes closes the holder like the zero-owner
/// arm, and a later clean write grants its owner.
#[test]
fn an_unmasked_owner_word_ends_the_holder() -> anyhow::Result<()> {
    let parent = B256::repeat_byte(0x51);
    let labelhash = B256::repeat_byte(0x52);
    let node = keccak256([parent.as_slice(), labelhash.as_slice()].concat());
    let word = |owner: &str| {
        let mut data = [0_u8; 32];
        data[12..].copy_from_slice(addr(owner).as_slice());
        data.to_vec()
    };
    let resource = registry_only_of(&format!("{node:#x}"));
    let clean = vec![old_registry_owner(10, parent, labelhash, word(CHILD_OWNER))];
    let dirty = vec![old_registry_owner(
        11,
        parent,
        labelhash,
        hex::decode(LLL_UNMASKED_OWNER_WORD)?,
    )];
    let successor = vec![old_registry_owner(12, parent, labelhash, word(AWAY_OWNER))];
    let failures = holder_row_failures(
        &lll_old_registry_input,
        vec![
            HolderRow::new(
                "C11 an unmasked owner word",
                clean.clone(),
                vec![dirty.clone()],
                resource,
                LiveGrants::new(),
            ),
            HolderRow::new(
                "C11 an unmasked owner word, then a clean write",
                clean,
                vec![dirty, successor],
                resource,
                held_by(AWAY_OWNER, None),
            ),
        ],
    )?;
    assert_no_failures(failures)
}

/// Class E: the registry is watched only after the NameWrapper, so the wrapped child's
/// resource has no remembered owner when its first registry write is seen.
#[test]
fn a_first_registry_write_after_a_wrap_grants_its_owner() -> anyhow::Result<()> {
    let resource = registry_only_of(&child_hex());
    let build = |logs: Vec<RawLogInput>| {
        let mut input = batch(logs);
        for admission in &mut input.admissions {
            if admission.address == REGISTRY {
                admission.active_from_block = Some(3);
            }
        }
        input
    };
    let mut wrapped = unwrapped_child_setup()[..2].to_vec();
    wrapped.extend(wrap_parent(1).into_iter().map(|mut raw| {
        raw.log_index += 2;
        raw
    }));
    wrapped.extend([
        new_owner(2, 0, "child", NAME_WRAPPER),
        transfer_single(2, 1, HOLDER, ZERO, CHILD_OWNER, child()),
        name_wrapped(2, 2, child(), b"\x05child\x06parent\x03eth\0", CHILD_OWNER),
    ]);
    let failures = holder_row_failures(
        &build,
        vec![
            HolderRow::new(
                "E2 the parent's write is the first one seen",
                wrapped.clone(),
                vec![vec![new_owner(3, 0, "child", AWAY_OWNER)]],
                resource,
                held_by(AWAY_OWNER, None),
            )
            .lookahead(),
            HolderRow::new(
                "E3 the NameWrapper's setResolver is the first one seen",
                wrapped,
                vec![vec![new_resolver(3, 0, child(), NEXT_RESOLVER)]],
                resource,
                LiveGrants::new(),
            ),
        ],
    )?;
    assert_no_failures(failures)
}

/// Class G: `.eth` wraps with a live lease. `registerAndWrapETH2LD` writes the registry to the
/// NameWrapper inside its registration, which the lease takes over, and an `unwrapETH2LD` later
/// in the same block writes it to the new owner while the lease is live. Neither leaves a
/// holder, so the unwrap revokes nothing the registration's write granted, with the
/// controller's registration and with the BaseRegistrar's numeric one, and with the unwrap in
/// the next block too.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L381-L395 @ ens_v1@91c966f)
#[test]
fn an_unwrap_after_a_register_and_wrap_revokes_nothing() -> anyhow::Result<()> {
    let resource = registry_only_of(&leased_hex());
    // The BaseRegistrar's numeric `NameRegistered` follows its registry write, and the
    // controller's `NameRegistered` closes the transaction after the wrap.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
    let register_and_wrap = |numeric: bool| {
        let mut logs = vec![
            lease_transfer(2, 0, ZERO, NAME_WRAPPER),
            new_owner_under(node(&["eth"]), 2, 1, "leased", NAME_WRAPPER),
            transfer_single(2, 2, NAME_WRAPPER, ZERO, HOLDER, leased()),
            name_wrapped_with(
                2,
                3,
                leased(),
                b"\x06leased\x03eth\0",
                HOLDER,
                (DOT_ETH_FUSES, 4_000_000_000 + 90 * 86_400),
            ),
        ];
        if numeric {
            logs.insert(2, numeric_registered(2, 2, NAME_WRAPPER, 4_000_000_000));
            logs[3].log_index = 3;
            logs[4].log_index = 4;
        } else {
            logs.push(registered(2, 4, HOLDER, 4_000_000_000));
        }
        logs
    };
    let unwrap = |block: i64, first_log: i64| {
        let mut logs = vec![
            transfer_single(block, first_log, HOLDER, HOLDER, ZERO, leased()),
            registry_transfer(block, first_log + 1, leased(), TRANSFEREE),
            name_unwrapped(block, first_log + 2, leased(), TRANSFEREE),
            lease_transfer(block, first_log + 3, NAME_WRAPPER, TRANSFEREE),
        ];
        if block == 2 {
            logs = logs.into_iter().map(|raw| in_transaction(raw, 1)).collect();
        }
        logs
    };
    let eth = vec![new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT)];
    let no_revoke = |event: &NormalizedEvent| !is_grant(event);
    let mut rows = Vec::new();
    for numeric in [false, true] {
        let mut same_block = register_and_wrap(numeric);
        same_block.extend(unwrap(2, 5));
        let rows_for = |row, steps| {
            HolderRow::new(row, eth.clone(), steps, resource, LiveGrants::new()).forbid(no_revoke)
        };
        let (same, next) = if numeric {
            (
                "G2 numeric registration, unwrap later in its block",
                "G2 numeric registration, unwrap in the next block",
            )
        } else {
            (
                "G2 controller registration, unwrap later in its block",
                "G2 controller registration, unwrap in the next block",
            )
        };
        rows.push((numeric, rows_for(same, vec![same_block]).lookahead()));
        rows.push((
            numeric,
            rows_for(next, vec![register_and_wrap(numeric), unwrap(3, 0)]),
        ));
    }
    let (controller, numeric): (Vec<_>, Vec<_>) =
        rows.into_iter().partition(|(numeric, _)| !numeric);
    let mut failures = holder_row_failures(
        &lease_batch,
        controller.into_iter().map(|(_, row)| row).collect(),
    )?;
    failures.extend(holder_row_failures(
        &numeric_batch,
        numeric.into_iter().map(|(_, row)| row).collect(),
    )?);
    assert_no_failures(failures)
}

/// G4: a numeric registration that leaves a divergent registry owner selected, as `registerOnly`
/// does, finds that owner already holding the resource at the node's resolver and emits no row
/// on it.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
#[test]
fn a_register_only_registration_keeps_the_registry_owners_grants() -> anyhow::Result<()> {
    let resource = registry_only_of(&leased_hex());
    let setup = vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
        new_owner_under(node(&["eth"]), 1, 1, "leased", AWAY_OWNER),
        new_resolver(1, 2, leased(), RESOLVER),
    ];
    let failures = holder_row_failures(
        &numeric_batch,
        vec![
            HolderRow::new(
                "G4 registerOnly beside a registry owner",
                setup,
                vec![vec![
                    lease_transfer(2, 0, ZERO, HOLDER),
                    numeric_registered(2, 1, HOLDER, 4_000_000_000),
                ]],
                resource,
                held_by(AWAY_OWNER, Some(RESOLVER)),
            )
            .forbid(|event| event.block_number == Some(2)),
        ],
    )?;
    assert_no_failures(failures)
}

/// The release boundary: a lapsed unwrapped lease hands the name back to its registry owner,
/// which then holds the registry-only resource with the node's resolver. When the
/// registration's registry write was its own transaction, so the lease never took it over, the
/// owner already holds the resource and the release adds no row.
#[test]
fn a_lease_release_hands_the_resource_back_to_the_registry_owner() -> anyhow::Result<()> {
    let resource = registry_only_of(&leased_hex());
    let release = vec![new_owner_under(
        node(&["eth"]),
        RELEASED,
        0,
        "other",
        AWAY_OWNER,
    )];
    let registration = |own_transaction: bool| {
        let mut write = new_owner_under(node(&["eth"]), 2, 0, "leased", HOLDER);
        let mut registration = registered(2, 1, HOLDER, 10);
        if own_transaction {
            write = in_transaction(write, 0);
            registration = in_transaction(registration, 1);
        }
        vec![write, registration, new_resolver(3, 0, leased(), RESOLVER)]
    };
    let eth = vec![new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT)];
    let registered_again = vec![
        lease_transfer(RELEASED + 1, 0, HOLDER, ZERO),
        lease_transfer(RELEASED + 1, 1, ZERO, AWAY_OWNER),
        new_owner_under(node(&["eth"]), RELEASED + 1, 2, "leased", AWAY_OWNER),
        registered(RELEASED + 1, 3, AWAY_OWNER, 9_000_000),
    ];
    let failures = holder_row_failures(
        &lease_batch,
        vec![
            HolderRow::new(
                "E12 a release after a registration the lease took over",
                eth.clone(),
                vec![registration(false), release.clone()],
                resource,
                held_by(HOLDER, Some(RESOLVER)),
            )
            .lookahead(),
            HolderRow::new(
                "E12 a release when the registry owner already holds the resource",
                eth.clone(),
                vec![registration(true), release.clone()],
                resource,
                held_by(HOLDER, Some(RESOLVER)),
            )
            .forbid(|event| event.block_number == Some(RELEASED)),
            // Another account registers the released name: `_register` burns the old token,
            // mints the new one and writes the registry to its owner. The lease takes that
            // write over, so the old owner's grants end and no one holds the resource.
            // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
            HolderRow::new(
                "E12 a release, then a registration by another account",
                eth.clone(),
                vec![
                    registration(false),
                    release.clone(),
                    registered_again.clone(),
                ],
                resource,
                LiveGrants::new(),
            )
            .lookahead(),
            HolderRow::new(
                "E12 a release, a registration by another account, then its registry write",
                eth,
                vec![
                    registration(false),
                    release.clone(),
                    registered_again,
                    vec![registry_transfer(RELEASED + 2, 0, leased(), TRANSFEREE)],
                ],
                resource,
                held_by(TRANSFEREE, Some(RESOLVER)),
            ),
        ],
    )?;
    assert_no_failures(failures)
}

/// A label the registrar controllers accept but the normalizer rejects, so its name gets no
/// surface. The controllers check only its length.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L191-L193 @ ens_v1@91c966f)
/// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L364-L366 @ basenames@1809bbc)
const UNSURFACED: &str = "LEASED";

fn unsurfaced() -> B256 {
    node(&[UNSURFACED, "eth"])
}

/// A controller's `NameRegistered` of `LEASED.eth` for `owner`, as `lease_batch` watches it.
fn unsurfaced_registered(block: i64, log_index: i64, owner: &str, expires: u64) -> RawLogInput {
    raw_at(
        NameRegistered {
            name: UNSURFACED.to_owned(),
            label: keccak256(UNSURFACED.as_bytes()),
            owner: addr(owner),
            expires: U256::from(expires),
        }
        .encode_log_data(),
        block,
        log_index,
        CONTRACT,
    )
}

/// The BaseRegistrar's `Transfer` of the `LEASED` token from `from` to `to`.
fn unsurfaced_transfer(block: i64, log_index: i64, from: &str, to: &str) -> RawLogInput {
    raw_at(
        v1_registrar::Transfer {
            from: addr(from),
            to: addr(to),
            tokenId: U256::from_be_bytes(keccak256(UNSURFACED.as_bytes()).0),
        }
        .encode_log_data(),
        block,
        log_index,
        CONTRACT,
    )
}

/// A lapsed unwrapped lease on a surface-unknown name hands the resource back to its registry
/// owner, as on a name with a surface. The release binds no surface for such a name, but the
/// hand-back is the holder's, which every other registry-only row follows whether or not the
/// name has a surface. When the registration's registry write was its own transaction the owner
/// already holds the resource and the release adds no row, and a Graveyard owner is granted
/// nothing. U5 is a guard, not this route: a Sepolia numeric-only registration names no label,
/// but the registry write before it already materialized the node's surface, so its release
/// hands back through the surface path.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
#[test]
fn a_lapsed_lease_on_a_surface_unknown_name_hands_the_resource_back_to_its_registry_owner()
-> anyhow::Result<()> {
    let resource = registry_only_of(&format!("{:#x}", unsurfaced()));
    let eth = vec![new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT)];
    let release = vec![new_owner_under(
        node(&["eth"]),
        RELEASED,
        0,
        "other",
        AWAY_OWNER,
    )];
    let registration = |own_transaction: bool, resolver: bool| {
        let mut write = new_owner_under(node(&["eth"]), 2, 0, UNSURFACED, HOLDER);
        let mut registration = unsurfaced_registered(2, 1, HOLDER, 10);
        if own_transaction {
            write = in_transaction(write, 0);
            registration = in_transaction(registration, 1);
        }
        let mut logs = vec![write, registration];
        if resolver {
            logs.push(new_resolver(3, 0, unsurfaced(), RESOLVER));
        }
        logs
    };
    let to_graveyard = vec![
        unsurfaced_transfer(4, 0, HOLDER, GRAVEYARD),
        registry_transfer(4, 1, unsurfaced(), GRAVEYARD),
    ];
    let mut failures = holder_row_failures(
        &lease_batch,
        vec![
            HolderRow::new(
                "U1 a release with a resolver set",
                eth.clone(),
                vec![registration(false, true), release.clone()],
                resource,
                held_by(HOLDER, Some(RESOLVER)),
            ),
            HolderRow::new(
                "U2 a release with no resolver",
                eth.clone(),
                vec![registration(false, false), release.clone()],
                resource,
                held_by(HOLDER, None),
            ),
            HolderRow::new(
                "U3 a release when the registry owner already holds the resource",
                eth.clone(),
                vec![registration(true, true), release.clone()],
                resource,
                held_by(HOLDER, Some(RESOLVER)),
            )
            .forbid(|event| event.block_number == Some(RELEASED)),
        ],
    )?;
    let graveyard_build = |logs: Vec<RawLogInput>| with_graveyard(lease_batch(logs));
    failures.extend(holder_row_failures(
        &graveyard_build,
        vec![
            HolderRow::new(
                "U4 a release to a Graveyard registry owner",
                eth,
                vec![registration(false, true), to_graveyard, release],
                resource,
                LiveGrants::new(),
            )
            .forbid(|event| about(event, GRAVEYARD)),
        ],
    )?);
    // `_register` writes the registry before the numeric `NameRegistered`, and that write
    // materializes the node's surface, so the registration's surface is known.
    let numeric_registration = vec![
        lease_transfer(2, 0, ZERO, HOLDER),
        new_owner_under(node(&["eth"]), 2, 1, "leased", HOLDER),
        numeric_registered(2, 2, HOLDER, 10),
        new_resolver(3, 0, leased(), RESOLVER),
    ];
    failures.extend(holder_row_failures(
        &numeric_batch,
        vec![HolderRow::new(
            "U5 a release after a Sepolia numeric-only registration, which keeps its surface",
            vec![new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT)],
            vec![
                numeric_registration,
                vec![new_owner_under(
                    node(&["eth"]),
                    RELEASED,
                    0,
                    "other",
                    AWAY_OWNER,
                )],
            ],
            registry_only_of(&leased_hex()),
            held_by(HOLDER, Some(RESOLVER)),
        )],
    )?);
    assert_no_failures(failures)
}

/// A registration transaction that also holds writes the registration does not take over.
/// CHILD_OWNER, the node's stale registry owner, hands the record to AWAY_OWNER and takes it back
/// before `register` runs in the same transaction: the BaseRegistrar mints, writes the registry
/// to CHILD_OWNER and emits its numeric `NameRegistered`. The lease takes the writes naming the
/// registrant over, and AWAY_OWNER's grant ends at the write back, so after the block no one holds
/// the resource and a later registry write grants only its owner, in every layout.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
#[test]
fn a_registration_after_writes_it_does_not_take_over_leaves_no_holder() -> anyhow::Result<()> {
    let registration = vec![
        registry_transfer(2, 0, leased(), AWAY_OWNER),
        registry_transfer(2, 1, leased(), CHILD_OWNER),
        lease_transfer(2, 2, ZERO, CHILD_OWNER),
        new_owner_under(node(&["eth"]), 2, 3, "leased", CHILD_OWNER),
        numeric_registered(2, 4, CHILD_OWNER, 4_000_000_000),
    ];
    let failures = holder_row_failures(
        &numeric_batch,
        vec![
            HolderRow::new(
                "registration that takes part of its transaction, then a registry write",
                vec![
                    new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
                    new_owner_under(node(&["eth"]), 1, 1, "leased", CHILD_OWNER),
                ],
                vec![
                    registration,
                    vec![registry_transfer(3, 0, leased(), TRANSFEREE)],
                ],
                registry_only_of(&leased_hex()),
                held_by(TRANSFEREE, None),
            )
            .lookahead(),
        ],
    )?;
    assert_no_failures(failures)
}

/// A registrar controller that registers to itself before it hands the name to its registrant.
const REGISTERING_CONTROLLER: &str = "0x00000000000000000000000000000000000000c7";

/// Where TRANSFEREE's next registry write, to AWAY_OWNER, lands after a hand-on.
#[derive(Clone, Copy)]
enum NextWrite {
    SameTransaction,
    LaterTransaction,
    NextBlock,
}

/// How a hand-on's registration gets its resolver.
#[derive(Clone, Copy, PartialEq)]
enum HandOnResolver {
    /// None: the controller registers straight to HOLDER.
    Zero,
    /// The controller's: it registers to itself, writes the record to HOLDER with the resolver
    /// and transfers the token to HOLDER.
    Controller,
    /// HOLDER's own: a zero-resolver registration, then HOLDER's registry `setResolver` before
    /// the hand-on.
    Own,
}

/// The hand-on transaction at block 2: HOLDER, a contract registrant, registers `leased.eth`,
/// then transfers the token to TRANSFEREE and sets the registry owner to TRANSFEREE. `numeric`
/// emits the registration as the BaseRegistrar's numeric `NameRegistered`, as Sepolia admits
/// it. Otherwise it is the controller's `NameRegistered`, as mainnet admits it. `resolver` sets
/// how the name gets its resolver before the hand-on.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L287-L317 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333-L341 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)
fn handed_on(numeric: bool, resolver: HandOnResolver) -> Vec<RawLogInput> {
    const EXPIRES: u64 = 4_000_000_000;
    let mut logs = Vec::new();
    let mut log_index = 0;
    let mut push = |raw: fn(i64) -> RawLogInput| {
        logs.push(raw(log_index));
        log_index += 1;
    };
    if resolver == HandOnResolver::Controller {
        push(|index| lease_transfer(2, index, ZERO, REGISTERING_CONTROLLER));
        push(|index| new_owner_under(node(&["eth"]), 2, index, "leased", REGISTERING_CONTROLLER));
        if numeric {
            push(|index| numeric_registered(2, index, REGISTERING_CONTROLLER, EXPIRES));
        }
        push(|index| registry_transfer(2, index, leased(), HOLDER));
        push(|index| new_resolver(2, index, leased(), RESOLVER));
        push(|index| lease_transfer(2, index, REGISTERING_CONTROLLER, HOLDER));
        if !numeric {
            push(|index| registered(2, index, HOLDER, EXPIRES));
        }
    } else {
        push(|index| lease_transfer(2, index, ZERO, HOLDER));
        push(|index| new_owner_under(node(&["eth"]), 2, index, "leased", HOLDER));
        if numeric {
            push(|index| numeric_registered(2, index, HOLDER, EXPIRES));
        } else {
            push(|index| registered(2, index, HOLDER, EXPIRES));
        }
        if resolver == HandOnResolver::Own {
            push(|index| new_resolver(2, index, leased(), RESOLVER));
        }
    }
    push(|index| lease_transfer(2, index, HOLDER, TRANSFEREE));
    push(|index| registry_transfer(2, index, leased(), TRANSFEREE));
    logs
}

/// One row of the hand-on table: the hand-on, then TRANSFEREE's registry write to AWAY_OWNER at
/// `next`. AWAY_OWNER ends as the only holder, with `resolver_control` when the name has a
/// resolver, and no row on the resource names HOLDER after the hand-on.
fn hand_on_row(
    row: &'static str,
    numeric: bool,
    resolver: HandOnResolver,
    next: NextWrite,
) -> HolderRow {
    let mut hand_on = handed_on(numeric, resolver);
    let last = hand_on.last().map_or(0, |raw| raw.log_index);
    let steps = match next {
        NextWrite::SameTransaction => {
            hand_on.push(registry_transfer(2, last + 1, leased(), AWAY_OWNER));
            vec![hand_on]
        }
        NextWrite::LaterTransaction => {
            hand_on.push(in_transaction(
                registry_transfer(2, last + 1, leased(), AWAY_OWNER),
                1,
            ));
            vec![hand_on]
        }
        NextWrite::NextBlock => vec![hand_on, vec![registry_transfer(3, 0, leased(), AWAY_OWNER)]],
    };
    HolderRow::new(
        row,
        vec![new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT)],
        steps,
        registry_only_of(&leased_hex()),
        held_by(
            AWAY_OWNER,
            (resolver != HandOnResolver::Zero).then_some(RESOLVER),
        ),
    )
    .forbid(move |event| {
        about(event, HOLDER)
            && (event.block_number, event.transaction_index, event.log_index)
                > (Some(2), Some(0), Some(last))
    })
}

/// Where a Sepolia hand-on's rows about HOLDER miss the registry-only resource, in any layout of
/// `row`: in each scope HOLDER holds, the grant at HOLDER's token transfer and the revoke at
/// TRANSFEREE's registry write must each appear once, both on that resource and in that scope.
/// The scopes are `resource_control`, and `resolver_control` at RESOLVER when HOLDER set it
/// itself. The reconcile keeps each grant, and keeps each revoke as the first pass emitted it.
fn hand_on_pair_failures(row: &HolderRow, resolver: HandOnResolver) -> anyhow::Result<Vec<String>> {
    let hand_on = handed_on(true, resolver);
    let write = hand_on.last().map_or(0, |raw| raw.log_index);
    let transfer = write - 1;
    let resource_scope = serde_json::json!({"kind": "resource"});
    let resolver_scope = serde_json::json!({
        "chain_id": CHAIN,
        "kind": "resolver",
        "resolver_address": RESOLVER.to_ascii_lowercase(),
    });
    let mut scopes = vec![resource_scope];
    if resolver == HandOnResolver::Own {
        scopes.push(resolver_scope);
    }
    let mut failures = Vec::new();
    for (layout, outputs) in every_layout(&numeric_batch, &row.prefix, &row.steps)? {
        for scope in &scopes {
            for (log_index, granted, kept) in [(transfer, true, "grant"), (write, false, "revoke")]
            {
                let resources = outputs
                    .iter()
                    .flat_map(|output| output.normalized_events.iter())
                    .filter(|event| {
                        event.event_kind == "PermissionChanged"
                            && about(event, HOLDER)
                            && event.after_state["scope"] == *scope
                            && (event.block_number, event.transaction_index, event.log_index)
                                == (Some(2), Some(0), Some(log_index))
                            && is_grant(event) == granted
                    })
                    .map(|event| event.resource_id)
                    .collect::<Vec<_>>();
                if resources != vec![Some(row.resource)] {
                    failures.push(format!(
                        "{}, {layout}: the kept {kept} of HOLDER in {scope} at log {log_index} is on {resources:?}, expected only {:?}",
                        row.row, row.resource
                    ));
                }
            }
        }
    }
    Ok(failures)
}

/// A contract registrant can hand its name on in the registration's own transaction, with
/// public calls only: `register` checks no caller, the token's owner may transfer it, and the
/// registry owner may write the record whoever holds the token. The transfer to TRANSFEREE
/// selects the registry-only authority with HOLDER as its holder, and TRANSFEREE's registry
/// write, which the lease takes over, ends HOLDER. TRANSFEREE's next write then leaves its
/// owner as the only holder, wherever it lands.
///
/// On Sepolia the numeric `NameRegistered` reconciles its whole transaction. It drops HOLDER's
/// rows at the registry writes it treats as the registration's setup, but keeps the revoke that
/// ends the transfer's grant to HOLDER, so the reconciled rows hold what the first pass
/// committed and a block replay rebuilds it. Mainnet's controller `NameRegistered` reconciles
/// only the logs before it, and HOLDER's calls come after it. Row 2 of the table is
/// `a_block_replay_after_a_hand_on_keeps_one_holder`.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L247-L260 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L49 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
#[test]
fn a_registration_handed_on_in_its_transaction_leaves_one_holder() -> anyhow::Result<()> {
    use NextWrite::{LaterTransaction, NextBlock, SameTransaction};
    // Sepolia admits the BaseRegistrar's numeric `NameRegistered`, mainnet the controller's.
    let (sepolia, mainnet) = (true, false);
    let rows = [
        (
            sepolia,
            "1 Sepolia, zero resolver, next write in its transaction",
            HandOnResolver::Zero,
            SameTransaction,
        ),
        (
            sepolia,
            "3 Sepolia, zero resolver, next write in the next block",
            HandOnResolver::Zero,
            NextBlock,
        ),
        (
            mainnet,
            "4 mainnet, zero resolver, next write in its transaction",
            HandOnResolver::Zero,
            SameTransaction,
        ),
        (
            mainnet,
            "5 mainnet, zero resolver, next write later in its block",
            HandOnResolver::Zero,
            LaterTransaction,
        ),
        (
            mainnet,
            "6 mainnet, zero resolver, next write in the next block",
            HandOnResolver::Zero,
            NextBlock,
        ),
        (
            sepolia,
            "7 Sepolia, resolver, next write in its transaction",
            HandOnResolver::Controller,
            SameTransaction,
        ),
        (
            sepolia,
            "8 Sepolia, resolver, next write later in its block",
            HandOnResolver::Controller,
            LaterTransaction,
        ),
        (
            sepolia,
            "9 Sepolia, resolver, next write in the next block",
            HandOnResolver::Controller,
            NextBlock,
        ),
        (
            mainnet,
            "10 mainnet, resolver, next write in its transaction",
            HandOnResolver::Controller,
            SameTransaction,
        ),
        (
            mainnet,
            "11 mainnet, resolver, next write later in its block",
            HandOnResolver::Controller,
            LaterTransaction,
        ),
        (
            mainnet,
            "12 mainnet, resolver, next write in the next block",
            HandOnResolver::Controller,
            NextBlock,
        ),
        (
            sepolia,
            "14 Sepolia, own resolver, next write later in its block",
            HandOnResolver::Own,
            LaterTransaction,
        ),
    ];
    let mut failures = Vec::new();
    for (numeric, row, resolver, next) in rows {
        let build: &dyn Fn(Vec<RawLogInput>) -> BatchInput = if numeric {
            &numeric_batch
        } else {
            &lease_batch
        };
        let mut holder_row = hand_on_row(row, numeric, resolver, next);
        if numeric && resolver == HandOnResolver::Zero && matches!(next, NextBlock) {
            holder_row = holder_row.lookahead();
        }
        if numeric && resolver != HandOnResolver::Controller {
            match hand_on_pair_failures(&holder_row, resolver) {
                Ok(found) => failures.extend(found),
                Err(error) => failures.push(format!("{row}: {error:#}")),
            }
        }
        match holder_row_failures(build, vec![holder_row]) {
            Ok(found) => failures.extend(found),
            Err(error) => failures.push(format!("{row}: {error:#}")),
        }
    }
    assert_no_failures(failures)
}

/// Row 2 of the hand-on table: on Sepolia, with a zero resolver, TRANSFEREE gives the record to
/// AWAY_OWNER in a later transaction of the hand-on's block. The first pass grants AWAY_OWNER
/// with no holder left to revoke, and the block replay rebuilds the same single holder from the
/// reconciled rows.
#[test]
fn a_block_replay_after_a_hand_on_keeps_one_holder() -> anyhow::Result<()> {
    let row = "2 Sepolia, zero resolver, next write later in its block";
    let zero = HandOnResolver::Zero;
    let mut failures = hand_on_pair_failures(
        &hand_on_row(row, true, zero, NextWrite::LaterTransaction),
        zero,
    )
    .unwrap_or_else(|error| vec![format!("{row}: {error:#}")]);
    failures.extend(
        holder_row_failures(
            &numeric_batch,
            vec![hand_on_row(row, true, zero, NextWrite::LaterTransaction)],
        )
        .unwrap_or_else(|error| vec![format!("{row}: {error:#}")]),
    );
    assert_no_failures(failures)
}

/// A registrant that registers its lapsed name again directly keeps the registry-only resource
/// beside its new lease. The release hands the resource back to HOLDER, the registry owner, and
/// `_register` burns the old token, mints the new one to HOLDER and writes the registry to
/// HOLDER again. That write names the holder it already has, so it makes no row for the lease to
/// take over and HOLDER keeps `resource_control` and `resolver_control` there.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
#[test]
fn a_registrant_registering_its_lapsed_name_again_keeps_the_resource() -> anyhow::Result<()> {
    let failures = holder_row_failures(
        &numeric_batch,
        vec![
            HolderRow::new(
                "the same registrant registers its lapsed unwrapped name again",
                vec![new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT)],
                vec![
                    vec![
                        lease_transfer(2, 0, ZERO, HOLDER),
                        new_owner_under(node(&["eth"]), 2, 1, "leased", HOLDER),
                        numeric_registered(2, 2, HOLDER, 10),
                        new_resolver(3, 0, leased(), RESOLVER),
                    ],
                    vec![new_owner_under(
                        node(&["eth"]),
                        RELEASED,
                        0,
                        "other",
                        AWAY_OWNER,
                    )],
                    vec![
                        lease_transfer(RELEASED + 1, 0, HOLDER, ZERO),
                        lease_transfer(RELEASED + 1, 1, ZERO, HOLDER),
                        new_owner_under(node(&["eth"]), RELEASED + 1, 2, "leased", HOLDER),
                        numeric_registered(RELEASED + 1, 3, HOLDER, 9_000_000_000),
                    ],
                ],
                registry_only_of(&leased_hex()),
                held_by(HOLDER, Some(RESOLVER)),
            )
            .lookahead(),
        ],
    )?;
    assert_no_failures(failures)
}

/// A restore that finds two subjects with live `resource_control` on one registry-only
/// resource, or `resolver_control` outside its holder, fails rather than pick one.
#[test]
fn a_restore_with_two_live_holders_fails() -> anyhow::Result<()> {
    let head = interpret(unwrapped_child_setup())?;
    let resource = registry_only_of(&child_hex());
    let prior = head
        .normalized_events
        .iter()
        .map(prior_event)
        .collect::<Vec<_>>();
    let grant = |power: &str| {
        prior
            .iter()
            .find(|event| {
                event.event_kind == "PermissionChanged"
                    && event.resource_id == Some(resource)
                    && event.after_state["effective_powers"] == json!([power])
            })
            .cloned()
            .expect("the child's registry write grants its owner")
    };
    let restore = |extra: PriorEventInput| {
        let mut input = batch(vec![new_resolver(3, 0, child(), NEXT_RESOLVER)]);
        input.prior_events = prior.clone();
        input.prior_events.push(extra);
        interpret_test_batch(input)
    };
    for (power, expected) in [
        ("resource_control", "more than one live holder"),
        ("resolver_control", "outside one holder scope"),
    ] {
        let mut second = grant(power);
        second.retained_state_key = second
            .retained_state_key
            .replace(&CHILD_OWNER.to_ascii_lowercase(), PROBE_OWNER);
        second.after_state["subject"] = json!(PROBE_OWNER);
        let error = restore(second).expect_err("a second live holder fails the restore");
        assert!(
            format!("{error:#}").contains(expected),
            "{power}: unexpected error: {error:#}"
        );
    }
    Ok(())
}
