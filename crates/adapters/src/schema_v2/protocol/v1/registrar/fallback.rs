use super::*;

mod registrar_lifecycle {
    use super::*;
    sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NameRenewed(uint256 indexed id, uint256 expires);
        event ControllerAdded(address indexed controller);
        event ControllerRemoved(address indexed controller);
    }
}

pub(super) fn decode_registrar_controller(
    raw: &RawLogInput,
    approved: bool,
) -> anyhow::Result<String> {
    Ok(if approved {
        address_hex(
            decode_event_log::<registrar_lifecycle::ControllerAdded>(
                &raw.topics,
                &raw.data,
                "registrar ControllerAdded log is malformed",
            )?
            .controller,
        )
    } else {
        address_hex(
            decode_event_log::<registrar_lifecycle::ControllerRemoved>(
                &raw.topics,
                &raw.data,
                "registrar ControllerRemoved log is malformed",
            )?
            .controller,
        )
    })
}

pub(super) fn decode_registrar_lifecycle(
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<(B256, Option<String>, Value)> {
    if selected.event.signature == "NameRegistered(uint256,address,uint256)" {
        let event = decode_event_log::<registrar_lifecycle::NameRegistered>(
            &raw.topics,
            &raw.data,
            "registrar NameRegistered log is malformed",
        )?;
        let owner = address_hex(event.owner);
        return Ok((
            B256::from(event.id.to_be_bytes::<32>()),
            Some(owner.clone()),
            json!({
                "source_event": "NameRegistered",
                "registrant": owner,
                "expiry": u64::try_from(event.expires).ok(),
            }),
        ));
    }
    let event = decode_event_log::<registrar_lifecycle::NameRenewed>(
        &raw.topics,
        &raw.data,
        "registrar NameRenewed log is malformed",
    )?;
    Ok((
        B256::from(event.id.to_be_bytes::<32>()),
        None,
        json!({
            "source_event": "NameRenewed",
            "expiry": u64::try_from(event.expires).ok(),
        }),
    ))
}

/// The fallback source: a registrar `NameRegistered` / `NameRenewed` still held
/// when its block ended, so no admitted controller event carried the label. The
/// fact is derived from the registrar's own payload, without a label, and flagged.
pub(in crate::schema_v2::protocol) fn interpret_held(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let (labelhash, _, mut after) = decode_registrar_lifecycle(selected, raw)?;
    let namehash = registrar_namehash(selected, labelhash);
    let registration = selected.event.signature == "NameRegistered(uint256,address,uint256)";
    // A renewal refreshes a registration this family already holds; it does not
    // invent one. Registrar identity for a wrapped name waits for its unwrap, and
    // a name with no registrar state has no expiry to release spuriously.
    if !registration
        && state
            .v1_registrar(&selected.source.namespace, &namehash)
            .is_none()
    {
        return Ok(Interpreted::new());
    }
    // The fact's expiry is `i64` like every other registrar expiry; a value past it
    // reads as never expiring, exactly as the controller path decodes it.
    after["expiry"] = json!(after.get("expiry").and_then(Value::as_i64));
    after["controller_admitted"] = Value::Bool(false);
    let mut output = name_fact(selected, raw, state, None, labelhash, after, registration)?;
    // Without a label there may be no name surface to link, and the events
    // reference one by identity; the resource carries the fact instead.
    if !state.v1_surface_materialized(&selected.source.namespace, &namehash) {
        for event in &mut output.events {
            event.logical_name_id = None;
        }
    }
    Ok(output)
}
