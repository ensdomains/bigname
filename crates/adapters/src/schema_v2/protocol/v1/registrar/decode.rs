use alloy_primitives::B256;
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use crate::{
    evm_abi::{address_hex, decode_event_log_data_as, hex_string},
    schema_v2::{catalog::Selected, model::RawLogInput},
};

mod simple {
    use super::*;
    sol! {
        event RawNameRegistered(bytes name, bytes32 indexed label, address indexed owner, uint256 expires);
        event RawNameRenewed(bytes name, bytes32 indexed label, uint256 expires);
    }
}

mod cost {
    use super::*;
    sol! {
        event RawNameRegistered(bytes name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
        event RawNameRenewed(bytes name, bytes32 indexed label, uint256 cost, uint256 expires);
    }
}

mod premium {
    use super::*;
    sol! {
        event RawNameRegistered(bytes name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires);
    }
}

mod premium_referrer {
    use super::*;
    sol! {
        event RawNameRegistered(bytes name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires, bytes32 referrer);
    }
}

mod renew_referrer {
    use super::*;
    sol! { event RawNameRenewed(bytes name, bytes32 indexed label, uint256 cost, uint256 expires, bytes32 referrer); }
}

pub(super) fn name(
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<(Vec<u8>, B256, Value)> {
    match selected.event.signature.as_str() {
        "NameRegistered(string,bytes32,address,uint256)" => {
            let event = decode_event_log_data_as::<simple::RawNameRegistered>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?}),
            ))
        }
        "NameRegistered(string,bytes32,address,uint256,uint256)" => {
            let event = decode_event_log_data_as::<cost::RawNameRegistered>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"cost":event.cost.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?}),
            ))
        }
        "NameRegistered(string,bytes32,address,uint256,uint256,uint256)" => {
            let event = decode_event_log_data_as::<premium::RawNameRegistered>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"base_cost":event.baseCost.to_string(),"premium":event.premium.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?}),
            ))
        }
        "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)" => {
            let event = decode_event_log_data_as::<premium_referrer::RawNameRegistered>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"base_cost":event.baseCost.to_string(),"premium":event.premium.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?,"referrer":hex_string(event.referrer)}),
            ))
        }
        "NameRenewed(string,bytes32,uint256)" => {
            let event = decode_event_log_data_as::<simple::RawNameRenewed>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRenewed log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRenewed","expiry":crate::evm_abi::u256_i64(event.expires, "NameRenewed expiry")?}),
            ))
        }
        "NameRenewed(string,bytes32,uint256,uint256)" => {
            let event = decode_event_log_data_as::<cost::RawNameRenewed>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRenewed log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRenewed","cost":event.cost.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRenewed expiry")?}),
            ))
        }
        "NameRenewed(string,bytes32,uint256,uint256,bytes32)" => {
            let event = decode_event_log_data_as::<renew_referrer::RawNameRenewed>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameRenewed log is malformed",
            )?;
            Ok((
                event.name.to_vec(),
                event.label,
                json!({"source_event":"NameRenewed","cost":event.cost.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRenewed expiry")?,"referrer":hex_string(event.referrer)}),
            ))
        }
        signature => bail!("unsupported registrar ABI event {signature}"),
    }
}
