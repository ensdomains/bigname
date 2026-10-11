//! A PermissionedResolver proxy at `LINKED` and its record-ID setters, as the real resolver
//! initializes and writes them.
use super::*;

pub(super) const LINKED: &str = "0x0000000000000000000000000000000000025910";
pub(super) const IMPLEMENTATION: &str = "0x115eb53f0c60696633855f90b138178fb40b2b2c";
pub(super) const FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
sol! {
    event ResolverCreated();
    event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name);
    event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value);
    event AddressUpdated(uint256 indexed recordId, uint256 coinType, bytes addressBytes);
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}

/// The proxy's deployment through the factory. Its target is `FACTORY`. The proxy initializes
/// the root setter and link grants of `owner` before use.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L119-L132 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L10-L58 @ ens_v2_sepolia_20261001@07e55a05)
pub(super) fn deploy_linked(owner: Address) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let resolver: Address = LINKED.parse()?;
    let implementation: Address = IMPLEMENTATION.parse()?;
    Ok(vec![
        (resolver, Upgraded { implementation }.encode_log_data()),
        (resolver, ResolverCreated {}.encode_log_data()),
        (
            resolver,
            EACRolesChanged {
                resource: U256::ZERO,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: [0, 4, 28, 128, 132, 156]
                    .into_iter()
                    .fold(U256::ZERO, |roles, bit| roles | (U256::from(1) << bit)),
            }
            .encode_log_data(),
        ),
        (
            FACTORY.parse()?,
            ProxyDeployed {
                sender: owner,
                proxyAddress: resolver,
                salt: U256::from(25910),
                implementation,
            }
            .encode_log_data(),
        ),
    ])
}

/// Links record `id` to `name`. The empty name is the resolver-wide default.
pub(super) fn link(name: &str, id: u64) -> Result<(Address, alloy_primitives::LogData)> {
    let dns = if name.is_empty() {
        vec![0]
    } else {
        bigname_domain::normalization::normalize_name(name)?.dns_encoded_name
    };
    Ok((
        LINKED.parse()?,
        Linked {
            recordId: U256::from(id),
            node: bigname_lookup::ens_namehash_hex(name)?.parse()?,
            name: dns.into(),
        }
        .encode_log_data(),
    ))
}

pub(super) fn text(
    id: u64,
    key: &str,
    value: &str,
) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        LINKED.parse()?,
        TextUpdated {
            recordId: U256::from(id),
            keyHash: keccak256(key),
            key: key.into(),
            value: value.into(),
        }
        .encode_log_data(),
    ))
}

pub(super) fn address_bytes(
    id: u64,
    coin: u64,
    value: Vec<u8>,
) -> Result<(Address, alloy_primitives::LogData)> {
    Ok((
        LINKED.parse()?,
        AddressUpdated {
            recordId: U256::from(id),
            coinType: U256::from(coin),
            addressBytes: value.into(),
        }
        .encode_log_data(),
    ))
}

pub(super) fn address(id: u64, value: &str) -> Result<(Address, alloy_primitives::LogData)> {
    address_bytes(id, 60, value.parse::<Address>()?.to_vec())
}
