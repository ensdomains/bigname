use alloy_primitives::LogData;
use alloy_sol_types::sol;

sol! {
    interface V1Registry {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event Transfer(bytes32 indexed node, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event NewTTL(bytes32 indexed node, uint64 ttl);
    }

    interface V1RegistrarToken {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }

    interface V1LegacyController {
        event NameRegistered(
            string name,
            bytes32 indexed label,
            address indexed owner,
            uint256 cost,
            uint256 expires
        );
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    }

    interface V1WrappedController {
        event NameRegistered(
            string name,
            bytes32 indexed label,
            address indexed owner,
            uint256 baseCost,
            uint256 premium,
            uint256 expires
        );
    }

    interface V1UnwrappedController {
        event NameRegistered(
            string name,
            bytes32 indexed label,
            address indexed owner,
            uint256 baseCost,
            uint256 premium,
            uint256 expires,
            bytes32 referrer
        );
        event NameRenewed(
            string name,
            bytes32 indexed label,
            uint256 cost,
            uint256 expires,
            bytes32 referrer
        );
    }

    interface V1Wrapper {
        event NameWrapped(
            bytes32 indexed node,
            bytes name,
            address owner,
            uint32 fuses,
            uint64 expiry
        );
        event NameUnwrapped(bytes32 indexed node, address owner);
        event FusesSet(bytes32 indexed node, uint32 fuses);
        event ExpiryExtended(bytes32 indexed node, uint64 expiry);
        event TransferSingle(
            address indexed operator,
            address indexed from,
            address indexed to,
            uint256 id,
            uint256 value
        );
    }

    interface V1Resolver {
        event AddrChanged(bytes32 indexed node, address a);
        event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
        event TextChanged(
            bytes32 indexed node,
            string indexed indexedKey,
            string key,
            string value
        );
        event ContenthashChanged(bytes32 indexed node, bytes hash);
        event NameChanged(bytes32 indexed node, string name);
        event VersionChanged(bytes32 indexed node, uint64 newVersion);
    }

    interface V1Reverse {
        event ReverseClaimed(address indexed addr, bytes32 indexed node);
    }

    interface V2Registry {
        event RegistryCreated();
        event LabelRegistered(
            uint256 indexed tokenId,
            bytes32 indexed labelHash,
            string label,
            address owner,
            uint64 expiry,
            address indexed sender
        );
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
        event SubregistryUpdated(
            uint256 indexed tokenId,
            address indexed subregistry,
            address indexed sender
        );
        event ResolverUpdated(
            uint256 indexed tokenId,
            address indexed resolver,
            address indexed sender
        );
        event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
        event TransferSingle(
            address indexed operator,
            address indexed from,
            address indexed to,
            uint256 id,
            uint256 value
        );
        event EACRolesChanged(
            uint256 indexed resource,
            address indexed account,
            uint256 oldRoleBitmap,
            uint256 newRoleBitmap
        );
        event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
        event ParentUpdated(address indexed parent, string label, address indexed sender);
        event Upgraded(address indexed implementation);
    }

    interface V2Registrar {
        event NameRegistered(
            uint256 indexed tokenId,
            string label,
            address owner,
            address subregistry,
            address resolver,
            uint64 duration,
            address paymentToken,
            bytes32 indexed referrer,
            uint256 base,
            uint256 premium
        );
        event NameRenewed(
            uint256 indexed tokenId,
            string label,
            uint64 duration,
            uint64 newExpiry,
            address paymentToken,
            bytes32 indexed referrer,
            uint256 amount
        );
    }

    interface V2Resolver {
        event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
        event TextChanged(
            bytes32 indexed node,
            string indexed indexedKey,
            string key,
            string value
        );
        event ContenthashChanged(bytes32 indexed node, bytes hash);
        event NameChanged(bytes32 indexed node, string name);
        event VersionChanged(bytes32 indexed node, uint64 newVersion);
    }
}

pub fn encoded_topics(encoded: &LogData) -> Vec<String> {
    encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect()
}
