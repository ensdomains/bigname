// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

interface Registry {
    function setOwner(bytes32 node, address owner) external;
}

// The real controller invokes this resolver after assigning registry ownership,
// before its final registrar token transfer and NameRegistered event.
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L301-L317 @ ens_v1@91c966f)
contract CallbackResolver {
    Registry immutable registry;
    address immutable recipient;

    constructor(Registry registry_, address recipient_) {
        registry = registry_;
        recipient = recipient_;
    }

    function multicallWithNodeCheck(bytes32 node, bytes[] calldata)
        external returns (bytes[] memory results)
    {
        registry.setOwner(node, recipient);
        return new bytes[](0);
    }
}
