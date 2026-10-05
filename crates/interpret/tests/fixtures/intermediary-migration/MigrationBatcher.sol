// SPDX-License-Identifier: MIT
pragma solidity 0.8.25;
interface Registrar {
    function transferFrom(address from, address to, uint256 id) external;
    function safeTransferFrom(address from, address to, uint256 id, bytes calldata data) external;
}
contract MigrationBatcher {
    function migrate(address registrar, address controller, uint256 id, bytes calldata data) external {
        Registrar(registrar).transferFrom(msg.sender, address(this), id);
        Registrar(registrar).safeTransferFrom(address(this), controller, id, data);
    }
}
