//! ENSv1, ENSv2, and Basenames event normalization adapters.

#[cfg(feature = "schema-v2")]
#[allow(dead_code)]
mod evm_abi;
#[cfg(feature = "schema-v2")]
pub mod schema_v2;

#[cfg(feature = "schema-v2")]
pub use schema_v2::{
    AdapterSession as SchemaV2AdapterSession, AddressAdmissionInput,
    BatchInput as SchemaV2BatchInput, BatchOutput as SchemaV2BatchOutput, DiscoveryRuleInput,
    ManifestInput as SchemaV2ManifestInput, NormalizedEvent as SchemaV2NormalizedEvent,
    PriorEventInput, RawLogInput as SchemaV2RawLogInput, interpret_schema_v2_batch,
    interpret_schema_v2_batch_incremental,
};
