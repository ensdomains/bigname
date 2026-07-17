use bigname_storage::RawCodeHashCorrectionUpdate;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct AddressCorrectionCensus {
    pub(super) scanned_count: i64,
    pub(super) already_correct_count: i64,
    pub(super) to_correct_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DerivedCodeHash {
    pub(super) code_hash: String,
    pub(super) code_byte_length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CorrectionSampleRow {
    pub(super) raw_code_hash_id: i64,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) contract_address: String,
    pub(super) rederived_code_hash: String,
    pub(super) rederived_code_byte_length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VerifiedCorrectionUpdate {
    pub(super) update: RawCodeHashCorrectionUpdate,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) contract_address: String,
}
