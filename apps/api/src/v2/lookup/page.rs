#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReverseLookupPage {
    pub(super) entries: Vec<bigname_storage::ReverseIdentityRecordRow>,
    pub(super) next_cursor: Option<String>,
    pub(super) total_count: Option<u64>,
    pub(super) has_more: bool,
}
