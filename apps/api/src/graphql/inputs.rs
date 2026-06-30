use async_graphql::InputObject;

/// Subgraph `DomainFilter`. Field names that are snake_case in the subgraph schema (`owner_in`,
/// `name_contains`) are pinned explicitly; async-graphql would otherwise camelCase them. Only
/// `owner`, `owner_in`, `name`, `name_contains`, and `isMigrated` affect storage filters; the rest
/// are declared for compatibility with subgraph-shaped variables.
#[derive(InputObject, Default)]
#[graphql(name = "DomainFilter")]
pub(crate) struct DomainFilter {
    pub(crate) id: Option<String>,
    pub(crate) owner: Option<String>,
    #[graphql(name = "owner_in")]
    pub(crate) owner_in: Option<Vec<String>>,
    pub(crate) name: Option<String>,
    #[graphql(name = "name_contains")]
    pub(crate) name_contains: Option<String>,
    #[graphql(name = "isMigrated")]
    pub(crate) is_migrated: Option<bool>,
}

/// Subgraph `RegistrationFilter`.
#[derive(InputObject, Default)]
#[graphql(name = "RegistrationFilter")]
pub(crate) struct RegistrationFilter {
    pub(crate) registrant: Option<String>,
    #[graphql(name = "registrant_in")]
    pub(crate) registrant_in: Option<Vec<String>>,
}
