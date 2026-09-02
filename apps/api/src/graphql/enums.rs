use async_graphql::Enum;

/// Subgraph `Domain_orderBy`. The underscore + lowercase-`o` type name and the lowercase-camel
/// value names are set explicitly rather than relying on async-graphql's default
/// SCREAMING_SNAKE_CASE rename.
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "Domain_orderBy")]
pub(crate) enum DomainOrderBy {
    #[graphql(name = "createdAt")]
    CreatedAt,
    #[graphql(name = "expiryDate")]
    ExpiryDate,
    /// Entity ID order uses the namehash bytes rendered as lowercase hexadecimal text.
    #[graphql(name = "id")]
    Id,
    #[graphql(name = "name")]
    Name,
    /// Degenerate on Sepolia v2 — no producer writes `registration_date`, so the column is NULL.
    #[graphql(name = "registrationDate")]
    RegistrationDate,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "OrderDirection")]
pub(crate) enum OrderDirection {
    #[graphql(name = "asc")]
    Asc,
    #[graphql(name = "desc")]
    Desc,
}

/// Policy argument reserved for per-entity indexing-error behavior.
#[derive(Enum, Copy, Clone, Default, Eq, PartialEq)]
#[graphql(name = "_SubgraphErrorPolicy_")]
pub(crate) enum SubgraphErrorPolicy {
    #[graphql(name = "allow")]
    Allow,
    #[graphql(name = "deny")]
    #[default]
    Deny,
}
