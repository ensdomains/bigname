//! The `include` selector of the resolver overview.

use crate::v2::{V2Error, V2Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolverOverviewInclude {
    nodes: bool,
    aliases: bool,
    links: bool,
    roles: bool,
    events: bool,
}

impl ResolverOverviewInclude {
    pub(super) fn all() -> Self {
        Self {
            nodes: true,
            aliases: true,
            links: true,
            roles: true,
            events: true,
        }
    }

    pub(super) fn empty() -> Self {
        Self {
            nodes: false,
            aliases: false,
            links: false,
            roles: false,
            events: false,
        }
    }

    pub(super) fn requests(self, section: &str) -> bool {
        match section {
            "nodes" => self.nodes,
            "aliases" => self.aliases,
            "links" => self.links,
            "roles" => self.roles,
            "events" => self.events,
            _ => false,
        }
    }
}

pub(crate) fn resolver_overview_include(include: &[String]) -> V2Result<ResolverOverviewInclude> {
    let mut parsed = ResolverOverviewInclude::empty();
    let mut saw_value = false;

    for value in include {
        saw_value = true;
        match value.as_str() {
            "nodes" => parsed.nodes = true,
            "aliases" => parsed.aliases = true,
            "links" => parsed.links = true,
            "roles" => parsed.roles = true,
            "events" => parsed.events = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only nodes, aliases, links, roles, or events",
                ));
            }
        }
    }

    Ok(if saw_value {
        parsed
    } else {
        ResolverOverviewInclude::all()
    })
}
