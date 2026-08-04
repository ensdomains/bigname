#[path = "support/cursors.rs"]
mod support_cursors;
#[path = "support/service.rs"]
mod support_service;

use support_cursors::*;
use support_service::*;
use v2::support::*;

#[cfg(test)]
use v2::support::primary_name_lookup as support_primary_name_lookup;
