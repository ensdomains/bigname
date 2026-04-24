use anyhow::{Result, bail};

use crate::{
    BASENAMES_EXECUTION_SOURCE_FAMILY, BASENAMES_NAMESPACE, ENS_EXECUTION_SOURCE_FAMILY,
    ENS_NAMESPACE,
};

pub(crate) fn verified_primary_context_label(namespace: &str) -> Result<&'static str> {
    match namespace {
        ENS_NAMESPACE => Ok("ENS verified-primary"),
        BASENAMES_NAMESPACE => Ok("Basenames verified-primary"),
        other => bail!("verified-primary namespace {other} is unsupported"),
    }
}

pub(super) fn verified_primary_execution_source_family(namespace: &str) -> Result<&'static str> {
    match namespace {
        ENS_NAMESPACE => Ok(ENS_EXECUTION_SOURCE_FAMILY),
        BASENAMES_NAMESPACE => Ok(BASENAMES_EXECUTION_SOURCE_FAMILY),
        other => bail!("verified-primary namespace {other} is unsupported"),
    }
}
