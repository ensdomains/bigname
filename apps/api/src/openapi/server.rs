use serde_json::{Map as JsonMap, json};
use sqlx::types::JsonValue;

use crate::{API_ROUTE_DEFINITIONS, SOFTWARE_VERSION};

use super::schemas::openapi_components;

pub(crate) const OPENAPI_DOCS_HTML: &str = include_str!("docs.html");

pub(crate) fn render_openapi_document() -> String {
    let mut rendered =
        serde_json::to_string_pretty(&openapi_document()).expect("OpenAPI document must render");
    rendered.push('\n');
    rendered
}

pub(crate) fn openapi_document() -> JsonValue {
    let mut paths = JsonMap::new();
    for route in API_ROUTE_DEFINITIONS {
        if let Some(path_item) = route.openapi_path_item() {
            paths.insert(route.path.to_owned(), path_item);
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "bigname API v1",
            "version": SOFTWARE_VERSION,
            "description": "Machine-readable publication of the currently shipped public API surface derived from apps/api/src/main.rs.",
        },
        "paths": JsonValue::Object(paths),
        "components": openapi_components(),
    })
}
