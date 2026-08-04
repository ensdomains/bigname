mod openapi {
    mod responses {
        include!("openapi/responses.rs");
    }

    mod route_operations {
        include!("openapi/route_operations.rs");
    }

    mod schemas {
        include!("openapi/schemas.rs");
    }

    mod server {
        include!("openapi/server.rs");
    }

    pub(super) use server::{OPENAPI_DOCS_HTML, openapi_document, render_openapi_document};
}

use openapi::{OPENAPI_DOCS_HTML, openapi_document, render_openapi_document};
