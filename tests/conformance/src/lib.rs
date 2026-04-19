mod shipped_api {
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/api_main.rs"));

    #[cfg(test)]
    mod conformance {
        include!("conformance/harness.rs");

        include!("conformance/helpers.rs");

        include!("conformance/collections.rs");

        include!("conformance/exact_name.rs");

        include!("conformance/resolution_and_permissions.rs");

        include!("conformance/primary_names.rs");

        include!("conformance/history.rs");
    }
}
