# Offline OpenAPI validation schema

`openapi-3.1-2025-09-15.json` is an unchanged copy of the OpenAPI Initiative's
[OpenAPI 3.1 schema dated 2025-09-15](https://spec.openapis.org/oas/3.1/schema/2025-09-15).
It was retrieved on 2026-09-30 and is distributed under the accompanying
Apache 2.0 `LICENSE` from the
[OpenAPI Specification repository](https://github.com/OAI/OpenAPI-Specification/blob/main/LICENSE).

SHA-256: `d0a3955182364c7b5fdebfd0583ecad259a870b4a2fe86a1b0fe8785f8224fed`.

This resource validates OpenAPI document structure. Its Schema Object permits
JSON Schema keywords without validating those keywords itself. API tests also
validate component schemas against the JSON Schema 2020-12 meta-schema and
compile their references with the pinned Rust validator. Validation resolves
local resources only; it never fetches schemas over the network.
