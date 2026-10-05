#!/usr/bin/env python3
"""Exercise the source-to-artifact path without network, Cargo, or third-party packages."""

import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from openapi.generator import Generator, main  # noqa: E402
from openapi.markdown import ContractError  # noqa: E402


API = """# Test API
<!-- openapi:enum ErrorCode -->
| Code | HTTP | Meaning |
| --- | --- | --- |
| `invalid_input` | 400 | Bad input. |
| `stale` | 409 | Read changed. |
| `conflict` | 409 | No selected snapshot. |
<!-- openapi:enum Status -->
| Value |
| --- |
| `ok` |
| `unsupported` |
## Objects
### Envelope
Common [metadata](#objects).
<!-- openapi:object Envelope -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `meta` | map of string to string | always | Metadata. |
### Response
Extends Envelope.
<!-- openapi:object Response -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object Record | always | The record. |
### Record
<!-- openapi:object Record -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | Name. |
| `status` | enum Status | always | Result. |
| `reason` | nullable string | when unsupported | Why. |
| `labels` | array of string | optional | Labels, separated by `\\|`. |
### ErrorEnvelope
<!-- openapi:object ErrorEnvelope -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `error` | object ErrorBody | always | Error. |
### ErrorBody
<!-- openapi:object ErrorBody -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `code` | enum ErrorCode | always | Code. |
### Presence conditions
<!-- openapi:conditions -->
| Condition | Holds when |
| --- | --- |
| unsupported | The `status` is `unsupported`. |
"""

ROUTES = """# Routes
<!-- openapi:headers -->
| Header | Type | Description |
| --- | --- | --- |
| `ETag` | string | Validator. |
### `GET /v1/names/{name}`
<!-- openapi:parameters GET /v1/names/{name} -->
| Parameter | In | Type | Required | Default | Description |
| --- | --- | --- | --- | --- | --- |
| `name` | path | string | yes | none | Name. |
| `include` | query | array of enum `counts`, `roles` | no | `counts,roles` | Expansions. |
| `status` | query | enum Status | no | `ok` | Status. |
| `page_size` | query | integer | no | `100` | Page size. |
| `active` | query | boolean | no | `false` | Active. |
<!-- openapi:responses GET /v1/names/{name} -->
| Status | Body | Code | Headers | When |
| --- | --- | --- | --- | --- |
| 200 | object Response | none | `ETag` | Available. |
| 304 | none | none | `ETag` | Unchanged. |
| 400 | object ErrorEnvelope | `invalid_input` | none | Bad input. |
| 409 | object ErrorEnvelope | `stale` | none | Publication changed. |
| 409 | object ErrorEnvelope | `conflict` | none | Conflict. |
"""


class GenerationTests(unittest.TestCase):
    def generate(self, api=API, routes=ROUTES):
        return Generator(api, routes).generate()

    def reject(self, api=API, routes=ROUTES, match=None):
        with self.assertRaisesRegex(ContractError, match or ".+"):
            self.generate(api, routes)

    def test_complete_operation_and_flattened_closed_schema(self):
        doc = self.generate()
        self.assertEqual(doc["openapi"], "3.1.0")
        self.assertEqual(doc["info"]["version"], "__BIGNAME_VERSION__")
        self.assertEqual(doc["info"]["x-build-sha"], "__BIGNAME_BUILD_SHA__")
        schema = doc["components"]["schemas"]["Response"]
        self.assertEqual(list(schema["properties"]), ["meta", "data"])
        self.assertEqual(schema["required"], ["meta", "data"])
        self.assertFalse(schema["additionalProperties"])
        self.assertNotIn("allOf", schema)
        operation = doc["paths"]["/v1/names/{name}"]["get"]
        self.assertEqual(operation["operationId"], "get_v1_names_name")
        self.assertEqual(operation["tags"], ["names"])
        self.assertIn("#get-v1namesname", operation["description"])
        self.assertEqual(operation["parameters"][1]["schema"]["default"], ["counts", "roles"])
        self.assertFalse(operation["parameters"][1]["explode"])
        self.assertIn("`stale`", operation["responses"]["409"]["description"])
        self.assertIn("`conflict`", operation["responses"]["409"]["description"])
        self.assertNotIn("content", operation["responses"]["304"])
        prop = doc["components"]["schemas"]["Record"]["properties"]
        self.assertEqual(prop["reason"]["type"], ["string", "null"])
        self.assertEqual(prop["reason"]["x-presence"], "when unsupported")
        self.assertIn("The `status` is `unsupported`", prop["reason"]["description"])
        self.assertIn("`|`", prop["labels"]["description"])

    def test_repeated_query_array_uses_form_explode_and_bounds(self):
        row = "| `window` | query | repeated array [1, 32] of string | no | none | Windows. |\n"
        routes = ROUTES.replace("<!-- openapi:responses", row + "<!-- openapi:responses")
        generator = Generator(API, routes)
        parameters = generator.generate()["paths"]["/v1/names/{name}"]["get"]["parameters"]
        parameter = next(p for p in parameters if p["name"] == "window")
        self.assertEqual(parameter["style"], "form")
        self.assertTrue(parameter["explode"])
        self.assertEqual(parameter["schema"], {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 32})
        self.assertFalse(next(p for p in parameters if p["name"] == "include")["explode"])
        for length in (1, 32):
            generator.validate(["1..2"] * length, parameter["schema"], "window")
        for length in (0, 33):
            with self.assertRaisesRegex(ContractError, "window.*(minItems|maxItems)"):
                generator.validate(["1..2"] * length, parameter["schema"], "window")
        for replacement in ("repeated string", "repeated array [32, 1] of string", "repeated array of array of string"):
            self.reject(routes=routes.replace("repeated array [1, 32] of string", replacement))
        for location in ("header", "path", "body"):
            self.reject(routes=routes.replace("`window` | query", f"`window` | {location}"), match="repeated is only valid for query arrays")
        self.reject(api=API.replace("array of string | optional", "repeated array of string | optional"))

    def test_link_resolution(self):
        doc = Generator(API, ROUTES, "https://docs.example.test/reference/").generate()
        self.assertIn("https://docs.example.test/reference/api-v1.md#objects", doc["components"]["schemas"]["Envelope"]["description"])

    def test_bounded_integers_validate_parameters_and_defaults(self):
        routes = ROUTES.replace("| integer | no | `100`", "| integer [1, 200] | no | `100`")
        generator = Generator(API, routes)
        params = generator.generate()["paths"]["/v1/names/{name}"]["get"]["parameters"]
        schema = next(p["schema"] for p in params if p["name"] == "page_size")
        self.assertEqual(schema, {"type": "integer", "minimum": 1, "maximum": 200, "default": 100})
        for value in (1, 200):
            generator.validate(value, schema, "page_size")
            self.generate(routes=routes.replace("`100`", f"`{value}`"))
        for value in (0, 201):
            with self.assertRaisesRegex(ContractError, "page_size.*(minimum|maximum)"):
                generator.validate(value, schema, "page_size")
            self.reject(routes=routes.replace("`100`", f"`{value}`"), match="default of page_size.*(minimum|maximum)")
        for expr in ("integer [200, 1]", "integer [1.5, 200]", "integer [01, 200]", "integer [1,]", "string [1, 200]"):
            self.reject(routes=routes.replace("integer [1, 200]", expr))

    def test_bounded_integer_object_examples_and_nullable_values(self):
        api = API.replace("| `name` | string", "| `name` | nullable integer [-2, 2]")
        for value in (None, -2, 2):
            example = {"name": value, "status": "ok"}
            doc = self.generate(api=api + "\n```json openapi-example Record\n" + json.dumps(example) + "\n```\n")
            schema = doc["components"]["schemas"]["Record"]["properties"]["name"]
            self.assertEqual((schema["type"], schema["minimum"], schema["maximum"]), (["integer", "null"], -2, 2))
        for value in (-3, 3):
            example = {"name": value, "status": "ok"}
            self.reject(api=api + "\n```json openapi-example Record\n" + json.dumps(example) + "\n```\n", match="(minimum|maximum)")

    def test_bounded_arrays_keep_query_serialization_and_validate_defaults(self):
        routes = ROUTES.replace("array of enum", "array [1, 2] of enum")
        generator = Generator(API, routes)
        param = generator.generate()["paths"]["/v1/names/{name}"]["get"]["parameters"][1]
        self.assertEqual((param["style"], param["explode"]), ("form", False))
        schema = param["schema"]
        self.assertEqual((schema["minItems"], schema["maxItems"]), (1, 2))
        for value in (["counts"], ["counts", "roles"]):
            generator.validate(value, schema, "include")
        for value in ([], ["counts", "roles", "counts"]):
            with self.assertRaisesRegex(ContractError, "include.*(minItems|maxItems)"):
                generator.validate(value, schema, "include")
        self.reject(routes=routes.replace("`counts,roles`", "`counts,roles,counts`"), match="default of include.*maxItems")
        self.reject(routes=routes.replace("array [1, 2]", "array [2, 2]").replace("`counts,roles`", "`counts`"), match="default of include.*minItems")
        for expr in ("array [2, 1]", "array [-1, 2]", "array [0, 2.5]", "array [00, 2]", "array [0,]"):
            self.reject(routes=routes.replace("array [1, 2]", expr))

    def test_bounded_array_object_examples_and_nullable_values(self):
        api = API.replace("array of string", "nullable array [0, 2] of integer [1, 200]")
        for value in (None, [], [1], [1, 200]):
            example = {"name": "alice.eth", "status": "ok", "labels": value}
            doc = self.generate(api=api + "\n```json openapi-example Record\n" + json.dumps(example) + "\n```\n")
            schema = doc["components"]["schemas"]["Record"]["properties"]["labels"]
            self.assertEqual((schema["type"], schema["minItems"], schema["maxItems"]), (["array", "null"], 0, 2))
        for value in ([1, 2, 3], [201]):
            example = {"name": "alice.eth", "status": "ok", "labels": value}
            self.reject(api=api + "\n```json openapi-example Record\n" + json.dumps(example) + "\n```\n", match="(maxItems|maximum)")

    def test_error_responses_restrict_codes_and_keep_the_envelope_closed(self):
        generator = Generator(API, ROUTES)
        responses = generator.generate()["paths"]["/v1/names/{name}"]["get"]["responses"]
        for status, allowed, rejected in [("400", ["invalid_input"], ["stale", "conflict"]),
                                          ("409", ["stale", "conflict"], ["invalid_input"])]:
            schema = responses[status]["content"]["application/json"]["schema"]
            for code in allowed:
                generator.validate({"error": {"code": code}}, schema, status)
            for code in rejected:
                with self.assertRaisesRegex(ContractError, "enum"):
                    generator.validate({"error": {"code": code}}, schema, status)
            with self.assertRaisesRegex(ContractError, "undeclared field"):
                generator.validate({"error": {"code": allowed[0], "extra": True}}, schema, status)

    def test_body_parameter_is_request_body(self):
        routes = ROUTES.replace("GET ", "POST ").replace(
            "| `active` | query | boolean | no | `false` | Active. |",
            "| `body` | body | object Record | yes | none | Input. |",
        )
        operation = self.generate(routes=routes)["paths"]["/v1/names/{name}"]["post"]
        self.assertEqual(operation["requestBody"]["content"]["application/json"]["schema"]["$ref"], "#/components/schemas/Record")
        self.assertNotIn("body", [p["in"] for p in operation["parameters"]])
        self.reject(routes=routes.replace("POST ", "GET "))

    def test_empty_parameter_table(self):
        routes = ROUTES.replace("/{name}", "")
        routes = "\n".join(line for line in routes.splitlines() if not line.startswith(("| `name` | path", "| `include`", "| `status`", "| `page_size`", "| `active`")))
        operation = self.generate(routes=routes)["paths"]["/v1/names"]["get"]
        self.assertEqual(operation["parameters"], [])

    def test_duplicate_unknown_and_malformed_markers(self):
        for source in (
            API + API,
            API.replace("openapi:object Record", "openapi:wat Record"),
            API.replace("<!-- openapi:object Record -->", "<!-- openapi:object Record --> trailing"),
            API.replace("<!-- openapi:object Record -->", "<!-- openapi:object Record -->\n"),
            API + "\n<!-- openapi object Omitted -->\n",
        ):
            with self.subTest(source=source[-150:]):
                self.reject(api=source)

    def test_unmarked_and_nested_markers_are_ignored(self):
        extras = """
```markdown
<!-- openapi:unknown Bogus -->
```
> <!-- openapi:object Bogus -->
  <!-- openapi:object Bogus -->
| Unrelated | Table |
| --- | --- |
| arbitrary | prose |
"""
        self.assertEqual(self.generate(), self.generate(api=API + extras))

    def test_bad_columns_cells_and_object_location(self):
        for source in (
            API.replace("| Field | Type | Presence | Description |", "| Field | Type | Description | Presence |", 1),
            API.replace("| `name` | string | always | Name. |", "| `name` | string | always | |"),
            API.replace("| `name` | string | always | Name. |", "| `name` | string | always | Name. | extra |"),
            API.replace("### Record", "### Other"),
            API.replace("## Objects", "## Other"),
        ):
            self.reject(api=source)

    def test_unknown_and_unreferenced_definitions(self):
        self.reject(api=API.replace("object Record |", "object Missing |"))
        self.reject(api=API.replace("enum Status | always", "enum Missing | always"))
        self.reject(api=API.replace("when unsupported", "when unknown"))
        self.reject(api=API.replace("when unsupported", "optional"), match="unreferenced condition")
        self.reject(api=API + "\n<!-- openapi:enum Unused -->\n| Value |\n| --- |\n| `x` |\n", match="unreferenced enum")

    def test_duplicate_and_cyclic_inheritance(self):
        self.reject(api=API.replace("| `data` | object Record", "| `meta` | object Record"), match="duplicate field")
        self.reject(api=API.replace("### Envelope\n", "### Envelope\nExtends Response.\n"), match="misplaced Extends")
        self.reject(api=API.replace("<!-- openapi:object Envelope -->", "Extends Response.\n<!-- openapi:object Envelope -->"), match="cycle")
        self.reject(api=API.replace("Extends Envelope.", "Extends Missing."), match="unknown parent")

    def test_types_and_enum_literals_are_strict(self):
        for expr in ("number", "nullable nullable string", "enum ok", "enum `ok`, `ok`", "one of object Record", "array string", "map of integer to string"):
            self.reject(api=API.replace("| `name` | string", f"| `name` | {expr}"))
        self.reject(api=API.replace("| `ok` |", "| ok |"))
        self.reject(api=API.replace("| `ok` |", "| `ok` |\n| `ok` |"))

    def test_one_of_requires_exclusive_alternatives(self):
        extra = """
### Named
<!-- openapi:object Named -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | Name. |
### Addressed
<!-- openapi:object Addressed -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `address` | string | always | Address. |
"""
        api = API.replace("| `name` | string | always", "| `name` | one of string, object Named, object Addressed | always") + extra
        self.generate(api=api)
        self.reject(api=api.replace("| `address` |", "| `name` |"), match="overlap")

    def test_one_of_discriminators(self):
        extra = """
### Named
<!-- openapi:object Named -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `name` | always | Kind. |
### Addressed
<!-- openapi:object Addressed -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `address` | always | Kind. |
"""
        api = API.replace("| `name` | string | always", "| `name` | one of object Named, object Addressed | always") + extra
        self.generate(api=api)
        self.reject(api=api.replace("enum `address`", "enum `name`"), match="overlap")

    def test_pairing_parameters_and_defaults(self):
        for routes in (
            ROUTES.replace("<!-- openapi:responses GET /v1/names/{name} -->", ""),
            ROUTES.replace("| `name` | path", "| `other` | path"),
            ROUTES.replace("| yes | none | Name.", "| no | none | Name."),
            ROUTES.replace("| yes | none | Name.", "| yes | `alice` | Name."),
            ROUTES.replace("| no | `100`", "| maybe | `100`"),
            ROUTES.replace("| no | `100`", "| no | `1.5`"),
            ROUTES.replace("| no | `false`", "| no | `FALSE`"),
            ROUTES.replace("| no | `ok`", "| no | `unknown`"),
            ROUTES.replace("| no | `counts,roles`", "| no | `counts,other`"),
            ROUTES.replace("| `status` | query | enum Status", "| `status` | query | object Record"),
        ):
            self.reject(routes=routes)

    def test_response_errors_headers_and_success(self):
        for routes in (
            ROUTES.replace("| 400 |", "| 404 |"),
            ROUTES.replace("`invalid_input`", "`unknown`"),
            ROUTES.replace("`ETag` | Available", "`Unknown` | Available"),
            ROUTES.replace("| 409 | object ErrorEnvelope | `conflict`", "| 409 | object Record | `conflict`"),
            ROUTES.replace("| 409 | object ErrorEnvelope | `conflict`", "| 409 | object ErrorEnvelope | `stale`"),
            ROUTES.replace("| 200 | object Response", "| 200 | object Record"),
            ROUTES.replace("| 304 | none", "| 304 | object Response"),
            "\n".join(line for line in ROUTES.splitlines() if not line.startswith("| 200 |")),
        ):
            self.reject(routes=routes)
        self.reject(api=API.replace("| Code | HTTP | Meaning |", "| Code | Status | Meaning |"))

    def test_executable_examples_validate_and_publish(self):
        value = {"meta": {}, "data": {"name": "alice.eth", "status": "ok"}}
        api = API + "\n```json openapi-example Response\n" + json.dumps(value) + "\n```\n"
        doc = self.generate(api=api)
        self.assertEqual(doc["components"]["schemas"]["Response"]["examples"], [value])
        self.reject(api=api.replace('"name": "alice.eth"', '"name": 5'))
        self.reject(api=api.replace('"status": "ok"', '"status": "unknown"'))
        self.reject(api=api.replace('"name": "alice.eth"', '"extra": "alice.eth"'))
        self.reject(api=api.replace('"name": "alice.eth"', '"name": NaN'))
        self.reject(api=api.replace("json openapi-example Response", "json openapi-example Missing"))
        self.reject(api=api.replace("json openapi-example Response", "json openapi-example"))

    def test_nullable_reference_and_nested_map(self):
        api = API.replace("| `labels` | array of string", "| `labels` | nullable map of string to array of nullable enum Status")
        schema = self.generate(api=api)["components"]["schemas"]["Record"]["properties"]["labels"]
        self.assertEqual(schema["type"], ["object", "null"])
        self.assertEqual(schema["additionalProperties"]["items"]["anyOf"][1], {"type": "null"})

    def test_explicit_open_json_leaf_does_not_open_surrounding_object(self):
        api = API.replace("| `meta` | map of string to string", "| `meta` | map of string to json")
        generator = Generator(api, ROUTES)
        schema = generator.generate()["components"]["schemas"]["Response"]
        self.assertEqual(schema["properties"]["meta"]["additionalProperties"], {})
        generator.validate({"meta": {"nested": {"anything": [None, False, 1.25]}}, "data": {"name": "x", "status": "ok"}}, schema, "payload")
        with self.assertRaisesRegex(ContractError, "undeclared field"):
            generator.validate({"meta": {}, "data": {"name": "x", "status": "ok"}, "extra": 1}, schema, "payload")

    def test_cli_is_deterministic_and_check_never_writes(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            root = Path(directory)
            (root / "api-v1.md").write_text(API)
            routes = root / "api-v1-routes.md"
            routes.write_text(ROUTES)
            output = root / "openapi.json"
            args = ["--docs-dir", str(root), "--output", str(output)]
            self.assertEqual(main(args + ["--check"]), 1)
            self.assertFalse(output.exists())
            self.assertEqual(main(args), 0)
            original = output.read_bytes()
            self.assertEqual(main(args), 0)
            self.assertEqual(output.read_bytes(), original)
            self.assertEqual(main(args + ["--check"]), 0)
            routes.write_text(ROUTES.replace("Available.", "New description."))
            self.assertEqual(main(args + ["--check"]), 1)
            self.assertEqual(output.read_bytes(), original)
            routes.write_text(ROUTES.replace("object Response", "object Missing"))
            self.assertEqual(main(args), 1)
            self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
