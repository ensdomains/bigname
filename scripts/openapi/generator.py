"""Deterministically compile the two API contract documents; no network or packages."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import sys
from urllib.parse import urljoin

from .markdown import ContractError, NAME, description, literal, literals, require, scan

DOCS_BASE = "https://github.com/ensdomains/bigname/blob/main/docs/"
DIALECT = "https://json-schema.org/draft/2020-12/schema"
SCALARS = {"string", "integer", "boolean"}
BOUNDED_INTEGER = r"integer \[(-?(?:0|[1-9][0-9]*)), (-?(?:0|[1-9][0-9]*))\]"
ARRAY = r"array(?: \[((?:0|[1-9][0-9]*)), ((?:0|[1-9][0-9]*))\])? of (.+)"


class Generator:
    def __init__(self, api_text, routes_text, base_url=DOCS_BASE):
        require(base_url.startswith(("https://", "http://")) and base_url.endswith("/"), "documentation base URL must be an absolute URL ending in /")
        self.base_url = base_url
        self.tables, self.examples = {}, []
        for filename, text in (("api-v1.md", api_text), ("api-v1-routes.md", routes_text)):
            tables, examples = scan(text, filename, base_url)
            self.examples.extend(examples)
            for table in tables:
                group = self.tables.setdefault(table.kind, {})
                require(table.key not in group, f"{table.source}: duplicate {table.kind} {table.key}")
                group[table.key] = table
        self.objects = self.tables.get("object", {})
        self.enums = self.tables.get("enum", {})
        require(not self.objects.keys() & self.enums.keys(), "object and enum names must be unique")
        self.schemas, self.fields, self.condition_text, self.headers = {}, {}, {}, {}
        self.used_objects, self.used_enums, self.used_conditions = set(), set(), set()
        self.unions, self.defaults, self.error_status = [], [], {}

    def prose(self, text, routes=False):
        return description(text, "api-v1-routes.md" if routes else "api-v1.md", self.base_url)

    def reference(self, kind, name):
        definitions = self.objects if kind == "object" else self.enums
        require(name in definitions, f"undefined {kind} {name}")
        (self.used_objects if kind == "object" else self.used_enums).add(name)
        return {"$ref": f"#/components/schemas/{name}"}

    def type_schema(self, expr):
        if expr == "json":
            return {}
        if expr.startswith("nullable "):
            require(not expr.startswith("nullable nullable "), f"invalid nullable type: {expr}")
            inner = self.type_schema(expr[9:])
            if "type" in inner and "enum" not in inner:
                return {**inner, "type": [inner["type"], "null"]}
            return {"anyOf": [inner, {"type": "null"}]}
        if expr in SCALARS:
            return {"type": expr}
        match = re.fullmatch(BOUNDED_INTEGER, expr)
        if match:
            minimum, maximum = map(int, match.groups())
            require(minimum <= maximum, f"integer minimum exceeds maximum: {expr}")
            return {"type": "integer", "minimum": minimum, "maximum": maximum}
        match = re.fullmatch(ARRAY, expr)
        if match:
            schema = {"type": "array", "items": self.type_schema(match[3])}
            if match[1] is not None:
                minimum, maximum = int(match[1]), int(match[2])
                require(minimum <= maximum, f"array minimum exceeds maximum: {expr}")
                schema.update(minItems=minimum, maxItems=maximum)
            return schema
        if expr.startswith("map of string to "):
            return {"type": "object", "additionalProperties": self.type_schema(expr[17:])}
        match = re.fullmatch(rf"(object|enum) ({NAME})", expr)
        if match:
            return self.reference(*match.groups())
        if expr.startswith("enum "):
            return {"type": "string", "enum": literals(expr[5:])}
        if expr.startswith("one of "):
            alternatives = expr[7:].split(", ")
            require(len(alternatives) >= 2 and len(set(alternatives)) == len(alternatives), f"invalid one of: {expr}")
            require(all(a in SCALARS or re.fullmatch(rf"object {NAME}", a) for a in alternatives), f"invalid one of alternatives: {expr}")
            self.unions.append(alternatives)
            return {"oneOf": [self.type_schema(a) for a in alternatives]}
        raise ContractError(f"unknown type: {expr!r}")

    def compile_enums_conditions(self):
        conditions = self.tables.get("conditions", {})
        require(len(conditions) == 1, "exactly one conditions table is required")
        for row in conditions["conditions"].rows:
            name = row["Condition"]
            require(re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", name), f"invalid condition name: {name}")
            require(name not in self.condition_text, f"duplicate condition {name}")
            self.condition_text[name] = self.prose(row["Holds when"])
        for name, table in self.enums.items():
            values, notes = [], {}
            require(table.rows, f"enum {name} has no values")
            if name == "ErrorCode":
                require("HTTP" in table.headers, "ErrorCode must have an HTTP column")
            for row in table.rows:
                value = literal(row[table.headers[0]])
                require(value not in values, f"duplicate enum value {name}.{value}")
                values.append(value)
                text = " ".join(row[h] for h in table.headers[1:])
                if text:
                    notes[value] = self.prose(text)
                if name == "ErrorCode":
                    require(re.fullmatch(r"[45][0-9]{2}", row["HTTP"]), f"invalid ErrorCode HTTP status: {row['HTTP']}")
                    self.error_status[value] = row["HTTP"]
            self.schemas[name] = {"type": "string", "enum": values}
            if notes:
                self.schemas[name]["x-enum-descriptions"] = notes

    def object_fields(self, name, stack=()):
        if name in self.fields:
            return self.fields[name]
        require(name in self.objects, f"unknown parent object {name}")
        require(name not in stack, f"Extends cycle: {' -> '.join((*stack, name))}")
        table = self.objects[name]
        fields = {}
        if table.parent:
            self.used_objects.add(table.parent)
            fields.update(deepcopy(self.object_fields(table.parent, (*stack, name))))
        for row in table.rows:
            field = literal(row["Field"])
            require(field not in fields, f"duplicate field {name}.{field}, including inherited fields")
            presence = row["Presence"]
            schema = self.type_schema(row["Type"])
            schema["description"] = self.prose(row["Description"])
            if presence not in ("always", "optional"):
                match = re.fullmatch(r"(?:only )?when ([a-z][a-z0-9]*(?:_[a-z0-9]+)*)", presence)
                require(match and match[1] in self.condition_text, f"unknown presence condition: {presence}")
                self.used_conditions.add(match[1])
                schema["x-presence"] = presence
                schema["description"] += "\n\nPresence condition: " + self.condition_text[match[1]]
            fields[field] = (schema, presence == "always")
        self.fields[name] = fields
        return fields

    def compile_objects(self):
        for name, table in self.objects.items():
            fields = self.object_fields(name)
            schema = {
                "type": "object",
                "properties": {field: value[0] for field, value in fields.items()},
                "additionalProperties": False,
            }
            required = [field for field, value in fields.items() if value[1]]
            if required:
                schema["required"] = required
            if table.description:
                schema["description"] = table.description
            self.schemas[name] = schema
        for alternatives in self.unions:
            for i, left in enumerate(alternatives):
                for right in alternatives[i + 1:]:
                    require(self.exclusive(left, right), f"one of alternatives overlap: {left}, {right}")

    def exclusive(self, left, right):
        if left in SCALARS or right in SCALARS:
            return left != right
        a, b = self.fields[left[7:]], self.fields[right[7:]]
        if any(required and field not in b for field, (_, required) in a.items()):
            return True
        if any(required and field not in a for field, (_, required) in b.items()):
            return True
        for field in a.keys() & b.keys():
            x, y = a[field], b[field]
            if x[1] and y[1] and len(x[0].get("enum", [])) == len(y[0].get("enum", [])) == 1 and x[0]["enum"] != y[0]["enum"]:
                return True
        return False

    def compile_headers(self):
        tables = self.tables.get("headers", {})
        require(len(tables) == 1, "exactly one headers table is required")
        names = set()
        for row in tables["headers"].rows:
            name = literal(row["Header"])
            require(name.lower() not in names, f"duplicate header {name}")
            names.add(name.lower())
            self.headers[name] = {"schema": self.type_schema(row["Type"]), "description": self.prose(row["Description"], True)}

    def is_parameter_type(self, expr):
        match = re.fullmatch(ARRAY, expr)
        if match:
            expr = match[3]
        return expr in SCALARS or expr.startswith("enum ") or bool(re.fullmatch(BOUNDED_INTEGER, expr))

    def parse_default(self, raw, schema):
        raw = literal(raw)
        resolved = self.schemas.get(schema.get("$ref", "").rsplit("/", 1)[-1], schema)
        kind = resolved.get("type")
        if kind == "string":
            return raw
        if kind == "integer":
            require(re.fullmatch(r"-?(?:0|[1-9][0-9]*)", raw), f"invalid integer default: {raw}")
            return int(raw)
        if kind == "boolean":
            require(raw in ("true", "false"), f"invalid boolean default: {raw}")
            return raw == "true"
        if kind == "array":
            return [self.parse_default(f"`{part}`", schema["items"]) for part in raw.split(",")]
        raise ContractError(f"default not supported for {kind}")

    def parameters(self, table, method, path):
        result, body, seen, path_rows = [], None, set(), set()
        for row in table.rows:
            name, location = literal(row["Parameter"]), row["In"]
            require(location in ("query", "path", "header", "body"), f"unknown parameter location: {location}")
            identity = (location, name.lower() if location == "header" else name)
            require(identity not in seen, f"duplicate parameter {location} {name}")
            seen.add(identity)
            require(row["Required"] in ("yes", "no"), f"invalid required cell: {row['Required']}")
            required = row["Required"] == "yes"
            require(not required or row["Default"] == "none", f"required parameter {name} cannot have a default")
            expr = row["Type"]
            repeated = expr.startswith("repeated ")
            if repeated:
                expr = expr.removeprefix("repeated ")
                require(location == "query" and re.fullmatch(ARRAY, expr), "repeated is only valid for query arrays")
            schema = self.type_schema(expr)
            note = self.prose(row["Description"], True)
            if location == "body":
                require(method == "POST" and body is None and name == "body" and re.fullmatch(rf"object {NAME}", expr) and row["Default"] == "none", "invalid request body parameter")
                body = {"required": required, "description": note, "content": {"application/json": {"schema": schema}}}
                continue
            require(self.is_parameter_type(expr), f"invalid {location} parameter type: {expr}")
            if location == "path":
                require(required, f"path parameter {name} must be required")
                path_rows.add(name)
            if row["Default"] != "none":
                value = self.parse_default(row["Default"], schema)
                self.validate(value, schema, f"default of {name}")
                schema["default"] = value
            parameter = {"name": name, "in": location, "required": required, "description": note, "schema": schema}
            if schema.get("type") == "array":
                parameter.update(style="form" if location == "query" else "simple", explode=repeated)
            result.append(parameter)
        segments = re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", path)
        require(len(segments) == len(set(segments)), f"duplicate path segment in {path}")
        require("{" not in re.sub(r"\{[A-Za-z_][A-Za-z0-9_]*\}", "", path) and "}" not in re.sub(r"\{[A-Za-z_][A-Za-z0-9_]*\}", "", path), f"invalid path template {path}")
        require(set(segments) == path_rows, f"path parameters do not match {path}: {sorted(path_rows)}")
        return result, body

    def extends_envelope(self, name):
        parent = self.objects[name].parent
        return bool(parent and (parent == "Envelope" or self.extends_envelope(parent)))

    def responses(self, table):
        responses, bodies, seen, success = {}, {}, set(), False
        error_codes = {}
        for row in table.rows:
            status, body = row["Status"], row["Body"]
            require(re.fullmatch(r"[1-5][0-9]{2}", status), f"invalid response status {status}")
            code = None if row["Code"] == "none" else literal(row["Code"])
            require((status, code) not in seen, f"duplicate response {status} {code}")
            seen.add((status, code))
            require(status not in bodies or bodies[status] == body, f"response {status} has different bodies")
            bodies[status] = body
            body_match = re.fullmatch(rf"object ({NAME})", body)
            require(body == "none" or body_match, f"invalid response body {body}")
            schema = self.reference("object", body_match[1]) if body_match else None
            if status.startswith("2"):
                success = True
                require(code is None and body_match and self.extends_envelope(body_match[1]), f"success response {status} must extend Envelope and have no code")
            elif status == "304":
                require(code is None and body == "none", "304 must have no code or body")
            else:
                require(status.startswith(("4", "5")) and body == "object ErrorEnvelope" and self.error_status.get(code) == status, f"error code {code} does not map to response {status}")
                error_codes.setdefault(status, []).append(code)
            note = self.prose(row["When"], True)
            if code:
                note = f"`{code}`: {note}"
            if status in responses:
                responses[status]["description"] += "\n\n" + note
            else:
                responses[status] = {"description": note}
                if schema:
                    responses[status]["content"] = {"application/json": {"schema": schema}}
            if row["Headers"] != "none":
                for header in literals(row["Headers"]):
                    require(header in self.headers, f"undefined response header {header}")
                    responses[status].setdefault("headers", {})[header] = {"$ref": f"#/components/headers/{header}"}
        require(success, f"{table.key} has no success response")
        for status, codes in error_codes.items():
            content = responses[status]["content"]["application/json"]
            # Intersect with the closed envelope: the overlay narrows only the code.
            content["schema"] = {"allOf": [content["schema"], {
                "type": "object", "properties": {"error": {
                    "type": "object", "properties": {"code": {"enum": codes}}
                }}
            }]}
        return responses

    def validate(self, value, schema, context):
        """Validate docs examples/defaults against only this generator's output vocabulary.

        Standards-level schema and live payload validation belongs to the Rust
        contract tests using the pinned offline JSON Schema implementation.
        """
        if "$ref" in schema:
            name = schema["$ref"].rsplit("/", 1)[-1]
            require(name in self.schemas, f"unresolved reference {schema['$ref']}")
            self.validate(value, self.schemas[name], context)
        for constraint in schema.get("allOf", []):
            self.validate(value, constraint, context)
        for keyword in ("anyOf", "oneOf"):
            if keyword in schema:
                matches = 0
                for alternative in schema[keyword]:
                    try:
                        self.validate(value, alternative, context)
                        matches += 1
                    except ContractError:
                        pass
                require(matches >= 1 if keyword == "anyOf" else matches == 1, f"{context}: invalid {keyword} value")
        if "enum" in schema:
            require(value in schema["enum"], f"{context}: not an allowed enum value")
        if "type" not in schema:
            return
        kind = schema["type"]
        kinds = kind if isinstance(kind, list) else [kind]
        actual = ("null" if value is None else "boolean" if isinstance(value, bool) else
                  "integer" if isinstance(value, int) or isinstance(value, float) and value.is_integer() else
                  "string" if isinstance(value, str) else "array" if isinstance(value, list) else
                  "object" if isinstance(value, dict) else "number")
        require(actual in kinds, f"{context}: expected {kind}, found {actual}")
        if actual == "integer":
            if "minimum" in schema:
                require(value >= schema["minimum"], f"{context}: below minimum {schema['minimum']}")
            if "maximum" in schema:
                require(value <= schema["maximum"], f"{context}: above maximum {schema['maximum']}")
        if actual == "array":
            if "minItems" in schema:
                require(len(value) >= schema["minItems"], f"{context}: below minItems {schema['minItems']}")
            if "maxItems" in schema:
                require(len(value) <= schema["maxItems"], f"{context}: above maxItems {schema['maxItems']}")
            for index, item in enumerate(value):
                self.validate(item, schema["items"], f"{context}[{index}]")
        if actual == "object":
            require(set(schema.get("required", [])) <= value.keys(), f"{context}: missing required fields")
            for key, item in value.items():
                prop = schema.get("properties", {}).get(key, schema.get("additionalProperties", True))
                require(prop is not False, f"{context}: undeclared field {key}")
                if isinstance(prop, dict):
                    self.validate(item, prop, f"{context}.{key}")

    def generate(self):
        self.compile_enums_conditions()
        self.compile_objects()
        self.compile_headers()
        parameters, responses = self.tables.get("parameters", {}), self.tables.get("responses", {})
        require(parameters and parameters.keys() == responses.keys(), "each operation requires exactly one parameters and one responses table")
        paths, ids = {}, set()
        for key in sorted(parameters):
            method, path = key.split(" ", 1)
            table = parameters[key]
            operation_id = re.sub(r"[{}]", "", method.lower() + path.replace("/", "_").replace("-", "_"))
            require(operation_id not in ids, f"duplicate operationId {operation_id}")
            ids.add(operation_id)
            params, body = self.parameters(table, method, path)
            operation = {
                "operationId": operation_id,
                "tags": [path.split("/")[2]],
                "description": f"See [{key}]({urljoin(self.base_url, 'api-v1-routes.md')}#{table.anchor}).",
                "parameters": params,
                "responses": self.responses(responses[key]),
            }
            if body:
                operation["requestBody"] = body
            paths.setdefault(path, {})[method.lower()] = operation
        for name, raw in self.examples:
            self.reference("object", name)
            try:
                value = json.loads(raw, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
            except ValueError as error:
                raise ContractError(f"invalid JSON example {name}: {error}") from error
            self.validate(value, self.schemas[name], f"example {name}")
            self.schemas[name].setdefault("examples", []).append(value)
        for kind, defined, used in (("object", self.objects, self.used_objects), ("enum", self.enums, self.used_enums), ("condition", self.condition_text, self.used_conditions)):
            require(not defined.keys() - used, f"unreferenced {kind}s: {', '.join(sorted(defined.keys() - used))}")
        return {
            "openapi": "3.1.0",
            "jsonSchemaDialect": DIALECT,
            "info": {"title": "Bigname API", "version": "__BIGNAME_VERSION__", "x-build-sha": "__BIGNAME_BUILD_SHA__", "description": "Generated from the checked-in API contract tables."},
            "paths": paths,
            "components": {"schemas": self.schemas, "headers": self.headers},
        }


def main(argv=None):
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the checked-in artifact differs; never write")
    parser.add_argument("--docs-dir", type=Path, default=root / "docs")
    parser.add_argument("--output", type=Path, default=root / "apps/api/openapi.json")
    parser.add_argument("--docs-base-url", default=DOCS_BASE)
    args = parser.parse_args(argv)
    try:
        generator = Generator(*(args.docs_dir.joinpath(name).read_text(encoding="utf-8") for name in ("api-v1.md", "api-v1-routes.md")), args.docs_base_url)
        artifact = json.dumps(generator.generate(), ensure_ascii=False, indent=2) + "\n"
        if args.check:
            require(args.output.exists() and args.output.read_text(encoding="utf-8") == artifact, f"{args.output} is stale; run scripts/generate-openapi")
        else:
            args.output.write_text(artifact, encoding="utf-8")
    except (ContractError, OSError) as error:
        print(f"OpenAPI generation failed: {error}", file=sys.stderr)
        return 1
    print("OpenAPI artifact is current." if args.check else f"Generated {args.output}")
    return 0
