"""The deliberately small Markdown table grammar in docs/api-v1.md."""

from dataclasses import dataclass
import re
from urllib.parse import urljoin


class ContractError(ValueError):
    """A source document cannot describe an unambiguous API contract."""


NAME = r"[A-Z][A-Za-z0-9]*"
OPERATION = r"(GET|POST) (/v1/[A-Za-z0-9_{}./-]+)"
MARKER = re.compile(
    rf"<!-- openapi:(?:(object|enum) ({NAME})|(conditions|headers)|"
    rf"(parameters|responses) {OPERATION}) -->"
)
HEADERS = {
    "object": ["Field", "Type", "Presence", "Description"],
    "conditions": ["Condition", "Holds when"],
    "headers": ["Header", "Type", "Description"],
    "parameters": ["Parameter", "In", "Type", "Required", "Default", "Description"],
    "responses": ["Status", "Body", "Code", "Headers", "When"],
}


def require(ok, message):
    if not ok:
        raise ContractError(message)


def literal(value):
    require(re.fullmatch(r"`[^`]+`", value), f"expected a backticked literal: {value!r}")
    return value[1:-1]


def literals(value):
    require(re.fullmatch(r"`[^`]+`(?:, `[^`]+`)*", value), f"invalid literal list: {value!r}")
    values = re.findall(r"`([^`]+)`", value)
    require(len(values) == len(set(values)), f"duplicate literal: {value!r}")
    return values


def cells(line):
    require(line.startswith("|") and line.endswith("|"), "table rows must start and end with |")
    values = [v.strip().replace(r"\|", "|") for v in re.split(r"(?<!\\)\|", line[1:-1])]
    require(all(values), "table cells cannot be empty; use none")
    return values


def description(value, filename, base_url):
    """Keep prose as Markdown and resolve documentation links for clients."""
    return re.sub(
        r"(\]\()([^\s)]+)(\))",
        lambda m: m[1] + urljoin(urljoin(base_url, filename), m[2]) + m[3],
        value,
    )


def slug(heading):
    return re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")


@dataclass
class Table:
    kind: str
    key: str
    headers: list
    rows: list
    source: str
    description: str = ""
    parent: str = ""
    anchor: str = ""


def scan(text, filename, base_url):
    tables, examples = [], []
    lines = text.splitlines()
    section, heading, heading_start, anchor = "", "", 0, ""
    anchors = {}
    i = 0
    while i < len(lines):
        line = lines[i]
        fence = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence:
            start = i
            i += 1
            while i < len(lines) and not re.fullmatch(
                rf" {{0,3}}{re.escape(fence[1][0])}{{{len(fence[1])},}}\s*", lines[i]
            ):
                i += 1
            info = fence[2].strip()
            if "openapi-example" in info:
                match = re.fullmatch(rf"json openapi-example ({NAME})", info)
                require(match and i < len(lines), f"{filename}:{start + 1}: malformed executable example")
                examples.append((match[1], "\n".join(lines[start + 1:i])))
            i += 1
            continue
        header = re.match(r"^(#{1,6}) (.+)$", line)
        if header:
            level, title = len(header[1]), header[2]
            raw_anchor = slug(title)
            count = anchors.get(raw_anchor, 0)
            anchors[raw_anchor] = count + 1
            anchor = raw_anchor + (f"-{count}" if count else "")
            if level == 2:
                section = title
                heading = ""
            if level == 3:
                heading, heading_start = title, i + 1
        if not re.match(r"<!--\s*openapi\b", line):
            i += 1
            continue
        source = f"{filename}:{i + 1}"
        match = MARKER.fullmatch(line)
        require(match, f"{source}: malformed or unknown OpenAPI marker")
        kind = match[1] or match[3] or match[4]
        key = match[2] or (f"{match[5]} {match[6]}" if match[4] else kind)
        expected_file = "api-v1-routes.md" if kind in ("parameters", "responses", "headers") else "api-v1.md"
        require(filename == expected_file, f"{source}: {kind} belongs in {expected_file}")
        parent, prose = "", ""
        if kind == "object":
            require(section == "Objects" and heading == key, f"{source}: object must be under Objects / ### {key}")
            paragraphs = lines[heading_start:i]
            while paragraphs and not paragraphs[-1].strip():
                paragraphs.pop()
            if paragraphs and paragraphs[-1].startswith("Extends "):
                extends = re.fullmatch(rf"Extends ({NAME})\.", paragraphs.pop())
                require(extends, f"{source}: malformed Extends line")
                parent = extends[1]
            require(not any(p.startswith("Extends ") for p in paragraphs), f"{source}: misplaced Extends line")
            prose = description("\n".join(paragraphs).strip(), filename, base_url)
        if kind == "conditions":
            require(section == "Objects", f"{source}: conditions belong under Objects")
        i += 1
        require(i < len(lines) and lines[i].startswith("|"), f"{source}: marker must immediately precede table")
        columns = cells(lines[i])
        require(kind == "enum" or columns == HEADERS[kind], f"{source}: unexpected {kind} columns: {columns}")
        require(len(columns) == len(set(columns)), f"{source}: duplicate columns")
        i += 1
        require(i < len(lines), f"{source}: missing table separator")
        separators = cells(lines[i])
        require(len(separators) == len(columns) and all(re.fullmatch(r":?-{3,}:?", s) for s in separators), f"{source}: invalid table separator")
        rows = []
        i += 1
        while i < len(lines) and lines[i].startswith("|"):
            row = cells(lines[i])
            require(len(row) == len(columns), f"{filename}:{i + 1}: wrong number of cells")
            rows.append(dict(zip(columns, row)))
            i += 1
        tables.append(Table(kind, key, columns, rows, source, prose, parent, anchor))
    return tables, examples
