#!/usr/bin/env python3
"""Confirm one observed search cohort on a completed disposable 10k prefix.

The 200 sequential warm HTTP observations use the unchanged production binary.
A second API process enables statement logging only on its own PostgreSQL
connections, after timing, to retain actual SQL and bind values. Both forced
plan modes execute that captured statement; neither is claimed as the normal
API plan choice. This is exploratory evidence, not frozen-release acceptance.
"""
import argparse
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def module(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


sys.dont_write_bytecode = True
helper = module("node-scale-prefix")
resource = module("node-scale-resource")
http = module("node-scale-http")
GIB = 1024**3
QUERY = {"q": "a", "match": "contains", "page_size": 200, "namespace": "ens"}


def logged_statements(raw):
    """Retain multiline PostgreSQL execute records and their same-PID details."""
    header = re.compile(r"^\d{4}-\d\d-\d\d .*?\[(\d+)\] (LOG|DETAIL|STATEMENT|ERROR|WARNING|CONTEXT):\s*(.*)$")
    pending, records, current = {}, [], None
    for line in raw.splitlines():
        match = header.match(line)
        if match:
            pid, level, message = match.groups()
            if level == "LOG" and re.match(r"duration: [\d.]+ ms\s+execute [^:]+:", message):
                timing, statement = re.split(r" ms\s+execute [^:]+:\s*", message, maxsplit=1)
                current = {"pid": pid, "server_duration_ms": float(timing.removeprefix("duration: ")),
                           "sql": statement, "parameter_detail": None}
                pending[pid] = current
                records.append(current)
            elif level == "DETAIL" and message.startswith("parameters:") and pid in pending:
                current = pending.pop(pid)
                current["parameter_detail"] = message.removeprefix("parameters:").strip()
                current = None
            else:
                # Parse/bind records also carry DETAIL, but do not belong to
                # the preceding execute record from this connection.
                pending.pop(pid, None)
                current = None
        elif current is not None:
            current["sql"] += "\n" + line.removeprefix("\t")
    return records


def parameter_literals(detail):
    # The retained log's SQL string literals are used verbatim by EXECUTE. The
    # parser refuses an incomplete/truncated detail instead of guessing a bind.
    matches = list(re.finditer(r"(?:^|, )\$(\d+) = (NULL|'(?:[^']|'')*')", detail))
    assert matches and "".join(match.group(0) for match in matches) == detail, detail
    assert [int(match.group(1)) for match in matches] == list(range(1, 8)), "captured candidate parameter count differs"
    return [match.group(2) for match in matches]


def plan_nodes(value):
    if isinstance(value, dict):
        if "Node Type" in value:
            yield {key: value[key] for key in ["Node Type", "Relation Name", "Index Name", "Alias",
                   "Actual Rows", "Actual Loops", "Rows Removed by Filter", "Rows Removed by Join Filter",
                   "Shared Hit Blocks", "Shared Read Blocks", "Temp Read Blocks", "Temp Written Blocks",
                   "Actual Total Time", "Sort Method", "Sort Space Used", "Sort Space Type"] if key in value}
        for child in value.values():
            yield from plan_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from plan_nodes(child)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prior = args.prefix.resolve(strict=True)
    plan = json.loads((prior / "run-plan.json").read_text())
    prefix = json.loads((prior / "prefix-report.json").read_text())
    assert prefix["http_fidelity_screen_passed"] and prefix["names"] == 10000
    assert not prefix["feature_gate_complete"]
    head = plan["source_head"]
    clone = Path(plan["source_clone"])
    harness = Path(__file__).resolve().parent
    assert helper.capture("git", "rev-parse", "HEAD", cwd=clone) == head
    assert not helper.capture("git", "status", "--porcelain=v1", cwd=clone)
    assert not helper.capture("git", "status", "--porcelain=v1", cwd=harness)
    driver_head = helper.capture("git", "rev-parse", "HEAD", cwd=harness)
    original = clone.parent
    prepared = json.loads((original / "prepare-report.json").read_text())
    phase_folder, seen = prior, set()
    while not (phase_folder / "bytes-project-report.json").exists():
        assert phase_folder not in seen, "cyclic inheritance"
        seen.add(phase_folder)
        phase_folder = Path(json.loads((phase_folder / "run-plan.json").read_text())["resumed_from"])
    final_phase = json.loads((phase_folder / "bytes-project-report.json").read_text())
    expected_head = final_phase["results"]["head"]
    baseline = json.loads((prior / "resource-baseline.json").read_text())
    pg = plan["containers"]["postgres"]
    state = helper.inspect(pg)
    resource.verify(state, 4, 8*GIB, head)
    assert state["Image"] == plan["postgres_image"] and not state["State"]["OOMKilled"]
    environment = dict(value.split("=", 1) for value in state["Config"]["Env"])
    connection = f"postgres://bigname:{environment['POSTGRES_PASSWORD']}@{pg}:5432/bigname_node_scale"
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    started, minimum_free, running_api = time.monotonic(), shutil.disk_usage(output).free, None
    assert minimum_free >= 100*GIB and baseline["free_bytes"]-minimum_free <= 60*GIB, "original envelope exceeded before startup"
    identifier = "bigname-node-scale-focus-" + uuid.uuid4().hex[:8]
    records, plans = [], {}

    def guard():
        nonlocal minimum_free
        minimum_free = min(minimum_free, shutil.disk_usage(output).free)
        assert minimum_free >= 100*GIB and baseline["free_bytes"]-minimum_free <= 60*GIB, "original physical envelope exceeded"
        assert time.monotonic()-started < 1800, "bounded diagnostic exceeded thirty minutes"
        postgres_state = helper.inspect(pg)
        assert postgres_state["State"]["Running"] and not postgres_state["State"]["OOMKilled"]

    def psql(statement):
        return subprocess.check_output(["docker", "exec", "-i", pg, "psql", "-U", "bigname", "-d",
                   "bigname_node_scale", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], input=statement, text=True)

    def publication():
        value = json.loads(psql("SELECT jsonb_build_object('head',current_block_number,'hash',current_block_hash,'input_hash',input_content_hash,'generation',sequence) FROM bigname_phase.project_family_marker WHERE chain_id='ethereum-sepolia' AND state='live';"))
        assert value["head"] == expected_head and value["input_hash"] == prepared["interpreter_content_hash"], value
        return value

    def stop_api(label):
        nonlocal running_api
        if running_api is None:
            return
        status = helper.inspect(running_api)
        with (output / (label + "-api.log")).open("x") as logfile:
            subprocess.run(["docker", "logs", running_api], stdout=logfile, stderr=subprocess.STDOUT, check=False)
        metrics = None
        if status["State"]["Running"]:
            metrics = {"memory_peak": helper.capture("docker", "exec", running_api, "cat", "/sys/fs/cgroup/memory.peak"),
                       "memory_events": helper.capture("docker", "exec", running_api, "cat", "/sys/fs/cgroup/memory.events"),
                       "cpu_stat": helper.capture("docker", "exec", running_api, "cat", "/sys/fs/cgroup/cpu.stat"),
                       "io_stat": helper.capture("docker", "exec", running_api, "cat", "/sys/fs/cgroup/io.stat")}
            helper.run("docker", "stop", "--timeout", "5", running_api)
        helper.save(output / (label + "-api-resource.json"), {"container": resource.attestation(status), "metrics": metrics})
        running_api = None
        assert not status["State"]["OOMKilled"] and metrics and int(metrics["memory_peak"]) <= 512*1024**2

    def start_api(label, diagnostic=False):
        nonlocal running_api
        digest = prepared["api_binary_sha256"]
        assert re.fullmatch(r"[0-9a-f]{64}", digest)
        binary = f"/source/target/benchmark-gate/{head}/release/benchmark-gate-artifacts/bigname-api-{digest}"
        url = connection
        if diagnostic:
            url += "?" + urllib.parse.urlencode({"options": "-c log_min_duration_statement=0 -c log_parameter_max_length=-1"})
        running_api = identifier + "-" + label
        helper.run("docker", "create", "--name", running_api, *helper.labels(head), "--network", plan["network"],
                   "--cpus", "2", "--memory", "512m", "--memory-swap", "512m", "--publish", "127.0.0.1::3000",
                   "--workdir", "/source", "--mount", f"type=bind,src={clone},dst=/source,readonly",
                   "--mount", f"type=volume,src={plan['volumes']['target']},dst=/source/target,readonly",
                   "--env", f"BIGNAME_DATABASE_URL={url}", "--env", "BIGNAME_DATABASE_MAX_CONNECTIONS=8",
                   "--env", "RUST_LOG=info", plan["build_image"], "bash", "-c",
                   'set -e; actual=$(sha256sum "$1"); test "${actual%% *}" = "$2"; exec "$1" serve --bind-addr 0.0.0.0:3000',
                   "node-scale-api", binary, digest)
        resource.verify(helper.inspect(running_api), 2, 512*1024**2, head)
        helper.run("docker", "start", running_api)
        binding = helper.inspect(running_api)["NetworkSettings"]["Ports"]["3000/tcp"][0]
        assert binding["HostIp"] == "127.0.0.1"
        base = "http://127.0.0.1:" + binding["HostPort"]
        for _ in range(60):
            guard()
            try:
                with urllib.request.urlopen(base + "/healthz", timeout=1) as response:
                    if response.status == 200:
                        return base
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            assert helper.inspect(running_api)["State"]["Running"]
            time.sleep(1)
        raise RuntimeError("focused API did not become ready within sixty seconds")

    try:
        if not state["State"]["Running"]:
            helper.run("docker", "start", pg)
        for _ in range(60):
            ready = subprocess.run(["docker", "exec", pg, "pg_isready", "-U", "bigname", "-d", "bigname_node_scale"], capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("preserved PostgreSQL did not become ready")
        guard()
        before = publication()
        helper.save(output / "source.json", {"runtime_source_head": head, "runtime_source_tree": plan["source_tree"],
                    "api_binary_sha256": prepared["api_binary_sha256"], "controller_source_head": driver_head,
                    "controller_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "epoch": "bytes", "publication": before, "query": QUERY,
                    "diagnostic_logging_after_timing_only": True, "baseline": baseline, "feature_gate_complete": False})
        base = start_api("warm")
        reference = None
        for index in range(205):
            response = http.request(base, "/v1/search", QUERY)
            http.save(output, f"http-{index:03}.json", response)
            assert response["status"] == 200 and len(response["body"]["data"]) == 200, response
            reference = reference or response["response_sha256"]
            assert response["response_sha256"] == reference, "stable publication changed its full JSON response"
            if index >= 5:
                records.append(response["milliseconds"])
            guard()
        stop_api("warm")
        diagnostic_since = datetime.datetime.now(datetime.timezone.utc).isoformat()
        base = start_api("capture", diagnostic=True)
        captured_response = http.request(base, "/v1/search", QUERY)
        http.save(output, "capture-http.json", captured_response)
        assert captured_response["response_sha256"] == reference
        stop_api("capture")
        raw = subprocess.run(["docker", "logs", "--since", diagnostic_since, pg], text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True).stdout
        (output / "postgres-capture.log").write_text(raw)
        statements = logged_statements(raw)
        helper.save(output / "captured-statements.json", statements)
        candidates = [row for row in statements if "/* storage:families.name.search_candidates */" in row["sql"]]
        assert len(candidates) == 1, "capture must prove the exact candidate call count; inspect retained SQL"
        selected = candidates[0]
        literals = parameter_literals(selected["parameter_detail"])
        assert literals == ["'{ens}'", "NULL", "'%a%'", "NULL", "NULL", "NULL", "'804'"], literals
        (output / "production-candidates.sql").write_text(selected["sql"])
        result_digests = {}
        for mode in ["force_custom_plan", "force_generic_plan"]:
            statement = ("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\nSET LOCAL search_path=bigname_phase;\nSET LOCAL statement_timeout=25000;\n"
                         f"SET LOCAL plan_cache_mode={mode};\nPREPARE captured_search(text[],text,text,text,text,text,bigint) AS "
                         + selected["sql"] + ";\nEXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) EXECUTE captured_search("
                         + ",".join(literals) + ");\nSELECT 'NODE_SCALE_RESULT_ROWS';\nEXECUTE captured_search("
                         + ",".join(literals) + ");\nROLLBACK;\n")
            (output / (mode + ".sql")).write_text(statement)
            executed = psql(statement)
            plan_json, rows = executed.split("\nNODE_SCALE_RESULT_ROWS\n", 1)
            (output / (mode + ".json")).write_text(plan_json+"\n")
            (output / (mode + "-result-rows.txt")).write_text(rows)
            result_digests[mode] = {"rows": len(rows.splitlines()), "sha256": hashlib.sha256(rows.encode()).hexdigest()}
            plans[mode] = json.loads(plan_json)
            helper.save(output / (mode + "-work.json"), list(plan_nodes(plans[mode])))
            guard()
        assert result_digests["force_custom_plan"] == result_digests["force_generic_plan"], result_digests
        assert publication() == before
        p50, p95, p99 = [http.percentile(records, fraction) for fraction in [0.50, 0.95, 0.99]]
        report = {"runtime_source_head": head, "epoch": "bytes", "publication": before, "query": QUERY,
                  "concurrency": 1, "warmups": 5, "observations": len(records), "samples_ms": records,
                  "p50_ms": p50, "p95_ms": p95, "p99_ms": p99, "response_sha256": reference,
                  "warm_budget_pass": p50 <= 25 and p95 <= 75 and p99 <= 150,
                  "candidate_server_duration_ms": selected["server_duration_ms"],
                  "plan_modes_are_forced_diagnostics": True, "candidate_result_digests": result_digests, "minimum_free_bytes": minimum_free,
                  "additional_physical_peak_bytes": max(0, baseline["free_bytes"]-minimum_free),
                  "feature_gate_complete": False, "frozen_candidate_acceptance": False}
        helper.save(output / "focused-report.json", report)
        print(json.dumps({key: report[key] for key in ["runtime_source_head", "observations", "p50_ms", "p95_ms", "p99_ms", "warm_budget_pass", "feature_gate_complete"]}))
    finally:
        try:
            if running_api:
                stop_api("interrupted")
        finally:
            if helper.inspect(pg)["State"]["Running"]:
                helper.run("docker", "stop", "--timeout", "5", pg)


if __name__ == "__main__":
    main()
