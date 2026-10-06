#!/usr/bin/env python3
"""Run a disposable 10k/100k fidelity prefix with the accepted resource caps.

Requires already available local build/PostgreSQL images. The build image must
provide the pinned workspace Rust toolchain, clang, libclang, Python, jq and
sha256sum. No image is pulled, no existing database is touched, and resources
remain available for inspection. This is not the final million-name gate.
"""
import argparse
import datetime
import json
import hashlib
import re
import shutil
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

GIB = 1024 ** 3
RECIPE = "registry-node-scale-v1"


def capture(*args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def run(*args, cwd=None, input=None):
    subprocess.run(args, cwd=cwd, input=input, text=True, check=True)


def save(path, value):
    with path.open("x") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")


def inspect(name):
    return json.loads(capture("docker", "inspect", name))[0]


def labels(head):
    return ["--label", f"bigname.node-scale.recipe={RECIPE}",
            "--label", f"bigname.node-scale.source-head={head}"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--names", type=int, choices=[10000, 100000], required=True)
    parser.add_argument("--baseline", type=Path, help="Reuse the original resource baseline captured before builder setup")
    parser.add_argument("--resume-from", type=Path, help="Resume an exact preserved publication with a corrected HTTP controller")
    parser.add_argument("--resume-epoch", choices=["structural", "bytes"], default="structural")
    parser.add_argument("--build-image", required=True)
    parser.add_argument("--postgres-image", required=True)
    args = parser.parse_args()
    source = args.source.resolve(strict=True)
    assert not capture("git", "status", "--porcelain=v1", cwd=source), "commit the harness before execution"
    head = capture("git", "rev-parse", "HEAD", cwd=source)
    tree = capture("git", "rev-parse", "HEAD^{tree}", cwd=source)
    # Resolve supplied local images before mutation; never allow a tag to move
    # between containers, and never implicitly pull an absent image.
    build = json.loads(capture("docker", "image", "inspect", args.build_image))[0]
    postgres = json.loads(capture("docker", "image", "inspect", args.postgres_image))[0]
    output = args.output.resolve()
    assert not output.exists(), "retain each attempt in a fresh output directory"
    output.mkdir(parents=True)
    harness = Path(__file__).resolve().parent
    assert not capture("git", "status", "--porcelain=v1", cwd=harness), "commit the controller before execution"
    driver_head = capture("git", "rev-parse", "HEAD", cwd=harness)
    resource = harness / "node-scale-resource.py"
    baseline = output / "resource-baseline.json"
    if args.baseline:
        supplied = args.baseline.resolve(strict=True)
        original = json.loads(supplied.read_text())
        assert original["source_head"] == head, "supplied baseline belongs to a different source"
        current_free = shutil.disk_usage(original["filesystem_path"]).free
        assert current_free >= 100*GIB and original["free_bytes"]-current_free <= 60*GIB
        baseline.write_bytes(supplied.read_bytes())
    else:
        run(sys.executable, str(resource), "baseline", "--filesystem-path", str(output),
            "--output", str(baseline), cwd=source)
    identifier = f"bigname-node-scale-{args.names}-{uuid.uuid4().hex[:8]}"
    pg = identifier + "-pg"
    network = identifier + "-net"
    target = identifier + "-target"
    cargo_cache = identifier + "-cargo"
    git_cache = identifier + "-cargo-git"
    database = "bigname_node_scale"
    marker = str(uuid.uuid4())
    previous = args.resume_from.resolve(strict=True) if args.resume_from else None
    resume_index = ["structural", "changed", "bytes"].index(args.resume_epoch) if previous else -1
    clone = output / "source"
    corpus_directory = output / "corpus"
    inherited_reports = []
    if previous:
        old = json.loads((previous / "run-plan.json").read_text())
        assert all(old[key] == value for key, value in {
            "recipe_version": RECIPE, "source_head": head, "source_tree": tree,
            "names": args.names, "build_image": build["Id"], "postgres_image": postgres["Id"],
            "resource_baseline_sha256": hashlib.sha256(baseline.read_bytes()).hexdigest(),
        }.items()), "resume provenance differs from the original run"
        assert source == Path(old["source_clone"]), "resume must use the original immutable source clone"
        if args.resume_epoch == "structural":
            assert not list(previous.glob("changed-*-report.json")), "structural checkpoint has already advanced"
        clone, corpus_directory = source, source.parent / "corpus"
        original = source.parent
        pg, network, marker = old["containers"]["postgres"], old["network"], old["disposable_marker"]
        target, cargo_cache, git_cache = [old["volumes"][key] for key in ["target", "cargo_cache", "cargo_git_cache"]]
        state = inspect(pg)
        assert state["State"]["Running"] and not state["State"]["OOMKilled"]
        assert state["Image"] == postgres["Id"]
        assert state["Config"]["Labels"]["bigname.node-scale.source-head"] == head
        assert state["Config"]["Labels"]["bigname.node-scale.recipe"] == RECIPE
        assert [state["HostConfig"][key] for key in ["NanoCpus", "Memory", "MemorySwap"]] == [4_000_000_000, 8*GIB, 8*GIB]
        environment = dict(value.split("=", 1) for value in state["Config"]["Env"])
        password = environment["POSTGRES_PASSWORD"]
        prepared = json.loads((original / "prepare-report.json").read_text())
        checkpoint_head = prepared["results"][args.resume_epoch + "_head"]
        retained = [original / "prepare-report.json"]
        for epoch in ["structural", "changed", "bytes"][:resume_index+1]:
            folder = original if epoch == "structural" else previous
            retained += [folder / (epoch + "-" + name + "-report.json") for name in ["seed", "interpret", "project"]]
        for path in retained:
            result = json.loads(path.read_text())
            assert result["source_head"] == head and result["source_tree_clean"]
            if "interpret" in path.name or "project" in path.name:
                assert result["results"]["head"] == prepared["results"][path.name.split("-")[0] + "_head"]
                assert result["results"]["identity_counts"]["surfaces"] == args.names+1
        statement = "SELECT jsonb_agg(jsonb_build_object('phase',phase_name,'status',phase_status,'head',current_block_number,'redo',redo_in_progress)) FROM bigname_phase.chain_phase_state WHERE chain_id='ethereum-sepolia' AND phase_name IN ('ingest','interpret','project')"
        current = json.loads(capture("docker", "exec", pg, "psql", "-U", "bigname", "-d", database, "-At", "-c", statement))
        assert len(current) == 3 and all(row["status"] == "completed" and row["head"] == checkpoint_head and not row["redo"] for row in current), current
        inherited_reports = [path for folder in {previous, original} for pattern in ["*-resource/resource-report.json", "*-api-resource-report.json"] for path in folder.glob(pattern)]
        retained += inherited_reports + [corpus_directory / name for name in ["corpus.json", "samples.json"]]
        save(output / "inherited-evidence.json", {"checkpoint": "after-" + args.resume_epoch + "-project", "current_phase_state": current,
             "files": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in retained]})
    else:
        # This password belongs only to the new unexposed disposable server.
        password = uuid.uuid4().hex
    connection = f"postgres://bigname:{password}@{pg}:5432/{database}"
    corpus_mount = ["--mount", f"type=bind,src={corpus_directory},dst=/corpus,readonly"] if previous else []
    container_corpus = "/corpus" if previous else "/evidence/corpus"
    save(output / "run-plan.json", {
        "recipe_version": RECIPE, "source_head": head, "source_tree": tree,
        "names": args.names, "build_image": build["Id"], "postgres_image": postgres["Id"],
        "images_available_before_prefix": True, "disposable_marker": marker,
        "resource_baseline_sha256": hashlib.sha256(baseline.read_bytes()).hexdigest(),
        "containers": {"postgres": pg}, "network": network,
        "volumes": {"target": target, "cargo_cache": cargo_cache, "cargo_git_cache": git_cache},
        "source_clone": str(clone), "feature_gate_complete": False,
        "resumed_from": str(previous) if previous else None, "controller_source_head": driver_head,
        "controller_files": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in [Path(__file__), resource, harness / "node-scale-http.py"]},
    })
    if not previous:
        # Managed worktree .git files contain host-absolute paths. An independent
        # clone keeps the real clean-source attestation valid inside the container.
        run("git", "clone", "--no-hardlinks", "--no-checkout", str(source), str(clone))
        run("git", "checkout", "--detach", head, cwd=clone)
        assert capture("git", "rev-parse", "HEAD^{tree}", cwd=clone) == tree
        # Nested writable volume mounts need their mountpoint before the parent is read-only.
        (clone / "target").mkdir()
        run("docker", "network", "create", *labels(head), network)
        run("docker", "volume", "create", *labels(head), target)
        run("docker", "volume", "create", *labels(head), cargo_cache)
        run("docker", "volume", "create", *labels(head), git_cache)
        data = output / "postgres-data"
        data.mkdir()
        run("docker", "create", "--name", pg, *labels(head), "--network", network,
            "--cpus", "4", "--memory", "8g", "--memory-swap", "8g", "--shm-size", "512m",
            "--mount", f"type=bind,src={data},dst=/var/lib/postgresql/data",
            "--env", "POSTGRES_USER=bigname", "--env", f"POSTGRES_PASSWORD={password}",
            "--env", f"POSTGRES_DB={database}", postgres["Id"],
            "postgres", "-c", "jit=off", "-c", "shared_buffers=1GB",
            "-c", "work_mem=16MB", "-c", "track_io_timing=on",
            "-c", "log_temp_files=0", "-c", "max_connections=100")
        run("docker", "start", pg)
        for _ in range(60):
            ready = subprocess.run(["docker", "exec", pg, "pg_isready", "-U", "bigname", "-d", database], capture_output=True)
            if ready.returncode == 0:
                break
            assert inspect(pg)["State"]["Running"], "new PostgreSQL exited; inspect retained container"
            time.sleep(1)
        else:
            raise RuntimeError("disposable PostgreSQL did not become ready within 60s")
        sql = f"""CREATE SCHEMA bigname_benchmark;
    CREATE TABLE bigname_benchmark.disposable_copy_marker (
     marker uuid PRIMARY KEY, database_name text NOT NULL, prepared_at timestamptz NOT NULL DEFAULT now());
    INSERT INTO bigname_benchmark.disposable_copy_marker(marker,database_name)
     VALUES ('{marker}'::uuid,current_database());
    """
        run("docker", "exec", "-i", pg, "psql", "-U", "bigname", "-d", database, "-v", "ON_ERROR_STOP=1", input=sql)
        settings = capture("docker", "exec", pg, "psql", "-U", "bigname", "-d", database,
                           "-At", "-c", "SELECT jsonb_object_agg(name,setting) FROM pg_settings")
        save(output / "postgres-settings.json", json.loads(settings))
    phase_containers = []

    def measure(name, operation):
        container = identifier + "-" + name
        report = f"/evidence/{name}-report.json"
        run("docker", "create", "--name", container, *labels(head), "--network", network,
            "--cpus", "2", "--memory", "4g", "--memory-swap", "4g", "--workdir", "/source",
            "--mount", f"type=bind,src={clone},dst=/source,readonly",
            "--mount", f"type=volume,src={target},dst=/source/target",
            "--mount", f"type=volume,src={cargo_cache},dst=/usr/local/cargo/registry",
            "--mount", f"type=volume,src={git_cache},dst=/usr/local/cargo/git",
            "--mount", f"type=bind,src={output},dst=/evidence", *corpus_mount,
            "--env", "CARGO_BUILD_JOBS=2", "--env", "BIGNAME_BENCHMARK_CARGO_PROFILE=release",
            "--env", f"BIGNAME_BENCHMARK_DATABASE_URL={connection}",
            "--env", f"BIGNAME_BUILD_SHA={head}", build["Id"],
            "bash", "./scripts/benchmark-gate", "--report", report, "node-scale", *operation)
        phase_containers.append(container)
        run(sys.executable, str(resource), "run", "--baseline", str(baseline),
            "--postgres-container", pg, "--phase-container", container,
            "--kind", operation[0], "--output", str(output / (name + "-resource")), cwd=source)
        return json.loads((output / (name + "-report.json")).read_text())

    if not previous:
        prepared = measure("prepare", ["prepare", "--names", str(args.names), "--directory", container_corpus])
    corpus = prepared["results"]
    common = ["--directory", container_corpus, "--expected-database-name", database,
              "--disposable-marker", marker, "--allow-disposable-copy-writes"]
    def http_screen(epoch):
        api = identifier + "-" + epoch + "-api"
        digest = prepared["api_binary_sha256"]
        assert re.fullmatch(r"[0-9a-f]{64}", digest)
        binary = f"/source/target/benchmark-gate/{head}/release/benchmark-gate-artifacts/bigname-api-{digest}"
        run("docker", "create", "--name", api, *labels(head), "--network", network,
            "--cpus", "2", "--memory", "512m", "--memory-swap", "512m",
            "--publish", "127.0.0.1::3000", "--workdir", "/source",
            "--mount", f"type=bind,src={clone},dst=/source,readonly",
            "--mount", f"type=volume,src={target},dst=/source/target,readonly",
            "--env", f"BIGNAME_DATABASE_URL={connection}", "--env", "BIGNAME_DATABASE_MAX_CONNECTIONS=8",
            "--env", "RUST_LOG=info", build["Id"], "bash", "-c",
            'set -e; actual=$(sha256sum "$1"); test "${actual%% *}" = "$2"; exec "$1" serve --bind-addr 0.0.0.0:3000',
            "node-scale-api", binary, digest)
        run("docker", "start", api)
        container = inspect(api)
        binding = container["NetworkSettings"]["Ports"]["3000/tcp"][0]
        assert binding["HostIp"] == "127.0.0.1"
        base_url = "http://127.0.0.1:" + binding["HostPort"]
        minimum_free = shutil.disk_usage(output).free
        baseline_free = json.loads(baseline.read_text())["free_bytes"]
        code = None
        metrics = None
        process = None
        try:
            for _ in range(60):
                try:
                    with urllib.request.urlopen(base_url + "/healthz", timeout=1) as response:
                        if response.status == 200:
                            break
                except (urllib.error.URLError, TimeoutError, ConnectionError):
                    pass
                assert inspect(api)["State"]["Running"], "API exited before readiness"
                time.sleep(1)
            else:
                raise RuntimeError("disposable API did not become ready within 60s")
            with (output / (epoch + "-http-driver.log")).open("x") as logfile, (output / (epoch + "-http-resources.jsonl")).open("x") as samples:
                process = subprocess.Popen([sys.executable, str(harness / "node-scale-http.py"),
                    "--corpus", str(corpus_directory), "--base-url", base_url, "--epoch", epoch,
                    "--output", str(output / (epoch + "-http")), "--smoke"], stdout=logfile, stderr=subprocess.STDOUT)
                while process.poll() is None:
                    free = shutil.disk_usage(output).free
                    minimum_free = min(minimum_free, free)
                    if free < 100*GIB or baseline_free-free > 60*GIB:
                        run("docker", "stop", "--time", "5", api, pg)
                        raise RuntimeError("physical space boundary crossed during HTTP")
                    stats = capture("docker", "stats", "--no-stream", "--format", "{{json .}}", pg, api)
                    samples.write(json.dumps({"free_bytes": free, "docker_stats": [json.loads(row) for row in stats.splitlines()]})+"\n")
                    samples.flush()
                    assert inspect(api)["State"]["Running"] and inspect(pg)["State"]["Running"], "disposable service exited during HTTP"
                    time.sleep(1)
                code = process.wait()
            metrics = {"memory_peak_bytes": int(capture("docker", "exec", api, "cat", "/sys/fs/cgroup/memory.peak")),
                       "proc_status": capture("docker", "exec", api, "cat", "/proc/1/status"),
                       "cpu_stat": capture("docker", "exec", api, "cat", "/sys/fs/cgroup/cpu.stat"),
                       "io_stat": capture("docker", "exec", api, "cat", "/sys/fs/cgroup/io.stat")}
            assert metrics["memory_peak_bytes"] <= 512*1024**2, metrics
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                process.wait(timeout=15)
            with (output / (epoch + "-api.log")).open("x") as logfile:
                subprocess.run(["docker", "logs", api], stdout=logfile, stderr=subprocess.STDOUT, check=False)
            final = inspect(api)
            minimum_free = min(minimum_free, shutil.disk_usage(output).free)
            save(output / (epoch + "-api-resource-report.json"), {
                "source_head": head, "api_binary_sha256": digest, "container_id": final["Id"],
                "image": final["Image"], "limits": {key: final["HostConfig"][key] for key in ["NanoCpus", "Memory", "MemorySwap"]},
                "oom_killed": final["State"]["OOMKilled"], "http_driver_exit_code": code,
                "minimum_free_bytes": minimum_free, "additional_physical_peak_bytes": max(0,baseline_free-minimum_free),
                "process_and_cgroup": metrics, "feature_gate_complete": False})
            if final["State"]["Running"]:
                run("docker", "stop", "--time", "5", api)
        assert code == 0 and not final["State"]["OOMKilled"], "HTTP fidelity screen failed; retain evidence"
        assert minimum_free >= 100*GIB and baseline_free-minimum_free <= 60*GIB

    for index, epoch in enumerate(["structural", "changed", "bytes"]):
        epoch_head = str(corpus[epoch + "_head"])
        if index < resume_index:
            continue
        if index > resume_index:
            measure(epoch + "-seed", ["seed", *common, "--head", epoch_head, *(["--append"] if index else [])])
            measure(epoch + "-interpret", ["interpret", *common, "--head", epoch_head])
            measure(epoch + "-project", ["project", *common, "--head", epoch_head])
        http_screen(epoch)
    report_paths = list(output.glob("*-resource/resource-report.json")) + list(output.glob("*-api-resource-report.json"))
    reports = [json.loads(path.read_text()) for path in [*inherited_reports, *report_paths]]
    save(output / "prefix-report.json", {
        "source_head": head, "source_tree": tree, "interpreter_content_hash": prepared["interpreter_content_hash"],
        "names": args.names, "raw_digest": corpus["raw"]["rolling_keccak256"],
        "phase_containers": phase_containers, "postgres_container": pg,
        "resumed_from": str(previous) if previous else None, "controller_source_head": driver_head,
        "additional_physical_peak_bytes": max(row["additional_physical_peak_bytes"] for row in reports),
        "minimum_free_bytes": min(row["minimum_free_bytes"] for row in reports),
        "completed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "http_fidelity_screen_passed": True, "warm_budget_measurement_pending": True,
        "redo_reorg_imports_pending": True,
        "feature_gate_complete": False,
    })
    print(f"Prefix phases completed; evidence and disposable resources retained at {output}")


if __name__ == "__main__":
    main()
