#!/usr/bin/env python3
"""Record and enforce the accepted disposable node-scale resource envelope.

Resource creation stays explicit. This wrapper starts only a pre-created,
labelled phase container and stops only those supplied disposable containers
if the physical-space boundary is crossed. It never deletes a database/volume.
"""
import argparse
import datetime
import json
from pathlib import Path
import shutil
import subprocess
import time

GIB = 1024 ** 3
RECIPE = "registry-node-scale-v1"


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def inspect(name):
    return json.loads(command("docker", "inspect", name))[0]


def save(path, value):
    with path.open("x") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")


def verify(container, cpu, memory, source_head=None):
    labels = container["Config"].get("Labels") or {}
    assert labels.get("bigname.node-scale.recipe") == RECIPE, "container is not labelled as this disposable recipe"
    if source_head:
        assert labels.get("bigname.node-scale.source-head") == source_head, "phase source does not match baseline"
    limits = container["HostConfig"]
    assert limits["NanoCpus"] == cpu * 1_000_000_000, "unexpected CPU limit"
    assert limits["Memory"] == memory, "unexpected memory limit"
    assert limits["MemorySwap"] == memory, "phase may use swap beyond its memory allocation"
    assert not limits["Privileged"], "benchmark container must not be privileged"


def attestation(container):
    return {"id": container["Id"], "image": container["Image"], "state": container["State"],
            "labels": container["Config"].get("Labels"),
            "limits": {key: container["HostConfig"][key] for key in ["NanoCpus", "Memory", "MemorySwap", "Privileged"]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="operation", required=True)
    baseline = subs.add_parser("baseline")
    baseline.add_argument("--filesystem-path", type=Path, required=True)
    baseline.add_argument("--output", type=Path, required=True)
    baseline.add_argument("--original-preflight", type=Path, help="Preserve the earlier physical preflight as the accounting baseline")
    run = subs.add_parser("run")
    run.add_argument("--baseline", type=Path, required=True)
    run.add_argument("--postgres-container", required=True)
    run.add_argument("--phase-container", required=True)
    run.add_argument("--kind", choices=["prepare", "seed", "interpret", "project"], required=True)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    head = command("git", "rev-parse", "HEAD")
    assert not command("git", "status", "--porcelain=v1"), "resource evidence requires a clean committed checkout"
    if args.operation == "baseline":
        path = args.filesystem_path.resolve(strict=True)
        free = shutil.disk_usage(path).free
        assert free >= 100 * GIB, "less than 100GiB free before build/fixture creation"
        prior = json.loads(args.original_preflight.read_text()) if args.original_preflight else None
        accounting_free = prior["free_bytes_before_new_build_resources"] if prior else free
        assert accounting_free-free <= 60*GIB, "original resource envelope already exceeded"
        save(args.output, {"source_head": head, "filesystem_path": str(path), "free_bytes": accounting_free,
             "measured_free_bytes_at_source_seal": free, "original_preflight": prior,
             "recorded_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
             "instruction": "Original physical baseline includes new image/setup/cache/clone/target/database/WAL bytes."})
        return
    base = json.loads(args.baseline.read_text())
    assert base["source_head"] == head
    assert args.postgres_container != args.phase_container
    pg = inspect(args.postgres_container)
    phase = inspect(args.phase_container)
    verify(pg, 4, 8*GIB)
    verify(phase, 2, 4*GIB, head)
    assert pg["State"]["Running"], "disposable PostgreSQL is not running"
    assert phase["State"]["Status"] == "created", "each measurement needs a fresh phase container for cgroup peak accounting"
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "container-attestation.json", {"postgres": attestation(pg), "phase": attestation(phase), "baseline": base})
    minimum_free = shutil.disk_usage(base["filesystem_path"]).free
    assert minimum_free >= 100*GIB and base["free_bytes"]-minimum_free <= 60*GIB, "resource envelope exceeded before phase start"
    started = time.monotonic()
    reason = None
    with (args.output / "phase.log").open("x") as output, (args.output / "samples.jsonl").open("x") as samples:
        process = subprocess.Popen(["docker", "start", "--attach", args.phase_container], stdout=output, stderr=subprocess.STDOUT)
        while process.poll() is None:
            free = shutil.disk_usage(base["filesystem_path"]).free
            minimum_free = min(minimum_free, free)
            sample = {"elapsed_seconds": time.monotonic()-started, "free_bytes": free,
                      "additional_physical_bytes": max(0,base["free_bytes"]-free)}
            stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", args.postgres_container, args.phase_container], text=True, capture_output=True)
            sample["docker_stats"] = [json.loads(row) for row in stats.stdout.splitlines()] if stats.returncode == 0 else None
            samples.write(json.dumps(sample)+"\n")
            samples.flush()
            if free < 100*GIB or base["free_bytes"]-free > 60*GIB:
                reason = "physical space boundary crossed"
                subprocess.run(["docker", "stop", "--time", "5", args.phase_container, args.postgres_container], check=True)
                break
            if time.monotonic()-started > 6*3600:
                reason = "six-hour phase deadline crossed"
                subprocess.run(["docker", "stop", "--time", "5", args.phase_container], check=True)
                break
            time.sleep(1)
        code = process.wait()
    minimum_free = min(minimum_free, shutil.disk_usage(base["filesystem_path"]).free)
    final = inspect(args.phase_container)
    final_postgres = inspect(args.postgres_container)
    report = {"source_head": head, "kind": args.kind, "exit_code": code, "stop_reason": reason,
              "container_exit_code": final["State"]["ExitCode"], "oom_killed": final["State"]["OOMKilled"],
              "postgres_running": final_postgres["State"]["Running"],
              "postgres_oom_killed": final_postgres["State"]["OOMKilled"],
              "elapsed_seconds": time.monotonic()-started, "minimum_free_bytes": minimum_free,
              "additional_physical_peak_bytes": max(0,base["free_bytes"]-minimum_free),
              "feature_gate_complete": False}
    save(args.output / "resource-report.json", report)
    print(json.dumps(report))
    assert minimum_free >= 100*GIB and base["free_bytes"]-minimum_free <= 60*GIB, report
    assert not reason and code == 0 and final["State"]["ExitCode"] == 0 and not final["State"]["OOMKilled"], report
    assert final_postgres["State"]["Running"] and not final_postgres["State"]["OOMKilled"], report


if __name__ == "__main__":
    main()
