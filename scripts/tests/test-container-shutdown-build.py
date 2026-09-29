#!/usr/bin/env python3
"""Check the real script's build-only boundary without launching services."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "test-container-shutdown"
STUB = '''#!/usr/bin/env python3
import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["COMMAND_LOG"], "a") as log:
    log.write(json.dumps([name] + args) + "\\n")
if name == "git" and "rev-parse" in args:
    print(os.environ["BIGNAME_E2E_SHUTDOWN_SOURCE_SHA"])
if name == "docker" and args[:2] == ["buildx", "inspect"]:
    sys.exit(1)
if name == "docker" and args[:2] == ["buildx", "build"]:
    for arg in args:
        if arg.startswith("type=local,dest="):
            dest = pathlib.Path(arg.split("dest=", 1)[1].split(",", 1)[0])
            dest.mkdir(parents=True)
            (dest / "index.json").write_text("{}")
'''


class BuildOnly(unittest.TestCase):
    def exercise(self, build_only):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ["git", "docker", "cargo"]:
                stub = root / name
                stub.write_text(STUB)
                stub.chmod(0o755)
            log = root / "commands.jsonl"
            env = dict(os.environ)
            env.update({
                "PATH": str(root) + os.pathsep + env["PATH"],
                "COMMAND_LOG": str(log),
                "BIGNAME_E2E_SHUTDOWN_SOURCE_SHA": "a" * 40,
                "BIGNAME_E2E_SHUTDOWN_EVIDENCE": str(root / "evidence"),
                "BIGNAME_E2E_SHUTDOWN_BUILD_CACHE": str(root / "cache"),
            })
            result = subprocess.run(
                ["bash", str(SCRIPT)] + (["--build-only"] if build_only else []),
                env=env, text=True, capture_output=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertTrue(any(call[:3] == ["docker", "buildx", "build"] for call in calls))
            self.assertTrue((root / "cache" / "index.json").exists())
            self.assertTrue(any(call[:3] == ["docker", "buildx", "rm"] for call in calls))
            tests = [call for call in calls if call[0] == "cargo"]
            if build_only:
                self.assertEqual(tests, [])
                self.assertIn("no shutdown scenario was run", result.stdout)
            else:
                self.assertEqual(len(tests), 1)
                self.assertEqual(tests[0][1], "test")
                self.assertIn(
                    "scenarios::service_shutdown::api_signals_drain_accepted_indexed_name_read",
                    tests[0],
                )
                self.assertIn("--exact", tests[0])

    def test_build_only_exports_cache_without_running_tests(self):
        self.exercise(True)

    def test_normal_mode_still_runs_the_shutdown_scenario(self):
        self.exercise(False)


if __name__ == "__main__":
    unittest.main()
