#!/usr/bin/env python3
"""Exercise the boundary between a validated merge and an unvalidated push."""

import copy
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest


sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "ci_check_plan", Path(__file__).resolve().parents[1] / "ci-check-plan.py"
)
plan = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan)


class QueueValidation(unittest.TestCase):
    def setUp(self):
        self.env = {
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_SHA": "a" * 40,
            "GITHUB_REPOSITORY": "ensdomains/bigname",
        }
        self.run = {
            "event": "merge_group",
            "status": "completed",
            "conclusion": "success",
            "head_sha": self.env["GITHUB_SHA"],
            "head_repository": {"full_name": self.env["GITHUB_REPOSITORY"]},
            "path": ".github/workflows/ci.yml",
            "head_branch": "gh-readonly-queue/main/pr-995-previous",
            "html_url": "https://github.com/ensdomains/bigname/actions/runs/123",
        }

    def select(self, runs):
        def fetch(endpoint):
            self.assertEqual(
                endpoint,
                "repos/ensdomains/bigname/actions/workflows/ci.yml/runs"
                f"?event=merge_group&head_sha={'a' * 40}&status=success&per_page=100",
            )
            return {"workflow_runs": runs}

        return plan.queue_run(self.env, fetch)

    def test_same_commit_success_is_reused(self):
        self.assertEqual(self.select([self.run]), self.run)

    def test_pr_and_queue_events_always_run_the_checks(self):
        for event in ["pull_request", "merge_group", "workflow_dispatch"]:
            with self.subTest(event=event):
                self.env["GITHUB_EVENT_NAME"] = event
                self.assertIsNone(plan.queue_run(self.env, lambda _: self.fail("API called")))

    def test_other_push_refs_never_reuse(self):
        self.env["GITHUB_REF"] = "refs/heads/topic"
        self.assertIsNone(plan.queue_run(self.env, lambda _: self.fail("API called")))

    def test_absent_queue_result_runs_full_validation(self):
        self.assertIsNone(self.select([]))

    def test_failed_incomplete_or_unrelated_evidence_is_not_reused(self):
        differences = {
            "event": ["push", "pull_request"],
            "status": ["queued", "in_progress"],
            "conclusion": ["failure", "cancelled", "skipped", None],
            "head_sha": ["b" * 40],
            "head_repository": [{"full_name": "someone/bigname"}],
            "path": [".github/workflows/docker.yml"],
            "head_branch": ["main", "gh-readonly-queue/release/pr-995-previous"],
        }
        for key, values in differences.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    run = copy.deepcopy(self.run)
                    run[key] = value
                    self.assertIsNone(self.select([run]))

    def test_matching_success_can_follow_an_unrelated_result(self):
        unrelated = dict(self.run, head_sha="b" * 40)
        self.assertEqual(self.select([unrelated, self.run]), self.run)

    def test_api_failure_or_invalid_payload_runs_full_validation(self):
        def failure(_):
            raise subprocess.CalledProcessError(1, ["gh", "api"])

        self.assertIsNone(plan.queue_run(self.env, failure))
        self.assertIsNone(plan.queue_run(self.env, lambda _: {}))
        self.assertIsNone(plan.queue_run(self.env, lambda _: {"workflow_runs": None}))


if __name__ == "__main__":
    unittest.main()
