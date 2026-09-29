#!/usr/bin/env python3
"""Reuse successful merge-queue validation for an identical push to main."""

import json
import os
import subprocess


def queue_run(env, fetch):
    if env.get("GITHUB_EVENT_NAME") != "push" or env.get("GITHUB_REF") != "refs/heads/main":
        return None
    repository, sha = env["GITHUB_REPOSITORY"], env["GITHUB_SHA"]
    endpoint = (
        f"repos/{repository}/actions/workflows/ci.yml/runs"
        f"?event=merge_group&head_sha={sha}&status=success&per_page=100"
    )
    try:
        runs = fetch(endpoint)["workflow_runs"]
        for run in runs:
            if (
                run.get("event") == "merge_group"
                and run.get("status") == "completed"
                and run.get("conclusion") == "success"
                and run.get("head_sha") == sha
                and run.get("head_repository", {}).get("full_name") == repository
                and run.get("path", "").split("@")[0] == ".github/workflows/ci.yml"
                and run.get("head_branch", "").startswith("gh-readonly-queue/main/")
            ):
                return run
    except (KeyError, TypeError, ValueError, subprocess.SubprocessError, OSError) as error:
        print(f"Could not confirm prior queue validation; running all checks: {error}")
    return None


def fetch_runs(endpoint):
    result = subprocess.run(
        ["gh", "api", endpoint], check=True, capture_output=True, text=True, timeout=30
    )
    return json.loads(result.stdout)


def main():
    run = queue_run(os.environ, fetch_runs)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"reuse={'true' if run else 'false'}\n")
    if run:
        message = (
            f"Validation already passed for {os.environ['GITHUB_SHA']} in "
            f"{run['html_url']}. This push refreshes caches and artifacts without "
            "running the checks again."
        )
    else:
        message = "Running the complete CI checks for this event."
    print(message)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write(message + "\n")


if __name__ == "__main__":
    main()
