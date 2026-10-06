#!/usr/bin/env python3
"""Actual HTTP screen for a published local node-scale fixture.

The smoke form validates response shape and sample ownership only. Measurement
mode retains each cohort separately and cannot certify the production profile.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def request(base, path, query=None, body=None):
    url = base.rstrip("/") + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    request_body = body
    outgoing = urllib.request.Request(url, data=canonical(body) if body is not None else None,
                                     headers={"Content-Type": "application/json"} if body is not None else {})
    started = time.perf_counter()
    try:
        response = urllib.request.urlopen(outgoing, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = json.load(response)
        status = response.status
    return {"url": url, "request_body": request_body, "status": status, "milliseconds": (time.perf_counter()-started)*1000,
            "body": body, "response_sha256": hashlib.sha256(canonical(body)).hexdigest()}


def save(directory, name, value):
    path = directory / name
    with path.open("x") as handle:
        json.dump(value, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def percentile(samples, fraction):
    return sorted(samples)[max(0, math.ceil(len(samples)*fraction)-1)]



def route_matrix(args, manifest, results):
    namespace = manifest["namespace"]
    visible = [sample for sample in manifest["samples"]
               if args.epoch != "bytes" or sample["bytes_epoch_visible"]]
    # Choose distinct combinations from the actual saved strata. Every sample
    # still receives a detail read; this matrix adds collection/record routes.
    strata = {}
    for sample in visible:
        key = (sample["depth"], sample["byte_observation"], sample["resolver_cohort"])
        strata.setdefault(key, sample)
    selected = list(strata.values())
    retained = []

    def checked(label, path, query=None, body=None):
        result = request(args.base_url, path, query, body)
        save(args.output, "route-" + label + ".json", result)
        assert result["status"] == 200, result
        retained.append({"case": label, **{key: value for key, value in result.items() if key != "body"}})
        return result["body"]

    for sample in selected:
        ordinal = sample["ordinal"]
        path = "/v1/names/" + urllib.parse.quote(sample["input"], safe="")
        records = checked(f"records-{ordinal}", path + "/records",
                          {"namespace": namespace, "source": "indexed", "keys": "text:url", "include": "inventory"})
        expected_value = None if args.epoch == "structural" else sample["changed_text_url"]
        assert records["data"]["records"]["text:url"].get("value") == expected_value, records
        for scope in ["name", "registration", "both"]:
            history = checked(f"history-{ordinal}-{scope}", path + "/history",
                              {"namespace": namespace, "scope": scope, "include": "data", "page_size": 200})
            assert history["data"], history
        for size in [1, 50, 200]:
            query = {"namespace": namespace, "include": "counts", "page_size": size}
            children = checked(f"subnames-{ordinal}-{size}", path + "/subnames", query)
            assert children["page"]["total_count"] == sample["direct_children"], children
            assert len(children["data"]) == min(size, sample["direct_children"]), children
            if children["page"]["has_more"]:
                query["cursor"] = children["page"]["next_cursor"]
                after = checked(f"subnames-{ordinal}-{size}-continuation", path + "/subnames", query)
                assert after["data"], after
                first_ids = {row["namehash"] for row in children["data"]}
                assert not first_ids.intersection(row["namehash"] for row in after["data"]), after
    for profile in ["detail", "feed"]:
        # Keep batches within the existing public bound; no server knob changes.
        for offset in range(0, len(visible), 20):
            samples = visible[offset:offset+20]
            body = {"namespace": namespace, "profile": profile,
                    "inputs": [{"id": str(sample["ordinal"]), "name": sample["input"]} for sample in samples]}
            lookup = checked(f"lookup-{profile}-{offset}", "/v1/lookup", body=body)
            assert len(lookup["data"]) == len(samples), lookup
            for row, sample in zip(lookup["data"], samples):
                record = row["record"]
                assert row["status"] == "ok" and record["namehash"] == sample["namehash"], row
                if profile == "detail":
                    assert record["owner"].lower() == sample["owner"].lower(), row
                else:
                    assert "owner" not in record, row
    for owner in sorted({sample["owner"] for sample in selected})[:10]:
        names = checked("address-" + owner, f"/v1/addresses/{owner}/names",
                        {"namespace": namespace, "relation": "owner", "page_size": 200})
        assert names["data"] and all(row["owner"].lower() == owner.lower() for row in names["data"]), names
    if args.epoch != "structural":
        for resolver in sorted({sample["changed_resolver"] for sample in selected if sample["changed_resolver"]}):
            body = checked("resolver-" + resolver, f"/v1/resolvers/{manifest['chain_id']}/{resolver}", {"page_size": 200})
            assert body["data"]["bound_names"]["data"], body
            assert all(row["resolver"]["address"].lower() == resolver.lower() for row in body["data"]["bound_names"]["data"]), body
    # Include every expiry tie and both orderings. A structural child with no
    # lease must not be invented in the expiry result.
    for order in ["asc", "desc"]:
        body = checked("expiry-" + order, "/v1/names", {"namespace": namespace,
                       "expires_after": 2000000000, "expires_before": 2000000000+4*86400,
                       "order": order, "page_size": 200})
        if args.epoch != "bytes":
            assert not body["data"], body
        else:
            assert body["data"], body
            expiries = [int(row["expires_at"]) for row in body["data"]]
            assert all(2000000000 <= value < 2000000000+4*86400 for value in expiries), body
            assert expiries == sorted(expiries, reverse=order == "desc"), body
    results["routes"] = retained


def measure_search(args, results, cohort, query, first):
    assert first["status"] == 200, first
    for concurrency in ([1] if args.smoke else [1, 2, 4]):
        count = 1 if args.smoke else args.observations
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            observations = list(executor.map(lambda _: request(args.base_url, "/v1/search", query), range(count)))
        assert all(item["status"] == 200 for item in observations), cohort
        assert all(item["response_sha256"] == first["response_sha256"] for item in observations), f"stable-publication response changed: {cohort}"
        samples = [item["milliseconds"] for item in observations]
        p50, p95, p99 = [percentile(samples, fraction) for fraction in [0.50, 0.95, 0.99]]
        results["search"].append({"cohort": cohort, "concurrency": concurrency,
            "returned_hits": len(first["body"]["data"]), "first_request_ms": first["milliseconds"],
            "samples_ms": samples, "p50_ms": p50, "p95_ms": p95, "p99_ms": p99,
            "warm_budget_pass": None if args.smoke else p50 <= 25 and p95 <= 75 and p99 <= 150,
            "response_sha256": first["response_sha256"]})
    save(args.output, f"progress-{cohort}.json", results["search"][-(1 if args.smoke else 3):])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--epoch", choices=["structural", "changed", "bytes"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--observations", type=int, default=200)
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.base_url)
    assert parsed.scheme == "http" and parsed.hostname in ["127.0.0.1", "localhost", "::1"], "requires a disposable loopback API"
    assert not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
    assert args.smoke or args.observations >= 200, "measurement requires at least 200 observations per cohort"
    args.output.mkdir(parents=True, exist_ok=False)
    corpus = json.loads((args.corpus / "corpus.json").read_text())
    manifest = json.loads((args.corpus / "samples.json").read_text())
    namespace = manifest["namespace"]
    results = {"source_head": corpus["source_head"], "interpreter_content_hash": corpus["interpreter_content_hash"],
               "raw_digest": corpus["raw"]["rolling_keccak256"], "epoch": args.epoch,
               "smoke": args.smoke, "feature_gate_complete": False, "samples": [], "search": []}
    for sample in manifest["samples"]:
        path = "/v1/names/" + urllib.parse.quote(sample["input"], safe="")
        result = request(args.base_url, path, {"namespace": namespace, "source": "indexed", "include": "counts"})
        expected_status = 404 if args.epoch == "bytes" and not sample["bytes_epoch_visible"] else 200
        save(args.output, f"detail-{sample['ordinal']}.json", result)
        assert result["status"] == expected_status, result
        if expected_status == 200:
            row = result["body"]["data"]
            assert row["namehash"] == sample["namehash"], result
            assert row["owner"].lower() == sample["owner"].lower(), result
            expected_registration = "wrapped" if args.epoch == "bytes" and sample["byte_observation"] == "valid" else "registered"
            assert row["registration_status"] == expected_registration, result
            if args.epoch == "structural":
                assert row.get("expires_at") is None and row.get("registered_at") is None, result
            if sample["direct_children"]:
                assert row["subname_count"] == sample["direct_children"], result
        results["samples"].append({key: value for key, value in result.items() if key != "body"})

    route_matrix(args, manifest, results)

    # A unique node-only root leaf gives the rare contains case. Its hash is a
    # supported text fragment; bracketed search fragments remain out of scope.
    rare = next(sample for sample in manifest["samples"] if sample["byte_observation"] == "none" and sample["direct_children"] == 0 and sample["depth"] == 1)
    rare_hash = rare["input"].split(".")[0][1:-1]
    deep = next(sample for sample in manifest["samples"] if sample["depth"] == 16)
    deep_hash = deep["input"].split(".")[0][1:-1]
    terms = [("contains-common", "contains", "a"), ("contains-deep", "contains", deep_hash),
             ("contains-raw", "contains", "root-"), ("contains-rare", "contains", rare_hash),
             ("contains-no-hit", "contains", "zz-node-scale-absent-zz"),
             ("prefix-raw", "prefix", "root-"), ("prefix-no-hit", "prefix", "zz-node-scale-absent-zz")]
    for label, match, term in terms:
        for page_size in [1, 50, 200]:
            for scoped in [False, True]:
                query = {"q": term, "match": match, "page_size": page_size}
                if scoped:
                    query["namespace"] = namespace
                first = request(args.base_url, "/v1/search", query)
                cohort = f"{label}-size{page_size}-{'scoped' if scoped else 'bare'}"
                save(args.output, f"search-{cohort}.json", first)
                assert first["status"] == 200, first
                assert isinstance(first["body"]["data"], list), first
                hits = len(first["body"]["data"])
                if "no-hit" in label:
                    assert hits == 0, first
                if label == "contains-rare":
                    assert hits == 1 and first["body"]["data"][0]["namehash"] == rare["namehash"], first
                measure_search(args, results, cohort, query, first)
                page = first["body"]["page"]
                if page["has_more"]:
                    continuation_query = {**query, "cursor": page["next_cursor"]}
                    after = request(args.base_url, "/v1/search", continuation_query)
                    save(args.output, f"search-{cohort}-continuation.json", after)
                    assert after["status"] == 200 and after["body"]["data"], after
                    first_ids = {row["namehash"] for row in first["body"]["data"]}
                    assert not first_ids.intersection(row["namehash"] for row in after["body"]["data"]), after
                    measure_search(args, results, cohort + "-continuation", continuation_query, after)
    save(args.output, "http-report.json", results)
    assert args.smoke or all(row["warm_budget_pass"] for row in results["search"]), "supported search exceeds the unchanged 25/75/150ms budget"
    print(json.dumps({"sample_count": len(results["samples"]), "search_cohorts": len(results["search"]), "smoke": args.smoke, "feature_gate_complete": False}))


if __name__ == "__main__":
    main()
