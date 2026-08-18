#!/usr/bin/env python3
"""
Minimal DAG engine — runs the JSON pipeline definition with stubbed task handlers.

Purpose: demonstrate that the JSON DAG is a real executable contract, not a diagram.
Shows: dependency resolution, topological ordering, cycle detection, per-node retry
with exponential backoff, error routing to a dead-letter node, secret indirection,
and run metrics.

Usage:
    python engine.py pipelines/doc_rag_ingest.json
    python engine.py pipelines/doc_rag_ingest.json --fail extract_text
"""

import argparse
import json
import os
import random
import sys
import time
from collections import defaultdict


class PipelineError(Exception):
    pass


# --- secret handling -------------------------------------------------------
# Secrets are referenced, never inlined. The engine resolves secret_ref at run
# time from the environment (a real deployment would hit a vault/secret store).

def resolve_secret(ref):
    key = ref.split("/")[-1].upper()
    val = os.environ.get(key)
    return f"<resolved:{key}>" if val else f"<missing:{key}>"


def resolve_connections(conns):
    resolved = {}
    for name, cfg in conns.items():
        entry = dict(cfg)
        if "secret_ref" in entry:
            entry["credential"] = resolve_secret(entry.pop("secret_ref"))
        resolved[name] = entry
    return resolved


# --- graph -----------------------------------------------------------------

def topo_sort(nodes):
    """Kahn's algorithm. Raises on cycles or dangling dependencies."""
    ids = {n["id"] for n in nodes}
    # Routed nodes (dead_letter) are not part of the main flow.
    flow = [n for n in nodes if n.get("trigger") != "on_route"]
    indeg = {n["id"]: 0 for n in flow}
    adj = defaultdict(list)

    for n in flow:
        for dep in n.get("depends_on", []):
            if dep not in ids:
                raise PipelineError(f"node '{n['id']}' depends on unknown node '{dep}'")
            adj[dep].append(n["id"])
            indeg[n["id"]] += 1

    queue = [nid for nid, d in indeg.items() if d == 0]
    order = []
    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for nxt in adj[nid]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)

    if len(order) != len(flow):
        stuck = [nid for nid, d in indeg.items() if d > 0]
        raise PipelineError(f"cycle detected among nodes: {stuck}")
    return order


# --- task handlers (stubbed) ----------------------------------------------
# Each returns the outputs its node declares. In a real deployment these are
# the platform's task library; the DAG contract above is unchanged.

def run_task(node, ctx, force_fail):
    task = node["task"]
    nid = node["id"]

    if nid == force_fail:
        raise RuntimeError(f"injected failure in '{nid}'")

    if task == "connector.s3.list":
        docs = [f"inbound/report_{i}.pdf" for i in range(1, 13)]
        return {"documents": docs}
    if task == "transform.document.extract_text":
        return {"text": "x" * 4200, "doc_id": "doc-001", "page_count": 14}
    if task == "control.assert":
        for a in node["params"]["assertions"]:
            pass  # expressions evaluated against ctx in a real engine
        return {}
    if task == "transform.text.chunk":
        return {"chunks": list(range(148))}
    if task == "ai.embed":
        # Simulates a flaky upstream API — exercises the retry policy.
        if random.random() < 0.45:
            raise RuntimeError("429 rate limited by embedding API")
        return {"vectors": 148}
    if task == "connector.pgvector.upsert":
        return {"rows_written": 148}
    if task == "connector.s3.put":
        return {"written": True}
    if task == "observability.emit":
        return {}
    raise PipelineError(f"no handler for task type '{task}'")


def execute_node(node, ctx, defaults, force_fail, log):
    policy = {**defaults.get("retry", {}), **node.get("retry", {})}
    attempts = policy.get("max_attempts", 1)
    delay = policy.get("initial_delay_s", 1)

    for attempt in range(1, attempts + 1):
        try:
            out = run_task(node, ctx, force_fail)
            log(f"  ok    {node['id']}" + (f" (attempt {attempt})" if attempt > 1 else ""))
            return out
        except Exception as e:
            if attempt == attempts:
                log(f"  FAIL  {node['id']} after {attempt} attempt(s): {e}")
                raise
            log(f"  retry {node['id']} attempt {attempt} failed: {e} -> sleeping {delay}s")
            time.sleep(min(delay, 0.4))  # shortened for demo
            delay *= 2 if policy.get("backoff") == "exponential" else 1


# --- runner ----------------------------------------------------------------

def run(path, force_fail=None, seed=7):
    random.seed(seed)
    with open(path) as f:
        spec = json.load(f)

    nodes = {n["id"]: n for n in spec["nodes"]}
    defaults = spec.get("defaults", {})
    lines = []
    log = lambda m: (lines.append(m), print(m))[0]

    log(f"pipeline: {spec['pipeline']} v{spec['version']}")
    conns = resolve_connections(spec.get("connections", {}))
    for name, c in conns.items():
        log(f"  conn  {name:<10} {c['type']:<9} cred={c.get('credential', 'none')}")

    order = topo_sort(spec["nodes"])
    log(f"  order {' -> '.join(order)}")
    log("")

    ctx, dead_letter, started = {}, [], time.time()
    for nid in order:
        node = nodes[nid]
        try:
            ctx[nid] = execute_node(node, ctx, defaults, force_fail, log)
        except Exception as e:
            action = node.get("on_error", defaults.get("on_error", "fail"))
            if action == "route":
                route = node["error_route"]
                dead_letter.append({"node": nid, "error": str(e)})
                log(f"  route {nid} -> {route}")
                execute_node(nodes[route], ctx, defaults, None, log)
                log(f"  halt  downstream of {nid} skipped for this item")
                break
            log(f"  abort pipeline failed at {nid}")
            return {"status": "failed", "failed_node": nid, "log": lines}

    duration_ms = int((time.time() - started) * 1000)
    result = {
        "status": "failed_with_dead_letter" if dead_letter else "success",
        "docs_processed": len(ctx.get("list_documents", {}).get("documents", [])),
        "chunks_indexed": ctx.get("upsert_index", {}).get("rows_written", 0),
        "dead_lettered": len(dead_letter),
        "run_duration_ms": duration_ms,
    }
    log("")
    log(f"result: {json.dumps({k: v for k, v in result.items()})}")
    if dead_letter:
        log(f"ALERT: {len(dead_letter)} item(s) dead-lettered -> {dead_letter}")
    result["log"] = lines
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("pipeline")
    ap.add_argument("--fail", dest="fail", default=None,
                    help="inject a failure at this node id to demo error routing")
    a = ap.parse_args()
    r = run(a.pipeline, a.fail)
    sys.exit(0 if r["status"] == "success" else 1)
