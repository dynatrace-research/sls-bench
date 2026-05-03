"""
Synthetic log generator for a modeled Cloudflare MCP edge incident (BGP policy term reordering).

High-level plan
1) Embed the provided System Description and Scenario as Python dictionaries: SYSTEM and SCENARIO.
   - Components include service identity (svc), hosts, log templates (lvl/msg/vars/state_vars), and
     background emission behavior (beh) for normal (n) and failure (f).
   - Flows include per-minute request rates (rpm), per-attempt emission sequences, latency pairs, retry
     behavior (max/expected attempts, per-retry logs, backoff), and tracing flags.
   - Scenario includes the two-phase timeline (normal then failure), failure events with persistent
     rate/latency multipliers, and one-shot operational markers.

2) Deterministic simulation
   - Fixed seed for both Python's random and NumPy Generator for full reproducibility.
   - Scenario minute 0 maps to 2026-03-13T12:00:00.000Z (UTC).
   - Generate logs for each minute in each phase:
       a) Background logs via per-minute Poisson counts; timestamps uniform within the minute.
       b) Flow instances via per-minute Poisson counts from effective rpm; start times uniform within minute.

3) Failure event controller (piecewise multipliers with persistence)
   - At failure start, all multipliers are 1.0.
   - When an event specifies rate_multipliers or latency_multipliers, they override the active values
     from that event minute onward until overridden again.
   - Rate multipliers apply to:
       - failure flows (scale rpm)
       - failure background emits (scale per_min for that component.log_id)
   - Latency multipliers apply to:
       - failure flow latencies (scale p50 and p95 for every emission step in the flow)

4) Flow semantics, retries, and tracing
   - Each flow instance creates a stable context (req_id, trace_id if enabled, colo, cache_key, origin_host/ip, etc.).
   - Attempt count sampling matches expected_attempts using a floor/ceil mixture, bounded by max_attempts.
   - Per-attempt logs appear in the specified order with delays sampled from lognormal distributions derived
     from (p50, p95), with a soft cap at ~3x p95 and small jitter.
   - Retry-only logs (emit_per_retry) are emitted once per retry attempt (attempt 2..A), after attempt end
     and before the next attempt begins, and include the sampled backoff.

5) Variable coherence
   - Prefer context values when available; otherwise sample from template domains.
   - Override key variables for causal consistency (e.g., timeout flows produce 52x/5xx status codes,
     upstream_error err_type aligns with the flow).

6) Output
   - Write logs to logs.csv with columns: timestamp, level, message, trace_id, service, host
   - Rows are sorted ascending by timestamp, timestamps are ISO 8601 with milliseconds and Z suffix.

Notes
- External components (upstream_peer, customer_origin) have templates but emit no logs (as modeled).
- One-shots are emitted at the event time with small sub-second jitter and are not scaled by rate multipliers.
- Background emits with scope=global still attach a host identity when the component has hosts (choose hosts[0]).
"""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network
from typing import Any, Dict, List, Optional, Tuple
import uuid

import numpy as np
import pandas as pd


# -----------------------------
# Determinism
# -----------------------------
SEED = 1337
random.seed(SEED)
RNG = np.random.default_rng(SEED)

BASE_TIME = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)


# -----------------------------
# Embedded inputs (SYSTEM, SCENARIO)
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {
        "id": "cloudflare_mcp_edge",
        "desc": (
            "A representative model of Cloudflare edge traffic handling in Multi-Colo PoPs (MCP), "
            "where HTTP requests are terminated at the edge, served from cache when possible, and "
            "otherwise routed to internal compute clusters and/or customer origins."
        ),
    },
    "states": {"n": "normal", "f": "failure"},
    "components": [
        {
            "id": "edge_proxy",
            "name": "Edge Proxy (HTTP reverse proxy)",
            "svc": "edge-proxy",
            "hosts": ["edge-mcp-01", "edge-mcp-02", "edge-mcp-03", "edge-mcp-04", "edge-mcp-05", "edge-mcp-06"],
            "to": [{"dst": "cache_cluster", "proto": "grpc"}, {"dst": "multimog_lb", "proto": "grpc"}],
            "logs": {
                "req_received": {
                    "desc": "Client request accepted at the edge and assigned correlation IDs.",
                    "lvl": "INFO",
                    "msg": "recv {method} {path} from {client_ip} ua={ua} req_id={req_id} trace_id={trace_id} colo={colo}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "HEAD"]},
                        "path": {"k": "str", "v": "url_path"},
                        "client_ip": {"k": "ip", "v": "0.0.0.0/0"},
                        "ua": {"k": "str", "v": "user_agent"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "colo": {
                            "k": "ch",
                            "v": [
                                "ams07", "atl01", "iad03", "ord02", "fra02", "lhr02", "lax02", "mad01",
                                "man01", "mia01", "mil01", "bom01", "ewr01", "kix01", "gru01", "sjc01",
                                "sin01", "syd01", "nrt01",
                            ],
                        },
                    },
                },
                "resp_sent": {
                    "desc": "Final response sent to the client including cache outcome and timing.",
                    "lvl": "INFO",
                    "msg": "sent {status_code} bytes={bytes} dur_ms={dur_ms} req_id={req_id} trace_id={trace_id} colo={colo} cache={cache_status}",
                    "vars": {
                        "bytes": {"k": "i", "v": [200, 500000]},
                        "dur_ms": {"k": "i", "v": [0, 60000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "colo": {
                            "k": "ch",
                            "v": [
                                "ams07", "atl01", "iad03", "ord02", "fra02", "lhr02", "lax02", "mad01",
                                "man01", "mia01", "mil01", "bom01", "ewr01", "kix01", "gru01", "sjc01",
                                "sin01", "syd01", "nrt01",
                            ],
                        },
                        "cache_status": {"k": "ch", "v": ["HIT", "MISS", "BYPASS"]},
                    },
                    "state_vars": {
                        "n": {"status_code": {"k": "ch", "v": [200, 206, 301, 302, 304, 404]}},
                        "f": {"status_code": {"k": "ch", "v": [200, 206, 301, 302, 304, 404, 500, 502, 503, 504, 520, 522, 523]}},
                    },
                },
                "upstream_error": {
                    "desc": "Upstream/internal dependency error annotated for diagnostics.",
                    "lvl": "ERROR",
                    "msg": "upstream error {err_type} detail={detail} req_id={req_id} trace_id={trace_id} colo={colo}",
                    "vars": {
                        "err_type": {"k": "ch", "v": ["origin_timeout", "origin_connect", "lb_no_route", "backend_overload"]},
                        "detail": {"k": "str", "v": "err_detail"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "colo": {
                            "k": "ch",
                            "v": [
                                "ams07", "atl01", "iad03", "ord02", "fra02", "lhr02", "lax02", "mad01",
                                "man01", "mia01", "mil01", "bom01", "ewr01", "kix01", "gru01", "sjc01",
                                "sin01", "syd01", "nrt01",
                            ],
                        },
                    },
                },
                "retry_scheduled": {
                    "desc": "Edge-layer retry scheduled for attempt 2+ after an upstream error.",
                    "lvl": "WARN",
                    "msg": "retrying attempt={attempt} after_ms={backoff_ms} reason={reason} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [10, 1000]},
                        "reason": {"k": "ch", "v": ["timeout", "connect_error", "route_unreachable", "overload"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "edge_metrics": {
                    "desc": "Periodic runtime metrics summary per instance.",
                    "lvl": "INFO",
                    "msg": "metrics cpu_pct={cpu_pct} mem_pct={mem_pct} open_conns={open_conns}",
                    "vars": {
                        "cpu_pct": {"k": "f", "v": [1.0, 95.0]},
                        "mem_pct": {"k": "f", "v": [5.0, 98.0]},
                        "open_conns": {"k": "i", "v": [10, 200000]},
                    },
                },
                "edge_conntrack_drops": {
                    "desc": "Kernel/network drops observed by the proxy host.",
                    "lvl": "WARN",
                    "msg": "conntrack drops={drops} reason={reason}",
                    "vars": {
                        "drops": {"k": "i", "v": [0, 20000]},
                        "reason": {"k": "ch", "v": ["nf_conntrack_full", "route_unreachable", "backlog_overflow"]},
                    },
                },
                "edge_gc_pause": {
                    "desc": "Application runtime GC pause report.",
                    "lvl": "DEBUG",
                    "msg": "gc pause_ms={pause_ms} heap_mb={heap_mb}",
                    "vars": {"pause_ms": {"k": "f", "v": [0.1, 500.0]}, "heap_mb": {"k": "f", "v": [50.0, 4000.0]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "edge_metrics", "per_min": 2.0, "scope": "per_host"}, {"id": "edge_gc_pause", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "edge_metrics", "per_min": 2.0, "scope": "per_host"}, {"id": "edge_conntrack_drops", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "cache_cluster",
            "name": "Cache Cluster (edge cache)",
            "svc": "cache-cluster",
            "hosts": ["cache-mcp-01", "cache-mcp-02", "cache-mcp-03", "cache-mcp-04"],
            "to": [{"dst": "origin_fetcher", "proto": "grpc"}, {"dst": "edge_proxy", "proto": "grpc"}],
            "logs": {
                "cache_hit": {
                    "desc": "Cache lookup succeeded and a fresh object was found.",
                    "lvl": "INFO",
                    "msg": "cache hit key={cache_key} ttl_s={ttl_s} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "cache_key": {"k": "hex", "v": 16},
                        "ttl_s": {"k": "i", "v": [1, 86400]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cache_miss": {
                    "desc": "Cache lookup missed or object was bypassed/expired.",
                    "lvl": "INFO",
                    "msg": "cache miss key={cache_key} reason={miss_reason} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "cache_key": {"k": "hex", "v": 16},
                        "miss_reason": {"k": "ch", "v": ["expired", "not_found", "bypass"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "cache_stats": {
                    "desc": "Periodic cache performance counters.",
                    "lvl": "INFO",
                    "msg": "stats hit_rate={hit_rate} items={items} evictions={evictions}",
                    "vars": {"hit_rate": {"k": "f", "v": [0.0, 1.0]}, "items": {"k": "i", "v": [0, 50000000]}, "evictions": {"k": "i", "v": [0, 200000]}},
                },
                "cache_eviction": {
                    "desc": "Eviction event due to memory pressure or TTL constraints.",
                    "lvl": "DEBUG",
                    "msg": "evict key={cache_key} reason={reason} items={items}",
                    "vars": {"cache_key": {"k": "hex", "v": 16}, "reason": {"k": "ch", "v": ["lru", "ttl_expired", "size_limit"]}, "items": {"k": "i", "v": [0, 50000000]}},
                },
                "peer_route_missing": {
                    "desc": "Cache node cannot reach an expected peer over site-local networking.",
                    "lvl": "WARN",
                    "msg": "peer route missing peer_ip={peer_ip} vrf={vrf} action={action}",
                    "vars": {"peer_ip": {"k": "ip", "v": "10.0.0.0/8"}, "vrf": {"k": "ch", "v": ["site-local", "internal"]}, "action": {"k": "ch", "v": ["retry", "fail_open", "drop"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cache_stats", "per_min": 1.5, "scope": "per_host"}, {"id": "cache_eviction", "per_min": 0.3, "scope": "per_host"}]},
                "f": {"emit": [{"id": "cache_stats", "per_min": 1.5, "scope": "per_host"}, {"id": "peer_route_missing", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "origin_fetcher",
            "name": "Origin Fetcher",
            "svc": "origin-fetcher",
            "hosts": ["fetch-mcp-01", "fetch-mcp-02", "fetch-mcp-03"],
            "to": [{"dst": "customer_origin", "proto": "https"}, {"dst": "cache_cluster", "proto": "grpc"}],
            "logs": {
                "fetch_start": {
                    "desc": "Start of an origin fetch attempt.",
                    "lvl": "INFO",
                    "msg": "origin fetch start host={origin_host} ip={origin_ip} req_id={req_id} trace_id={trace_id}",
                    "vars": {"origin_host": {"k": "str", "v": "hostname"}, "origin_ip": {"k": "ip", "v": None}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "fetch_success": {
                    "desc": "Successful origin fetch completed.",
                    "lvl": "INFO",
                    "msg": "origin fetch ok status={origin_status} dur_ms={dur_ms} req_id={req_id} trace_id={trace_id}",
                    "vars": {"origin_status": {"k": "ch", "v": [200, 204, 301, 302, 304, 404, 500]}, "dur_ms": {"k": "i", "v": [1, 20000]}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "fetch_timeout": {
                    "desc": "Origin fetch timed out at a particular stage.",
                    "lvl": "ERROR",
                    "msg": "origin fetch timeout stage={stage} dur_ms={dur_ms} req_id={req_id} trace_id={trace_id}",
                    "vars": {"stage": {"k": "ch", "v": ["connect", "tls", "first_byte"]}, "dur_ms": {"k": "i", "v": [500, 60000]}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "fetch_connect_fail": {
                    "desc": "Immediate origin connect failure (no route, refused, DNS, etc.).",
                    "lvl": "ERROR",
                    "msg": "origin connect failed err={err} req_id={req_id} trace_id={trace_id}",
                    "vars": {"err": {"k": "ch", "v": ["no_route", "network_unreachable", "connection_refused", "dns_nxdomain"]}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "fetcher_metrics": {
                    "desc": "Periodic fetcher performance counters.",
                    "lvl": "INFO",
                    "msg": "metrics qps={qps} inflight={inflight} err_rate={err_rate}",
                    "vars": {"qps": {"k": "f", "v": [0.0, 2000.0]}, "inflight": {"k": "i", "v": [0, 20000]}, "err_rate": {"k": "f", "v": [0.0, 1.0]}},
                },
                "resolver_error": {
                    "desc": "Resolver/egress helper reports upstream resolution/connectivity issues.",
                    "lvl": "WARN",
                    "msg": "resolver error type={type} upstream={upstream} detail={detail}",
                    "vars": {"type": {"k": "ch", "v": ["timeout", "servfail", "network_unreachable"]}, "upstream": {"k": "ip", "v": None}, "detail": {"k": "str", "v": "resolver_err"}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "fetcher_metrics", "per_min": 0.8, "scope": "per_host"}]},
                "f": {"emit": [{"id": "fetcher_metrics", "per_min": 0.8, "scope": "per_host"}, {"id": "resolver_error", "per_min": 0.4, "scope": "per_host"}]},
            },
        },
        {
            "id": "multimog_lb",
            "name": "Multimog (internal load balancer)",
            "svc": "multimog",
            "hosts": ["mmog-mcp-01", "mmog-mcp-02", "mmog-mcp-03"],
            "to": [{"dst": "compute_worker", "proto": "tcp"}, {"dst": "edge_proxy", "proto": "grpc"}],
            "logs": {
                "route_request": {
                    "desc": "Routing decision made for an incoming request.",
                    "lvl": "DEBUG",
                    "msg": "route service={service} decision={decision} backend={backend} req_id={req_id} trace_id={trace_id}",
                    "vars": {"service": {"k": "ch", "v": ["http_app", "workers", "api"]}, "decision": {"k": "ch", "v": ["selected", "fail_open", "fail_closed"]}, "backend": {"k": "str", "v": "cluster_id"}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "forward_fail_no_route": {
                    "desc": "Forwarding failed because the backend was unreachable in the site-local mesh.",
                    "lvl": "ERROR",
                    "msg": "forward failed no route to backend={backend} prefix={prefix} req_id={req_id} trace_id={trace_id}",
                    "vars": {"backend": {"k": "str", "v": "cluster_id"}, "prefix": {"k": "str", "v": "ip_prefix"}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "backend_overloaded": {
                    "desc": "Backend signaled overload; requests are being rejected or queued excessively.",
                    "lvl": "WARN",
                    "msg": "backend overload backend={backend} qdepth={qdepth} shedding={shedding} req_id={req_id} trace_id={trace_id}",
                    "vars": {"backend": {"k": "str", "v": "cluster_id"}, "qdepth": {"k": "i", "v": [0, 20000]}, "shedding": {"k": "ch", "v": ["true", "false"]}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "mesh_route_missing": {
                    "desc": "Background symptom that the site-local mesh lacks required routes.",
                    "lvl": "WARN",
                    "msg": "mesh route missing dst={dst_ip} vrf={vrf} action={action}",
                    "vars": {"dst_ip": {"k": "ip", "v": "10.0.0.0/8"}, "vrf": {"k": "ch", "v": ["site-local", "internal"]}, "action": {"k": "ch", "v": ["drop", "reroute"]}},
                },
                "lb_metrics": {
                    "desc": "Periodic load balancer metrics per instance.",
                    "lvl": "INFO",
                    "msg": "metrics routed={routed} failures={failures} p95_ms={p95_ms}",
                    "vars": {"routed": {"k": "i", "v": [0, 500000]}, "failures": {"k": "i", "v": [0, 200000]}, "p95_ms": {"k": "f", "v": [0.1, 60000.0]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "lb_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "lb_metrics", "per_min": 1.0, "scope": "per_host"}, {"id": "mesh_route_missing", "per_min": 2.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "compute_worker",
            "name": "Compute Worker (service runtime)",
            "svc": "compute-worker",
            "hosts": ["wrk-mcp-01", "wrk-mcp-02", "wrk-mcp-03", "wrk-mcp-04"],
            "to": [{"dst": "multimog_lb", "proto": "tcp"}],
            "logs": {
                "request_rejected_overload": {
                    "desc": "Request rejected because the worker is overloaded.",
                    "lvl": "ERROR",
                    "msg": "reject overload qdepth={qdepth} max={max_qdepth} req_id={req_id} trace_id={trace_id}",
                    "vars": {"qdepth": {"k": "i", "v": [0, 20000]}, "max_qdepth": {"k": "i", "v": [100, 20000]}, "req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "worker_shed_load": {
                    "desc": "Background load shedding report.",
                    "lvl": "WARN",
                    "msg": "shed load reason={reason} qdepth={qdepth} dropped={dropped}",
                    "vars": {"reason": {"k": "ch", "v": ["cpu_saturation", "queue_limit", "upstream_failures"]}, "qdepth": {"k": "i", "v": [0, 20000]}, "dropped": {"k": "i", "v": [0, 200000]}},
                },
                "worker_metrics": {
                    "desc": "Periodic worker metrics.",
                    "lvl": "INFO",
                    "msg": "metrics cpu_pct={cpu_pct} qdepth={qdepth} active={active}",
                    "vars": {"cpu_pct": {"k": "f", "v": [1.0, 99.0]}, "qdepth": {"k": "i", "v": [0, 20000]}, "active": {"k": "i", "v": [0, 20000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_metrics", "per_min": 0.8, "scope": "per_host"}]},
                "f": {"emit": [{"id": "worker_metrics", "per_min": 0.8, "scope": "per_host"}, {"id": "worker_shed_load", "per_min": 0.8, "scope": "per_host"}]},
            },
        },
        {
            "id": "spine_router",
            "name": "MCP Spine Router (BGP + policy)",
            "svc": "mcp-spine",
            "hosts": ["spine-mcp-a", "spine-mcp-b"],
            "to": [{"dst": "upstream_peer", "proto": "bgp"}],
            "logs": {
                "config_commit_applied": {
                    "desc": "Configuration commit applied on a router (used as discrete operational marker).",
                    "lvl": "INFO",
                    "msg": "commit applied commit_id={commit_id} cr_id={cr_id} target={target}",
                    "vars": {"commit_id": {"k": "hex", "v": 8}, "cr_id": {"k": "str", "v": "CRQ-YYYYMMDD-####"}, "target": {"k": "ch", "v": ["mcp_spines"]}},
                },
                "policy_term_order_warning": {
                    "desc": "Policy term order changed in a way that may alter match/accept behavior.",
                    "lvl": "WARN",
                    "msg": "policy AGGREGATES-OUT term order changed moved_terms={moved_terms} now_after={now_after}",
                    "vars": {"moved_terms": {"k": "str", "v": "term_list"}, "now_after": {"k": "ch", "v": ["REJECT_THE_REST"]}},
                },
                "prefix_withdrawn": {
                    "desc": "Router withdrew a prefix from a peer due to policy evaluation.",
                    "lvl": "ERROR",
                    "msg": "bgp withdrew prefix={prefix} peer={peer} reason={reason}",
                    "vars": {"prefix": {"k": "str", "v": "ip_prefix"}, "peer": {"k": "str", "v": "asn_or_peer_name"}, "reason": {"k": "ch", "v": ["policy_reject", "term_reorder", "manual_withdraw"]}},
                },
                "bgp_session_flap": {
                    "desc": "BGP session transitions with basic diagnostics.",
                    "lvl": "WARN",
                    "msg": "bgp session {state} peer={peer} uptime_s={uptime_s} last_err={last_err}",
                    "vars": {"state": {"k": "ch", "v": ["down", "up"]}, "peer": {"k": "str", "v": "asn_or_peer_name"}, "uptime_s": {"k": "i", "v": [0, 86400]}, "last_err": {"k": "str", "v": "bgp_err"}},
                },
                "bgp_summary": {
                    "desc": "Periodic BGP status summary.",
                    "lvl": "INFO",
                    "msg": "bgp summary peers_up={peers_up} prefixes_adv={prefixes_adv}",
                    "vars": {"peers_up": {"k": "i", "v": [0, 20]}},
                    "state_vars": {"n": {"prefixes_adv": {"k": "i", "v": [3500, 5000]}}, "f": {"prefixes_adv": {"k": "i", "v": [0, 1500]}}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "bgp_summary", "per_min": 0.5, "scope": "per_host"}, {"id": "bgp_session_flap", "per_min": 0.05, "scope": "per_host"}]},
                "f": {"emit": [{"id": "bgp_summary", "per_min": 0.5, "scope": "per_host"}, {"id": "prefix_withdrawn", "per_min": 18.0, "scope": "per_host"}, {"id": "bgp_session_flap", "per_min": 0.6, "scope": "per_host"}]},
            },
        },
        {
            "id": "config_pusher",
            "name": "Network Config Pusher (automation)",
            "svc": "netcfg",
            "hosts": ["netcfg-01"],
            "to": [{"dst": "spine_router", "proto": "ssh"}],
            "logs": {
                "scheduler_tick": {
                    "desc": "Periodic scheduler/queue tick for rollout controller.",
                    "lvl": "DEBUG",
                    "msg": "scheduler tick queued_jobs={queued_jobs} inflight={inflight}",
                    "vars": {"queued_jobs": {"k": "i", "v": [0, 5000]}, "inflight": {"k": "i", "v": [0, 500]}},
                },
                "rollout_start": {
                    "desc": "Rollout started (used as discrete operational marker).",
                    "lvl": "INFO",
                    "msg": "start rollout cr_id={cr_id} fleet={fleet} version={version}",
                    "vars": {"cr_id": {"k": "str", "v": "CRQ-YYYYMMDD-####"}, "fleet": {"k": "ch", "v": ["legacy_pops", "mcp_pops"]}, "version": {"k": "str", "v": "git_sha"}},
                },
                "rollout_change": {
                    "desc": "Rollout modified/aborted (used as discrete operational marker).",
                    "lvl": "WARN",
                    "msg": "rollout change action={action} cr_id={cr_id} by={by}",
                    "vars": {"action": {"k": "ch", "v": ["pause", "abort", "resume"]}, "cr_id": {"k": "str", "v": "CRQ-YYYYMMDD-####"}, "by": {"k": "str", "v": "username_or_bot"}},
                },
            },
            "beh": {"n": {"emit": [{"id": "scheduler_tick", "per_min": 0.2, "scope": "global"}]}, "f": {"emit": [{"id": "scheduler_tick", "per_min": 0.2, "scope": "global"}]}},
        },
        {
            "id": "noc_monitor",
            "name": "NOC Monitoring / Alerting",
            "svc": "noc-monitor",
            "hosts": ["noc-01", "noc-02"],
            "to": [{"dst": "edge_proxy", "proto": "https"}, {"dst": "spine_router", "proto": "tcp"}],
            "logs": {
                "http_probe_ok": {
                    "desc": "Synthetic HTTP probe succeeded.",
                    "lvl": "INFO",
                    "msg": "probe ok target={target} status={status} rtt_ms={rtt_ms}",
                    "vars": {"target": {"k": "str", "v": "vip_or_url"}, "status": {"k": "i", "v": [200, 399]}, "rtt_ms": {"k": "f", "v": [1.0, 2000.0]}},
                },
                "http_probe_fail": {
                    "desc": "Synthetic HTTP probe failed.",
                    "lvl": "WARN",
                    "msg": "probe fail target={target} err={err} rtt_ms={rtt_ms}",
                    "vars": {"target": {"k": "str", "v": "vip_or_url"}, "err": {"k": "ch", "v": ["timeout", "connection_refused", "tls_error", "no_route"]}, "rtt_ms": {"k": "f", "v": [1.0, 10000.0]}},
                },
                "monitor_tick": {
                    "desc": "Periodic monitoring loop tick.",
                    "lvl": "DEBUG",
                    "msg": "tick checks={checks} queued_alerts={queued_alerts}",
                    "vars": {"checks": {"k": "i", "v": [0, 50000]}, "queued_alerts": {"k": "i", "v": [0, 5000]}},
                },
                "alert_rule_fired": {
                    "desc": "Alert rule fired based on aggregated probe/telemetry conditions.",
                    "lvl": "CRITICAL",
                    "msg": "alert fired rule={rule} severity={severity} value={value}",
                    "vars": {"rule": {"k": "ch", "v": ["edge_5xx_rate", "origin_timeout_rate", "bgp_withdrawals", "mesh_route_missing"]}, "severity": {"k": "ch", "v": ["page", "ticket"]}, "value": {"k": "f", "v": [0.0, 1.0]}},
                },
                "incident_declared": {
                    "desc": "Incident declared (used as discrete operational marker).",
                    "lvl": "CRITICAL",
                    "msg": "incident declared id={incident_id} commander={commander}",
                    "vars": {"incident_id": {"k": "str", "v": "INC-YYYYMMDD-###"}, "commander": {"k": "str", "v": "username"}},
                },
            },
            "beh": {"n": {"emit": [{"id": "monitor_tick", "per_min": 0.5, "scope": "per_host"}]}, "f": {"emit": [{"id": "monitor_tick", "per_min": 0.5, "scope": "per_host"}, {"id": "alert_rule_fired", "per_min": 2.0, "scope": "per_host"}]}},
        },
        {
            "id": "upstream_peer",
            "name": "Upstream Transit/Peer (external)",
            "svc": None,
            "hosts": [],
            "to": [{"dst": "spine_router", "proto": "bgp"}],
            "logs": {
                "peer_notice": {
                    "desc": "Placeholder log template; not emitted in this model.",
                    "lvl": "INFO",
                    "msg": "peer notice type={type} detail={detail}",
                    "vars": {"type": {"k": "ch", "v": ["route_update", "keepalive", "session_reset"]}, "detail": {"k": "str", "v": "peer_detail"}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "customer_origin",
            "name": "Customer Origin (external)",
            "svc": None,
            "hosts": [],
            "to": [{"dst": "origin_fetcher", "proto": "https"}],
            "logs": {
                "origin_access": {
                    "desc": "Placeholder origin access log template; not emitted in this model.",
                    "lvl": "INFO",
                    "msg": "origin access path={path} status={status} bytes={bytes}",
                    "vars": {"path": {"k": "str", "v": "url_path"}, "status": {"k": "i", "v": [200, 599]}, "bytes": {"k": "i", "v": [0, 10000000]}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "tracing": {"on": True, "origins": ["edge_proxy"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "desc": "Normal traffic mix.",
            "req": [
                {
                    "id": "http_cache_hit",
                    "desc": "Client HTTP request served from cache within the MCP PoP.",
                    "rpm": 280.0,
                    "path": ["edge_proxy", "cache_cluster", "edge_proxy"],
                    "emit": ["edge_proxy.req_received", "cache_cluster.cache_hit", "edge_proxy.resp_sent"],
                    "latency_ms": [[0, 0], [2, 8], [1, 5]],
                    "trace": True,
                },
                {
                    "id": "http_cache_miss_origin_ok",
                    "desc": "Cache miss triggers origin fetch and returns successfully.",
                    "rpm": 120.0,
                    "path": ["edge_proxy", "cache_cluster", "origin_fetcher", "customer_origin", "origin_fetcher", "cache_cluster", "edge_proxy"],
                    "emit": ["edge_proxy.req_received", "cache_cluster.cache_miss", "origin_fetcher.fetch_start", "origin_fetcher.fetch_success", "edge_proxy.resp_sent"],
                    "latency_ms": [[0, 0], [3, 12], [1, 4], [45, 220], [2, 15]],
                    "trace": True,
                },
                {
                    "id": "edge_healthcheck_ok",
                    "desc": "Synthetic HTTP probe to edge VIP succeeds.",
                    "rpm": 30.0,
                    "path": ["noc_monitor", "edge_proxy"],
                    "emit": ["noc_monitor.http_probe_ok"],
                    "latency_ms": [[5, 50]],
                    "trace": False,
                },
            ],
        },
        "f": {
            "desc": "Failure traffic mix.",
            "req": [
                {
                    "id": "http_cache_hit_degraded",
                    "desc": "Remaining cache hits that still succeed despite internal instability.",
                    "rpm": 60.0,
                    "path": ["edge_proxy", "cache_cluster", "edge_proxy"],
                    "emit": ["edge_proxy.req_received", "cache_cluster.cache_hit", "edge_proxy.resp_sent"],
                    "latency_ms": [[0, 0], [4, 40], [2, 20]],
                    "trace": True,
                },
                {
                    "id": "http_origin_timeout",
                    "desc": "Cache miss where origin fetch fails and edge returns 52x/5xx.",
                    "rpm": 60.0,
                    "path": ["edge_proxy", "cache_cluster", "origin_fetcher", "cache_cluster", "edge_proxy"],
                    "emit": ["edge_proxy.req_received", "cache_cluster.cache_miss", "origin_fetcher.fetch_start", "origin_fetcher.fetch_timeout", "edge_proxy.upstream_error", "edge_proxy.resp_sent"],
                    "latency_ms": [[0, 0], [6, 60], [2, 10], [3000, 20000], [1, 5], [1, 10]],
                    "retry": {"max_attempts": 3, "expected_attempts": 2.3, "emit_per_retry": ["edge_proxy.retry_scheduled"], "backoff_ms": [[50, 150], [150, 400]]},
                    "trace": True,
                },
                {
                    "id": "http_internal_lb_fail",
                    "desc": "Dynamic request routed via Multimog fails immediately with no-route to backend.",
                    "rpm": 30.0,
                    "path": ["edge_proxy", "multimog_lb", "edge_proxy"],
                    "emit": ["edge_proxy.req_received", "multimog_lb.forward_fail_no_route", "edge_proxy.upstream_error", "edge_proxy.resp_sent"],
                    "latency_ms": [[0, 0], [20, 250], [1, 6], [1, 10]],
                    "retry": {"max_attempts": 3, "expected_attempts": 2.1, "emit_per_retry": ["edge_proxy.retry_scheduled"], "backoff_ms": [[40, 120], [120, 350]]},
                    "trace": True,
                },
                {
                    "id": "http_compute_overloaded_503",
                    "desc": "Multimog reaches a backend, but the selected small cluster rejects due to overload.",
                    "rpm": 20.0,
                    "path": ["edge_proxy", "multimog_lb", "compute_worker", "multimog_lb", "edge_proxy"],
                    "emit": ["edge_proxy.req_received", "multimog_lb.route_request", "compute_worker.request_rejected_overload", "edge_proxy.upstream_error", "edge_proxy.resp_sent"],
                    "latency_ms": [[0, 0], [10, 80], [50, 800], [1, 6], [1, 10]],
                    "retry": {"max_attempts": 2, "expected_attempts": 2.0, "emit_per_retry": ["edge_proxy.retry_scheduled"], "backoff_ms": [[30, 120]]},
                    "trace": True,
                },
                {
                    "id": "edge_healthcheck_fail",
                    "desc": "Synthetic HTTP probe to edge VIP fails during the incident.",
                    "rpm": 30.0,
                    "path": ["noc_monitor", "edge_proxy"],
                    "emit": ["noc_monitor.http_probe_fail"],
                    "latency_ms": [[20, 2000]],
                    "trace": False,
                },
            ],
        },
    },
    "assumptions": [],
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "mcp_bgp_policy_term_reorder_outage",
        "title": "MCP PoP outage due to BGP policy term reordering withdrawing site-local prefixes",
        "states": {"n": "normal", "f": "failure"},
        "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
        "phases": {
            "n": {
                "flows": ["http_cache_hit", "http_cache_miss_origin_ok", "edge_healthcheck_ok"],
                "manifestation": ["edge_proxy.resp_sent", "cache_cluster.cache_hit", "spine_router.bgp_summary", "noc_monitor.http_probe_ok"],
            },
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "component": "spine_router",
                        "event": "commit applied; withdrawals begin",
                        "why": "term reorder causes reject-before-accept",
                        "flows": ["http_origin_timeout", "http_internal_lb_fail"],
                        "rate_multipliers": {
                            "http_cache_hit_degraded": 1.2,
                            "http_origin_timeout": 1.0,
                            "http_internal_lb_fail": 0.6,
                            "http_compute_overloaded_503": 0.0,
                            "multimog_lb.mesh_route_missing": 0.5,
                            "noc_monitor.alert_rule_fired": 0.0,
                        },
                        "latency_multipliers": {"http_origin_timeout": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "config_pusher.rollout_start", "count": 1},
                            {"ref": "spine_router.config_commit_applied", "count": 2},
                            {"ref": "spine_router.policy_term_order_warning", "count": 2},
                        ],
                        "manifestation": ["spine_router.prefix_withdrawn", "origin_fetcher.fetch_timeout", "multimog_lb.forward_fail_no_route"],
                    },
                    {
                        "order": 2,
                        "at_min": 24,
                        "component": "multimog_lb",
                        "event": "mesh forwarding degrades further; overload appears; monitoring escalates",
                        "why": "collapsed routing concentrates traffic",
                        "flows": ["http_origin_timeout", "http_internal_lb_fail", "http_compute_overloaded_503", "edge_healthcheck_fail"],
                        "rate_multipliers": {
                            "http_cache_hit_degraded": 0.8,
                            "http_origin_timeout": 1.3,
                            "http_internal_lb_fail": 1.5,
                            "http_compute_overloaded_503": 1.0,
                            "edge_healthcheck_fail": 1.2,
                            "multimog_lb.mesh_route_missing": 2.0,
                            "noc_monitor.alert_rule_fired": 1.0,
                        },
                        "latency_multipliers": {"http_origin_timeout": {"p50": 2.0, "p95": 3.0}, "http_internal_lb_fail": {"p50": 1.2, "p95": 1.8}},
                        "one_shots": [{"ref": "noc_monitor.incident_declared", "count": 1}],
                        "manifestation": ["multimog_lb.mesh_route_missing", "compute_worker.request_rejected_overload", "noc_monitor.alert_rule_fired"],
                    },
                    {
                        "order": 3,
                        "at_min": 30,
                        "component": "spine_router",
                        "event": "verification changes; flaps increase; withdrawals briefly decrease",
                        "why": "inconsistent policy state causes oscillation",
                        "flows": ["http_origin_timeout", "http_internal_lb_fail", "http_compute_overloaded_503"],
                        "rate_multipliers": {
                            "http_cache_hit_degraded": 1.1,
                            "http_origin_timeout": 1.0,
                            "http_internal_lb_fail": 1.1,
                            "http_compute_overloaded_503": 0.7,
                            "spine_router.prefix_withdrawn": 0.8,
                            "spine_router.bgp_session_flap": 3.0,
                            "multimog_lb.mesh_route_missing": 1.5,
                            "noc_monitor.alert_rule_fired": 1.0,
                        },
                        "latency_multipliers": {"http_origin_timeout": {"p50": 1.5, "p95": 2.5}},
                        "one_shots": [{"ref": "config_pusher.rollout_change", "count": 1}],
                        "manifestation": ["spine_router.bgp_session_flap", "spine_router.prefix_withdrawn", "edge_proxy.retry_scheduled"],
                    },
                    {
                        "order": 4,
                        "at_min": 35,
                        "component": "config_pusher",
                        "event": "overlapping reverts; oscillation returns; probes/withdrawals spike",
                        "why": "change collision reintroduces reject-before-accept",
                        "flows": ["http_origin_timeout", "http_internal_lb_fail", "http_compute_overloaded_503", "edge_healthcheck_fail"],
                        "rate_multipliers": {
                            "http_cache_hit_degraded": 0.9,
                            "http_origin_timeout": 1.4,
                            "http_internal_lb_fail": 1.6,
                            "http_compute_overloaded_503": 1.2,
                            "edge_healthcheck_fail": 1.3,
                            "spine_router.prefix_withdrawn": 1.4,
                            "spine_router.bgp_session_flap": 6.0,
                            "multimog_lb.mesh_route_missing": 2.0,
                            "noc_monitor.alert_rule_fired": 1.5,
                        },
                        "latency_multipliers": {"http_origin_timeout": {"p50": 2.2, "p95": 3.5}, "http_internal_lb_fail": {"p50": 1.5, "p95": 2.5}},
                        "one_shots": [{"ref": "spine_router.config_commit_applied", "count": 2}],
                        "manifestation": ["spine_router.prefix_withdrawn", "noc_monitor.alert_rule_fired", "origin_fetcher.fetch_timeout"],
                    },
                ],
                "steady": [
                    {"component": "spine_router", "condition": "withdrawals and flaps persist", "user_impact": "timeouts and instability", "manifestation": ["spine_router.prefix_withdrawn", "spine_router.bgp_session_flap", "spine_router.bgp_summary"]},
                    {"component": "multimog_lb", "condition": "mesh routes missing", "user_impact": "502/503 and latency", "manifestation": ["multimog_lb.mesh_route_missing", "multimog_lb.forward_fail_no_route"]},
                    {"component": "edge_proxy", "condition": "retries and upstream errors persist", "user_impact": "52x/5xx after retries", "manifestation": ["edge_proxy.upstream_error", "edge_proxy.retry_scheduled", "edge_proxy.resp_sent"]},
                ],
                "feedback_loops": [{"id": "retry_amplification_overload", "desc": "retries amplify load and instability"}],
                "flows": ["http_cache_hit_degraded", "http_origin_timeout", "http_internal_lb_fail", "http_compute_overloaded_503", "edge_healthcheck_fail"],
                "manifestation": [
                    "spine_router.prefix_withdrawn",
                    "spine_router.bgp_session_flap",
                    "spine_router.bgp_summary",
                    "multimog_lb.mesh_route_missing",
                    "multimog_lb.forward_fail_no_route",
                    "compute_worker.request_rejected_overload",
                    "origin_fetcher.fetch_timeout",
                    "edge_proxy.upstream_error",
                    "edge_proxy.retry_scheduled",
                    "noc_monitor.alert_rule_fired",
                    "noc_monitor.http_probe_fail",
                ],
            },
        },
        "assumptions": [],
    }
}


# -----------------------------
# Helpers: lookups & sampling
# -----------------------------
COMPONENTS: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}


def _svc_of(comp_id: str) -> str:
    svc = COMPONENTS[comp_id].get("svc")
    return "" if svc is None else str(svc)


def _hosts_of(comp_id: str) -> List[str]:
    return list(COMPONENTS[comp_id].get("hosts") or [])


def _template(comp_id: str, log_id: str) -> Dict[str, Any]:
    return COMPONENTS[comp_id]["logs"][log_id]


def _iso8601_ms(dt: datetime) -> str:
    s = dt.isoformat(timespec="milliseconds")
    return s[:-6] + "Z" if s.endswith("+00:00") else s


def _rand_hex(n: int) -> str:
    alphabet = "0123456789abcdef"
    idx = RNG.integers(0, 16, size=n, endpoint=False)
    return "".join(alphabet[int(i)] for i in idx)


def _rand_uuid_str() -> str:
    b = RNG.bytes(16)
    u = uuid.UUID(bytes=b)
    # make it look like uuid4 (version/variant)
    u_int = u.int
    u_int &= ~(0xF << 76)
    u_int |= (0x4 << 76)
    u_int &= ~(0x3 << 62)
    u_int |= (0x2 << 62)
    return str(uuid.UUID(int=u_int))


def _rand_ip(cidr: Optional[str]) -> str:
    if cidr is None:
        while True:
            a = int(RNG.integers(1, 224))
            b = int(RNG.integers(0, 256))
            c = int(RNG.integers(0, 256))
            d = int(RNG.integers(1, 255))
            ip = f"{a}.{b}.{c}.{d}"
            if not (ip.startswith("10.") or ip.startswith("192.168.") or ip.startswith("172.16.") or ip.startswith("127.")):
                return ip
    if cidr == "0.0.0.0/0":
        return _rand_ip(None)
    net = ip_network(cidr, strict=False)
    n = net.num_addresses
    if n <= 2:
        return str(net.network_address)
    offset = int(RNG.integers(1, n - 1))
    return str(net.network_address + offset)


def _sample_choice(options: List[Any]) -> Any:
    return options[int(RNG.integers(0, len(options)))]


def _sample_int(lo: int, hi: int) -> int:
    return int(RNG.integers(lo, hi + 1))


def _sample_float(lo: float, hi: float, ndigits: int = 3) -> float:
    return float(round(float(RNG.uniform(lo, hi)), ndigits))


_URL_PATHS = [
    "/", "/index.html", "/robots.txt", "/favicon.ico", "/api/v1/items", "/api/v1/login",
    "/static/app.js", "/static/app.css", "/images/logo.png", "/checkout", "/account/settings",
    "/search?q=widgets", "/blog/edge-routing", "/docs/api", "/healthz",
]
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0",
    "curl/8.1.2",
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/121.0 Mobile",
    "k6/0.45.0",
]


def _sample_str(hint: str) -> str:
    if hint == "url_path":
        return _sample_choice(_URL_PATHS)
    if hint == "user_agent":
        return _sample_choice(_USER_AGENTS)
    if hint == "hostname":
        i = _sample_int(1, 5000)
        return _sample_choice(["www", "api", "static", "assets", "shop"]) + f"{i}.example.com"
    if hint == "cluster_id":
        region = _sample_choice(["ams", "iad", "lhr", "fra", "sjc", "sin", "syd"])
        return f"cluster-{region}-{_sample_int(1, 120)}"
    if hint == "ip_prefix":
        a = 10
        b = _sample_int(0, 255)
        c = _sample_int(0, 255)
        mask = _sample_choice([16, 20, 24])
        if mask == 16:
            return f"{a}.{b}.0.0/16"
        if mask == 20:
            return f"{a}.{b}.{(c // 16) * 16}.0/20"
        return f"{a}.{b}.{c}.0/24"
    if hint == "asn_or_peer_name":
        return _sample_choice([f"AS{_sample_int(1000, 9000)}", f"transit-{_sample_int(1, 12)}", f"peer-{_sample_int(1, 30)}"])
    if hint == "term_list":
        return _sample_choice(["SITE-LOCAL,COMMUNITIES", "SITE-LOCAL,MED,COMMUNITIES", "SITE-LOCAL-EXPORT,REWRITE,COMMUNITIES"])
    if hint == "bgp_err":
        return _sample_choice(["Hold Timer Expired", "Cease/Administrative Reset", "Notification/6/5", "Socket Error: No route to host"])
    if hint == "err_detail":
        return _sample_choice(["timeout waiting for upstream", "connect: no route to host", "route lookup failed", "backend queue overflow"])
    if hint == "resolver_err":
        return _sample_choice(["udp timeout to resolver", "servfail from upstream", "network unreachable to upstream resolver"])
    if hint == "vip_or_url":
        return _sample_choice(["https://edge.example.net/health", "https://vip.mcp.example.net/", "https://ams07-vip.example.net/"])
    if hint == "CRQ-YYYYMMDD-####":
        return f"CRQ-20260313-{_sample_int(1, 9999):04d}"
    if hint == "INC-YYYYMMDD-###":
        return f"INC-20260313-{_sample_int(1, 999):03d}"
    if hint == "git_sha":
        return _rand_hex(7)
    if hint == "username_or_bot":
        return _sample_choice(["neteng-bot", "rollout-bot", "alice", "bob", "carol"])
    if hint == "username":
        return _sample_choice(["alice", "bob", "carol", "dana", "erin"])
    return _sample_choice(["-", "n/a", "unknown"])


def _sample_var(spec: Dict[str, Any]) -> Any:
    k = spec["k"]
    v = spec["v"]
    if k == "i":
        return _sample_int(int(v[0]), int(v[1]))
    if k == "f":
        return _sample_float(float(v[0]), float(v[1]), ndigits=3)
    if k == "ch":
        return _sample_choice(list(v))
    if k == "uuid":
        return _rand_uuid_str()
    if k == "hex":
        return _rand_hex(int(v))
    if k == "ip":
        return _rand_ip(None if v is None else str(v))
    if k == "str":
        return _sample_str(str(v))
    return ""


def _extract_placeholders(msg: str) -> List[str]:
    return re.findall(r"{([a-zA-Z0-9_]+)}", msg)


def _render_message(comp_id: str, log_id: str, state: str, ctx: Dict[str, Any]) -> Tuple[str, str]:
    tpl = _template(comp_id, log_id)
    msg_t = tpl["msg"]
    placeholders = _extract_placeholders(msg_t)

    domains: Dict[str, Dict[str, Any]] = {}
    domains.update(tpl.get("vars") or {})
    if "state_vars" in tpl:
        domains.update(tpl["state_vars"][state])

    vals: Dict[str, Any] = {}
    for var in placeholders:
        if var in ctx:
            vals[var] = ctx[var]
        else:
            vals[var] = _sample_var(domains[var])

    for k, v in list(vals.items()):
        if isinstance(v, float):
            vals[k] = float(round(v, 3))

    return tpl["lvl"], msg_t.format(**vals)


# -----------------------------
# Latency/backoff sampling
# -----------------------------
_Z95 = 1.6448536269514722


def _lognormal_mu_sigma_from_p50_p95(p50: float, p95: float) -> Optional[Tuple[float, float]]:
    if p50 <= 0 or p95 <= 0 or p95 < p50:
        return None
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / _Z95
    return mu, max(1e-6, sigma)


def _sample_delay_ms(p50: float, p95: float, softcap_mult: float = 3.0) -> int:
    if p50 == 0 and p95 == 0:
        return 0
    if p50 <= 0 and p95 > 0:
        x = float(RNG.exponential(scale=max(1.0, p95 / 3.0)))
        x = min(x, p95 * softcap_mult)
        return int(max(0.0, round(x)))

    params = _lognormal_mu_sigma_from_p50_p95(p50, p95)
    if params is None:
        x = float(RNG.uniform(0.0, max(1.0, p95)))
        return int(max(0.0, round(x)))

    mu, sigma = params
    x = float(RNG.lognormal(mean=mu, sigma=sigma))
    cap = max(1.0, p95 * softcap_mult)
    x = min(x, cap)
    x *= float(1.0 + RNG.uniform(-0.05, 0.05))
    return int(max(0.0, round(x)))


def _sample_backoff_ms(p50: float, p95: float) -> int:
    return _sample_delay_ms(p50, p95, softcap_mult=3.0)


def _sample_attempt_count(max_attempts: int, expected: float) -> int:
    if max_attempts <= 1:
        return 1
    expected = max(1.0, min(float(max_attempts), float(expected)))
    lo = max(1, min(max_attempts, int(math.floor(expected))))
    hi = max(1, min(max_attempts, int(math.ceil(expected))))
    if lo == hi:
        return lo
    p_hi = expected - lo
    return hi if float(RNG.random()) < p_hi else lo


# -----------------------------
# Failure multiplier controller
# -----------------------------
@dataclass(frozen=True)
class LatMult:
    p50: float
    p95: float


def _build_failure_multiplier_tables() -> Tuple[List[Dict[str, float]], List[Dict[str, LatMult]]]:
    scen = SCENARIO["scenario"]
    f_start = int(scen["time"]["phases"]["f"]["start_min"])
    f_end = int(scen["time"]["phases"]["f"]["end_min"])
    events = sorted(scen["phases"]["f"]["events"], key=lambda e: e["order"])

    flow_ids = [f["id"] for f in SYSTEM["flows"]["f"]["req"]]

    bg_keys = set()
    for comp_id, comp in COMPONENTS.items():
        for e in comp.get("beh", {}).get("f", {}).get("emit", []):
            bg_keys.add(f"{comp_id}.{e['id']}")

    active_flow: Dict[str, float] = {fid: 1.0 for fid in flow_ids}
    active_bg: Dict[str, float] = {k: 1.0 for k in bg_keys}
    active_lat: Dict[str, LatMult] = {fid: LatMult(1.0, 1.0) for fid in flow_ids}

    per_min_rate: List[Dict[str, float]] = []
    per_min_lat: List[Dict[str, LatMult]] = []

    event_idx = 0
    for minute in range(f_start, f_end):
        while event_idx < len(events) and int(events[event_idx]["at_min"]) == minute:
            ev = events[event_idx]
            for k, v in (ev.get("rate_multipliers") or {}).items():
                if k in active_flow:
                    active_flow[k] = float(v)
                elif k in active_bg:
                    active_bg[k] = float(v)
            for fid, mult in (ev.get("latency_multipliers") or {}).items():
                if fid in active_lat:
                    active_lat[fid] = LatMult(float(mult["p50"]), float(mult["p95"]))
            event_idx += 1

        per_min_rate.append({"__minute": float(minute), **active_flow, **active_bg})
        per_min_lat.append({"__minute": minute, **active_lat})

    return per_min_rate, per_min_lat


FAIL_RATE_TABLE, FAIL_LAT_TABLE = _build_failure_multiplier_tables()


def _failure_rate_multiplier(minute: int, key: str) -> float:
    scen = SCENARIO["scenario"]
    f_start = int(scen["time"]["phases"]["f"]["start_min"])
    idx = minute - f_start
    if idx < 0 or idx >= len(FAIL_RATE_TABLE):
        return 1.0
    return float(FAIL_RATE_TABLE[idx].get(key, 1.0))


def _failure_lat_multiplier(minute: int, flow_id: str) -> LatMult:
    scen = SCENARIO["scenario"]
    f_start = int(scen["time"]["phases"]["f"]["start_min"])
    idx = minute - f_start
    if idx < 0 or idx >= len(FAIL_LAT_TABLE):
        return LatMult(1.0, 1.0)
    lm = FAIL_LAT_TABLE[idx].get(flow_id)
    return lm if lm is not None else LatMult(1.0, 1.0)


# -----------------------------
# Simulation
# -----------------------------
def _pick_hosts_for_flow_instance(flow_path: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for comp_id in set(flow_path):
        hosts = _hosts_of(comp_id)
        mapping[comp_id] = "" if not hosts else str(_sample_choice(hosts))
    return mapping


def _minute_start_dt(minute: int) -> datetime:
    return BASE_TIME + timedelta(minutes=minute)


def _add_log(
    rows: List[Dict[str, Any]],
    ts: datetime,
    comp_id: str,
    log_id: str,
    state: str,
    ctx: Dict[str, Any],
    trace_col: str,
    host_override: Optional[str] = None,
) -> None:
    lvl, msg = _render_message(comp_id, log_id, state, ctx)
    rows.append(
        {
            "_ts": ts,
            "timestamp": "",
            "level": lvl,
            "message": msg[:1000],
            "trace_id": trace_col,
            "service": _svc_of(comp_id),
            "host": host_override if host_override is not None else (ctx.get("__host") or ""),
        }
    )


def _flow_context(flow_id: str, state: str) -> Dict[str, Any]:
    colo = _sample_choice(_template("edge_proxy", "req_received")["vars"]["colo"]["v"])
    return {
        "method": _sample_choice(["GET", "GET", "GET", "POST", "HEAD"]),
        "path": _sample_choice(_URL_PATHS),
        "ua": _sample_choice(_USER_AGENTS),
        "client_ip": _rand_ip("0.0.0.0/0"),
        "colo": colo,
        "cache_key": _rand_hex(16),
        "origin_host": _sample_str("hostname"),
        "origin_ip": _rand_ip(None),
        "backend": _sample_str("cluster_id"),
        "prefix": _sample_str("ip_prefix"),
        "__flow_id": flow_id,
        "__state": state,
    }


def _status_for_flow(flow_id: str) -> Tuple[int, str]:
    if flow_id in ("http_cache_hit", "http_cache_hit_degraded"):
        return (int(_sample_choice([200, 200, 200, 304, 206, 301])), "HIT")
    if flow_id == "http_cache_miss_origin_ok":
        return (int(_sample_choice([200, 200, 200, 304, 404])), "MISS")
    if flow_id == "http_origin_timeout":
        return (int(_sample_choice([522, 522, 504, 520])), "MISS")
    if flow_id == "http_internal_lb_fail":
        return (int(_sample_choice([523, 523, 502, 520])), "BYPASS")
    if flow_id == "http_compute_overloaded_503":
        return (int(_sample_choice([503, 503, 502, 520])), "BYPASS")
    if flow_id == "edge_healthcheck_ok":
        return (200, "BYPASS")
    if flow_id == "edge_healthcheck_fail":
        return (0, "BYPASS")
    return (200, "BYPASS")


def _upstream_error_for_flow(flow_id: str, ctx: Dict[str, Any]) -> Tuple[str, str]:
    if flow_id == "http_origin_timeout":
        stage = str(ctx.get("stage", "first_byte"))
        return ("origin_timeout", f"{stage} timeout to {ctx['origin_host']} ({ctx['origin_ip']})")
    if flow_id == "http_internal_lb_fail":
        return ("lb_no_route", f"no route to backend={ctx['backend']} prefix={ctx['prefix']}")
    if flow_id == "http_compute_overloaded_503":
        return ("backend_overload", f"backend={ctx['backend']} overloaded qdepth={int(ctx.get('qdepth', 0))}")
    return ("origin_connect", "connect error")


def _retry_reason_for_flow(flow_id: str) -> str:
    if flow_id == "http_origin_timeout":
        return str(_sample_choice(["timeout", "timeout", "route_unreachable"]))
    if flow_id == "http_internal_lb_fail":
        return str(_sample_choice(["route_unreachable", "route_unreachable", "connect_error"]))
    if flow_id == "http_compute_overloaded_503":
        return str(_sample_choice(["overload", "overload", "timeout"]))
    return str(_sample_choice(["timeout", "connect_error", "route_unreachable", "overload"]))


def _simulate_background(rows: List[Dict[str, Any]], state: str, minute: int) -> None:
    for comp_id, comp in COMPONENTS.items():
        emits = comp.get("beh", {}).get(state, {}).get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            base = float(e["per_min"])
            scope = e.get("scope", "per_host")

            mult = 1.0
            if state == "f":
                mult = _failure_rate_multiplier(minute, f"{comp_id}.{log_id}")
            rate = base * mult
            if rate <= 0.0:
                continue

            if scope == "global":
                # One emitter, but it should still have a host identity when available.
                hosts = _hosts_of(comp_id) or [""]
                host_instances = [str(hosts[0]) if hosts and hosts[0] != "" else ""]
                counts = [int(RNG.poisson(rate))]
            else:
                hosts = _hosts_of(comp_id) or [""]
                host_instances = hosts
                counts = [int(RNG.poisson(rate)) for _ in host_instances]

            for host, cnt in zip(host_instances, counts):
                for _ in range(cnt):
                    t = _minute_start_dt(minute) + timedelta(seconds=float(RNG.uniform(0.0, 60.0)))
                    ctx: Dict[str, Any] = {"__host": host}

                    # Light shaping to match narratives (within template domains)
                    if comp_id == "spine_router" and log_id == "bgp_summary":
                        if state == "f":
                            ctx["peers_up"] = _sample_int(0, 12)
                            ctx["prefixes_adv"] = _sample_int(0, 1500)
                        else:
                            ctx["peers_up"] = _sample_int(10, 20)
                            ctx["prefixes_adv"] = _sample_int(3500, 5000)
                    if comp_id == "noc_monitor" and log_id == "alert_rule_fired":
                        ctx["value"] = float(round(float(min(1.0, max(0.0, RNG.beta(6, 2)))), 3))

                    _add_log(rows, t, comp_id, log_id, state, ctx, trace_col="", host_override=host)


def _simulate_flow_instances(rows: List[Dict[str, Any]], state: str, minute: int) -> None:
    flows = SYSTEM["flows"][state]["req"]
    for flow in flows:
        flow_id = flow["id"]
        base_rpm = float(flow["rpm"])

        mult = 1.0
        if state == "f":
            mult = _failure_rate_multiplier(minute, flow_id)
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0.0:
            continue

        nreq = int(RNG.poisson(eff_rpm))
        for _ in range(nreq):
            start = _minute_start_dt(minute) + timedelta(seconds=float(RNG.uniform(0.0, 60.0)))
            host_map = _pick_hosts_for_flow_instance(flow["path"])

            req_id = _rand_uuid_str()
            trace_id = ""
            if SYSTEM["tracing"]["on"] and bool(flow.get("trace", False)):
                trace_id = _rand_hex(32)

            ctx_common = _flow_context(flow_id, state)
            ctx_common["req_id"] = req_id
            if trace_id:
                ctx_common["trace_id"] = trace_id

            retry = flow.get("retry")
            attempts = _sample_attempt_count(int(retry["max_attempts"]), float(retry["expected_attempts"])) if retry else 1

            lm = _failure_lat_multiplier(minute, flow_id) if state == "f" else LatMult(1.0, 1.0)

            attempt_start = start
            for attempt in range(1, attempts + 1):
                t = attempt_start
                total_dur_ms = 0

                for i, ref in enumerate(flow["emit"]):
                    comp_id, log_id = ref.split(".", 1)
                    p50, p95 = flow["latency_ms"][i]
                    p50_eff = float(p50) * (lm.p50 if state == "f" else 1.0)
                    p95_eff = float(p95) * (lm.p95 if state == "f" else 1.0)

                    delay_ms = _sample_delay_ms(p50_eff, p95_eff)
                    t = t + timedelta(milliseconds=delay_ms)
                    total_dur_ms += delay_ms

                    ctx = dict(ctx_common)
                    ctx["__host"] = host_map.get(comp_id, "")

                    # Coherence overrides by log type
                    if comp_id == "cache_cluster" and log_id == "cache_hit":
                        ctx["cache_key"] = ctx_common["cache_key"]
                        ctx["ttl_s"] = _sample_int(10, 3600) if state == "f" else _sample_int(60, 86400)

                    if comp_id == "cache_cluster" and log_id == "cache_miss":
                        ctx["cache_key"] = ctx_common["cache_key"]
                        if flow_id in ("http_cache_miss_origin_ok", "http_origin_timeout"):
                            ctx["miss_reason"] = _sample_choice(["not_found", "expired", "not_found"])
                        else:
                            ctx["miss_reason"] = _sample_choice(["bypass", "expired"])

                    if comp_id == "origin_fetcher" and log_id == "fetch_start":
                        ctx["origin_host"] = ctx_common["origin_host"]
                        ctx["origin_ip"] = ctx_common["origin_ip"]

                    if comp_id == "origin_fetcher" and log_id == "fetch_success":
                        ctx["dur_ms"] = int(min(20000, max(1, total_dur_ms)))
                        ctx["origin_status"] = _sample_choice([200, 200, 200, 304, 404])

                    if comp_id == "origin_fetcher" and log_id == "fetch_timeout":
                        stage = _sample_choice(["connect", "first_byte", "first_byte", "tls"])
                        ctx["stage"] = stage
                        ctx_common["stage"] = stage
                        ctx["dur_ms"] = int(min(60000, max(500, total_dur_ms)))

                    if comp_id == "multimog_lb" and log_id == "route_request":
                        ctx["decision"] = "selected"
                        ctx["service"] = _sample_choice(["http_app", "workers", "api"])
                        ctx["backend"] = ctx_common["backend"]

                    if comp_id == "multimog_lb" and log_id == "forward_fail_no_route":
                        ctx["backend"] = ctx_common["backend"]
                        ctx["prefix"] = ctx_common["prefix"]

                    if comp_id == "compute_worker" and log_id == "request_rejected_overload":
                        qdepth = _sample_int(9000, 20000)
                        max_q = _sample_int(1000, max(1000, min(18000, qdepth - 1)))
                        ctx["qdepth"] = qdepth
                        ctx["max_qdepth"] = max_q
                        ctx_common["qdepth"] = qdepth

                    if comp_id == "edge_proxy" and log_id == "upstream_error":
                        err_type, detail = _upstream_error_for_flow(flow_id, {**ctx_common, **ctx})
                        ctx["err_type"] = err_type
                        ctx["detail"] = detail

                    if comp_id == "edge_proxy" and log_id == "resp_sent":
                        status_code, cache_status = _status_for_flow(flow_id)
                        ctx["status_code"] = status_code
                        ctx["cache_status"] = cache_status
                        if status_code >= 500 or status_code in (520, 522, 523, 504):
                            ctx["bytes"] = _sample_int(200, 8000)
                        else:
                            ctx["bytes"] = _sample_int(500, 250000)
                        ctx["dur_ms"] = int(min(60000, max(0, total_dur_ms)))

                    if comp_id == "noc_monitor" and log_id in ("http_probe_ok", "http_probe_fail"):
                        ctx["target"] = _sample_str("vip_or_url")
                        rtt = float(min(10000.0, max(1.0, total_dur_ms + float(RNG.uniform(0.0, 50.0)))))
                        ctx["rtt_ms"] = float(round(rtt, 3))
                        if log_id == "http_probe_ok":
                            ctx["status"] = _sample_int(200, 399)
                        else:
                            ctx["err"] = _sample_choice(["timeout", "no_route", "tls_error", "timeout"])

                    _add_log(rows, t, comp_id, log_id, state, ctx, trace_col=trace_id, host_override=ctx["__host"])

                last_attempt_end = t

                if retry and attempt < attempts:
                    p50_b, p95_b = retry["backoff_ms"][attempt - 1]
                    backoff_ms = _sample_backoff_ms(float(p50_b), float(p95_b))

                    for ref in (retry.get("emit_per_retry") or []):
                        comp_id, log_id = ref.split(".", 1)
                        ctx = dict(ctx_common)
                        ctx["__host"] = host_map.get(comp_id, "")
                        ctx["attempt"] = attempt + 1
                        ctx["backoff_ms"] = int(backoff_ms)
                        ctx["reason"] = _retry_reason_for_flow(flow_id)
                        t_retry = last_attempt_end + timedelta(milliseconds=float(RNG.uniform(1.0, 25.0)))
                        _add_log(rows, t_retry, comp_id, log_id, state, ctx, trace_col=trace_id, host_override=ctx["__host"])

                    attempt_start = last_attempt_end + timedelta(milliseconds=backoff_ms)


def _emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: e["order"])
    fixed_cr = "CRQ-20260313-0421"

    for ev in events:
        at_min = int(ev["at_min"])
        for o in (ev.get("one_shots") or []):
            ref = o["ref"]
            count = int(o["count"])
            comp_id, log_id = ref.split(".", 1)
            hosts = o.get("hosts")
            if hosts is None:
                hosts = _hosts_of(comp_id) or [""]
            if not hosts:
                hosts = [""]

            base_t = _minute_start_dt(at_min) + timedelta(seconds=float(RNG.uniform(0.0, 1.0)))
            for j in range(count):
                host = str(hosts[j % len(hosts)])
                ctx: Dict[str, Any] = {"__host": host}

                if comp_id == "config_pusher" and log_id in ("rollout_start", "rollout_change"):
                    ctx["cr_id"] = fixed_cr
                if comp_id == "spine_router" and log_id == "config_commit_applied":
                    ctx["cr_id"] = fixed_cr
                    ctx["target"] = "mcp_spines"
                if comp_id == "noc_monitor" and log_id == "incident_declared":
                    ctx["incident_id"] = "INC-20260313-007"
                    ctx["commander"] = "alice"

                t = base_t + timedelta(milliseconds=float(j) * float(RNG.uniform(3.0, 15.0)))
                _add_log(rows, t, comp_id, log_id, "f", ctx, trace_col="", host_override=host)


def generate_logs() -> pd.DataFrame:
    scen = SCENARIO["scenario"]
    n_start = int(scen["time"]["phases"]["n"]["start_min"])
    n_end = int(scen["time"]["phases"]["n"]["end_min"])
    f_start = int(scen["time"]["phases"]["f"]["start_min"])
    f_end = int(scen["time"]["phases"]["f"]["end_min"])

    rows: List[Dict[str, Any]] = []

    for minute in range(n_start, n_end):
        _simulate_background(rows, "n", minute)
        _simulate_flow_instances(rows, "n", minute)

    for minute in range(f_start, f_end):
        _simulate_background(rows, "f", minute)
        _simulate_flow_instances(rows, "f", minute)

    _emit_one_shots(rows)

    df = pd.DataFrame(rows)
    df = df.sort_values("_ts", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["_ts"].apply(_iso8601_ms)
    df = df.drop(columns=["_ts"])

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df["trace_id"] = df["trace_id"].fillna("").astype(str)
    df["service"] = df["service"].fillna("").astype(str)
    df["host"] = df["host"].fillna("").astype(str)
    return df


def main() -> None:
    df = generate_logs()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
