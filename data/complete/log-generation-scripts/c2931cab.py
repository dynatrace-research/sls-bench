"""
Log stream simulator for the "reddit_coordination_outage" system under the
"aug11_autoscaler_zk_migration_outage" scenario.

Plan / approach
---------------
1) Embed inputs:
   - SYSTEM: Python dict representation of the provided system_description YAML.
   - SCENARIO: Python dict representation of the provided scenario YAML.
   These dicts contain the components, log templates, behaviors, flows, and
   scenario timeline/events/multipliers/one-shots.

2) Deterministic simulation:
   - Fixed seeds for both `random` and `numpy`.
   - Base time: 2026-03-13T12:00:00.000Z maps to scenario minute 0.

3) Emission sources:
   A) Background logs
      For each simulated minute, for each component behavior entry in the
      active state (normal 'n' or failure 'f'):
        - Determine effective per-minute rate (failure state only) by applying
          scenario rate multipliers of the form "component_id.log_id" with
          persistence/override semantics.
        - Generate counts via Poisson(rate), with host fan-out:
            * scope=per_host -> independent Poisson per host
            * scope=global   -> one Poisson for the component; each line is
              assigned a host instance.

   B) Request flows
      For each simulated minute, for each active flow in that state:
        - Determine effective rpm (failure only) by applying scenario flow rate
          multipliers (by flow id) with persistence/override semantics.
        - Generate request start counts via Poisson(rpm), start times uniformly
          within the minute.
        - For each request instance:
            * If tracing enabled and flow.trace true -> generate a new 32-hex
              trace_id and propagate across all logs in the instance (including
              retries).
            * Generate one req_id (uuid4) for request-shaped flows and reuse it
              across logs.
            * Sample attempts count from expected_attempts (if retry present)
              using a 2-point distribution between floor(E) and ceil(E) (bounded
              by max_attempts).
            * Emit per-attempt logs in the exact order listed, using delays
              sampled from a lognormal distribution parameterized by (p50, p95).
              - In failure, apply the active latency multiplier for that flow
                (p50 and p95 scale separately) based on the minute the request
                *started*.
            * For retries: emit retry-only logs once per retry attempt (attempts
              2..A), and sample backoff delays from lognormal(p50,p95) with a
              soft cap (2.5-3.0*p95). The logged backoff_ms matches the sampled
              delay; we do not hard-truncate to the template's nominal domain.

4) Variable coherence:
   - For flows, we maintain a per-instance context (req_id, trace_id, route/path,
     chosen upstream, status, etc.).
   - For certain logs (edge access/upstream errors, app start/end/db logs, etc.)
     we override sampled variables to keep messages consistent with the flow
     meaning (e.g., 503 flow -> no_healthy_upstream and status=503; db_timeout
     flow -> upstream_timeout and status=504; down_mode flows -> upstream=down_mode).
   - Per-attempt durations (upstream_ms, timeout detail) are computed relative to
     the attempt start time (not the initial request start), so retries don't
     incorrectly include earlier attempts/backoff time.

5) Failure event controller:
   - At the start of each failure-minute, apply any scenario events whose at_min
     equals that minute:
       * override active rate multipliers
       * override active latency multipliers
       * emit one-shots at that event time (not affected by multipliers)

6) Output:
   - Collect all log rows, sort by timestamp ascending, and write logs.csv with
     columns: timestamp, level, message, trace_id, service, host.
"""

from __future__ import annotations

import math
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {
        "id": "reddit_coordination_outage",
        "desc": (
            "A social/API platform behind an edge LB with app, cache, DB tiers; "
            "autoscaling uses ZooKeeper membership; a migration plus unexpected "
            "autoscaler enablement causes mass terminations and outage then cold-cache DB overload."
        ),
    },
    "states": {"n": "normal", "f": "failure"},
    "components": [
        {
            "id": "edge_lb",
            "name": "Edge Load Balancer",
            "svc": "edge-lb",
            "hosts": ["edge-01", "edge-02", "edge-03", "edge-04"],
            "to": [
                {"dst": "app_cluster", "proto": "https", "desc": "Forwards end-user HTTP requests."},
                {"dst": "monitoring", "proto": "https", "desc": "Exposes metrics endpoints."},
            ],
            "logs": {
                "hc_ok": {
                    "desc": "Periodic healthcheck result summary emitted by each edge instance.",
                    "lvl": "INFO",
                    "msg": "healthcheck ok target={target} rtt_ms={rtt_ms}",
                    "vars": {"target": {"k": "ch", "v": ["app_cluster", "cache_cluster", "db_cluster"]}},
                    "state_vars": {"n": {"rtt_ms": {"k": "i", "v": [1, 50]}}, "f": {"rtt_ms": {"k": "i", "v": [1, 500]}}},
                },
                "access_log": {
                    "desc": "Access log for completed requests at the edge.",
                    "lvl": "INFO",
                    "msg": "req completed req_id={req_id} trace_id={trace_id} method={method} path={path} status={status} upstream={upstream} upstream_ms={upstream_ms} bytes_out={bytes_out}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "path": {"k": "str", "v": "http_path"},
                        "upstream": {"k": "ch", "v": ["app_cluster", "down_mode", "none"]},
                        "upstream_ms": {"k": "i", "v": [0, 10000]},
                        "bytes_out": {"k": "i", "v": [200, 500000]},
                    },
                    "state_vars": {"n": {"status": {"k": "ch", "v": [200, 204, 304]}}, "f": {"status": {"k": "ch", "v": [200, 429, 503, 504]}}},
                },
                "upstream_error": {
                    "desc": "Edge routing/upstream failure log.",
                    "lvl": "ERROR",
                    "msg": "upstream error req_id={req_id} trace_id={trace_id} upstream={upstream} error={error} detail={detail}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "upstream": {"k": "ch", "v": ["app_cluster", "none"]},
                        "detail": {"k": "str", "v": "err_detail"},
                    },
                    "state_vars": {
                        "n": {"error": {"k": "ch", "v": ["upstream_reset", "connect_timeout"]}},
                        "f": {"error": {"k": "ch", "v": ["no_healthy_upstream", "connect_timeout", "upstream_timeout", "down_mode"]}},
                    },
                },
                "retry_scheduled": {
                    "desc": "Edge-side retry scheduling log.",
                    "lvl": "WARN",
                    "msg": "retrying req_id={req_id} trace_id={trace_id} attempt={attempt} reason={reason} backoff_ms={backoff_ms}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["503", "connect_error", "timeout"]},
                        "backoff_ms": {"k": "i", "v": [50, 2000]},
                    },
                },
                "down_mode_served": {
                    "desc": "Edge served a down-mode response without proxying.",
                    "lvl": "WARN",
                    "msg": "served down mode req_id={req_id} trace_id={trace_id} path={path} status={status} reason={reason}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "path": {"k": "str", "v": "http_path"},
                        "reason": {"k": "ch", "v": ["maintenance", "restore_in_progress"]},
                    },
                    "state_vars": {"n": {"status": {"k": "ch", "v": [200]}}, "f": {"status": {"k": "ch", "v": [200, 503]}}},
                },
            },
            "beh": {
                "n": {"desc": "Routes traffic normally and emits periodic healthcheck logs.", "emit": [{"id": "hc_ok", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Still emits healthchecks, RTTs higher/variable.", "emit": [{"id": "hc_ok", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "app_cluster",
            "name": "Application Cluster",
            "svc": "app-cluster",
            "hosts": ["app-01", "app-02", "app-03", "app-04", "app-05", "app-06"],
            "to": [
                {"dst": "edge_lb", "proto": "https", "desc": "Returns responses back through edge."},
                {"dst": "cache_cluster", "proto": "redis", "desc": "Cache reads/writes."},
                {"dst": "db_cluster", "proto": "jdbc", "desc": "DB queries."},
                {"dst": "monitoring", "proto": "https", "desc": "Metrics/health endpoints."},
            ],
            "logs": {
                "req_start": {
                    "desc": "Start of request handling.",
                    "lvl": "INFO",
                    "msg": "request start req_id={req_id} trace_id={trace_id} route={route} user_id={user_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "route": {"k": "ch", "v": ["/", "/r/{sub}/hot", "/api/v1/me", "/api/v1/subreddit/{sub}/hot"]},
                        "user_id": {"k": "i", "v": [1, 50000000]},
                    },
                },
                "db_query": {
                    "desc": "DB query completion attribution.",
                    "lvl": "INFO",
                    "msg": "db query req_id={req_id} trace_id={trace_id} db_host={db_host} sql_hash={sql_hash} duration_ms={duration_ms} rows={rows}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "db_host": {"k": "ch", "v": ["db-01", "db-02", "db-03"]},
                        "sql_hash": {"k": "hex", "v": 16},
                        "rows": {"k": "i", "v": [0, 500]},
                    },
                    "state_vars": {"n": {"duration_ms": {"k": "i", "v": [1, 200]}}, "f": {"duration_ms": {"k": "i", "v": [10, 4000]}}},
                },
                "req_end": {
                    "desc": "End of request handling.",
                    "lvl": "INFO",
                    "msg": "request end req_id={req_id} trace_id={trace_id} status={status} latency_ms={latency_ms} cache={cache}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": [200, 304]}, "latency_ms": {"k": "i", "v": [5, 400]}, "cache": {"k": "ch", "v": ["hit", "hit_stale", "miss"]}},
                        "f": {"status": {"k": "ch", "v": [200, 304, 500, 504]}, "latency_ms": {"k": "i", "v": [20, 10000]}, "cache": {"k": "ch", "v": ["hit", "hit_stale", "miss", "miss_cold"]}},
                    },
                },
                "db_timeout": {
                    "desc": "App timed out waiting on DB.",
                    "lvl": "ERROR",
                    "msg": "db timeout req_id={req_id} trace_id={trace_id} db_host={db_host} timeout_ms={timeout_ms} waited_ms={waited_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}, "db_host": {"k": "ch", "v": ["db-01", "db-02", "db-03"]}, "timeout_ms": {"k": "i", "v": [500, 3000]}},
                    "state_vars": {"n": {"waited_ms": {"k": "i", "v": [100, 2000]}}, "f": {"waited_ms": {"k": "i", "v": [500, 15000]}}},
                },
                "gc_pause_warn": {
                    "desc": "GC pause warning.",
                    "lvl": "WARN",
                    "msg": "gc pause duration_ms={duration_ms} heap_mb={heap_mb}",
                    "vars": {"heap_mb": {"k": "i", "v": [512, 8192]}},
                    "state_vars": {"n": {"duration_ms": {"k": "i", "v": [50, 500]}}, "f": {"duration_ms": {"k": "i", "v": [100, 3000]}}},
                },
            },
            "beh": {
                "n": {"desc": "Healthy app instances with low GC pressure.", "emit": [{"id": "gc_pause_warn", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"desc": "Higher work and queuing increase GC pauses.", "emit": [{"id": "gc_pause_warn", "per_min": 0.4, "scope": "per_host"}]},
            },
        },
        {
            "id": "cache_cluster",
            "name": "Cache Cluster (Redis/Memcache)",
            "svc": "cache-cluster",
            "hosts": ["cache-01", "cache-02", "cache-03"],
            "to": [{"dst": "app_cluster", "proto": "redis", "desc": "Serves cache reads/writes."}, {"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "redis_stats": {
                    "desc": "Periodic cache node stats snapshot.",
                    "lvl": "INFO",
                    "msg": "redis stats node={node} connected_clients={clients} used_memory_mb={mem_mb} key_count={key_count} hit_rate={hit_rate}",
                    "vars": {"node": {"k": "ch", "v": ["cache-01", "cache-02", "cache-03"]}, "clients": {"k": "i", "v": [10, 50000]}, "mem_mb": {"k": "i", "v": [256, 65536]}},
                    "state_vars": {"n": {"key_count": {"k": "i", "v": [1000000, 50000000]}, "hit_rate": {"k": "f", "v": [0.80, 0.99]}}, "f": {"key_count": {"k": "i", "v": [0, 5000000]}, "hit_rate": {"k": "f", "v": [0.00, 0.70]}}},
                },
                "warmup_tick": {
                    "desc": "Periodic warmup progress report.",
                    "lvl": "INFO",
                    "msg": "cache warmup loaded_keys={loaded_keys} load_rate_keys_s={rate_keys_s} miss_rate={miss_rate}",
                    "vars": {"rate_keys_s": {"k": "i", "v": [0, 200000]}},
                    "state_vars": {"n": {"loaded_keys": {"k": "i", "v": [1000000, 50000000]}, "miss_rate": {"k": "f", "v": [0.01, 0.20]}}, "f": {"loaded_keys": {"k": "i", "v": [0, 10000000]}, "miss_rate": {"k": "f", "v": [0.20, 1.00]}}},
                },
            },
            "beh": {
                "n": {"desc": "Stable cache fleet emitting periodic stats.", "emit": [{"id": "redis_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Stats show reduced keyspace and warmup logs persist.", "emit": [{"id": "redis_stats", "per_min": 1.0, "scope": "per_host"}, {"id": "warmup_tick", "per_min": 2.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "db_cluster",
            "name": "Database Cluster",
            "svc": "db-cluster",
            "hosts": ["db-01", "db-02", "db-03"],
            "to": [{"dst": "app_cluster", "proto": "jdbc", "desc": "Returns query results."}, {"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "db_metrics": {
                    "desc": "Periodic DB host metrics snapshot.",
                    "lvl": "INFO",
                    "msg": "db metrics host={host} connections={connections} qps={qps} cpu_pct={cpu_pct}",
                    "vars": {"host": {"k": "ch", "v": ["db-01", "db-02", "db-03"]}, "connections": {"k": "i", "v": [50, 2000]}, "qps": {"k": "i", "v": [100, 20000]}, "cpu_pct": {"k": "i", "v": [1, 100]}},
                },
                "conn_pool_waiting": {
                    "desc": "Connection pool contention marker.",
                    "lvl": "ERROR",
                    "msg": "connection pool waiting host={host} waiting={waiting} max={max} wait_ms_p95={wait_ms_p95}",
                    "vars": {"host": {"k": "ch", "v": ["db-01", "db-02", "db-03"]}, "max": {"k": "i", "v": [200, 3000]}},
                    "state_vars": {"n": {"waiting": {"k": "i", "v": [0, 50]}, "wait_ms_p95": {"k": "i", "v": [0, 200]}}, "f": {"waiting": {"k": "i", "v": [10, 2000]}, "wait_ms_p95": {"k": "i", "v": [50, 15000]}}},
                },
                "slow_query_warn": {
                    "desc": "Slow query warning.",
                    "lvl": "WARN",
                    "msg": "slow query host={host} duration_ms={duration_ms} normalized_sql={normalized_sql}",
                    "vars": {"host": {"k": "ch", "v": ["db-01", "db-02", "db-03"]}, "normalized_sql": {"k": "str", "v": "sql"}},
                    "state_vars": {"n": {"duration_ms": {"k": "i", "v": [200, 1500]}}, "f": {"duration_ms": {"k": "i", "v": [500, 20000]}}},
                },
            },
            "beh": {
                "n": {"desc": "Steady metrics and occasional slow queries.", "emit": [{"id": "db_metrics", "per_min": 1.0, "scope": "per_host"}, {"id": "slow_query_warn", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Under cold-cache load, waits and slow queries more frequent.", "emit": [{"id": "db_metrics", "per_min": 1.0, "scope": "per_host"}, {"id": "conn_pool_waiting", "per_min": 3.0, "scope": "per_host"}, {"id": "slow_query_warn", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "zookeeper",
            "name": "ZooKeeper Cluster",
            "svc": "zookeeper",
            "hosts": ["zk-01", "zk-02", "zk-03"],
            "to": [{"dst": "autoscaler", "proto": "tcp", "desc": "Returns snapshot responses."}, {"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "quorum_status": {"desc": "Periodic ZooKeeper quorum status.", "lvl": "INFO", "msg": "zk quorum status node={node} role={role} zxid={zxid} peers_up={peers_up}", "vars": {"node": {"k": "ch", "v": ["zk-01", "zk-02", "zk-03"]}, "role": {"k": "ch", "v": ["leader", "follower"]}, "zxid": {"k": "hex", "v": 8}, "peers_up": {"k": "i", "v": [1, 3]}}},
                "migration_stage": {"desc": "Migration progress marker.", "lvl": "WARN", "msg": "zk migration stage={stage} progress_pct={progress_pct} source_cluster={source} dest_cluster={dest}", "vars": {"stage": {"k": "ch", "v": ["prep", "snapshot_copy", "cutover_partial", "cutover", "verify"]}, "progress_pct": {"k": "i", "v": [0, 100]}, "source": {"k": "ch", "v": ["zk-legacy"]}, "dest": {"k": "ch", "v": ["zk-modern"]}}},
                "read_snapshot": {"desc": "Autoscaler read a consistent membership snapshot.", "lvl": "INFO", "msg": "snapshot read client={client} servers_seen={servers_seen} version={version}", "vars": {"client": {"k": "ch", "v": ["autoscaler"]}, "version": {"k": "i", "v": [1, 1000000]}}, "state_vars": {"n": {"servers_seen": {"k": "i", "v": [200, 5000]}}, "f": {"servers_seen": {"k": "i", "v": [0, 5000]}}}},
                "read_inconsistent": {"desc": "Autoscaler read an inconsistent/partial snapshot.", "lvl": "ERROR", "msg": "inconsistent snapshot client={client} servers_seen={servers_seen} missing_azs={missing_azs} version={version}", "vars": {"client": {"k": "ch", "v": ["autoscaler"]}, "missing_azs": {"k": "ch", "v": ["1", "2", "3"]}, "version": {"k": "i", "v": [1, 1000000]}}, "state_vars": {"n": {"servers_seen": {"k": "i", "v": [200, 5000]}}, "f": {"servers_seen": {"k": "i", "v": [0, 2000]}}}},
            },
            "beh": {
                "n": {"desc": "Stable quorum with routine status logs.", "emit": [{"id": "quorum_status", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"desc": "Migration markers appear alongside routine quorum status.", "emit": [{"id": "quorum_status", "per_min": 0.5, "scope": "per_host"}, {"id": "migration_stage", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "autoscaler",
            "name": "Autoscaler",
            "svc": "autoscaler",
            "hosts": ["as-01", "as-02"],
            "to": [{"dst": "zookeeper", "proto": "tcp", "desc": "Reads membership/health info."}, {"dst": "cloud_api", "proto": "https", "desc": "Instance lifecycle calls."}, {"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "reconcile_tick": {"desc": "Scheduler tick.", "lvl": "INFO", "msg": "reconcile tick enabled={enabled} last_run_age_s={age_s}", "vars": {"enabled": {"k": "ch", "v": ["true", "false"]}, "age_s": {"k": "i", "v": [0, 600]}}},
                "reconcile_start": {"desc": "Start of a reconciliation run.", "lvl": "INFO", "msg": "reconcile start run_id={run_id} reason={reason}", "vars": {"run_id": {"k": "uuid", "v": None}, "reason": {"k": "ch", "v": ["periodic", "startup", "manual"]}}},
                "reconcile_done": {"desc": "End of successful reconciliation.", "lvl": "INFO", "msg": "reconcile done run_id={run_id} desired={desired} current={current} actions={actions} duration_ms={duration_ms}", "vars": {"run_id": {"k": "uuid", "v": None}, "desired": {"k": "i", "v": [50, 2000]}, "current": {"k": "i", "v": [0, 2000]}, "actions": {"k": "i", "v": [0, 500]}}, "state_vars": {"n": {"duration_ms": {"k": "i", "v": [50, 2000]}}, "f": {"duration_ms": {"k": "i", "v": [100, 10000]}}}},
                "scale_decision_bad": {"desc": "Unsafe scaling decision.", "lvl": "CRITICAL", "msg": "unsafe scale decision run_id={run_id} snapshot_servers_seen={servers_seen} terminate_count={terminate_count} guardrails={guardrails}", "vars": {"run_id": {"k": "uuid", "v": None}, "servers_seen": {"k": "i", "v": [0, 5000]}, "terminate_count": {"k": "i", "v": [1, 2000]}, "guardrails": {"k": "ch", "v": ["none", "partial"]}}},
                "enabled_state_change": {"desc": "Autoscaler enabled/disabled change.", "lvl": "WARN", "msg": "autoscaler enabled state changed enabled={enabled} actor={actor} reason={reason}", "vars": {"enabled": {"k": "ch", "v": ["true", "false"]}, "actor": {"k": "ch", "v": ["operator", "pkg_manager", "sre_bot"]}, "reason": {"k": "str", "v": "reason"}}},
                "bulk_termination_summary": {"desc": "Summary of bulk termination burst.", "lvl": "CRITICAL", "msg": "bulk termination issued count={count} duration_s={duration_s} reason={reason} run_id={run_id}", "vars": {"count": {"k": "i", "v": [10, 1000]}, "duration_s": {"k": "i", "v": [1, 60]}, "reason": {"k": "ch", "v": ["bad_zk_snapshot", "operator"]}, "run_id": {"k": "uuid", "v": None}}},
            },
            "beh": {
                "n": {"desc": "Periodic reconciliation and steady ticks.", "emit": [{"id": "reconcile_tick", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Ticks continue; cadence can increase.", "emit": [{"id": "reconcile_tick", "per_min": 2.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "pkg_manager",
            "name": "Package/Config Management",
            "svc": "pkg-manager",
            "hosts": ["pkg-01", "pkg-02"],
            "to": [{"dst": "autoscaler", "proto": "ssh", "desc": "Applies config changes."}, {"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "drift_scan": {"desc": "Drift detection scan summary.", "lvl": "INFO", "msg": "drift scan component={component} drift_found={drift_found} changes={changes}", "vars": {"component": {"k": "ch", "v": ["autoscaler", "zookeeper"]}, "drift_found": {"k": "ch", "v": ["true", "false"]}, "changes": {"k": "i", "v": [0, 50]}}},
                "config_revert": {"desc": "Drift remediation applied.", "lvl": "WARN", "msg": "config reverted by package manager component={component} from={from_state} to={to_state} ticket={ticket}", "vars": {"component": {"k": "ch", "v": ["autoscaler"]}, "from_state": {"k": "ch", "v": ["disabled", "enabled"]}, "to_state": {"k": "ch", "v": ["disabled", "enabled"]}, "ticket": {"k": "str", "v": "ticket"}}},
            },
            "beh": {
                "n": {"desc": "Periodically scans for drift.", "emit": [{"id": "drift_scan", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"desc": "Scans continue during incident.", "emit": [{"id": "drift_scan", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "cloud_api",
            "name": "Cloud Provider API (EC2)",
            "svc": "cloud-api",
            "hosts": ["awsapi-01", "awsapi-02"],
            "to": [{"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "api_request_metrics": {"desc": "Aggregate cloud API metrics.", "lvl": "INFO", "msg": "aws api metrics api={api} success={success} throttle={throttle} latency_ms_p95={latency_ms_p95}", "vars": {"api": {"k": "ch", "v": ["TerminateInstances", "DescribeInstances"]}, "success": {"k": "i", "v": [0, 5000]}, "throttle": {"k": "i", "v": [0, 5000]}}, "state_vars": {"n": {"latency_ms_p95": {"k": "i", "v": [20, 400]}}, "f": {"latency_ms_p95": {"k": "i", "v": [20, 2000]}}}},
                "terminate_instances_batch": {"desc": "Bulk terminate instances request log.", "lvl": "CRITICAL", "msg": "TerminateInstances batch request batch_id={batch_id} instance_count={instance_count} caller={caller} http_status={http_status}", "vars": {"batch_id": {"k": "hex", "v": 12}, "instance_count": {"k": "i", "v": [1, 1000]}, "caller": {"k": "ch", "v": ["autoscaler"]}}, "state_vars": {"n": {"http_status": {"k": "ch", "v": ["200"]}}, "f": {"http_status": {"k": "ch", "v": ["200", "429", "500"]}}}},
            },
            "beh": {
                "n": {"desc": "Periodic call metrics.", "emit": [{"id": "api_request_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Periodic call metrics with higher tail latencies.", "emit": [{"id": "api_request_metrics", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "monitoring",
            "name": "Monitoring/Alerting",
            "svc": "monitoring",
            "hosts": ["mon-01", "mon-02"],
            "to": [],
            "logs": {
                "scrape_success": {"desc": "Successful Prometheus scrape.", "lvl": "DEBUG", "msg": "prom scrape ok job={job} targets={targets}", "vars": {"job": {"k": "ch", "v": ["edge_lb", "app_cluster", "cache_cluster", "db_cluster", "zookeeper", "autoscaler", "pkg_manager", "cloud_api"]}, "targets": {"k": "i", "v": [1, 200]}}},
                "alert_firing": {"desc": "Alert firing notification.", "lvl": "CRITICAL", "msg": "ALERT firing name={name} severity={severity} value={value} summary={summary}", "vars": {"name": {"k": "ch", "v": ["site_unreachable", "5xx_spike", "db_latency_high"]}, "severity": {"k": "ch", "v": ["page", "ticket"]}, "value": {"k": "f", "v": [0.0, 100.0]}, "summary": {"k": "str", "v": "alert_summary"}}},
            },
            "beh": {
                "n": {"desc": "Continuous metric scraping.", "emit": [{"id": "scrape_success", "per_min": 6.0, "scope": "global"}]},
                "f": {"desc": "Scraping continues; alerts emitted via one-shots.", "emit": [{"id": "scrape_success", "per_min": 6.0, "scope": "global"}]},
            },
        },
        {
            "id": "ops_console",
            "name": "Operations Console / Feature Flag Controller",
            "svc": "ops-console",
            "hosts": ["ops-01"],
            "to": [{"dst": "edge_lb", "proto": "https", "desc": "Pushes/updates edge flags."}, {"dst": "monitoring", "proto": "https", "desc": "Metrics."}],
            "logs": {
                "audit_log": {"desc": "Low-rate operations audit log.", "lvl": "INFO", "msg": "ops audit action={action} actor={actor} target={target}", "vars": {"action": {"k": "ch", "v": ["view_dashboard", "ack_alert", "toggle_down_mode"]}, "actor": {"k": "ch", "v": ["oncall", "sre_bot"]}, "target": {"k": "ch", "v": ["edge_lb", "autoscaler", "zookeeper", "cache_cluster", "db_cluster"]}}},
                "down_mode_enabled": {"desc": "Down mode toggled on/off.", "lvl": "WARN", "msg": "down mode set enabled={enabled} actor={actor} reason={reason}", "vars": {"enabled": {"k": "ch", "v": ["true", "false"]}, "actor": {"k": "ch", "v": ["oncall", "sre"]}, "reason": {"k": "str", "v": "reason"}}},
            },
            "beh": {
                "n": {"desc": "Occasional audit actions.", "emit": [{"id": "audit_log", "per_min": 0.1, "scope": "global"}]},
                "f": {"desc": "Audit continues.", "emit": [{"id": "audit_log", "per_min": 0.1, "scope": "global"}]},
            },
        },
    ],
    "tracing": {"on": True, "origins": ["edge_lb"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "desc": "Normal user traffic through edge to app with cache/DB; autoscaler reconciles via ZooKeeper.",
            "req": [
                {"id": "web_page_ok", "desc": "Browser loads a Reddit page successfully.", "rpm": 180.0, "path": ["edge_lb", "app_cluster", "cache_cluster", "app_cluster", "edge_lb"], "emit": ["app_cluster.req_start", "app_cluster.req_end", "edge_lb.access_log"], "latency_ms": [[0, 1], [20, 120], [1, 8]], "trace": True},
                {"id": "api_get_ok", "desc": "Successful API GET.", "rpm": 120.0, "path": ["edge_lb", "app_cluster", "cache_cluster", "app_cluster", "edge_lb"], "emit": ["app_cluster.req_start", "app_cluster.req_end", "edge_lb.access_log"], "latency_ms": [[0, 1], [15, 100], [1, 8]], "trace": True},
                {"id": "autoscaler_reconcile_ok", "desc": "Autoscaler reconciliation using consistent ZooKeeper snapshot.", "rpm": 1.0, "path": ["autoscaler", "zookeeper", "autoscaler"], "emit": ["autoscaler.reconcile_start", "zookeeper.read_snapshot", "autoscaler.reconcile_done"], "latency_ms": [[0, 1], [50, 150], [5, 30]], "trace": False},
            ],
        },
        "f": {
            "desc": "Autoscaler enabled during partial ZooKeeper migration -> terminations -> outage -> cold cache and DB overload.",
            "req": [
                {"id": "web_page_503_no_backends", "desc": "Browser fails because edge has no healthy backends.", "rpm": 180.0, "path": ["edge_lb"], "emit": ["edge_lb.upstream_error", "edge_lb.access_log"], "latency_ms": [[2, 30], [0, 2]], "trace": True},
                {"id": "api_get_503_retrying", "desc": "API fails with 503 and retries.", "rpm": 120.0, "path": ["edge_lb"], "emit": ["edge_lb.upstream_error", "edge_lb.access_log"], "latency_ms": [[2, 40], [0, 2]], "retry": {"max_attempts": 3, "expected_attempts": 2.3, "emit_per_retry": ["edge_lb.retry_scheduled"], "backoff_ms": [[200, 500], [500, 1500]]}, "trace": True},
                {"id": "web_page_down_mode", "desc": "Browser served down-mode page by edge.", "rpm": 60.0, "path": ["edge_lb"], "emit": ["edge_lb.down_mode_served", "edge_lb.access_log"], "latency_ms": [[0, 2], [0, 2]], "trace": True},
                {"id": "api_down_mode", "desc": "API receives down-mode response.", "rpm": 40.0, "path": ["edge_lb"], "emit": ["edge_lb.down_mode_served", "edge_lb.access_log"], "latency_ms": [[0, 2], [0, 2]], "trace": True},
                {"id": "web_page_slow_ok", "desc": "Browser succeeds but slow due to cache misses and DB work.", "rpm": 110.0, "path": ["edge_lb", "app_cluster", "cache_cluster", "app_cluster", "db_cluster", "app_cluster", "edge_lb"], "emit": ["app_cluster.req_start", "app_cluster.db_query", "app_cluster.req_end", "edge_lb.access_log"], "latency_ms": [[0, 1], [40, 250], [10, 150], [1, 10]], "trace": True},
                {"id": "api_get_slow_ok", "desc": "API succeeds but slow due to cold caches and elevated DB latency.", "rpm": 70.0, "path": ["edge_lb", "app_cluster", "cache_cluster", "app_cluster", "db_cluster", "app_cluster", "edge_lb"], "emit": ["app_cluster.req_start", "app_cluster.db_query", "app_cluster.req_end", "edge_lb.access_log"], "latency_ms": [[0, 1], [30, 220], [10, 140], [1, 10]], "trace": True},
                {"id": "web_page_db_timeout", "desc": "Browser request times out in app/DB and surfaces as 504.", "rpm": 50.0, "path": ["edge_lb", "app_cluster", "cache_cluster", "app_cluster", "db_cluster", "app_cluster", "edge_lb"], "emit": ["app_cluster.req_start", "app_cluster.db_timeout", "edge_lb.upstream_error", "edge_lb.access_log"], "latency_ms": [[0, 1], [800, 5000], [5, 50], [0, 2]], "trace": True},
                {"id": "api_db_timeout", "desc": "API request times out; client retries once.", "rpm": 30.0, "path": ["edge_lb", "app_cluster", "cache_cluster", "app_cluster", "db_cluster", "app_cluster", "edge_lb"], "emit": ["app_cluster.req_start", "app_cluster.db_timeout", "edge_lb.upstream_error", "edge_lb.access_log"], "latency_ms": [[0, 1], [700, 4000], [5, 60], [0, 2]], "retry": {"max_attempts": 2, "expected_attempts": 1.6, "emit_per_retry": ["edge_lb.retry_scheduled"], "backoff_ms": [[300, 800]]}, "trace": True},
                {"id": "autoscaler_reconcile_bad", "desc": "Autoscaler reads inconsistent ZooKeeper data and decides unsafe terminations.", "rpm": 2.0, "path": ["autoscaler", "zookeeper", "autoscaler"], "emit": ["autoscaler.reconcile_start", "zookeeper.read_inconsistent", "autoscaler.scale_decision_bad"], "latency_ms": [[0, 1], [80, 300], [5, 80]], "trace": False},
            ],
        },
    },
    "assumptions": [
        "Timeline compressed to 48 minutes; ends in degraded state.",
        "App servers serve website and API; separate flows model patterns.",
        "Cache-to-DB access modeled via app->cache then (on miss) app->db.",
        "Mass termination represented with one-shot summaries and cloud API batches.",
        "Retries modeled at edge using flow retry definitions.",
    ],
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "aug11_autoscaler_zk_migration_outage",
        "title": "Autoscaler re-enabled during ZooKeeper migration triggers mass terminations",
        "states": {"n": "normal", "f": "failure"},
        "time": {"total_minutes": 48, "phases": {"n": {"start_min": 0, "end_min": 24}, "f": {"start_min": 24, "end_min": 48}}},
        "phases": {
            "n": {
                "flows": ["web_page_ok", "api_get_ok", "autoscaler_reconcile_ok"],
                "manifestation": ["edge_lb.hc_ok", "app_cluster.req_end", "edge_lb.access_log", "zookeeper.quorum_status", "monitoring.scrape_success"],
            },
            "f": {
                "events": [
                    {"order": 1, "at_min": 24, "component": "pkg_manager", "flows": ["autoscaler_reconcile_bad", "web_page_slow_ok", "api_get_slow_ok"], "rate_multipliers": {"web_page_slow_ok": 1.0, "api_get_slow_ok": 1.0, "autoscaler_reconcile_bad": 1.0, "web_page_503_no_backends": 0.0, "api_get_503_retrying": 0.0, "web_page_down_mode": 0.0, "api_down_mode": 0.0, "web_page_db_timeout": 0.0, "api_db_timeout": 0.0, "db_cluster.conn_pool_waiting": 0.2, "cache_cluster.warmup_tick": 0.0}, "one_shots": [{"ref": "pkg_manager.config_revert", "count": 2}, {"ref": "autoscaler.enabled_state_change", "count": 1, "hosts": ["as-01"]}], "manifestation": ["pkg_manager.config_revert", "autoscaler.enabled_state_change", "zookeeper.migration_stage"]},
                    {"order": 2, "at_min": 25, "component": "autoscaler", "flows": ["autoscaler_reconcile_bad", "web_page_503_no_backends", "api_get_503_retrying"], "rate_multipliers": {"web_page_slow_ok": 0.0, "api_get_slow_ok": 0.0, "web_page_503_no_backends": 1.2, "api_get_503_retrying": 1.2, "autoscaler_reconcile_bad": 1.0, "app_cluster.gc_pause_warn": 0.1, "cache_cluster.redis_stats": 0.0, "cache_cluster.warmup_tick": 0.0, "db_cluster.conn_pool_waiting": 0.1}, "one_shots": [{"ref": "autoscaler.bulk_termination_summary", "count": 1}, {"ref": "cloud_api.terminate_instances_batch", "count": 3}, {"ref": "monitoring.alert_firing", "count": 2}], "manifestation": ["autoscaler.scale_decision_bad", "zookeeper.read_inconsistent", "cloud_api.terminate_instances_batch", "edge_lb.upstream_error", "monitoring.alert_firing"]},
                    {"order": 3, "at_min": 30, "component": "ops_console", "flows": ["web_page_down_mode", "api_down_mode", "web_page_503_no_backends", "api_get_503_retrying"], "rate_multipliers": {"web_page_down_mode": 1.0, "api_down_mode": 1.0, "web_page_503_no_backends": 0.3, "api_get_503_retrying": 0.2, "autoscaler_reconcile_bad": 0.5, "app_cluster.gc_pause_warn": 0.3, "cache_cluster.redis_stats": 0.3, "cache_cluster.warmup_tick": 0.2, "db_cluster.conn_pool_waiting": 0.2}, "one_shots": [{"ref": "ops_console.down_mode_enabled", "count": 1, "hosts": ["ops-01"]}], "manifestation": ["ops_console.down_mode_enabled", "edge_lb.down_mode_served", "edge_lb.upstream_error"]},
                    {"order": 4, "at_min": 38, "component": "edge_lb", "flows": ["web_page_slow_ok", "api_get_slow_ok", "web_page_db_timeout", "api_db_timeout"], "rate_multipliers": {"web_page_down_mode": 0.0, "api_down_mode": 0.0, "web_page_slow_ok": 0.9, "api_get_slow_ok": 0.9, "web_page_db_timeout": 0.3, "api_db_timeout": 0.2, "web_page_503_no_backends": 0.05, "api_get_503_retrying": 0.02, "autoscaler_reconcile_bad": 0.2, "app_cluster.gc_pause_warn": 1.0, "cache_cluster.redis_stats": 1.0, "cache_cluster.warmup_tick": 1.0, "db_cluster.conn_pool_waiting": 1.0, "db_cluster.slow_query_warn": 1.0}, "latency_multipliers": {"web_page_slow_ok": {"p50": 1.2, "p95": 1.5}, "api_get_slow_ok": {"p50": 1.2, "p95": 1.6}}, "manifestation": ["app_cluster.req_end", "cache_cluster.warmup_tick", "db_cluster.conn_pool_waiting"]},
                    {"order": 5, "at_min": 42, "component": "db_cluster", "flows": ["web_page_db_timeout", "api_db_timeout", "web_page_slow_ok", "api_get_slow_ok"], "rate_multipliers": {"web_page_slow_ok": 0.8, "api_get_slow_ok": 0.8, "web_page_db_timeout": 2.0, "api_db_timeout": 2.5, "db_cluster.conn_pool_waiting": 3.0, "db_cluster.slow_query_warn": 4.0, "app_cluster.gc_pause_warn": 1.5, "cache_cluster.warmup_tick": 1.2}, "latency_multipliers": {"web_page_db_timeout": {"p50": 1.3, "p95": 2.0}, "api_db_timeout": {"p50": 1.3, "p95": 2.2}, "web_page_slow_ok": {"p50": 1.5, "p95": 2.0}, "api_get_slow_ok": {"p50": 1.5, "p95": 2.2}}, "manifestation": ["db_cluster.conn_pool_waiting", "db_cluster.slow_query_warn", "app_cluster.db_timeout", "edge_lb.upstream_error", "edge_lb.retry_scheduled"]},
                    {"order": 6, "at_min": 46, "component": "cache_cluster", "flows": ["web_page_slow_ok", "api_get_slow_ok", "web_page_db_timeout", "api_db_timeout"], "rate_multipliers": {"web_page_slow_ok": 0.9, "api_get_slow_ok": 0.9, "web_page_db_timeout": 1.2, "api_db_timeout": 1.5, "db_cluster.conn_pool_waiting": 2.0, "db_cluster.slow_query_warn": 2.0, "app_cluster.gc_pause_warn": 1.2}, "latency_multipliers": {"web_page_slow_ok": {"p50": 1.3, "p95": 1.8}, "api_get_slow_ok": {"p50": 1.3, "p95": 2.0}, "web_page_db_timeout": {"p50": 1.2, "p95": 1.8}, "api_db_timeout": {"p50": 1.2, "p95": 2.0}}, "manifestation": ["cache_cluster.redis_stats", "cache_cluster.warmup_tick", "db_cluster.conn_pool_waiting", "edge_lb.upstream_error"]},
                ],
                "steady": [
                    {"component": "edge_lb", "manifestation": ["edge_lb.upstream_error", "edge_lb.retry_scheduled", "edge_lb.access_log"]},
                    {"component": "cache_cluster", "manifestation": ["cache_cluster.redis_stats", "cache_cluster.warmup_tick"]},
                    {"component": "db_cluster", "manifestation": ["db_cluster.conn_pool_waiting", "db_cluster.slow_query_warn"]},
                ],
                "flows": ["autoscaler_reconcile_bad", "web_page_503_no_backends", "api_get_503_retrying", "web_page_down_mode", "api_down_mode", "web_page_slow_ok", "api_get_slow_ok", "web_page_db_timeout", "api_db_timeout"],
                "manifestation": ["pkg_manager.config_revert", "autoscaler.enabled_state_change", "autoscaler.scale_decision_bad", "autoscaler.bulk_termination_summary", "zookeeper.read_inconsistent", "cloud_api.terminate_instances_batch", "edge_lb.upstream_error", "edge_lb.retry_scheduled", "ops_console.down_mode_enabled", "edge_lb.down_mode_served", "cache_cluster.warmup_tick", "db_cluster.conn_pool_waiting", "db_cluster.slow_query_warn", "app_cluster.db_timeout", "monitoring.alert_firing"],
            },
        },
    },
    "assumptions": ["Compressed incident window; ends degraded.", "Down mode modeled as edge-served response controlled by ops console.", "Terminations represented by one-shot summaries and cloud API batch logs."],
}

BASE_TIME = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)

_HEX_ALPH = "0123456789abcdef"
_SUBS = ["python", "news", "funny", "gaming", "technology", "askreddit", "pics", "worldnews", "science", "movies"]


def iso_utc_ms(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def rand_hex(n: int) -> str:
    return "".join(random.choice(_HEX_ALPH) for _ in range(n))


def sample_lognormal_ms(p50: float, p95: float, softcap_mult: float = 2.5) -> float:
    p50 = float(p50)
    p95 = float(p95)
    if p95 <= 0:
        return 0.0
    if p50 <= 0:
        return random.random() * p95
    if p95 <= p50:
        return p50

    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    if sigma <= 0:
        return p50

    x = random.lognormvariate(mu, sigma)
    softcap = softcap_mult * p95
    return min(x, softcap)


def sample_attempts(max_attempts: int, expected_attempts: float) -> int:
    E = clamp(expected_attempts, 1.0, float(max_attempts))
    a = int(math.floor(E))
    b = int(math.ceil(E))
    a = max(1, min(max_attempts, a))
    b = max(1, min(max_attempts, b))
    if a == b:
        return a
    p_b = E - a
    return b if random.random() < p_b else a


def sample_from_domain(kind: str, dom: Any) -> Any:
    if kind == "i":
        lo, hi = int(dom[0]), int(dom[1])
        return random.randint(lo, hi)
    if kind == "f":
        lo, hi = float(dom[0]), float(dom[1])
        return lo + (hi - lo) * random.random()
    if kind == "ch":
        return random.choice(list(dom))
    if kind == "uuid":
        return str(uuid.uuid4())
    if kind == "hex":
        return rand_hex(int(dom))
    if kind == "ip":
        return f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
    if kind == "str":
        hint = str(dom or "")
        if hint == "http_path":
            sub = random.choice(_SUBS)
            choices = ["/", f"/r/{sub}/hot", "/api/v1/me", f"/api/v1/subreddit/{sub}/hot"]
            return random.choice(choices)
        if hint == "err_detail":
            return random.choice(
                [
                    "no healthy backends for cluster=app_cluster",
                    "connect timeout to upstream=app_cluster",
                    "upstream read timeout",
                    "routing table empty; all backends draining",
                ]
            )
        if hint == "reason":
            return random.choice(
                [
                    "config drift remediation",
                    "restore in progress",
                    "migration safety procedure",
                    "incident response",
                    "guardrails disabled during maintenance",
                ]
            )
        if hint == "ticket":
            return f"INC-2026-0313-{random.randint(100,999)}"
        if hint == "sql":
            return random.choice(
                [
                    "SELECT * FROM posts WHERE subreddit_id=? ORDER BY score DESC LIMIT ?",
                    "SELECT * FROM comments WHERE post_id=? ORDER BY created_utc DESC LIMIT ?",
                    "SELECT * FROM users WHERE id=?",
                    "SELECT * FROM sessions WHERE user_id=? AND active=1",
                ]
            )
        if hint == "alert_summary":
            return random.choice(["Site availability below SLO threshold", "5xx rate above alerting threshold", "Database p95 latency elevated"])
        return hint[:120] if hint else "n/a"
    return str(dom)


@dataclass(frozen=True)
class LogRef:
    component_id: str
    log_id: str


def parse_ref(ref: str) -> LogRef:
    c, l = ref.split(".", 1)
    return LogRef(c, l)


COMPONENTS: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}


def get_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return COMPONENTS[component_id]["logs"][log_id]


def get_service(component_id: str) -> str:
    svc = COMPONENTS[component_id].get("svc")
    return svc or ""


def get_hosts(component_id: str) -> List[str]:
    return COMPONENTS[component_id].get("hosts") or []


def pick_host(component_id: str) -> str:
    hs = get_hosts(component_id)
    if not hs:
        return ""
    return random.choice(hs)


def template_placeholders(msg: str) -> List[str]:
    return re.findall(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}", msg)


def build_failure_events() -> List[Dict[str, Any]]:
    evs = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    evs.sort(key=lambda e: (e["at_min"], e["order"]))
    return evs


FAIL_EVENTS = build_failure_events()
N_END = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
F_START = SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]
TOTAL_MINUTES = SCENARIO["scenario"]["time"]["total_minutes"]


def format_value(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.3f}".rstrip("0").rstrip(".")
    return str(v)


def generate_vars(component_id: str, log_id: str, state: str, overrides: Dict[str, Any], minute: int, host: str) -> Dict[str, Any]:
    tmpl = get_template(component_id, log_id)
    out: Dict[str, Any] = {}

    for k, spec in (tmpl.get("vars") or {}).items():
        out[k] = sample_from_domain(spec["k"], spec["v"])

    sv = tmpl.get("state_vars")
    if sv:
        for k, spec in (sv.get(state) or {}).items():
            out[k] = sample_from_domain(spec["k"], spec["v"])

    if "host" in out and host:
        out["host"] = host
    if "node" in out and host:
        out["node"] = host

    out.update(overrides)

    if "route" in out and isinstance(out["route"], str) and "{sub}" in out["route"]:
        out["route"] = out["route"].replace("{sub}", random.choice(_SUBS))
    if "path" in out and isinstance(out["path"], str) and "{sub}" in out["path"]:
        out["path"] = out["path"].replace("{sub}", random.choice(_SUBS))

    return out


def emit_log(rows: List[Dict[str, Any]], ts_s: float, component_id: str, log_id: str, state: str, trace_id: str, host: str, overrides: Dict[str, Any], minute: int) -> None:
    tmpl = get_template(component_id, log_id)
    if trace_id and "trace_id" in template_placeholders(tmpl["msg"]):
        overrides = dict(overrides)
        overrides["trace_id"] = trace_id

    vars_ = generate_vars(component_id, log_id, state, overrides, minute=minute, host=host)
    msg = tmpl["msg"].format(**{k: format_value(v) for k, v in vars_.items()})

    dt = BASE_TIME + timedelta(seconds=ts_s)
    rows.append(
        {
            "timestamp": iso_utc_ms(dt),
            "level": tmpl["lvl"],
            "message": msg[:1000],
            "trace_id": trace_id or "",
            "service": get_service(component_id),
            "host": host or "",
            "_ts_s": ts_s,
        }
    )


def pick_route_for_flow(flow_id: str) -> str:
    if flow_id in {"web_page_ok", "web_page_slow_ok", "web_page_db_timeout"}:
        return random.choice(["/", f"/r/{random.choice(_SUBS)}/hot"])
    if flow_id in {"api_get_ok", "api_get_slow_ok", "api_db_timeout"}:
        return random.choice(["/api/v1/me", f"/api/v1/subreddit/{random.choice(_SUBS)}/hot"])
    if flow_id in {"web_page_503_no_backends", "web_page_down_mode"}:
        return random.choice(["/", f"/r/{random.choice(_SUBS)}/hot"])
    if flow_id in {"api_get_503_retrying", "api_down_mode"}:
        return random.choice(["/api/v1/me", f"/api/v1/subreddit/{random.choice(_SUBS)}/hot"])
    return "/"


def cache_trend_failure(minute: int) -> Tuple[int, float]:
    restore_t = max(0, minute - 38)
    base_keys = int(clamp((restore_t**1.35) * 180000, 0, 5_000_000))
    keys = int(clamp(base_keys + random.randint(-80000, 120000), 0, 5_000_000))
    hr = clamp(0.05 + (keys / 5_000_000) * 0.55 + (random.random() - 0.5) * 0.06, 0.00, 0.70)
    return keys, hr


def warmup_trend_failure(minute: int) -> Tuple[int, float]:
    restore_t = max(0, minute - 38)
    loaded = int(clamp((restore_t**1.25) * 280000 + random.randint(-120000, 220000), 0, 10_000_000))
    miss_rate = clamp(0.95 - (loaded / 10_000_000) * 0.60 + (random.random() - 0.5) * 0.08, 0.20, 1.00)
    return loaded, miss_rate


def db_pressure_from_multiplier(mult: float) -> float:
    return clamp((math.log1p(mult) / math.log1p(4.0)), 0.0, 1.0)


def init_active_multipliers() -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    return {}, {}


def apply_event_multipliers(active_rate: Dict[str, float], active_lat: Dict[str, Dict[str, float]], ev: Dict[str, Any]) -> None:
    for k, v in (ev.get("rate_multipliers") or {}).items():
        active_rate[k] = float(v)
    for fid, mult in (ev.get("latency_multipliers") or {}).items():
        active_lat[fid] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}


def get_rate_multiplier(active_rate: Dict[str, float], key: str) -> float:
    return float(active_rate.get(key, 1.0))


def get_latency_multiplier(active_lat: Dict[str, Dict[str, float]], flow_id: str) -> Dict[str, float]:
    return dict(active_lat.get(flow_id, {"p50": 1.0, "p95": 1.0}))


def simulate() -> pd.DataFrame:
    random.seed(1337)
    np.random.seed(1337)
    rng = np.random.default_rng(1337)

    rows: List[Dict[str, Any]] = []

    flows_n = {f["id"]: f for f in SYSTEM["flows"]["n"]["req"]}
    flows_f = {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}

    bg_emit: Dict[str, List[Tuple[str, str, float, str]]] = {"n": [], "f": []}
    for comp_id, comp in COMPONENTS.items():
        for st in ("n", "f"):
            for e in (comp.get("beh", {}).get(st, {}).get("emit") or []):
                bg_emit[st].append((comp_id, e["id"], float(e["per_min"]), e.get("scope", "per_host")))

    active_rate, active_lat = init_active_multipliers()
    fail_event_idx = 0
    last_db_wait_mult = 1.0

    for minute in range(TOTAL_MINUTES):
        state = "n" if minute < N_END else "f"

        if state == "f":
            while fail_event_idx < len(FAIL_EVENTS) and int(FAIL_EVENTS[fail_event_idx]["at_min"]) == minute:
                ev = FAIL_EVENTS[fail_event_idx]
                apply_event_multipliers(active_rate, active_lat, ev)

                for os in (ev.get("one_shots") or []):
                    ref = parse_ref(os["ref"])
                    count = int(os["count"])
                    hosts = os.get("hosts")
                    if hosts is None:
                        hosts = get_hosts(ref.component_id) or [""]

                    base_ts = minute * 60.0
                    for i in range(count):
                        host = hosts[i % len(hosts)] if hosts else ""
                        ts_s = base_ts + (i * 0.015) + random.random() * 0.02

                        overrides: Dict[str, Any] = {}
                        if os["ref"] == "pkg_manager.config_revert":
                            overrides.update({"component": "autoscaler", "from_state": "disabled", "to_state": "enabled"})
                        elif os["ref"] == "autoscaler.enabled_state_change":
                            overrides.update({"enabled": "true", "actor": "pkg_manager", "reason": "config drift remediation"})
                        elif os["ref"] == "autoscaler.bulk_termination_summary":
                            overrides.update({"count": random.randint(650, 950), "duration_s": random.randint(10, 20), "reason": "bad_zk_snapshot"})
                        elif os["ref"] == "cloud_api.terminate_instances_batch":
                            overrides.update({"instance_count": random.randint(150, 400), "caller": "autoscaler", "http_status": random.choice(["200", "200", "429"])})
                        elif os["ref"] == "monitoring.alert_firing":
                            overrides.update({"name": random.choice(["site_unreachable", "5xx_spike"]), "severity": "page", "value": clamp(70 + random.random() * 30, 0.0, 100.0), "summary": "Site availability below SLO threshold"})
                        elif os["ref"] == "ops_console.down_mode_enabled":
                            overrides.update({"enabled": "true", "actor": "oncall", "reason": "restore in progress"})

                        emit_log(rows, ts_s, ref.component_id, ref.log_id, state, trace_id="", host=host, overrides=overrides, minute=minute)

                fail_event_idx += 1

            last_db_wait_mult = get_rate_multiplier(active_rate, "db_cluster.conn_pool_waiting")

        for comp_id, log_id, per_min, scope in bg_emit[state]:
            mult = 1.0
            if state == "f":
                mult = get_rate_multiplier(active_rate, f"{comp_id}.{log_id}")
            rate = per_min * mult
            if rate <= 0:
                continue

            hosts = get_hosts(comp_id)
            if scope == "global":
                count = int(rng.poisson(rate))
                for _ in range(count):
                    ts_s = minute * 60.0 + random.random() * 60.0
                    host = pick_host(comp_id) if hosts else ""
                    overrides: Dict[str, Any] = {}

                    if comp_id == "autoscaler" and log_id == "reconcile_tick":
                        overrides["enabled"] = "true"
                    if comp_id == "pkg_manager" and log_id == "drift_scan":
                        overrides["drift_found"] = "false" if state == "n" else random.choice(["false", "false", "true"])
                        overrides["changes"] = 0 if overrides["drift_found"] == "false" else random.randint(1, 5)
                    if comp_id == "edge_lb" and log_id == "hc_ok":
                        overrides["target"] = "app_cluster" if state == "f" else random.choice(["app_cluster", "cache_cluster", "db_cluster"])

                    if comp_id == "cache_cluster" and log_id == "redis_stats":
                        if state == "f":
                            keys, hr = cache_trend_failure(minute)
                            overrides["key_count"] = keys
                            overrides["hit_rate"] = hr
                            overrides["node"] = host
                        else:
                            overrides["node"] = host
                    if comp_id == "cache_cluster" and log_id == "warmup_tick":
                        if state == "f":
                            loaded, miss = warmup_trend_failure(minute)
                            overrides["loaded_keys"] = loaded
                            overrides["miss_rate"] = miss

                    if comp_id == "db_cluster" and log_id == "conn_pool_waiting":
                        pressure = db_pressure_from_multiplier(last_db_wait_mult if state == "f" else 1.0)
                        if state == "f":
                            overrides["waiting"] = int(clamp(30 + pressure * 1700 + random.randint(-50, 120), 10, 2000))
                            overrides["wait_ms_p95"] = int(clamp(150 + pressure * 12000 + random.randint(-100, 600), 50, 15000))
                            overrides["host"] = host
                        else:
                            overrides["host"] = host
                    if comp_id == "db_cluster" and log_id == "slow_query_warn":
                        if state == "f":
                            mult_sq = get_rate_multiplier(active_rate, "db_cluster.slow_query_warn")
                            pressure = db_pressure_from_multiplier(mult_sq)
                            overrides["duration_ms"] = int(clamp(800 + pressure * 16000 + random.randint(-200, 900), 500, 20000))
                            overrides["host"] = host
                        else:
                            overrides["host"] = host

                    if comp_id == "db_cluster" and log_id == "db_metrics":
                        overrides["host"] = host

                    if comp_id == "zookeeper" and log_id == "quorum_status":
                        overrides["node"] = host
                    if comp_id == "zookeeper" and log_id == "migration_stage" and state == "f":
                        tmin = minute - F_START
                        stage = "snapshot_copy" if tmin < 6 else "cutover_partial" if tmin < 14 else "verify"
                        overrides.update({"stage": stage, "progress_pct": int(clamp(tmin * 7 + random.randint(-5, 10), 0, 100))})

                    emit_log(rows, ts_s, comp_id, log_id, state, trace_id="", host=host, overrides=overrides, minute=minute)
            else:
                host_list = hosts if hosts else [""]
                for h in host_list:
                    count = int(rng.poisson(rate))
                    for _ in range(count):
                        ts_s = minute * 60.0 + random.random() * 60.0
                        overrides = {}

                        if comp_id == "autoscaler" and log_id == "reconcile_tick":
                            overrides["enabled"] = "true"
                        if comp_id == "pkg_manager" and log_id == "drift_scan":
                            overrides["drift_found"] = "false" if state == "n" else random.choice(["false", "false", "true"])
                            overrides["changes"] = 0 if overrides["drift_found"] == "false" else random.randint(1, 5)
                        if comp_id == "edge_lb" and log_id == "hc_ok":
                            overrides["target"] = "app_cluster" if state == "f" else random.choice(["app_cluster", "cache_cluster", "db_cluster"])

                        if comp_id == "cache_cluster" and log_id == "redis_stats":
                            if state == "f":
                                keys, hr = cache_trend_failure(minute)
                                overrides["key_count"] = keys
                                overrides["hit_rate"] = hr
                            overrides["node"] = h
                        if comp_id == "cache_cluster" and log_id == "warmup_tick" and state == "f":
                            loaded, miss = warmup_trend_failure(minute)
                            overrides["loaded_keys"] = loaded
                            overrides["miss_rate"] = miss

                        if comp_id == "db_cluster" and log_id == "conn_pool_waiting":
                            pressure = db_pressure_from_multiplier(last_db_wait_mult if state == "f" else 1.0)
                            if state == "f":
                                overrides["waiting"] = int(clamp(30 + pressure * 1700 + random.randint(-50, 120), 10, 2000))
                                overrides["wait_ms_p95"] = int(clamp(150 + pressure * 12000 + random.randint(-100, 600), 50, 15000))
                                overrides["host"] = h
                            else:
                                overrides["host"] = h

                        if comp_id == "db_cluster" and log_id == "slow_query_warn":
                            if state == "f":
                                mult_sq = get_rate_multiplier(active_rate, "db_cluster.slow_query_warn")
                                pressure = db_pressure_from_multiplier(mult_sq)
                                overrides["duration_ms"] = int(clamp(800 + pressure * 16000 + random.randint(-200, 900), 500, 20000))
                            overrides["host"] = h

                        if comp_id == "db_cluster" and log_id == "db_metrics":
                            overrides["host"] = h

                        if comp_id == "zookeeper" and log_id == "quorum_status":
                            overrides["node"] = h
                        if comp_id == "zookeeper" and log_id == "migration_stage" and state == "f":
                            tmin = minute - F_START
                            stage = "snapshot_copy" if tmin < 6 else "cutover_partial" if tmin < 14 else "verify"
                            overrides.update({"stage": stage, "progress_pct": int(clamp(tmin * 7 + random.randint(-5, 10), 0, 100))})

                        emit_log(rows, ts_s, comp_id, log_id, state, trace_id="", host=h, overrides=overrides, minute=minute)

        active_flows = flows_n if state == "n" else flows_f
        for flow_id, flow in active_flows.items():
            rpm = float(flow["rpm"])
            if state == "f":
                rpm *= get_rate_multiplier(active_rate, flow_id)
            if rpm <= 0:
                continue

            nreq = int(rng.poisson(rpm))
            if nreq <= 0:
                continue

            lat_mult = {"p50": 1.0, "p95": 1.0}
            if state == "f":
                lat_mult = get_latency_multiplier(active_lat, flow_id)

            for _ in range(nreq):
                start_ts = minute * 60.0 + random.random() * 60.0

                inst_hosts: Dict[str, str] = {}
                comp_ids = [parse_ref(e).component_id for e in flow.get("emit", [])]
                if flow.get("retry"):
                    comp_ids += [parse_ref(r).component_id for r in flow.get("retry", {}).get("emit_per_retry", [])]
                for cid in set(comp_ids):
                    inst_hosts[cid] = pick_host(cid)

                trace_id = ""
                if SYSTEM["tracing"]["on"] and flow.get("trace", False):
                    trace_id = rand_hex(32)

                req_id = str(uuid.uuid4())
                route_or_path = pick_route_for_flow(flow_id)
                method = "GET"  # all our generated routes are HTTP paths

                retry = flow.get("retry")
                attempts = 1
                backoffs: List[List[float]] = []
                if retry:
                    attempts = sample_attempts(int(retry["max_attempts"]), float(retry["expected_attempts"]))
                    backoffs = list(retry.get("backoff_ms") or [])

                if flow_id in {"web_page_ok", "api_get_ok", "web_page_slow_ok", "api_get_slow_ok"}:
                    outcome = "ok"
                elif flow_id in {"web_page_503_no_backends", "api_get_503_retrying"}:
                    outcome = "no_backends"
                elif flow_id in {"web_page_down_mode", "api_down_mode"}:
                    outcome = "down_mode"
                elif flow_id in {"web_page_db_timeout", "api_db_timeout"}:
                    outcome = "db_timeout"
                elif flow_id.startswith("autoscaler_"):
                    outcome = "autoscaler"
                else:
                    outcome = "other"

                chosen_db_host = random.choice(["db-01", "db-02", "db-03"])
                run_id = str(uuid.uuid4())

                prev_attempt_end = start_ts
                for attempt in range(1, attempts + 1):
                    if attempt >= 2 and retry:
                        p50_b, p95_b = backoffs[attempt - 2]
                        sampled_backoff_ms = sample_lognormal_ms(p50_b, p95_b, softcap_mult=2.8)
                        backoff_ms = max(1, int(round(sampled_backoff_ms)))  # allow values beyond nominal template domain

                        retry_ts = prev_attempt_end + 0.002 + random.random() * 0.010
                        emit_log(
                            rows,
                            retry_ts,
                            "edge_lb",
                            "retry_scheduled",
                            state,
                            trace_id=trace_id,
                            host=inst_hosts.get("edge_lb", pick_host("edge_lb")),
                            overrides={
                                "req_id": req_id,
                                "attempt": attempt,
                                "reason": "503" if outcome == "no_backends" else "timeout",
                                "backoff_ms": backoff_ms,
                            },
                            minute=minute,
                        )
                        attempt_start_ts = prev_attempt_end + backoff_ms / 1000.0
                    else:
                        attempt_start_ts = start_ts

                    last_ts = attempt_start_ts
                    emitted_ts: Dict[str, float] = {}

                    for j, ref_s in enumerate(flow.get("emit", [])):
                        ref = parse_ref(ref_s)
                        p50, p95 = flow["latency_ms"][j]
                        eff_p50 = p50 * lat_mult["p50"] if state == "f" else p50
                        eff_p95 = p95 * lat_mult["p95"] if state == "f" else p95
                        dt_ms = sample_lognormal_ms(eff_p50, eff_p95, softcap_mult=2.7)
                        next_ts = last_ts + dt_ms / 1000.0
                        if next_ts <= last_ts:
                            next_ts = last_ts + 0.001

                        overrides: Dict[str, Any] = {}
                        host = inst_hosts.get(ref.component_id, pick_host(ref.component_id))

                        if outcome in {"ok", "db_timeout"} and ref.component_id == "app_cluster":
                            overrides["req_id"] = req_id
                            overrides["trace_id"] = trace_id
                            if ref.log_id == "req_start":
                                overrides["route"] = route_or_path
                            elif ref.log_id == "db_query":
                                d_ms = int(clamp(dt_ms + random.randint(-15, 25), 10 if state == "f" else 1, 4000 if state == "f" else 200))
                                overrides.update({"db_host": chosen_db_host, "duration_ms": d_ms})
                            elif ref.log_id == "req_end":
                                if "app_req_start" in emitted_ts:
                                    app_lat_ms = int(clamp((next_ts - emitted_ts["app_req_start"]) * 1000.0, 5 if state == "n" else 20, 400 if state == "n" else 10000))
                                else:
                                    app_lat_ms = int(clamp(dt_ms, 5 if state == "n" else 20, 400 if state == "n" else 10000))
                                overrides["latency_ms"] = app_lat_ms
                                overrides["status"] = 200 if outcome == "ok" else 504
                                if state == "n":
                                    overrides["cache"] = random.choice(["hit", "hit_stale", "miss"])
                                else:
                                    overrides["cache"] = random.choice(["miss_cold", "miss", "hit_stale"]) if minute >= 38 else "miss"
                        if outcome == "db_timeout" and ref.component_id == "app_cluster" and ref.log_id == "db_timeout":
                            overrides["req_id"] = req_id
                            overrides["trace_id"] = trace_id
                            overrides["db_host"] = chosen_db_host
                            timeout_ms = random.randint(1500, 3000)
                            waited_ms = int(clamp(timeout_ms + random.randint(200, 4000), 500, 15000))
                            overrides.update({"timeout_ms": timeout_ms, "waited_ms": waited_ms})

                        if ref.component_id == "edge_lb":
                            overrides["req_id"] = req_id
                            overrides["trace_id"] = trace_id
                            overrides["method"] = method
                            overrides["path"] = route_or_path

                            if ref.log_id == "upstream_error":
                                if outcome == "no_backends":
                                    overrides.update({"upstream": "none", "error": "no_healthy_upstream", "detail": "no healthy backends for cluster=app_cluster"})
                                elif outcome == "db_timeout":
                                    elapsed_ms = int(clamp((next_ts - attempt_start_ts) * 1000.0, 0, 60000))
                                    overrides.update({"upstream": "app_cluster", "error": "upstream_timeout", "detail": f"upstream read timeout after {elapsed_ms}ms"})
                                else:
                                    overrides.update({"upstream": "app_cluster", "error": "connect_timeout", "detail": "connect timeout to upstream=app_cluster"})
                            elif ref.log_id == "down_mode_served":
                                overrides.update({"reason": "restore_in_progress", "status": 200, "path": route_or_path})
                            elif ref.log_id == "access_log":
                                if outcome == "no_backends":
                                    status = 503
                                    upstream = "none"
                                elif outcome == "down_mode":
                                    status = 200
                                    upstream = "down_mode"
                                elif outcome == "db_timeout":
                                    status = 504
                                    upstream = "app_cluster"
                                else:
                                    status = 200
                                    upstream = "app_cluster"

                                upstream_ms = int(clamp((next_ts - attempt_start_ts) * 1000.0 + random.randint(-2, 12), 0, 10000))
                                bytes_out = random.randint(700, 60000) if "api" in flow_id else random.randint(1500, 180000)
                                overrides.update({"status": status, "upstream": upstream, "upstream_ms": upstream_ms, "bytes_out": bytes_out})

                        if outcome == "autoscaler":
                            if ref.component_id == "autoscaler" and ref.log_id == "reconcile_start":
                                overrides.update({"run_id": run_id, "reason": "periodic"})
                            elif ref.component_id == "autoscaler" and ref.log_id == "reconcile_done":
                                overrides.update({"run_id": run_id, "desired": random.randint(300, 800), "current": random.randint(300, 800), "actions": random.randint(0, 3)})
                            elif ref.component_id == "autoscaler" and ref.log_id == "scale_decision_bad":
                                servers_seen = random.randint(0, 900)
                                terminate_count = int(clamp(500 + random.randint(-80, 220), 1, 2000))
                                overrides.update({"run_id": run_id, "servers_seen": servers_seen, "terminate_count": terminate_count, "guardrails": random.choice(["none", "partial"])})
                            elif ref.component_id == "zookeeper" and ref.log_id in {"read_snapshot", "read_inconsistent"}:
                                overrides["version"] = int(rand_hex(6), 16) % 1000000 + 1
                                if ref.log_id == "read_snapshot":
                                    overrides["servers_seen"] = random.randint(800, 2500) if state == "n" else random.randint(200, 2000)
                                else:
                                    overrides["servers_seen"] = random.randint(0, 700)
                                    overrides["missing_azs"] = random.choice(["1", "2", "3"])

                        if ref.component_id == "app_cluster" and ref.log_id == "req_start":
                            emitted_ts["app_req_start"] = next_ts

                        emit_log(rows, next_ts, ref.component_id, ref.log_id, state, trace_id=trace_id if flow.get("trace") else "", host=host, overrides=overrides, minute=minute)
                        last_ts = next_ts

                    prev_attempt_end = last_ts

    df = pd.DataFrame(rows)
    df.sort_values(by=["_ts_s", "service", "host", "level"], inplace=True, kind="mergesort")
    df.drop(columns=["_ts_s"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
