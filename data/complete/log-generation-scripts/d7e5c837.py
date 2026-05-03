"""
Plan / simulation design (deterministic, reproducible)
=====================================================

Goal
----
Generate a plausible log stream for a simplified Chrome Sync system under a normal phase followed by a
failure phase with event-driven rate/latency multipliers, retries, and discrete one-shot logs. Output
is written to logs.csv with columns:
timestamp, level, message, trace_id, service, host (sorted ascending).

Key modeling choices
--------------------
1) Time base:
   - Scenario minute 0 maps to 2026-03-13T12:00:00.000Z (UTC).
   - We simulate emissions minute-by-minute. Within each minute, events are placed uniformly at random.

2) Determinism:
   - Use a fixed seed for both random and numpy.
   - Implement deterministic UUID generation using the numpy RNG.

3) Emission sources (only those allowed by the model):
   - Background logs via components[].beh.<state>.emit[] (Poisson per-minute counts).
   - Flow logs per request attempt via flows.<state>.req[].emit[] (Poisson RPM per minute).
   - Retry-only logs via flows.<state>.req[].retry.emit_per_retry[] (emitted once per retry attempt).
   - One-shot logs via scenario.phases.f.events[].one_shots[] (not rate-scaled).

4) Failure controller:
   - Failure events apply persistent overrides of:
       a) rate multipliers for flows (flow_id -> multiplier) and background log sources (component.log_id -> multiplier)
       b) latency multipliers for flows (flow_id -> {p50, p95})
   - Active multipliers for a minute are the latest overrides at or before that minute.

5) Latency and backoff sampling:
   - Use lognormal distributions parameterized by p50/p95 (p50=median; p95=95th percentile),
     with a softcap at 3*p95.
   - For flow latency_ms entries, treat each as a delay since the previous emitted log within an attempt.
   - For retry backoff_ms, sample lognormal with the same softcap rule.

6) Tracing:
   - tracing.on=true. For each flow instance with trace=true, generate one 32-hex trace_id at the origin
     (chrome_client) and propagate across all logs of that flow instance including retries.

7) Variable coherence within flows:
   - Maintain a per-flow context with shared IDs (session_id, client_id, report_id, request_bytes, etc.).
   - Ensure state-dependent template variables stay within their allowed state-specific domains when overridden.
     In particular:
       * During failure, chrome_client.sync_response.throttled_types is constrained to its failure-domain.
       * During failure, chrome_client.crash_upload_done.http_status is constrained to its failure-domain.

Notes:
- This is a synthetic stream designed to be diagnostically coherent rather than a perfect reproduction of
  Chrome/Google logs. It respects the provided emission mechanisms and scenario multipliers.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Embedded model data (SYSTEM)
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {
        "id": "chrome_sync_quota_throttle_crash",
        "desc": (
            "A simplified model of Chrome Sync with desktop clients posting sync cycles to a Sync Server, "
            "which consults a Quota Service and persists metadata to storage. During an incident a faulty "
            "LB configuration overloads quota-service; Sync Server returns throttle-all (sometimes including "
            "unknown types), triggering a client parsing bug and crash; crash uploads can also fail."
        ),
    },
    "states": {"n": "normal", "f": "failure"},
    "components": [
        {
            "id": "chrome_client",
            "name": "Chrome Desktop Client",
            "svc": None,
            "hosts": ["mac_01", "mac_02", "mac_03", "mac_04", "mac_05", "win_01", "win_02", "win_03", "linux_01", "linux_02"],
            "to": [
                {"dst": "sync_server", "proto": "https", "desc": "Sync protocol requests and responses over HTTPS."},
                {"dst": "crash_collector", "proto": "https", "desc": "Crash report uploads over HTTPS."},
            ],
            "logs": {
                "browser_heartbeat": {
                    "desc": "Periodic client heartbeat emitted regardless of sync activity.",
                    "lvl": "INFO",
                    "msg": "Browser heartbeat: version={version} os={os} uptime_s={uptime_s}",
                    "vars": {
                        "version": {"k": "ch", "v": ["25.0.1354.0", "25.0.1357.0", "24.0.1312.0", "23.0.1271.0"]},
                        "os": {"k": "ch", "v": ["macos", "windows", "linux"]},
                        "uptime_s": {"k": "i", "v": [60, 172800]},
                    },
                },
                "sync_job_start": {
                    "desc": "Sync scheduler starts a sync job/session.",
                    "lvl": "INFO",
                    "msg": "Sync job started: profile={profile} source={source} session={session_id} trace={trace_id}",
                    "vars": {
                        "profile": {"k": "hex", "v": 8},
                        "source": {"k": "ch", "v": ["startup", "periodic", "local_change"]},
                        "session_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "sync_request": {
                    "desc": "Client is about to POST a sync request payload.",
                    "lvl": "DEBUG",
                    "msg": "Posting sync request: server={server} types={type_count} bytes={bytes} session={session_id} trace={trace_id}",
                    "vars": {
                        "server": {"k": "ch", "v": ["https://sync.example.com/sync"]},
                        "type_count": {"k": "i", "v": [1, 25]},
                        "bytes": {"k": "i", "v": [200, 75000]},
                        "session_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "sync_response": {
                    "desc": "Client received a response and decoded the high-level status and throttling list.",
                    "lvl": "INFO",
                    "msg": "Sync response received: status={status} throttled_types={throttled_types} session={session_id} trace={trace_id}",
                    "vars": {
                        "session_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {
                            "status": {"k": "ch", "v": ["OK", "OK_NOOP"]},
                            "throttled_types": {"k": "ch", "v": ["[]", "[SESSIONS]", "[PASSWORDS]"]},
                        },
                        "f": {
                            "status": {"k": "ch", "v": ["OK", "THROTTLED_ALL", "HTTP_503", "HTTP_500"]},
                            "throttled_types": {"k": "ch", "v": ["[]", "[BOOKMARKS,PASSWORDS,SESSIONS,AUTOFILL]", "[ALL_TYPES]", "[ALL_TYPES,UNKNOWN_TYPE]"]},
                        },
                    },
                },
                "throttle_parse_warning": {
                    "desc": "Client encountered an unknown datatype identifier while interpreting throttling instructions.",
                    "lvl": "WARN",
                    "msg": "Unknown specifics field number {field_no} in throttle response; mapping to UNSPECIFIED session={session_id} trace={trace_id}",
                    "vars": {
                        "field_no": {"k": "i", "v": [0, 4096]},
                        "session_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "sync_exception": {
                    "desc": "Unhandled exception on sync thread shortly before crash.",
                    "lvl": "ERROR",
                    "msg": "Unhandled exception in SyncThread: ex={ex} what={what} session={session_id} trace={trace_id}",
                    "vars": {
                        "ex": {"k": "ch", "v": ["std::out_of_range", "std::exception"]},
                        "what": {"k": "ch", "v": ["bitset::set: __position (which is 18446744073709551615) >= this->size()", "vector::_M_range_check: __n (which is -1) >= this->size()"]},
                        "session_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "chrome_crash": {
                    "desc": "Crash marker for the browser process after abort/terminate on the sync thread.",
                    "lvl": "CRITICAL",
                    "msg": "Process crash: thread=Chrome_SyncThread signal={signal} reason={reason} report_id={report_id}",
                    "vars": {
                        "signal": {"k": "ch", "v": ["SIGABRT"]},
                        "reason": {"k": "ch", "v": ["uncaught_exception", "terminate_called"]},
                        "report_id": {"k": "hex", "v": 16},
                    },
                },
                "sync_retry_scheduled": {
                    "desc": "Client schedules a retry of the full sync cycle after a recoverable failure.",
                    "lvl": "WARN",
                    "msg": "Scheduling sync retry: session={session_id} attempt={attempt} backoff_ms={backoff_ms} trace={trace_id}",
                    "vars": {
                        "session_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 4]},
                        "backoff_ms": {"k": "i", "v": [100, 60000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "crash_upload_attempt": {
                    "desc": "Client attempts to upload a crash report to the crash collector.",
                    "lvl": "INFO",
                    "msg": "Crash report upload attempt: report_id={report_id} endpoint={endpoint} attempt={attempt}",
                    "vars": {
                        "report_id": {"k": "hex", "v": 16},
                        "endpoint": {"k": "ch", "v": ["https://crash.example.com/upload", "https://crash.corp.example.com/upload"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                    },
                },
                "crash_upload_done": {
                    "desc": "Client finishes crash upload attempt with a summarized outcome.",
                    "lvl": "INFO",
                    "msg": "Crash report upload finished: report_id={report_id} result={result} http_status={http_status}",
                    "vars": {"report_id": {"k": "hex", "v": 16}},
                    "state_vars": {
                        "n": {"result": {"k": "ch", "v": ["success"]}, "http_status": {"k": "ch", "v": ["200"]}},
                        "f": {"result": {"k": "ch", "v": ["success", "failed", "dropped"]}, "http_status": {"k": "ch", "v": ["200", "503", "429", "0"]}},
                    },
                },
                "crash_upload_retry_scheduled": {
                    "desc": "Client schedules a retry for crash upload after a failure.",
                    "lvl": "WARN",
                    "msg": "Scheduling crash upload retry: report_id={report_id} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "report_id": {"k": "hex", "v": 16},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [200, 60000]},
                    },
                },
            },
            "beh": {
                "n": {"desc": "Stable clients with occasional heartbeats.", "emit": [{"id": "browser_heartbeat", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Heartbeats continue; some clients crash in sync thread.", "emit": [{"id": "browser_heartbeat", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "sync_server",
            "name": "Chrome Sync Server",
            "svc": "chrome-sync",
            "hosts": ["sync_01", "sync_02", "sync_03"],
            "to": [
                {"dst": "quota_service", "proto": "grpc", "desc": "RPC used to evaluate quota and throttling decisions per client and datatype."},
                {"dst": "sync_storage", "proto": "grpc", "desc": "Reads/writes sync metadata and commits."},
                {"dst": "chrome_client", "proto": "https", "desc": "HTTPS responses back to clients."},
            ],
            "logs": {
                "health_ok": {
                    "desc": "Periodic service health line with basic resource usage.",
                    "lvl": "INFO",
                    "msg": "Health OK: cpu={cpu} mem_mb={mem_mb} open_fds={open_fds}",
                    "vars": {"cpu": {"k": "f", "v": [0.05, 0.95]}, "mem_mb": {"k": "i", "v": [256, 8192]}, "open_fds": {"k": "i", "v": [200, 20000]}},
                },
                "quota_rpc_slow": {
                    "desc": "Warning when quota RPC latency is high.",
                    "lvl": "WARN",
                    "msg": "Quota RPC latency high: p95_ms={p95_ms} in_flight={in_flight}",
                    "vars": {"p95_ms": {"k": "i", "v": [50, 20000]}, "in_flight": {"k": "i", "v": [1, 5000]}},
                },
                "sync_req_received": {
                    "desc": "Access log for incoming sync request.",
                    "lvl": "INFO",
                    "msg": "Incoming /sync: client={client_id} session={session_id} bytes={bytes} trace={trace_id}",
                    "vars": {"client_id": {"k": "hex", "v": 12}, "session_id": {"k": "uuid", "v": None}, "bytes": {"k": "i", "v": [200, 75000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "quota_check_ok": {
                    "desc": "Server-side record that quota call succeeded and allowed the request.",
                    "lvl": "INFO",
                    "msg": "Quota OK: client={client_id} allowed_types={allowed_types} trace={trace_id}",
                    "vars": {"client_id": {"k": "hex", "v": 12}, "allowed_types": {"k": "ch", "v": ["ALL", "MOST", "LIMITED"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "quota_check_timeout": {
                    "desc": "Quota call failed or timed out; server will decide how to respond.",
                    "lvl": "ERROR",
                    "msg": "Quota check failed: err={err} timeout_ms={timeout_ms} client={client_id} trace={trace_id}",
                    "vars": {"err": {"k": "ch", "v": ["DEADLINE_EXCEEDED", "UNAVAILABLE", "RESOURCE_EXHAUSTED"]}, "timeout_ms": {"k": "i", "v": [200, 20000]}, "client_id": {"k": "hex", "v": 12}, "trace_id": {"k": "hex", "v": 32}},
                },
                "sync_resp_200": {
                    "desc": "Successful sync response.",
                    "lvl": "INFO",
                    "msg": "Responded /sync: http_status=200 commit_count={commit_count} throttled_types={throttled_types} trace={trace_id}",
                    "vars": {"commit_count": {"k": "i", "v": [0, 50]}, "throttled_types": {"k": "ch", "v": ["[]", "[SESSIONS]", "[PASSWORDS]"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "sync_resp_throttle_all": {
                    "desc": "Throttle-all response when server is in fail-closed mode.",
                    "lvl": "WARN",
                    "msg": "Responded /sync: http_status=429 throttle_mode=ALL throttled_types={throttled_types} trace={trace_id}",
                    "vars": {"throttled_types": {"k": "ch", "v": ["[ALL_TYPES]", "[ALL_TYPES,UNKNOWN_TYPE]", "[BOOKMARKS,PASSWORDS,SESSIONS,AUTOFILL]"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "sync_resp_503": {
                    "desc": "Server returns a 503 due to dependency outage or overload.",
                    "lvl": "ERROR",
                    "msg": "Responded /sync: http_status=503 reason={reason} trace={trace_id}",
                    "vars": {"reason": {"k": "ch", "v": ["quota_unavailable", "upstream_timeout", "over_capacity"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "fail_closed_enabled": {
                    "desc": "Operational marker indicating fail-closed throttle-all mode was enabled.",
                    "lvl": "INFO",
                    "msg": "Fail-closed mode enabled: action=throttle_all reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["quota_service_degraded", "safety_fallback"]}},
                },
            },
            "beh": {
                "n": {"desc": "Healthy sync servers.", "emit": [{"id": "health_ok", "per_min": 2.0, "scope": "per_host"}]},
                "f": {
                    "desc": "Slow quota dependencies and intermittent throttle-all / 503.",
                    "emit": [{"id": "health_ok", "per_min": 2.0, "scope": "per_host"}, {"id": "quota_rpc_slow", "per_min": 1.0, "scope": "per_host"}],
                },
            },
        },
        {
            "id": "quota_service",
            "name": "Quota Service",
            "svc": "quota-service",
            "hosts": ["quota_01", "quota_02"],
            "to": [{"dst": "sync_server", "proto": "grpc", "desc": "Returns quota evaluation results to Sync Servers over gRPC."}],
            "logs": {
                "health_ok": {
                    "desc": "Periodic health/telemetry line.",
                    "lvl": "INFO",
                    "msg": "Health OK: qps={qps} p95_ms={p95_ms} queue_depth={queue_depth}",
                    "vars": {"qps": {"k": "i", "v": [0, 200000]}, "p95_ms": {"k": "i", "v": [1, 20000]}, "queue_depth": {"k": "i", "v": [0, 500000]}},
                },
                "overload_warn": {
                    "desc": "Warning that the service is overloaded and shedding/queuing.",
                    "lvl": "WARN",
                    "msg": "Overload: state={state} queue_depth={queue_depth} drop_rate={drop_rate}",
                    "vars": {"state": {"k": "ch", "v": ["OK", "DEGRADED", "OVERLOADED"]}, "queue_depth": {"k": "i", "v": [1000, 500000]}, "drop_rate": {"k": "f", "v": [0.0, 1.0]}},
                },
                "rpc_deadline_exceeded": {
                    "desc": "Per-request log when a quota RPC misses its deadline.",
                    "lvl": "ERROR",
                    "msg": "RPC deadline exceeded: method=CheckQuota client={client_id} elapsed_ms={elapsed_ms} trace={trace_id}",
                    "vars": {"client_id": {"k": "hex", "v": 12}, "elapsed_ms": {"k": "i", "v": [200, 20000]}, "trace_id": {"k": "hex", "v": 32}},
                },
            },
            "beh": {
                "n": {"desc": "Fast quota evaluations.", "emit": [{"id": "health_ok", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Sustained overload.", "emit": [{"id": "health_ok", "per_min": 1.0, "scope": "per_host"}, {"id": "overload_warn", "per_min": 3.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "traffic_lb",
            "name": "Traffic Load Balancer / Config Plane",
            "svc": "edge-lb",
            "hosts": ["lb_01"],
            "to": [{"dst": "quota_service", "proto": "tcp", "desc": "Routes traffic toward quota-service backends."}],
            "logs": {
                "config_change_applied": {
                    "desc": "A routing/load-balancing configuration change was applied.",
                    "lvl": "INFO",
                    "msg": "LB config applied: change_id={change_id} target=quota-service rule={rule}",
                    "vars": {"change_id": {"k": "hex", "v": 10}, "rule": {"k": "ch", "v": ["backend_weight_update", "health_check_tweak", "hash_ring_change"]}},
                },
                "config_change_no_comment": {
                    "desc": "Change applied without a human-readable comment.",
                    "lvl": "WARN",
                    "msg": "LB config change {change_id} has no comment",
                    "vars": {"change_id": {"k": "hex", "v": 10}},
                },
                "pool_imbalance": {
                    "desc": "Periodic warning when backend distribution becomes skewed.",
                    "lvl": "WARN",
                    "msg": "Backend pool imbalance: pool=quota-service skew={skew} unhealthy_backends={unhealthy}",
                    "vars": {"skew": {"k": "f", "v": [0.0, 1.0]}, "unhealthy": {"k": "i", "v": [0, 50]}},
                },
            },
            "beh": {
                "n": {"desc": "LB stable.", "emit": [{"id": "pool_imbalance", "per_min": 0.1, "scope": "global"}]},
                "f": {"desc": "Frequent pool imbalance warnings.", "emit": [{"id": "pool_imbalance", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "sync_storage",
            "name": "Sync Metadata Storage",
            "svc": "sync-storage",
            "hosts": ["store_01", "store_02"],
            "to": [{"dst": "sync_server", "proto": "grpc", "desc": "Returns read/write results to the Sync Server."}, {"dst": "crash_collector", "proto": "grpc", "desc": "Acknowledges crash-report persistence operations."}],
            "logs": {
                "health_ok": {
                    "desc": "Periodic storage health metric line.",
                    "lvl": "INFO",
                    "msg": "Storage health: p95_ms={p95_ms} open_conns={open_conns}",
                    "vars": {"p95_ms": {"k": "i", "v": [1, 5000]}, "open_conns": {"k": "i", "v": [1, 20000]}},
                },
                "write_commit": {
                    "desc": "Commit metadata write for a sync cycle.",
                    "lvl": "INFO",
                    "msg": "Write commit: commit_id={commit_id} bytes={bytes} trace={trace_id}",
                    "vars": {"commit_id": {"k": "hex", "v": 12}, "bytes": {"k": "i", "v": [50, 200000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "write_slow": {
                    "desc": "Warning when storage writes are slow.",
                    "lvl": "WARN",
                    "msg": "Slow write: op=write_commit p95_ms={p95_ms} trace={trace_id}",
                    "vars": {"p95_ms": {"k": "i", "v": [50, 20000]}, "trace_id": {"k": "hex", "v": 32}},
                },
            },
            "beh": {
                "n": {"desc": "Healthy storage.", "emit": [{"id": "health_ok", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Mostly healthy; occasional slow writes.", "emit": [{"id": "health_ok", "per_min": 1.0, "scope": "per_host"}, {"id": "write_slow", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "crash_collector",
            "name": "Crash Collector Service",
            "svc": "crash-collector",
            "hosts": ["crash_01", "crash_02"],
            "to": [{"dst": "sync_storage", "proto": "grpc", "desc": "Writes crash payload metadata to storage."}, {"dst": "chrome_client", "proto": "https", "desc": "HTTPS responses back to the client."}],
            "logs": {
                "health_ok": {
                    "desc": "Periodic crash collector health line.",
                    "lvl": "INFO",
                    "msg": "Crash collector health: in_flight={in_flight} p95_ms={p95_ms}",
                    "vars": {"in_flight": {"k": "i", "v": [0, 20000]}, "p95_ms": {"k": "i", "v": [1, 20000]}},
                },
                "upload_received": {
                    "desc": "Crash upload request received.",
                    "lvl": "INFO",
                    "msg": "Crash upload received: report_id={report_id} client={client_id} bytes={bytes}",
                    "vars": {"report_id": {"k": "hex", "v": 16}, "client_id": {"k": "hex", "v": 12}, "bytes": {"k": "i", "v": [2000, 5000000]}},
                },
                "upload_failed": {
                    "desc": "Crash upload rejected or failed at the collector.",
                    "lvl": "ERROR",
                    "msg": "Crash upload failed: report_id={report_id} http_status={http_status} err={err}",
                    "vars": {"report_id": {"k": "hex", "v": 16}, "http_status": {"k": "ch", "v": ["429", "503", "500"]}, "err": {"k": "ch", "v": ["over_capacity", "upstream_timeout", "rate_limited"]}},
                },
            },
            "beh": {
                "n": {"desc": "Collector healthy.", "emit": [{"id": "health_ok", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"desc": "Collector up but rejects some uploads.", "emit": [{"id": "health_ok", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
    ],
    "tracing": {"on": True, "origins": ["chrome_client"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "desc": "Normal operations flows.",
            "req": [
                {
                    "id": "sync_cycle_success",
                    "desc": "Successful sync cycle with quota check OK and commit persisted.",
                    "rpm": 160.0,
                    "path": ["chrome_client", "sync_server", "quota_service", "sync_server", "sync_storage", "sync_server", "chrome_client"],
                    "emit": [
                        "chrome_client.sync_job_start",
                        "chrome_client.sync_request",
                        "sync_server.sync_req_received",
                        "sync_server.quota_check_ok",
                        "sync_storage.write_commit",
                        "sync_server.sync_resp_200",
                        "chrome_client.sync_response",
                    ],
                    "latency_ms": [[0, 1], [20, 70], [15, 60], [10, 45], [20, 90], [5, 25], [10, 40]],
                    "trace": True,
                },
                {
                    "id": "crash_report_upload_success",
                    "desc": "Occasional successful crash upload (unrelated background noise).",
                    "rpm": 2.0,
                    "path": ["chrome_client", "crash_collector", "chrome_client"],
                    "emit": ["chrome_client.crash_upload_attempt", "crash_collector.upload_received", "chrome_client.crash_upload_done"],
                    "latency_ms": [[0, 1], [80, 350], [10, 40]],
                    "trace": True,
                },
            ],
        },
        "f": {
            "desc": "Failure operations flows.",
            "req": [
                {
                    "id": "sync_cycle_ok_degraded",
                    "desc": "Sync still succeeds but with higher latency while dependencies are strained.",
                    "rpm": 150.0,
                    "path": ["chrome_client", "sync_server", "quota_service", "sync_server", "sync_storage", "sync_server", "chrome_client"],
                    "emit": [
                        "chrome_client.sync_job_start",
                        "chrome_client.sync_request",
                        "sync_server.sync_req_received",
                        "sync_server.quota_check_ok",
                        "sync_storage.write_commit",
                        "sync_server.sync_resp_200",
                        "chrome_client.sync_response",
                    ],
                    "latency_ms": [[0, 1], [30, 120], [20, 120], [25, 250], [35, 400], [10, 80], [15, 100]],
                    "trace": True,
                },
                {
                    "id": "sync_cycle_throttle_known",
                    "desc": "Server responds with throttle-all for known types; client applies throttle and does not crash.",
                    "rpm": 80.0,
                    "path": ["chrome_client", "sync_server", "quota_service", "sync_server", "chrome_client"],
                    "emit": [
                        "chrome_client.sync_job_start",
                        "chrome_client.sync_request",
                        "sync_server.sync_req_received",
                        "sync_server.sync_resp_throttle_all",
                        "chrome_client.sync_response",
                    ],
                    "latency_ms": [[0, 1], [25, 100], [20, 120], [40, 300], [10, 60]],
                    "trace": True,
                },
                {
                    "id": "sync_cycle_throttle_unknown_crash",
                    "desc": "Throttle-all includes an unrecognized datatype identifier; client hits out_of_range and crashes.",
                    "rpm": 50.0,
                    "path": ["chrome_client", "sync_server", "quota_service", "sync_server", "chrome_client"],
                    "emit": [
                        "chrome_client.sync_job_start",
                        "chrome_client.sync_request",
                        "sync_server.sync_req_received",
                        "sync_server.sync_resp_throttle_all",
                        "chrome_client.sync_response",
                        "chrome_client.throttle_parse_warning",
                        "chrome_client.sync_exception",
                        "chrome_client.chrome_crash",
                    ],
                    "latency_ms": [[0, 1], [25, 100], [20, 120], [40, 300], [5, 30], [2, 10], [1, 5], [1, 5]],
                    "trace": True,
                },
                {
                    "id": "sync_cycle_quota_timeout_retry",
                    "desc": "Quota RPC times out; server returns 503; client retries the entire sync cycle with backoff.",
                    "rpm": 40.0,
                    "path": ["chrome_client", "sync_server", "quota_service", "sync_server", "chrome_client"],
                    "emit": [
                        "chrome_client.sync_job_start",
                        "chrome_client.sync_request",
                        "sync_server.sync_req_received",
                        "quota_service.rpc_deadline_exceeded",
                        "sync_server.quota_check_timeout",
                        "sync_server.sync_resp_503",
                        "chrome_client.sync_response",
                    ],
                    "latency_ms": [[0, 1], [25, 120], [20, 120], [300, 4000], [1, 10], [5, 40], [10, 60]],
                    "retry": {
                        "max_attempts": 4,
                        "expected_attempts": 2.5,
                        "emit_per_retry": ["chrome_client.sync_retry_scheduled"],
                        "backoff_ms": [[200, 800], [400, 1500], [800, 3000]],
                    },
                    "trace": True,
                },
                {
                    "id": "crash_report_upload_fail_retry",
                    "desc": "Crash upload attempts fail (429/503) and are retried with backoff; some are ultimately dropped.",
                    "rpm": 30.0,
                    "path": ["chrome_client", "crash_collector", "chrome_client"],
                    "emit": ["chrome_client.crash_upload_attempt", "crash_collector.upload_failed", "chrome_client.crash_upload_done"],
                    "latency_ms": [[0, 1], [100, 1200], [10, 60]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 2.0,
                        "emit_per_retry": ["chrome_client.crash_upload_retry_scheduled"],
                        "backoff_ms": [[500, 2000], [1500, 8000]],
                    },
                    "trace": True,
                },
            ],
        },
    },
    "assumptions": [
        "Fleet scale approximated by small host lists; RPM represents aggregated traffic.",
    ],
}


# ------------------------------
# Embedded timeline (SCENARIO)
# ------------------------------
SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "chrome_sync_throttle_unknown_type_crash",
        "title": "Chrome SyncThread crashes during quota-service incident (unknown throttled datatype)",
        "states": {"n": "normal", "f": "failure"},
        "time": {"total_minutes": 30, "phases": {"n": {"start_min": 0, "end_min": 15}, "f": {"start_min": 15, "end_min": 30}}},
        "phases": {
            "n": {
                "flows": ["sync_cycle_success", "crash_report_upload_success"],
                "manifestation": ["chrome_client.sync_job_start", "sync_server.sync_req_received", "sync_server.sync_resp_200", "chrome_client.sync_response"],
            },
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 15,
                        "component": "traffic_lb",
                        "flows": ["sync_cycle_ok_degraded", "sync_cycle_quota_timeout_retry"],
                        "rate_multipliers": {
                            "sync_cycle_throttle_known": 0.0,
                            "sync_cycle_throttle_unknown_crash": 0.0,
                            "crash_report_upload_fail_retry": 0.0,
                            "sync_cycle_quota_timeout_retry": 0.2,
                            "quota_service.overload_warn": 1.5,
                            "sync_server.quota_rpc_slow": 1.2,
                        },
                        "one_shots": [
                            {"ref": "traffic_lb.config_change_applied", "count": 1, "hosts": ["lb_01"]},
                            {"ref": "traffic_lb.config_change_no_comment", "count": 1, "hosts": ["lb_01"]},
                        ],
                        "manifestation": ["traffic_lb.config_change_applied", "traffic_lb.pool_imbalance", "quota_service.overload_warn"],
                    },
                    {
                        "order": 2,
                        "at_min": 18,
                        "component": "sync_server",
                        "flows": ["sync_cycle_throttle_known", "sync_cycle_quota_timeout_retry"],
                        "rate_multipliers": {
                            "sync_cycle_ok_degraded": 0.8,
                            "sync_cycle_throttle_known": 0.8,
                            "sync_cycle_quota_timeout_retry": 1.0,
                            "crash_report_upload_fail_retry": 0.2,
                            "sync_cycle_throttle_unknown_crash": 0.0,
                            "quota_service.overload_warn": 2.0,
                            "sync_server.quota_rpc_slow": 1.5,
                        },
                        "latency_multipliers": {
                            "sync_cycle_quota_timeout_retry": {"p50": 1.3, "p95": 1.6},
                            "sync_cycle_throttle_known": {"p50": 1.2, "p95": 1.5},
                        },
                        "one_shots": [{"ref": "sync_server.fail_closed_enabled", "count": 1, "hosts": ["sync_01"]}],
                        "manifestation": ["sync_server.sync_resp_throttle_all", "sync_server.sync_resp_503", "chrome_client.sync_retry_scheduled"],
                    },
                    {
                        "order": 3,
                        "at_min": 20,
                        "component": "chrome_client",
                        "flows": ["sync_cycle_throttle_unknown_crash", "sync_cycle_throttle_known"],
                        "rate_multipliers": {
                            "sync_cycle_ok_degraded": 0.6,
                            "sync_cycle_throttle_known": 1.0,
                            "sync_cycle_throttle_unknown_crash": 1.2,
                            "sync_cycle_quota_timeout_retry": 1.2,
                            "crash_report_upload_fail_retry": 1.0,
                            "quota_service.overload_warn": 2.5,
                            "sync_server.quota_rpc_slow": 2.0,
                        },
                        "latency_multipliers": {
                            "sync_cycle_throttle_unknown_crash": {"p50": 1.1, "p95": 1.2},
                            "sync_cycle_ok_degraded": {"p50": 1.2, "p95": 1.6},
                        },
                        "manifestation": ["chrome_client.throttle_parse_warning", "chrome_client.sync_exception", "chrome_client.chrome_crash"],
                    },
                    {
                        "order": 4,
                        "at_min": 23,
                        "component": "crash_collector",
                        "flows": ["crash_report_upload_fail_retry", "sync_cycle_quota_timeout_retry", "sync_cycle_throttle_unknown_crash"],
                        "rate_multipliers": {
                            "crash_report_upload_fail_retry": 1.3,
                            "sync_cycle_throttle_unknown_crash": 1.5,
                            "sync_cycle_quota_timeout_retry": 1.3,
                            "sync_cycle_ok_degraded": 0.5,
                            "quota_service.overload_warn": 3.0,
                            "sync_server.quota_rpc_slow": 2.0,
                        },
                        "latency_multipliers": {"crash_report_upload_fail_retry": {"p50": 1.4, "p95": 2.0}},
                        "manifestation": ["crash_collector.upload_failed", "chrome_client.crash_upload_done", "sync_server.sync_resp_throttle_all"],
                    },
                ],
                "steady": [
                    {"component": "quota_service", "manifestation": ["quota_service.overload_warn", "quota_service.health_ok"]},
                    {"component": "sync_server", "manifestation": ["sync_server.quota_rpc_slow", "sync_server.sync_resp_throttle_all", "sync_server.sync_resp_503"]},
                    {"component": "chrome_client", "manifestation": ["chrome_client.throttle_parse_warning", "chrome_client.chrome_crash", "chrome_client.sync_retry_scheduled"]},
                    {"component": "crash_collector", "manifestation": ["crash_collector.upload_failed", "chrome_client.crash_upload_done"]},
                ],
                "flows": [
                    "sync_cycle_ok_degraded",
                    "sync_cycle_throttle_known",
                    "sync_cycle_throttle_unknown_crash",
                    "sync_cycle_quota_timeout_retry",
                    "crash_report_upload_fail_retry",
                ],
                "manifestation": [
                    "traffic_lb.pool_imbalance",
                    "quota_service.overload_warn",
                    "sync_server.quota_rpc_slow",
                    "sync_server.sync_resp_throttle_all",
                    "sync_server.sync_resp_503",
                    "chrome_client.throttle_parse_warning",
                    "chrome_client.sync_exception",
                    "chrome_client.chrome_crash",
                    "chrome_client.sync_retry_scheduled",
                    "crash_collector.upload_failed",
                    "chrome_client.crash_upload_done",
                ],
            },
        },
    }
}


# -----------------------------
# RNG and helper functionality
# -----------------------------
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)
RNG = np.random.default_rng(SEED)

BASE_TIME = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)


def iso8601_ms(dt: datetime) -> str:
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:23] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def rand_hex(n: int) -> str:
    b = RNG.integers(0, 256, size=(n + 1) // 2, dtype=np.uint8).tobytes()
    return b.hex()[:n]


def rand_uuid() -> str:
    h = rand_hex(32)
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def sample_choice(options: List[Any], weights: Optional[List[float]] = None) -> Any:
    if weights is None:
        return options[int(RNG.integers(0, len(options)))]
    w = np.array(weights, dtype=float)
    w = w / w.sum()
    return options[int(RNG.choice(len(options), p=w))]


def sample_int(lo: int, hi: int) -> int:
    return int(RNG.integers(lo, hi + 1))


def sample_float(lo: float, hi: float) -> float:
    return float(RNG.random() * (hi - lo) + lo)


def lognormal_from_p50_p95(p50: float, p95: float) -> Tuple[float, float]:
    p50 = max(1e-6, float(p50))
    p95 = max(p50, float(p95))
    mu = math.log(p50)
    sigma = 0.0 if p95 == p50 else (math.log(p95) - mu) / 1.645
    sigma = max(0.0, sigma)
    return mu, sigma


def sample_lognormal_ms(p50: float, p95: float, softcap_mult: float = 3.0) -> int:
    mu, sigma = lognormal_from_p50_p95(p50, p95)
    if sigma == 0.0:
        x = math.exp(mu)
    else:
        x = float(RNG.lognormal(mean=mu, sigma=sigma))
    softcap = softcap_mult * max(p95, 1.0)
    x = min(x, softcap)
    x += float(RNG.normal(0.0, 2.0))
    return int(max(0.0, round(x)))


# -----------------------------
# Model indexing / lookups
# -----------------------------
COMPONENTS: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Dict[str, Any]]
    state_vars: Optional[Dict[str, Dict[str, Dict[str, Any]]]]


TEMPLATES: Dict[str, LogTemplate] = {}
for cid, comp in COMPONENTS.items():
    for lid, ldef in comp["logs"].items():
        TEMPLATES[f"{cid}.{lid}"] = LogTemplate(
            component_id=cid,
            log_id=lid,
            level=ldef["lvl"],
            msg=ldef["msg"],
            vars=ldef.get("vars", {}),
            state_vars=ldef.get("state_vars"),
        )

FLOWS_N: Dict[str, Dict[str, Any]] = {f["id"]: f for f in SYSTEM["flows"]["n"]["req"]}
FLOWS_F: Dict[str, Dict[str, Any]] = {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}

TRACING_ON = bool(SYSTEM["tracing"]["on"])


def component_service_host(component_id: str, host_override: Optional[str] = None) -> Tuple[str, str]:
    comp = COMPONENTS[component_id]
    svc = comp["svc"] or ""
    hosts = comp.get("hosts", []) or []
    if host_override is not None:
        return svc, host_override
    if not hosts:
        return svc, ""
    return svc, str(sample_choice(hosts))


def severity_index_for_minute(minute: int, f_start: int) -> int:
    if minute < f_start:
        return 0
    events = SCENARIO["scenario"]["phases"]["f"]["events"]
    idx = 1
    for e in events:
        if minute >= e["at_min"]:
            idx = e["order"]
        else:
            break
    return idx


# -----------------------------
# Scenario multipliers (failure)
# -----------------------------
def build_failure_multiplier_tables() -> Tuple[Dict[int, Dict[str, float]], Dict[int, Dict[str, float]], Dict[int, Dict[str, Dict[str, float]]]]:
    scen = SCENARIO["scenario"]
    f_start = scen["time"]["phases"]["f"]["start_min"]
    f_end = scen["time"]["phases"]["f"]["end_min"]
    events = sorted(scen["phases"]["f"]["events"], key=lambda x: x["order"])

    active_flow_rate: Dict[str, float] = {}
    active_bg_rate: Dict[str, float] = {}
    active_latency: Dict[str, Dict[str, float]] = {}

    for fid in FLOWS_F.keys():
        active_flow_rate[fid] = 1.0
        active_latency[fid] = {"p50": 1.0, "p95": 1.0}

    for cid, comp in COMPONENTS.items():
        for emit in comp["beh"]["f"].get("emit", []):
            key = f"{cid}.{emit['id']}"
            active_bg_rate[key] = 1.0

    flow_rate_by_min: Dict[int, Dict[str, float]] = {}
    bg_rate_by_min: Dict[int, Dict[str, float]] = {}
    latency_by_min: Dict[int, Dict[str, Dict[str, float]]] = {}

    ev_i = 0
    for m in range(f_start, f_end):
        while ev_i < len(events) and events[ev_i]["at_min"] == m:
            ev = events[ev_i]
            for k, mult in (ev.get("rate_multipliers") or {}).items():
                if "." in k:
                    active_bg_rate[k] = float(mult)
                else:
                    active_flow_rate[k] = float(mult)
            for fid, lm in (ev.get("latency_multipliers") or {}).items():
                active_latency[fid] = {"p50": float(lm.get("p50", 1.0)), "p95": float(lm.get("p95", 1.0))}
            ev_i += 1

        flow_rate_by_min[m] = dict(active_flow_rate)
        bg_rate_by_min[m] = dict(active_bg_rate)
        latency_by_min[m] = {k: dict(v) for k, v in active_latency.items()}

    return flow_rate_by_min, bg_rate_by_min, latency_by_min


FLOW_RATE_MULT_BY_MIN, BG_RATE_MULT_BY_MIN, LAT_MULT_BY_MIN = build_failure_multiplier_tables()


# -----------------------------
# Template variable generation
# -----------------------------
def sample_var(kind: str, domain: Any) -> Any:
    if kind == "i":
        return sample_int(int(domain[0]), int(domain[1]))
    if kind == "f":
        val = sample_float(float(domain[0]), float(domain[1]))
        return round(val, 3)
    if kind == "ch":
        return sample_choice(list(domain))
    if kind == "uuid":
        return rand_uuid()
    if kind == "hex":
        return rand_hex(int(domain))
    if kind == "ip":
        return "10.0.0." + str(sample_int(1, 254))
    if kind == "str":
        return str(domain)
    raise ValueError(f"Unknown var kind: {kind}")


def render_message(template: LogTemplate, state: str, overrides: Dict[str, Any]) -> str:
    values: Dict[str, Any] = {}
    for var, spec in template.vars.items():
        values[var] = sample_var(spec["k"], spec["v"])
    if template.state_vars is not None:
        for var, spec in template.state_vars[state].items():
            values[var] = sample_var(spec["k"], spec["v"])
    values.update(overrides)
    msg = template.msg.format(**values)
    if len(msg) > 1000:
        msg = msg[:1000]
    return msg


# -----------------------------
# Retry attempt sampling
# -----------------------------
def expected_trunc_geo(p: float, A: int) -> float:
    q = 1.0 - p
    expv = 0.0
    for k in range(1, A):
        expv += k * (q ** (k - 1)) * p
    expv += A * (q ** (A - 1))
    return expv


def solve_trunc_geo_p(target_mean: float, A: int) -> float:
    target_mean = float(clamp(target_mean, 1.0, float(A)))
    lo, hi = 1e-6, 1.0
    for _ in range(60):
        mid = (lo + hi) / 2.0
        em = expected_trunc_geo(mid, A)
        if em > target_mean:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def sample_attempt_count(max_attempts: int, expected_attempts: float) -> int:
    if max_attempts <= 1:
        return 1
    p = solve_trunc_geo_p(expected_attempts, max_attempts)
    q = 1.0 - p
    u = float(RNG.random())
    cdf = 0.0
    for k in range(1, max_attempts):
        pk = (q ** (k - 1)) * p
        cdf += pk
        if u <= cdf:
            return k
    return max_attempts


# -----------------------------
# Flow coherence logic
# -----------------------------
def choose_hosts_for_flow(flow: Dict[str, Any]) -> Dict[str, str]:
    comps = set(ref.split(".", 1)[0] for ref in flow.get("emit", []))
    host_map: Dict[str, str] = {}
    for cid in comps:
        comp_hosts = COMPONENTS[cid].get("hosts", []) or []
        host_map[cid] = "" if not comp_hosts else str(sample_choice(comp_hosts))
    return host_map


def flow_latency_multiplier(minute: int, flow_id: str, state: str) -> Dict[str, float]:
    if state != "f":
        return {"p50": 1.0, "p95": 1.0}
    return LAT_MULT_BY_MIN.get(minute, {}).get(flow_id, {"p50": 1.0, "p95": 1.0})


def coherent_overrides_for_log(
    state: str,
    flow_id: str,
    ref: str,
    ctx: Dict[str, Any],
    attempt_no: int,
    is_retry_marker: bool,
    minute_started: int,
    failure_severity: int,
) -> Dict[str, Any]:
    o: Dict[str, Any] = {}

    if "trace_id" in ctx:
        o["trace_id"] = ctx["trace_id"]
    if "session_id" in ctx:
        o["session_id"] = ctx["session_id"]
    if "client_id" in ctx:
        o["client_id"] = ctx["client_id"]
    if "report_id" in ctx:
        o["report_id"] = ctx["report_id"]

    if ref in ("chrome_client.sync_request", "sync_server.sync_req_received"):
        o["bytes"] = ctx["sync_bytes"]

    if ref == "sync_server.sync_resp_200":
        o["throttled_types"] = ctx.get("server_throttled_types", "[]")
    if ref == "sync_server.sync_resp_throttle_all":
        o["throttled_types"] = ctx.get("server_throttled_types", "[ALL_TYPES]")
    if ref == "chrome_client.sync_response":
        o["status"] = ctx.get("client_status", "OK" if state == "n" else "OK")
        o["throttled_types"] = ctx.get("server_throttled_types", "[]")

    if ref == "sync_server.quota_check_timeout":
        o["err"] = ctx.get("quota_err", "DEADLINE_EXCEEDED")
        o["timeout_ms"] = ctx.get("quota_timeout_ms", sample_int(500, 5000))
    if ref == "sync_server.sync_resp_503":
        o["reason"] = ctx.get("resp_503_reason", "upstream_timeout")

    if ref == "quota_service.rpc_deadline_exceeded":
        base = 800 + 600 * (failure_severity - 1)
        elapsed = int(clamp(base + abs(float(RNG.normal(0, 500))) + sample_int(0, 1500), 200, 20000))
        o["elapsed_ms"] = elapsed

    if ref == "chrome_client.throttle_parse_warning":
        o["field_no"] = int(clamp(2000 + sample_int(-500, 1500), 1, 4096))
    if ref == "chrome_client.sync_exception":
        o["ex"] = "std::out_of_range"
        o["what"] = sample_choice(
            [
                "vector::_M_range_check: __n (which is -1) >= this->size()",
                "bitset::set: __position (which is 18446744073709551615) >= this->size()",
            ],
            weights=[0.7, 0.3],
        )
    if ref == "chrome_client.chrome_crash":
        o["signal"] = "SIGABRT"
        o["reason"] = sample_choice(["uncaught_exception", "terminate_called"], weights=[0.7, 0.3])
        if "crash_report_id" in ctx:
            o["report_id"] = ctx["crash_report_id"]

    if ref == "chrome_client.crash_upload_attempt":
        o["attempt"] = attempt_no
    if ref == "crash_collector.upload_received":
        o["client_id"] = ctx.get("client_id", rand_hex(12))
        o["bytes"] = ctx.get("crash_bytes", sample_int(5000, 500000))
    if ref == "crash_collector.upload_failed":
        # Constrain to statuses compatible with chrome_client.crash_upload_done.state_vars.f
        hs = sample_choice(["429", "503"], weights=[0.55, 0.45] if failure_severity >= 4 else [0.45, 0.55])
        o["http_status"] = hs
        if hs == "429":
            o["err"] = sample_choice(["rate_limited", "over_capacity"], weights=[0.6, 0.4])
        else:
            o["err"] = sample_choice(["over_capacity", "upstream_timeout"], weights=[0.6, 0.4])
        ctx["last_upload_http_status"] = hs
        ctx["last_upload_err"] = o["err"]
    if ref == "chrome_client.crash_upload_done":
        if flow_id == "crash_report_upload_success":
            o["result"] = "success"
            o["http_status"] = "200"
        else:
            hs = str(ctx.get("last_upload_http_status", "503"))
            if hs not in {"200", "429", "503", "0"}:
                hs = "0"
            if attempt_no >= ctx.get("attempts_total", attempt_no) and ctx.get("attempts_total", attempt_no) > 1:
                o["result"] = sample_choice(["failed", "dropped"], weights=[0.75, 0.25])
            else:
                o["result"] = "failed"
            o["http_status"] = hs

    if is_retry_marker and ref == "chrome_client.sync_retry_scheduled":
        o["attempt"] = attempt_no
        o["backoff_ms"] = ctx.get("next_backoff_ms", sample_int(200, 2000))
    if is_retry_marker and ref == "chrome_client.crash_upload_retry_scheduled":
        o["attempt"] = attempt_no
        o["backoff_ms"] = ctx.get("next_backoff_ms", sample_int(500, 5000))

    if ref == "quota_service.overload_warn":
        if failure_severity <= 1:
            o["state"] = sample_choice(["DEGRADED", "OK"], weights=[0.8, 0.2])
            o["queue_depth"] = sample_int(20000, 150000)
            o["drop_rate"] = round(sample_float(0.0, 0.15), 3)
        elif failure_severity == 2:
            o["state"] = sample_choice(["DEGRADED", "OVERLOADED"], weights=[0.6, 0.4])
            o["queue_depth"] = sample_int(80000, 260000)
            o["drop_rate"] = round(sample_float(0.05, 0.35), 3)
        elif failure_severity == 3:
            o["state"] = sample_choice(["OVERLOADED", "DEGRADED"], weights=[0.75, 0.25])
            o["queue_depth"] = sample_int(160000, 380000)
            o["drop_rate"] = round(sample_float(0.15, 0.60), 3)
        else:
            o["state"] = "OVERLOADED"
            o["queue_depth"] = sample_int(250000, 500000)
            o["drop_rate"] = round(sample_float(0.35, 0.90), 3)

    if ref == "sync_server.quota_rpc_slow":
        base_p95 = 250 + 250 * max(0, failure_severity - 1)
        o["p95_ms"] = int(clamp(base_p95 + abs(float(RNG.normal(0, 300))) + sample_int(0, 800), 50, 20000))
        o["in_flight"] = int(clamp(100 + 150 * failure_severity + abs(float(RNG.normal(0, 250))) * 3, 1, 5000))

    if ref == "traffic_lb.pool_imbalance":
        if state == "f":
            o["skew"] = round(clamp(sample_float(0.4, 1.0) + float(RNG.normal(0, 0.08)), 0.0, 1.0), 3)
            o["unhealthy"] = int(clamp(sample_int(0, 10) + failure_severity + sample_int(0, 5), 0, 50))

    return o


# -----------------------------
# Log emission implementation
# -----------------------------
def add_log(rows: List[Dict[str, Any]], ts_ms: int, template_ref: str, state: str, trace_id: str, host_override: Optional[str], overrides: Dict[str, Any]) -> None:
    tmpl = TEMPLATES[template_ref]
    service, host = component_service_host(tmpl.component_id, host_override=host_override)
    msg = render_message(tmpl, state=state, overrides=overrides)
    dt = BASE_TIME + timedelta(milliseconds=int(ts_ms))
    rows.append(
        {
            "timestamp": dt,
            "level": tmpl.level,
            "message": msg,
            "trace_id": trace_id or "",
            "service": service,
            "host": host,
        }
    )


def emit_background_for_minute(rows: List[Dict[str, Any]], minute: int, state: str, f_start: int) -> None:
    failure_sev = severity_index_for_minute(minute, f_start)
    for cid, comp in COMPONENTS.items():
        beh = comp["beh"][state]
        for emit in beh.get("emit", []):
            log_id = emit["id"]
            base_rate = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            key = f"{cid}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = float(BG_RATE_MULT_BY_MIN.get(minute, {}).get(key, 1.0))
            eff_rate = base_rate * mult

            hosts = comp.get("hosts", []) or []
            if scope == "per_host":
                host_list = hosts if hosts else [""]
                for h in host_list:
                    count = int(RNG.poisson(eff_rate))
                    for _ in range(count):
                        offset_ms = int(RNG.integers(0, 60_000))
                        ts_ms = minute * 60_000 + offset_ms
                        ref = f"{cid}.{log_id}"
                        overrides = coherent_overrides_for_log(
                            state=state,
                            flow_id="",
                            ref=ref,
                            ctx={},
                            attempt_no=1,
                            is_retry_marker=False,
                            minute_started=minute,
                            failure_severity=failure_sev,
                        )
                        add_log(rows, ts_ms, ref, state, trace_id="", host_override=h, overrides=overrides)
            else:
                count = int(RNG.poisson(eff_rate))
                for _ in range(count):
                    offset_ms = int(RNG.integers(0, 60_000))
                    ts_ms = minute * 60_000 + offset_ms
                    h = "" if not hosts else str(sample_choice(hosts))
                    ref = f"{cid}.{log_id}"
                    overrides = coherent_overrides_for_log(
                        state=state,
                        flow_id="",
                        ref=ref,
                        ctx={},
                        attempt_no=1,
                        is_retry_marker=False,
                        minute_started=minute,
                        failure_severity=failure_sev,
                    )
                    add_log(rows, ts_ms, ref, state, trace_id="", host_override=h, overrides=overrides)


def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    scen = SCENARIO["scenario"]
    f_events = scen["phases"]["f"]["events"]
    f_start = scen["time"]["phases"]["f"]["start_min"]
    for ev in f_events:
        at_min = int(ev["at_min"])
        failure_sev = severity_index_for_minute(at_min, f_start)
        for shot in ev.get("one_shots") or []:
            ref = shot["ref"]
            count = int(shot["count"])
            comp_id, _ = ref.split(".", 1)
            hosts = shot.get("hosts")
            if hosts is None:
                hosts = COMPONENTS[comp_id].get("hosts", []) or [""]
            for i in range(count):
                offset_ms = int(RNG.integers(0, 1000)) + i
                ts_ms = at_min * 60_000 + offset_ms
                host_override = str(sample_choice(list(hosts))) if hosts else ""
                overrides = coherent_overrides_for_log(
                    state="f",
                    flow_id="",
                    ref=ref,
                    ctx={},
                    attempt_no=1,
                    is_retry_marker=False,
                    minute_started=at_min,
                    failure_severity=failure_sev,
                )
                add_log(rows, ts_ms, ref, "f", trace_id="", host_override=host_override, overrides=overrides)


def emit_flows_for_minute(rows: List[Dict[str, Any]], minute: int, state: str, f_start: int) -> None:
    if state == "n":
        flows = FLOWS_N
        flow_rate_mult: Dict[str, float] = {}
    else:
        flows = FLOWS_F
        flow_rate_mult = FLOW_RATE_MULT_BY_MIN.get(minute, {})

    failure_sev = severity_index_for_minute(minute, f_start)

    for flow_id, flow in flows.items():
        base_rpm = float(flow["rpm"])
        mult = 1.0
        if state == "f":
            mult = float(flow_rate_mult.get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0.0:
            continue

        starts = int(RNG.poisson(eff_rpm))
        if starts <= 0:
            continue

        for _ in range(starts):
            start_offset_ms = int(RNG.integers(0, 60_000))
            flow_start_ts = minute * 60_000 + start_offset_ms

            ctx: Dict[str, Any] = {}
            ctx["session_id"] = rand_uuid()
            ctx["client_id"] = rand_hex(12)
            ctx["sync_bytes"] = sample_int(300, 65000)
            ctx["crash_bytes"] = sample_int(10_000, 800_000)

            ctx["crash_report_id"] = rand_hex(16)
            ctx["report_id"] = ctx["crash_report_id"]

            trace_id = ""
            if TRACING_ON and flow.get("trace", False):
                trace_id = rand_hex(32)
                ctx["trace_id"] = trace_id

            # Decide intended outcomes per variant, while keeping overridden state_vars values valid.
            if flow_id == "sync_cycle_success":
                ctx["server_throttled_types"] = sample_choice(["[]", "[SESSIONS]", "[PASSWORDS]"], weights=[0.85, 0.10, 0.05])
                ctx["client_status"] = sample_choice(["OK", "OK_NOOP"], weights=[0.9, 0.1])
            elif flow_id == "sync_cycle_ok_degraded":
                # Keep client sync_response.throttled_types within failure-domain AND consistent with server 200 template.
                ctx["server_throttled_types"] = "[]"
                ctx["client_status"] = "OK"
            elif flow_id == "sync_cycle_throttle_known":
                ctx["server_throttled_types"] = sample_choice(["[ALL_TYPES]", "[BOOKMARKS,PASSWORDS,SESSIONS,AUTOFILL]"], weights=[0.7, 0.3])
                ctx["client_status"] = "THROTTLED_ALL"
            elif flow_id == "sync_cycle_throttle_unknown_crash":
                ctx["server_throttled_types"] = "[ALL_TYPES,UNKNOWN_TYPE]"
                ctx["client_status"] = "THROTTLED_ALL"
            elif flow_id == "sync_cycle_quota_timeout_retry":
                ctx["server_throttled_types"] = "[]"
                ctx["client_status"] = "HTTP_503"
                ctx["quota_err"] = sample_choice(["DEADLINE_EXCEEDED", "UNAVAILABLE"], weights=[0.75, 0.25])
                ctx["quota_timeout_ms"] = int(clamp(800 + 700 * (failure_sev - 1) + abs(float(RNG.normal(0, 400))), 200, 20000))
                ctx["resp_503_reason"] = sample_choice(["upstream_timeout", "quota_unavailable", "over_capacity"], weights=[0.55, 0.30, 0.15])
            elif flow_id == "crash_report_upload_success":
                pass
            elif flow_id == "crash_report_upload_fail_retry":
                pass

            retry = flow.get("retry")
            attempts_total = 1
            if retry:
                attempts_total = sample_attempt_count(int(retry["max_attempts"]), float(retry["expected_attempts"]))
            ctx["attempts_total"] = attempts_total

            host_map = choose_hosts_for_flow(flow)

            latm = flow_latency_multiplier(minute, flow_id, state)

            ts = flow_start_ts
            prev_attempt_end = ts

            for attempt_no in range(1, attempts_total + 1):
                if attempt_no > 1 and retry:
                    b_p50, b_p95 = retry["backoff_ms"][attempt_no - 2]
                    backoff_ms = sample_lognormal_ms(b_p50, b_p95, softcap_mult=3.0)
                    ctx["next_backoff_ms"] = backoff_ms

                    marker_refs = retry.get("emit_per_retry", [])
                    marker_ts = prev_attempt_end + sample_int(0, 10)
                    for mr in marker_refs:
                        overrides = coherent_overrides_for_log(
                            state=state,
                            flow_id=flow_id,
                            ref=mr,
                            ctx=ctx,
                            attempt_no=attempt_no,
                            is_retry_marker=True,
                            minute_started=minute,
                            failure_severity=failure_sev,
                        )
                        add_log(rows, marker_ts, mr, state, trace_id=trace_id, host_override=host_map.get(mr.split(".", 1)[0], ""), overrides=overrides)
                        marker_ts += sample_int(0, 5)

                    ts = prev_attempt_end + backoff_ms + sample_int(0, 25)
                else:
                    ts = flow_start_ts

                emits = flow.get("emit", [])
                lat_pairs = flow.get("latency_ms", [])
                assert len(emits) == len(lat_pairs)

                for ref, (p50, p95) in zip(emits, lat_pairs):
                    eff_p50 = float(p50) * float(latm["p50"]) if state == "f" else float(p50)
                    eff_p95 = float(p95) * float(latm["p95"]) if state == "f" else float(p95)
                    ts += sample_lognormal_ms(eff_p50, eff_p95, softcap_mult=3.0)

                    overrides = coherent_overrides_for_log(
                        state=state,
                        flow_id=flow_id,
                        ref=ref,
                        ctx=ctx,
                        attempt_no=attempt_no,
                        is_retry_marker=False,
                        minute_started=minute,
                        failure_severity=failure_sev,
                    )
                    add_log(rows, ts, ref, state, trace_id=trace_id, host_override=host_map.get(ref.split(".", 1)[0], ""), overrides=overrides)

                prev_attempt_end = ts


def main() -> None:
    scen = SCENARIO["scenario"]
    total_minutes = int(scen["time"]["total_minutes"])
    f_start = int(scen["time"]["phases"]["f"]["start_min"])

    rows: List[Dict[str, Any]] = []

    emit_one_shots(rows)

    for minute in range(total_minutes):
        state = "n" if minute < f_start else "f"
        emit_background_for_minute(rows, minute, state, f_start)
        emit_flows_for_minute(rows, minute, state, f_start)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].apply(iso8601_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
