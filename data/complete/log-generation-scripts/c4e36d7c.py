"""
Plan / simulation design (deterministic, reproducible):

1) Data embedding
   - Copy the provided system description into SYSTEM (Python dict) capturing:
     components (id, svc, hosts, logs templates, beh emit rates) and flows (n/f),
     plus tracing config.
   - Copy the provided scenario into SCENARIO (Python dict) capturing:
     time boundaries, phases.n flows + manifestation, phases.f events (at_min, rate_multipliers,
     latency_multipliers, one_shots, manifestation), steady state blocks, and the f-phase
     diagnostic flows + manifestation inventory.

2) Timeline & base time
   - Scenario minute 0 maps to 2026-03-13T12:00:00.000Z (UTC).
   - Simulate minutes [0, total_minutes) with per-minute Poisson sampling for:
       a) background emitters (component.beh.<state>.emit)
       b) entry flow instances (flows.<state>.req[].rpm, scaled in failure by scenario multipliers)
   - Flow log timestamps are produced by adding sampled inter-log latencies (lognormal derived
     from [p50, p95]) to the flow instance start time. Retry chains add retry-only logs and
     sampled backoff between attempts.

3) Failure event controller (piecewise, persistent overrides)
   - At the start of failure (minute f.start_min), all multipliers are 1.0.
   - As we iterate minute-by-minute within failure, apply any event(s) at that minute:
       * rate_multipliers override and persist (flows and background log sources)
       * latency_multipliers override and persist (per flow id, scales p50/p95)
       * one_shots emit discrete logs at that event minute (not rate-scaled)
   - Multipliers affect emissions whose *start minute* is within the interval while active.
     Flow logs may spill across minutes due to latency/backoff; their latencies use the active
     latency multiplier at the flow's start minute.

4) Log generation rules
   - Allowed emission mechanisms only:
       * background (beh emit)
       * per-flow per-attempt emit list
       * retry-only emit_per_retry list (attempts 2..A)
       * scenario one_shots
   - Host/service identity:
       * host is taken from the emitting component's hosts list (or "" if none).
       * For flow instances, each involved component picks one host for the whole instance
         to keep host identity consistent within the instance.
   - Trace propagation:
       * tracing is ON; for flows with trace:true, generate one 32-hex trace_id per flow instance.
       * All logs of that instance (including retries) carry that trace_id in the CSV column.
       * If a message template includes {trace_id}, it is filled with the same value.

5) Variable sampling & coherence
   - Render message templates using variables sampled from defined domains (vars + state_vars).
   - For flow-correlated fields (req_id, trace_id, user_id, playlist_uri, track_uri, client_ip, user_hint),
     generate once per flow instance and reuse across all logs.
   - For response durations (dur_ms, wait_ms, startup_ms), prefer measured time deltas derived from
     the simulated timestamps, then clamp into the configured domain to avoid contradictions.
   - For selected "symptom" background logs (I/O wait, syslog flush), bias toward higher values in failure.
   - For event-marker one-shots (feature flag update, firewall rule, crashes/restarts, hard reset),
     apply overrides so emitted fields match the modeled incident progression (e.g., flag is disabled,
     firewall targets HTTPS/clients, crash reasons align with I/O stall).

6) Output
   - Collect all generated rows, sort by timestamp (and a sequence tiebreaker), format timestamp as
     ISO8601 with milliseconds and 'Z', and write logs.csv with exactly:
       timestamp, level, message, trace_id, service, host

Notes / implementation choices:
- Numpy's RNG.integers() cannot generate 128-bit ints; UUIDs are generated deterministically from RNG bytes.
- Retry-only logs (emit_per_retry) are emitted at the moment a retry is scheduled (end of the prior attempt),
  with attempt number referring to the upcoming attempt; for backoff flows, sleep_ms is set to the chosen backoff.
"""

from __future__ import annotations

import math
import random
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ----------------------------
# Determinism
# ----------------------------
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)
RNG = np.random.default_rng(SEED)

BASE_TIME = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)

# ----------------------------
# Embedded input data
# ----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "spotify_playback_popcount"},
    "states": {"n": "normal", "f": "failure"},
    "components": [
        {
            "id": "desktop_client",
            "svc": "desktop-client",
            "hosts": [],
            "to": [{"dst": "accesspoint", "proto": "https"}],
            "logs": {
                "popcount_retry_no_backoff": {
                    "lvl": "WARN",
                    "msg": "retrying popcount request req_id={req_id} attempt={attempt} reason={reason}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 6]},
                        "reason": {"k": "ch", "v": ["timeout", "server_error"]},
                    },
                },
                "popcount_conn_failed": {
                    "lvl": "WARN",
                    "msg": "popcount connection failed req_id={req_id} err={err} target={target}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "err": {"k": "ch", "v": ["ECONNREFUSED", "ETIMEDOUT", "ENETUNREACH"]},
                        "target": {"k": "str", "v": "host:port"},
                    },
                },
                "client_backoff_scheduled": {
                    "lvl": "INFO",
                    "msg": "backoff scheduled for {op} req_id={req_id} sleep_ms={sleep_ms} attempt={attempt}",
                    "vars": {
                        "op": {"k": "ch", "v": ["connect_accesspoint", "popcount_fetch"]},
                        "req_id": {"k": "uuid", "v": None},
                        "sleep_ms": {"k": "i", "v": [100, 60000]},
                        "attempt": {"k": "i", "v": [2, 6]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "accesspoint",
            "svc": "accesspoint",
            "hosts": ["ap-eu-01", "ap-eu-02", "ap-eu-03", "ap-eu-04", "ap-eu-05", "ap-eu-06"],
            "to": [
                {"dst": "popcount", "proto": "grpc"},
                {"dst": "bartender", "proto": "grpc"},
                {"dst": "playback_service", "proto": "grpc"},
                {"dst": "auth_service", "proto": "grpc"},
                {"dst": "log_collector", "proto": "tcp"},
            ],
            "logs": {
                "playback_req": {
                    "lvl": "INFO",
                    "msg": "playback start request user={user_id} track={track_uri} req_id={req_id} trace_id={trace_id} client_ip={client_ip}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1000000, 9000000]},
                        "track_uri": {"k": "str", "v": "spotify:track:{id}"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                        "client_ip": {"k": "ip", "v": None},
                    },
                },
                "playback_resp": {
                    "lvl": "INFO",
                    "msg": "playback start response req_id={req_id} status={status} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "bytes": {"k": "i", "v": [0, 2000000]}},
                    "state_vars": {
                        "n": {
                            "status": {"k": "ch", "v": [200, 206]},
                            "dur_ms": {"k": "i", "v": [20, 200]},
                        },
                        "f": {
                            "status": {"k": "ch", "v": [200, 206, 503, 504]},
                            "dur_ms": {"k": "i", "v": [80, 12000]},
                        },
                    },
                },
                "login_req": {
                    "lvl": "INFO",
                    "msg": "login request user={user_hint} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_hint": {"k": "str", "v": "email_hash"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "login_resp": {
                    "lvl": "INFO",
                    "msg": "login response req_id={req_id} status={status} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}},
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": [200, 401]}, "dur_ms": {"k": "i", "v": [30, 400]}},
                        "f": {
                            "status": {"k": "ch", "v": [200, 401, 502, 504]},
                            "dur_ms": {"k": "i", "v": [80, 15000]},
                        },
                    },
                },
                "login_timeout": {
                    "lvl": "ERROR",
                    "msg": "login gateway timeout req_id={req_id} wait_ms={wait_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "wait_ms": {"k": "i", "v": [1000, 20000]}},
                },
                "discovery_req": {
                    "lvl": "INFO",
                    "msg": "discovery page request user={user_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1000000, 9000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "discovery_resp": {
                    "lvl": "INFO",
                    "msg": "discovery page response req_id={req_id} status={status} dur_ms={dur_ms} source={source}",
                    "vars": {"req_id": {"k": "uuid", "v": None}},
                    "state_vars": {
                        "n": {
                            "status": {"k": "ch", "v": [200]},
                            "dur_ms": {"k": "i", "v": [10, 250]},
                            "source": {"k": "ch", "v": ["cache"]},
                        },
                        "f": {
                            "status": {"k": "ch", "v": [200, 503, 504]},
                            "dur_ms": {"k": "i", "v": [50, 20000]},
                            "source": {"k": "ch", "v": ["cache", "live", "degraded_cache"]},
                        },
                    },
                },
                "popcount_req": {
                    "lvl": "INFO",
                    "msg": "popcount subscribers fetch playlist={playlist_uri} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "playlist_uri": {"k": "str", "v": "spotify:playlist:{id}"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "popcount_resp": {
                    "lvl": "INFO",
                    "msg": "popcount subscribers response req_id={req_id} status={status} dur_ms={dur_ms} subscriber_count={subscriber_count}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "subscriber_count": {"k": "i", "v": [0, 5000]}},
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": [200]}, "dur_ms": {"k": "i", "v": [5, 150]}},
                        "f": {"status": {"k": "ch", "v": [200, 503, 504]}, "dur_ms": {"k": "i", "v": [5, 15000]}},
                    },
                },
                "popcount_timeout_verbose": {
                    "lvl": "ERROR",
                    "msg": "popcount proxy timeout req_id={req_id} timeout_ms={timeout_ms} queued={queued} err={err} stack_id={stack_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "timeout_ms": {"k": "i", "v": [1000, 20000]},
                        "queued": {"k": "i", "v": [0, 20000]},
                        "err": {"k": "ch", "v": ["upstream_timeout", "context_deadline_exceeded", "client_cancelled"]},
                        "stack_id": {"k": "hex", "v": 8},
                    },
                },
                "io_wait_high": {
                    "lvl": "WARN",
                    "msg": "io wait high iowait_pct={iowait_pct} disk={disk} pending_writes={pending_writes}",
                    "vars": {
                        "iowait_pct": {"k": "f", "v": [0.0, 100.0]},
                        "disk": {"k": "ch", "v": ["/dev/nvme0n1", "/dev/sda"]},
                        "pending_writes": {"k": "i", "v": [0, 500000]},
                    },
                },
                "syslog_flush_slow": {
                    "lvl": "WARN",
                    "msg": "syslog flush slow flush_ms={flush_ms} queue_bytes={queue_bytes}",
                    "vars": {
                        "flush_ms": {"k": "i", "v": [10, 20000]},
                        "queue_bytes": {"k": "i", "v": [1000000, 800000000]},
                    },
                },
                "queue_depth_warn": {
                    "lvl": "WARN",
                    "msg": "upstream request queue high upstream={upstream} depth={depth}",
                    "vars": {
                        "upstream": {"k": "ch", "v": ["popcount", "bartender", "playback", "auth"]},
                        "depth": {"k": "i", "v": [50, 50000]},
                    },
                },
                "process_crash": {
                    "lvl": "CRITICAL",
                    "msg": "process crashed signal={signal} pid={pid} reason={reason}",
                    "vars": {
                        "signal": {"k": "ch", "v": ["SIGKILL", "SIGABRT", "SIGSEGV"]},
                        "pid": {"k": "i", "v": [1000, 65000]},
                        "reason": {"k": "ch", "v": ["io_stall", "oom", "watchdog_timeout"]},
                    },
                },
                "process_restart": {
                    "lvl": "INFO",
                    "msg": "process started pid={pid} build={build} reason={reason}",
                    "vars": {
                        "pid": {"k": "i", "v": [1000, 65000]},
                        "build": {"k": "hex", "v": 7},
                        "reason": {"k": "ch", "v": ["supervisor_restart", "manual_restart"]},
                    },
                },
                "firewall_block_applied": {
                    "lvl": "WARN",
                    "msg": "firewall rule applied action={action} src={src} dst_port={dst_port} ttl_s={ttl_s}",
                    "vars": {
                        "action": {"k": "ch", "v": ["DROP", "REJECT"]},
                        "src": {"k": "ch", "v": ["0.0.0.0/0", "eu_clients"]},
                        "dst_port": {"k": "i", "v": [1, 65535]},
                        "ttl_s": {"k": "i", "v": [60, 3600]},
                    },
                },
                "hard_reset": {
                    "lvl": "WARN",
                    "msg": "host hard reset initiated by={by} reason={reason}",
                    "vars": {
                        "by": {"k": "ch", "v": ["sre_oncall", "infra_automation"]},
                        "reason": {"k": "ch", "v": ["unresponsive_io", "kernel_hang"]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "queue_depth_warn", "per_min": 0.2, "scope": "per_host"},
                        {"id": "syslog_flush_slow", "per_min": 0.1, "scope": "per_host"},
                        {"id": "io_wait_high", "per_min": 0.05, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "queue_depth_warn", "per_min": 2.0, "scope": "per_host"},
                        {"id": "syslog_flush_slow", "per_min": 3.0, "scope": "per_host"},
                        {"id": "io_wait_high", "per_min": 4.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "popcount",
            "svc": "popcount",
            "hosts": ["popcount-01", "popcount-02", "popcount-03"],
            "to": [
                {"dst": "log_collector", "proto": "tcp"},
                {"dst": "bartender", "proto": "grpc"},
                {"dst": "accesspoint", "proto": "grpc"},
            ],
            "logs": {
                "get_subscribers_ok": {
                    "lvl": "INFO",
                    "msg": "get subscribers ok playlist={playlist_uri} req_id={req_id} dur_ms={dur_ms} rows={rows}",
                    "vars": {
                        "playlist_uri": {"k": "str", "v": "spotify:playlist:{id}"},
                        "req_id": {"k": "uuid", "v": None},
                        "rows": {"k": "i", "v": [0, 5000]},
                    },
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [3, 120]}}, "f": {"dur_ms": {"k": "i", "v": [10, 20000]}}},
                },
                "fast_fail_empty": {
                    "lvl": "WARN",
                    "msg": "fast fail enabled returning empty subscribers playlist={playlist_uri} req_id={req_id} reason={reason}",
                    "vars": {
                        "playlist_uri": {"k": "str", "v": "spotify:playlist:{id}"},
                        "req_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["latency_budget_exceeded", "over_capacity"]},
                    },
                },
                "threadpool_saturated": {
                    "lvl": "ERROR",
                    "msg": "threadpool saturated pool={pool} active={active} queued={queued} p95_ms={p95_ms}",
                    "vars": {
                        "pool": {"k": "ch", "v": ["http_worker", "grpc_worker"]},
                        "active": {"k": "i", "v": [50, 500]},
                        "queued": {"k": "i", "v": [500, 50000]},
                        "p95_ms": {"k": "i", "v": [50, 30000]},
                    },
                },
                "health_unhealthy": {
                    "lvl": "WARN",
                    "msg": "healthcheck unhealthy status={status} latency_p95_ms={latency_p95_ms}",
                    "vars": {"status": {"k": "ch", "v": ["DEGRADED", "UNHEALTHY"]}, "latency_p95_ms": {"k": "i", "v": [100, 60000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "threadpool_saturated", "per_min": 0.1, "scope": "per_host"}, {"id": "health_unhealthy", "per_min": 0.05, "scope": "per_host"}]},
                "f": {"emit": [{"id": "threadpool_saturated", "per_min": 2.0, "scope": "per_host"}, {"id": "health_unhealthy", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "bartender",
            "svc": "bartender",
            "hosts": ["bartender-01", "bartender-02", "bartender-03"],
            "to": [{"dst": "popcount", "proto": "grpc"}, {"dst": "accesspoint", "proto": "grpc"}, {"dst": "log_collector", "proto": "tcp"}],
            "logs": {
                "discovery_rendered": {
                    "lvl": "INFO",
                    "msg": "rendered discovery page user={user_id} req_id={req_id} dur_ms={dur_ms} source={source}",
                    "vars": {"user_id": {"k": "i", "v": [1000000, 9000000]}, "req_id": {"k": "uuid", "v": None}},
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [20, 300]}, "source": {"k": "ch", "v": ["cache"]}},
                        "f": {"dur_ms": {"k": "i", "v": [30, 20000]}, "source": {"k": "ch", "v": ["cache", "live", "degraded_cache"]}},
                    },
                },
                "popcount_call_failed": {
                    "lvl": "WARN",
                    "msg": "popcount call failed req_id={req_id} playlist={playlist_uri} err={err} attempt_ms={attempt_ms}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "playlist_uri": {"k": "str", "v": "spotify:playlist:{id}"},
                        "err": {"k": "ch", "v": ["deadline_exceeded", "unavailable", "over_capacity"]},
                        "attempt_ms": {"k": "i", "v": [50, 20000]},
                    },
                },
                "popcount_latency_warn": {"lvl": "WARN", "msg": "popcount dependency slow p95_ms={p95_ms} error_rate={error_rate}", "vars": {"p95_ms": {"k": "i", "v": [50, 30000]}, "error_rate": {"k": "f", "v": [0.0, 1.0]}}},
                "cache_refresh": {"lvl": "INFO", "msg": "discovery cache refresh completed items={items} dur_ms={dur_ms}", "vars": {"items": {"k": "i", "v": [1000, 200000]}, "dur_ms": {"k": "i", "v": [50, 10000]}}},
                "feature_flag_update": {
                    "lvl": "INFO",
                    "msg": "feature flag updated flag={flag} old={old} new={new} actor={actor}",
                    "vars": {"flag": {"k": "ch", "v": ["discovery_popcount_dependency"]}, "old": {"k": "ch", "v": ["on", "off"]}, "new": {"k": "ch", "v": ["on", "off"]}, "actor": {"k": "ch", "v": ["sre_oncall", "bartender_oncall"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cache_refresh", "per_min": 0.5, "scope": "per_host"}, {"id": "popcount_latency_warn", "per_min": 0.05, "scope": "per_host"}]},
                "f": {"emit": [{"id": "cache_refresh", "per_min": 0.5, "scope": "per_host"}, {"id": "popcount_latency_warn", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "playback_service",
            "svc": "playback-service",
            "hosts": ["playback-01", "playback-02", "playback-03", "playback-04"],
            "to": [{"dst": "accesspoint", "proto": "grpc"}, {"dst": "log_collector", "proto": "tcp"}],
            "logs": {
                "session_started": {
                    "lvl": "INFO",
                    "msg": "stream session started user={user_id} track={track_uri} req_id={req_id} codec={codec} startup_ms={startup_ms}",
                    "vars": {"user_id": {"k": "i", "v": [1000000, 9000000]}, "track_uri": {"k": "str", "v": "spotify:track:{id}"}, "req_id": {"k": "uuid", "v": None}, "codec": {"k": "ch", "v": ["aac", "ogg", "mp3"]}},
                    "state_vars": {"n": {"startup_ms": {"k": "i", "v": [20, 200]}}, "f": {"startup_ms": {"k": "i", "v": [80, 12000]}}},
                },
                "metrics_flush": {"lvl": "DEBUG", "msg": "metrics flush series={series} dur_ms={dur_ms}", "vars": {"series": {"k": "i", "v": [100, 20000]}, "dur_ms": {"k": "i", "v": [1, 5000]}}},
            },
            "beh": {"n": {"emit": [{"id": "metrics_flush", "per_min": 0.5, "scope": "per_host"}]}, "f": {"emit": [{"id": "metrics_flush", "per_min": 0.5, "scope": "per_host"}]}},
        },
        {
            "id": "auth_service",
            "svc": "auth-service",
            "hosts": ["auth-01", "auth-02", "auth-03"],
            "to": [{"dst": "accesspoint", "proto": "grpc"}, {"dst": "log_collector", "proto": "tcp"}],
            "logs": {
                "login_ok": {"lvl": "INFO", "msg": "login ok req_id={req_id} user={user_id} method={method}", "vars": {"req_id": {"k": "uuid", "v": None}, "user_id": {"k": "i", "v": [1000000, 9000000]}, "method": {"k": "ch", "v": ["password", "oauth_refresh", "sso"]}}},
                "login_failed": {"lvl": "WARN", "msg": "login failed req_id={req_id} reason={reason}", "vars": {"req_id": {"k": "uuid", "v": None}, "reason": {"k": "ch", "v": ["invalid_credentials", "rate_limited", "backend_unavailable"]}}},
                "token_signing_lag": {"lvl": "WARN", "msg": "token signing lag p95_ms={p95_ms} queue={queue}", "vars": {"p95_ms": {"k": "i", "v": [5, 10000]}, "queue": {"k": "i", "v": [0, 5000]}}},
            },
            "beh": {"n": {"emit": [{"id": "token_signing_lag", "per_min": 0.2, "scope": "per_host"}]}, "f": {"emit": [{"id": "token_signing_lag", "per_min": 0.2, "scope": "per_host"}]}},
        },
        {
            "id": "log_collector",
            "svc": "log-collector",
            "hosts": ["syslog-01", "syslog-02"],
            "to": [],
            "logs": {"disk_sync_warn": {"lvl": "WARN", "msg": "disk sync slow flush_ms={flush_ms} dropped_messages={dropped}", "vars": {"flush_ms": {"k": "i", "v": [10, 30000]}, "dropped": {"k": "i", "v": [0, 500000]}}}},
            "beh": {"n": {"emit": [{"id": "disk_sync_warn", "per_min": 0.2, "scope": "per_host"}]}, "f": {"emit": [{"id": "disk_sync_warn", "per_min": 1.0, "scope": "per_host"}]}},
        },
    ],
    "tracing": {"on": True, "origins": ["desktop_client"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "req": [
                {"id": "playback_start_ok", "rpm": 200.0, "path": ["desktop_client", "accesspoint", "playback_service", "accesspoint"], "emit": ["accesspoint.playback_req", "playback_service.session_started", "accesspoint.playback_resp"], "latency_ms": [[5, 20], [30, 120], [2, 10]], "trace": True},
                {"id": "auth_login_ok", "rpm": 70.0, "path": ["desktop_client", "accesspoint", "auth_service", "accesspoint"], "emit": ["accesspoint.login_req", "auth_service.login_ok", "accesspoint.login_resp"], "latency_ms": [[5, 20], [40, 200], [2, 10]], "trace": True},
                {"id": "discovery_page_ok", "rpm": 110.0, "path": ["desktop_client", "accesspoint", "bartender", "accesspoint"], "emit": ["accesspoint.discovery_req", "bartender.discovery_rendered", "accesspoint.discovery_resp"], "latency_ms": [[5, 20], [25, 120], [2, 10]], "trace": True},
                {"id": "popcount_subscribers_ok", "rpm": 90.0, "path": ["desktop_client", "accesspoint", "popcount", "accesspoint"], "emit": ["accesspoint.popcount_req", "popcount.get_subscribers_ok", "accesspoint.popcount_resp"], "latency_ms": [[5, 20], [15, 80], [2, 10]], "trace": True},
            ]
        },
        "f": {
            "req": [
                {"id": "playback_start_degraded", "rpm": 160.0, "path": ["desktop_client", "accesspoint", "playback_service", "accesspoint"], "emit": ["accesspoint.playback_req", "playback_service.session_started", "accesspoint.playback_resp"], "latency_ms": [[10, 60], [150, 4000], [5, 40]], "trace": True},
                {"id": "auth_login_ok", "rpm": 45.0, "path": ["desktop_client", "accesspoint", "auth_service", "accesspoint"], "emit": ["accesspoint.login_req", "auth_service.login_ok", "accesspoint.login_resp"], "latency_ms": [[10, 80], [60, 800], [5, 40]], "trace": True},
                {"id": "auth_login_timeout", "rpm": 20.0, "path": ["desktop_client", "accesspoint"], "emit": ["accesspoint.login_req", "accesspoint.login_timeout"], "latency_ms": [[10, 120], [1500, 15000]], "trace": True},
                {"id": "discovery_page_ok_cached", "rpm": 50.0, "path": ["desktop_client", "accesspoint", "bartender", "accesspoint"], "emit": ["accesspoint.discovery_req", "bartender.discovery_rendered", "accesspoint.discovery_resp"], "latency_ms": [[10, 80], [60, 1500], [5, 40]], "trace": True},
                {"id": "discovery_page_popcount_timeout", "rpm": 40.0, "path": ["desktop_client", "accesspoint", "bartender", "popcount", "bartender", "accesspoint"], "emit": ["accesspoint.discovery_req", "bartender.popcount_call_failed", "accesspoint.discovery_resp"], "latency_ms": [[10, 80], [500, 15000], [5, 40]], "trace": True},
                {"id": "popcount_subscribers_empty_fastfail", "rpm": 60.0, "path": ["desktop_client", "accesspoint", "popcount", "accesspoint"], "emit": ["accesspoint.popcount_req", "popcount.fast_fail_empty", "accesspoint.popcount_resp"], "latency_ms": [[10, 80], [10, 250], [5, 40]], "trace": True},
                {
                    "id": "popcount_subscribers_timeout_retry_storm",
                    "rpm": 60.0,
                    "path": ["desktop_client", "accesspoint", "popcount", "accesspoint"],
                    "emit": ["accesspoint.popcount_req", "accesspoint.popcount_timeout_verbose"],
                    "latency_ms": [[10, 80], [1500, 20000]],
                    "retry": {"max_attempts": 6, "expected_attempts": 4.0, "emit_per_retry": ["desktop_client.popcount_retry_no_backoff"], "backoff_ms": [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]},
                    "trace": True,
                },
                {
                    "id": "popcount_subscribers_conn_refused_backoff",
                    "rpm": 40.0,
                    "path": ["desktop_client", "accesspoint"],
                    "emit": ["desktop_client.popcount_conn_failed"],
                    "latency_ms": [[5, 50]],
                    "retry": {"max_attempts": 4, "expected_attempts": 2.5, "emit_per_retry": ["desktop_client.client_backoff_scheduled"], "backoff_ms": [[200, 800], [800, 3000], [3000, 12000]]},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "ops_6000_popcount_retry_storm",
        "title": "OPS-6000: Popcount overload triggers retry storm and Accesspoint I/O collapse",
        "states": {"n": "normal", "f": "failure"},
        "time": {"total_minutes": 36, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 36}}},
        "phases": {
            "n": {
                "flows": ["playback_start_ok", "auth_login_ok", "discovery_page_ok", "popcount_subscribers_ok"],
                "manifestation": ["accesspoint.playback_resp", "accesspoint.login_resp", "accesspoint.discovery_resp", "accesspoint.popcount_resp"],
            },
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 18,
                        "component": "bartender",
                        "flows": ["discovery_page_popcount_timeout", "discovery_page_ok_cached", "popcount_subscribers_timeout_retry_storm"],
                        "rate_multipliers": {
                            "accesspoint.io_wait_high": 0.0,
                            "accesspoint.syslog_flush_slow": 0.0,
                            "accesspoint.queue_depth_warn": 0.5,
                            "popcount_subscribers_conn_refused_backoff": 0.0,
                        },
                        "manifestation": ["bartender.popcount_latency_warn", "bartender.popcount_call_failed", "popcount.threadpool_saturated"],
                    },
                    {
                        "order": 2,
                        "at_min": 21,
                        "component": "bartender",
                        "flows": ["popcount_subscribers_timeout_retry_storm", "discovery_page_popcount_timeout", "popcount_subscribers_empty_fastfail"],
                        "rate_multipliers": {
                            "popcount_subscribers_timeout_retry_storm": 1.8,
                            "discovery_page_popcount_timeout": 0.2,
                            "popcount_subscribers_empty_fastfail": 0.7,
                            "accesspoint.queue_depth_warn": 1.5,
                            "accesspoint.io_wait_high": 0.0,
                            "accesspoint.syslog_flush_slow": 0.0,
                            "popcount.threadpool_saturated": 2.0,
                            "popcount.health_unhealthy": 1.2,
                            "bartender.popcount_latency_warn": 1.5,
                            "popcount_subscribers_conn_refused_backoff": 0.0,
                        },
                        "one_shots": [{"ref": "bartender.feature_flag_update", "count": 1, "hosts": ["bartender-01"]}],
                        "manifestation": ["accesspoint.popcount_timeout_verbose", "desktop_client.popcount_retry_no_backoff", "popcount.health_unhealthy"],
                    },
                    {
                        "order": 3,
                        "at_min": 25,
                        "component": "accesspoint",
                        "flows": ["auth_login_timeout", "popcount_subscribers_timeout_retry_storm", "playback_start_degraded"],
                        "rate_multipliers": {
                            "popcount_subscribers_timeout_retry_storm": 2.3,
                            "auth_login_timeout": 2.0,
                            "discovery_page_ok_cached": 0.6,
                            "discovery_page_popcount_timeout": 0.1,
                            "popcount_subscribers_empty_fastfail": 0.5,
                            "accesspoint.io_wait_high": 1.0,
                            "accesspoint.syslog_flush_slow": 1.0,
                            "accesspoint.queue_depth_warn": 2.5,
                            "popcount.threadpool_saturated": 3.0,
                            "popcount.health_unhealthy": 1.5,
                            "bartender.popcount_latency_warn": 2.0,
                            "log_collector.disk_sync_warn": 2.0,
                            "popcount_subscribers_conn_refused_backoff": 0.0,
                        },
                        "latency_multipliers": {"playback_start_degraded": {"p50": 1.3, "p95": 1.8}, "auth_login_timeout": {"p50": 1.2, "p95": 1.5}},
                        "one_shots": [
                            {"ref": "accesspoint.process_crash", "count": 10, "hosts": ["ap-eu-01", "ap-eu-02", "ap-eu-03", "ap-eu-04", "ap-eu-05", "ap-eu-06"]},
                            {"ref": "accesspoint.process_restart", "count": 10, "hosts": ["ap-eu-01", "ap-eu-02", "ap-eu-03", "ap-eu-04", "ap-eu-05", "ap-eu-06"]},
                        ],
                        "manifestation": ["accesspoint.syslog_flush_slow", "accesspoint.io_wait_high", "accesspoint.popcount_timeout_verbose", "accesspoint.process_crash"],
                    },
                    {
                        "order": 4,
                        "at_min": 30,
                        "component": "accesspoint",
                        "flows": ["popcount_subscribers_conn_refused_backoff", "popcount_subscribers_timeout_retry_storm", "auth_login_timeout"],
                        "rate_multipliers": {
                            "popcount_subscribers_timeout_retry_storm": 0.7,
                            "popcount_subscribers_conn_refused_backoff": 1.0,
                            "auth_login_timeout": 0.7,
                            "playback_start_degraded": 0.95,
                            "discovery_page_ok_cached": 0.8,
                            "discovery_page_popcount_timeout": 0.05,
                            "popcount_subscribers_empty_fastfail": 0.9,
                            "accesspoint.io_wait_high": 0.6,
                            "accesspoint.syslog_flush_slow": 0.6,
                            "accesspoint.queue_depth_warn": 1.3,
                            "popcount.threadpool_saturated": 1.8,
                            "popcount.health_unhealthy": 1.2,
                            "bartender.popcount_latency_warn": 1.2,
                            "log_collector.disk_sync_warn": 1.0,
                        },
                        "one_shots": [
                            {"ref": "accesspoint.firewall_block_applied", "count": 6, "hosts": ["ap-eu-01", "ap-eu-02", "ap-eu-03", "ap-eu-04", "ap-eu-05", "ap-eu-06"]},
                            {"ref": "accesspoint.hard_reset", "count": 2, "hosts": ["ap-eu-02", "ap-eu-05"]},
                        ],
                        "manifestation": ["accesspoint.firewall_block_applied", "desktop_client.popcount_conn_failed", "desktop_client.client_backoff_scheduled", "accesspoint.io_wait_high"],
                    },
                ],
                "steady": [
                    {"component": "popcount", "manifestation": ["popcount.threadpool_saturated", "popcount.health_unhealthy", "popcount.fast_fail_empty"]},
                    {"component": "accesspoint", "manifestation": ["accesspoint.io_wait_high", "accesspoint.syslog_flush_slow", "accesspoint.login_timeout"]},
                    {"component": "playback_service", "manifestation": ["playback_service.session_started", "accesspoint.playback_resp"]},
                ],
                "flows": [
                    "playback_start_degraded",
                    "auth_login_ok",
                    "auth_login_timeout",
                    "discovery_page_ok_cached",
                    "discovery_page_popcount_timeout",
                    "popcount_subscribers_empty_fastfail",
                    "popcount_subscribers_timeout_retry_storm",
                    "popcount_subscribers_conn_refused_backoff",
                ],
                "manifestation": [
                    "popcount.threadpool_saturated",
                    "popcount.health_unhealthy",
                    "bartender.popcount_latency_warn",
                    "bartender.popcount_call_failed",
                    "accesspoint.popcount_timeout_verbose",
                    "accesspoint.syslog_flush_slow",
                    "accesspoint.io_wait_high",
                    "accesspoint.login_timeout",
                    "desktop_client.popcount_retry_no_backoff",
                    "desktop_client.client_backoff_scheduled",
                ],
            },
        },
    }
}


# ----------------------------
# Helpers: domains, sampling, formatting
# ----------------------------
PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
Z95 = 1.6448536269514722


def _hex(n: int) -> str:
    return "".join(RNG.choice(list("0123456789abcdef"), size=n))


def _uuid4_det() -> str:
    """
    Deterministic UUIDv4-like from RNG bytes.
    We set the RFC 4122 version/variant bits so it looks like a proper v4 UUID.
    """
    b = bytearray(RNG.bytes(16))
    b[6] = (b[6] & 0x0F) | 0x40  # version 4
    b[8] = (b[8] & 0x3F) | 0x80  # variant RFC4122
    return str(uuid.UUID(bytes=bytes(b)))


def _ip() -> str:
    a = int(RNG.integers(80, 100))
    b = int(RNG.integers(0, 256))
    c = int(RNG.integers(0, 256))
    d = int(RNG.integers(1, 255))
    return f"{a}.{b}.{c}.{d}"


_component_by_id: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}


def _hinted_str(hint: str) -> str:
    if "{id}" in hint:
        return hint.replace("{id}", _hex(16))
    if hint == "email_hash":
        return _hex(16)
    if hint == "host:port":
        ap_hosts = _component_by_id["accesspoint"]["hosts"]
        return f"{RNG.choice(ap_hosts)}:443"
    return f"{hint}-{_hex(6)}"


def sample_var(kind: str, domain: Any) -> Any:
    if kind == "i":
        lo, hi = int(domain[0]), int(domain[1])
        return int(RNG.integers(lo, hi + 1))
    if kind == "f":
        lo, hi = float(domain[0]), float(domain[1])
        val = float(RNG.uniform(lo, hi))
        return float(f"{val:.3f}")
    if kind == "ch":
        v = RNG.choice(domain)
        return v.item() if hasattr(v, "item") else v
    if kind == "uuid":
        return _uuid4_det()
    if kind == "hex":
        return _hex(int(domain))
    if kind == "ip":
        return _ip()
    if kind == "str":
        return _hinted_str(str(domain))
    raise ValueError(f"Unknown var kind: {kind}")


def lognormal_ms(p50: float, p95: float, *, softcap_mult: float = 2.5) -> float:
    p50 = float(p50)
    p95 = float(p95)
    if p50 <= 0 and p95 <= 0:
        return 0.0
    p50 = max(p50, 0.001)
    p95 = max(p95, p50)
    sigma = math.log(p95 / p50) / Z95 if p95 > p50 else 0.0
    mu = math.log(p50)
    val = float(RNG.lognormal(mean=mu, sigma=sigma))
    softcap = softcap_mult * p95
    if softcap > 0:
        val = min(val, softcap)
    val += float(RNG.normal(0.0, 1.5))  # tiny jitter
    return max(0.0, val)


def dt_to_iso_ms(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def minute_start(m: int) -> datetime:
    return BASE_TIME + timedelta(minutes=m)


# Build (component_id, log_id) -> template dict and placeholder set
_log_templates: Dict[Tuple[str, str], Dict[str, Any]] = {}
_log_placeholders: Dict[Tuple[str, str], set] = {}

for comp_id, comp in _component_by_id.items():
    for log_id, tpl in comp.get("logs", {}).items():
        _log_templates[(comp_id, log_id)] = tpl
        _log_placeholders[(comp_id, log_id)] = set(PLACEHOLDER_RE.findall(tpl["msg"]))

# Flows by state and id
_flows: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for st in ("n", "f"):
    for f in SYSTEM["flows"][st]["req"]:
        _flows[st][f["id"]] = f


# ----------------------------
# Rendering / coherence utilities
# ----------------------------
def split_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def clamp_int(val: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(val)))


def bias_range_int(lo: int, hi: int, *, bias: float) -> int:
    bias = float(bias)
    a = 1.0 + 4.0 * bias
    b = 1.0 + 4.0 * (1.0 - bias)
    x = float(RNG.beta(a, b))
    return int(lo + x * (hi - lo))


def bias_range_float(lo: float, hi: float, *, bias: float) -> float:
    a = 1.0 + 4.0 * bias
    b = 1.0 + 4.0 * (1.0 - bias)
    x = float(RNG.beta(a, b))
    return float(f"{(lo + x * (hi - lo)):.3f}")


def render_message(
    comp_id: str,
    log_id: str,
    state: str,
    *,
    context: Dict[str, Any],
    overrides: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    tpl = _log_templates[(comp_id, log_id)]
    placeholders = _log_placeholders[(comp_id, log_id)]
    overrides = overrides or {}

    values: Dict[str, Any] = {}

    # Template domains (vars + state_vars)
    domains: Dict[str, Dict[str, Any]] = {}
    domains.update(tpl.get("vars", {}))
    if "state_vars" in tpl and tpl["state_vars"] is not None:
        domains.update(tpl["state_vars"][state])

    # First, fill from overrides/context where applicable
    for k in placeholders:
        if k in overrides:
            values[k] = overrides[k]
        elif k in context:
            values[k] = context[k]

    # Fill remaining from domains
    for k in placeholders:
        if k in values:
            continue
        dom = domains.get(k)
        values[k] = sample_var(dom["k"], dom["v"]) if dom is not None else "unknown"

    # Symptom biases for background signals (keep within declared ranges)
    if comp_id == "accesspoint" and log_id == "io_wait_high" and state == "f":
        values["iowait_pct"] = bias_range_float(0.0, 100.0, bias=0.75)
        values["pending_writes"] = bias_range_int(0, 500000, bias=0.80)
    if comp_id == "accesspoint" and log_id == "syslog_flush_slow" and state == "f":
        values["flush_ms"] = bias_range_int(10, 20000, bias=0.80)
        values["queue_bytes"] = bias_range_int(1000000, 800000000, bias=0.75)
    if comp_id == "accesspoint" and log_id == "queue_depth_warn" and state == "f":
        if "upstream" in placeholders and "upstream" not in overrides:
            v = RNG.choice(["popcount", "popcount", "popcount", "bartender", "auth", "playback"])
            values["upstream"] = v.item() if hasattr(v, "item") else v
        if "depth" in placeholders and "depth" not in overrides:
            values["depth"] = bias_range_int(50, 50000, bias=0.75)

    if comp_id == "popcount" and log_id in ("threadpool_saturated", "health_unhealthy") and state == "f":
        if log_id == "threadpool_saturated":
            values["active"] = bias_range_int(50, 500, bias=0.85)
            values["queued"] = bias_range_int(500, 50000, bias=0.85)
            values["p95_ms"] = bias_range_int(50, 30000, bias=0.80)
        else:
            values["latency_p95_ms"] = bias_range_int(100, 60000, bias=0.80)
            v = RNG.choice(["DEGRADED", "UNHEALTHY", "UNHEALTHY"])
            values["status"] = v.item() if hasattr(v, "item") else v

    if comp_id == "bartender" and log_id == "popcount_latency_warn" and state == "f":
        values["p95_ms"] = bias_range_int(50, 30000, bias=0.80)
        values["error_rate"] = bias_range_float(0.0, 1.0, bias=0.70)

    msg = tpl["msg"].format(**values)
    lvl = tpl["lvl"]
    return lvl, msg


# ----------------------------
# Retry attempt sampling
# ----------------------------
def sample_attempt_count(retry_cfg: Optional[Dict[str, Any]]) -> int:
    if not retry_cfg:
        return 1
    max_attempts = int(retry_cfg["max_attempts"])
    expected = float(retry_cfg["expected_attempts"])
    expected = max(1.0, min(float(max_attempts), expected))
    if max_attempts == 1:
        return 1
    # attempts = 1 + Binomial(n=max-1, p) where p yields desired expectation
    p = (expected - 1.0) / float(max_attempts - 1)
    p = max(0.0, min(1.0, p))
    extra = int(RNG.binomial(n=max_attempts - 1, p=p))
    return 1 + extra


# ----------------------------
# Event multiplier controller
# ----------------------------
@dataclass
class ActiveMultipliers:
    flow_rate: Dict[str, float]
    bg_rate: Dict[str, float]  # key: "component.log_id"
    flow_latency: Dict[str, Dict[str, float]]  # key flow_id -> {"p50":x, "p95":y}


def init_active_multipliers() -> ActiveMultipliers:
    return ActiveMultipliers(
        flow_rate=defaultdict(lambda: 1.0),
        bg_rate=defaultdict(lambda: 1.0),
        flow_latency=defaultdict(lambda: {"p50": 1.0, "p95": 1.0}),
    )


def apply_event(active: ActiveMultipliers, event: Dict[str, Any]) -> None:
    for k, v in event.get("rate_multipliers", {}).items():
        if "." in k:
            active.bg_rate[k] = float(v)
        else:
            active.flow_rate[k] = float(v)
    for fid, mult in event.get("latency_multipliers", {}).items():
        active.flow_latency[fid] = {"p50": float(mult["p50"]), "p95": float(mult["p95"])}


# ----------------------------
# Emission core
# ----------------------------
rows: List[Dict[str, Any]] = []
_seq = 0


def emit_row(ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    global _seq
    if trace_id and (len(trace_id) != 32 or not all(c in "0123456789abcdef" for c in trace_id)):
        raise ValueError(f"Invalid trace_id generated: {trace_id}")
    if len(message) > 1000:
        message = message[:997] + "..."
    rows.append(
        {
            "timestamp_dt": ts,
            "seq": _seq,
            "level": level,
            "message": message.replace("\n", " "),
            "trace_id": trace_id,
            "service": service or "",
            "host": host or "",
        }
    )
    _seq += 1


def component_identity(comp_id: str, host: str) -> Tuple[str, str]:
    comp = _component_by_id[comp_id]
    return (comp.get("svc") or "", host or "")


def choose_host_for_component(comp_id: str) -> str:
    hosts = _component_by_id[comp_id].get("hosts", [])
    if not hosts:
        return ""
    return str(RNG.choice(hosts))


def one_shot_narrative_overrides(ref: str, at_min: int) -> Dict[str, Any]:
    """
    Ensure operational/marker one-shots match the intended incident progression.
    Values are chosen within declared domains, but constrained to the narrative.
    """
    overrides: Dict[str, Any] = {}

    if ref == "bartender.feature_flag_update":
        # Event 2: operator disables the dependency.
        overrides.update(
            {
                "flag": "discovery_popcount_dependency",
                "old": "on",
                "new": "off",
                "actor": "sre_oncall",
            }
        )

    if ref == "accesspoint.firewall_block_applied":
        # Event 4: firewall blocks client ingress to Accesspoints (HTTPS).
        overrides.update(
            {
                "src": "eu_clients",
                "dst_port": 443,
                "ttl_s": 900,  # within [60,3600]
                "action": "DROP" if int(at_min) % 2 == 0 else "REJECT",
            }
        )

    if ref == "accesspoint.process_crash":
        # Event 3: I/O collapse -> watchdog kills / forced termination.
        overrides.update(
            {
                "signal": "SIGKILL",
                "reason": "io_stall" if int(at_min) >= 25 else "watchdog_timeout",
            }
        )

    if ref == "accesspoint.process_restart":
        overrides.update({"reason": "supervisor_restart"})

    if ref == "accesspoint.hard_reset":
        # Event 4: hard reset of unresponsive nodes.
        overrides.update({"by": "infra_automation", "reason": "unresponsive_io"})

    return overrides


def emit_one_shots(at_min: int, one_shots: List[Dict[str, Any]]) -> None:
    base = minute_start(at_min)
    for shot in one_shots:
        ref = shot["ref"]
        comp_id, log_id = split_ref(ref)
        hosts = shot.get("hosts") or _component_by_id[comp_id].get("hosts", []) or [""]
        count = int(shot["count"])

        base_overrides = one_shot_narrative_overrides(ref, at_min)

        for i in range(count):
            host = str(hosts[i % len(hosts)])
            ts = base + timedelta(milliseconds=int(RNG.integers(0, 2000)))

            # Allow per-line small variation while keeping the critical narrative fields pinned.
            overrides = dict(base_overrides)

            lvl, msg = render_message(comp_id, log_id, "f", context={}, overrides=overrides)
            service, host2 = component_identity(comp_id, host)
            emit_row(ts, lvl, msg, "", service, host2)


def simulate_background(minute: int, state: str, active: Optional[ActiveMultipliers]) -> None:
    for comp_id, comp in _component_by_id.items():
        emits = comp.get("beh", {}).get(state, {}).get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            base_rate = float(e["per_min"])
            scope = e.get("scope", "per_host")
            mult = 1.0
            if state == "f" and active is not None:
                mult = float(active.bg_rate.get(f"{comp_id}.{log_id}", 1.0))
            rate = base_rate * mult
            if rate <= 0.0:
                continue

            hosts = comp.get("hosts", []) or [""]
            if scope == "per_host":
                for host in hosts:
                    count = int(RNG.poisson(rate))
                    for _ in range(count):
                        ts = minute_start(minute) + timedelta(milliseconds=int(RNG.integers(0, 60000)))
                        lvl, msg = render_message(comp_id, log_id, state, context={}, overrides={})
                        service, host2 = component_identity(comp_id, host)
                        emit_row(ts, lvl, msg, "", service, host2)
            else:
                count = int(RNG.poisson(rate))
                for _ in range(count):
                    host = choose_host_for_component(comp_id)
                    ts = minute_start(minute) + timedelta(milliseconds=int(RNG.integers(0, 60000)))
                    lvl, msg = render_message(comp_id, log_id, state, context={}, overrides={})
                    service, host2 = component_identity(comp_id, host)
                    emit_row(ts, lvl, msg, "", service, host2)


def scaled_latency_pairs(flow_id: str, state: str, base_pairs: List[List[float]], active: Optional[ActiveMultipliers]) -> List[Tuple[float, float]]:
    if state != "f" or active is None:
        return [(float(p50), float(p95)) for p50, p95 in base_pairs]
    mult = active.flow_latency.get(flow_id, {"p50": 1.0, "p95": 1.0})
    return [(float(p50) * float(mult["p50"]), float(p95) * float(mult["p95"])) for p50, p95 in base_pairs]


def simulate_flow_instance(flow: Dict[str, Any], state: str, start_ts: datetime, active: Optional[ActiveMultipliers]) -> None:
    trace_id = _hex(32) if SYSTEM["tracing"]["on"] and flow.get("trace", False) else ""

    # Per-component host selection for the instance
    instance_hosts: Dict[str, str] = {}
    for comp_id in set(flow.get("path", [])):
        instance_hosts[comp_id] = choose_host_for_component(comp_id)

    # Shared coherent context
    req_id = _uuid4_det()
    user_id = int(RNG.integers(1000000, 9000000))
    track_uri = f"spotify:track:{_hex(16)}"
    playlist_uri = f"spotify:playlist:{_hex(16)}"
    client_ip = _ip()
    user_hint = _hex(16)
    subscriber_count = int(RNG.integers(0, 5001))

    context: Dict[str, Any] = {
        "req_id": req_id,
        "trace_id": trace_id,
        "user_id": user_id,
        "track_uri": track_uri,
        "playlist_uri": playlist_uri,
        "client_ip": client_ip,
        "user_hint": user_hint,
    }

    retry_cfg = flow.get("retry")
    attempts = sample_attempt_count(retry_cfg)
    base_emit = flow.get("emit", [])
    base_lat = flow.get("latency_ms", [])
    lat_pairs = scaled_latency_pairs(flow["id"], state, base_lat, active)

    current_attempt_start = start_ts

    # Track request log emission start for computing dur_ms later.
    op_start_times: Dict[str, datetime] = {}

    def record_op_start_if_req(ref: str, ts: datetime) -> None:
        if ref in ("accesspoint.playback_req", "accesspoint.login_req", "accesspoint.discovery_req", "accesspoint.popcount_req"):
            op_start_times[ref] = ts

    def compute_elapsed_ms(ref_req: str, ts_now: datetime) -> Optional[int]:
        if ref_req not in op_start_times:
            return None
        return int(round((ts_now - op_start_times[ref_req]).total_seconds() * 1000.0))

    for attempt_idx in range(1, attempts + 1):
        t = current_attempt_start

        # Per-attempt emit list
        for j, ref in enumerate(base_emit):
            c_id, l_id = split_ref(ref)
            p50, p95 = lat_pairs[j]
            delay_ms = lognormal_ms(p50, p95)
            t = t + timedelta(milliseconds=delay_ms)

            overrides: Dict[str, Any] = {}

            # Flow-specific coherence adjustments
            if ref == "playback_service.session_started":
                elapsed = compute_elapsed_ms("accesspoint.playback_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("startup_ms", {"v": [20, 12000]})["v"]
                    overrides["startup_ms"] = clamp_int(max(int(elapsed), 20), int(dom[0]), int(dom[1]))

            if ref == "bartender.discovery_rendered":
                elapsed = compute_elapsed_ms("accesspoint.discovery_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("dur_ms", {"v": [20, 20000]})["v"]
                    overrides["dur_ms"] = clamp_int(max(int(elapsed), 20), int(dom[0]), int(dom[1]))
                    if state == "f":
                        v = RNG.choice(["cache", "cache", "degraded_cache", "live"])
                        overrides["source"] = v.item() if hasattr(v, "item") else v

            if ref == "popcount.get_subscribers_ok":
                dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("dur_ms", {"v": [3, 20000]})["v"]
                overrides["dur_ms"] = clamp_int(int(max(3, round(delay_ms))), int(dom[0]), int(dom[1]))
                overrides["rows"] = subscriber_count
                overrides["playlist_uri"] = playlist_uri
                overrides["req_id"] = req_id

            if ref == "accesspoint.playback_resp":
                elapsed = compute_elapsed_ms("accesspoint.playback_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("dur_ms", {"v": [20, 12000]})["v"]
                    overrides["dur_ms"] = clamp_int(int(elapsed), int(dom[0]), int(dom[1]))
                if state == "n":
                    overrides["status"] = int(RNG.choice([200, 206]))
                    overrides["bytes"] = int(RNG.integers(200000, 2000000))
                else:
                    if flow["id"] == "playback_start_degraded" and RNG.random() < 0.04:
                        overrides["status"] = int(RNG.choice([503, 504]))
                        overrides["bytes"] = int(RNG.integers(0, 50000))
                    else:
                        overrides["status"] = int(RNG.choice([200, 206]))
                        overrides["bytes"] = int(RNG.integers(150000, 2000000))

            if ref == "accesspoint.login_resp":
                elapsed = compute_elapsed_ms("accesspoint.login_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("dur_ms", {"v": [30, 15000]})["v"]
                    overrides["dur_ms"] = clamp_int(int(elapsed), int(dom[0]), int(dom[1]))
                overrides["status"] = 200

            if ref == "accesspoint.login_timeout":
                elapsed = compute_elapsed_ms("accesspoint.login_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("vars", {}).get("wait_ms", {"v": [1000, 20000]})["v"]
                    overrides["wait_ms"] = clamp_int(int(elapsed), int(dom[0]), int(dom[1]))

            if ref == "accesspoint.discovery_resp":
                elapsed = compute_elapsed_ms("accesspoint.discovery_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("dur_ms", {"v": [10, 20000]})["v"]
                    overrides["dur_ms"] = clamp_int(int(elapsed), int(dom[0]), int(dom[1]))
                if state == "n":
                    overrides["status"] = 200
                    overrides["source"] = "cache"
                else:
                    if flow["id"] == "discovery_page_popcount_timeout":
                        if RNG.random() < 0.15:
                            overrides["status"] = 200
                            overrides["source"] = "degraded_cache"
                        else:
                            overrides["status"] = int(RNG.choice([503, 504]))
                            v = RNG.choice(["live", "degraded_cache"])
                            overrides["source"] = v.item() if hasattr(v, "item") else v
                    else:
                        overrides["status"] = 200
                        v = RNG.choice(["cache", "cache", "degraded_cache"])
                        overrides["source"] = v.item() if hasattr(v, "item") else v

            if ref == "accesspoint.popcount_resp":
                elapsed = compute_elapsed_ms("accesspoint.popcount_req", t)
                if elapsed is not None:
                    dom = _log_templates[(c_id, l_id)].get("state_vars", {}).get(state, {}).get("dur_ms", {"v": [5, 15000]})["v"]
                    overrides["dur_ms"] = clamp_int(int(elapsed), int(dom[0]), int(dom[1]))
                overrides["status"] = 200
                if state == "f" and flow["id"] == "popcount_subscribers_empty_fastfail":
                    overrides["subscriber_count"] = int(RNG.integers(0, 6))
                else:
                    overrides["subscriber_count"] = subscriber_count

            if ref == "bartender.popcount_call_failed":
                overrides["playlist_uri"] = playlist_uri
                overrides["req_id"] = req_id
                overrides["attempt_ms"] = clamp_int(int(round(delay_ms)), 50, 20000)
                v = RNG.choice(["deadline_exceeded", "deadline_exceeded", "over_capacity", "unavailable"])
                overrides["err"] = v.item() if hasattr(v, "item") else v

            if ref == "popcount.fast_fail_empty":
                overrides["playlist_uri"] = playlist_uri
                overrides["req_id"] = req_id
                v = RNG.choice(["over_capacity", "latency_budget_exceeded", "over_capacity"])
                overrides["reason"] = v.item() if hasattr(v, "item") else v

            if ref == "accesspoint.popcount_timeout_verbose":
                overrides["req_id"] = req_id
                overrides["queued"] = bias_range_int(0, 20000, bias=0.85)
                overrides["timeout_ms"] = clamp_int(int(round(delay_ms)), 1000, 20000)
                v = RNG.choice(["upstream_timeout", "context_deadline_exceeded", "upstream_timeout", "client_cancelled"])
                overrides["err"] = v.item() if hasattr(v, "item") else v
                overrides["stack_id"] = _hex(8)

            lvl, msg = render_message(c_id, l_id, state, context=context, overrides=overrides)
            service, host = component_identity(c_id, instance_hosts.get(c_id, ""))
            emit_row(t, lvl, msg, trace_id, service, host)
            record_op_start_if_req(ref, t)

        # Retry scheduling (if any)
        if attempt_idx < attempts and retry_cfg:
            backoffs = retry_cfg.get("backoff_ms") or []
            idx = attempt_idx - 1  # between attempt_idx and attempt_idx+1
            if idx < len(backoffs):
                b50, b95 = backoffs[idx]
                if float(b50) <= 0 and float(b95) <= 0:
                    backoff_ms = 0.0
                else:
                    backoff_ms = lognormal_ms(float(b50), float(b95), softcap_mult=3.0)
            else:
                backoff_ms = 0.0

            # Emit per-retry logs once for the upcoming attempt (attempt_idx+1)
            for retry_ref in retry_cfg.get("emit_per_retry", []):
                c_id, l_id = split_ref(retry_ref)
                ts = t + timedelta(milliseconds=int(RNG.integers(0, 10)))
                overrides = {"req_id": req_id}

                if "attempt" in _log_placeholders[(c_id, l_id)]:
                    overrides["attempt"] = attempt_idx + 1

                if retry_ref == "desktop_client.popcount_retry_no_backoff":
                    v = RNG.choice(["timeout", "timeout", "timeout", "server_error"])
                    overrides["reason"] = v.item() if hasattr(v, "item") else v

                if retry_ref == "desktop_client.client_backoff_scheduled":
                    overrides["op"] = "popcount_fetch"
                    sleep_dom = _log_templates[(c_id, l_id)]["vars"]["sleep_ms"]["v"]
                    overrides["sleep_ms"] = clamp_int(int(round(backoff_ms)), int(sleep_dom[0]), int(sleep_dom[1]))

                lvl, msg = render_message(c_id, l_id, state, context=context, overrides=overrides)
                service, host = component_identity(c_id, instance_hosts.get(c_id, ""))
                emit_row(ts, lvl, msg, trace_id, service, host)

            current_attempt_start = t + timedelta(milliseconds=backoff_ms + float(RNG.integers(0, 25)))


def simulate_flows(minute: int, state: str, active: Optional[ActiveMultipliers]) -> None:
    flows = SYSTEM["flows"][state]["req"]
    for flow in flows:
        base_rpm = float(flow["rpm"])
        mult = 1.0
        if state == "f" and active is not None:
            mult = float(active.flow_rate.get(flow["id"], 1.0))
        rpm = base_rpm * mult
        if rpm <= 0.0:
            continue
        count = int(RNG.poisson(rpm))
        for _ in range(count):
            start_ts = minute_start(minute) + timedelta(milliseconds=int(RNG.integers(0, 60000)))
            simulate_flow_instance(flow, state, start_ts, active)


# ----------------------------
# Run simulation
# ----------------------------
total_minutes = int(SCENARIO["scenario"]["time"]["total_minutes"])
n_end = int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"])

events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: int(e["order"]))
event_by_minute: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
for ev in events:
    event_by_minute[int(ev["at_min"])].append(ev)

active = init_active_multipliers()

for m in range(total_minutes):
    state = "n" if m < n_end else "f"

    if state == "f":
        for ev in event_by_minute.get(m, []):
            apply_event(active, ev)
            if "one_shots" in ev:
                emit_one_shots(m, ev["one_shots"])

    simulate_background(m, state, active if state == "f" else None)
    simulate_flows(m, state, active if state == "f" else None)

# ----------------------------
# Finalize CSV
# ----------------------------
df = pd.DataFrame(rows)
df.sort_values(["timestamp_dt", "seq"], inplace=True)

df["timestamp"] = df["timestamp_dt"].apply(dt_to_iso_ms)
out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

row_count = len(out)
if not (20000 <= row_count <= 100000):
    raise RuntimeError(f"Generated {row_count} rows, outside target [20000, 100000].")

out.to_csv("logs.csv", index=False)
