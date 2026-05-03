import math
import hashlib
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict

import numpy as np
import pandas as pd


# Fixed seeds for reproducibility (even though this simulator is mostly hash-deterministic).
random.seed(0)
np.random.seed(0)


SYSTEM: Dict[str, Any] = {
    "id": "platformsh_eu_routing_plane",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["gateway", "ops_runner"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "ops_runner",
            "svc": "ops-runner",
            "hosts": ["opsctl-1"],
            "logs": {
                "gateway_restart": {
                    "lvl": "INFO",
                    "msg": "Maintenance step: restarting gateways target={target} reason={reason}",
                    "vars": {
                        "target": {"k": "ch", "v": ["gateway_cluster"]},
                        "reason": {"k": "ch", "v": ["upgrade_apply"]},
                    },
                },
                "deploy_kazoo_patch": {
                    "lvl": "INFO",
                    "msg": "Deploy: orchestrator hotfix applied build={build} change={change}",
                    "vars": {
                        "build": {"k": "ch", "v": ["orch-2.7.4-hotfix1"]},
                        "change": {"k": "ch", "v": ["kazoo_startup_throttle_semaphore_1000"]},
                    },
                },
                "debug_strace_attached": {
                    "lvl": "INFO",
                    "msg": "Attached strace pid={pid} focus={focus}",
                    "vars": {
                        "pid": {"k": "i", "v": [1200, 9800]},
                        "focus": {"k": "ch", "v": ["zookeeper_client_io"]},
                    },
                },
                "strace_epipe_seen": {
                    "lvl": "INFO",
                    "msg": "strace observation pid={pid} syscall={syscall} errno={errno}",
                    "vars": {
                        "pid": {"k": "i", "v": [1200, 9800]},
                        "syscall": {"k": "ch", "v": ["write"]},
                        "errno": {"k": "ch", "v": ["EPIPE"]},
                    },
                },
                "zookeeper_maxbuffer_increase": {
                    "lvl": "INFO",
                    "msg": "ZooKeeper config change: jute.maxbuffer from={from_bytes} to={to_bytes}",
                    "vars": {
                        "from_bytes": {"k": "i", "v": [1048576, 1048576]},
                        "to_bytes": {"k": "i", "v": [4194304, 4194304]},
                    },
                },
                "zookeeper_restart": {
                    "lvl": "INFO",
                    "msg": "ZooKeeper restart initiated reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["apply_maxbuffer_change"]}},
                },
                "probe_start": {
                    "lvl": "INFO",
                    "msg": "Probe start url={url} trace={trace_id}",
                    "vars": {
                        "url": {"k": "ch", "v": ["https://app-backend.internal/health"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "Probe ok status={status} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "status": {"k": "i", "v": [200, 200]},
                        "dur_ms": {"k": "i", "v": [8, 80]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "gateway",
            "svc": "edge-gateway",
            "hosts": ["gw-eu-1", "gw-eu-2", "gw-eu-3", "gw-eu-4"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "Incoming {method} {host}{uri} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["shop.example.eu", "api.example.eu", "www.example.eu"]},
                        "uri": {"k": "ch", "v": ["/", "/api/v1/items", "/login", "/static/app.js"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_access": {
                    "lvl": "INFO",
                    "msg": "HTTP {method} {host}{uri} -> {status} dur_ms={dur_ms} upstream={upstream} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["shop.example.eu", "api.example.eu", "www.example.eu"]},
                        "uri": {"k": "ch", "v": ["/", "/api/v1/items", "/login", "/static/app.js"]},
                        "status": {"k": "i", "v": [200, 304]},
                        "dur_ms": {"k": "i", "v": [10, 220]},
                        "upstream": {"k": "ch", "v": ["app_backend"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_503_no_route_empty": {
                    "lvl": "WARN",
                    "msg": "HTTP {method} {host}{uri} -> 503 no_route dur_ms={dur_ms} route_table={route_table} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["shop.example.eu", "api.example.eu", "www.example.eu"]},
                        "uri": {"k": "ch", "v": ["/", "/api/v1/items", "/login", "/static/app.js"]},
                        "dur_ms": {"k": "i", "v": [1, 40]},
                        "route_table": {"k": "ch", "v": ["empty"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_503_no_route_partial": {
                    "lvl": "WARN",
                    "msg": "HTTP {method} {host}{uri} -> 503 no_route dur_ms={dur_ms} route_table={route_table} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["shop.example.eu", "api.example.eu", "www.example.eu"]},
                        "uri": {"k": "ch", "v": ["/", "/api/v1/items", "/login", "/static/app.js"]},
                        "dur_ms": {"k": "i", "v": [1, 60]},
                        "route_table": {"k": "ch", "v": ["partial"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "refresh_start": {
                    "lvl": "DEBUG",
                    "msg": "Route refresh start cached_gen={cached_gen}",
                    "vars": {"cached_gen": {"k": "i", "v": [1000, 1400]}},
                },
                "refresh_ok": {
                    "lvl": "INFO",
                    "msg": "Refreshed routes gen={gen} routes={routes} dur_ms={dur_ms}",
                    "vars": {
                        "gen": {"k": "i", "v": [1001, 1500]},
                        "routes": {"k": "i", "v": [5000, 12000]},
                        "dur_ms": {"k": "i", "v": [15, 180]},
                    },
                },
                "refresh_fail": {
                    "lvl": "ERROR",
                    "msg": "Route refresh failed err={err} dur_ms={dur_ms}",
                    "vars": {
                        "err": {"k": "ch", "v": ["upstream_503", "upstream_500"]},
                        "dur_ms": {"k": "i", "v": [200, 5000]},
                    },
                },
                "refresh_retry": {
                    "lvl": "WARN",
                    "msg": "Retrying route refresh attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [80, 1200]},
                    },
                },
                "gw_metrics": {
                    "lvl": "INFO",
                    "msg": "Gateway metrics route_count={route_count} refresh_age_s={refresh_age_s}",
                    "vars": {
                        "route_count": {"k": "i", "v": [5000, 12000]},
                        "refresh_age_s": {"k": "i", "v": [0, 300]},
                    },
                },
                "gw_metrics_degraded": {
                    "lvl": "INFO",
                    "msg": "Gateway metrics route_count={route_count} refresh_age_s={refresh_age_s}",
                    "vars": {
                        "route_count": {"k": "i", "v": [0, 50]},
                        "refresh_age_s": {"k": "i", "v": [60, 900]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "gw_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "gw_metrics", "per_min": 1.0, "scope": "per_host"},
                        {"id": "gw_metrics_degraded", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "orchestrator",
            "svc": "orchestrator",
            "hosts": ["orch-eu-1", "orch-eu-2"],
            "logs": {
                "serve_app_list_ok": {
                    "lvl": "INFO",
                    "msg": "Served app list gen={gen} apps={apps} dur_ms={dur_ms}",
                    "vars": {
                        "gen": {"k": "i", "v": [1001, 1500]},
                        "apps": {"k": "i", "v": [800, 4000]},
                        "dur_ms": {"k": "i", "v": [10, 120]},
                    },
                },
                "zk_fetch_fail": {
                    "lvl": "ERROR",
                    "msg": "Failed to read state from ZooKeeper reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["session_lost", "connection_closed"]}},
                },
                "zk_session_expired": {
                    "lvl": "WARN",
                    "msg": "ZooKeeper session expired; reconnecting session={session_id}",
                    "vars": {"session_id": {"k": "hex", "v": 16}},
                },
                "startup_waiting_locks": {
                    "lvl": "INFO",
                    "msg": "Startup waiting for ownership locks acquired={acquired} total={total}",
                    "vars": {
                        "acquired": {"k": "i", "v": [0, 1100000]},
                        "total": {"k": "i", "v": [900000, 1100000]},
                    },
                },
                "zk_jute_buffer_error": {
                    "lvl": "ERROR",
                    "msg": "ZooKeeper server rejected request: packet_len={packet_len} max_buffer={max_buffer}",
                    "vars": {
                        "packet_len": {"k": "i", "v": [1100000, 2500000]},
                        "max_buffer": {"k": "i", "v": [1048576, 1048576]},
                    },
                },
                "orch_metrics": {
                    "lvl": "INFO",
                    "msg": "Orchestrator metrics zk_state={zk_state} owned_locks={owned_locks}",
                    "vars": {"owned_locks": {"k": "i", "v": [0, 1100000]}},
                    "state_vars": {
                        "n": {"zk_state": {"k": "ch", "v": ["connected"]}},
                        "f": {"zk_state": {"k": "ch", "v": ["starting", "disconnected"]}},
                    },
                },
                "orch_metrics_recovered": {
                    "lvl": "INFO",
                    "msg": "Orchestrator metrics zk_state={zk_state} owned_locks={owned_locks}",
                    "vars": {
                        "zk_state": {"k": "ch", "v": ["connected"]},
                        "owned_locks": {"k": "i", "v": [700000, 1100000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "orch_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "orch_metrics", "per_min": 1.0, "scope": "per_host"},
                        {"id": "orch_metrics_recovered", "per_min": 1.0, "scope": "per_host"},
                        {"id": "zk_session_expired", "per_min": 0.4, "scope": "per_host"},
                        {"id": "startup_waiting_locks", "per_min": 1.0, "scope": "per_host"},
                        {"id": "zk_jute_buffer_error", "per_min": 4.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "zookeeper",
            "svc": "zookeeper",
            "hosts": ["zk-eu-1", "zk-eu-2", "zk-eu-3"],
            "logs": {
                "quorum_ok": {
                    "lvl": "INFO",
                    "msg": "Quorum OK leader={leader} zxid={zxid}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["zk-eu-1", "zk-eu-2", "zk-eu-3"]},
                        "zxid": {"k": "hex", "v": 8},
                    },
                },
                "packet_too_large": {
                    "lvl": "WARN",
                    "msg": "Rejecting request packetLen={packet_len} exceeds jute.maxbuffer={max_buffer}",
                    "vars": {
                        "packet_len": {"k": "i", "v": [1100000, 2500000]},
                        "max_buffer": {"k": "i", "v": [1048576, 1048576]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "quorum_ok", "per_min": 0.2, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "quorum_ok", "per_min": 0.2, "scope": "per_host"},
                        {"id": "packet_too_large", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "app_backend",
            "svc": "app-runtime",
            "hosts": ["app-eu-1", "app-eu-2", "app-eu-3", "app-eu-4"],
            "logs": {
                "app_access": {
                    "lvl": "INFO",
                    "msg": "Handled {method} {uri} status={status} dur_ms={dur_ms} app={app} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/", "/api/v1/items", "/login", "/health"]},
                        "status": {"k": "i", "v": [200, 200]},
                        "dur_ms": {"k": "i", "v": [3, 80]},
                        "app": {"k": "ch", "v": ["shop", "api", "www"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "app_metrics": {
                    "lvl": "INFO",
                    "msg": "App metrics healthy={healthy} p95_ms={p95_ms}",
                    "vars": {"healthy": {"k": "ch", "v": ["true"]}, "p95_ms": {"k": "i", "v": [20, 120]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "app_metrics", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "app_metrics", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
    ],
    "flows": {
        "n": [
            {
                "id": "user_web_request_ok",
                "rpm": 400.0,
                "emit": ["gateway.req_start", "app_backend.app_access", "gateway.http_access"],
                "latency_ms": [[1, 4], [8, 25], [1, 6]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "gateway_config_refresh_ok",
                "rpm": 60.0,
                "emit": ["gateway.refresh_start", "orchestrator.serve_app_list_ok", "gateway.refresh_ok"],
                "latency_ms": [[1, 4], [15, 80], [2, 12]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 1.1,
                    "emit_per_retry": ["gateway.refresh_retry"],
                    "backoff_ms": [[80, 300], [200, 600]],
                },
                "trace": False,
            },
            {
                "id": "ops_backend_probe",
                "rpm": 1.0,
                "emit": ["ops_runner.probe_start", "app_backend.app_access", "ops_runner.probe_ok"],
                "latency_ms": [[1, 4], [5, 30], [1, 3]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "user_web_request_503_empty",
                "rpm": 350.0,
                "emit": ["gateway.http_503_no_route_empty"],
                "latency_ms": [[2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "user_web_request_503_partial",
                "rpm": 350.0,
                "emit": ["gateway.http_503_no_route_partial"],
                "latency_ms": [[2, 35]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "user_web_request_ok_recovered",
                "rpm": 350.0,
                "emit": ["gateway.req_start", "app_backend.app_access", "gateway.http_access"],
                "latency_ms": [[1, 4], [8, 28], [1, 6]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "gateway_config_refresh_fail",
                "rpm": 60.0,
                "emit": ["gateway.refresh_start", "orchestrator.zk_fetch_fail", "gateway.refresh_fail"],
                "latency_ms": [[1, 4], [250, 1600], [2, 15]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 2.4,
                    "emit_per_retry": ["gateway.refresh_retry"],
                    "backoff_ms": [[120, 700], [300, 1200]],
                },
                "trace": False,
            },
            {
                "id": "gateway_config_refresh_ok_recovered",
                "rpm": 60.0,
                "emit": ["gateway.refresh_start", "orchestrator.serve_app_list_ok", "gateway.refresh_ok"],
                "latency_ms": [[1, 4], [20, 120], [2, 12]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 1.1,
                    "emit_per_retry": ["gateway.refresh_retry"],
                    "backoff_ms": [[80, 300], [200, 600]],
                },
                "trace": False,
            },
            {
                "id": "ops_backend_probe",
                "rpm": 2.0,
                "emit": ["ops_runner.probe_start", "app_backend.app_access", "ops_runner.probe_ok"],
                "latency_ms": [[1, 4], [5, 35], [1, 3]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "eu_gateway_restart_kazoo_zk_buffer_incident",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
    "events_f": [
        {
            "order": 1,
            "at_min": 20,
            "rate_multipliers": {
                "user_web_request_ok_recovered": 0.0,
                "user_web_request_503_partial": 0.0,
                "gateway_config_refresh_ok_recovered": 0.0,
                "gateway.gw_metrics": 0.0,
                "orchestrator.zk_jute_buffer_error": 0.0,
                "orchestrator.orch_metrics_recovered": 0.0,
                "zookeeper.packet_too_large": 0.0,
            },
            "latency_multipliers": {"gateway_config_refresh_fail": {"p50": 1.0, "p95": 1.0}},
            "one_shots": [{"ref": "ops_runner.gateway_restart", "count": 1, "hosts": ["opsctl-1"]}],
        },
        {
            "order": 2,
            "at_min": 27,
            "rate_multipliers": {
                "orchestrator.zk_session_expired": 6.0,
                "orchestrator.startup_waiting_locks": 2.0,
                "orchestrator.orch_metrics_recovered": 0.0,
            },
            "latency_multipliers": {"gateway_config_refresh_fail": {"p50": 1.8, "p95": 2.5}},
            "one_shots": [
                {"ref": "ops_runner.debug_strace_attached", "count": 1, "hosts": ["opsctl-1"]},
                {"ref": "ops_runner.strace_epipe_seen", "count": 1, "hosts": ["opsctl-1"]},
            ],
        },
        {
            "order": 3,
            "at_min": 37,
            "rate_multipliers": {
                "orchestrator.zk_session_expired": 0.6,
                "orchestrator.zk_jute_buffer_error": 1.0,
                "zookeeper.packet_too_large": 1.0,
            },
            "latency_multipliers": {"gateway_config_refresh_fail": {"p50": 0.9, "p95": 1.1}},
            "one_shots": [{"ref": "ops_runner.deploy_kazoo_patch", "count": 1, "hosts": ["opsctl-1"]}],
        },
        {
            "order": 4,
            "at_min": 45,
            "rate_multipliers": {
                "user_web_request_503_empty": 0.0,
                "user_web_request_503_partial": 0.05,
                "user_web_request_ok_recovered": 0.95,
                "gateway_config_refresh_fail": 0.1,
                "gateway_config_refresh_ok_recovered": 1.0,
                "gateway.gw_metrics": 1.0,
                "gateway.gw_metrics_degraded": 0.0,
                "orchestrator.orch_metrics": 0.0,
                "orchestrator.orch_metrics_recovered": 1.0,
                "orchestrator.zk_jute_buffer_error": 0.0,
                "zookeeper.packet_too_large": 0.0,
                "orchestrator.zk_session_expired": 0.2,
                "orchestrator.startup_waiting_locks": 0.2,
            },
            "latency_multipliers": {
                "gateway_config_refresh_ok_recovered": {"p50": 0.9, "p95": 0.9},
                "user_web_request_ok_recovered": {"p50": 1.0, "p95": 1.0},
                "user_web_request_503_partial": {"p50": 1.0, "p95": 1.0},
            },
            "one_shots": [
                {"ref": "ops_runner.zookeeper_maxbuffer_increase", "count": 1, "hosts": ["opsctl-1"]},
                {"ref": "ops_runner.zookeeper_restart", "count": 1, "hosts": ["opsctl-1"]},
            ],
        },
    ],
}


BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def ihash(*parts: Any) -> int:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def u01(*parts: Any) -> float:
    x = ihash(*parts) % 1_000_000
    return (x + 0.5) / 1_000_000.0


def hex_from(*parts: Any, length: int) -> str:
    s = "|".join(str(p) for p in parts)
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    if length <= len(h):
        return h[:length]
    out = h
    while len(out) < length:
        out += hashlib.sha256((out + s).encode("utf-8")).hexdigest()
    return out[:length]


# Acklam's inverse normal CDF approximation
def norm_ppf(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    q = p - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
        * q
        / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


def lognormal_ms(p50: float, p95: float, seed_parts: Tuple[Any, ...], cap_mult: float = 3.0) -> int:
    p50 = max(p50, 0.001)
    p95 = max(p95, p50)
    if abs(p95 - p50) < 1e-9:
        return max(1, int(round(p50)))
    z95 = 1.6448536269514722
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / z95
    u = u01("ln", *seed_parts)
    z = norm_ppf(u)
    x = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    x = min(x, cap)
    return max(1, int(round(x)))


def dt_from_min(minute: float) -> datetime:
    return BASE_TIME + timedelta(seconds=minute * 60.0)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


@dataclass
class LogDef:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Optional[Dict[str, Any]]


def build_indices(system: Dict[str, Any]):
    comp_by_id = {c["id"]: c for c in system["components"]}
    log_by_ref: Dict[str, LogDef] = {}
    for cid, c in comp_by_id.items():
        for lid, ld in c.get("logs", {}).items():
            log_by_ref[f"{cid}.{lid}"] = LogDef(
                component_id=cid,
                log_id=lid,
                lvl=ld["lvl"],
                msg=ld["msg"],
                vars=ld.get("vars", {}),
                state_vars=ld.get("state_vars"),
            )
    flows_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for f in system["flows"][st]:
            flows_by_state[st][f["id"]] = f
    return comp_by_id, log_by_ref, flows_by_state


COMP_BY_ID, LOG_BY_REF, FLOWS_BY_STATE = build_indices(SYSTEM)


def alloc_count(expected: float, key: str, carries: Dict[str, float]) -> int:
    if expected <= 1e-12:
        carries[key] = 0.0
        return 0
    c = carries.get(key, 0.0)
    x = expected + c
    n = int(math.floor(x + 1e-12))
    carries[key] = x - n
    return n


def schedule_times(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    dur = max(0.001, end_ts - start_ts)
    out = []
    for i in range(n):
        frac = (i + 0.5) / n
        base = start_ts + frac * dur
        jitter = (u01("jit", key, i) - 0.5) * 0.5  # +/-250ms
        t = base + jitter
        t = max(start_ts, min(end_ts - 1e-6, t))
        out.append(datetime.fromtimestamp(t, tz=timezone.utc))
    return out


def choose_from_list(values: List[Any], *seed_parts: Any) -> Any:
    if not values:
        return ""
    idx = ihash("ch", *seed_parts) % len(values)
    return values[idx]


def gen_from_domain(dom: Dict[str, Any], var_name: str, seed_parts: Tuple[Any, ...]) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        return choose_from_list(list(v), var_name, *seed_parts)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        if var_name == "status" and lo == 200 and hi == 304:
            return 200 if (ihash("status", *seed_parts) % 5) != 0 else 304
        return lo + (ihash("i", var_name, *seed_parts) % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if abs(hi - lo) < 1e-12:
            return lo
        x = lo + (hi - lo) * u01("f", var_name, *seed_parts)
        return float(f"{x:.3f}")
    if k == "hex":
        length = int(v)
        return hex_from(var_name, *seed_parts, length=length)
    if k == "uuid":
        return str(uuid.UUID(hex=hex_from(var_name, *seed_parts, length=32)))
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return f"{var_name}-{ihash('str', *seed_parts) % 100000}"
    return ""


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def get_int_bounds(ref: str, state: str, var_name: str) -> Optional[Tuple[int, int]]:
    ld = LOG_BY_REF.get(ref)
    if not ld:
        return None
    dom = None
    if ld.state_vars and state in ld.state_vars and var_name in ld.state_vars[state]:
        dom = ld.state_vars[state][var_name]
    elif var_name in ld.vars:
        dom = ld.vars[var_name]
    if not dom or dom.get("k") != "i":
        return None
    v = dom.get("v", [None, None])
    return int(v[0]), int(v[1])


def clamp_to_domain(ref: str, state: str, var_name: str, value: int) -> int:
    b = get_int_bounds(ref, state, var_name)
    if not b:
        return int(value)
    return clamp_int(int(value), b[0], b[1])


def special_background_overrides(component_id: str, log_id: str, state: str, ts: datetime, host: str) -> Dict[str, Any]:
    minute = int((ts - BASE_TIME).total_seconds() // 60)
    overrides: Dict[str, Any] = {}

    if component_id == "gateway" and log_id == "gw_metrics":
        lo, hi = 5000, 12000
        base = lo + (ihash("gw", host, minute) % (hi - lo + 1))
        age = ihash("age", host, minute) % 60
        overrides["route_count"] = base
        overrides["refresh_age_s"] = age

    if component_id == "gateway" and log_id == "gw_metrics_degraded":
        lo, hi = 0, 50
        base = lo + (ihash("gwdeg", host, minute) % (hi - lo + 1))
        age = 120 + (ihash("aged", host, minute) % 600)
        overrides["route_count"] = base
        overrides["refresh_age_s"] = clamp_int(age, 60, 900)

    if component_id == "orchestrator" and log_id == "startup_waiting_locks":
        total = 1100000
        prog = min(1.0, max(0.0, (minute - 20) / 30.0))
        jitter = int((u01("locks", host, minute) - 0.5) * 20000)
        acquired = int(prog * total) + jitter
        acquired = clamp_int(acquired, 0, total)
        overrides["total"] = total
        overrides["acquired"] = acquired

    if component_id == "orchestrator" and log_id == "orch_metrics":
        if state == "n":
            overrides["zk_state"] = "connected"
            overrides["owned_locks"] = 850000 + (ihash("owned", host, minute) % 150000)
        else:
            if 27 <= minute < 37:
                overrides["zk_state"] = "disconnected" if (ihash("zkst", host, minute) % 3) != 0 else "starting"
            else:
                overrides["zk_state"] = "starting" if (ihash("zkst", host, minute) % 4) != 0 else "disconnected"
            overrides["owned_locks"] = clamp_int(50000 + (ihash("ownedf", host, minute) % 250000), 0, 1100000)

    if component_id == "orchestrator" and log_id == "orch_metrics_recovered":
        overrides["zk_state"] = "connected"
        overrides["owned_locks"] = 850000 + (ihash("ownedr", host, minute) % 250000)

    if component_id == "zookeeper" and log_id == "quorum_ok":
        leader = ["zk-eu-1", "zk-eu-2", "zk-eu-3"][minute % 3]
        overrides["leader"] = leader
        overrides["zxid"] = hex_from("zxid", leader, minute, length=8)

    return overrides


def render_log(ref: str, state: str, bound: Dict[str, Any], ts: datetime, host: str) -> Tuple[str, str]:
    ld = LOG_BY_REF[ref]
    all_domains: Dict[str, Any] = {}
    if ld.state_vars and state in ld.state_vars:
        all_domains.update(ld.state_vars[state])
    all_domains.update(ld.vars)

    values: Dict[str, Any] = {}
    overrides = special_background_overrides(ld.component_id, ld.log_id, state, ts, host)
    for k, v in overrides.items():
        if k not in bound:
            values[k] = v

    seed_parts = (ref, state, fmt_ts(ts), host)
    for var_name, dom in all_domains.items():
        if var_name in bound:
            values[var_name] = bound[var_name]
        elif var_name in values:
            continue
        else:
            values[var_name] = gen_from_domain(dom, var_name, seed_parts)

    msg = ld.msg.format_map(values)
    return ld.lvl, msg


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, msg: str, trace_id: str, service: str, host: str):
    rows.append({"ts": ts, "timestamp": "", "level": level, "message": msg, "trace_id": trace_id, "service": service, "host": host})


def component_identity(component_id: str) -> Tuple[str, List[str]]:
    c = COMP_BY_ID[component_id]
    return c.get("svc", "") or "", c.get("hosts", []) or []


def parse_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


def active_failure_intervals(scenario: Dict[str, Any]) -> List[Tuple[int, int, Dict[str, float], Dict[str, Dict[str, float]]]]:
    f_start = scenario["time"]["phases"]["f"]["start_min"]
    f_end = scenario["time"]["phases"]["f"]["end_min"]
    events = sorted(scenario["events_f"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [f_start] + [e["at_min"] for e in events] + [f_end]
    boundaries = sorted(set(boundaries))

    cur_rate: Dict[str, float] = {}
    cur_lat: Dict[str, Dict[str, float]] = {}

    ev_by_min: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        ev_by_min[e["at_min"]].append(e)

    intervals: List[Tuple[int, int, Dict[str, float], Dict[str, Dict[str, float]]]] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        if s in ev_by_min:
            for e in sorted(ev_by_min[s], key=lambda x: x["order"]):
                for k, v in (e.get("rate_multipliers") or {}).items():
                    cur_rate[k] = float(v)
                for k, v in (e.get("latency_multipliers") or {}).items():
                    cur_lat[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        e = boundaries[i + 1]
        intervals.append((s, e, dict(cur_rate), dict(cur_lat)))
    return intervals


FAIL_INTERVALS = active_failure_intervals(SCENARIO)


def get_rate_multiplier(state: str, rate_mult: Dict[str, float], source_key: str) -> float:
    if state != "f":
        return 1.0
    return float(rate_mult.get(source_key, 1.0))


def get_latency_multiplier(state: str, lat_mult: Dict[str, Dict[str, float]], flow_id: str) -> Tuple[float, float]:
    if state != "f":
        return 1.0, 1.0
    m = lat_mult.get(flow_id)
    if not m:
        return 1.0, 1.0
    return float(m.get("p50", 1.0)), float(m.get("p95", 1.0))


def assign_attempts(n: int, expected_attempts: float, max_attempts: int, key: str) -> List[int]:
    if n <= 0:
        return []
    expected_attempts = max(1.0, min(float(expected_attempts), float(max_attempts)))
    lo = int(math.floor(expected_attempts))
    hi = int(math.ceil(expected_attempts))
    lo = min(lo, max_attempts)
    hi = min(hi, max_attempts)
    if hi == lo:
        return [lo] * n
    p = expected_attempts - lo
    exp_hi = p * n
    base = int(math.floor(exp_hi + 1e-12))
    frac = exp_hi - base
    extra = 1 if u01("attempt_round", key, n) < frac else 0
    num_hi = min(n, base + extra)

    out = [lo] * n
    if num_hi > 0:
        step = n / num_hi
        for k in range(num_hi):
            idx = int(round((k + 0.5) * step - 0.5))
            idx = clamp_int(idx, 0, n - 1)
            out[idx] = hi
    return out


def simulate_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, rate_mult: Dict[str, float], carries: Dict[str, float]):
    start_dt = dt_from_min(start_min)
    end_dt = dt_from_min(end_min)
    dur_min = end_min - start_min

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {}).get("emit", []) or []
        svc, hosts = component_identity(cid)
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult = get_rate_multiplier(state, rate_mult, f"{cid}.{log_id}")
            eff = per_min * mult
            if eff <= 0.0:
                if scope == "per_host":
                    for h in hosts:
                        carries[f"bg|{state}|{cid}.{log_id}|{h}"] = 0.0
                else:
                    carries[f"bg|{state}|{cid}.{log_id}|global"] = 0.0
                continue

            if scope == "global":
                key = f"bg|{state}|{cid}.{log_id}|global"
                expected = eff * dur_min
                n = alloc_count(expected, key, carries)
                times = schedule_times(start_dt, end_dt, n, key)
                for i, ts in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    lvl, msg = render_log(f"{cid}.{log_id}", state, {}, ts, host)
                    emit_row(rows, ts, lvl, msg, "", svc, host)
            else:
                for h in hosts:
                    key = f"bg|{state}|{cid}.{log_id}|{h}"
                    expected = eff * dur_min
                    n = alloc_count(expected, key, carries)
                    times = schedule_times(start_dt, end_dt, n, key)
                    for ts in times:
                        lvl, msg = render_log(f"{cid}.{log_id}", state, {}, ts, h)
                        emit_row(rows, ts, lvl, msg, "", svc, h)


def choose_user_request_fields(flow_id: str, inst_id: int) -> Tuple[str, str, str, str]:
    method = "GET" if (ihash("m", flow_id, inst_id) % 4) != 0 else "POST"
    host = ["shop.example.eu", "api.example.eu", "www.example.eu"][ihash("h", flow_id, inst_id) % 3]
    if "503" in flow_id:
        uri_choices = ["/", "/api/v1/items", "/login", "/static/app.js"]
    else:
        uri_choices = ["/", "/api/v1/items", "/login"]
    uri = uri_choices[ihash("u", flow_id, inst_id) % len(uri_choices)]
    app = "api" if ("api." in host or uri.startswith("/api")) else ("shop" if "shop." in host else "www")
    return method, host, uri, app


def choose_gateway_host(inst_id: int) -> str:
    hosts = COMP_BY_ID["gateway"]["hosts"]
    return hosts[inst_id % len(hosts)]


def choose_orch_host(inst_id: int) -> str:
    hosts = COMP_BY_ID["orchestrator"]["hosts"]
    return hosts[inst_id % len(hosts)]


def choose_app_host(inst_id: int) -> str:
    hosts = COMP_BY_ID["app_backend"]["hosts"]
    return hosts[inst_id % len(hosts)]


def fit_total_into_bounds(delays: List[int], mins: List[int], maxs: List[int], total_lo: int, total_hi: int) -> List[int]:
    min_sum = sum(mins)
    if total_hi < min_sum:
        total_hi = min_sum
    if total_lo < min_sum:
        total_lo = min_sum

    delays = [max(mins[i], min(int(delays[i]), maxs[i])) for i in range(len(delays))]
    total = sum(delays)

    if total > total_hi:
        need = total - total_hi
        while need > 0:
            best_j = None
            best_slack = 0
            for j in range(len(delays)):
                slack = delays[j] - mins[j]
                if slack > best_slack or (slack == best_slack and slack > 0 and (best_j is None or j > best_j)):
                    best_slack = slack
                    best_j = j
            if best_j is None or best_slack <= 0:
                break
            take = min(best_slack, need)
            delays[best_j] -= take
            need -= take

    total = sum(delays)
    if total < total_lo:
        need = total_lo - total
        for j in range(len(delays) - 1, -1, -1):
            slack = maxs[j] - delays[j]
            if slack <= 0:
                continue
            add = min(slack, need)
            delays[j] += add
            need -= add
            if need <= 0:
                break

    return delays


def determine_zk_reason(flow_start_min: int, flow_id: str, inst_global: int, attempt: int) -> str:
    # Bind once per (instance, attempt) so orchestrator.zk_fetch_fail.reason and gateway.refresh_fail.err agree.
    if 27 <= flow_start_min < 37:
        return "session_lost"
    if 37 <= flow_start_min < 45:
        return "connection_closed"
    return "connection_closed" if (ihash("zk_reason", flow_id, inst_global, attempt) % 3) != 0 else "session_lost"


def is_success_only_flow(flow: Dict[str, Any]) -> bool:
    """
    If a flow has retries but its emit chain contains only "start/success" logs and no explicit failure terminal,
    then multiple attempts should not emit multiple success terminals (that would read like "retry after success").
    Here we model intermediate attempts as "silent failures" that only emit the start log.
    """
    em = flow.get("emit", []) or []
    # Treat these tokens as evidence the attempt's failure is explicitly modeled.
    failure_tokens = ["fail", "503", "error"]
    for ref in em:
        low = ref.lower()
        if any(tok in low for tok in failure_tokens):
            return False
    return True


def simulate_flow_instances(
    rows: List[Dict[str, Any]],
    state: str,
    flow: Dict[str, Any],
    start_min: int,
    end_min: int,
    rate_mult: Dict[str, float],
    lat_mult: Dict[str, Dict[str, float]],
    carries: Dict[str, float],
    flow_inst_seq: List[int],
):
    flow_id = flow["id"]
    mult = get_rate_multiplier(state, rate_mult, flow_id)
    rpm = float(flow["rpm"]) * mult
    if rpm <= 0.0:
        carries[f"flow|{state}|{flow_id}"] = 0.0
        return

    dur_min = end_min - start_min
    expected_instances = rpm * dur_min
    n_instances = alloc_count(expected_instances, f"flow|{state}|{flow_id}", carries)
    if n_instances <= 0:
        return

    start_dt = dt_from_min(start_min)
    end_dt = dt_from_min(end_min)

    start_times = schedule_times(start_dt, end_dt, n_instances, f"flow_starts|{state}|{flow_id}|{start_min}-{end_min}")

    attempts = assign_attempts(
        n_instances,
        flow["retry"]["expected_attempts"],
        flow["retry"]["max_attempts"],
        f"attempts|{state}|{flow_id}|{start_min}-{end_min}",
    )

    p50m, p95m = get_latency_multiplier(state, lat_mult, flow_id)

    success_only = is_success_only_flow(flow) and (flow.get("retry", {}).get("max_attempts", 1) or 1) > 1

    for i in range(n_instances):
        inst_global = flow_inst_seq[0]
        flow_inst_seq[0] += 1
        trace_id = hex_from("trace", state, flow_id, inst_global, length=32) if flow.get("trace", False) else ""
        t = start_times[i]

        gw_host = choose_gateway_host(inst_global)
        orch_host = choose_orch_host(inst_global)
        app_host = choose_app_host(inst_global)
        ops_host = "opsctl-1"

        method, host, uri, app = choose_user_request_fields(flow_id, inst_global)

        cached_gen = 1000 + (ihash("cg", flow_id, inst_global) % 401)
        gen = cached_gen + 1 + (ihash("genj", flow_id, inst_global) % 7)
        gen = clamp_int(gen, 1001, 1500)
        routes = 5000 + (ihash("routes", flow_id, inst_global) % 7001)
        apps = 800 + (routes // 3) + (ihash("apps", flow_id, inst_global) % 200)
        apps = clamp_int(apps, 800, 4000)

        A = attempts[i]
        emit_refs_full = flow["emit"]
        N_full = len(emit_refs_full)

        flow_start_min = int((start_times[i] - BASE_TIME).total_seconds() // 60)

        for a in range(1, A + 1):
            # For retry attempts (2..A), emit emit_per_retry, then wait backoff_ms, then begin the next attempt.
            if a > 1:
                bo_pair = flow["retry"]["backoff_ms"][a - 2]
                bo = lognormal_ms(float(bo_pair[0]), float(bo_pair[1]), ("bo", state, flow_id, inst_global, a))
                for retry_ref in flow["retry"].get("emit_per_retry", []) or []:
                    if get_int_bounds(retry_ref, state, "backoff_ms"):
                        bo = clamp_to_domain(retry_ref, state, "backoff_ms", bo)
                        break

                for retry_ref in flow["retry"].get("emit_per_retry", []) or []:
                    rcid, _ = parse_ref(retry_ref)
                    svc, _hosts = component_identity(rcid)
                    rhost = gw_host if rcid == "gateway" else (ops_host if rcid == "ops_runner" else (orch_host if rcid == "orchestrator" else ""))
                    bound_retry = {"attempt": a, "backoff_ms": bo}
                    lvl, msg = render_log(retry_ref, state, bound_retry, t, rhost)
                    emit_row(rows, t, lvl, msg, trace_id if flow.get("trace", False) else "", svc, rhost)

                t = t + timedelta(milliseconds=bo)

            # If this is a success-only flow and we have retries, model intermediate attempts as silent failures:
            # emit only the first "start" log, then spend some time (unlogged), then retry.
            if success_only and A > 1 and a < A and N_full >= 1:
                ref0 = emit_refs_full[0]
                cid0, _ = parse_ref(ref0)
                svc0, _hosts0 = component_identity(cid0)
                emit_host0 = gw_host if cid0 == "gateway" else (orch_host if cid0 == "orchestrator" else (app_host if cid0 == "app_backend" else (ops_host if cid0 == "ops_runner" else "")))

                bound0: Dict[str, Any] = {}
                if ref0 == "gateway.refresh_start":
                    bound0.update({"cached_gen": cached_gen})
                lvl0, msg0 = render_log(ref0, state, bound0, t, emit_host0)
                emit_row(rows, t, lvl0, msg0, trace_id if flow.get("trace", False) else "", svc0, emit_host0)

                # Spend time roughly consistent with the next hop latency pair (if present), then allow retry.
                lat_pairs = flow["latency_ms"]
                if len(lat_pairs) > 1:
                    sp50 = float(lat_pairs[1][0]) * p50m
                    sp95 = float(lat_pairs[1][1]) * p95m
                else:
                    sp50 = float(lat_pairs[0][0]) * p50m
                    sp95 = float(lat_pairs[0][1]) * p95m
                spent = lognormal_ms(sp50, sp95, ("silent_attempt", state, flow_id, inst_global, a))
                t = t + timedelta(milliseconds=spent)
                continue

            # Bind per-attempt meaning-bearing context once.
            attempt_ctx: Dict[str, Any] = {}
            if "orchestrator.zk_fetch_fail" in emit_refs_full or "gateway.refresh_fail" in emit_refs_full:
                attempt_ctx["zk_reason"] = determine_zk_reason(flow_start_min, flow_id, inst_global, a)
                attempt_ctx["gw_err"] = "upstream_503" if attempt_ctx["zk_reason"] == "session_lost" else "upstream_500"

            emit_refs = emit_refs_full
            N = N_full

            # latency_ms pairs are deltas since the previous emitted log in the same attempt.
            lat_pairs = flow["latency_ms"]
            attempt_delays: List[int] = [0] * N
            if N == 1:
                sp50 = float(lat_pairs[0][0]) * p50m
                sp95 = float(lat_pairs[0][1]) * p95m
                attempt_delays[0] = lognormal_ms(sp50, sp95, ("lat", state, flow_id, inst_global, a, 0))
            else:
                attempt_delays[0] = 0
                for j in range(1, N):
                    sp50 = float(lat_pairs[j][0]) * p50m
                    sp95 = float(lat_pairs[j][1]) * p95m
                    attempt_delays[j] = lognormal_ms(sp50, sp95, ("lat", state, flow_id, inst_global, a, j))

            # Clamp per-step dur_ms-bearing intermediate logs (dur_ms is interpreted as per-step timing there).
            mins_sub: List[int] = []
            maxs_sub: List[int] = []
            if N > 1:
                for j in range(1, N):
                    b = get_int_bounds(emit_refs[j], state, "dur_ms")
                    if b and j < N - 1:
                        attempt_delays[j] = clamp_int(attempt_delays[j], b[0], b[1])
                        mins_sub.append(b[0])
                        maxs_sub.append(b[1])
                    else:
                        mins_sub.append(1)
                        maxs_sub.append(10**9)

            # Fit total duration to the terminal log's dur_ms domain when present.
            term_bounds = get_int_bounds(emit_refs[-1], state, "dur_ms")
            if term_bounds:
                if N == 1:
                    attempt_delays[0] = clamp_int(attempt_delays[0], term_bounds[0], term_bounds[1])
                else:
                    attempt_delays[1:] = fit_total_into_bounds(
                        attempt_delays[1:], mins=mins_sub, maxs=maxs_sub, total_lo=term_bounds[0], total_hi=term_bounds[1]
                    )

            total_gap = attempt_delays[0] if N == 1 else sum(attempt_delays[1:])

            # Emit attempt logs in encoded order for every attempt.
            t_attempt = t
            for j, ref in enumerate(emit_refs):
                cid, _ = parse_ref(ref)
                svc, _hosts = component_identity(cid)
                if cid == "gateway":
                    emit_host = gw_host
                elif cid == "orchestrator":
                    emit_host = orch_host
                elif cid == "app_backend":
                    emit_host = app_host
                elif cid == "ops_runner":
                    emit_host = ops_host
                else:
                    emit_host = ""

                if N == 1:
                    if j == 0:
                        t_attempt = t_attempt + timedelta(milliseconds=attempt_delays[0])
                else:
                    if j > 0:
                        t_attempt = t_attempt + timedelta(milliseconds=attempt_delays[j])

                bound: Dict[str, Any] = {}
                if ref == "gateway.req_start":
                    bound.update({"method": method, "host": host, "uri": uri, "trace_id": trace_id})
                elif ref == "app_backend.app_access":
                    step_ms = attempt_delays[j] if (N > 1 and j > 0) else max(1, attempt_delays[0])
                    dms = clamp_to_domain(ref, state, "dur_ms", step_ms)
                    if flow_id.startswith("ops_backend_probe"):
                        bound.update({"method": "GET", "uri": "/health", "status": 200, "dur_ms": dms, "app": "api", "trace_id": trace_id})
                    else:
                        bound.update(
                            {
                                "method": method,
                                "uri": uri if uri in ["/", "/api/v1/items", "/login"] else "/",
                                "status": 200,
                                "dur_ms": dms,
                                "app": app,
                                "trace_id": trace_id,
                            }
                        )
                elif ref == "gateway.http_access":
                    if method == "GET" and uri.startswith("/static") and (ihash("304", inst_global) % 2) == 0:
                        status = 304
                    else:
                        status = 200
                    dms = clamp_to_domain(ref, state, "dur_ms", total_gap)
                    bound.update({"method": method, "host": host, "uri": uri, "status": status, "dur_ms": dms, "upstream": "app_backend", "trace_id": trace_id})
                elif ref == "gateway.http_503_no_route_empty":
                    dms = clamp_to_domain(ref, state, "dur_ms", total_gap)
                    bound.update({"method": method, "host": host, "uri": uri, "dur_ms": dms, "route_table": "empty", "trace_id": trace_id})
                elif ref == "gateway.http_503_no_route_partial":
                    dms = clamp_to_domain(ref, state, "dur_ms", total_gap)
                    bound.update({"method": method, "host": host, "uri": uri, "dur_ms": dms, "route_table": "partial", "trace_id": trace_id})
                elif ref == "gateway.refresh_start":
                    bound.update({"cached_gen": cached_gen})
                elif ref == "orchestrator.serve_app_list_ok":
                    step_ms = attempt_delays[j] if (N > 1 and j > 0) else max(1, attempt_delays[0])
                    dms = clamp_to_domain(ref, state, "dur_ms", step_ms)
                    bound.update({"gen": gen, "apps": apps, "dur_ms": dms})
                elif ref == "gateway.refresh_ok":
                    dms = clamp_to_domain(ref, state, "dur_ms", total_gap)
                    bound.update({"gen": gen, "routes": routes, "dur_ms": dms})
                elif ref == "orchestrator.zk_fetch_fail":
                    bound.update({"reason": attempt_ctx.get("zk_reason", "connection_closed")})
                elif ref == "gateway.refresh_fail":
                    dms = clamp_to_domain(ref, state, "dur_ms", total_gap)
                    bound.update({"err": attempt_ctx.get("gw_err", "upstream_500"), "dur_ms": dms})
                elif ref == "ops_runner.probe_start":
                    bound.update({"url": "https://app-backend.internal/health", "trace_id": trace_id})
                elif ref == "ops_runner.probe_ok":
                    dms = clamp_to_domain(ref, state, "dur_ms", total_gap)
                    bound.update({"status": 200, "dur_ms": dms, "trace_id": trace_id})

                lvl, msg = render_log(ref, state, bound, t_attempt, emit_host)
                emit_row(rows, t_attempt, lvl, msg, trace_id if flow.get("trace", False) else "", svc, emit_host)

            # Attempt ends at the modeled end time.
            t = t_attempt


def emit_one_shots(rows: List[Dict[str, Any]], scenario: Dict[str, Any]):
    events = sorted(scenario["events_f"], key=lambda x: (x["at_min"], x["order"]))
    for idx, e in enumerate(events):
        at_dt = dt_from_min(e["at_min"])
        if idx + 1 < len(events):
            next_dt = dt_from_min(events[idx + 1]["at_min"])
        else:
            next_dt = dt_from_min(scenario["time"]["phases"]["f"]["end_min"])
        upper = min(at_dt + timedelta(seconds=60), next_dt)

        for os in e.get("one_shots", []) or []:
            ref = os["ref"]
            cid, _ = parse_ref(ref)
            svc, _hosts = component_identity(cid)
            hosts = os.get("hosts") or (COMP_BY_ID[cid].get("hosts") or [])
            count = int(os["count"])
            for i in range(count):
                jitter_ms = int(u01("osjit", ref, e["at_min"], i) * 2000)  # [0, 2000) ms
                ts = at_dt + timedelta(milliseconds=jitter_ms)
                if ts < at_dt:
                    ts = at_dt
                if ts >= upper:
                    ts = upper - timedelta(milliseconds=1)
                host = hosts[i % len(hosts)] if hosts else ""
                lvl, msg = render_log(ref, "f", {}, ts, host)
                emit_row(rows, ts, lvl, msg, "", svc, host)


def main():
    rows: List[Dict[str, Any]] = []
    carries: Dict[str, float] = defaultdict(float)
    flow_inst_seq = [0]

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]

    simulate_background(rows, "n", n_start, n_end, rate_mult={}, carries=carries)
    for flow in SYSTEM["flows"]["n"]:
        simulate_flow_instances(rows, "n", flow, n_start, n_end, rate_mult={}, lat_mult={}, carries=carries, flow_inst_seq=flow_inst_seq)

    for (s, e, rate_mult, lat_mult) in FAIL_INTERVALS:
        simulate_background(rows, "f", s, e, rate_mult=rate_mult, carries=carries)
        for flow in SYSTEM["flows"]["f"]:
            simulate_flow_instances(rows, "f", flow, s, e, rate_mult=rate_mult, lat_mult=lat_mult, carries=carries, flow_inst_seq=flow_inst_seq)

    emit_one_shots(rows, SCENARIO)

    df = pd.DataFrame(rows)
    df = df.sort_values("ts", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["ts"].apply(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
