import math
import hashlib
import re
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from statistics import NormalDist

SYSTEM: Dict[str, Any] = {
    "id": "quay_registry",
    "states": {"n": "normal", "f": "failure"},
    "components": {
        "ingress_router": {
            "svc": "openshift-router",
            "hosts": ["router-a", "router-b"],
            "logs": {
                "http_access": {
                    "lvl": "INFO",
                    "msg": "access method={method} uri={uri} status={status} dur_ms={dur_ms} bytes={bytes} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PUT", "DELETE"]},
                        "uri": {"k": "ch", "v": ["/v2/*", "/api/v1/appregistry/packages", "/api/v1/appregistry/blobs"]},
                        "dur_ms": {"k": "i", "v": [1, 70000]},
                        "bytes": {"k": "i", "v": [200, 6000000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": [200, 201, 202, 404]}},
                        "f": {"status": {"k": "ch", "v": [200, 201, 202, 404, 502, 503]}},
                    },
                },
                "router_health": {
                    "lvl": "INFO",
                    "msg": "router health ok upstream_p50_ms={up_p50_ms} upstream_p95_ms={up_p95_ms}",
                    "vars": {
                        "up_p50_ms": {"k": "i", "v": [1, 50]},
                        "up_p95_ms": {"k": "i", "v": [5, 300]},
                    },
                },
                "upstream_connect_warn": {
                    "lvl": "WARN",
                    "msg": "upstream connect warning upstream=quay_app err={err}",
                    "vars": {
                        "err": {"k": "ch", "v": ["connect_timeout", "connection_reset", "tls_handshake_failed"]},
                    },
                },
            },
            "beh": {
                "n": [
                    {"id": "router_health", "per_min": 0.5, "scope": "per_host"},
                    {"id": "upstream_connect_warn", "per_min": 0.02, "scope": "per_host"},
                ],
                "f": [
                    {"id": "router_health", "per_min": 0.5, "scope": "per_host"},
                    {"id": "upstream_connect_warn", "per_min": 0.25, "scope": "per_host"},
                ],
            },
        },
        "quay_app": {
            "svc": "quay-app",
            "hosts": ["quay-1", "quay-2", "quay-3", "quay-4", "quay-5", "quay-6", "quay-7", "quay-8"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start method={method} endpoint={endpoint} req_id={req_id} ua={ua} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PUT", "DELETE"]},
                        "endpoint": {"k": "ch", "v": ["registry_pull", "registry_push", "appreg_list_packages", "appreg_get_blobs"]},
                        "req_id": {"k": "uuid", "v": None},
                        "ua": {"k": "ch", "v": ["docker", "helm", "operatorhub", "quay_web"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "db_acquire": {
                    "lvl": "DEBUG",
                    "msg": "db acquire worker={worker} open_conns={open_conns}",
                    "vars": {"worker": {"k": "i", "v": [1, 32]}},
                    "state_vars": {
                        "n": {"open_conns": {"k": "i", "v": [0, 20]}},
                        "f": {"open_conns": {"k": "i", "v": [10, 80]}},
                    },
                },
                "db_query": {
                    "lvl": "INFO",
                    "msg": "db query endpoint={endpoint} op={op} rows={rows} db_ms={db_ms}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["registry_pull", "registry_push", "appreg_list_packages", "appreg_get_blobs"]},
                        "op": {"k": "ch", "v": ["select", "insert", "update"]},
                    },
                    "state_vars": {
                        "n": {"rows": {"k": "i", "v": [0, 1200]}, "db_ms": {"k": "i", "v": [1, 500]}},
                        "f": {"rows": {"k": "i", "v": [0, 5000]}, "db_ms": {"k": "i", "v": [50, 60000]}},
                    },
                },
                "req_end": {
                    "lvl": "INFO",
                    "msg": "req done req_id={req_id} status={status} total_ms={total_ms} db_ms={db_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "total_ms": {"k": "i", "v": [1, 70000]}, "db_ms": {"k": "i", "v": [1, 60000]}},
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": [200, 201, 202, 404]}},
                        "f": {"status": {"k": "ch", "v": [200, 201, 404]}},
                    },
                },
                "req_error": {
                    "lvl": "WARN",
                    "msg": "req fail req_id={req_id} status={status} err={err} total_ms={total_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "err": {"k": "ch", "v": ["db_connect_refused", "db_pool_exhausted", "readonly_mode"]}, "total_ms": {"k": "i", "v": [1, 70000]}},
                    "state_vars": {"n": {"status": {"k": "ch", "v": [400, 401, 403, 404, 429]}}, "f": {"status": {"k": "ch", "v": [502, 503]}}},
                },
                "req_error_db": {
                    "lvl": "WARN",
                    "msg": "req fail req_id={req_id} status={status} err={err} total_ms={total_ms}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "err": {"k": "ch", "v": ["db_lock_timeout", "upstream_reset"]}, "total_ms": {"k": "i", "v": [1, 70000]}},
                    "state_vars": {"n": {"status": {"k": "ch", "v": [500]}}, "f": {"status": {"k": "ch", "v": [502, 503]}}},
                },
                "worker_metric": {
                    "lvl": "INFO",
                    "msg": "workers busy={busy} total={total} greenlets={greenlets} qlen={qlen}",
                    "vars": {"busy": {"k": "i", "v": [0, 32]}, "total": {"k": "i", "v": [8, 32]}, "greenlets": {"k": "i", "v": [50, 4000]}, "qlen": {"k": "i", "v": [0, 2000]}},
                },
                "gc_run": {
                    "lvl": "INFO",
                    "msg": "namespace_gc run deleted={deleted} db_ms={db_ms}",
                    "vars": {"deleted": {"k": "i", "v": [0, 200]}, "db_ms": {"k": "i", "v": [20, 20000]}},
                },
                "pod_restart": {
                    "lvl": "WARN",
                    "msg": "pod restart reason={reason} generation={generation}",
                    "vars": {"reason": {"k": "ch", "v": ["oom_kill", "liveness_probe_failed", "manual_bounce"]}, "generation": {"k": "i", "v": [1, 50]}},
                },
                "mode_changed": {
                    "lvl": "INFO",
                    "msg": "config applied read_only={read_only} db_conn_limit={db_conn_limit}",
                    "vars": {"read_only": {"k": "ch", "v": ["true"]}, "db_conn_limit": {"k": "i", "v": [30, 60]}},
                },
            },
            "beh": {"n": [{"id": "worker_metric", "per_min": 1.0, "scope": "per_host"}, {"id": "gc_run", "per_min": 0.4, "scope": "per_host"}],
                    "f": [{"id": "worker_metric", "per_min": 2.0, "scope": "per_host"}, {"id": "gc_run", "per_min": 0.6, "scope": "per_host"}]},
        },
        "mysql_rds": {
            "svc": "rds-mysql",
            "hosts": ["rds-1"],
            "logs": {
                "conn_stats_n": {
                    "lvl": "INFO",
                    "msg": "mysql stats current_conns={current} max_conns={max} threads_running={threads} slow_qpm={slow_qpm}",
                    "vars": {"current": {"k": "i", "v": [35, 65]}, "max": {"k": "i", "v": [180, 260]}, "threads": {"k": "i", "v": [5, 35]}, "slow_qpm": {"k": "i", "v": [0, 10]}},
                },
                "conn_stats_high": {
                    "lvl": "WARN",
                    "msg": "mysql stats current_conns={current} max_conns={max} threads_running={threads} slow_qpm={slow_qpm}",
                    "vars": {"max": {"k": "i", "v": [260, 260]}, "current": {"k": "i", "v": [180, 260]}, "threads": {"k": "i", "v": [80, 260]}, "slow_qpm": {"k": "i", "v": [30, 200]}},
                },
                "conn_stats_capped": {
                    "lvl": "INFO",
                    "msg": "mysql stats current_conns={current} max_conns={max} threads_running={threads} slow_qpm={slow_qpm}",
                    "vars": {"max": {"k": "i", "v": [110, 110]}, "current": {"k": "i", "v": [70, 108]}, "threads": {"k": "i", "v": [40, 108]}, "slow_qpm": {"k": "i", "v": [10, 120]}},
                },
                "lock_wait": {
                    "lvl": "WARN",
                    "msg": "innodb lock wait timeout trx={trx} wait_ms={wait_ms}",
                    "vars": {"trx": {"k": "hex", "v": 16}, "wait_ms": {"k": "i", "v": [1000, 60000]}},
                },
                "too_many_conn": {
                    "lvl": "ERROR",
                    "msg": "too many connections refused user={user} src={src_ip}",
                    "vars": {"user": {"k": "ch", "v": ["quay"]}, "src_ip": {"k": "ip", "v": "10.0.0.0/16"}},
                },
                "set_max_connections": {"lvl": "INFO", "msg": "set global max_connections={max}", "vars": {"max": {"k": "i", "v": [110, 110]}}},
                "instance_restart": {"lvl": "CRITICAL", "msg": "rds instance restart initiated reason={reason}", "vars": {"reason": {"k": "ch", "v": ["lockup_recovery", "maintenance"]}}},
            },
            "beh": {
                "n": [{"id": "conn_stats_n", "per_min": 1.0, "scope": "global"}, {"id": "lock_wait", "per_min": 0.1, "scope": "global"}, {"id": "too_many_conn", "per_min": 0.01, "scope": "global"}],
                "f": [{"id": "conn_stats_high", "per_min": 1.0, "scope": "global"}, {"id": "conn_stats_capped", "per_min": 1.0, "scope": "global"}, {"id": "lock_wait", "per_min": 5.0, "scope": "global"}, {"id": "too_many_conn", "per_min": 3.0, "scope": "global"}],
            },
        },
    },
    "tracing": {"on": True, "origins": ["ingress_router"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": [
            {"id": "docker_pull_ok_n", "rpm": 90, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[1, 3], [2, 8], [10, 60], [2, 10], [1, 3]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_push_ok_n", "rpm": 20, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[1, 3], [2, 10], [20, 150], [3, 15], [1, 3]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "appreg_list_packages_ok_n", "rpm": 5, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[1, 3], [2, 10], [30, 200], [3, 20], [1, 3]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "appreg_get_blobs_ok_n", "rpm": 5, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[1, 3], [2, 10], [40, 250], [3, 20], [1, 3]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
        ],
        "f": [
            {"id": "appreg_list_packages_slow_f", "rpm": 120, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[2, 6], [10, 80], [800, 45000], [10, 200], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "appreg_get_blobs_slow_f", "rpm": 90, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[2, 6], [10, 90], [1200, 60000], [10, 250], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "appreg_502_f", "rpm": 35, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_error_db", "ingress_router.http_access"], "latency_ms": [[2, 6], [10, 120], [1500, 65000], [5, 60], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "appreg_502_conn_refused_f", "rpm": 15, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.req_error", "ingress_router.http_access"], "latency_ms": [[2, 6], [5, 50], [20, 500], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_pull_ok_f", "rpm": 70, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_end", "ingress_router.http_access"], "latency_ms": [[2, 5], [8, 60], [60, 4000], [5, 60], [2, 5]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_pull_502_f", "rpm": 40, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_error_db", "ingress_router.http_access"], "latency_ms": [[2, 5], [10, 90], [500, 35000], [5, 50], [2, 5]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_pull_502_conn_refused_f", "rpm": 20, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.req_error", "ingress_router.http_access"], "latency_ms": [[2, 5], [5, 50], [20, 500], [2, 5]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_push_502_db_f", "rpm": 15, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.db_query", "quay_app.req_error_db", "ingress_router.http_access"], "latency_ms": [[2, 6], [10, 120], [800, 45000], [5, 60], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_push_502_conn_refused_f", "rpm": 10, "emit": ["quay_app.req_start", "quay_app.db_acquire", "quay_app.req_error", "ingress_router.http_access"], "latency_ms": [[2, 6], [5, 60], [30, 700], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
            {"id": "docker_push_503_readonly_f", "rpm": 25, "emit": ["quay_app.req_start", "quay_app.req_error", "ingress_router.http_access"], "latency_ms": [[2, 6], [2, 15], [2, 6]], "retry": {"max_attempts": 1, "expected_attempts": 1.0}, "trace": True},
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "quay_app_registry_connection_storm_may28",
    "time": {"total_minutes": 44, "phases": {"n": {"start_min": 0, "end_min": 22}, "f": {"start_min": 22, "end_min": 44}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 22,
                    "rate_multipliers": {
                        "mysql_rds.conn_stats_high": 1.0,
                        "mysql_rds.conn_stats_capped": 0.0,
                        "mysql_rds.lock_wait": 0.05,
                        "mysql_rds.too_many_conn": 0.05,
                        "quay_app.gc_run": 1.0,
                        "docker_push_503_readonly_f": 0.0,
                        "appreg_502_f": 0.5,
                        "appreg_502_conn_refused_f": 0.3,
                        "docker_pull_502_f": 0.4,
                        "docker_pull_502_conn_refused_f": 0.3,
                        "docker_push_502_db_f": 0.4,
                        "docker_push_502_conn_refused_f": 0.3,
                    },
                    "latency_multipliers": {
                        "appreg_list_packages_slow_f": {"p50": 1.0, "p95": 1.0},
                        "appreg_get_blobs_slow_f": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 28,
                    "rate_multipliers": {
                        "appreg_502_f": 1.6,
                        "appreg_502_conn_refused_f": 1.8,
                        "docker_pull_502_f": 1.4,
                        "docker_pull_502_conn_refused_f": 1.6,
                        "docker_push_502_db_f": 1.3,
                        "docker_push_502_conn_refused_f": 1.4,
                        "mysql_rds.lock_wait": 2.0,
                        "mysql_rds.too_many_conn": 2.5,
                    },
                    "latency_multipliers": {
                        "appreg_list_packages_slow_f": {"p50": 1.6, "p95": 1.3},
                        "appreg_get_blobs_slow_f": {"p50": 1.7, "p95": 1.2},
                        "docker_pull_ok_f": {"p50": 1.5, "p95": 1.4},
                        "docker_pull_502_f": {"p50": 1.5, "p95": 1.6},
                    },
                    "one_shots": [
                        {"ref": "quay_app.pod_restart", "count": 8, "hosts": ["quay-1", "quay-2", "quay-3", "quay-4", "quay-5", "quay-6", "quay-7", "quay-8"]},
                        {"ref": "mysql_rds.instance_restart", "count": 1, "hosts": ["rds-1"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 35,
                    "rate_multipliers": {
                        "mysql_rds.conn_stats_high": 0.0,
                        "mysql_rds.conn_stats_capped": 1.0,
                        "mysql_rds.lock_wait": 0.2,
                        "mysql_rds.too_many_conn": 0.3,
                        "quay_app.gc_run": 0.0,
                        "appreg_502_f": 0.4,
                        "appreg_502_conn_refused_f": 0.2,
                        "docker_pull_502_f": 0.6,
                        "docker_pull_502_conn_refused_f": 0.2,
                        "docker_push_502_db_f": 0.0,
                        "docker_push_502_conn_refused_f": 0.0,
                        "docker_push_503_readonly_f": 1.0,
                    },
                    "latency_multipliers": {
                        "appreg_list_packages_slow_f": {"p50": 1.2, "p95": 1.1},
                        "appreg_get_blobs_slow_f": {"p50": 1.3, "p95": 1.1},
                        "docker_pull_ok_f": {"p50": 1.2, "p95": 1.2},
                    },
                    "one_shots": [
                        {"ref": "mysql_rds.set_max_connections", "count": 1, "hosts": ["rds-1"]},
                        {"ref": "quay_app.mode_changed", "count": 1, "hosts": ["quay-1"]},
                    ],
                },
            ]
        }
    },
}

SEED = 13371337
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

random.seed(SEED)
np.random.seed(SEED)

PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")

_NORMAL_INV = NormalDist()


def stable_hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16)


def hex_n(seed: str, n: int) -> str:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()
    if n <= 32:
        return h[:n]
    out = h
    while len(out) < n:
        h = hashlib.md5((seed + ":" + out).encode("utf-8")).hexdigest()
        out += h
    return out[:n]


def uuid_from_seed(seed: str) -> str:
    h = hex_n(seed, 32)
    time_low = h[0:8]
    time_mid = h[8:12]
    time_hi = list(h[12:16])
    time_hi[0] = "4"
    time_hi = "".join(time_hi)
    clk_seq = list(h[16:20])
    clk_seq[0] = format((int(clk_seq[0], 16) & 0x3) | 0x8, "x")
    clk_seq = "".join(clk_seq)
    node = h[20:32]
    return f"{time_low}-{time_mid}-{time_hi}-{clk_seq}-{node}"


def ip_from_cidr_seed(cidr: str, seed: str) -> str:
    if cidr != "10.0.0.0/16":
        x = stable_hash_int(seed) % 254 + 1
        y = (stable_hash_int(seed + ":b") % 254) + 1
        return f"10.0.{x}.{y}"
    h = stable_hash_int(seed)
    a = (h >> 8) & 0xFF
    b = h & 0xFF
    if b == 0:
        b = 1
    return f"10.0.{a}.{b}"


def quantile_u(seed: str) -> float:
    h = stable_hash_int(seed) % 1000000
    return (h + 0.5) / 1000001.0


def sample_lognormal_ms(p50: float, p95: float, seed: str) -> int:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.6448536269514722
    u = 0.50 + 0.45 * quantile_u(seed)
    z = _NORMAL_INV.inv_cdf(u)
    x = math.exp(mu + sigma * z)
    soft_cap = 2.5 * p95
    x = min(x, soft_cap)
    return max(1, int(round(x)))


def fmt_ts(dt: datetime) -> str:
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:23] + "Z"


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]


def build_templates(system: Dict[str, Any]) -> Dict[str, LogTemplate]:
    out: Dict[str, LogTemplate] = {}
    for cid, comp in system["components"].items():
        for lid, tpl in comp["logs"].items():
            ref = f"{cid}.{lid}"
            out[ref] = LogTemplate(
                component_id=cid,
                log_id=lid,
                lvl=tpl["lvl"],
                msg=tpl["msg"],
                vars=tpl.get("vars", {}),
                state_vars=tpl.get("state_vars", {}),
            )
    return out


TEMPLATES = build_templates(SYSTEM)


def get_domain(tpl: LogTemplate, state: str, var: str) -> Optional[Dict[str, Any]]:
    if var in tpl.state_vars.get(state, {}):
        return tpl.state_vars[state][var]
    return tpl.vars.get(var)


def choose_from_domain(domain: Dict[str, Any], seed: str) -> Any:
    k = domain["k"]
    v = domain.get("v")
    h = stable_hash_int(seed)
    if k == "ch":
        arr = list(v)
        return arr[h % len(arr)]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        return lo + (h % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = quantile_u(seed)
        return lo + (hi - lo) * u
    if k == "uuid":
        return uuid_from_seed(seed)
    if k == "hex":
        n = int(v)
        return hex_n(seed, n)
    if k == "ip":
        return ip_from_cidr_seed(str(v), seed)
    if k == "str":
        return str(v) if v is not None else ""
    return ""


def render_message(tpl: LogTemplate, state: str, binding: Dict[str, Any], seed_prefix: str) -> str:
    needed = set(PLACEHOLDER_RE.findall(tpl.msg))
    vals: Dict[str, Any] = {}
    for var in needed:
        if var in binding:
            vals[var] = binding[var]
            continue
        domain = get_domain(tpl, state, var)
        if domain is None:
            vals[var] = ""
            continue
        vals[var] = choose_from_domain(domain, f"{seed_prefix}:{tpl.component_id}.{tpl.log_id}:{var}")
    return tpl.msg.format(**vals)


class CarryRounding:
    def __init__(self):
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        expected = float(expected)
        if expected <= 0:
            return 0
        base = math.floor(expected)
        frac = expected - base
        c = self.carry.get(key, 0.0) + frac
        add = int(c >= 1.0)
        if add:
            c -= 1.0
        self.carry[key] = c
        return int(base + add)


def schedule_uniform(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end - start).total_seconds()
    if dur_s <= 0:
        return []
    out: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        t = start + timedelta(seconds=dur_s * frac)
        j = (stable_hash_int(f"{SEED}:{key}:{i}") % 241) - 120
        t = t + timedelta(milliseconds=j)
        if t < start:
            t = start + timedelta(milliseconds=1)
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def endpoint_for_flow(flow_id: str) -> str:
    if flow_id.startswith("docker_pull"):
        return "registry_pull"
    if flow_id.startswith("docker_push"):
        return "registry_push"
    if "list_packages" in flow_id:
        return "appreg_list_packages"
    if "get_blobs" in flow_id:
        return "appreg_get_blobs"
    if flow_id.startswith("appreg_"):
        return "appreg_list_packages"
    return "registry_pull"


def uri_for_endpoint(endpoint: str) -> str:
    if endpoint in ("registry_pull", "registry_push"):
        return "/v2/*"
    if endpoint == "appreg_list_packages":
        return "/api/v1/appregistry/packages"
    if endpoint == "appreg_get_blobs":
        return "/api/v1/appregistry/blobs"
    return "/v2/*"


def method_for_endpoint(endpoint: str) -> str:
    if endpoint == "registry_pull":
        return "GET"
    if endpoint == "registry_push":
        return "PUT"
    if endpoint.startswith("appreg_"):
        return "GET"
    return "GET"


def op_for_endpoint(endpoint: str, success: bool) -> str:
    if endpoint == "registry_push":
        return "insert" if success else "update"
    if endpoint == "registry_pull":
        return "select"
    if endpoint.startswith("appreg_"):
        return "select"
    return "select"


def bytes_for_endpoint(endpoint: str, seed: str) -> int:
    h = stable_hash_int(seed)
    if endpoint == "registry_pull":
        lo, hi = 50_000, 6_000_000
        return lo + (h % (hi - lo + 1))
    if endpoint == "registry_push":
        lo, hi = 80_000, 4_000_000
        return lo + (h % (hi - lo + 1))
    lo, hi = 800, 250_000
    return lo + (h % (hi - lo + 1))


def choose_host(component_id: str, seed: str) -> str:
    hosts = SYSTEM["components"][component_id].get("hosts", [])
    if not hosts:
        return ""
    return hosts[stable_hash_int(seed) % len(hosts)]


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))


def apply_latency_multiplier(pair: List[float], mul: Dict[str, float]) -> Tuple[float, float]:
    p50, p95 = float(pair[0]), float(pair[1])
    if not mul:
        return p50, p95
    return p50 * float(mul.get("p50", 1.0)), p95 * float(mul.get("p95", 1.0))


def build_failure_intervals() -> List[Dict[str, Any]]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart <= e["at_min"] <= fend}) + [fend]
    boundaries = sorted(boundaries)
    current_rate: Dict[str, float] = {}
    current_lat: Dict[str, Dict[str, float]] = {}
    intervals: List[Dict[str, Any]] = []

    ev_idx = 0
    for i in range(len(boundaries) - 1):
        start_min = boundaries[i]
        end_min = boundaries[i + 1]
        while ev_idx < len(events) and events[ev_idx]["at_min"] <= start_min:
            ev = events[ev_idx]
            for k, v in ev.get("rate_multipliers", {}).items():
                current_rate[k] = float(v)
            for k, v in ev.get("latency_multipliers", {}).items():
                current_lat[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
            ev_idx += 1
        intervals.append(
            {
                "start_min": start_min,
                "end_min": end_min,
                "rate_multipliers": dict(current_rate),
                "latency_multipliers": dict(current_lat),
            }
        )
    return intervals


FAILURE_INTERVALS = build_failure_intervals()


def get_rate_mul(interval: Dict[str, Any], source_key: str) -> float:
    return float(interval["rate_multipliers"].get(source_key, 1.0))


def get_lat_mul(interval: Dict[str, Any], flow_id: str) -> Dict[str, float]:
    return interval["latency_multipliers"].get(flow_id, {"p50": 1.0, "p95": 1.0})


def special_background_binding(component_id: str, log_id: str, state: str, seed: str) -> Dict[str, Any]:
    if component_id == "mysql_rds" and log_id in ("conn_stats_n", "conn_stats_high", "conn_stats_capped"):
        tpl = TEMPLATES[f"{component_id}.{log_id}"]
        dom_max = tpl.vars["max"]["v"]
        dom_cur = tpl.vars["current"]["v"]
        dom_thr = tpl.vars["threads"]["v"]
        dom_slow = tpl.vars["slow_qpm"]["v"]

        maxv = choose_from_domain({"k": "i", "v": dom_max}, seed + ":max")
        curv = choose_from_domain({"k": "i", "v": dom_cur}, seed + ":cur")
        curv = min(curv, maxv)
        thrv = choose_from_domain({"k": "i", "v": dom_thr}, seed + ":thr")
        thrv = min(thrv, curv)
        slow = choose_from_domain({"k": "i", "v": dom_slow}, seed + ":slow")

        return {"max": maxv, "current": curv, "threads": thrv, "slow_qpm": slow}

    if component_id == "quay_app" and log_id == "worker_metric":
        total = 32 if state == "f" else 24
        if state == "f":
            busy = clamp_int(int(round(total * (0.75 + 0.2 * quantile_u(seed + ":busy")))), 0, 32)
            greenlets = clamp_int(int(round(1000 + 2500 * quantile_u(seed + ":g"))), 50, 4000)
            qlen = clamp_int(int(round(200 + 1500 * quantile_u(seed + ":q"))), 0, 2000)
        else:
            busy = clamp_int(int(round(total * (0.20 + 0.35 * quantile_u(seed + ":busy")))), 0, 32)
            greenlets = clamp_int(int(round(200 + 900 * quantile_u(seed + ":g"))), 50, 4000)
            qlen = clamp_int(int(round(0 + 120 * quantile_u(seed + ":q"))), 0, 2000)
        return {"total": total, "busy": busy, "greenlets": greenlets, "qlen": qlen}

    return {}


def simulate_flow_instance(
    flow: Dict[str, Any],
    state: str,
    start_ts: datetime,
    interval: Optional[Dict[str, Any]],
    instance_ordinal: int,
) -> List[Dict[str, Any]]:
    flow_id = flow["id"]
    emit_refs = flow["emit"]
    latency_pairs = flow["latency_ms"]
    assert len(emit_refs) == len(latency_pairs)

    lat_mul = {"p50": 1.0, "p95": 1.0}
    if state == "f" and interval is not None:
        lat_mul = get_lat_mul(interval, flow_id)

    trace_id = hex_n(f"{SEED}:trace:{flow_id}:{instance_ordinal}", 32) if flow.get("trace", False) and SYSTEM["tracing"]["on"] else ""
    req_id = uuid_from_seed(f"{SEED}:req:{flow_id}:{instance_ordinal}")
    endpoint = endpoint_for_flow(flow_id)
    method = method_for_endpoint(endpoint)
    uri = uri_for_endpoint(endpoint)

    is_success = True
    is_prequery_error = False
    is_postquery_error = False
    read_only = False

    if "503_readonly" in flow_id:
        is_success = False
        is_prequery_error = True
        read_only = True
    elif "conn_refused" in flow_id:
        is_success = False
        is_prequery_error = True
    elif "_502_" in flow_id or flow_id.endswith("_502_f") or flow_id.endswith("_502_db_f") or flow_id.startswith("docker_pull_502") or flow_id.startswith("appreg_502") or flow_id.startswith("docker_push_502"):
        is_success = False
        is_postquery_error = any(ref.endswith("req_error_db") for ref in emit_refs)
        if not is_postquery_error:
            is_prequery_error = any(ref.endswith("req_error") for ref in emit_refs)

    if is_success:
        if endpoint == "registry_push":
            status = 201
        else:
            status = 200
    else:
        status = 503 if read_only else 502

    ua = "docker" if endpoint.startswith("registry_") else "operatorhub"
    op = op_for_endpoint(endpoint, is_success)

    comp_host: Dict[str, str] = {}
    for ref in emit_refs:
        cid, _ = ref.split(".", 1)
        if cid not in comp_host:
            comp_host[cid] = choose_host(cid, f"{SEED}:host:{flow_id}:{instance_ordinal}:{cid}")

    delays_ms: List[int] = []
    for j, pair in enumerate(latency_pairs):
        p50, p95 = apply_latency_multiplier(pair, lat_mul)
        ms = sample_lognormal_ms(p50, p95, f"{SEED}:lat:{flow_id}:{instance_ordinal}:{j}")
        delays_ms.append(ms)

    def find_idx(suffix: str) -> Optional[int]:
        for i, ref in enumerate(emit_refs):
            if ref.endswith(suffix):
                return i
        return None

    idx_req_start = find_idx("req_start")
    idx_db_acquire = find_idx("db_acquire")
    idx_db_query = find_idx("db_query")
    idx_done = find_idx("req_end")
    if idx_done is None:
        idx_done = find_idx("req_error_db")
    if idx_done is None:
        idx_done = find_idx("req_error")
    idx_router = find_idx("http_access")

    if idx_db_query is not None and idx_db_acquire is not None and idx_db_query > idx_db_acquire:
        tpl_dbq = TEMPLATES["quay_app.db_query"]
        dom = get_domain(tpl_dbq, state, "db_ms")
        if dom and dom["k"] == "i":
            lo, hi = int(dom["v"][0]), int(dom["v"][1])
            delays_ms[idx_db_query] = clamp_int(delays_ms[idx_db_query], lo, hi)

    if idx_router is None:
        idx_router = len(emit_refs) - 1
    total_dur = sum(delays_ms[: idx_router + 1])
    dur_cap = 70000
    if total_dur > dur_cap:
        s = dur_cap / total_dur
        delays_ms = [max(1, int(round(d * s))) for d in delays_ms]
        if idx_db_query is not None and idx_db_acquire is not None and idx_db_query > idx_db_acquire:
            tpl_dbq = TEMPLATES["quay_app.db_query"]
            dom = get_domain(tpl_dbq, state, "db_ms")
            if dom and dom["k"] == "i":
                lo, hi = int(dom["v"][0]), int(dom["v"][1])
                delays_ms[idx_db_query] = clamp_int(delays_ms[idx_db_query], lo, hi)

    ts_list: List[datetime] = []
    cur = start_ts
    for d in delays_ms:
        cur = cur + timedelta(milliseconds=int(d))
        ts_list.append(cur)

    db_ms_val = None
    if idx_db_query is not None and idx_db_acquire is not None:
        db_ms_val = int(round((ts_list[idx_db_query] - ts_list[idx_db_acquire]).total_seconds() * 1000))
    if idx_req_start is None:
        idx_req_start = 0
    if idx_done is None:
        idx_done = len(ts_list) - 1
    total_ms_val = int(round((ts_list[idx_done] - ts_list[idx_req_start]).total_seconds() * 1000))
    dur_ms_val = int(round((ts_list[idx_router] - start_ts).total_seconds() * 1000))

    bytes_val = bytes_for_endpoint(endpoint, f"{SEED}:bytes:{flow_id}:{instance_ordinal}")

    rows_val = None
    if idx_db_query is not None:
        tpl_dbq = TEMPLATES["quay_app.db_query"]
        dom_rows = get_domain(tpl_dbq, state, "rows")
        if dom_rows and dom_rows["k"] == "i":
            lo, hi = int(dom_rows["v"][0]), int(dom_rows["v"][1])
            base = 0
            if db_ms_val is not None:
                base = int(round(db_ms_val / (8 if state == "f" else 3)))
            wig = stable_hash_int(f"{SEED}:rows:{flow_id}:{instance_ordinal}") % 250
            rows_val = clamp_int(base + wig, lo, hi)

    open_conns_val = None
    tpl_acq = TEMPLATES["quay_app.db_acquire"]
    dom_oc = get_domain(tpl_acq, state, "open_conns")
    if dom_oc and dom_oc["k"] == "i":
        lo, hi = int(dom_oc["v"][0]), int(dom_oc["v"][1])
        u = quantile_u(f"{SEED}:oc:{flow_id}:{instance_ordinal}")
        q = 0.30 + 0.25 * u if state == "n" else 0.70 + 0.25 * u
        open_conns_val = clamp_int(int(round(lo + (hi - lo) * q)), lo, hi)

    worker_val = 1 + (stable_hash_int(f"{SEED}:worker:{flow_id}:{instance_ordinal}") % 32)

    err_val = None
    if not is_success:
        if read_only:
            err_val = "readonly_mode"
        elif is_prequery_error:
            err_val = "db_connect_refused" if (stable_hash_int(f"{SEED}:err:{flow_id}:{instance_ordinal}") % 2 == 0) else "db_pool_exhausted"
        else:
            err_val = "db_lock_timeout"

    rows_out: List[Dict[str, Any]] = []
    for j, ref in enumerate(emit_refs):
        tpl = TEMPLATES[ref]
        cid = tpl.component_id
        host = comp_host.get(cid, choose_host(cid, f"{SEED}:hostfallback:{flow_id}:{instance_ordinal}:{cid}"))
        service = SYSTEM["components"][cid]["svc"] or ""

        binding: Dict[str, Any] = {}
        if ref == "quay_app.req_start":
            binding.update({"method": method, "endpoint": endpoint, "req_id": req_id, "ua": ua, "trace_id": trace_id})
        elif ref == "quay_app.db_acquire":
            binding.update({"worker": worker_val, "open_conns": open_conns_val})
        elif ref == "quay_app.db_query":
            binding.update({"endpoint": endpoint, "op": op})
            if rows_val is not None:
                binding["rows"] = rows_val
            if db_ms_val is not None:
                binding["db_ms"] = clamp_int(db_ms_val, 1, 60000 if state == "f" else 500)
        elif ref == "quay_app.req_end":
            binding.update({"req_id": req_id, "status": status, "total_ms": clamp_int(total_ms_val, 1, 70000), "db_ms": clamp_int(db_ms_val or 0, 1, 60000)})
        elif ref == "quay_app.req_error":
            binding.update({"req_id": req_id, "status": status, "err": err_val or "db_connect_refused", "total_ms": clamp_int(total_ms_val, 1, 70000)})
        elif ref == "quay_app.req_error_db":
            binding.update({"req_id": req_id, "status": status, "err": err_val or "db_lock_timeout", "total_ms": clamp_int(total_ms_val, 1, 70000)})
        elif ref == "ingress_router.http_access":
            binding.update({"method": method, "uri": uri, "status": status, "dur_ms": clamp_int(dur_ms_val, 1, 70000), "bytes": bytes_val, "trace_id": trace_id})

        msg = render_message(tpl, state, binding, f"{SEED}:flow:{flow_id}:{instance_ordinal}:{j}")
        rows_out.append(
            {
                "timestamp_dt": ts_list[j],
                "level": tpl.lvl,
                "message": msg,
                "trace_id": trace_id if flow.get("trace", False) else "",
                "service": service,
                "host": host,
            }
        )
    return rows_out


def simulate_background_interval(
    component_id: str,
    state: str,
    start_dt: datetime,
    end_dt: datetime,
    rate_multipliers: Optional[Dict[str, float]],
    rounder: CarryRounding,
) -> List[Dict[str, Any]]:
    comp = SYSTEM["components"][component_id]
    emits = comp["beh"][state]
    out: List[Dict[str, Any]] = []
    hosts = comp.get("hosts", [])
    for emit in emits:
        log_id = emit["id"]
        scope = emit.get("scope", "per_host")
        per_min = float(emit["per_min"])
        mul_key = f"{component_id}.{log_id}"
        mul = 1.0
        if state == "f" and rate_multipliers is not None:
            mul = float(rate_multipliers.get(mul_key, 1.0))
        eff = per_min * mul
        if eff <= 0:
            continue

        duration_min = (end_dt - start_dt).total_seconds() / 60.0

        if scope == "global":
            expected = eff * duration_min
            cnt = rounder.alloc(f"bg:{mul_key}:global", expected)
            times = schedule_uniform(start_dt, end_dt, cnt, f"bg:{mul_key}:global:{start_dt.isoformat()}")
            for i, ts in enumerate(times):
                ref = f"{component_id}.{log_id}"
                tpl = TEMPLATES[ref]
                host = hosts[0] if hosts else ""
                binding = special_background_binding(component_id, log_id, state, f"{SEED}:bg:{mul_key}:global:{i}")
                msg = render_message(tpl, state, binding, f"{SEED}:bg:{mul_key}:global:{i}")
                out.append({"timestamp_dt": ts, "level": tpl.lvl, "message": msg, "trace_id": "", "service": comp["svc"] or "", "host": host})
        else:
            for h in hosts:
                expected = eff * duration_min
                cnt = rounder.alloc(f"bg:{mul_key}:host:{h}", expected)
                times = schedule_uniform(start_dt, end_dt, cnt, f"bg:{mul_key}:host:{h}:{start_dt.isoformat()}")
                for i, ts in enumerate(times):
                    ref = f"{component_id}.{log_id}"
                    tpl = TEMPLATES[ref]
                    binding = special_background_binding(component_id, log_id, state, f"{SEED}:bg:{mul_key}:host:{h}:{i}")
                    msg = render_message(tpl, state, binding, f"{SEED}:bg:{mul_key}:host:{h}:{i}")
                    out.append({"timestamp_dt": ts, "level": tpl.lvl, "message": msg, "trace_id": "", "service": comp["svc"] or "", "host": h})
    return out


def simulate_one_shots() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at_min = int(ev["at_min"])
        base_ts = BASE_TIME + timedelta(minutes=at_min)
        for os_idx, os in enumerate(ev.get("one_shots", [])):
            ref = os["ref"]
            cnt = int(os["count"])
            allowed_hosts = list(os.get("hosts", []))
            tpl = TEMPLATES[ref]
            cid = tpl.component_id
            service = SYSTEM["components"][cid]["svc"] or ""
            for i in range(cnt):
                # One-shots must not occur before the event time; keep deterministic sub-minute positive jitter.
                j_ms = stable_hash_int(f"{SEED}:oneshot:{ref}:{at_min}:{os_idx}:{i}") % 7001  # 0..7000ms
                ts = base_ts + timedelta(milliseconds=j_ms)
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else choose_host(cid, f"{SEED}:oneshot:{ref}:{i}")
                binding: Dict[str, Any] = {}
                if ref == "quay_app.mode_changed":
                    binding = {"read_only": "true", "db_conn_limit": 40}
                msg = render_message(tpl, "f", binding, f"{SEED}:oneshot:{ref}:{at_min}:{os_idx}:{i}")
                out.append({"timestamp_dt": ts, "level": tpl.lvl, "message": msg, "trace_id": "", "service": service, "host": host})
    return out


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    rounder = CarryRounding()
    rows: List[Dict[str, Any]] = []

    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]

    n_start_dt = BASE_TIME + timedelta(minutes=nstart)
    n_end_dt = BASE_TIME + timedelta(minutes=nend)
    for cid in sorted(SYSTEM["components"].keys()):
        rows.extend(simulate_background_interval(cid, "n", n_start_dt, n_end_dt, None, rounder))

    normal_flows = SYSTEM["flows"]["n"]
    for flow in sorted(normal_flows, key=lambda x: x["id"]):
        duration_min = (n_end_dt - n_start_dt).total_seconds() / 60.0
        expected_instances = float(flow["rpm"]) * duration_min
        cnt = rounder.alloc(f"flow:n:{flow['id']}", expected_instances)
        starts = schedule_uniform(n_start_dt, n_end_dt, cnt, f"flow:n:{flow['id']}:{n_start_dt.isoformat()}")
        for i, st in enumerate(starts):
            rows.extend(simulate_flow_instance(flow, "n", st, None, i))

    failure_flows = {f["id"]: f for f in SYSTEM["flows"]["f"]}
    for interval in FAILURE_INTERVALS:
        istart = BASE_TIME + timedelta(minutes=int(interval["start_min"]))
        iend = BASE_TIME + timedelta(minutes=int(interval["end_min"]))

        for cid in sorted(SYSTEM["components"].keys()):
            rows.extend(simulate_background_interval(cid, "f", istart, iend, interval["rate_multipliers"], rounder))

        duration_min = (iend - istart).total_seconds() / 60.0
        for flow_id in sorted(failure_flows.keys()):
            flow = failure_flows[flow_id]
            mul = get_rate_mul(interval, flow_id)
            eff_rpm = float(flow["rpm"]) * mul
            expected_instances = eff_rpm * duration_min
            cnt = rounder.alloc(f"flow:f:{flow_id}", expected_instances)
            starts = schedule_uniform(istart, iend, cnt, f"flow:f:{flow_id}:{istart.isoformat()}")
            for i, st in enumerate(starts):
                ordinal = stable_hash_int(f"{SEED}:ord:{flow_id}:{interval['start_min']}:{i}") % 10_000_000
                rows.extend(simulate_flow_instance(flow, "f", st, interval, ordinal))

    rows.extend(simulate_one_shots())

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)
    df = df.drop(columns=["timestamp_dt"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
