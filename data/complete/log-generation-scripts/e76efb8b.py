import re
import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Deterministic seeding (even though this simulator primarily uses hash-based determinism)
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "account_service_edge_outage"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "alb_gateway",
            "svc": "account-alb",
            "hosts": ["alb-1"],
            "logs": {
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "{method} {uri} status={status} dur_ms={dur_ms} target={target}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/account/api/oauth/verify", "/account/api/login"]},
                        "status": {"k": "ch", "v": [200, 401, 503]},
                        "dur_ms": {"k": "i", "v": [1, 6000]},
                        "target": {"k": "ch", "v": ["ngx-a", "ngx-b", "ngx-c"]},
                    },
                },
                "access_no_targets": {
                    "lvl": "ERROR",
                    "msg": "{method} {uri} status=503 dur_ms={dur_ms} reason=no_healthy_targets",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/account/api/oauth/verify", "/account/api/login"]},
                        "dur_ms": {"k": "i", "v": [1, 300]},
                    },
                },
                "targets_marked_unhealthy": {
                    "lvl": "ERROR",
                    "msg": "target_group={tg} marked_unhealthy={count} reason={reason}",
                    "vars": {
                        "tg": {"k": "ch", "v": ["account-nginx-tg"]},
                        "count": {"k": "i", "v": [1, 3]},
                        "reason": {"k": "ch", "v": ["health_check_timeout"]},
                    },
                },
                "healthcheck_fail": {
                    "lvl": "WARN",
                    "msg": "healthcheck failed path=/health target={target} timeout_ms={timeout_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["ngx-a", "ngx-b", "ngx-c"]},
                        "timeout_ms": {"k": "i", "v": [500, 1500]},
                    },
                },
            },
            "beh": {
                "n": {"emit": []},
                "f": {"emit": [{"id": "healthcheck_fail", "per_min": 6, "scope": "global"}]},
            },
        },
        {
            "id": "nginx_proxy",
            "svc": "account-nginx",
            "hosts": ["ngx-a", "ngx-b", "ngx-c"],
            "logs": {
                "worker_report": {
                    "lvl": "INFO",
                    "msg": "workers busy={busy} free={free} memcache_pending={mc_pending}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "busy": {"k": "i", "v": [0, 3]},
                            "free": {"k": "i", "v": [5, 8]},
                            "mc_pending": {"k": "i", "v": [0, 4]},
                        },
                        "f": {
                            "busy": {"k": "i", "v": [6, 8]},
                            "free": {"k": "i", "v": [0, 2]},
                            "mc_pending": {"k": "i", "v": [10, 120]},
                        },
                    },
                },
                "memcached_timeout": {
                    "lvl": "WARN",
                    "msg": "memcached timeout key={key} timeout_ms={timeout_ms} active_conns={active_conns}",
                    "vars": {
                        "key": {"k": "str", "v": "verify_token_cache_key"},
                        "timeout_ms": {"k": "i", "v": [100, 100]},
                        "active_conns": {"k": "i", "v": [5, 120]},
                    },
                },
                "cache_miss": {
                    "lvl": "INFO",
                    "msg": "cache MISS key={key}",
                    "vars": {"key": {"k": "str", "v": "verify_token_cache_key"}},
                },
                "access_cache_hit": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} {uri} status=200 cache=HIT rt_ms={rt_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "uri": {"k": "ch", "v": ["/account/api/oauth/verify"]},
                        "rt_ms": {"k": "i", "v": [1, 6000]},
                    },
                },
                "access_upstream_200": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} {uri} status=200 upstream=account_app rt_ms={rt_ms} upstream_ms={upstream_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "uri": {"k": "ch", "v": ["/account/api/oauth/verify"]},
                        "rt_ms": {"k": "i", "v": [1, 6000]},
                        "upstream_ms": {"k": "i", "v": [1, 5500]},
                    },
                },
                "access_pass_200": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} {uri} status=200 upstream=account_app rt_ms={rt_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "uri": {"k": "ch", "v": ["/account/api/login"]},
                        "rt_ms": {"k": "i", "v": [1, 6000]},
                    },
                },
                "access_503": {
                    "lvl": "ERROR",
                    "msg": "req_id={req_id} {uri} status=503 error=no_free_workers",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "uri": {"k": "ch", "v": ["/account/api/login", "/account/api/oauth/verify"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_report", "per_min": 2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "worker_report", "per_min": 2, "scope": "per_host"}]},
            },
        },
        {
            "id": "account_app",
            "svc": "account-jaxrs",
            "hosts": ["app-a", "app-b", "app-c"],
            "logs": {
                "verify_token_ok": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} verifyToken ok user={user_id} backend=redis dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "user_id": {"k": "i", "v": [1000000, 9999999]},
                        "dur_ms": {"k": "i", "v": [1, 5500]},
                    },
                },
                "signin_ok": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} signIn ok user={user_id} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "user_id": {"k": "i", "v": [1000000, 9999999]},
                        "dur_ms": {"k": "i", "v": [5, 6000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "memcached_cluster",
            "svc": "account-memcached",
            "hosts": ["mc-1", "mc-2", "mc-3"],
            "logs": {
                "conn_saturation": {
                    "lvl": "WARN",
                    "msg": "listener saturated active_conns={active_conns} evictions={evictions} timeouts_s={timeouts_s}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active_conns": {"k": "i", "v": [5, 40]},
                            "evictions": {"k": "i", "v": [0, 6]},
                            "timeouts_s": {"k": "i", "v": [0, 1]},
                        },
                        "f": {
                            "active_conns": {"k": "i", "v": [40, 140]},
                            "evictions": {"k": "i", "v": [5, 60]},
                            "timeouts_s": {"k": "i", "v": [1, 15]},
                        },
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "conn_saturation", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "conn_saturation", "per_min": 1.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "redis_cluster",
            "svc": "account-redis",
            "hosts": ["redis-1", "redis-2"],
            "logs": {
                "latency_report": {
                    "lvl": "INFO",
                    "msg": "p99_ms={p99_ms} blocked_clients={blocked} ops_s={ops_s}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "p99_ms": {"k": "i", "v": [1, 12]},
                            "blocked": {"k": "i", "v": [0, 2]},
                            "ops_s": {"k": "i", "v": [20, 120]},
                        },
                        "f": {
                            "p99_ms": {"k": "i", "v": [8, 160]},
                            "blocked": {"k": "i", "v": [0, 25]},
                            "ops_s": {"k": "i", "v": [80, 450]},
                        },
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "latency_report", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "latency_report", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "verify_cache_hit",
                    "rpm": 400,
                    "emit": ["nginx_proxy.access_cache_hit", "alb_gateway.access_ok"],
                    "latency_ms": [[1, 6], [2, 12]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "verify_cache_miss_fallback",
                    "rpm": 80,
                    "emit": [
                        "nginx_proxy.cache_miss",
                        "account_app.verify_token_ok",
                        "nginx_proxy.access_upstream_200",
                        "alb_gateway.access_ok",
                    ],
                    "latency_ms": [[1, 5], [3, 15], [2, 20], [2, 25]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "signin_success",
                    "rpm": 120,
                    "emit": ["account_app.signin_ok", "nginx_proxy.access_pass_200", "alb_gateway.access_ok"],
                    "latency_ms": [[20, 120], [2, 15], [2, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "verify_memcached_timeout_fallback",
                    "rpm": 380,
                    "emit": [
                        "nginx_proxy.memcached_timeout",
                        "account_app.verify_token_ok",
                        "nginx_proxy.access_upstream_200",
                        "alb_gateway.access_ok",
                    ],
                    "latency_ms": [[200, 1200], [10, 140], [5, 80], [5, 90]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "verify_cache_hit_residual",
                    "rpm": 50,
                    "emit": ["nginx_proxy.access_cache_hit", "alb_gateway.access_ok"],
                    "latency_ms": [[2, 50], [2, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "signin_degraded",
                    "rpm": 120,
                    "emit": ["account_app.signin_ok", "nginx_proxy.access_pass_200", "alb_gateway.access_ok"],
                    "latency_ms": [[250, 2200], [30, 300], [30, 350]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "signin_nginx_503",
                    "rpm": 60,
                    "emit": ["nginx_proxy.access_503", "alb_gateway.access_ok"],
                    "latency_ms": [[5, 250], [5, 260]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "alb_503_no_targets",
                    "rpm": 500,
                    "emit": ["alb_gateway.access_no_targets"],
                    "latency_ms": [[1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "account_service_nginx_memcached_saturation"},
    "time": {
        "total_minutes": 50,
        "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "signin_nginx_503": 0.0,
                        "alb_503_no_targets": 0.0,
                        "alb_gateway.healthcheck_fail": 0.0,
                        "redis_cluster.latency_report": 1.5,
                    },
                    "latency_multipliers": {
                        "verify_memcached_timeout_fallback": {"p50": 1.0, "p95": 1.0},
                        "signin_degraded": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 33,
                    "rate_multipliers": {
                        "signin_degraded": 0.6,
                        "signin_nginx_503": 1.0,
                        "verify_memcached_timeout_fallback": 1.2,
                        "verify_cache_hit_residual": 0.5,
                        "alb_gateway.healthcheck_fail": 1.0,
                        "nginx_proxy.worker_report": 2.0,
                        "redis_cluster.latency_report": 2.0,
                    },
                    "latency_multipliers": {
                        "signin_degraded": {"p50": 2.0, "p95": 3.0},
                        "verify_memcached_timeout_fallback": {"p50": 1.2, "p95": 1.3},
                    },
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 38,
                    "rate_multipliers": {
                        "verify_memcached_timeout_fallback": 0.0,
                        "verify_cache_hit_residual": 0.0,
                        "signin_degraded": 0.0,
                        "signin_nginx_503": 0.0,
                        "alb_503_no_targets": 1.0,
                        "alb_gateway.healthcheck_fail": 1.0,
                        "nginx_proxy.worker_report": 1.5,
                        "redis_cluster.latency_report": 1.0,
                        "memcached_cluster.conn_saturation": 0.4,
                    },
                    "latency_multipliers": {"alb_503_no_targets": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [{"ref": "alb_gateway.targets_marked_unhealthy", "count": 1, "hosts": ["alb-1"]}],
                },
            ]
        }
    },
}

_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def stable_u(key: str) -> float:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    x = int(h[:8], 16)
    return x / 2**32


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0:
        return base
    u = stable_u("round:" + key)
    return base + (1 if u < frac else 0)


def isoformat_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def even_schedule(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    dur = max(0.0, end_ts - start_ts)
    if dur == 0.0:
        return [start_dt for _ in range(count)]
    cell = dur / count
    jitter_range = min(0.30, cell * 0.30)  # seconds
    out: List[datetime] = []
    for i in range(count):
        center = start_ts + (i + 0.5) * cell
        u = stable_u(f"jitter:{key}:{i}")
        jitter = (u * 2.0 - 1.0) * jitter_range
        t = center + jitter
        if t < start_ts:
            t = start_ts
        if t >= end_ts:
            t = np.nextafter(end_ts, start_ts)
        out.append(datetime.fromtimestamp(t, tz=timezone.utc))
    return out


def value_from_domain(domain: Dict[str, Any], seed: str) -> Any:
    k = domain["k"]
    v = domain["v"]
    u = stable_u(f"dom:{seed}")
    if k == "ch":
        vals = list(v)
        idx = int(math.floor(u * len(vals)))
        if idx >= len(vals):
            idx = len(vals) - 1
        return vals[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        span = hi - lo + 1
        return lo + int(math.floor(u * span))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + u * (hi - lo)
    if k == "hex":
        length = int(v)
        h = hashlib.sha256(f"hex:{seed}".encode("utf-8")).hexdigest()
        return h[:length]
    if k == "uuid":
        h = hashlib.sha256(f"uuid:{seed}".encode("utf-8")).hexdigest()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "ip":
        h = hashlib.sha256(f"ip:{seed}".encode("utf-8")).hexdigest()
        a = int(h[:2], 16)
        b = int(h[2:4], 16)
        c = int(h[4:6], 16)
        d = int(h[6:8], 16)
        return f"{a}.{b}.{c}.{d}"
    if k == "str":
        hint = str(v)
        if hint == "verify_token_cache_key":
            h = hashlib.sha256(f"vk:{seed}".encode("utf-8")).hexdigest()
            return f"verify_token:{h[:24]}"
        return f"{hint}:{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:12]}"
    return str(v)


def sample_latency_ms(p50: float, p95: float, key: str) -> int:
    if p50 < 0:
        p50 = 0
    if p95 < p50:
        p95 = p50
    u = stable_u(f"lat:{key}")
    x = p50 + (p95 - p50) * (u**4)
    if x < 1.0:
        x = 1.0
    return int(round(x))


def get_placeholders(msg: str) -> List[str]:
    return list(dict.fromkeys(_PLACEHOLDER_RE.findall(msg)))


@dataclass(frozen=True)
class LogRef:
    comp_id: str
    log_id: str


components_by_id: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

logdefs_by_ref: Dict[str, Dict[str, Any]] = {}
logref_obj_by_ref: Dict[str, LogRef] = {}
for comp in SYSTEM["components"]:
    comp_id = comp["id"]
    for log_id, ld in comp["logs"].items():
        ref = f"{comp_id}.{log_id}"
        logdefs_by_ref[ref] = ld
        logref_obj_by_ref[ref] = LogRef(comp_id=comp_id, log_id=log_id)

flows_by_state: Dict[str, List[Dict[str, Any]]] = {
    "n": list(SYSTEM["flows"]["n"]["req"]),
    "f": list(SYSTEM["flows"]["f"]["req"]),
}
flows_by_id_state: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st, fls in flows_by_state.items():
    for f in fls:
        flows_by_id_state[(st, f["id"])] = f

f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

boundaries = sorted(set([f_start] + [e["at_min"] for e in events] + [f_end]))
failure_intervals: List[Dict[str, Any]] = []

rate_mult_flow: Dict[str, float] = {}
rate_mult_bg: Dict[str, float] = {}
lat_mult_flow: Dict[str, Tuple[float, float]] = {}

event_ptr = 0
for i in range(len(boundaries) - 1):
    start_min = boundaries[i]
    end_min = boundaries[i + 1]
    while event_ptr < len(events) and events[event_ptr]["at_min"] == start_min:
        ev = events[event_ptr]
        for k, v in ev.get("rate_multipliers", {}).items():
            if "." in k:
                rate_mult_bg[k] = float(v)
            else:
                rate_mult_flow[k] = float(v)
        for k, mults in ev.get("latency_multipliers", {}).items():
            lat_mult_flow[k] = (float(mults.get("p50", 1.0)), float(mults.get("p95", 1.0)))
        event_ptr += 1
    failure_intervals.append(
        {
            "state": "f",
            "start_min": start_min,
            "end_min": end_min,
            "rate_mult_flow": dict(rate_mult_flow),
            "rate_mult_bg": dict(rate_mult_bg),
            "lat_mult_flow": dict(lat_mult_flow),
        }
    )

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

rows: List[Dict[str, Any]] = []


def emit_row(ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {"timestamp": ts, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host}
    )


def render_log_message(
    comp_id: str,
    log_id: str,
    state: str,
    seed: str,
    overrides: Dict[str, Any],
) -> Tuple[str, str]:
    comp = components_by_id[comp_id]
    ld = comp["logs"][log_id]
    msg_tmpl = ld["msg"]
    placeholders = get_placeholders(msg_tmpl)

    values: Dict[str, Any] = {}
    if "state_vars" in ld and ld["state_vars"] and state in ld["state_vars"]:
        for k, dom in ld["state_vars"][state].items():
            values[k] = value_from_domain(dom, f"{seed}:{comp_id}:{log_id}:{k}")

    for k, dom in (ld.get("vars") or {}).items():
        if k not in values:
            values[k] = value_from_domain(dom, f"{seed}:{comp_id}:{log_id}:{k}")

    values.update(overrides)

    for ph in placeholders:
        if ph not in values:
            values[ph] = value_from_domain({"k": "str", "v": ph}, f"{seed}:{comp_id}:{log_id}:{ph}")

    try:
        rendered = msg_tmpl.format(**values)
    except Exception:
        rendered = msg_tmpl.format(**{k: str(v) for k, v in values.items()})
    return ld["lvl"], rendered


def choose_host_for_component(comp_id: str, seed: str) -> str:
    comp = components_by_id[comp_id]
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    if len(hosts) == 1:
        return hosts[0]
    u = stable_u(f"host:{seed}:{comp_id}")
    idx = int(math.floor(u * len(hosts)))
    if idx >= len(hosts):
        idx = len(hosts) - 1
    return hosts[idx]


def get_service(comp_id: str) -> str:
    return components_by_id[comp_id].get("svc") or ""


def schedule_background_interval(state: str, start_min: int, end_min: int, rate_mult_bg_snapshot: Optional[Dict[str, float]]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, (end_dt - start_dt).total_seconds() / 60.0)

    for comp_id in sorted(components_by_id.keys()):
        comp = components_by_id[comp_id]
        beh = comp.get("beh", {}).get(state, {}).get("emit", []) or []
        for beh_entry in beh:
            log_id = beh_entry["id"]
            per_min = float(beh_entry["per_min"])
            scope = beh_entry.get("scope", "per_host")
            mult = 1.0
            if state == "f" and rate_mult_bg_snapshot is not None:
                mult = float(rate_mult_bg_snapshot.get(f"{comp_id}.{log_id}", 1.0))
            eff = per_min * mult
            if eff <= 0:
                continue

            if scope == "global":
                expected = eff * duration_min
                count = deterministic_round(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:global")
                times = even_schedule(start_dt, end_dt, count, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:global")
                host = (comp.get("hosts") or [""])[0] if (comp.get("hosts") or []) else ""
                for j, t in enumerate(times):
                    seed = f"bg:{state}:{comp_id}.{log_id}:{start_min}-{end_min}:{j}:{host}"
                    lvl, msg = render_log_message(comp_id, log_id, state, seed, overrides={})
                    emit_row(t, lvl, msg, "", get_service(comp_id), host)
            else:
                hosts = comp.get("hosts") or []
                for host in hosts:
                    expected = eff * duration_min
                    count = deterministic_round(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:{host}")
                    times = even_schedule(start_dt, end_dt, count, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:{host}")
                    for j, t in enumerate(times):
                        seed = f"bg:{state}:{comp_id}.{log_id}:{start_min}-{end_min}:{j}:{host}"
                        lvl, msg = render_log_message(comp_id, log_id, state, seed, overrides={})
                        emit_row(t, lvl, msg, "", get_service(comp_id), host)


def build_flow_context(flow_id: str, state: str, seed: str, nginx_host: str) -> Dict[str, Any]:
    if flow_id in ("verify_cache_hit", "verify_cache_miss_fallback", "verify_memcached_timeout_fallback", "verify_cache_hit_residual"):
        uri = "/account/api/oauth/verify"
        method = "GET"
        alb_status = 200
    elif flow_id in ("signin_success", "signin_degraded"):
        uri = "/account/api/login"
        method = "POST"
        alb_status = 200
    elif flow_id == "signin_nginx_503":
        uri = "/account/api/login"
        method = "POST"
        alb_status = 503
    elif flow_id == "alb_503_no_targets":
        u = stable_u(f"alb_no_targets_uri:{seed}")
        uri = "/account/api/oauth/verify" if u < 0.75 else "/account/api/login"
        method = "GET" if uri.endswith("/verify") else "POST"
        alb_status = 503
    else:
        uri = "/account/api/oauth/verify"
        method = "GET"
        alb_status = 200

    req_id = value_from_domain({"k": "hex", "v": 16}, f"reqid:{seed}")
    user_id = value_from_domain({"k": "i", "v": [1000000, 9999999]}, f"uid:{seed}")

    ctx = {
        "flow_id": flow_id,
        "state": state,
        "method": method,
        "uri": uri,
        "alb_status": alb_status,
        "target": nginx_host if nginx_host else value_from_domain({"k": "ch", "v": ["ngx-a", "ngx-b", "ngx-c"]}, f"tgt:{seed}"),
        "req_id": req_id,
        "user_id": user_id,
        "key": value_from_domain({"k": "str", "v": "verify_token_cache_key"}, f"key:{seed}:{req_id}"),
    }
    return ctx


def get_domain_max(ld: Dict[str, Any], var_name: str) -> Optional[int]:
    dom = (ld.get("vars") or {}).get(var_name)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    return int(dom["v"][1])


def simulate_flow_instances_for_interval(
    state: str,
    start_min: int,
    end_min: int,
    rate_mult_flow_snapshot: Optional[Dict[str, float]],
    lat_mult_flow_snapshot: Optional[Dict[str, Tuple[float, float]]],
) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, (end_dt - start_dt).total_seconds() / 60.0)

    flows = flows_by_state[state]
    for flow in sorted(flows, key=lambda x: x["id"]):
        flow_id = flow["id"]
        base_rpm = float(flow["rpm"])
        mult = 1.0
        if state == "f" and rate_mult_flow_snapshot is not None:
            mult = float(rate_mult_flow_snapshot.get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0:
            continue
        expected_instances = eff_rpm * duration_min
        n_instances = deterministic_round(expected_instances, f"flow:{state}:{start_min}-{end_min}:{flow_id}")
        start_times = even_schedule(start_dt, end_dt, n_instances, f"flow:{state}:{start_min}-{end_min}:{flow_id}")

        for idx, t0 in enumerate(start_times):
            inst_seed = f"{state}:{flow_id}:{start_min}-{end_min}:inst:{idx}:{isoformat_ms(t0)}"

            nginx_host = choose_host_for_component("nginx_proxy", inst_seed) if any(ref.startswith("nginx_proxy.") for ref in flow["emit"]) else ""
            app_host = choose_host_for_component("account_app", inst_seed) if any(ref.startswith("account_app.") for ref in flow["emit"]) else ""
            alb_host = choose_host_for_component("alb_gateway", inst_seed) if any(ref.startswith("alb_gateway.") for ref in flow["emit"]) else ""

            ctx = build_flow_context(flow_id, state, inst_seed, nginx_host)

            p50_mult, p95_mult = (1.0, 1.0)
            if state == "f" and lat_mult_flow_snapshot is not None:
                p50_mult, p95_mult = lat_mult_flow_snapshot.get(flow_id, (1.0, 1.0))

            emit_refs = flow["emit"]
            raw_delays: List[int] = []
            for li, (p50, p95) in enumerate(flow["latency_ms"]):
                p50_s = float(p50) * float(p50_mult)
                p95_s = float(p95) * float(p95_mult)
                d = sample_latency_ms(p50_s, p95_s, f"{inst_seed}:log:{li}")
                raw_delays.append(d)

            constraints: List[Tuple[int, int]] = []
            per_delay_caps: Dict[int, int] = {}
            for li, ref in enumerate(emit_refs):
                lr = logref_obj_by_ref[ref]
                ld = components_by_id[lr.comp_id]["logs"][lr.log_id]
                phs = get_placeholders(ld["msg"])

                if "rt_ms" in phs:
                    mx = get_domain_max(ld, "rt_ms")
                    if mx is not None:
                        constraints.append((li, mx))
                if "dur_ms" in phs:
                    mx = get_domain_max(ld, "dur_ms")
                    if mx is not None:
                        if lr.comp_id == "alb_gateway":
                            constraints.append((li, mx))
                        else:
                            per_delay_caps[li] = mx

            cum_raw = np.cumsum(raw_delays)
            scale = 1.0
            for li, mx in constraints:
                if cum_raw[li] > 0:
                    scale = min(scale, float(mx) / float(cum_raw[li]))
            scaled = [max(1, int(round(d * scale))) for d in raw_delays] if scale < 1.0 else list(raw_delays)

            for li, cap in per_delay_caps.items():
                if li < len(scaled) and scaled[li] > cap:
                    scaled[li] = cap

            cum = 0
            for li, d in enumerate(scaled):
                cum += d
                mxs = [mx for (cidx, mx) in constraints if cidx == li]
                if mxs:
                    mx = min(mxs)
                    if cum > mx:
                        over = cum - mx
                        scaled[li] = max(1, scaled[li] - over)
                        cum -= over

            cum_ms = 0
            app_dur_ms: Optional[int] = None
            for li, ref in enumerate(emit_refs):
                lr = logref_obj_by_ref[ref]
                comp_id = lr.comp_id
                log_id = lr.log_id
                comp = components_by_id[comp_id]
                if comp_id == "nginx_proxy":
                    host = nginx_host
                elif comp_id == "account_app":
                    host = app_host
                elif comp_id == "alb_gateway":
                    host = alb_host
                else:
                    host = choose_host_for_component(comp_id, inst_seed)

                cum_ms += scaled[li]
                ts = t0 + timedelta(milliseconds=int(cum_ms))

                ld = comp["logs"][log_id]
                phs = get_placeholders(ld["msg"])
                overrides: Dict[str, Any] = {}

                if "req_id" in phs:
                    overrides["req_id"] = ctx["req_id"]
                if "uri" in phs:
                    overrides["uri"] = ctx["uri"]
                if "method" in phs:
                    overrides["method"] = ctx["method"]
                if "target" in phs:
                    overrides["target"] = ctx["target"]
                if "status" in phs:
                    overrides["status"] = ctx["alb_status"]
                if "user_id" in phs:
                    overrides["user_id"] = ctx["user_id"]
                if "key" in phs:
                    overrides["key"] = ctx["key"]

                if "rt_ms" in phs:
                    overrides["rt_ms"] = int(cum_ms)
                if "dur_ms" in phs:
                    if comp_id == "alb_gateway":
                        overrides["dur_ms"] = int(cum_ms)
                    elif comp_id == "account_app":
                        overrides["dur_ms"] = int(scaled[li])
                        app_dur_ms = int(scaled[li])
                    else:
                        overrides["dur_ms"] = int(scaled[li])

                if "upstream_ms" in phs:
                    if app_dur_ms is None:
                        app_dur_ms = max(1, int(scaled[li]))
                    mx = get_domain_max(ld, "upstream_ms")
                    um = int(app_dur_ms)
                    if mx is not None and um > mx:
                        um = mx
                    overrides["upstream_ms"] = um

                if comp_id == "nginx_proxy" and log_id == "memcached_timeout":
                    if state == "f":
                        overrides["active_conns"] = int(value_from_domain({"k": "i", "v": [60, 120]}, f"mcac:{inst_seed}:{li}"))
                    else:
                        overrides["active_conns"] = int(value_from_domain({"k": "i", "v": [5, 40]}, f"mcac:{inst_seed}:{li}"))

                lvl, msg = render_log_message(comp_id, log_id, state, inst_seed, overrides=overrides)
                emit_row(ts, lvl, msg, "", get_service(comp_id), host)


def emit_one_shots() -> None:
    # One-shots must be emitted at (or after) the event minute boundary.
    # We keep deterministic small sub-minute jitter strictly within the event minute, never before base_ts.
    for ev in events:
        at_min = ev["at_min"]
        for one in ev.get("one_shots", []) or []:
            ref = one["ref"]
            count = int(one["count"])
            allowed_hosts = one.get("hosts")
            lr = logref_obj_by_ref[ref]
            comp_id = lr.comp_id
            log_id = lr.log_id
            comp = components_by_id[comp_id]
            service = get_service(comp_id)
            base_ts = BASE_TIME + timedelta(minutes=at_min)
            minute_end = base_ts + timedelta(minutes=1)

            # Spread within the first 5 seconds of the minute for stability; clamp within [base_ts, minute_end).
            window_ms = 5000

            for i in range(count):
                u = stable_u(f"oneshot:{ref}:{at_min}:{i}")
                # deterministic within [0, window_ms)
                pos_ms = int(((i + u) / max(1, count)) * window_ms)
                ts = base_ts + timedelta(milliseconds=pos_ms)
                if ts < base_ts:
                    ts = base_ts
                if ts >= minute_end:
                    ts = minute_end - timedelta(milliseconds=1)

                if allowed_hosts:
                    host = allowed_hosts[i % len(allowed_hosts)]
                else:
                    host = (comp.get("hosts") or [""])[0] if (comp.get("hosts") or []) else ""

                seed = f"oneshot:{ref}:{at_min}:{i}:{host}"
                overrides: Dict[str, Any] = {}
                if comp_id == "alb_gateway" and log_id == "targets_marked_unhealthy":
                    overrides["count"] = 3
                lvl, msg = render_log_message(comp_id, log_id, "f", seed, overrides=overrides)
                emit_row(ts, lvl, msg, "", service, host)


n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
schedule_background_interval("n", n_start, n_end, rate_mult_bg_snapshot=None)
simulate_flow_instances_for_interval("n", n_start, n_end, rate_mult_flow_snapshot=None, lat_mult_flow_snapshot=None)

for it in failure_intervals:
    schedule_background_interval("f", it["start_min"], it["end_min"], rate_mult_bg_snapshot=it["rate_mult_bg"])
    simulate_flow_instances_for_interval(
        "f",
        it["start_min"],
        it["end_min"],
        rate_mult_flow_snapshot=it["rate_mult_flow"],
        lat_mult_flow_snapshot=it["lat_mult_flow"],
    )

emit_one_shots()

df = pd.DataFrame(rows)
df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
df["timestamp"] = df["timestamp"].apply(isoformat_ms)
df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

row_count = len(df)
if not (20000 <= row_count <= 100000):
    raise RuntimeError(f"Row count {row_count} out of required range [20000, 100000].")

df.to_csv("logs.csv", index=False)
