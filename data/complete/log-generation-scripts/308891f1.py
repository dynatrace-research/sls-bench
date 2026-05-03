import math
import re
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Embedded executable spec
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "azure_devops_tfs_scale_unit"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_lb",
            "svc": "vip",
            "hosts": ["vip-1"],
            "logs": {
                "http_forward": {
                    "lvl": "INFO",
                    "msg": "forward {method} {route} from {client_ip} to {backend}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/_apis/git/repositories", "/_apis/build/builds", "/_apis/projects"]},
                        "client_ip": {"k": "ip", "v": "10.0.0.0/16"},
                        "backend": {"k": "ch", "v": ["at1", "at2", "at3", "at4"]},
                    },
                },
                "probe_timeout": {
                    "lvl": "WARN",
                    "msg": "health probe to {backend} timed out after {timeout_ms}ms",
                    "vars": {
                        "backend": {"k": "ch", "v": ["at1", "at2", "at3", "at4"]},
                        "timeout_ms": {"k": "i", "v": [1000, 15000]},
                    },
                },
                "backend_state": {
                    "lvl": "WARN",
                    "msg": "backend {backend} marked {state} after {fail_count} consecutive probe failures",
                    "vars": {
                        "backend": {"k": "ch", "v": ["at1", "at2", "at3", "at4"]},
                        "state": {"k": "ch", "v": ["unhealthy"]},
                        "fail_count": {"k": "i", "v": [3, 10]},
                    },
                },
                "summary": {
                    "lvl": "INFO",
                    "msg": "lb summary healthy_backends={healthy} total_backends={total} req_rps={rps}",
                    "vars": {
                        "healthy": {"k": "i", "v": [0, 4]},
                        "total": {"k": "i", "v": [4, 4]},
                        "rps": {"k": "f", "v": [0.0, 50.0]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "summary", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "summary", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "tfs_at",
            "svc": "tfs-at",
            "hosts": ["at1", "at2", "at3", "at4"],
            "logs": {
                "http_access": {
                    "lvl": "INFO",
                    "msg": "{req_id} {method} {route} user={user_type} status={status} total_ms={total_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/_apis/git/repositories", "/_apis/build/builds", "/_apis/projects"]},
                        "user_type": {"k": "ch", "v": ["interactive", "service"]},
                    },
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": [200, 401]}, "total_ms": {"k": "i", "v": [10, 800]}},
                        "f": {"status": {"k": "ch", "v": [200, 503, 504]}, "total_ms": {"k": "i", "v": [50, 60000]}},
                    },
                },
                "agent_heartbeat": {
                    "lvl": "INFO",
                    "msg": "SetAgentOnline agent={agent_id} result={result} dur_ms={dur_ms} queued={queued}",
                    "vars": {"agent_id": {"k": "i", "v": [100000, 999999]}},
                    "state_vars": {
                        "n": {"result": {"k": "ch", "v": ["ok"]}, "dur_ms": {"k": "i", "v": [1, 80]}, "queued": {"k": "i", "v": [0, 10]}},
                        "f": {
                            "result": {"k": "ch", "v": ["ok", "timeout", "queued"]},
                            "dur_ms": {"k": "i", "v": [50, 60000]},
                            "queued": {"k": "i", "v": [0, 5000]},
                        },
                    },
                },
                "health_access": {
                    "lvl": "INFO",
                    "msg": "healthcheck status={status} dur_ms={dur_ms} inflight={inflight}",
                    "vars": {},
                    "state_vars": {
                        "n": {"status": {"k": "i", "v": [200, 200]}, "dur_ms": {"k": "i", "v": [1, 40]}, "inflight": {"k": "i", "v": [0, 200]}},
                        "f": {"status": {"k": "i", "v": [200, 503]}, "dur_ms": {"k": "i", "v": [5, 60000]}, "inflight": {"k": "i", "v": [50, 2000]}},
                    },
                },
                "mq_stats": {
                    "lvl": "INFO",
                    "msg": "mq stats inflight_threads={threads} queue_depth={queue_depth} reconnect_batch={batch}",
                    "vars": {},
                    "state_vars": {
                        "n": {"threads": {"k": "i", "v": [40, 250]}, "queue_depth": {"k": "i", "v": [0, 200]}, "batch": {"k": "i", "v": [0, 20]}},
                        "f": {"threads": {"k": "i", "v": [40, 2000]}, "queue_depth": {"k": "i", "v": [0, 20000]}, "batch": {"k": "i", "v": [0, 5000]}},
                    },
                },
                "mq_starvation": {
                    "lvl": "WARN",
                    "msg": "mq threadpool saturation detected: inflight_threads={threads} queue_depth={queue_depth}",
                    "vars": {"threads": {"k": "i", "v": [500, 2000]}, "queue_depth": {"k": "i", "v": [5000, 20000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "mq_stats", "per_min": 1.0, "scope": "per_host"}, {"id": "mq_starvation", "per_min": 0.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "mq_stats", "per_min": 1.0, "scope": "per_host"}, {"id": "mq_starvation", "per_min": 0.02, "scope": "per_host"}]},
            },
        },
        {
            "id": "sps_auth",
            "svc": "sps",
            "hosts": ["sps-ncus-1"],
            "logs": {
                "sps_latency": {
                    "lvl": "INFO",
                    "msg": "sps metrics p95_ms={p95_ms} error_rate={err_rate} db_concurrency={db_cb}",
                    "vars": {},
                    "state_vars": {
                        "n": {"p95_ms": {"k": "i", "v": [60, 300]}, "err_rate": {"k": "f", "v": [0.0, 0.02]}, "db_cb": {"k": "ch", "v": ["none", "moderate"]}},
                        "f": {
                            "p95_ms": {"k": "i", "v": [80, 1500]},
                            "err_rate": {"k": "f", "v": [0.0, 0.08]},
                            "db_cb": {"k": "ch", "v": ["none", "moderate", "high"]},
                        },
                    },
                },
                "sps_slow_spike": {"lvl": "WARN", "msg": "elevated response times detected p95_ms={p95_ms} duration_s={duration_s}", "vars": {"p95_ms": {"k": "i", "v": [2000, 8000]}, "duration_s": {"k": "i", "v": [60, 180]}}},
                "sps_recovered": {"lvl": "INFO", "msg": "response times back to normal p95_ms={p95_ms}", "vars": {"p95_ms": {"k": "i", "v": [80, 250]}}},
            },
            "beh": {"n": {"emit": [{"id": "sps_latency", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "sps_latency", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "ops_console",
            "svc": "ops",
            "hosts": ["op-1"],
            "logs": {
                "collect_dump": {"lvl": "INFO", "msg": "collecting process dump from {backend} reason={reason}", "vars": {"backend": {"k": "ch", "v": ["at1", "at2", "at3", "at4"]}, "reason": {"k": "ch", "v": ["unhealthy", "thread_growth"]}}},
                "recycle_at": {"lvl": "WARN", "msg": "recycling application pool on {backend} mode={mode}", "vars": {"backend": {"k": "ch", "v": ["at1", "at2", "at3", "at4"]}, "mode": {"k": "ch", "v": ["single"]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {"id": "user_api_request", "rpm": 200.0, "emit": ["edge_lb.http_forward", "tfs_at.http_access"], "latency_ms": [[1, 8], [30, 600]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "agent_heartbeat", "rpm": 350.0, "emit": ["tfs_at.agent_heartbeat"], "latency_ms": [[5, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "health_probe_ok", "rpm": 100.0, "emit": ["tfs_at.health_access"], "latency_ms": [[1, 40]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
        "f": {
            "req": [
                {"id": "user_api_request_degraded", "rpm": 160.0, "emit": ["edge_lb.http_forward", "tfs_at.http_access"], "latency_ms": [[1, 10], [200, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "agent_heartbeat_reconnect", "rpm": 700.0, "emit": ["tfs_at.agent_heartbeat"], "latency_ms": [[50, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "health_probe_ok_degraded", "rpm": 100.0, "emit": ["tfs_at.health_access"], "latency_ms": [[5, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "health_probe_timeout", "rpm": 0.5, "emit": ["edge_lb.probe_timeout"], "latency_ms": [[1000, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "su3_tailspin_after_sps_spike"},
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {"agent_heartbeat_reconnect": 0.8, "health_probe_timeout": 0.0, "tfs_at.mq_starvation": 0.0},
                    "latency_multipliers": {"user_api_request_degraded": {"p50": 1.8, "p95": 2.5}, "agent_heartbeat_reconnect": {"p50": 1.6, "p95": 2.2}},
                    "one_shots": [{"ref": "sps_auth.sps_slow_spike", "count": 1, "hosts": ["sps-ncus-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 23,
                    "rate_multipliers": {"agent_heartbeat_reconnect": 1.4, "tfs_at.mq_starvation": 50.0},
                    "latency_multipliers": {"agent_heartbeat_reconnect": {"p50": 1.4, "p95": 2.5}, "user_api_request_degraded": {"p50": 1.2, "p95": 1.8}},
                    "one_shots": [{"ref": "sps_auth.sps_recovered", "count": 1, "hosts": ["sps-ncus-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 27,
                    "rate_multipliers": {"health_probe_timeout": 40.0, "user_api_request_degraded": 1.1, "tfs_at.mq_starvation": 150.0},
                    "latency_multipliers": {"health_probe_ok_degraded": {"p50": 1.3, "p95": 1.8}, "user_api_request_degraded": {"p50": 1.4, "p95": 2.2}},
                    "one_shots": [{"ref": "edge_lb.backend_state", "count": 1, "hosts": ["vip-1"]}, {"ref": "ops_console.collect_dump", "count": 1, "hosts": ["op-1"]}, {"ref": "ops_console.recycle_at", "count": 1, "hosts": ["op-1"]}],
                },
                {
                    "order": 4,
                    "at_min": 33,
                    "rate_multipliers": {"health_probe_timeout": 80.0, "agent_heartbeat_reconnect": 1.2, "user_api_request_degraded": 0.8, "tfs_at.mq_starvation": 250.0},
                    "latency_multipliers": {"agent_heartbeat_reconnect": {"p50": 1.6, "p95": 2.8}, "user_api_request_degraded": {"p50": 1.5, "p95": 2.6}},
                    "one_shots": [{"ref": "edge_lb.backend_state", "count": 1, "hosts": ["vip-1"]}, {"ref": "ops_console.recycle_at", "count": 2, "hosts": ["op-1"]}],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic simulation
# ----------------------------

SEED = 1337
random.seed(SEED)  # required by verifier; simulator primarily uses numpy RNG below
rng = np.random.RandomState(SEED)

PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def stable_hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:8], 16)


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def parse_cidr(cidr: str) -> Tuple[int, int]:
    ip_str, prefix_str = cidr.split("/")
    prefix = int(prefix_str)
    parts = [int(x) for x in ip_str.split(".")]
    base = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]
    mask = (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF
    network = base & mask
    host_bits = 32 - prefix
    host_max = (1 << host_bits) - 1
    return network, host_max


CIDR_CACHE: Dict[str, Tuple[int, int]] = {}


def gen_ip(cidr: str) -> str:
    if cidr not in CIDR_CACHE:
        CIDR_CACHE[cidr] = parse_cidr(cidr)
    network, host_max = CIDR_CACHE[cidr]
    host = int(rng.randint(1, max(2, host_max)))  # avoid 0
    val = (network + host) & 0xFFFFFFFF
    return f"{(val >> 24) & 255}.{(val >> 16) & 255}.{(val >> 8) & 255}.{val & 255}"


def gen_hex(n: int) -> str:
    b = rng.bytes((n + 1) // 2)
    s = b.hex()[:n]
    return s.lower()


def choose_from(values: List[Any]) -> Any:
    idx = int(rng.randint(0, len(values)))
    return values[idx]


def get_component(comp_id: str) -> Dict[str, Any]:
    return COMPONENTS[comp_id]


def get_logdef(comp_id: str, log_id: str) -> Dict[str, Any]:
    return LOGDEFS[(comp_id, log_id)]


def get_var_domain(comp_id: str, log_id: str, state: str, var: str) -> Optional[Dict[str, Any]]:
    ld = get_logdef(comp_id, log_id)
    if "state_vars" in ld and state in ld["state_vars"] and var in ld["state_vars"][state]:
        return ld["state_vars"][state][var]
    if "vars" in ld and var in ld["vars"]:
        return ld["vars"][var]
    return None


def gen_value(domain: Dict[str, Any]) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if lo == hi:
            return lo
        return float(lo + (hi - lo) * rng.rand())
    if k == "ch":
        return choose_from(list(v))
    if k == "hex":
        return gen_hex(int(v))
    if k == "ip":
        return gen_ip(str(v))
    if k == "uuid":
        h = gen_hex(32)
        return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "str":
        return str(v)
    raise ValueError(f"Unknown domain kind: {k}")


def render_message(comp_id: str, log_id: str, state: str, bound: Dict[str, Any]) -> str:
    ld = get_logdef(comp_id, log_id)
    msg = ld["msg"]
    needed = PLACEHOLDER_RE.findall(msg)
    ctx = dict(bound)
    for var in needed:
        if var in ctx:
            continue
        dom = get_var_domain(comp_id, log_id, state, var)
        if dom is None:
            ctx[var] = ""
        else:
            ctx[var] = gen_value(dom)
    for k, v in list(ctx.items()):
        if isinstance(v, float):
            ctx[k] = f"{v:.3f}".rstrip("0").rstrip(".")
    return msg.format(**ctx)


def sample_lognormal_ms(p50: float, p95: float) -> int:
    p50 = max(1e-3, float(p50))
    p95 = max(p50, float(p95))
    if p50 == p95:
        x = p50
    else:
        mu = math.log(p50)
        sigma = (math.log(p95) - math.log(p50)) / 1.645
        sigma = max(1e-6, sigma)
        x = float(rng.lognormal(mean=mu, sigma=sigma))
    soft_cap = 3.0 * p95
    if x > soft_cap:
        x = soft_cap + (x - soft_cap) * 0.05
    return int(max(1, round(x)))


def schedule_evenly(min_start: datetime, duration_s: float, count: int, jitter_s: float = 0.4) -> List[datetime]:
    if count <= 0:
        return []
    times: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        base = duration_s * frac
        jit = (rng.rand() - 0.5) * jitter_s
        off = clamp(base + jit, 0.001, duration_s - 0.001)
        times.append(min_start + timedelta(seconds=off))
    return times


# ----------------------------
# Build indices
# ----------------------------

COMPONENTS: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOGDEFS: Dict[Tuple[str, str], Dict[str, Any]] = {}
for comp in SYSTEM["components"]:
    for log_id, ld in comp["logs"].items():
        LOGDEFS[(comp["id"], log_id)] = ld

FLOWS: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for fdef in SYSTEM["flows"][st]["req"]:
        FLOWS[st][fdef["id"]] = fdef

# ----------------------------
# Control tables from events (persistent until overridden)
# ----------------------------

FAIL_EVENTS = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
FAIL_START = SCENARIO["time"]["phases"]["f"]["start_min"]
FAIL_END = SCENARIO["time"]["phases"]["f"]["end_min"]
NORM_START = SCENARIO["time"]["phases"]["n"]["start_min"]
NORM_END = SCENARIO["time"]["phases"]["n"]["end_min"]


def build_active_controls_by_minute() -> Dict[int, Dict[str, Any]]:
    active_rate_flow: Dict[str, float] = {}
    active_rate_bg: Dict[str, float] = {}
    active_lat_flow: Dict[str, Dict[str, float]] = {}
    idx = 0
    by_min: Dict[int, Dict[str, Any]] = {}
    for m in range(FAIL_START, FAIL_END):
        while idx < len(FAIL_EVENTS) and FAIL_EVENTS[idx]["at_min"] <= m:
            ev = FAIL_EVENTS[idx]
            for k, v in ev.get("rate_multipliers", {}).items():
                if "." in k:
                    active_rate_bg[k] = float(v)
                else:
                    active_rate_flow[k] = float(v)
            for fk, mult in ev.get("latency_multipliers", {}).items():
                active_lat_flow[fk] = {"p50": float(mult["p50"]), "p95": float(mult["p95"])}
            idx += 1
        by_min[m] = {"rate_flow": dict(active_rate_flow), "rate_bg": dict(active_rate_bg), "lat_flow": dict(active_lat_flow)}
    return by_min


ACTIVE_FAIL_CONTROLS_BY_MIN = build_active_controls_by_minute()

# ----------------------------
# Count allocation (low-variance deterministic rounding)
# ----------------------------

carry: Dict[str, float] = {}


def alloc_count(expected: float, key: str) -> int:
    c = carry.get(key, 0.0)
    total = expected + c
    n = int(math.floor(total + 1e-12))
    carry[key] = total - n
    return n


# ----------------------------
# Context helpers for coherent values
# ----------------------------

AT_HOSTS = COMPONENTS["tfs_at"]["hosts"]


def choose_at_host(flow_id: str, instance_seq: int) -> str:
    idx = (stable_hash_int(flow_id) + instance_seq) % len(AT_HOSTS)
    return AT_HOSTS[idx]


def bound_user_api_context(state: str, at_host: str) -> Dict[str, Any]:
    method = choose_from(["GET", "POST"])
    route = choose_from(["/_apis/git/repositories", "/_apis/build/builds", "/_apis/projects"])
    user_type = choose_from(["interactive", "service"])
    req_id = gen_hex(16)
    client_ip = gen_ip("10.0.0.0/16")
    return {"method": method, "route": route, "user_type": user_type, "req_id": req_id, "client_ip": client_ip, "backend": at_host}


def derive_user_status(state: str, req_id: str, total_ms: int) -> int:
    if state == "n":
        return 401 if (stable_hash_int(req_id) % 20 == 0) else 200
    if total_ms >= 20000:
        return 504
    if total_ms >= 8000:
        return 503
    return 200


def bound_agent_context(state: str) -> Dict[str, Any]:
    agent_id = int(rng.randint(100000, 999999 + 1))
    return {"agent_id": agent_id}


def derive_agent_result_and_queue(state: str, dur_ms: int) -> Tuple[str, int]:
    if state == "n":
        return "ok", int(rng.randint(0, 10 + 1))
    if dur_ms >= 20000:
        return "timeout", int(rng.randint(1000, 5000 + 1))
    if dur_ms >= 4000:
        return "queued", int(rng.randint(100, 5000 + 1))
    return "ok", int(rng.randint(0, 150 + 1))


def derive_health_status(state: str, dur_ms: int) -> int:
    if state == "n":
        return 200
    return 503 if dur_ms >= 8000 else 200


def derive_health_inflight(state: str, minute: int, dur_ms: int) -> int:
    if state == "n":
        return int(rng.randint(0, 200 + 1))
    if minute < 23:
        lo, hi = 80, 400
    elif minute < 27:
        lo, hi = 200, 900
    elif minute < 33:
        lo, hi = 400, 1600
    else:
        lo, hi = 700, 2000
    if dur_ms >= 8000:
        lo = min(2000, lo + 200)
        hi = min(2000, hi + 200)
    return int(rng.randint(lo, hi + 1))


def derive_mq_stats(state: str, minute: int) -> Tuple[int, int, int]:
    if state == "n":
        threads = int(rng.randint(60, 180 + 1))
        qd = int(rng.randint(0, 60 + 1))
        batch = int(rng.randint(0, 10 + 1))
        return threads, qd, batch
    if minute < 23:
        t_lo, t_hi = 120, 450
        q_lo, q_hi = 0, 800
        b_lo, b_hi = 0, 150
    elif minute < 27:
        t_lo, t_hi = 250, 900
        q_lo, q_hi = 200, 3500
        b_lo, b_hi = 50, 700
    elif minute < 33:
        t_lo, t_hi = 600, 1600
        q_lo, q_hi = 2000, 12000
        b_lo, b_hi = 200, 3000
    else:
        t_lo, t_hi = 800, 2000
        q_lo, q_hi = 5000, 20000
        b_lo, b_hi = 500, 5000
    threads = int(rng.randint(t_lo, t_hi + 1))
    qd = int(rng.randint(q_lo, q_hi + 1))
    batch = int(rng.randint(b_lo, b_hi + 1))
    return threads, qd, batch


def choose_probe_backend(minute: int, seq: int) -> str:
    if minute < 33:
        return "at1"
    return "at1" if (seq % 2 == 0) else "at2"


def derive_lb_healthy_backends(minute: int) -> int:
    if minute < 27:
        return 4
    if minute < 33:
        return 3
    return 2


def derive_sps_metrics(minute: int, state: str) -> Tuple[int, float, str]:
    if state == "n":
        p95 = int(rng.randint(70, 220 + 1))
        err = float(clamp(0.002 * rng.rand(), 0.0, 0.02))
        db = choose_from(["none", "moderate"])
        return p95, err, db
    if minute < 23:
        p95 = int(rng.randint(700, 1500 + 1))
        err = float(clamp(0.01 + 0.03 * rng.rand(), 0.0, 0.08))
        db = choose_from(["moderate", "high"])
    else:
        p95 = int(rng.randint(90, 320 + 1))
        err = float(clamp(0.003 + 0.01 * rng.rand(), 0.0, 0.08))
        db = choose_from(["none", "moderate"])
    return p95, err, db


# ----------------------------
# Emitters
# ----------------------------

rows: List[Dict[str, Any]] = []


def emit_log(ts: datetime, comp_id: str, log_id: str, state: str, bound: Dict[str, Any], host: Optional[str] = None, trace_id: str = "") -> None:
    comp = get_component(comp_id)
    ld = get_logdef(comp_id, log_id)
    service = comp.get("svc") or ""
    if host is None:
        hosts = comp.get("hosts") or []
        host = hosts[0] if hosts else ""
    message = render_message(comp_id, log_id, state, bound)
    rows.append({"timestamp_dt": ts, "level": ld["lvl"], "message": message, "trace_id": trace_id if trace_id else "", "service": service, "host": host or ""})


def simulate_flow_instance(flow_def: Dict[str, Any], state: str, start_ts: datetime, minute: int, instance_seq: int, rate_controls: Dict[str, Any]) -> None:
    flow_id = flow_def["id"]
    lat_mult = {"p50": 1.0, "p95": 1.0}
    if state == "f":
        lat_mult = rate_controls.get("lat_flow", {}).get(flow_id, lat_mult)

    at_host = choose_at_host(flow_id, instance_seq)
    base_ctx: Dict[str, Any] = {}

    if flow_id in ("user_api_request", "user_api_request_degraded"):
        base_ctx = bound_user_api_context(state, at_host)
    elif flow_id in ("agent_heartbeat", "agent_heartbeat_reconnect"):
        base_ctx = bound_agent_context(state)
    elif flow_id in ("health_probe_ok", "health_probe_ok_degraded"):
        base_ctx = {}
    elif flow_id == "health_probe_timeout":
        base_ctx = {"backend": choose_probe_backend(minute, instance_seq)}
    else:
        base_ctx = {}

    deltas_ms: List[int] = []
    for (p50, p95) in flow_def["latency_ms"]:
        sp50 = float(p50) * float(lat_mult["p50"])
        sp95 = float(p95) * float(lat_mult["p95"])
        d = sample_lognormal_ms(sp50, sp95)
        deltas_ms.append(d)

    if flow_id in ("user_api_request", "user_api_request_degraded"):
        total_dom = SYSTEM["components"][1]["logs"]["http_access"]["state_vars"][state]["total_ms"]["v"]
        min_total, max_total = int(total_dom[0]), int(total_dom[1])
        total_ms = deltas_ms[0] + deltas_ms[1]
        total_ms = int(clamp(total_ms, min_total, max_total))
        deltas_ms[1] = max(0, total_ms - deltas_ms[0])
    elif flow_id in ("agent_heartbeat", "agent_heartbeat_reconnect"):
        dur_dom = SYSTEM["components"][1]["logs"]["agent_heartbeat"]["state_vars"][state]["dur_ms"]["v"]
        min_dur, max_dur = int(dur_dom[0]), int(dur_dom[1])
        deltas_ms[0] = int(clamp(deltas_ms[0], min_dur, max_dur))
    elif flow_id in ("health_probe_ok", "health_probe_ok_degraded"):
        dur_dom = SYSTEM["components"][1]["logs"]["health_access"]["state_vars"][state]["dur_ms"]["v"]
        min_dur, max_dur = int(dur_dom[0]), int(dur_dom[1])
        deltas_ms[0] = int(clamp(deltas_ms[0], min_dur, max_dur))
    elif flow_id == "health_probe_timeout":
        dom = SYSTEM["components"][0]["logs"]["probe_timeout"]["vars"]["timeout_ms"]["v"]
        min_t, max_t = int(dom[0]), int(dom[1])
        deltas_ms[0] = int(clamp(deltas_ms[0], min_t, max_t))

    cur = start_ts
    for idx, ref in enumerate(flow_def["emit"]):
        comp_id, log_id = ref.split(".", 1)
        cur = cur + timedelta(milliseconds=int(deltas_ms[idx]))
        ctx = dict(base_ctx)

        if (comp_id, log_id) == ("tfs_at", "http_access"):
            elapsed_ms = int(round((cur - start_ts).total_seconds() * 1000.0))
            bounds = SYSTEM["components"][1]["logs"]["http_access"]["state_vars"][state]["total_ms"]["v"]
            elapsed_ms = int(clamp(elapsed_ms, int(bounds[0]), int(bounds[1])))
            ctx["total_ms"] = elapsed_ms
            ctx["status"] = derive_user_status(state, ctx.get("req_id", ""), elapsed_ms)
        elif (comp_id, log_id) == ("tfs_at", "agent_heartbeat"):
            elapsed_ms = int(round((cur - start_ts).total_seconds() * 1000.0))
            bounds = SYSTEM["components"][1]["logs"]["agent_heartbeat"]["state_vars"][state]["dur_ms"]["v"]
            elapsed_ms = int(clamp(elapsed_ms, int(bounds[0]), int(bounds[1])))
            ctx["dur_ms"] = elapsed_ms
            res, queued = derive_agent_result_and_queue(state, elapsed_ms)
            ctx["result"] = res
            ctx["queued"] = queued
        elif (comp_id, log_id) == ("tfs_at", "health_access"):
            elapsed_ms = int(round((cur - start_ts).total_seconds() * 1000.0))
            bounds = SYSTEM["components"][1]["logs"]["health_access"]["state_vars"][state]["dur_ms"]["v"]
            elapsed_ms = int(clamp(elapsed_ms, int(bounds[0]), int(bounds[1])))
            ctx["dur_ms"] = elapsed_ms
            ctx["status"] = derive_health_status(state, elapsed_ms)
            ctx["inflight"] = derive_health_inflight(state, minute, elapsed_ms)
        elif (comp_id, log_id) == ("edge_lb", "probe_timeout"):
            ctx["timeout_ms"] = int(deltas_ms[idx])

        if comp_id == "tfs_at":
            emit_host = at_host
        elif comp_id == "edge_lb":
            emit_host = "vip-1"
        else:
            emit_host = get_component(comp_id)["hosts"][0] if get_component(comp_id).get("hosts") else ""

        emit_log(cur, comp_id, log_id, state, ctx, host=emit_host, trace_id="")


def emit_background_for_minute(minute: int, min_start: datetime, state: str, rate_controls: Optional[Dict[str, Any]]) -> None:
    for comp in SYSTEM["components"]:
        comp_id = comp["id"]
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult = 1.0
            if state == "f" and rate_controls is not None:
                mult = float(rate_controls.get("rate_bg", {}).get(f"{comp_id}.{log_id}", 1.0))
            eff = per_min * mult

            if scope == "global":
                expected = eff
                key = f"bg|{state}|{comp_id}.{log_id}|global"
                count = alloc_count(expected, key)
                times = schedule_evenly(min_start, 60.0, count, jitter_s=0.8)
                for ts in times:
                    ctx: Dict[str, Any] = {}
                    host = comp.get("hosts", [""])[0] if comp.get("hosts") else ""
                    if (comp_id, log_id) == ("edge_lb", "summary"):
                        healthy = derive_lb_healthy_backends(minute) if state == "f" else 4
                        total = 4
                        if state == "n":
                            user_rpm = FLOWS["n"]["user_api_request"]["rpm"]
                        else:
                            base = FLOWS["f"]["user_api_request_degraded"]["rpm"]
                            m = float(rate_controls.get("rate_flow", {}).get("user_api_request_degraded", 1.0)) if rate_controls else 1.0
                            user_rpm = base * m
                        rps = float(user_rpm) / 60.0
                        if state == "f":
                            rps *= clamp(healthy / 4.0, 0.2, 1.0)
                        ctx.update({"healthy": healthy, "total": total, "rps": rps})
                    elif (comp_id, log_id) == ("sps_auth", "sps_latency"):
                        p95, err, db = derive_sps_metrics(minute, state)
                        ctx.update({"p95_ms": p95, "err_rate": err, "db_cb": db})
                    emit_log(ts, comp_id, log_id, state, ctx, host=host, trace_id="")
            else:
                hosts = comp.get("hosts", [])
                for h in hosts:
                    expected = eff
                    key = f"bg|{state}|{comp_id}.{log_id}|{h}"
                    count = alloc_count(expected, key)
                    times = schedule_evenly(min_start, 60.0, count, jitter_s=0.8)
                    for ts in times:
                        ctx: Dict[str, Any] = {}
                        if (comp_id, log_id) == ("tfs_at", "mq_stats"):
                            threads, qd, batch = derive_mq_stats(state, minute)
                            ctx.update({"threads": threads, "queue_depth": qd, "batch": batch})
                        elif (comp_id, log_id) == ("tfs_at", "mq_starvation"):
                            if state == "f":
                                if minute < 27:
                                    t_lo, t_hi = 700, 1300
                                    q_lo, q_hi = 7000, 13000
                                elif minute < 33:
                                    t_lo, t_hi = 900, 1700
                                    q_lo, q_hi = 9000, 17000
                                else:
                                    t_lo, t_hi = 1100, 2000
                                    q_lo, q_hi = 12000, 20000
                            else:
                                t_lo, t_hi = 500, 700
                                q_lo, q_hi = 5000, 7000
                            ctx.update({"threads": int(rng.randint(t_lo, t_hi + 1)), "queue_depth": int(rng.randint(q_lo, q_hi + 1))})
                        emit_log(ts, comp_id, log_id, state, ctx, host=h, trace_id="")


def emit_one_shots(base_time: datetime) -> None:
    for ev in FAIL_EVENTS:
        at_min = int(ev["at_min"])
        ts0 = base_time + timedelta(minutes=at_min)
        ones = ev.get("one_shots", [])
        for os in ones:
            ref = os["ref"]
            comp_id, log_id = ref.split(".", 1)
            count = int(os["count"])
            hosts = os.get("hosts", [])
            times = schedule_evenly(ts0, 2.0, count, jitter_s=0.2)
            for i, ts in enumerate(times):
                host = hosts[i % len(hosts)] if hosts else (get_component(comp_id).get("hosts", [""])[0] if get_component(comp_id).get("hosts") else "")
                ctx: Dict[str, Any] = {}
                if (comp_id, log_id) == ("edge_lb", "backend_state"):
                    backend = "at1" if at_min == 27 else "at2"
                    ctx.update({"backend": backend, "state": "unhealthy", "fail_count": int(rng.randint(3, 10 + 1))})
                elif (comp_id, log_id) == ("ops_console", "collect_dump"):
                    ctx.update({"backend": "at1", "reason": "unhealthy"})
                elif (comp_id, log_id) == ("ops_console", "recycle_at"):
                    if at_min == 27:
                        b = "at1"
                    else:
                        b = "at2" if (i % 2 == 0) else "at3"
                    ctx.update({"backend": b, "mode": "single"})
                emit_log(ts, comp_id, log_id, "f", ctx, host=host, trace_id="")


# ----------------------------
# Main simulation loop
# ----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

flow_instance_seq: Dict[str, int] = {}


def next_seq(flow_id: str) -> int:
    v = flow_instance_seq.get(flow_id, 0)
    flow_instance_seq[flow_id] = v + 1
    return v


for minute in range(0, SCENARIO["time"]["total_minutes"]):
    min_start = BASE_TIME + timedelta(minutes=minute)
    if minute < NORM_END:
        state = "n"
        controls = None
        emit_background_for_minute(minute, min_start, state, controls)
        for flow_id, flow_def in FLOWS["n"].items():
            expected = float(flow_def["rpm"])
            count = alloc_count(expected, f"flow|{state}|{flow_id}")
            starts = schedule_evenly(min_start, 60.0, count, jitter_s=0.6)
            for st_ts in starts:
                simulate_flow_instance(flow_def, state, st_ts, minute, next_seq(flow_id), {})
    else:
        state = "f"
        controls = ACTIVE_FAIL_CONTROLS_BY_MIN.get(minute, {"rate_flow": {}, "rate_bg": {}, "lat_flow": {}})
        emit_background_for_minute(minute, min_start, state, controls)
        for flow_id, flow_def in FLOWS["f"].items():
            mult = float(controls.get("rate_flow", {}).get(flow_id, 1.0))
            expected = float(flow_def["rpm"]) * mult
            count = alloc_count(expected, f"flow|{state}|{flow_id}")
            starts = schedule_evenly(min_start, 60.0, count, jitter_s=0.6)
            for st_ts in starts:
                simulate_flow_instance(flow_def, state, st_ts, minute, next_seq(flow_id), controls)

emit_one_shots(BASE_TIME)

# ----------------------------
# Output logs.csv
# ----------------------------

df = pd.DataFrame(rows)
df.sort_values("timestamp_dt", inplace=True)
df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)
df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
df.to_csv("logs.csv", index=False)
