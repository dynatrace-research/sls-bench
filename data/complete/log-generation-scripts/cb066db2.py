import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ----------------------------
# Deterministic seeding (verifier-required)
# ----------------------------
random.seed(0)
np.random.seed(0)

# ----------------------------
# Embedded normalized inputs
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "duo1_authentication_platform",
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "edge_proxy": {
            "svc": "duo-edge",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "{method} {route} status={status} dur_ms={dur_ms} req_id={req_id} upstream=auth_api",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/auth/v2/check"]},
                        "status": {"k": "i", "v": [200, 200]},
                        "dur_ms": {"k": "i", "v": [20, 3000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "access_504": {
                    "lvl": "WARN",
                    "msg": "{method} {route} status=504 dur_ms={dur_ms} req_id={req_id} upstream=auth_api err=upstream_timeout",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/auth/v2/check"]},
                        "dur_ms": {"k": "i", "v": [2500, 9000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "access_429": {
                    "lvl": "WARN",
                    "msg": "{method} {route} status=429 dur_ms={dur_ms} req_id={req_id} upstream=auth_api err=too_many_requests",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/auth/v2/check"]},
                        "dur_ms": {"k": "i", "v": [5, 80]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "rate_limit_enabled": {
                    "lvl": "INFO",
                    "msg": "enabled request limiting: max_rpm={max_rpm} scope=duo1",
                    "vars": {"max_rpm": {"k": "i", "v": [400, 900]}},
                },
                "proxy_worker_stats": {
                    "lvl": "DEBUG",
                    "msg": "metric proxy: active_conns={active_conns} queued={queued} cpu_pct={cpu_pct}",
                    "vars": {
                        "active_conns": {"k": "i", "v": [50, 800]},
                        "queued": {"k": "i", "v": [0, 3000]},
                        "cpu_pct": {"k": "i", "v": [5, 95]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "proxy_worker_stats", "per_min": 0.5}],
                "f": [{"id": "proxy_worker_stats", "per_min": 0.5}],
            },
        },
        "auth_api": {
            "svc": "duo-auth",
            "hosts": ["auth-1", "auth-2", "auth-3", "auth-4"],
            "logs": {
                "db_acquire_wait_warn": {
                    "lvl": "WARN",
                    "msg": "db connection wait high: waited_ms={waited_ms} queue_depth={queue_depth} req_id={req_id}",
                    "vars": {
                        "waited_ms": {"k": "i", "v": [200, 6000]},
                        "queue_depth": {"k": "i", "v": [50, 9000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "auth_ok": {
                    "lvl": "INFO",
                    "msg": "auth completed: result={result} dur_ms={dur_ms} req_id={req_id} user={user_id}",
                    "vars": {
                        "result": {"k": "ch", "v": ["allow", "deny"]},
                        "dur_ms": {"k": "i", "v": [30, 7000]},
                        "req_id": {"k": "hex", "v": 16},
                        "user_id": {"k": "uuid", "v": None},
                    },
                },
                "auth_timeout": {
                    "lvl": "ERROR",
                    "msg": "auth failed: error=db_acquire_timeout waited_ms={waited_ms} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "waited_ms": {"k": "i", "v": [2000, 9000]},
                        "dur_ms": {"k": "i", "v": [2500, 9500]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "auth_rejected_queue_full": {
                    "lvl": "WARN",
                    "msg": "auth rejected: reason=queue_full queue_depth={queue_depth} max_queue={max_queue} req_id={req_id}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [500, 9000]},
                        "max_queue": {"k": "i", "v": [2000, 2000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "queue_depth_metric": {
                    "lvl": "INFO",
                    "msg": "metric db_wait_queue: depth={queue_depth} inflight={inflight} p95_db_wait_ms={p95_wait_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "queue_depth": {"k": "i", "v": [0, 40]},
                            "inflight": {"k": "i", "v": [10, 120]},
                            "p95_wait_ms": {"k": "i", "v": [1, 30]},
                        },
                        "f": {
                            "queue_depth": {"k": "i", "v": [300, 9000]},
                            "inflight": {"k": "i", "v": [80, 600]},
                            "p95_wait_ms": {"k": "i", "v": [200, 7000]},
                        },
                    },
                },
                "db_pool_metric": {
                    "lvl": "INFO",
                    "msg": "metric db_pool: in_use={in_use} max={max} waiters={waiters}",
                    "vars": {"max": {"k": "i", "v": [200, 200]}},
                    "state_vars": {
                        "n": {
                            "in_use": {"k": "i", "v": [20, 120]},
                            "waiters": {"k": "i", "v": [0, 10]},
                        },
                        "f": {
                            "in_use": {"k": "i", "v": [150, 200]},
                            "waiters": {"k": "i", "v": [50, 1500]},
                        },
                    },
                },
                "queue_flushed": {
                    "lvl": "INFO",
                    "msg": "flushed pending db-wait queue: dropped={dropped} reason={reason}",
                    "vars": {
                        "dropped": {"k": "i", "v": [500, 6000]},
                        "reason": {"k": "ch", "v": ["manual_mitigation"]},
                    },
                },
                "queue_limit_enabled": {
                    "lvl": "INFO",
                    "msg": "set db-wait queue max={max_queue} action=reject_overflow",
                    "vars": {"max_queue": {"k": "i", "v": [2000, 2000]}},
                },
            },
            "beh": {
                "n": [{"id": "queue_depth_metric", "per_min": 1.0}, {"id": "db_pool_metric", "per_min": 1.0}],
                "f": [{"id": "queue_depth_metric", "per_min": 1.0}, {"id": "db_pool_metric", "per_min": 1.0}],
            },
        },
        "auth_db": {
            "svc": "duo-mysql",
            "hosts": ["db-1", "db-2"],
            "logs": {
                "db_conn_stats": {
                    "lvl": "INFO",
                    "msg": "metric db: active_conns={active} max_conns={max} threads_running={threads} qps={qps}",
                    "vars": {"max": {"k": "i", "v": [1000, 1000]}},
                    "state_vars": {
                        "n": {
                            "active": {"k": "i", "v": [150, 450]},
                            "threads": {"k": "i", "v": [10, 80]},
                            "qps": {"k": "i", "v": [300, 1200]},
                        },
                        "f": {
                            "active": {"k": "i", "v": [600, 980]},
                            "threads": {"k": "i", "v": [80, 300]},
                            "qps": {"k": "i", "v": [600, 2200]},
                        },
                    },
                },
                "slow_query_sample": {
                    "lvl": "WARN",
                    "msg": "slow query sample: dur_ms={dur_ms} rows={rows} query={query}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [200, 7000]},
                        "rows": {"k": "i", "v": [1, 8000]},
                        "query": {"k": "ch", "v": ["SELECT user_factors", "SELECT device_tokens", "UPDATE auth_sessions"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "db_conn_stats", "per_min": 1.0}, {"id": "slow_query_sample", "per_min": 0.05}],
                "f": [{"id": "db_conn_stats", "per_min": 1.0}, {"id": "slow_query_sample", "per_min": 0.5}],
            },
        },
    },
    "flows": {
        "n": {
            "auth_check_normal": {
                "rpm": 550.0,
                "emit": ["auth_api.auth_ok", "edge_proxy.access_ok"],
                "latency_ms": [[60, 180], [65, 200]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            }
        },
        "f": {
            "auth_check_ok_queued": {
                "rpm": 560.0,
                "emit": ["auth_api.db_acquire_wait_warn", "auth_api.auth_ok", "edge_proxy.access_ok"],
                "latency_ms": [[400, 1500], [450, 2200], [480, 2300]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "auth_check_ok_fast": {
                "rpm": 200.0,
                "emit": ["auth_api.auth_ok", "edge_proxy.access_ok"],
                "latency_ms": [[90, 250], [100, 280]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "auth_check_timeout": {
                "rpm": 140.0,
                "emit": ["auth_api.auth_timeout", "edge_proxy.access_504"],
                "latency_ms": [[3000, 6500], [3000, 6800]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "auth_check_rejected_queue_full": {
                "rpm": 120.0,
                "emit": ["auth_api.auth_rejected_queue_full", "edge_proxy.access_429"],
                "latency_ms": [[5, 15], [10, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "duo1_capacity_queue_backup_20180829",
    "time": {"total_minutes": 58, "phases": {"n": {"start_min": 0, "end_min": 26}, "f": {"start_min": 26, "end_min": 58}}},
    "events": [
        {
            "order": 1,
            "at_min": 26,
            "rate_multipliers": {"auth_check_ok_fast": 0.0, "auth_check_rejected_queue_full": 0.0},
            "latency_multipliers": {},
            "one_shots": [],
        },
        {
            "order": 2,
            "at_min": 34,
            "rate_multipliers": {
                "auth_check_ok_queued": 0.8,
                "auth_check_timeout": 0.8,
                "auth_check_ok_fast": 0.0,
                "auth_check_rejected_queue_full": 0.0,
            },
            "latency_multipliers": {},
            "one_shots": [{"ref": "edge_proxy.rate_limit_enabled", "count": 1, "hosts": ["edge-1"]}],
        },
        {
            "order": 3,
            "at_min": 46,
            "rate_multipliers": {
                "auth_check_ok_queued": 0.4,
                "auth_check_ok_fast": 0.8,
                "auth_check_timeout": 0.2,
                "auth_check_rejected_queue_full": 0.3,
            },
            "latency_multipliers": {
                "auth_check_ok_queued": {"p50": 0.8, "p95": 0.7},
                "auth_check_timeout": {"p50": 0.7, "p95": 0.7},
            },
            "one_shots": [
                {"ref": "auth_api.queue_flushed", "count": 4, "hosts": ["auth-1", "auth-2", "auth-3", "auth-4"]},
                {"ref": "auth_api.queue_limit_enabled", "count": 4, "hosts": ["auth-1", "auth-2", "auth-3", "auth-4"]},
            ],
        },
    ],
}

# ----------------------------
# Deterministic helpers
# ----------------------------

BASE_TIME = datetime(2018, 8, 29, 0, 0, 0, tzinfo=timezone.utc)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def stable_int(s: str) -> int:
    return int(md5_hex(s)[:16], 16)


def stable_u01(s: str) -> float:
    return (stable_int(s) % 10_000_000) / 10_000_000.0


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


def uuid_from_key(key: str) -> str:
    h = md5_hex(key)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def norm_ppf(p: float) -> float:
    # Acklam approximation; deterministic and sufficient for this simulator
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow

    if p <= 0.0:
        return -float("inf")
    if p >= 1.0:
        return float("inf")

    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )

    q = p - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
        / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


def lognormal_quantile(p50: float, p95: float, p: float) -> float:
    # parameterize by p50 and p95 (approx); deterministic via p selection
    if p50 <= 0:
        p50 = 1.0
    if p95 < p50:
        p95 = p50 * 1.01
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.645 if p95 > 0 else 0.0
    z = norm_ppf(p)
    return math.exp(mu + sigma * z)


def pick_from_domain(dom: Dict[str, Any], key: str, bias: float = 0.0) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        arr = v
        idx = stable_int(key) % len(arr)
        return arr[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        u = stable_u01(key)
        if bias > 0:
            u = 1.0 - (1.0 - u) ** (1.0 + 3.0 * bias)
        elif bias < 0:
            u = u ** (1.0 + 3.0 * (-bias))
        return lo + int(u * (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if lo == hi:
            return lo
        u = stable_u01(key)
        return lo + u * (hi - lo)
    if k == "hex":
        ln = int(v)
        return md5_hex(key)[:ln]
    if k == "uuid":
        return uuid_from_key(key)
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return str(v) if v is not None else ""
    return ""


@dataclass
class LogRow:
    ts: datetime
    level: str
    message: str
    trace_id: str
    service: str
    host: str


# ----------------------------
# Model lookups
# ----------------------------

def get_log_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][component_id]["logs"][log_id]


def component_identity(component_id: str) -> Tuple[str, List[str]]:
    c = SYSTEM["components"][component_id]
    return c.get("svc", "") or "", c.get("hosts", []) or []


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, logid = ref.split(".", 1)
    return comp, logid


# ----------------------------
# Control state (persistent events)
# ----------------------------

def build_event_index() -> Dict[int, Dict[str, Any]]:
    idx = {}
    for e in SCENARIO["events"]:
        idx[int(e["at_min"])] = e
    return idx


def default_rate_mults() -> Dict[str, float]:
    return {fid: 1.0 for fid in SYSTEM["flows"]["f"].keys()}


def default_latency_mults() -> Dict[str, Dict[str, float]]:
    return {fid: {"p50": 1.0, "p95": 1.0} for fid in SYSTEM["flows"]["f"].keys()}


# ----------------------------
# Scheduling / allocation
# ----------------------------

class CarryRounding:
    def __init__(self):
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return n


def minute_bounds(minute: int) -> Tuple[datetime, datetime]:
    start = BASE_TIME + timedelta(minutes=minute)
    end = BASE_TIME + timedelta(minutes=minute + 1)
    return start, end


def choose_host(component_id: str, seed_key: str, preferred: Optional[str] = None) -> str:
    _, hosts = component_identity(component_id)
    if not hosts:
        return ""
    if preferred is not None and preferred in hosts:
        return preferred
    idx = stable_int(seed_key) % len(hosts)
    return hosts[idx]


def render_log(component_id: str, log_id: str, state: str, key: str, overrides: Dict[str, Any], bias: float = 0.0) -> Tuple[str, str]:
    tmpl = get_log_template(component_id, log_id)
    vars_dom = dict(tmpl.get("vars", {}) or {})
    state_vars = tmpl.get("state_vars", {}) or {}
    if state in state_vars:
        for k, v in state_vars[state].items():
            vars_dom[k] = v

    vals: Dict[str, Any] = {}
    for name, dom in vars_dom.items():
        if name in overrides:
            vals[name] = overrides[name]
        else:
            vals[name] = pick_from_domain(dom, f"{key}|{component_id}.{log_id}|{name}", bias=bias)

    for name, value in overrides.items():
        vals[name] = value

    msg = tmpl["msg"].format(**vals)
    return tmpl["lvl"], msg


# ----------------------------
# Flow timing planning
# ----------------------------

def sample_abs_offsets_ms(lat_pairs: List[List[float]], key: str, p_lo: float, p_hi: float) -> List[int]:
    """
    YAML latency_ms pairs are treated here as *cumulative offsets* (ms since attempt start) for each emitted log.
    We sample an absolute offset per step and enforce monotone non-decreasing offsets.
    """
    offsets: List[int] = []
    prev = 0
    for i, (p50, p95) in enumerate(lat_pairs):
        u = stable_u01(f"{key}|abs_offset|{i}")
        p = p_lo + (p_hi - p_lo) * u
        x = lognormal_quantile(float(p50), float(p95), p)
        ms = max(1, int(round(x)))
        if i > 0 and ms <= prev:
            ms = prev + 1
        offsets.append(ms)
        prev = ms
    return offsets


def plan_flow_offsets(flow_id: str, lat_pairs_scaled: List[List[float]], key: str) -> List[int]:
    # Keep deterministic, avoid post-hoc total clamping (offsets already align to log domains in this model).
    if flow_id in ("auth_check_normal", "auth_check_ok_fast"):
        return sample_abs_offsets_ms(lat_pairs_scaled, key, p_lo=0.45, p_hi=0.78)

    if flow_id == "auth_check_ok_queued":
        return sample_abs_offsets_ms(lat_pairs_scaled, key, p_lo=0.55, p_hi=0.88)

    if flow_id == "auth_check_timeout":
        offsets = sample_abs_offsets_ms(lat_pairs_scaled, key, p_lo=0.65, p_hi=0.92)
        # Ensure coherence with dur_ms minimums (avoid message/timestamp mismatch without total caps).
        offsets[0] = max(offsets[0], 2500)  # auth_timeout dur_ms min
        offsets[1] = max(offsets[1], 2500)  # access_504 dur_ms min
        if offsets[1] <= offsets[0]:
            offsets[1] = offsets[0] + 1
        return offsets

    if flow_id == "auth_check_rejected_queue_full":
        offsets = sample_abs_offsets_ms(lat_pairs_scaled, key, p_lo=0.50, p_hi=0.90)
        offsets[0] = max(offsets[0], 1)
        offsets[1] = max(offsets[1], 5)  # access_429 dur_ms min
        if offsets[1] <= offsets[0]:
            offsets[1] = offsets[0] + 1
        return offsets

    return sample_abs_offsets_ms(lat_pairs_scaled, key, p_lo=0.55, p_hi=0.85)


# ----------------------------
# Simulation
# ----------------------------

def state_for_minute(m: int) -> str:
    return "n" if m < SCENARIO["time"]["phases"]["n"]["end_min"] else "f"


def mitigated_factor(m: int) -> float:
    return 1.0 if m >= 46 else 0.0


def emit_background_for_minute(minute: int, cr: CarryRounding, rows: List[LogRow]):
    state = state_for_minute(minute)
    m_start, _m_end = minute_bounds(minute)

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, [])
        if not beh:
            continue

        svc, hosts = component_identity(comp_id)
        for emit_def in beh:
            log_id = emit_def["id"]
            per_min = float(emit_def["per_min"])
            scope = emit_def.get("scope", "per_host")
            if scope not in ("per_host", "global"):
                scope = "per_host"

            if scope == "global":
                key = f"bg|{comp_id}.{log_id}|global|state={state}"
                n = cr.alloc(key, per_min)
                for i in range(n):
                    u = stable_u01(f"{key}|{minute}|{i}")
                    offset_ms = int(1000 + u * 58_000)
                    ts = m_start + timedelta(milliseconds=offset_ms)
                    bias = 0.35 if state == "f" else -0.25
                    lvl, msg = render_log(comp_id, log_id, state, f"{key}|{minute}|{i}", overrides={}, bias=bias)
                    host = "" if not hosts else choose_host(comp_id, f"{key}|host|{minute}|{i}")
                    rows.append(LogRow(ts=ts, level=lvl, message=msg, trace_id="", service=svc, host=host))
            else:
                for host in hosts:
                    key = f"bg|{comp_id}.{log_id}|host={host}|state={state}"
                    n = cr.alloc(key, per_min)
                    for i in range(n):
                        u = stable_u01(f"{key}|{minute}|{i}")
                        offset_ms = int(1000 + u * 58_000)
                        ts = m_start + timedelta(milliseconds=offset_ms)
                        if state == "n":
                            bias = -0.30
                        else:
                            bias = 0.45 - 0.15 * mitigated_factor(minute)
                        lvl, msg = render_log(comp_id, log_id, state, f"{key}|{minute}|{i}", overrides={}, bias=bias)
                        rows.append(LogRow(ts=ts, level=lvl, message=msg, trace_id="", service=svc, host=host))


def apply_flow_latency_mults(flow_id: str, lat_pairs: List[List[float]], lat_mult: Dict[str, Dict[str, float]]) -> List[List[float]]:
    m = lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
    p50m, p95m = float(m.get("p50", 1.0)), float(m.get("p95", 1.0))
    return [[float(p50) * p50m, float(p95) * p95m] for p50, p95 in lat_pairs]


def simulate_flow_instance(
    flow_id: str,
    flow_def: Dict[str, Any],
    state: str,
    start_ts: datetime,
    instance_key: str,
    minute_of_start: int,
    rows: List[LogRow],
):
    trace_on = bool(SYSTEM.get("tracing", {}).get("on", False))
    trace_id = md5_hex(f"trace|{instance_key}") if (trace_on and flow_def.get("trace", False)) else ""

    # Component-local host stickiness within the chain
    edge_host = choose_host("edge_proxy", f"{instance_key}|edge_host")
    auth_host = choose_host("auth_api", f"{instance_key}|auth_host")

    req_id = md5_hex(f"req|{instance_key}")[:16]

    offsets = flow_def["_planned_offsets_ms"]
    emit_refs = flow_def["emit"]

    for step_i, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        elapsed_ms = int(offsets[step_i])
        ts = start_ts + timedelta(milliseconds=elapsed_ms)

        svc, hosts = component_identity(comp_id)
        if comp_id == "edge_proxy":
            host = edge_host
        elif comp_id == "auth_api":
            host = auth_host
        else:
            host = "" if not hosts else choose_host(comp_id, f"{instance_key}|{comp_id}|host")

        overrides: Dict[str, Any] = {}

        if comp_id == "auth_api" and log_id == "db_acquire_wait_warn":
            overrides["waited_ms"] = clamp_int(elapsed_ms, 200, 6000)
            base = 500 + int(1.2 * overrides["waited_ms"])
            stage = 1.0 - 0.4 * mitigated_factor(minute_of_start)
            qd = int(base * stage + 2000 * (1.0 - stage))
            overrides["queue_depth"] = clamp_int(qd, 50, 9000)
            overrides["req_id"] = req_id

        elif comp_id == "auth_api" and log_id == "auth_ok":
            overrides["dur_ms"] = clamp_int(elapsed_ms, 30, 7000)
            overrides["req_id"] = req_id
            overrides["result"] = "allow" if (stable_int(f"{instance_key}|result") % 10) < 8 else "deny"
            overrides["user_id"] = uuid_from_key(f"{instance_key}|user")

        elif comp_id == "auth_api" and log_id == "auth_timeout":
            overrides["dur_ms"] = clamp_int(elapsed_ms, 2500, 9500)
            proc = 20 + (stable_int(f"{instance_key}|timeout_proc") % 61)
            waited = max(2000, overrides["dur_ms"] - proc)
            overrides["waited_ms"] = clamp_int(waited, 2000, 9000)
            overrides["req_id"] = req_id

        elif comp_id == "auth_api" and log_id == "auth_rejected_queue_full":
            overrides["max_queue"] = 2000
            qd = 2000 + (stable_int(f"{instance_key}|rej_qd") % (9000 - 2000 + 1))
            if minute_of_start >= 46:
                qd = 2000 + (stable_int(f"{instance_key}|rej_qd2") % 2500)
            overrides["queue_depth"] = clamp_int(qd, 500, 9000)
            overrides["req_id"] = req_id

        elif comp_id == "edge_proxy" and log_id in ("access_ok", "access_504", "access_429"):
            # dur_ms reflects end-to-end time since request start
            overrides["dur_ms"] = elapsed_ms
            overrides["req_id"] = req_id
            overrides["method"] = "POST"
            overrides["route"] = "/auth/v2/check"
            if log_id == "access_ok":
                overrides["status"] = 200

        bias = -0.10 if state == "n" else 0.20 - 0.10 * mitigated_factor(minute_of_start)
        lvl, msg = render_log(comp_id, log_id, state, f"{instance_key}|{step_i}|{comp_id}.{log_id}", overrides=overrides, bias=bias)
        rows.append(LogRow(ts=ts, level=lvl, message=msg, trace_id=trace_id, service=svc, host=host))


def emit_one_shots_for_event(event: Dict[str, Any], rows: List[LogRow]):
    at_min = int(event["at_min"])
    base_ts = BASE_TIME + timedelta(minutes=at_min)
    for os_def in event.get("one_shots", []) or []:
        ref = os_def["ref"]
        count = int(os_def["count"])
        allowed_hosts = os_def.get("hosts", None)
        comp_id, log_id = parse_ref(ref)
        svc, hosts = component_identity(comp_id)

        for i in range(count):
            jitter_ms = 50 + (stable_int(f"oneshot|{at_min}|{ref}|{i}") % 900)
            ts = base_ts + timedelta(milliseconds=jitter_ms + i)
            if allowed_hosts:
                host = allowed_hosts[i % len(allowed_hosts)]
            else:
                host = "" if not hosts else hosts[i % len(hosts)]
            lvl, msg = render_log(comp_id, log_id, "f", f"oneshot|{at_min}|{ref}|{i}", overrides={}, bias=0.0)
            rows.append(LogRow(ts=ts, level=lvl, message=msg, trace_id="", service=svc, host=host))


def simulate():
    total_minutes = int(SCENARIO["time"]["total_minutes"])
    event_by_min = build_event_index()

    rate_mult = default_rate_mults()
    lat_mult = default_latency_mults()

    bg_round = CarryRounding()
    flow_round = CarryRounding()

    rows: List[LogRow] = []

    flow_instance_seq: Dict[str, int] = {fid: 0 for fid in {**SYSTEM["flows"]["n"], **SYSTEM["flows"]["f"]}.keys()}

    for minute in range(total_minutes):
        if minute in event_by_min:
            ev = event_by_min[minute]
            emit_one_shots_for_event(ev, rows)

            for fid, mult in (ev.get("rate_multipliers", {}) or {}).items():
                rate_mult[fid] = float(mult)
            for fid, mm in (ev.get("latency_multipliers", {}) or {}).items():
                lat_mult.setdefault(fid, {"p50": 1.0, "p95": 1.0})
                lat_mult[fid]["p50"] = float(mm.get("p50", lat_mult[fid].get("p50", 1.0)))
                lat_mult[fid]["p95"] = float(mm.get("p95", lat_mult[fid].get("p95", 1.0)))

        emit_background_for_minute(minute, bg_round, rows)

        state = state_for_minute(minute)
        m_start, _m_end = minute_bounds(minute)

        if state == "n":
            for flow_id, flow_def in SYSTEM["flows"]["n"].items():
                expected = float(flow_def["rpm"])
                n = flow_round.alloc(f"flow|n|{flow_id}", expected)
                if n <= 0:
                    continue
                for i in range(n):
                    seq = flow_instance_seq[flow_id]
                    flow_instance_seq[flow_id] += 1

                    frac = (i + 0.5) / max(1, n)
                    base = m_start + timedelta(seconds=frac * 60.0)
                    jitter = (stable_int(f"start|{minute}|{flow_id}|{seq}") % 41) - 20
                    start_ts = base + timedelta(milliseconds=jitter)

                    lat_pairs_scaled = flow_def["latency_ms"]
                    inst_key = f"{flow_id}|{minute}|{seq}"
                    planned = plan_flow_offsets(flow_id, lat_pairs_scaled, inst_key)
                    flow_def_local = dict(flow_def)
                    flow_def_local["_planned_offsets_ms"] = planned

                    simulate_flow_instance(flow_id, flow_def_local, "n", start_ts, inst_key, minute, rows)

        else:
            for flow_id, flow_def in SYSTEM["flows"]["f"].items():
                mult = float(rate_mult.get(flow_id, 1.0))
                expected = float(flow_def["rpm"]) * mult
                n = flow_round.alloc(f"flow|f|{flow_id}", expected)
                if n <= 0:
                    continue

                lat_pairs_scaled = apply_flow_latency_mults(flow_id, flow_def["latency_ms"], lat_mult)

                for i in range(n):
                    seq = flow_instance_seq[flow_id]
                    flow_instance_seq[flow_id] += 1

                    frac = (i + 0.5) / max(1, n)
                    base = m_start + timedelta(seconds=frac * 60.0)
                    jitter = (stable_int(f"start|{minute}|{flow_id}|{seq}") % 61) - 30
                    start_ts = base + timedelta(milliseconds=jitter)

                    inst_key = f"{flow_id}|{minute}|{seq}"
                    planned = plan_flow_offsets(flow_id, lat_pairs_scaled, inst_key)
                    flow_def_local = dict(flow_def)
                    flow_def_local["_planned_offsets_ms"] = planned

                    simulate_flow_instance(flow_id, flow_def_local, "f", start_ts, inst_key, minute, rows)

    rows.sort(key=lambda r: (r.ts, r.service, r.host, r.level, r.message))

    df = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r.ts) for r in rows],
            "level": [r.level for r in rows],
            "message": [r.message for r in rows],
            "trace_id": [r.trace_id for r in rows],
            "service": [r.service for r in rows],
            "host": [r.host for r in rows],
        }
    )

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    simulate()
