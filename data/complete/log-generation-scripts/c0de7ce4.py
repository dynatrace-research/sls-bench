import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Embedded normalized model data
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "incident_app_heroku_pubsub_poison_pill"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "heroku_runtime": {
            "svc": "heroku",
            "hosts": ["router-1", "dyno-manager-1"],
            "logs": {
                "router_200": {
                    "lvl": "INFO",
                    "msg": "heroku-router: method={method} path={path} status=200 service={service_ms}ms dyno=web.1 request_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "path": {"k": "ch", "v": ["/slack/actions", "/api/v1/incidents", "/dashboard"]},
                        "service_ms": {"k": "i", "v": [5, 1200]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "router_h10": {
                    "lvl": "ERROR",
                    "msg": 'heroku-router: code=H10 desc="App crashed" method={method} path={path} status=503 request_id={req_id} dyno=web.1',
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "path": {"k": "ch", "v": ["/slack/actions", "/api/v1/incidents", "/dashboard"]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "dyno_exit": {
                    "lvl": "ERROR",
                    "msg": "heroku-dyno: Process exited status={exit_status} signal={signal} dyno=web.1",
                    "vars": {
                        "exit_status": {"k": "i", "v": [1, 137]},
                        "signal": {"k": "ch", "v": ["SIGABRT", "SIGKILL", "SIGSEGV"]},
                    },
                },
                "dyno_start": {
                    "lvl": "INFO",
                    "msg": "heroku-dyno: Starting process dyno=web.1 release={release}",
                    "vars": {"release": {"k": "ch", "v": ["v2022.11.18.2"]}},
                },
                "crash_loop_backoff": {
                    "lvl": "WARN",
                    "msg": "heroku-dyno: Crash loop detected; applying restart cooloff={cooloff_s}s dyno=web.1",
                    "vars": {"cooloff_s": {"k": "i", "v": [60, 1200]}},
                },
                "logplex_dropped": {
                    "lvl": "WARN",
                    "msg": "logplex: buffer overflow; dropped={dropped_lines} lines source=app/web.1",
                    "vars": {"dropped_lines": {"k": "i", "v": [200, 20000]}},
                },
                "log_rate_limited": {
                    "lvl": "WARN",
                    "msg": "logplex: rate-limited; suppressed={suppressed_lines} lines source=app/web.1",
                    "vars": {"suppressed_lines": {"k": "i", "v": [50, 5000]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "dyno_start", "per_min": 0.01, "scope": "global"},
                    {"id": "logplex_dropped", "per_min": 0.02, "scope": "global"},
                    {"id": "log_rate_limited", "per_min": 0.0, "scope": "global"},
                ],
                "f": [
                    {"id": "dyno_start", "per_min": 2.0, "scope": "global"},
                    {"id": "logplex_dropped", "per_min": 0.2, "scope": "global"},
                    {"id": "log_rate_limited", "per_min": 0.1, "scope": "global"},
                ],
            },
        },
        "incident_app": {
            "svc": "incident-app",
            "hosts": ["web.1"],
            "logs": {
                "http_200": {
                    "lvl": "INFO",
                    "msg": "http: {method} {route} status=200 dur={dur_ms}ms user={user_id} request_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/slack/actions", "/api/v1/incidents", "/dashboard"]},
                        "dur_ms": {"k": "i", "v": [10, 3000]},
                        "user_id": {"k": "hex", "v": 8},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "worker_msg_received": {
                    "lvl": "INFO",
                    "msg": "pubsub: received subscription={sub} msg_id={msg_id} delivery_attempt={attempt}",
                    "vars": {
                        "sub": {"k": "ch", "v": ["events-sub", "inc-nudges-sub"]},
                        "msg_id": {"k": "hex", "v": 24},
                        "attempt": {"k": "i", "v": [1, 50]},
                    },
                },
                "welcome_msg_received": {
                    "lvl": "INFO",
                    "msg": "pubsub: received subscription=inc-welcome-sub msg_id={msg_id} delivery_attempt={attempt}",
                    "vars": {
                        "msg_id": {"k": "hex", "v": 24},
                        "attempt": {"k": "i", "v": [1, 50]},
                    },
                },
                "worker_msg_acked": {
                    "lvl": "INFO",
                    "msg": "pubsub: acked subscription={sub} msg_id={msg_id} dur={dur_ms}ms",
                    "vars": {
                        "sub": {"k": "ch", "v": ["events-sub", "inc-nudges-sub"]},
                        "msg_id": {"k": "hex", "v": 24},
                        "dur_ms": {"k": "i", "v": [5, 5000]},
                    },
                },
                "panic_runtime": {
                    "lvl": "CRITICAL",
                    "msg": "panic: runtime error={panic} goroutines={goroutines}",
                    "vars": {
                        "panic": {"k": "ch", "v": ["invalid memory address or nil pointer dereference", "index out of range"]},
                        "goroutines": {"k": "i", "v": [150, 400]},
                    },
                },
                "health_tick": {
                    "lvl": "DEBUG",
                    "msg": "health: tick uptime_s={uptime_s} goroutines={goroutines}",
                    "vars": {
                        "uptime_s": {"k": "i", "v": [5, 200000]},
                        "goroutines": {"k": "i", "v": [120, 400]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "health_tick", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "health_tick", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "gcp_pubsub": {
            "svc": "pubsub",
            "hosts": ["pubsub-1"],
            "logs": {
                "deliver_msg": {
                    "lvl": "INFO",
                    "msg": "pubsub: deliver subscription={sub} msg_id={msg_id} attempt={attempt}",
                    "vars": {
                        "sub": {"k": "ch", "v": ["inc-welcome-sub", "events-sub", "inc-nudges-sub"]},
                        "msg_id": {"k": "hex", "v": 24},
                        "attempt": {"k": "i", "v": [1, 50]},
                    },
                }
            },
            "beh": {"n": [], "f": []},
        },
        "postgres": {
            "svc": "postgres",
            "hosts": ["pg-primary-1"],
            "logs": {
                "checkpoint_complete": {
                    "lvl": "INFO",
                    "msg": "postgres: checkpoint complete write_ms={write_ms} buffers={buffers}",
                    "vars": {
                        "write_ms": {"k": "i", "v": [50, 2000]},
                        "buffers": {"k": "i", "v": [1000, 200000]},
                    },
                }
            },
            "beh": {
                "n": [{"id": "checkpoint_complete", "per_min": 0.05, "scope": "global"}],
                "f": [{"id": "checkpoint_complete", "per_min": 0.05, "scope": "global"}],
            },
        },
        "monitoring": {
            "svc": "monitoring",
            "hosts": ["monitor-1"],
            "logs": {
                "pubsub_welcome_backlog_ok": {
                    "lvl": "INFO",
                    "msg": "metrics: pubsub subscription=inc-welcome-sub unacked={unacked} oldest_age_s={oldest_s}",
                    "vars": {
                        "unacked": {"k": "i", "v": [0, 3]},
                        "oldest_s": {"k": "i", "v": [0, 30]},
                    },
                },
                "pubsub_welcome_backlog_elevated": {
                    "lvl": "WARN",
                    "msg": "metrics: pubsub subscription=inc-welcome-sub unacked={unacked} oldest_age_s={oldest_s}",
                    "vars": {
                        "unacked": {"k": "i", "v": [50, 500]},
                        "oldest_s": {"k": "i", "v": [300, 2400]},
                    },
                },
            },
            "beh": {
                "n": [
                    {"id": "pubsub_welcome_backlog_ok", "per_min": 1.0, "scope": "global"},
                    {"id": "pubsub_welcome_backlog_elevated", "per_min": 0.0, "scope": "global"},
                ],
                "f": [
                    {"id": "pubsub_welcome_backlog_ok", "per_min": 1.0, "scope": "global"},
                    {"id": "pubsub_welcome_backlog_elevated", "per_min": 1.0, "scope": "global"},
                ],
            },
        },
        "ops_console": {
            "svc": "ops",
            "hosts": ["oncall-laptop-1"],
            "logs": {
                "manual_dyno_reboot": {
                    "lvl": "INFO",
                    "msg": "ops: manual dyno reboot app=incident-app dyno=web.1 actor={actor} reason={reason}",
                    "vars": {
                        "actor": {"k": "ch", "v": ["oncall", "responder"]},
                        "reason": {"k": "ch", "v": ["H10_app_crashed", "crash_loop"]},
                    },
                },
                "purge_subscription": {
                    "lvl": "WARN",
                    "msg": "ops: purged pubsub subscription={sub} removed_messages={removed} actor={actor}",
                    "vars": {
                        "sub": {"k": "ch", "v": ["inc-welcome-sub", "inc-nudges-sub"]},
                        "removed": {"k": "i", "v": [1, 500]},
                        "actor": {"k": "ch", "v": ["oncall", "responder"]},
                    },
                },
                "disable_feature": {
                    "lvl": "INFO",
                    "msg": "ops: set feature_flag={flag} enabled=false actor={actor}",
                    "vars": {
                        "flag": {"k": "ch", "v": ["incident_nudges", "welcome_messages"]},
                        "actor": {"k": "ch", "v": ["oncall", "responder"]},
                    },
                },
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": [
            {
                "id": "web_interaction_ok",
                "rpm": 500.0,
                "emit": ["incident_app.http_200", "heroku_runtime.router_200"],
                "latency_ms": [[60, 300], [5, 30]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "pubsub_consume_ok",
                "rpm": 60.0,
                "emit": ["gcp_pubsub.deliver_msg", "incident_app.worker_msg_received", "incident_app.worker_msg_acked"],
                "latency_ms": [[1, 5], [2, 10], [20, 250]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "web_interaction_ok_degraded",
                "rpm": 350.0,
                "emit": ["incident_app.http_200", "heroku_runtime.router_200"],
                "latency_ms": [[120, 700], [5, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "web_interaction_h10",
                "rpm": 150.0,
                "emit": ["heroku_runtime.router_h10"],
                "latency_ms": [[2, 15]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "pubsub_consume_ok_f",
                "rpm": 20.0,
                "emit": ["gcp_pubsub.deliver_msg", "incident_app.worker_msg_received", "incident_app.worker_msg_acked"],
                "latency_ms": [[1, 8], [2, 15], [30, 800]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "pubsub_welcome_consume_panics",
                "rpm": 2.5,
                "emit": [
                    "gcp_pubsub.deliver_msg",
                    "incident_app.welcome_msg_received",
                    "incident_app.panic_runtime",
                    "heroku_runtime.dyno_exit",
                ],
                "latency_ms": [[1, 8], [2, 10], [5, 40], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "incident_2022_11_18_heroku_h10_poison_pubsub_welcome"},
    "time": {"total_minutes": 56, "phases": {"n": {"start_min": 0, "end_min": 24}, "f": {"start_min": 24, "end_min": 56}}},
    "events": [
        {
            "order": 1,
            "at_min": 24,
            "rate_multipliers": {
                "monitoring.pubsub_welcome_backlog_ok": 0.0,
                "monitoring.pubsub_welcome_backlog_elevated": 1.0,
                "incident_app.health_tick": 0.2,
                "heroku_runtime.dyno_start": 1.0,
                "heroku_runtime.logplex_dropped": 2.0,
                "heroku_runtime.log_rate_limited": 0.0,
                "web_interaction_h10": 1.2,
                "web_interaction_ok_degraded": 0.8,
            },
            "latency_multipliers": {"web_interaction_ok_degraded": {"p50": 1.0, "p95": 1.2}},
            "one_shots": [],
        },
        {
            "order": 2,
            "at_min": 26,
            "rate_multipliers": {},
            "latency_multipliers": {},
            "one_shots": [{"ref": "ops_console.manual_dyno_reboot", "count": 1, "hosts": ["oncall-laptop-1"]}],
        },
        {
            "order": 3,
            "at_min": 31,
            "rate_multipliers": {
                "pubsub_welcome_consume_panics": 0.4,
                "web_interaction_h10": 1.6,
                "web_interaction_ok_degraded": 0.7,
                "heroku_runtime.dyno_start": 0.4,
                "heroku_runtime.logplex_dropped": 6.0,
                "heroku_runtime.log_rate_limited": 5.0,
                "incident_app.health_tick": 0.1,
            },
            "latency_multipliers": {"web_interaction_ok_degraded": {"p50": 1.1, "p95": 1.6}},
            "one_shots": [{"ref": "heroku_runtime.crash_loop_backoff", "count": 1, "hosts": ["dyno-manager-1"]}],
        },
        {
            "order": 4,
            "at_min": 53,
            "rate_multipliers": {
                "pubsub_welcome_consume_panics": 0.0,
                "web_interaction_h10": 0.1,
                "web_interaction_ok_degraded": 1.2,
                "pubsub_consume_ok_f": 1.3,
                "monitoring.pubsub_welcome_backlog_elevated": 0.0,
                "monitoring.pubsub_welcome_backlog_ok": 1.0,
                "heroku_runtime.dyno_start": 0.0,
                "heroku_runtime.logplex_dropped": 0.2,
                "heroku_runtime.log_rate_limited": 0.2,
                "incident_app.health_tick": 1.0,
            },
            "latency_multipliers": {"web_interaction_ok_degraded": {"p50": 0.9, "p95": 0.9}},
            "one_shots": [
                {"ref": "ops_console.purge_subscription", "count": 2, "hosts": ["oncall-laptop-1"]},
                {"ref": "ops_console.disable_feature", "count": 2, "hosts": ["oncall-laptop-1"]},
            ],
        },
    ],
}


# -----------------------------
# Deterministic helpers
# -----------------------------
BASE_TIME = datetime(2022, 11, 18, 0, 0, 0, tzinfo=timezone.utc)


def stable_hash_int(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def stable_u01(key: str, lo: float = 0.02, hi: float = 0.98) -> float:
    x = stable_hash_int(key) % 1_000_000
    u = (x + 0.5) / 1_000_000.0
    if u < lo:
        u = lo
    if u > hi:
        u = hi
    return u


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0:
        return base
    u = stable_u01(f"round:{key}", lo=0.0, hi=1.0)
    return base + (1 if u < frac else 0)


def norm_ppf(u: float) -> float:
    """Deterministic inverse standard normal CDF approximation (Acklam)."""
    u = float(u)
    if u <= 0.0:
        return -float("inf")
    if u >= 1.0:
        return float("inf")

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
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]

    plow = 0.02425
    phigh = 1.0 - plow

    if u < plow:
        q = math.sqrt(-2.0 * math.log(u))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den
    if u > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - u))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -(num / den)

    q = u - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def lognormal_from_p50_p95(p50: float, p95: float, u: float) -> float:
    p50 = max(0.001, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    z = norm_ppf(u)
    return math.exp(mu + sigma * z)


def sample_pos_ms(p50: float, p95: float, key: str, soft_cap_mult: float = 3.0, min_ms: int = 1) -> int:
    """
    Sample a positive-ish latency in ms. Enforce a small positive minimum to avoid
    contradicting templates that require dur_ms >= 5/10, etc. Per-field clamping is
    done separately when binding message timing fields.
    """
    u = stable_u01(f"ms:{key}")
    x = lognormal_from_p50_p95(p50, p95, u)
    cap = soft_cap_mult * float(p95)
    if x > cap:
        u2 = stable_u01(f"cap:{key}", lo=0.0, hi=1.0)
        x = cap * (0.85 + 0.15 * u2)
    return max(int(min_ms), int(round(x)))


def gen_hex(n: int, key: str) -> str:
    out = []
    counter = 0
    while len("".join(out)) < n:
        digest = hashlib.md5(f"{key}:{counter}".encode("utf-8")).hexdigest()
        out.append(digest)
        counter += 1
    return "".join(out)[:n]


def choose_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        idx = stable_hash_int(f"ch:{key}") % len(v)
        return v[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        span = hi - lo + 1
        return lo + (stable_hash_int(f"i:{key}") % span)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = stable_u01(f"f:{key}", lo=0.0, hi=1.0)
        return lo + (hi - lo) * u
    if k == "hex":
        return gen_hex(int(v), f"hex:{key}")
    if k == "uuid":
        hx = gen_hex(32, f"uuid:{key}")
        hx = hx[:12] + "4" + hx[13:16] + "a" + hx[17:]
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:]}"
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return f"{key}"
    return ""


def isoformat_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_log_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def get_int_domain(comp_id: str, log_id: str, var_name: str) -> Optional[Tuple[int, int]]:
    comp = SYSTEM["components"][comp_id]
    log = comp["logs"][log_id]
    dom = log.get("vars", {}).get(var_name)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    return lo, hi


@dataclass
class SegmentControls:
    bg_rate_mult: Dict[str, float]  # key: component.log_id
    flow_rate_mult: Dict[str, float]  # key: flow_id
    flow_latency_mult: Dict[str, Dict[str, float]]  # key: flow_id -> {"p50":x,"p95":y}


def build_failure_segments() -> List[Tuple[int, int, SegmentControls, List[Dict[str, Any]]]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted(set([f_start] + [e["at_min"] for e in events] + [f_end]))

    cur_bg: Dict[str, float] = {}
    cur_flow: Dict[str, float] = {}
    cur_lat: Dict[str, Dict[str, float]] = {}
    segments: List[Tuple[int, int, SegmentControls, List[Dict[str, Any]]]] = []

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    for i in range(len(boundaries) - 1):
        seg_start = boundaries[i]
        for e in events_by_min.get(seg_start, []):
            for k, mult in e.get("rate_multipliers", {}).items():
                if "." in k:
                    cur_bg[k] = float(mult)
                else:
                    cur_flow[k] = float(mult)
            for fid, lm in e.get("latency_multipliers", {}).items():
                cur_lat[fid] = {"p50": float(lm["p50"]), "p95": float(lm["p95"])}

        seg_end = boundaries[i + 1]
        one_shot_events = events_by_min.get(seg_start, [])
        segments.append(
            (
                seg_start,
                seg_end,
                SegmentControls(bg_rate_mult=dict(cur_bg), flow_rate_mult=dict(cur_flow), flow_latency_mult=dict(cur_lat)),
                one_shot_events,
            )
        )
    return segments


# -----------------------------
# Emission core
# -----------------------------
ROWS: List[Dict[str, Any]] = []


def emit_log(dt: datetime, log_ref: str, ctx: Dict[str, Any], trace_id: str, host_override: Optional[str] = None) -> None:
    comp_id, log_id = parse_log_ref(log_ref)
    comp = SYSTEM["components"][comp_id]
    tmpl = comp["logs"][log_id]
    vars_def = tmpl.get("vars", {})
    values: Dict[str, Any] = {}
    for var_name, dom in vars_def.items():
        if var_name in ctx:
            values[var_name] = ctx[var_name]
        else:
            val = choose_from_domain(dom, f"{comp_id}.{log_id}.{var_name}:{ctx.get('_key', '')}")
            ctx[var_name] = val
            values[var_name] = val

    msg = tmpl["msg"].format(**values)
    host = host_override if host_override is not None else (comp["hosts"][0] if comp.get("hosts") else "")
    ROWS.append(
        {
            "timestamp": dt,
            "level": tmpl["lvl"],
            "message": msg,
            "trace_id": trace_id,
            "service": comp.get("svc", "") or "",
            "host": host or "",
        }
    )


def choose_component_host(comp_id: str, key: str) -> str:
    hosts = SYSTEM["components"][comp_id].get("hosts", [])
    if not hosts:
        return ""
    idx = stable_hash_int(f"host:{comp_id}:{key}") % len(hosts)
    return hosts[idx]


def schedule_times_evenly(start_dt: datetime, end_dt: datetime, n: int, key: str, jitter_s: float = 0.18) -> List[datetime]:
    if n <= 0:
        return []
    dur_s = max(0.001, (end_dt - start_dt).total_seconds())
    out = []
    for i in range(n):
        frac = (i + 0.5) / n
        base = start_dt + timedelta(seconds=dur_s * frac)
        u = stable_u01(f"jit:{key}:{i}", lo=0.0, hi=1.0)
        jitter = (u - 0.5) * 2.0 * jitter_s
        t = base + timedelta(seconds=jitter)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        out.append(t)
    return out


def bind_flow_timing_and_ctx(flow_id: str, delays_ms: List[int], ctx: Dict[str, Any]) -> List[int]:
    """
    Bind observed timing fields to the already-chosen chronology, but enforce template domains
    by adjusting the chronology (delays) deterministically so message-carried timings and
    inter-log timestamp gaps agree and remain within modeled integer domains.
    """
    delays = list(delays_ms)

    # Web flows: http_200 dur_ms == delay[0]; router_200 service_ms == delay[0]+delay[1]
    if flow_id in ("web_interaction_ok", "web_interaction_ok_degraded"):
        if len(delays) >= 2:
            dur_dom = get_int_domain("incident_app", "http_200", "dur_ms") or (0, 10**9)
            svc_dom = get_int_domain("heroku_runtime", "router_200", "service_ms") or (0, 10**9)
            dur_lo, dur_hi = dur_dom
            svc_lo, svc_hi = svc_dom

            min_d1 = 1
            max_d0_allowed = min(dur_hi, max(dur_lo, svc_hi - min_d1))
            d0 = clamp_int(int(delays[0]), dur_lo, max_d0_allowed)

            d1_orig = max(min_d1, int(delays[1]))
            total_orig = d0 + d1_orig

            total_target = clamp_int(total_orig, max(svc_lo, d0 + min_d1), svc_hi)
            # If we had to clamp total below d0+min_d1 (possible only if d0 too big),
            # reduce d0 to make it feasible.
            if total_target < d0 + min_d1:
                d0 = clamp_int(total_target - min_d1, dur_lo, max_d0_allowed)
                total_target = clamp_int(total_target, max(svc_lo, d0 + min_d1), svc_hi)

            d1 = max(min_d1, total_target - d0)

            delays[0] = d0
            delays[1] = d1

            ctx["dur_ms"] = int(d0)
            ctx["service_ms"] = int(d0 + d1)

        # Keep route/path coherence across router/app logs
        if "path" in ctx and "route" not in ctx:
            ctx["route"] = ctx["path"]
        if "route" in ctx and "path" not in ctx:
            ctx["path"] = ctx["route"]

    # Pub/Sub consume flows: worker_msg_acked dur_ms == time between deliver and ack == delay[1]+delay[2]
    if flow_id in ("pubsub_consume_ok", "pubsub_consume_ok_f"):
        if len(delays) >= 3:
            dur_dom = get_int_domain("incident_app", "worker_msg_acked", "dur_ms") or (0, 10**9)
            dur_lo, dur_hi = dur_dom

            min_gap = 1
            d1 = max(min_gap, int(delays[1]))
            d2 = max(min_gap, int(delays[2]))
            total = d1 + d2

            total_target = clamp_int(total, dur_lo, dur_hi)

            # Adjust d2 primarily; if needed, adjust d1 too.
            d2_new = max(min_gap, total_target - d1)
            if d2_new + d1 != total_target:
                d1 = max(min_gap, total_target - min_gap)
                d2_new = max(min_gap, total_target - d1)

            delays[1] = d1
            delays[2] = d2_new
            ctx["dur_ms"] = int(d1 + d2_new)

    # Poison welcome flow just pins subscription semantics
    if flow_id == "pubsub_welcome_consume_panics":
        ctx["sub"] = "inc-welcome-sub"

    return delays


def simulate_flow_instance(
    flow: Dict[str, Any],
    start_dt: datetime,
    state: str,
    controls: Optional[SegmentControls],
    instance_idx: int,
    seg_key: str,
) -> None:
    flow_id = flow["id"]
    trace_id = ""  # tracing off; flows trace=false
    retry = flow.get("retry", {})
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    expected_attempts = max(1.0, min(float(max_attempts), expected_attempts))
    base_attempts = int(math.floor(expected_attempts))
    rem = expected_attempts - base_attempts
    attempts = max(1, base_attempts)
    if attempts < max_attempts and rem > 0:
        u = stable_u01(f"attempts:{flow_id}:{seg_key}:{instance_idx}", lo=0.0, hi=1.0)
        if u < rem:
            attempts += 1
    attempts = max(1, min(max_attempts, attempts))

    chain_key = f"{flow_id}:{seg_key}:{instance_idx}"
    comp_host: Dict[str, str] = {}
    for ref in flow["emit"] + retry.get("emit_per_retry", []):
        cid, _ = parse_log_ref(ref)
        if cid not in comp_host:
            comp_host[cid] = choose_component_host(cid, chain_key)

    lat_mult = {"p50": 1.0, "p95": 1.0}
    if state == "f" and controls is not None and flow_id in controls.flow_latency_mult:
        lat_mult = controls.flow_latency_mult[flow_id]

    cur_attempt_start = start_dt
    for attempt_idx in range(1, attempts + 1):
        ctx: Dict[str, Any] = {"_key": f"{chain_key}:a{attempt_idx}"}

        if flow_id in ("web_interaction_ok", "web_interaction_ok_degraded", "web_interaction_h10"):
            path_dom = SYSTEM["components"]["heroku_runtime"]["logs"]["router_200"]["vars"]["path"]
            method_dom = SYSTEM["components"]["heroku_runtime"]["logs"]["router_200"]["vars"]["method"]
            ctx["path"] = choose_from_domain(path_dom, f"{ctx['_key']}:path")
            ctx["route"] = ctx["path"]
            ctx["method"] = choose_from_domain(method_dom, f"{ctx['_key']}:method")
            ctx["req_id"] = gen_hex(16, f"{ctx['_key']}:req_id")
            ctx["user_id"] = gen_hex(8, f"{ctx['_key']}:user")

        if flow_id in ("pubsub_consume_ok", "pubsub_consume_ok_f"):
            sub_dom = SYSTEM["components"]["incident_app"]["logs"]["worker_msg_received"]["vars"]["sub"]
            ctx["sub"] = choose_from_domain(sub_dom, f"{ctx['_key']}:sub")
            ctx["msg_id"] = gen_hex(24, f"{ctx['_key']}:msg")
            ctx["attempt"] = 1 + (stable_hash_int(f"{ctx['_key']}:attempt") % 3)

        if flow_id == "pubsub_welcome_consume_panics":
            ctx["sub"] = "inc-welcome-sub"
            ctx["msg_id"] = gen_hex(24, f"{ctx['_key']}:msg")
            ctx["attempt"] = 1 + (stable_hash_int(f"{ctx['_key']}:attempt") % 12)

        if attempt_idx >= 2:
            for ridx, rref in enumerate(retry.get("emit_per_retry", [])):
                t = cur_attempt_start + timedelta(milliseconds=1 + ridx)
                cid, _ = parse_log_ref(rref)
                emit_log(t, rref, ctx, trace_id, host_override=comp_host.get(cid, ""))

        # Sample per-log delays for this attempt (between consecutive emits)
        delays_ms: List[int] = []
        for j, (p50, p95) in enumerate(flow["latency_ms"]):
            p50s = float(p50) * float(lat_mult.get("p50", 1.0))
            p95s = float(p95) * float(lat_mult.get("p95", 1.0))
            delays_ms.append(
                sample_pos_ms(
                    p50s,
                    p95s,
                    f"{flow_id}:{seg_key}:{instance_idx}:a{attempt_idx}:l{j}",
                    soft_cap_mult=2.5,
                    min_ms=1,
                )
            )

        # Bind / adjust delays to keep message timing fields within template domains
        delays_ms = bind_flow_timing_and_ctx(flow_id, delays_ms, ctx)

        t = cur_attempt_start
        for j, ref in enumerate(flow["emit"]):
            t = t + timedelta(milliseconds=delays_ms[j])
            cid, _ = parse_log_ref(ref)
            emit_log(t, ref, ctx, trace_id, host_override=comp_host.get(cid, ""))

        if attempt_idx < attempts:
            bidx = attempt_idx - 1
            backoff_list = retry.get("backoff_ms", [])
            if bidx < len(backoff_list):
                bp50, bp95 = backoff_list[bidx]
            else:
                bp50, bp95 = (50, 500)
            bo_ms = sample_pos_ms(
                bp50,
                bp95,
                f"backoff:{flow_id}:{seg_key}:{instance_idx}:a{attempt_idx}",
                soft_cap_mult=3.0,
                min_ms=1,
            )
            cur_attempt_start = t + timedelta(milliseconds=bo_ms)


def simulate_background(state: str, start_min: int, end_min: int, controls: Optional[SegmentControls]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for comp_id, comp in SYSTEM["components"].items():
        beh_list = comp.get("beh", {}).get(state, [])
        if not beh_list:
            continue
        for beh in beh_list:
            log_id = beh["id"]
            per_min = float(beh["per_min"])
            scope = beh.get("scope", "per_host")
            source_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f" and controls is not None:
                mult = float(controls.bg_rate_mult.get(source_key, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * dur_min
                n = det_round(expected, f"bg:{state}:{start_min}-{end_min}:{source_key}")
                times = schedule_times_evenly(start_dt, end_dt, n, f"bg:{source_key}:{start_min}-{end_min}")
                for i, t in enumerate(times):
                    ctx = {"_key": f"bg:{state}:{source_key}:{start_min}-{end_min}:{i}"}
                    host = choose_component_host(comp_id, f"{ctx['_key']}:host")
                    if source_key == "incident_app.health_tick":
                        # Keep within modeled integer domain [5, 200000]
                        uptime = int((t - BASE_TIME).total_seconds()) % 200000
                        ctx["uptime_s"] = clamp_int(uptime, 5, 200000)
                    emit_log(t, f"{comp_id}.{log_id}", ctx, "", host_override=host)
            else:
                hosts = comp.get("hosts", [])
                if not hosts:
                    continue
                for h in hosts:
                    expected = eff_per_min * dur_min
                    n = det_round(expected, f"bg:{state}:{start_min}-{end_min}:{source_key}:{h}")
                    times = schedule_times_evenly(start_dt, end_dt, n, f"bg:{source_key}:{h}:{start_min}-{end_min}")
                    for i, t in enumerate(times):
                        ctx = {"_key": f"bg:{state}:{source_key}:{h}:{start_min}-{end_min}:{i}"}
                        if source_key == "incident_app.health_tick":
                            uptime = int((t - BASE_TIME).total_seconds()) % 200000
                            ctx["uptime_s"] = clamp_int(uptime, 5, 200000)
                        emit_log(t, f"{comp_id}.{log_id}", ctx, "", host_override=h)


def simulate_flows_in_interval(state: str, start_min: int, end_min: int, controls: Optional[SegmentControls]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0
    flows = SYSTEM["flows"][state]
    seg_key = f"{state}:{start_min}-{end_min}"

    for flow in flows:
        flow_id = flow["id"]
        rpm = float(flow["rpm"])
        mult = 1.0
        if state == "f" and controls is not None:
            mult = float(controls.flow_rate_mult.get(flow_id, 1.0))
        eff_rpm = rpm * mult
        if eff_rpm <= 0:
            continue
        expected = eff_rpm * dur_min
        n = det_round(expected, f"flow:{seg_key}:{flow_id}")
        if n <= 0:
            continue
        starts = schedule_times_evenly(start_dt, end_dt, n, f"flow:{flow_id}:{seg_key}", jitter_s=0.12)
        for idx, sdt in enumerate(starts):
            simulate_flow_instance(flow, sdt, state, controls, idx, seg_key)


def emit_one_shots(at_min: int, one_shots: List[Dict[str, Any]]) -> None:
    t0 = BASE_TIME + timedelta(minutes=at_min)
    for shot_idx, shot in enumerate(one_shots):
        ref = shot["ref"]
        count = int(shot["count"])
        allowed_hosts = shot.get("hosts", [])
        for i in range(count):
            u = stable_u01(f"oneshot:{at_min}:{ref}:{shot_idx}:{i}", lo=0.0, hi=1.0)
            t = t0 + timedelta(seconds=0.2 + 4.5 * u)
            ctx = {"_key": f"oneshot:{at_min}:{ref}:{shot_idx}:{i}"}
            if allowed_hosts:
                host = allowed_hosts[i % len(allowed_hosts)]
            else:
                cid, _ = parse_log_ref(ref)
                host = choose_component_host(cid, ctx["_key"])
            emit_log(t, ref, ctx, "", host_override=host)


# -----------------------------
# Run simulation
# -----------------------------
def main() -> None:
    random.seed(0)
    np.random.seed(0)

    # Normal phase
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    simulate_background("n", n_start, n_end, None)
    simulate_flows_in_interval("n", n_start, n_end, None)

    # Failure phase: segments with persistent controls and one-shots
    for seg_start, seg_end, controls, events_at_start in build_failure_segments():
        for e in events_at_start:
            emit_one_shots(e["at_min"], e.get("one_shots", []))
        simulate_background("f", seg_start, seg_end, controls)
        simulate_flows_in_interval("f", seg_start, seg_end, controls)

    df = pd.DataFrame(ROWS)
    if df.empty:
        df = pd.DataFrame(columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    else:
        df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
        df["timestamp"] = df["timestamp"].apply(isoformat_ms)

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not df.empty:
        ts = df["timestamp"].tolist()
        if any(ts[i] > ts[i + 1] for i in range(len(ts) - 1)):
            raise RuntimeError("Timestamps are not sorted ascending.")

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row count {nrows} outside target range [20000, 100000].")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
