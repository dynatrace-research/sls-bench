import math
import hashlib
import ipaddress
import datetime as dt
import random
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Ensure deterministic behavior across stdlib and numpy global RNGs.
random.seed(0)
np.random.seed(0)

# -----------------------------
# Embedded normalized inputs
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "qa_site_homepage_healthcheck_outage"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "load_balancer",
            "svc": "edge-lb",
            "hosts": ["lb-1", "lb-2"],
            "logs": {
                "lb_req_ok": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} client={client_ip} {method} {uri} status=200 upstream={upstream} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/", "/questions", "/search"]},
                        "upstream": {"k": "ch", "v": ["web-1", "web-2", "web-3", "web-4"]},
                    },
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [5, 250]}}, "f": {"dur_ms": {"k": "i", "v": [10, 5000]}}},
                },
                "lb_req_upstream_timeout": {
                    "lvl": "WARN",
                    "msg": "req_id={req_id} client={client_ip} {method} {uri} status=504 upstream={upstream} timeout_ms={timeout_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/"]},
                        "upstream": {"k": "ch", "v": ["web-1", "web-2", "web-3", "web-4"]},
                        "timeout_ms": {"k": "i", "v": [1500, 3000]},
                    },
                    "state_vars": {},
                },
                "lb_req_no_backend": {
                    "lvl": "ERROR",
                    "msg": "req_id={req_id} client={client_ip} {method} {uri} status=503 reason=no_healthy_upstreams",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/", "/questions", "/search"]},
                    },
                    "state_vars": {},
                },
                "lb_healthcheck_ok": {
                    "lvl": "INFO",
                    "msg": "healthcheck backend={upstream} uri=/ status=200 dur_ms={dur_ms}",
                    "vars": {"upstream": {"k": "ch", "v": ["web-1", "web-2", "web-3", "web-4"]}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [2, 120]}}, "f": {"dur_ms": {"k": "i", "v": [5, 3000]}}},
                },
                "lb_healthcheck_fail_timeout": {
                    "lvl": "WARN",
                    "msg": "healthcheck backend={upstream} uri=/ result=timeout timeout_ms={timeout_ms}",
                    "vars": {
                        "upstream": {"k": "ch", "v": ["web-1", "web-2", "web-3", "web-4"]},
                        "timeout_ms": {"k": "i", "v": [1500, 3000]},
                    },
                    "state_vars": {},
                },
                "lb_pool_status": {
                    "lvl": "INFO",
                    "msg": "pool_status healthy={healthy} total=4",
                    "vars": {},
                    "state_vars": {"n": {"healthy": {"k": "i", "v": [4, 4]}}, "f": {"healthy": {"k": "i", "v": [1, 4]}}},
                },
                "lb_pool_status_all_unhealthy": {
                    "lvl": "INFO",
                    "msg": "pool_status healthy={healthy} unhealthy={unhealthy}",
                    "vars": {"healthy": {"k": "i", "v": [0, 0]}, "unhealthy": {"k": "i", "v": [4, 4]}},
                    "state_vars": {},
                },
                "lb_backend_marked_unhealthy": {
                    "lvl": "INFO",
                    "msg": "backend {upstream} marked unhealthy reason=healthcheck_timeout",
                    "vars": {"upstream": {"k": "ch", "v": ["web-1", "web-2", "web-3", "web-4"]}},
                    "state_vars": {},
                },
                "lb_backend_marked_healthy": {
                    "lvl": "INFO",
                    "msg": "backend {upstream} marked healthy reason=healthcheck_ok",
                    "vars": {"upstream": {"k": "ch", "v": ["web-1", "web-2", "web-3", "web-4"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "lb_pool_status", "per_min": 1.0}]},
                "f": {"emit": [{"id": "lb_pool_status", "per_min": 1.0}, {"id": "lb_pool_status_all_unhealthy", "per_min": 1.0}]},
            },
        },
        {
            "id": "web_frontend",
            "svc": "web-frontend",
            "hosts": ["web-1", "web-2", "web-3", "web-4"],
            "logs": {
                "web_req_completed": {
                    "lvl": "INFO",
                    "msg": "req_id={req_id} {method} {uri} status={status} dur_ms={dur_ms} user_agent={ua}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/", "/questions", "/search"]},
                        "status": {"k": "ch", "v": [200]},
                        "ua": {"k": "ch", "v": ["browser", "lb-healthcheck"]},
                    },
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [5, 1200]}}, "f": {"dur_ms": {"k": "i", "v": [10, 30000]}}},
                },
                "web_req_aborted": {
                    "lvl": "ERROR",
                    "msg": "req_id={req_id} {method} {uri} aborted reason={reason} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/"]},
                        "reason": {"k": "ch", "v": ["client_closed", "server_timeout"]},
                        "dur_ms": {"k": "i", "v": [1500, 30000]},
                    },
                    "state_vars": {},
                },
                "web_trim_slow": {
                    "lvl": "WARN",
                    "msg": "trim_unicode_whitespace slow elapsed_ms={elapsed_ms} input_len={input_len} sample='{sample}'",
                    "vars": {
                        "elapsed_ms": {"k": "i", "v": [250, 1400]},
                        "input_len": {"k": "i", "v": [15000, 25000]},
                        "sample": {"k": "str", "v": "short text snippet"},
                    },
                    "state_vars": {},
                },
                "web_cpu_metric": {
                    "lvl": "INFO",
                    "msg": "runtime cpu_pct={cpu} gc_pause_ms={gc_pause} run_queue={runq}",
                    "vars": {},
                    "state_vars": {
                        "n": {"cpu": {"k": "i", "v": [5, 60]}, "gc_pause": {"k": "i", "v": [0, 60]}, "runq": {"k": "i", "v": [0, 20]}},
                        "f": {"cpu": {"k": "i", "v": [20, 100]}, "gc_pause": {"k": "i", "v": [5, 200]}, "runq": {"k": "i", "v": [5, 200]}},
                    },
                },
                "web_deploy": {
                    "lvl": "INFO",
                    "msg": "deploy version={version} sha={sha} stage={stage}",
                    "vars": {"version": {"k": "ch", "v": ["2016.07.20-hotfix"]}, "sha": {"k": "hex", "v": 7}, "stage": {"k": "ch", "v": ["start"]}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "web_cpu_metric", "per_min": 1.0}]},
                "f": {"emit": [{"id": "web_cpu_metric", "per_min": 1.0}, {"id": "web_req_aborted", "per_min": 2.0}]},
            },
        },
        {
            "id": "monitoring",
            "svc": "site-monitor",
            "hosts": ["mon-1"],
            "logs": {
                "mon_probe": {
                    "lvl": "INFO",
                    "msg": "probe name={name} target={target} status={status} latency_ms={lat_ms}",
                    "vars": {"name": {"k": "ch", "v": ["homepage"]}, "target": {"k": "ch", "v": ["edge-lb"]}},
                    "state_vars": {"n": {"status": {"k": "ch", "v": ["ok"]}, "lat_ms": {"k": "i", "v": [20, 300]}}, "f": {"status": {"k": "ch", "v": ["warn", "crit"]}, "lat_ms": {"k": "i", "v": [5, 5000]}}},
                },
                "mon_alert_homepage_latency": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT homepage_latency severity=P1 p95_ms={p95_ms} window_s={window_s}",
                    "vars": {"p95_ms": {"k": "i", "v": [2000, 15000]}, "window_s": {"k": "i", "v": [60, 600]}},
                    "state_vars": {},
                },
                "mon_alert_all_backends_unhealthy": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT all_backends_unhealthy severity=P1 healthy={healthy} unhealthy={unhealthy}",
                    "vars": {"healthy": {"k": "i", "v": [0, 0]}, "unhealthy": {"k": "i", "v": [4, 4]}},
                    "state_vars": {},
                },
                "mon_incident_update": {
                    "lvl": "INFO",
                    "msg": "incident_update status={status} note='{note}'",
                    "vars": {"status": {"k": "ch", "v": ["investigating", "identified"]}, "note": {"k": "str", "v": "short note"}},
                    "state_vars": {},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "mon_probe", "per_min": 1.0, "scope": "global"},
                        {"id": "mon_alert_homepage_latency", "per_min": 0.0, "scope": "global"},
                        {"id": "mon_alert_all_backends_unhealthy", "per_min": 0.0, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "mon_probe", "per_min": 1.0, "scope": "global"},
                        {"id": "mon_alert_homepage_latency", "per_min": 0.2, "scope": "global"},
                        {"id": "mon_alert_all_backends_unhealthy", "per_min": 0.2, "scope": "global"},
                    ]
                },
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "homepage_view_ok",
                    "rpm": 240.0,
                    "emit": ["web_frontend.web_req_completed", "load_balancer.lb_req_ok"],
                    "latency_ms": [[25, 120], [30, 160]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "question_view_ok",
                    "rpm": 480.0,
                    "emit": ["web_frontend.web_req_completed", "load_balancer.lb_req_ok"],
                    "latency_ms": [[30, 180], [35, 220]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "lb_healthcheck_ok",
                    "rpm": 24.0,
                    "emit": ["web_frontend.web_req_completed", "load_balancer.lb_healthcheck_ok"],
                    "latency_ms": [[10, 60], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "homepage_slow_timeout",
                    "rpm": 200.0,
                    "emit": ["web_frontend.web_trim_slow", "load_balancer.lb_req_upstream_timeout"],
                    "latency_ms": [[300, 1400], [1500, 3000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "question_view_ok_f",
                    "rpm": 400.0,
                    "emit": ["web_frontend.web_req_completed", "load_balancer.lb_req_ok"],
                    "latency_ms": [[60, 1200], [80, 1500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "healthcheck_timeout",
                    "rpm": 24.0,
                    "emit": ["web_frontend.web_trim_slow", "load_balancer.lb_healthcheck_fail_timeout"],
                    "latency_ms": [[250, 1200], [1500, 3000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "site_req_no_backend",
                    "rpm": 720.0,
                    "emit": ["load_balancer.lb_req_no_backend"],
                    "latency_ms": [[1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "site_req_ok_degraded",
                    "rpm": 720.0,
                    "emit": ["web_frontend.web_req_completed", "load_balancer.lb_req_ok"],
                    "latency_ms": [[80, 4000], [100, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "healthcheck_ok_degraded",
                    "rpm": 24.0,
                    "emit": ["web_frontend.web_req_completed", "load_balancer.lb_healthcheck_ok"],
                    "latency_ms": [[30, 1500], [30, 2500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "incident_2016_07_20_homepage_regex_cpu_outage",
        "time": {
            "total_minutes": 40,
            "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}},
        },
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {
                            "site_req_no_backend": 0.0,
                            "site_req_ok_degraded": 0.0,
                            "healthcheck_ok_degraded": 0.0,
                            "monitoring.mon_alert_all_backends_unhealthy": 0.0,
                            "monitoring.mon_alert_homepage_latency": 5.0,
                            "load_balancer.lb_pool_status": 1.0,
                            "load_balancer.lb_pool_status_all_unhealthy": 0.0,
                        },
                        "latency_multipliers": {"question_view_ok_f": {"p50": 1.5, "p95": 2.0}},
                        "one_shots": [{"ref": "monitoring.mon_incident_update", "count": 1, "hosts": ["mon-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 24,
                        "rate_multipliers": {
                            "homepage_slow_timeout": 0.0,
                            "question_view_ok_f": 0.0,
                            "healthcheck_timeout": 1.0,
                            "healthcheck_ok_degraded": 0.0,
                            "site_req_no_backend": 1.0,
                            "site_req_ok_degraded": 0.0,
                            "monitoring.mon_alert_all_backends_unhealthy": 1.0,
                            "monitoring.mon_alert_homepage_latency": 0.0,
                            "load_balancer.lb_pool_status": 0.0,
                            "load_balancer.lb_pool_status_all_unhealthy": 1.0,
                            "web_frontend.web_req_aborted": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "load_balancer.lb_backend_marked_unhealthy", "count": 4, "hosts": ["lb-1"]},
                            {"ref": "monitoring.mon_incident_update", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 34,
                        "rate_multipliers": {
                            "site_req_no_backend": 0.8,
                            "site_req_ok_degraded": 0.2,
                            "healthcheck_timeout": 0.5,
                            "healthcheck_ok_degraded": 0.5,
                            "monitoring.mon_alert_all_backends_unhealthy": 0.5,
                            "monitoring.mon_alert_homepage_latency": 0.0,
                            "load_balancer.lb_pool_status": 1.0,
                            "load_balancer.lb_pool_status_all_unhealthy": 0.3,
                            "web_frontend.web_req_aborted": 0.3,
                        },
                        "latency_multipliers": {
                            "site_req_ok_degraded": {"p50": 1.8, "p95": 2.5},
                            "healthcheck_ok_degraded": {"p50": 1.5, "p95": 2.0},
                        },
                        "one_shots": [
                            {"ref": "web_frontend.web_deploy", "count": 1, "hosts": ["web-2"]},
                            {"ref": "load_balancer.lb_backend_marked_healthy", "count": 1, "hosts": ["lb-1"]},
                        ],
                    },
                ]
            }
        },
    }
}

# -----------------------------
# Helpers
# -----------------------------
LEVELS = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"}


def seed_from_parts(*parts: Any) -> int:
    s = "|".join(str(p) for p in parts)
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") & 0xFFFFFFFFFFFFFFFF


def rng_for(*parts: Any) -> np.random.Generator:
    return np.random.default_rng(seed_from_parts(*parts))


def iso8601_ms(ts: dt.datetime) -> str:
    s = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


def gen_value(domain: Dict[str, Any], rng: np.random.Generator) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "hex":
        n = int(v)
        alphabet = np.array(list("0123456789abcdef"))
        return "".join(rng.choice(alphabet, size=n).tolist())
    if k == "ip":
        net = ipaddress.ip_network(v, strict=False)
        size = net.num_addresses
        if size <= 2:
            addr = int(net.network_address)
        else:
            offset = int(rng.integers(1, min(size - 1, 255)))
            addr = int(net.network_address) + offset
        return str(ipaddress.ip_address(addr))
    if k == "ch":
        arr = v
        return arr[int(rng.integers(0, len(arr)))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(lo + (hi - lo) * rng.random())
    if k == "str":
        base = str(v)
        suffix = int(rng.integers(0, 1000))
        if base.endswith("snippet"):
            return base
        return f"{base} {suffix}"
    return str(v)


def schedule_evenly(start: dt.datetime, end: dt.datetime, n: int, rng: np.random.Generator, jitter_s: float = 0.25) -> List[dt.datetime]:
    if n <= 0:
        return []
    dur_s = (end - start).total_seconds()
    if dur_s <= 0:
        return [start] * n
    times = []
    for i in range(n):
        frac = (i + 0.5) / n
        base = start + dt.timedelta(seconds=dur_s * frac)
        jit = (rng.random() * 2 - 1) * jitter_s
        t = base + dt.timedelta(seconds=jit)
        if t < start:
            t = start
        if t >= end:
            t = end - dt.timedelta(milliseconds=1)
        times.append(t)
    return times


def allocate_count(expected: float, carry: float) -> Tuple[int, float]:
    x = expected + carry
    n = int(math.floor(x + 1e-12))
    carry = x - n
    if carry >= 1.0:
        add = int(math.floor(carry))
        n += add
        carry -= add
    return n, carry


def sample_latency_ms(p50: float, p95: float, rng: np.random.Generator, w_low: float = 0.15, w_high: float = 0.55) -> int:
    p50 = max(1e-6, float(p50))
    p95 = max(p50, float(p95))
    w = float(w_low + (w_high - w_low) * rng.random())
    val = math.exp(math.log(p50) * (1.0 - w) + math.log(p95) * w)
    return int(max(1, round(val)))


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, logid = ref.split(".", 1)
    return comp, logid


# -----------------------------
# Build indices
# -----------------------------
COMP_BY_ID: Dict[str, Any] = {c["id"]: c for c in SYSTEM["components"]}

LOG_TEMPLATES: Dict[Tuple[str, str], Dict[str, Any]] = {}
for comp in SYSTEM["components"]:
    cid = comp["id"]
    for lid, tpl in comp["logs"].items():
        LOG_TEMPLATES[(cid, lid)] = tpl

FLOW_BY_STATE_ID: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st in ("n", "f"):
    for fdef in SYSTEM["flows"][st]["req"]:
        FLOW_BY_STATE_ID[(st, fdef["id"])] = fdef


# -----------------------------
# Derive failure control intervals (persistent multipliers)
# -----------------------------
def build_failure_intervals() -> List[Dict[str, Any]]:
    fphase = SCENARIO["scenario"]["time"]["phases"]["f"]
    f_start, f_end = int(fphase["start_min"]), int(fphase["end_min"])
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    cur_rate_mult_flow: Dict[str, float] = {}
    cur_rate_mult_bg: Dict[Tuple[str, str], float] = {}
    cur_lat_mult: Dict[str, Dict[str, float]] = {}
    intervals: List[Dict[str, Any]] = []

    ev_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        ev_by_min.setdefault(int(e["at_min"]), []).append(e)

    mins_sorted = sorted(set([f_start] + [int(e["at_min"]) for e in events] + [f_end]))
    if mins_sorted[0] != f_start:
        mins_sorted = [f_start] + mins_sorted
    if mins_sorted[-1] != f_end:
        mins_sorted = mins_sorted + [f_end]

    for i in range(len(mins_sorted) - 1):
        m = mins_sorted[i]
        nxt = mins_sorted[i + 1]
        for e in ev_by_min.get(m, []):
            for k, v in e.get("rate_multipliers", {}).items():
                if "." in k:
                    comp_id, log_id = parse_ref(k)
                    cur_rate_mult_bg[(comp_id, log_id)] = float(v)
                else:
                    cur_rate_mult_flow[k] = float(v)
            for fid, mults in e.get("latency_multipliers", {}).items():
                cur_lat_mult[fid] = {"p50": float(mults.get("p50", 1.0)), "p95": float(mults.get("p95", 1.0))}
        intervals.append(
            {
                "start_min": m,
                "end_min": nxt,
                "rate_mult_flow": dict(cur_rate_mult_flow),
                "rate_mult_bg": dict(cur_rate_mult_bg),
                "lat_mult": dict(cur_lat_mult),
            }
        )
    return [iv for iv in intervals if iv["end_min"] > iv["start_min"]]


FAILURE_INTERVALS = build_failure_intervals()

# -----------------------------
# Emission
# -----------------------------
BASE_TIME = dt.datetime(2016, 7, 20, 0, 0, 0, tzinfo=dt.timezone.utc)
LOG_ROWS: List[Dict[str, Any]] = []


def emit_log(ts: dt.datetime, level: str, message: str, service: str, host: str, trace_id: str = "") -> None:
    if level not in LEVELS:
        raise ValueError(f"Invalid level {level}")
    LOG_ROWS.append(
        {"timestamp": ts, "level": level, "message": message, "trace_id": trace_id, "service": service or "", "host": host or ""}
    )


def render_template(component_id: str, log_id: str, state: str, bound: Dict[str, Any], rng: np.random.Generator) -> Tuple[str, str, str]:
    comp = COMP_BY_ID[component_id]
    tpl = LOG_TEMPLATES[(component_id, log_id)]
    vars_domains = dict(tpl.get("vars", {}))
    state_vars = tpl.get("state_vars", {}).get(state, {})
    all_domains = {**vars_domains, **state_vars}

    values: Dict[str, Any] = dict(bound)
    for k, dom in all_domains.items():
        if k not in values:
            values[k] = gen_value(dom, rng)

    msg = tpl["msg"].format(**values)
    return tpl["lvl"], msg, comp["svc"]


def get_dur_max_for(component_id: str, log_id: str, state: str, field: str = "dur_ms") -> Optional[int]:
    tpl = LOG_TEMPLATES[(component_id, log_id)]
    stv = tpl.get("state_vars", {}).get(state, {})
    dom = stv.get(field)
    if dom and dom.get("k") == "i":
        return int(dom["v"][1])
    dom2 = tpl.get("vars", {}).get(field)
    if dom2 and dom2.get("k") == "i":
        return int(dom2["v"][1])
    return None


def minute_of(ts: dt.datetime) -> int:
    return int((ts - BASE_TIME).total_seconds() // 60)


def special_bind_background(component_id: str, log_id: str, state: str, ts: dt.datetime, rng: np.random.Generator) -> Dict[str, Any]:
    m = minute_of(ts)
    if component_id == "load_balancer" and log_id == "lb_pool_status":
        if state == "n":
            return {"healthy": 4}
        if 20 <= m < 24:
            healthy = max(1, 4 - (m - 20))
        elif 24 <= m < 34:
            healthy = 1
        else:
            healthy = 1 if m < 37 else 2
        return {"healthy": healthy}
    if component_id == "monitoring" and log_id == "mon_probe":
        if state == "n":
            lat = int(rng.integers(40, 220))
            return {"status": "ok", "lat_ms": lat}
        if 20 <= m < 24:
            lat = int(rng.integers(2000, 4800))
            return {"status": "warn" if (m % 2 == 0) else "crit", "lat_ms": lat}
        if 24 <= m < 34:
            lat = int(rng.integers(10, 90))
            return {"status": "crit", "lat_ms": lat}
        if (m - 34) % 5 == 0:
            lat = int(rng.integers(700, 2500))
            return {"status": "warn", "lat_ms": lat}
        lat = int(rng.integers(15, 120))
        return {"status": "crit", "lat_ms": lat}
    if component_id == "monitoring" and log_id == "mon_alert_homepage_latency":
        p95 = int(rng.integers(4000, 12000))
        window = int(rng.integers(120, 600))
        return {"p95_ms": p95, "window_s": window}
    if component_id == "web_frontend" and log_id == "web_req_aborted":
        m = minute_of(ts)
        if 20 <= m < 24:
            return {"reason": "server_timeout"}
        if m >= 34:
            return {"reason": "client_closed" if (m % 2 == 0) else "server_timeout"}
    return {}


def emit_background_interval(state: str, start_min: int, end_min: int, rate_mult_bg: Dict[Tuple[str, str], float]) -> None:
    start = BASE_TIME + dt.timedelta(minutes=start_min)
    end = BASE_TIME + dt.timedelta(minutes=end_min)
    dur_min = end_min - start_min

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", [])
        for e in emits:
            log_id = e["id"]
            per_min = float(e.get("per_min", 0.0))
            scope = e.get("scope", "per_host")
            mult = 1.0
            if state == "f":
                mult = float(rate_mult_bg.get((cid, log_id), 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0.0:
                continue

            if scope == "global":
                key = ("bg", state, cid, log_id, "GLOBAL")
                carry = BG_CARRY.get(key, 0.0)
                expected = eff_per_min * dur_min
                n, carry = allocate_count(expected, carry)
                BG_CARRY[key] = carry
                if n <= 0:
                    continue
                r = rng_for("bg_ts", key, start_min, end_min)
                times = schedule_evenly(start, end, n, r, jitter_s=0.35)
                for i, ts in enumerate(times):
                    rrow = rng_for("bg_row", key, start_min, i)
                    bound = special_bind_background(cid, log_id, state, ts, rrow)
                    lvl, msg, svc = render_template(cid, log_id, state, bound, rrow)
                    host = comp["hosts"][0] if comp.get("hosts") else ""
                    emit_log(ts, lvl, msg, svc, host, trace_id="")
            else:
                for host in comp.get("hosts", []):
                    key = ("bg", state, cid, log_id, host)
                    carry = BG_CARRY.get(key, 0.0)
                    expected = eff_per_min * dur_min
                    n, carry = allocate_count(expected, carry)
                    BG_CARRY[key] = carry
                    if n <= 0:
                        continue
                    r = rng_for("bg_ts", key, start_min, end_min)
                    times = schedule_evenly(start, end, n, r, jitter_s=0.35)
                    for i, ts in enumerate(times):
                        rrow = rng_for("bg_row", key, start_min, i)
                        bound = special_bind_background(cid, log_id, state, ts, rrow)
                        lvl, msg, svc = render_template(cid, log_id, state, bound, rrow)
                        emit_log(ts, lvl, msg, svc, host, trace_id="")


def choose_uri_for_flow(flow_id: str, idx: int) -> str:
    if "healthcheck" in flow_id or flow_id in ("homepage_view_ok", "homepage_slow_timeout"):
        return "/"
    if "question" in flow_id:
        return "/questions"
    if flow_id in ("site_req_no_backend", "site_req_ok_degraded"):
        choices = ["/", "/questions", "/search"]
        return choices[idx % len(choices)]
    return "/"


def gen_client_ip(rng: np.random.Generator) -> str:
    net = ipaddress.ip_network("203.0.113.0/24", strict=False)
    offset = int(rng.integers(1, 255))
    return str(ipaddress.ip_address(int(net.network_address) + offset))


def simulate_flow_instance(state: str, flow_id: str, start_ts: dt.datetime, interval_lat_mult: Dict[str, Dict[str, float]], instance_idx: int) -> None:
    fdef = FLOW_BY_STATE_ID[(state, flow_id)]
    r = rng_for("flow", state, flow_id, minute_of(start_ts), instance_idx)

    lb_comp = COMP_BY_ID["load_balancer"]
    web_comp = COMP_BY_ID["web_frontend"]
    lb_host = lb_comp["hosts"][int(r.integers(0, len(lb_comp["hosts"])))]
    web_host = web_comp["hosts"][int(r.integers(0, len(web_comp["hosts"])))]
    trace_id = ""

    uri = choose_uri_for_flow(flow_id, instance_idx)
    method = "GET"

    req_id = "".join(r.choice(np.array(list("0123456789abcdef")), size=16).tolist())
    client_ip: Optional[str] = None
    ua = "browser"
    if "healthcheck" in flow_id:
        ua = "lb-healthcheck"

    # User-traffic flows through the LB access log templates require a real client IP.
    if flow_id in (
        "homepage_view_ok",
        "question_view_ok",
        "question_view_ok_f",
        "site_req_ok_degraded",
        "site_req_no_backend",
        "homepage_slow_timeout",  # FIX: user requests that time out still have a client IP
    ):
        client_ip = gen_client_ip(r)

    lm = interval_lat_mult.get(flow_id, None)
    p50m = float(lm["p50"]) if lm else 1.0
    p95m = float(lm["p95"]) if lm else 1.0

    emits = fdef["emit"]
    lats = fdef["latency_ms"]

    if flow_id in ("homepage_slow_timeout", "healthcheck_timeout"):
        timeout_ms = int(r.integers(2200, 3001))
        if flow_id == "homepage_slow_timeout":
            dom_min, dom_max = 300, 1400
        else:
            dom_min, dom_max = 250, 1200
        max_elapsed = min(dom_max, timeout_ms - 1500)
        if max_elapsed < dom_min:
            max_elapsed = dom_min
        elapsed_ms = int(r.integers(dom_min, max_elapsed + 1))
        if elapsed_ms >= timeout_ms:
            elapsed_ms = max(1, timeout_ms - 1)

        ts1 = start_ts + dt.timedelta(milliseconds=int(elapsed_ms))
        bound1 = {"elapsed_ms": elapsed_ms}
        lvl1, msg1, svc1 = render_template("web_frontend", "web_trim_slow", state, bound1, rng_for("flowlog", flow_id, instance_idx, 1))
        emit_log(ts1, lvl1, msg1, svc1, web_host, trace_id=trace_id)

        ts2 = start_ts + dt.timedelta(milliseconds=int(timeout_ms))
        if flow_id == "homepage_slow_timeout":
            bound2 = {
                "req_id": req_id,
                "client_ip": client_ip,
                "method": method,
                "uri": "/",
                "upstream": web_host,
                "timeout_ms": timeout_ms,
            }
            # client_ip must be present for this access-log template; fall back defensively if somehow unset.
            if bound2["client_ip"] is None:
                bound2["client_ip"] = gen_client_ip(rng_for("fallback_client_ip", state, flow_id, minute_of(start_ts), instance_idx))
            lvl2, msg2, svc2 = render_template("load_balancer", "lb_req_upstream_timeout", state, bound2, rng_for("flowlog", flow_id, instance_idx, 2))
            emit_log(ts2, lvl2, msg2, svc2, lb_host, trace_id=trace_id)
        else:
            bound2 = {"upstream": web_host, "timeout_ms": timeout_ms}
            lvl2, msg2, svc2 = render_template("load_balancer", "lb_healthcheck_fail_timeout", state, bound2, rng_for("flowlog", flow_id, instance_idx, 2))
            emit_log(ts2, lvl2, msg2, svc2, lb_host, trace_id=trace_id)
        return

    delays_ms: List[int] = []
    for j, (p50, p95) in enumerate(lats):
        d = sample_latency_ms(p50 * p50m, p95 * p95m, r, w_low=0.10, w_high=0.50 if state == "n" else 0.65)
        delays_ms.append(d)

    if len(emits) >= 2:
        comp2, log2 = parse_ref(emits[1])
        max_total = get_dur_max_for(comp2, log2, state, field="dur_ms")
        if max_total is not None:
            total = sum(delays_ms[:2])
            if total > max_total:
                delays_ms[1] = max(1, int(max_total - delays_ms[0]))

    cur = start_ts
    total_since_start = 0
    for j, ref in enumerate(emits):
        comp_id, log_id = parse_ref(ref)
        delay = int(delays_ms[j]) if j < len(delays_ms) else 1
        total_since_start += delay
        cur = start_ts + dt.timedelta(milliseconds=total_since_start)

        bound: Dict[str, Any] = {}
        if comp_id == "web_frontend" and log_id == "web_req_completed":
            bound.update({"req_id": req_id, "method": method, "uri": uri, "status": 200, "ua": ua, "dur_ms": delay})
        elif comp_id == "load_balancer" and log_id == "lb_req_ok":
            cip = client_ip if client_ip is not None else gen_client_ip(rng_for("fallback_client_ip_ok", state, flow_id, minute_of(start_ts), instance_idx))
            bound.update({"req_id": req_id, "client_ip": cip, "method": method, "uri": uri, "upstream": web_host, "dur_ms": total_since_start})
        elif comp_id == "load_balancer" and log_id == "lb_healthcheck_ok":
            bound.update({"upstream": web_host, "dur_ms": total_since_start})
        elif comp_id == "load_balancer" and log_id == "lb_req_no_backend":
            cip = client_ip if client_ip is not None else gen_client_ip(rng_for("fallback_client_ip_503", state, flow_id, minute_of(start_ts), instance_idx))
            bound.update({"req_id": req_id, "client_ip": cip, "method": method, "uri": uri})

        lvl, msg, svc = render_template(comp_id, log_id, state, bound, rng_for("flowlog", flow_id, instance_idx, j))
        host = lb_host if comp_id == "load_balancer" else (web_host if comp_id == "web_frontend" else "")
        emit_log(cur, lvl, msg, svc, host, trace_id=trace_id)


def emit_flow_interval(state: str, start_min: int, end_min: int, rate_mult_flow: Dict[str, float], lat_mult: Dict[str, Dict[str, float]]) -> None:
    start = BASE_TIME + dt.timedelta(minutes=start_min)
    end = BASE_TIME + dt.timedelta(minutes=end_min)
    dur_min = end_min - start_min

    for fdef in SYSTEM["flows"][state]["req"]:
        fid = fdef["id"]
        rpm = float(fdef["rpm"])
        mult = 1.0
        if state == "f":
            mult = float(rate_mult_flow.get(fid, 1.0))
        eff_rpm = rpm * mult
        if eff_rpm <= 0.0:
            continue
        key = ("flow", state, fid)
        carry = FLOW_CARRY.get(key, 0.0)
        expected = eff_rpm * dur_min
        n, carry = allocate_count(expected, carry)
        FLOW_CARRY[key] = carry
        if n <= 0:
            continue
        rts = rng_for("flow_ts", key, start_min, end_min)
        starts = schedule_evenly(start, end, n, rts, jitter_s=0.20)
        for i, ts0 in enumerate(starts):
            simulate_flow_instance(state, fid, ts0, lat_mult, i)


def emit_one_shots() -> None:
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        one_shots = e.get("one_shots", [])
        base_ts = BASE_TIME + dt.timedelta(minutes=at_min)
        for sidx, os in enumerate(one_shots):
            ref = os["ref"]
            count = int(os["count"])
            hosts = os.get("hosts", [])
            comp_id, log_id = parse_ref(ref)
            comp = COMP_BY_ID[comp_id]
            svc = comp["svc"]
            r = rng_for("one_shot", ref, at_min, sidx)
            for k in range(count):
                jit_ms = int(r.integers(0, 3500))
                ts = base_ts + dt.timedelta(milliseconds=jit_ms + k)
                host = hosts[min(k, len(hosts) - 1)] if hosts else (comp.get("hosts", [""])[0] if comp.get("hosts") else "")
                bound: Dict[str, Any] = {}
                if ref == "load_balancer.lb_backend_marked_unhealthy":
                    bound["upstream"] = f"web-{k+1}" if k < 4 else "web-1"
                elif ref == "load_balancer.lb_backend_marked_healthy":
                    bound["upstream"] = "web-2"
                elif ref == "monitoring.mon_incident_update":
                    bound["status"] = "investigating" if at_min == 20 else "identified"
                    bound["note"] = f"short note {at_min}"
                lvl, msg, _ = render_template(comp_id, log_id, "f", bound, rng_for("one_shot_row", ref, at_min, k))
                emit_log(ts, lvl, msg, svc, host, trace_id="")


# -----------------------------
# Run simulation
# -----------------------------
BG_CARRY: Dict[Tuple[Any, ...], float] = {}
FLOW_CARRY: Dict[Tuple[Any, ...], float] = {}

n_start = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
n_end = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
emit_background_interval("n", n_start, n_end, rate_mult_bg={})
emit_flow_interval("n", n_start, n_end, rate_mult_flow={}, lat_mult={})

for iv in FAILURE_INTERVALS:
    emit_background_interval("f", iv["start_min"], iv["end_min"], rate_mult_bg=iv["rate_mult_bg"])
    emit_flow_interval("f", iv["start_min"], iv["end_min"], rate_mult_flow=iv["rate_mult_flow"], lat_mult=iv["lat_mult"])

emit_one_shots()

# -----------------------------
# Finalize CSV
# -----------------------------
df = pd.DataFrame(LOG_ROWS)
df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True)
df = df.sort_values(by=["timestamp_dt", "service", "host", "level", "message"], kind="mergesort").drop(columns=["timestamp_dt"])
df["timestamp"] = df["timestamp"].apply(lambda x: iso8601_ms(x))
df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
df.to_csv("logs.csv", index=False)
