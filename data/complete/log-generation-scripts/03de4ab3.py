import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -------------------------
# Embedded executable spec
# -------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "stackexchange_stackegg_lb_incident"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False},
    "components": [
        {
            "id": "haproxy_lb",
            "svc": "haproxy",
            "hosts": ["lb-a", "lb-b"],
            "logs": {
                "frontend_accept": {
                    "lvl": "INFO",
                    "msg": "accept fe={fe} src={src_ip} method={method} uri={uri} req={req_id}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "src_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/questions", "/search", "/users", "/stackegg/state"]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "frontend_close_ok": {
                    "lvl": "INFO",
                    "msg": "close fe={fe} status={status} dur_ms={dur_ms} bytes={bytes} req={req_id}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "status": {"k": "ch", "v": ["200", "304"]},
                        "dur_ms": {"k": "i", "v": [10, 6000]},
                        "bytes": {"k": "i", "v": [300, 250000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "frontend_connect_timeout": {
                    "lvl": "ERROR",
                    "msg": "connect timeout fe={fe} src={src_ip} waited_ms={waited_ms}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "src_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "waited_ms": {"k": "i", "v": [1000, 15000]},
                    },
                },
                "stats_sample_40k": {
                    "lvl": "INFO",
                    "msg": "stats fe={fe} cur_sess={cur_sess} queued={queued} maxconn={maxconn} keepalive_s={ka_s}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "maxconn": {"k": "i", "v": [140, 140]},
                        "ka_s": {"k": "i", "v": [15, 15]},
                    },
                    "state_vars": {
                        "n": {"cur_sess": {"k": "i", "v": [40, 120]}, "queued": {"k": "i", "v": [0, 5]}},
                        "f": {"cur_sess": {"k": "i", "v": [90, 130]}, "queued": {"k": "i", "v": [0, 30]}},
                    },
                },
                "stats_sample_40k_cap": {
                    "lvl": "INFO",
                    "msg": "stats fe={fe} cur_sess={cur_sess} queued={queued} maxconn={maxconn} keepalive_s={ka_s}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "maxconn": {"k": "i", "v": [140, 140]},
                        "ka_s": {"k": "i", "v": [15, 15]},
                    },
                    "state_vars": {
                        "n": {"cur_sess": {"k": "i", "v": [40, 120]}, "queued": {"k": "i", "v": [0, 5]}},
                        "f": {"cur_sess": {"k": "i", "v": [130, 140]}, "queued": {"k": "i", "v": [20, 500]}},
                    },
                },
                "stats_sample_60k": {
                    "lvl": "INFO",
                    "msg": "stats fe={fe} cur_sess={cur_sess} queued={queued} maxconn={maxconn} keepalive_s={ka_s}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "maxconn": {"k": "i", "v": [210, 210]},
                        "ka_s": {"k": "i", "v": [15, 15]},
                    },
                    "state_vars": {
                        "n": {"cur_sess": {"k": "i", "v": [40, 120]}, "queued": {"k": "i", "v": [0, 5]}},
                        "f": {"cur_sess": {"k": "i", "v": [150, 210]}, "queued": {"k": "i", "v": [0, 120]}},
                    },
                },
                "maxconn_queue_warn_40k": {
                    "lvl": "WARN",
                    "msg": "maxconn reached fe={fe} maxconn={maxconn} queued={queued}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "maxconn": {"k": "i", "v": [140, 140]},
                        "queued": {"k": "i", "v": [20, 500]},
                    },
                },
                "maxconn_queue_warn_60k": {
                    "lvl": "WARN",
                    "msg": "maxconn reached fe={fe} maxconn={maxconn} queued={queued}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "maxconn": {"k": "i", "v": [210, 210]},
                        "queued": {"k": "i", "v": [20, 200]},
                    },
                },
                "reload_applied": {
                    "lvl": "INFO",
                    "msg": "reload applied fe={fe} maxconn={maxconn} keepalive_s={ka_s}",
                    "vars": {
                        "fe": {"k": "ch", "v": ["fe_public"]},
                        "maxconn": {"k": "i", "v": [210, 210]},
                        "ka_s": {"k": "i", "v": [15, 15]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "stats_sample_40k", "per_min": 1.0, "scope": "per_host"},
                        {"id": "maxconn_queue_warn_40k", "per_min": 0.0, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "stats_sample_40k", "per_min": 1.0, "scope": "per_host"},
                        {"id": "stats_sample_40k_cap", "per_min": 1.0, "scope": "per_host"},
                        {"id": "stats_sample_60k", "per_min": 1.0, "scope": "per_host"},
                        {"id": "maxconn_queue_warn_40k", "per_min": 6.0, "scope": "global"},
                        {"id": "maxconn_queue_warn_60k", "per_min": 6.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "web_app",
            "svc": "webapp",
            "hosts": ["web-1", "web-2", "web-3", "web-4"],
            "logs": {
                "http_req": {
                    "lvl": "INFO",
                    "msg": "req {req_id} {method} {uri} user={user_tier}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET"]},
                        "uri": {"k": "ch", "v": ["/questions", "/search", "/users"]},
                        "user_tier": {"k": "ch", "v": ["anon", "logged_in", "employee"]},
                    },
                },
                "http_resp_200": {
                    "lvl": "INFO",
                    "msg": "resp {req_id} 200 dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [5, 5000]},
                        "bytes": {"k": "i", "v": [2000, 400000]},
                    },
                },
                "stackegg_req": {
                    "lvl": "INFO",
                    "msg": "req {req_id} GET /stackegg/state user={user_tier}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "user_tier": {"k": "ch", "v": ["logged_in", "employee"]},
                    },
                },
                "stackegg_resp_200": {
                    "lvl": "INFO",
                    "msg": "resp {req_id} 200 stackegg_state_age_s={age_s} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "age_s": {"k": "i", "v": [0, 10]},
                        "dur_ms": {"k": "i", "v": [5, 3000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "feature_flag_db",
            "svc": "configdb",
            "hosts": ["db-1"],
            "logs": {
                "flag_update": {
                    "lvl": "INFO",
                    "msg": "feature_flag {flag} set to {value} by {actor}",
                    "vars": {
                        "flag": {"k": "ch", "v": ["stackegg_enabled"]},
                        "value": {"k": "ch", "v": ["true", "false"]},
                        "actor": {"k": "ch", "v": ["deploy_bot", "oncall_eng", "dba"]},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "ops_puppet",
            "svc": "puppet",
            "hosts": ["puppet-1"],
            "logs": {
                "puppet_change": {
                    "lvl": "INFO",
                    "msg": "puppet applied change={change} target={target} result=success",
                    "vars": {"change": {"k": "ch", "v": ["set_haproxy_maxconn_210"]}, "target": {"k": "ch", "v": ["lb_fleet"]}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "monitor_pingdom",
            "svc": "pingdom",
            "hosts": ["pingdom"],
            "logs": {
                "check_ok": {
                    "lvl": "INFO",
                    "msg": "check site={site} status=OK rtt_ms={rtt_ms}",
                    "vars": {"site": {"k": "ch", "v": ["stackoverflow"]}, "rtt_ms": {"k": "i", "v": [40, 400]}},
                },
                "check_fail": {
                    "lvl": "ERROR",
                    "msg": "check site={site} status=DOWN reason={reason} waited_ms={waited_ms}",
                    "vars": {
                        "site": {"k": "ch", "v": ["stackoverflow"]},
                        "reason": {"k": "ch", "v": ["tcp_connect_timeout", "http_503", "high_latency"]},
                        "waited_ms": {"k": "i", "v": [800, 15000]},
                    },
                },
                "check_alert": {
                    "lvl": "ERROR",
                    "msg": "ALERT site={site} status=DOWN reason={reason}",
                    "vars": {
                        "site": {"k": "ch", "v": ["stackoverflow"]},
                        "reason": {"k": "ch", "v": ["tcp_connect_timeout", "http_503", "high_latency"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "check_ok", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "check_ok", "per_min": 1.0, "scope": "global"}, {"id": "check_fail", "per_min": 1.0, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "page_view",
                    "rpm": 250.0,
                    "emit": ["haproxy_lb.frontend_accept", "web_app.http_req", "web_app.http_resp_200", "haproxy_lb.frontend_close_ok"],
                    "latency_ms": [[1, 3], [2, 6], [30, 180], [1, 3]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "page_view_ok_slow",
                    "rpm": 220.0,
                    "emit": ["haproxy_lb.frontend_accept", "web_app.http_req", "web_app.http_resp_200", "haproxy_lb.frontend_close_ok"],
                    "latency_ms": [[1, 5], [5, 20], [120, 700], [1, 5]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "page_view_connect_timeout",
                    "rpm": 80.0,
                    "emit": ["haproxy_lb.frontend_connect_timeout"],
                    "latency_ms": [[8000, 15000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "stackegg_state_ajax",
                    "rpm": 300.0,
                    "emit": ["haproxy_lb.frontend_accept", "web_app.stackegg_req", "web_app.stackegg_resp_200", "haproxy_lb.frontend_close_ok"],
                    "latency_ms": [[1, 4], [2, 8], [40, 250], [1, 4]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "stackegg_enablement_haproxy_maxconn_outage"},
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 18,
                    "rate_multipliers": {
                        "stackegg_state_ajax": 1.2,
                        "page_view_connect_timeout": 0.0,
                        "haproxy_lb.maxconn_queue_warn_40k": 0.0,
                        "haproxy_lb.maxconn_queue_warn_60k": 0.0,
                        "haproxy_lb.stats_sample_40k": 1.0,
                        "haproxy_lb.stats_sample_40k_cap": 0.0,
                        "haproxy_lb.stats_sample_60k": 0.0,
                        "monitor_pingdom.check_fail": 0.0,
                    },
                    "latency_multipliers": {"stackegg_state_ajax": {"p50": 1.2, "p95": 1.3}, "page_view_ok_slow": {"p50": 1.2, "p95": 1.4}},
                    "one_shots": [{"ref": "feature_flag_db.flag_update", "count": 1, "hosts": ["db-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 22,
                    "rate_multipliers": {
                        "stackegg_state_ajax": 1.3,
                        "page_view_connect_timeout": 1.6,
                        "page_view_ok_slow": 0.9,
                        "haproxy_lb.maxconn_queue_warn_40k": 1.0,
                        "haproxy_lb.maxconn_queue_warn_60k": 0.0,
                        "haproxy_lb.stats_sample_40k": 0.0,
                        "haproxy_lb.stats_sample_40k_cap": 1.0,
                        "haproxy_lb.stats_sample_60k": 0.0,
                        "monitor_pingdom.check_ok": 0.0,
                        "monitor_pingdom.check_fail": 1.0,
                    },
                    "latency_multipliers": {"stackegg_state_ajax": {"p50": 2.0, "p95": 3.0}, "page_view_ok_slow": {"p50": 2.5, "p95": 3.5}},
                    "one_shots": [{"ref": "monitor_pingdom.check_alert", "count": 1, "hosts": ["pingdom"]}],
                },
                {
                    "order": 3,
                    "at_min": 30,
                    "rate_multipliers": {
                        "stackegg_state_ajax": 0.6,
                        "page_view_connect_timeout": 0.5,
                        "page_view_ok_slow": 1.0,
                        "haproxy_lb.stats_sample_40k": 0.0,
                        "haproxy_lb.stats_sample_40k_cap": 0.0,
                        "haproxy_lb.stats_sample_60k": 1.0,
                        "haproxy_lb.maxconn_queue_warn_40k": 0.0,
                        "haproxy_lb.maxconn_queue_warn_60k": 0.5,
                        "monitor_pingdom.check_ok": 1.0,
                        "monitor_pingdom.check_fail": 0.0,
                    },
                    "latency_multipliers": {"stackegg_state_ajax": {"p50": 1.3, "p95": 1.8}, "page_view_ok_slow": {"p50": 1.5, "p95": 2.0}},
                    "one_shots": [
                        {"ref": "ops_puppet.puppet_change", "count": 1, "hosts": ["puppet-1"]},
                        {"ref": "haproxy_lb.reload_applied", "count": 2, "hosts": ["lb-a", "lb-b"]},
                    ],
                },
            ]
        }
    },
}

# -------------------------
# Helpers
# -------------------------

BASE_SEED = 1337
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

random.seed(BASE_SEED)
np.random.seed(BASE_SEED)


def md5_u32(text: str) -> int:
    d = hashlib.md5(text.encode("utf-8")).digest()
    return int.from_bytes(d[:4], "little", signed=False)


def make_rng(key: str) -> np.random.Generator:
    return np.random.default_rng((BASE_SEED ^ md5_u32(key)) & 0xFFFFFFFF)


def jitter_ms(key: str, lo: int = -200, hi: int = 200) -> int:
    span = hi - lo + 1
    return lo + (md5_u32(f"jit|{BASE_SEED}|{key}") % span)


def fmt_ts(ms_from_base: int) -> str:
    dt = BASE_TIME + timedelta(milliseconds=int(ms_from_base))
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def sample_ip_from_cidr(cidr: str, rng: np.random.Generator) -> str:
    base, mask = cidr.split("/")
    if mask != "24":
        raise ValueError(f"Unsupported CIDR: {cidr}")
    parts = base.split(".")
    prefix = ".".join(parts[:3])
    host = int(rng.integers(1, 255))
    return f"{prefix}.{host}"


def sample_hex(n: int, rng: np.random.Generator) -> str:
    digits = rng.integers(0, 16, size=n, dtype=np.int64)
    return "".join("0123456789abcdef"[int(x)] for x in digits)


def sample_domain(dom: Dict[str, Any], rng: np.random.Generator) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        return str(rng.choice(v))
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if lo == hi:
            return lo
        return float(lo + (hi - lo) * rng.random())
    if k == "hex":
        return sample_hex(int(v), rng)
    if k == "ip":
        return sample_ip_from_cidr(str(v), rng)
    if k == "uuid":
        a = sample_hex(8, rng)
        b = sample_hex(4, rng)
        c = sample_hex(4, rng)
        d = sample_hex(4, rng)
        e = sample_hex(12, rng)
        return f"{a}-{b}-{c}-{d}-{e}"
    if k == "str":
        return str(v)
    raise ValueError(f"Unknown domain kind: {k}")


def lognormal_from_p50_p95(p50: float, p95: float, rng: np.random.Generator, cap: Optional[float] = None) -> float:
    p50 = max(1e-9, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    sigma = math.log(p95 / p50) / 1.6448536269514722
    mu = math.log(p50)
    x = float(rng.lognormal(mean=mu, sigma=sigma))
    if cap is not None:
        x = min(x, cap)
    return x


def render_log(template: Dict[str, Any], state: str, rng: np.random.Generator, bound: Dict[str, Any]) -> str:
    values: Dict[str, Any] = {}
    for name, dom in template.get("vars", {}).items():
        if name in bound:
            values[name] = bound[name]
        else:
            values[name] = sample_domain(dom, rng)
    st = template.get("state_vars", {}).get(state, {})
    for name, dom in st.items():
        if name in bound:
            values[name] = bound[name]
        else:
            values[name] = sample_domain(dom, rng)
    for k, v in bound.items():
        values.setdefault(k, v)
    return template["msg"].format(**values)


@dataclass(frozen=True)
class EmittedRow:
    ms: int
    level: str
    message: str
    trace_id: str
    service: str
    host: str
    seq: int


# -------------------------
# Indices
# -------------------------

COMPONENTS: Dict[str, Any] = {c["id"]: c for c in SYSTEM["components"]}
LOG_TEMPLATES: Dict[str, Dict[str, Any]] = {}
for cid, comp in COMPONENTS.items():
    for lid, tmpl in comp.get("logs", {}).items():
        LOG_TEMPLATES[f"{cid}.{lid}"] = tmpl

FLOWS_BY_STATE: Dict[str, List[Dict[str, Any]]] = {st: SYSTEM["flows"][st]["req"] for st in ["n", "f"]}

# -------------------------
# Failure controls (persistent piecewise)
# -------------------------


def build_failure_controls() -> Tuple[Dict[int, Dict[str, float]], Dict[int, Dict[str, Tuple[float, float]]], List[Dict[str, Any]]]:
    f_phase = SCENARIO["phases"]["f"]
    events = sorted(f_phase.get("events", []), key=lambda e: (int(e["at_min"]), int(e.get("order", 0))))
    start_f = int(SCENARIO["time"]["phases"]["f"]["start_min"])
    end_f = int(SCENARIO["time"]["phases"]["f"]["end_min"])

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Tuple[float, float]] = {}
    per_minute_rate: Dict[int, Dict[str, float]] = {}
    per_minute_lat: Dict[int, Dict[str, Tuple[float, float]]] = {}
    one_shots: List[Dict[str, Any]] = []

    ev_i = 0
    for m in range(start_f, end_f):
        while ev_i < len(events) and int(events[ev_i]["at_min"]) == m:
            ev = events[ev_i]
            for k, v in ev.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fk, mults in ev.get("latency_multipliers", {}).items():
                active_lat[fk] = (float(mults["p50"]), float(mults["p95"]))
            for os_ in ev.get("one_shots", []) or []:
                one_shots.append({"at_min": m, **os_})
            ev_i += 1

        per_minute_rate[m] = dict(active_rate)
        per_minute_lat[m] = dict(active_lat)

    return per_minute_rate, per_minute_lat, one_shots


FAIL_RATE_BY_MIN, FAIL_LAT_BY_MIN, ONE_SHOTS = build_failure_controls()


def rate_mult_for(state: str, minute: int, key: str) -> float:
    if state != "f":
        return 1.0
    return float(FAIL_RATE_BY_MIN.get(minute, {}).get(key, 1.0))


def lat_mult_for(state: str, minute: int, flow_id: str) -> Tuple[float, float]:
    if state != "f":
        return 1.0, 1.0
    return FAIL_LAT_BY_MIN.get(minute, {}).get(flow_id, (1.0, 1.0))


# -------------------------
# Deterministic count allocator
# -------------------------


class CarryAllocator:
    def __init__(self) -> None:
        self.rem: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        r = self.rem.get(key, 0.0) + float(expected)
        n = int(math.floor(r + 1e-12))
        self.rem[key] = r - n
        return n


# -------------------------
# Simulation
# -------------------------

rows: List[EmittedRow] = []
global_seq = 0


def emit_row(ms: int, level: str, message: str, service: str, host: str, trace_id: str = "") -> None:
    global global_seq
    rows.append(EmittedRow(ms=int(ms), level=level, message=message, trace_id=trace_id, service=service, host=host, seq=global_seq))
    global_seq += 1


def choose_host_round_robin(hosts: List[str], idx: int) -> str:
    if not hosts:
        return ""
    return hosts[idx % len(hosts)]


def simulate_background_for_minute(minute: int, state: str, allocator: CarryAllocator) -> None:
    minute_start = minute * 60_000
    for cid, comp in COMPONENTS.items():
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope", "per_host")
            src_key = f"{cid}.{log_id}"
            mult = rate_mult_for(state, minute, src_key)
            eff = per_min * mult

            if eff <= 0.0:
                continue

            tmpl = LOG_TEMPLATES[src_key]
            svc = comp.get("svc", "") or ""
            hosts = comp.get("hosts", []) or [""]

            if scope == "per_host":
                for h in hosts:
                    alloc_key = f"bg|{state}|{src_key}|{h}"
                    n = allocator.alloc(alloc_key, eff)
                    if n <= 0:
                        continue
                    for i in range(n):
                        offset = int(((i + 0.5) / n) * 60_000) if n > 0 else 0
                        t_ms = minute_start + offset + jitter_ms(f"bg|{minute}|{src_key}|{h}|{i}", -120, 120)
                        rng = make_rng(f"bg|{minute}|{src_key}|{h}|{i}")

                        bound: Dict[str, Any] = {}
                        if src_key.startswith("haproxy_lb.stats_sample"):
                            maxconn = int(sample_domain(tmpl["vars"]["maxconn"], rng))
                            bound["maxconn"] = maxconn
                            stv = tmpl.get("state_vars", {}).get(state, {})
                            cur = int(sample_domain(stv["cur_sess"], rng))
                            cur = min(cur, maxconn)
                            bound["cur_sess"] = cur
                            bound["queued"] = int(sample_domain(stv["queued"], rng))
                            bound["fe"] = "fe_public"
                            bound["ka_s"] = 15

                        msg = render_log(tmpl, state, rng, bound)
                        emit_row(t_ms, tmpl["lvl"], msg, svc, h, "")

            elif scope == "global":
                alloc_key = f"bg|{state}|{src_key}|global"
                n = allocator.alloc(alloc_key, eff)
                if n <= 0:
                    continue
                for i in range(n):
                    offset = int(((i + 0.5) / n) * 60_000)
                    t_ms = minute_start + offset + jitter_ms(f"bg|{minute}|{src_key}|global|{i}", -120, 120)
                    rng = make_rng(f"bg|{minute}|{src_key}|global|{i}")
                    host = choose_host_round_robin(hosts, i)

                    bound = {}
                    msg = render_log(tmpl, state, rng, bound)
                    emit_row(t_ms, tmpl["lvl"], msg, svc, host, "")
            else:
                raise ValueError(f"Unknown background scope: {scope}")


def _cap_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))


def enforce_flow_timing_constraints(flow: Dict[str, Any], delays: List[int]) -> None:
    """
    Ensure message-carried observed timing fields can match emitted timestamp gaps without clamping.
    This respects the YAML templates (dur_ms domains) by capping the relevant hop delays and,
    if needed, adjusting the final hop to ensure haproxy close dur_ms is within its domain.
    """
    emit_refs: List[str] = flow["emit"]

    # Constrain web-app response durations to match their respective template domains.
    web_delay_idx: Optional[int] = None
    web_min, web_max = 0, 0
    if "web_app.http_resp_200" in emit_refs and "web_app.http_req" in emit_refs:
        web_delay_idx = emit_refs.index("web_app.http_resp_200")  # delay since previous log (http_req)
        web_min, web_max = 5, 5000
    elif "web_app.stackegg_resp_200" in emit_refs and "web_app.stackegg_req" in emit_refs:
        web_delay_idx = emit_refs.index("web_app.stackegg_resp_200")  # delay since previous log (stackegg_req)
        web_min, web_max = 5, 3000

    if web_delay_idx is not None:
        delays[web_delay_idx] = _cap_int(delays[web_delay_idx], web_min, web_max)

    # Constrain HAProxy close duration to its template domain and keep it coherent with timestamps.
    if "haproxy_lb.frontend_close_ok" in emit_refs and "haproxy_lb.frontend_accept" in emit_refs:
        accept_idx = emit_refs.index("haproxy_lb.frontend_accept")
        close_idx = emit_refs.index("haproxy_lb.frontend_close_ok")
        close_min, close_max = 10, 6000

        # close_dur = time(close) - time(accept) = sum of delays after accept up through close
        close_dur = sum(delays[accept_idx + 1 : close_idx + 1])

        if close_dur > close_max:
            reduce_needed = close_dur - close_max

            # Prefer reducing web response time first (since it dominates), then other internal hops.
            reduction_order: List[int] = []
            if web_delay_idx is not None and (accept_idx + 1) <= web_delay_idx <= close_idx:
                reduction_order.append(web_delay_idx)
            for idx in range(accept_idx + 1, close_idx + 1):
                if idx != web_delay_idx:
                    reduction_order.append(idx)

            for idx in reduction_order:
                if reduce_needed <= 0:
                    break
                minv = web_min if idx == web_delay_idx else 0
                available = max(0, delays[idx] - minv)
                dec = min(reduce_needed, available)
                delays[idx] -= dec
                reduce_needed -= dec

        # Recompute and ensure minimum too (very unlikely with the given latency hints, but keep coherent).
        close_dur = sum(delays[accept_idx + 1 : close_idx + 1])
        if close_dur < close_min:
            # Add to the final hop (the close log's own delay) so chronology matches.
            needed = close_min - close_dur
            delays[close_idx] += needed


def simulate_flow_instance(flow: Dict[str, Any], state: str, start_ms: int, minute_for_controls: int, instance_idx: int) -> None:
    flow_id = flow["id"]
    lat_p50_mult, lat_p95_mult = lat_mult_for(state, minute_for_controls, flow_id)

    hap = COMPONENTS["haproxy_lb"]
    web = COMPONENTS["web_app"]
    hap_host = choose_host_round_robin(hap["hosts"], instance_idx)
    web_host = choose_host_round_robin(web["hosts"], instance_idx)

    rng = make_rng(f"flow|{state}|{flow_id}|{minute_for_controls}|{instance_idx}|start={start_ms}")

    req_id = sample_hex(16, rng)
    src_ip = sample_ip_from_cidr("203.0.113.0/24", rng)

    if flow_id == "stackegg_state_ajax":
        uri = "/stackegg/state"
        user_tier = "employee" if int(rng.integers(0, 20)) == 0 else "logged_in"
        bytes_out = int(rng.integers(300, 250000 + 1))
    else:
        uri = str(rng.choice(["/questions", "/search", "/users"]))
        user_tier = str(rng.choice(["anon", "logged_in", "employee"]))
        bytes_out = int(rng.integers(2000, 250000 + 1))

    # Keep status coherent with web_app.http_resp_200 template, which always logs "200".
    status = "200"

    delays: List[int] = []
    for pair in flow["latency_ms"]:
        p50, p95 = float(pair[0]), float(pair[1])
        p50 *= lat_p50_mult
        p95 *= lat_p95_mult
        cap = 2.7 * p95
        d = lognormal_from_p50_p95(p50, p95, rng, cap=cap)
        delays.append(int(max(0.0, d)))

    if flow_id == "page_view_connect_timeout":
        tmpl = LOG_TEMPLATES["haproxy_lb.frontend_connect_timeout"]
        lo, hi = map(int, tmpl["vars"]["waited_ms"]["v"])
        delays[0] = _cap_int(delays[0], lo, hi)

    # Ensure timing fields (dur_ms) can exactly match the timestamp gaps without clamping.
    enforce_flow_timing_constraints(flow, delays)

    # Compute emitted timestamps from delays (delay[i] is since previous log; delay[0] since start).
    t = int(start_ms)
    emitted_ms: List[int] = []
    for d in delays:
        t += int(d)
        emitted_ms.append(t)

    chain_bound_common = {
        "fe": "fe_public",
        "src_ip": src_ip,
        "method": "GET",
        "uri": uri,
        "req_id": req_id,
        "user_tier": user_tier,
        "bytes": bytes_out,
        "status": status,
    }

    for step_idx, ref in enumerate(flow["emit"]):
        tmpl = LOG_TEMPLATES[ref]
        cid, _ = ref.split(".", 1)
        comp = COMPONENTS[cid]
        svc = comp.get("svc", "") or ""
        if cid == "haproxy_lb":
            host = hap_host
        elif cid == "web_app":
            host = web_host
        else:
            host = choose_host_round_robin(comp.get("hosts", []) or [""], instance_idx)

        bound = dict(chain_bound_common)

        if ref == "web_app.http_resp_200":
            dur = emitted_ms[step_idx] - emitted_ms[step_idx - 1]
            bound["dur_ms"] = int(dur)
            bound["bytes"] = bytes_out
        elif ref == "web_app.stackegg_resp_200":
            dur = emitted_ms[step_idx] - emitted_ms[step_idx - 1]
            bound["dur_ms"] = int(dur)
            bound["age_s"] = int(rng.integers(0, 11))
        elif ref == "haproxy_lb.frontend_close_ok":
            accept_ms = emitted_ms[0]
            close_ms = emitted_ms[step_idx]
            dur = close_ms - accept_ms
            bound["dur_ms"] = int(dur)
            bound["bytes"] = bytes_out
            bound["status"] = status
        elif ref == "haproxy_lb.frontend_connect_timeout":
            bound["waited_ms"] = int(delays[0])

        msg_rng = make_rng(f"msg|{state}|{flow_id}|{minute_for_controls}|{instance_idx}|{ref}")
        msg = render_log(tmpl, state, msg_rng, bound)
        emit_row(emitted_ms[step_idx], tmpl["lvl"], msg, svc, host, "")


def simulate_flows_for_minute(minute: int, state: str, allocator: CarryAllocator) -> None:
    minute_start = minute * 60_000
    minute_end = minute_start + 60_000

    for flow in FLOWS_BY_STATE[state]:
        flow_id = flow["id"]
        mult = rate_mult_for(state, minute, flow_id) if state == "f" else 1.0
        eff_rpm = float(flow["rpm"]) * float(mult)
        if eff_rpm <= 0.0:
            continue

        alloc_key = f"flow|{state}|{flow_id}"
        n_instances = allocator.alloc(f"{alloc_key}|starts", eff_rpm)
        if n_instances <= 0:
            continue

        for i in range(n_instances):
            offset = int(((i + 0.5) / n_instances) * 60_000)
            start_ms = minute_start + offset + jitter_ms(f"st|{minute}|{flow_id}|{i}", -200, 200)
            start_ms = max(minute_start, min(minute_end - 1, start_ms))
            simulate_flow_instance(flow, state, start_ms, minute, i)


def emit_one_shots() -> None:
    f_start = int(SCENARIO["time"]["phases"]["f"]["start_min"])
    for os_ in ONE_SHOTS:
        at_min = int(os_["at_min"])
        ref = os_["ref"]
        count = int(os_["count"])
        hosts = list(os_.get("hosts", []) or [])
        cid, _ = ref.split(".", 1)
        comp = COMPONENTS[cid]
        tmpl = LOG_TEMPLATES[ref]
        svc = comp.get("svc", "") or ""

        # Keep one-shots within the event minute (start inclusive, end exclusive).
        minute_start = at_min * 60_000
        base_ms = minute_start + 5_000 + jitter_ms(f"os|{at_min}|{ref}|base", -800, 800)
        base_ms = max(minute_start, min(minute_start + 59_900, base_ms))

        for i in range(count):
            host = hosts[i % len(hosts)] if hosts else choose_host_round_robin(comp.get("hosts", []) or [""], i)
            t_ms = base_ms + i * 200 + jitter_ms(f"os|{at_min}|{ref}|{i}", -80, 80)
            t_ms = max(minute_start, min(minute_start + 59_999, t_ms))
            rng = make_rng(f"os|{at_min}|{ref}|{i}")

            bound: Dict[str, Any] = {}
            if ref == "feature_flag_db.flag_update":
                bound["flag"] = "stackegg_enabled"
                bound["value"] = "true"
                bound["actor"] = "deploy_bot"
            elif ref == "monitor_pingdom.check_alert":
                bound["site"] = "stackoverflow"
                bound["reason"] = "tcp_connect_timeout"
            elif ref == "ops_puppet.puppet_change":
                bound["change"] = "set_haproxy_maxconn_210"
                bound["target"] = "lb_fleet"
            elif ref == "haproxy_lb.reload_applied":
                bound["fe"] = "fe_public"
                bound["maxconn"] = 210
                bound["ka_s"] = 15

            os_state = "f" if at_min >= f_start else "n"
            msg = render_log(tmpl, os_state, rng, bound)
            emit_row(t_ms, tmpl["lvl"], msg, svc, host, "")


def run() -> None:
    total_minutes = int(SCENARIO["time"]["total_minutes"])
    n_start = int(SCENARIO["time"]["phases"]["n"]["start_min"])
    n_end = int(SCENARIO["time"]["phases"]["n"]["end_min"])
    f_start = int(SCENARIO["time"]["phases"]["f"]["start_min"])
    f_end = int(SCENARIO["time"]["phases"]["f"]["end_min"])

    bg_alloc = CarryAllocator()
    flow_alloc = CarryAllocator()

    for m in range(total_minutes):
        if n_start <= m < n_end:
            state = "n"
        elif f_start <= m < f_end:
            state = "f"
        else:
            continue

        simulate_background_for_minute(m, state, bg_alloc)
        simulate_flows_for_minute(m, state, flow_alloc)

    emit_one_shots()

    rows_sorted = sorted(rows, key=lambda r: (r.ms, r.seq))
    df = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r.ms) for r in rows_sorted],
            "level": [r.level for r in rows_sorted],
            "message": [r.message for r in rows_sorted],
            "trace_id": [r.trace_id for r in rows_sorted],
            "service": [r.service for r in rows_sorted],
            "host": [r.host for r in rows_sorted],
        }
    )

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert df["timestamp"].is_monotonic_increasing, "timestamps are not sorted"
    assert 20_000 <= len(df) <= 100_000, f"log volume out of bounds: {len(df)}"

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    run()
