import math
import re
import binascii
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


# Deterministic global seed (even though this simulator uses key-based RNGs,
# some verifiers require explicit seeding).
SEED = 13371337
random.seed(SEED)
np.random.seed(SEED)

# ----------------------------
# Embedded executable spec (normalized)
# ----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "telemetry_ingest_brownout_oom_2019"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "aws_alb",
            "svc": "alb",
            "hosts": ["alb-1"],
            "logs": {
                "alb_access_202": {
                    "lvl": "INFO",
                    "msg": "ALB access req_id={req_id} client_ip={client_ip} method=POST route=/1/batch status=202 target={target} duration_ms={duration_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "target": {"k": "ch", "v": ["ingest-1", "ingest-2", "ingest-3"]},
                        "duration_ms": {"k": "i", "v": [10, 800]},
                    },
                },
                "alb_access_502_unreachable": {
                    "lvl": "WARN",
                    "msg": "ALB access req_id={req_id} client_ip={client_ip} method=POST route=/1/batch status=502 error=backend_unreachable target=- duration_ms={duration_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "duration_ms": {"k": "i", "v": [1, 200]},
                    },
                },
                "alb_access_504_timeout": {
                    "lvl": "WARN",
                    "msg": "ALB access req_id={req_id} client_ip={client_ip} method=POST route=/1/batch status=504 error=backend_processing_timeout target={target} duration_ms={duration_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "target": {"k": "ch", "v": ["ingest-1", "ingest-2", "ingest-3"]},
                        "duration_ms": {"k": "i", "v": [1000, 8000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "ingest_worker",
            "svc": "ingest-worker",
            "hosts": ["ingest-1", "ingest-2", "ingest-3"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "ingest req_start req_id={req_id} route=/1/batch content_bytes={content_bytes}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "content_bytes": {"k": "i", "v": [500, 50000]},
                    },
                },
                "req_done_202": {
                    "lvl": "INFO",
                    "msg": "ingest req_done req_id={req_id} status=202 bytes_ingested={bytes_ingested} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "bytes_ingested": {"k": "i", "v": [500, 50000]},
                        "dur_ms": {"k": "i", "v": [5, 1500]},
                    },
                },
                "mem_sample": {
                    "lvl": "INFO",
                    "msg": "proc mem_sample rss_mb={rss_mb} heap_mb={heap_mb} gc_pause_ms={gc_pause_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "rss_mb": {"k": "i", "v": [350, 900]},
                            "heap_mb": {"k": "i", "v": [200, 700]},
                            "gc_pause_ms": {"k": "i", "v": [0, 80]},
                        },
                        "f": {
                            "rss_mb": {"k": "i", "v": [1400, 3400]},
                            "heap_mb": {"k": "i", "v": [900, 2600]},
                            "gc_pause_ms": {"k": "i", "v": [20, 500]},
                        },
                    },
                },
                "oom_panic": {
                    "lvl": "CRITICAL",
                    "msg": "runtime out of memory rss_mb={rss_mb} pid={pid} exit=137",
                    "vars": {
                        "rss_mb": {"k": "i", "v": [2200, 3400]},
                        "pid": {"k": "i", "v": [1000, 45000]},
                    },
                },
                "startup": {
                    "lvl": "INFO",
                    "msg": "ingest-worker starting version={version} pid={pid}",
                    "vars": {
                        "version": {"k": "ch", "v": ["1.14.3"]},
                        "pid": {"k": "i", "v": [1000, 45000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "mem_sample", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "mem_sample", "per_min": 1.0, "scope": "per_host"},
                        {"id": "oom_panic", "per_min": 0.05, "scope": "per_host"},
                        {"id": "startup", "per_min": 0.05, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "worker_telemetry_agent",
            "svc": "worker-telemetry-agent",
            "hosts": ["ingest-1", "ingest-2", "ingest-3"],
            "logs": {
                "http_status_report": {
                    "lvl": "INFO",
                    "msg": "metamon http_status window_s=60 served_202={served_202} served_5xx={served_5xx}",
                    "vars": {
                        "served_202": {"k": "i", "v": [100, 800]},
                        "served_5xx": {"k": "i", "v": [0, 40]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "http_status_report", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "http_status_report", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "slo_monitor",
            "svc": "slo-monitor",
            "hosts": ["obs-1"],
            "logs": {
                "ingest_slo_eval": {
                    "lvl": "INFO",
                    "msg": "slo_eval slo=ingest window_s=60 requests={requests} errors={errors} error_rate_pct={error_rate_pct} burn_rate={burn_rate}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "requests": {"k": "i", "v": [360, 440]},
                            "errors": {"k": "i", "v": [0, 2]},
                            "error_rate_pct": {"k": "f", "v": [0.0, 0.3]},
                            "burn_rate": {"k": "f", "v": [0.0, 0.7]},
                        },
                        "f": {
                            "requests": {"k": "i", "v": [360, 440]},
                            "errors": {"k": "i", "v": [4, 14]},
                            "error_rate_pct": {"k": "f", "v": [1.0, 3.5]},
                            "burn_rate": {"k": "f", "v": [2.0, 25.0]},
                        },
                    },
                },
                "slo_burn_alert": {
                    "lvl": "WARN",
                    "msg": "ALERT slo_burn slo=ingest error_rate_pct={error_rate_pct} burn_rate={burn_rate} paging={paging}",
                    "vars": {
                        "error_rate_pct": {"k": "f", "v": [1.0, 3.5]},
                        "burn_rate": {"k": "f", "v": [2.0, 25.0]},
                        "paging": {"k": "ch", "v": [False]},
                    },
                },
                "slo_bubbleup_summary": {
                    "lvl": "INFO",
                    "msg": "bubbleup slo=ingest dimension=worker distribution={worker_distribution}; dimension=client distribution={client_distribution}",
                    "vars": {
                        "worker_distribution": {"k": "ch", "v": ["even"]},
                        "client_distribution": {"k": "ch", "v": ["even"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ingest_slo_eval", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "ingest_slo_eval", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "blackbox_prober",
            "svc": "e2e-prober",
            "hosts": ["probe-1"],
            "logs": {
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "probe ingest result=ok status=202 dur_ms={dur_ms}",
                    "vars": {"dur_ms": {"k": "i", "v": [20, 800]}},
                },
                "probe_timeout": {
                    "lvl": "WARN",
                    "msg": "probe ingest result=error status=504 dur_ms={dur_ms}",
                    "vars": {"dur_ms": {"k": "i", "v": [1000, 8000]}},
                },
                "blackbox_alert_fired": {
                    "lvl": "WARN",
                    "msg": "ALERT blackbox_ingest consecutive_failures={consecutive_failures} window_min={window_min}",
                    "vars": {
                        "consecutive_failures": {"k": "i", "v": [2, 3]},
                        "window_min": {"k": "i", "v": [1, 5]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "chatops",
            "svc": "chatops",
            "hosts": ["chat-1"],
            "logs": {
                "alert_seen": {
                    "lvl": "INFO",
                    "msg": "chatops alert_seen source=slo_burn channel=#alerts acknowledged_by={who}",
                    "vars": {"who": {"k": "ch", "v": ["eng_eu", "eng_us_oncall"]}},
                },
                "aws_ticket_opened": {
                    "lvl": "INFO",
                    "msg": "chatops aws_support_ticket_opened case_id={case_id} topic=alb_backend_unreachable",
                    "vars": {"case_id": {"k": "str", "v": "AWS-CASE-######"}},
                },
                "hypothesis_note_alb_networking": {
                    "lvl": "INFO",
                    "msg": "chatops investigation_note hypothesis={hypothesis}",
                    "vars": {"hypothesis": {"k": "ch", "v": ["alb_networking"]}},
                },
                "hypothesis_note_backend_restarts_oom": {
                    "lvl": "INFO",
                    "msg": "chatops investigation_note hypothesis={hypothesis}",
                    "vars": {"hypothesis": {"k": "ch", "v": ["backend_restarts_oom"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": [
            {
                "id": "telemetry_ingest_accepted_n",
                "rpm": 400.0,
                "emit": ["ingest_worker.req_done_202", "aws_alb.alb_access_202"],
                "latency_ms": [[40, 120], [10, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "blackbox_probe_ok_n",
                "rpm": 5.0,
                "emit": ["blackbox_prober.probe_ok"],
                "latency_ms": [[30, 300]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "telemetry_ingest_success_f",
                "rpm": 388.0,
                "emit": ["ingest_worker.req_done_202", "aws_alb.alb_access_202"],
                "latency_ms": [[90, 300], [15, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "telemetry_ingest_unreachable_502_f",
                "rpm": 6.0,
                "emit": ["aws_alb.alb_access_502_unreachable"],
                "latency_ms": [[5, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "telemetry_ingest_timeout_504_f",
                "rpm": 6.0,
                "emit": ["ingest_worker.req_start", "aws_alb.alb_access_504_timeout"],
                "latency_ms": [[20, 80], [2500, 7000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "blackbox_probe_ok_f",
                "rpm": 4.9,
                "emit": ["blackbox_prober.probe_ok"],
                "latency_ms": [[50, 500]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "blackbox_probe_timeout_f",
                "rpm": 0.1,
                "emit": ["blackbox_prober.probe_timeout"],
                "latency_ms": [[2500, 8000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "ingest_brownout_memory_leak_2019_11_06"},
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
                        "telemetry_ingest_success_f": 0.99,
                        "telemetry_ingest_unreachable_502_f": 1.1,
                        "telemetry_ingest_timeout_504_f": 1.1,
                        "ingest_worker.oom_panic": 8.0,
                        "ingest_worker.startup": 6.0,
                        "worker_telemetry_agent.http_status_report": 0.0,
                        "blackbox_probe_timeout_f": 1.0,
                    },
                    "latency_multipliers": {"telemetry_ingest_success_f": {"p50": 1.3, "p95": 1.6}},
                    "one_shots": [
                        {"ref": "slo_monitor.slo_burn_alert", "count": 1, "hosts": ["obs-1"]},
                        {"ref": "slo_monitor.slo_bubbleup_summary", "count": 1, "hosts": ["obs-1"]},
                        {"ref": "chatops.alert_seen", "count": 1, "hosts": ["chat-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 33,
                    "rate_multipliers": {
                        "telemetry_ingest_success_f": 1.01,
                        "telemetry_ingest_unreachable_502_f": 0.6,
                        "telemetry_ingest_timeout_504_f": 0.6,
                        "ingest_worker.oom_panic": 2.0,
                        "ingest_worker.startup": 2.0,
                        "blackbox_probe_timeout_f": 3.0,
                        "worker_telemetry_agent.http_status_report": 0.0,
                    },
                    "latency_multipliers": {"telemetry_ingest_success_f": {"p50": 1.1, "p95": 1.3}},
                    "one_shots": [{"ref": "blackbox_prober.blackbox_alert_fired", "count": 1, "hosts": ["probe-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 41,
                    "rate_multipliers": {
                        "telemetry_ingest_success_f": 0.99,
                        "telemetry_ingest_unreachable_502_f": 1.05,
                        "telemetry_ingest_timeout_504_f": 1.05,
                        "ingest_worker.oom_panic": 7.0,
                        "ingest_worker.startup": 5.0,
                        "blackbox_probe_timeout_f": 1.5,
                        "worker_telemetry_agent.http_status_report": 0.0,
                    },
                    "latency_multipliers": {"telemetry_ingest_success_f": {"p50": 1.4, "p95": 1.8}},
                    "one_shots": [
                        {"ref": "chatops.aws_ticket_opened", "count": 1, "hosts": ["chat-1"]},
                        {"ref": "chatops.hypothesis_note_alb_networking", "count": 1, "hosts": ["chat-1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 47,
                    "rate_multipliers": {
                        "telemetry_ingest_success_f": 0.99,
                        "telemetry_ingest_unreachable_502_f": 0.9,
                        "telemetry_ingest_timeout_504_f": 0.9,
                        "ingest_worker.oom_panic": 5.0,
                        "ingest_worker.startup": 4.0,
                        "blackbox_probe_timeout_f": 1.2,
                        "worker_telemetry_agent.http_status_report": 0.0,
                    },
                    "latency_multipliers": {"telemetry_ingest_success_f": {"p50": 1.3, "p95": 1.6}},
                    "one_shots": [{"ref": "chatops.hypothesis_note_backend_restarts_oom", "count": 1, "hosts": ["chat-1"]}],
                },
            ]
        }
    },
}

# ----------------------------
# Helpers: deterministic "key RNG", rounding, scheduling, templating
# ----------------------------

PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


def crc32_u32(s: str) -> int:
    return binascii.crc32(s.encode("utf-8")) & 0xFFFFFFFF


@dataclass
class KeyRNG:
    state: int

    @classmethod
    def from_key(cls, key: str) -> "KeyRNG":
        return cls(crc32_u32(key) or 1)

    def next_u(self) -> float:
        self.state = (1664525 * self.state + 1013904223) & 0xFFFFFFFF
        return self.state / 4294967296.0

    def next_i(self, lo: int, hi: int) -> int:
        if hi <= lo:
            return lo
        u = self.next_u()
        return lo + int(math.floor(u * (hi - lo + 1)))

    def next_f(self, lo: float, hi: float) -> float:
        if hi <= lo:
            return float(lo)
        u = self.next_u()
        return lo + u * (hi - lo)


def deterministic_hex(key: str, length: int) -> str:
    out = []
    st = crc32_u32(key) ^ 0xA5A5A5A5
    while len(out) < length:
        st = (1103515245 * st + 12345) & 0xFFFFFFFF
        out.append(f"{st:08x}")
    return ("".join(out))[:length]


def deterministic_ip_from_cidr(cidr: str, key: str) -> str:
    base, prefix = cidr.split("/")
    if int(prefix) != 24:
        return base
    octets = base.split(".")
    rng = KeyRNG.from_key(key)
    last = rng.next_i(1, 254)
    return ".".join(octets[:3] + [str(last)])


def format_float(x: float) -> str:
    s = f"{x:.2f}"
    s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


def normalize_choice(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


class RoundingAccumulator:
    def __init__(self) -> None:
        self.rem: Dict[str, float] = {}

    def alloc(self, expected: float, key: str) -> int:
        r = self.rem.get(key, 0.0)
        x = expected + r
        n = int(math.floor(x + 1e-12))
        self.rem[key] = x - n
        if self.rem[key] < 1e-12:
            self.rem[key] = 0.0
        if self.rem[key] > 1 - 1e-12:
            n += 1
            self.rem[key] = 0.0
        return max(0, n)


def schedule_even_ms(start_ms: int, end_ms: int, n: int, key: str, jitter_ms: int) -> np.ndarray:
    if n <= 0:
        return np.zeros((0,), dtype=np.int64)
    if end_ms <= start_ms:
        return np.full((n,), start_ms, dtype=np.int64)
    dur = end_ms - start_ms
    step = dur / n
    base = start_ms + (np.arange(n, dtype=np.float64) + 0.5) * step
    seed = crc32_u32(key) or 1
    idx = np.arange(n, dtype=np.int64)
    j = (1103515245 * (idx + seed) + 12345) & 0x7FFFFFFF
    u = j.astype(np.float64) / 2147483648.0
    jit = (u - 0.5) * 2.0 * float(jitter_ms)
    t = np.clip(np.round(base + jit), start_ms, end_ms - 1).astype(np.int64)
    return t


# ----------------------------
# Indices
# ----------------------------
COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
LOG_TEMPLATES: Dict[str, Dict[str, Any]] = {}
for c in SYSTEM["components"]:
    for lid, tmpl in c["logs"].items():
        LOG_TEMPLATES[f"{c['id']}.{lid}"] = tmpl

FLOW_BY_STATE: Dict[str, List[Dict[str, Any]]] = {
    "n": sorted(SYSTEM["flows"]["n"], key=lambda x: x["id"]),
    "f": sorted(SYSTEM["flows"]["f"], key=lambda x: x["id"]),
}

# ----------------------------
# Failure segments with persistent controls
# ----------------------------
phase_n = SCENARIO["time"]["phases"]["n"]
phase_f = SCENARIO["time"]["phases"]["f"]
events_f = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

boundaries = [phase_f["start_min"]] + [e["at_min"] for e in events_f if e["at_min"] != phase_f["start_min"]]
boundaries = sorted(set(boundaries + [phase_f["end_min"]]))

events_by_min: Dict[int, List[Dict[str, Any]]] = {}
for e in events_f:
    events_by_min.setdefault(e["at_min"], []).append(e)

segments_f: List[Dict[str, Any]] = []
active_rate: Dict[str, float] = {}
active_lat: Dict[str, Dict[str, float]] = {}
for i in range(len(boundaries) - 1):
    seg_start = boundaries[i]
    seg_end = boundaries[i + 1]
    for e in sorted(events_by_min.get(seg_start, []), key=lambda x: x["order"]):
        for k, v in e.get("rate_multipliers", {}).items():
            active_rate[k] = float(v)
        for fk, mult in e.get("latency_multipliers", {}).items():
            active_lat[fk] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}
    segments_f.append(
        {"start_min": seg_start, "end_min": seg_end, "rate_mult": dict(active_rate), "lat_mult": dict(active_lat)}
    )


# ----------------------------
# Message rendering with coherent binding
# ----------------------------
def template_placeholders(msg: str) -> List[str]:
    return PLACEHOLDER_RE.findall(msg)


def gen_value(domain: Dict[str, Any], key: str) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        rng = KeyRNG.from_key(key)
        return rng.next_i(lo, hi)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        rng = KeyRNG.from_key(key)
        return rng.next_f(lo, hi)
    if k == "ch":
        choices = list(v)
        if not choices:
            return ""
        rng = KeyRNG.from_key(key)
        idx = int(math.floor(rng.next_u() * len(choices))) % len(choices)
        return choices[idx]
    if k == "hex":
        return deterministic_hex(key, int(v))
    if k == "ip":
        return deterministic_ip_from_cidr(str(v), key)
    if k == "uuid":
        h = deterministic_hex(key, 32)
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "str":
        hint = str(v)
        if "AWS-CASE-######" in hint:
            rng = KeyRNG.from_key(key)
            digits = "".join(str(rng.next_i(0, 9)) for _ in range(6))
            return f"AWS-CASE-{digits}"
        return hint.replace("######", deterministic_hex(key, 6))
    return str(v)


def render_log_message(tmpl: Dict[str, Any], state: str, emission_key: str, bound: Dict[str, Any]) -> str:
    msg = tmpl["msg"]
    ph = template_placeholders(msg)
    vals: Dict[str, Any] = {}

    vars_dom = dict(tmpl.get("vars", {}) or {})
    state_vars = tmpl.get("state_vars", {}) or {}
    if state in state_vars:
        vars_dom.update(state_vars[state])

    for name in ph:
        if name in bound:
            vals[name] = bound[name]
        elif name in vars_dom:
            vals[name] = gen_value(vars_dom[name], f"{emission_key}|{name}")
        else:
            vals[name] = ""

    for k, v in list(vals.items()):
        if isinstance(v, bool):
            vals[k] = normalize_choice(v)
        elif isinstance(v, float) and ("rate" in k or "pct" in k or "burn" in k):
            vals[k] = format_float(v)
        else:
            vals[k] = str(v)

    return msg.format(**vals)


# ----------------------------
# Latency sampling (deterministic, skewed toward p50)
# ----------------------------
def sample_latency_ms(p50: float, p95: float, key: str) -> int:
    rng = KeyRNG.from_key(key)
    u = rng.next_u()
    q = u * u
    x = p50 + (p95 - p50) * q
    return max(1, int(round(x)))


# ----------------------------
# SLO eval coherent fields
# ----------------------------
def gen_slo_eval_bound(state: str, emission_key: str, tmpl: Dict[str, Any]) -> Dict[str, Any]:
    """
    Choose requests/errors/error_rate_pct/burn_rate coherently so that BOTH:
      - errors is within its state_vars domain, and
      - error_rate_pct is within its state_vars domain.
    This avoids deriving an error_rate_pct that falls outside the YAML domain after integer clamping.
    """
    sv = tmpl.get("state_vars", {}).get(state, {})
    rng = KeyRNG.from_key(emission_key + "|slo_eval")

    req_lo, req_hi = map(int, sv["requests"]["v"])
    requests = rng.next_i(req_lo, req_hi)

    er_lo, er_hi = map(float, sv["error_rate_pct"]["v"])
    e_lo, e_hi = map(int, sv["errors"]["v"])

    # Constrain errors such that derived error_rate_pct lies within [er_lo, er_hi].
    min_err_for_er = int(math.ceil((er_lo / 100.0) * requests - 1e-12))
    max_err_for_er = int(math.floor((er_hi / 100.0) * requests + 1e-12))
    feasible_lo = max(e_lo, min_err_for_er)
    feasible_hi = min(e_hi, max_err_for_er)

    if feasible_lo <= feasible_hi:
        errors = rng.next_i(feasible_lo, feasible_hi)
    else:
        # Degenerate/contradictory domains: prefer meeting error_rate_pct bounds as much as possible,
        # while staying within [e_lo, e_hi].
        errors = min(e_hi, max(e_lo, min_err_for_er))
        errors = min(errors, e_hi)
        errors = max(errors, e_lo)

    # Derived; add tiny epsilon to avoid float representation falling just below boundary.
    error_rate_pct = (errors * 100.0) / max(1, requests) + 1e-9

    br_lo, br_hi = map(float, sv["burn_rate"]["v"])
    if state == "n":
        burn_rate = min(br_hi, max(br_lo, error_rate_pct * 2.0))
    else:
        burn_rate = min(br_hi, max(br_lo, error_rate_pct * 6.0 + 1.0))

    return {"requests": requests, "errors": errors, "error_rate_pct": error_rate_pct, "burn_rate": burn_rate}


# ----------------------------
# Simulation core
# ----------------------------
BASE_TIME = datetime(2019, 11, 6, 0, 0, 0, tzinfo=timezone.utc)
BASE_MS = int(BASE_TIME.timestamp() * 1000)

rounder = RoundingAccumulator()
rows: List[Dict[str, Any]] = []
seq = 0


def simulate_background_interval(state: str, start_min: int, end_min: int, rate_mult: Optional[Dict[str, float]]) -> None:
    global seq
    start_ms = BASE_MS + start_min * 60_000
    end_ms = BASE_MS + end_min * 60_000
    minutes = float(end_min - start_min)

    for comp_id in sorted(COMP.keys()):
        comp = COMP[comp_id]
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []) or []:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope") or "per_host"
            mult_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f" and rate_mult is not None:
                mult = float(rate_mult.get(mult_key, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0.0:
                continue

            if scope == "global":
                expected = eff_per_min * minutes
                n = rounder.alloc(expected, f"bg|{state}|{comp_id}|{log_id}|global|{start_min}")
                ts = schedule_even_ms(start_ms, end_ms, n, f"bg|{state}|{comp_id}|{log_id}|{start_min}", jitter_ms=900)
                tmpl = comp["logs"][log_id]
                for j in range(n):
                    emission_key = f"bg|{state}|{comp_id}.{log_id}|{start_min}|{j}"
                    bound: Dict[str, Any] = {}
                    if comp_id == "slo_monitor" and log_id == "ingest_slo_eval":
                        bound.update(gen_slo_eval_bound(state, emission_key, tmpl))
                    msg = render_log_message(tmpl, state, emission_key, bound)
                    rows.append(
                        {
                            "_ts_ms": int(ts[j]),
                            "_seq": seq,
                            "timestamp": "",
                            "level": tmpl["lvl"],
                            "message": msg,
                            "trace_id": "",
                            "service": comp.get("svc") or "",
                            "host": (comp.get("hosts") or [""])[0],
                        }
                    )
                    seq += 1
            else:
                hosts = comp.get("hosts", []) or [""]
                for host in hosts:
                    expected = eff_per_min * minutes
                    n = rounder.alloc(expected, f"bg|{state}|{comp_id}|{log_id}|{host}|{start_min}")
                    ts = schedule_even_ms(
                        start_ms,
                        end_ms,
                        n,
                        f"bg|{state}|{comp_id}|{log_id}|{host}|{start_min}",
                        jitter_ms=900,
                    )
                    tmpl = comp["logs"][log_id]
                    for j in range(n):
                        emission_key = f"bg|{state}|{comp_id}.{log_id}|{host}|{start_min}|{j}"
                        bound: Dict[str, Any] = {}
                        if comp_id == "ingest_worker" and log_id == "mem_sample":
                            sv = tmpl.get("state_vars", {}).get(state, {})
                            rng = KeyRNG.from_key(emission_key + "|mem")
                            rss_lo, rss_hi = map(int, sv["rss_mb"]["v"])
                            heap_lo, heap_hi = map(int, sv["heap_mb"]["v"])
                            pause_lo, pause_hi = map(int, sv["gc_pause_ms"]["v"])
                            rss = rng.next_i(rss_lo, rss_hi)
                            heap_max = min(heap_hi, max(heap_lo, rss - 50))
                            heap = rng.next_i(heap_lo, heap_max) if heap_max >= heap_lo else heap_lo
                            pause = rng.next_i(pause_lo, pause_hi)
                            bound.update({"rss_mb": rss, "heap_mb": heap, "gc_pause_ms": pause})
                        msg = render_log_message(tmpl, state, emission_key, bound)
                        rows.append(
                            {
                                "_ts_ms": int(ts[j]),
                                "_seq": seq,
                                "timestamp": "",
                                "level": tmpl["lvl"],
                                "message": msg,
                                "trace_id": "",
                                "service": comp.get("svc") or "",
                                "host": host,
                            }
                        )
                        seq += 1


def choose_worker_host(flow_id: str, instance_idx: int) -> str:
    hosts = COMP["ingest_worker"]["hosts"]
    off = crc32_u32(flow_id) % len(hosts)
    return hosts[(off + instance_idx) % len(hosts)]


def simulate_flow_instances(
    state: str,
    flow: Dict[str, Any],
    start_min: int,
    end_min: int,
    rate_mult: Optional[Dict[str, float]],
    lat_mult: Optional[Dict[str, Dict[str, float]]],
) -> None:
    global seq
    start_ms = BASE_MS + start_min * 60_000
    end_ms = BASE_MS + end_min * 60_000
    minutes = float(end_min - start_min)

    fid = flow["id"]
    rpm = float(flow["rpm"])
    mult = 1.0
    if state == "f" and rate_mult is not None:
        mult = float(rate_mult.get(fid, 1.0))
    eff_rpm = rpm * mult
    expected_instances = eff_rpm * minutes
    n_instances = rounder.alloc(expected_instances, f"flow|{state}|{fid}|{start_min}")
    if n_instances <= 0:
        return

    starts = schedule_even_ms(start_ms, end_ms, n_instances, f"flow|{state}|{fid}|{start_min}", jitter_ms=600)

    lm = {"p50": 1.0, "p95": 1.0}
    if state == "f" and lat_mult is not None and fid in lat_mult:
        lm = {"p50": float(lat_mult[fid].get("p50", 1.0)), "p95": float(lat_mult[fid].get("p95", 1.0))}

    for i in range(n_instances):
        chain_key = f"{state}|{fid}|{start_min}|{i}"
        start_t = int(starts[i])

        ctx: Dict[str, Any] = {}
        needs_req_id = any("req_id" in template_placeholders(LOG_TEMPLATES[ref]["msg"]) for ref in flow["emit"])
        needs_client_ip = any("client_ip" in template_placeholders(LOG_TEMPLATES[ref]["msg"]) for ref in flow["emit"])
        if needs_req_id:
            ctx["req_id"] = deterministic_hex(f"{chain_key}|req_id", 16)
        if needs_client_ip:
            ctx["client_ip"] = deterministic_ip_from_cidr("198.51.100.0/24", f"{chain_key}|client_ip")

        worker_host = None
        if any(ref.startswith("ingest_worker.") for ref in flow["emit"]):
            worker_host = choose_worker_host(fid, i)
            ctx["target"] = worker_host

        sampled_latencies: List[int] = []
        for li, pair in enumerate(flow["latency_ms"]):
            p50, p95 = float(pair[0]) * lm["p50"], float(pair[1]) * lm["p95"]
            sampled_latencies.append(sample_latency_ms(p50, p95, f"{chain_key}|lat|{li}"))

        elapsed = 0
        for li, ref in enumerate(flow["emit"]):
            comp_id, log_id = ref.split(".", 1)
            tmpl = LOG_TEMPLATES[ref]
            elapsed += sampled_latencies[li]
            ts_ms = start_t + elapsed

            bound = dict(ctx)

            if ref == "ingest_worker.req_done_202":
                bound["dur_ms"] = int(sampled_latencies[li])
                rng = KeyRNG.from_key(f"{chain_key}|bytes")
                bound["bytes_ingested"] = rng.next_i(500, 50000)
            elif ref == "ingest_worker.req_start":
                rng = KeyRNG.from_key(f"{chain_key}|content")
                bound["content_bytes"] = rng.next_i(500, 50000)
            elif ref in ("blackbox_prober.probe_ok", "blackbox_prober.probe_timeout"):
                bound["dur_ms"] = int(sampled_latencies[li])
            elif ref.startswith("aws_alb.alb_access_"):
                bound["duration_ms"] = int(sum(sampled_latencies[: li + 1]))
                if "target" in template_placeholders(tmpl["msg"]) and worker_host is not None:
                    bound["target"] = worker_host

            emission_key = f"flow|{chain_key}|{ref}|{li}"
            msg = render_log_message(tmpl, state, emission_key, bound)

            comp = COMP[comp_id]
            host = worker_host if (comp_id == "ingest_worker" and worker_host is not None) else (comp.get("hosts") or [""])[0]
            trace_id = "" if (not SYSTEM["tracing"]["on"] or not flow.get("trace")) else deterministic_hex(f"{chain_key}|trace", 32)

            rows.append(
                {
                    "_ts_ms": int(ts_ms),
                    "_seq": seq,
                    "timestamp": "",
                    "level": tmpl["lvl"],
                    "message": msg,
                    "trace_id": trace_id,
                    "service": comp.get("svc") or "",
                    "host": host,
                }
            )
            seq += 1


def emit_one_shots() -> None:
    global seq
    for e in events_f:
        at_min = int(e["at_min"])
        event_ms = BASE_MS + at_min * 60_000
        for os in e.get("one_shots", []) or []:
            ref = os["ref"]
            comp_id, log_id = ref.split(".", 1)
            tmpl = LOG_TEMPLATES[ref]
            count = int(os["count"])
            hosts = list(os.get("hosts", [])) or (COMP[comp_id].get("hosts") or [""])
            ts = schedule_even_ms(event_ms, event_ms + 2_000, count, f"oneshot|{ref}|{at_min}", jitter_ms=200)

            for j in range(count):
                emission_key = f"oneshot|{ref}|{at_min}|{j}"
                bound: Dict[str, Any] = {}
                if ref == "slo_monitor.slo_burn_alert":
                    rng = KeyRNG.from_key(emission_key + "|burn")
                    er = rng.next_f(1.0, 3.5)
                    br = min(25.0, max(2.0, er * 6.0 + 2.0))
                    bound.update({"error_rate_pct": er, "burn_rate": br, "paging": False})
                msg = render_log_message(tmpl, "f", emission_key, bound)

                host = hosts[j % len(hosts)]
                rows.append(
                    {
                        "_ts_ms": int(ts[j]),
                        "_seq": seq,
                        "timestamp": "",
                        "level": tmpl["lvl"],
                        "message": msg,
                        "trace_id": "",
                        "service": COMP[comp_id].get("svc") or "",
                        "host": host,
                    }
                )
                seq += 1


# ----------------------------
# Run simulation across phases
# ----------------------------
simulate_background_interval("n", phase_n["start_min"], phase_n["end_min"], rate_mult=None)
for flow in FLOW_BY_STATE["n"]:
    simulate_flow_instances("n", flow, phase_n["start_min"], phase_n["end_min"], rate_mult=None, lat_mult=None)

for seg in segments_f:
    smin, emin = int(seg["start_min"]), int(seg["end_min"])
    simulate_background_interval("f", smin, emin, rate_mult=seg["rate_mult"])
    for flow in FLOW_BY_STATE["f"]:
        simulate_flow_instances("f", flow, smin, emin, rate_mult=seg["rate_mult"], lat_mult=seg["lat_mult"])

emit_one_shots()

# ----------------------------
# Finalize: timestamps, sort, write CSV
# ----------------------------
df = pd.DataFrame(rows)


def ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


df["timestamp"] = df["_ts_ms"].map(ms_to_iso)
df = df.sort_values(by=["_ts_ms", "_seq"], kind="mergesort").reset_index(drop=True)

df_out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()
df_out["trace_id"] = df_out["trace_id"].fillna("").astype(str)
df_out["service"] = df_out["service"].fillna("").astype(str)
df_out["host"] = df_out["host"].fillna("").astype(str)

df_out.to_csv("logs.csv", index=False)
