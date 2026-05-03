import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "hosted_package_repo"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "external_monitor": {
            "svc": "monitor",
            "hosts": ["mon-1"],
            "logs": {
                "check_ok": {
                    "lvl": "INFO",
                    "msg": "check ok url={url} status={status} latency_ms={latency_ms}",
                    "vars": {
                        "url": {"k": "ch", "v": ["https://repo.example.com/health", "https://repo.example.com/repodata/index.json"]},
                        "status": {"k": "ch", "v": ["200", "204"]},
                        "latency_ms": {"k": "i", "v": [20, 800]},
                    },
                },
                "check_fail": {
                    "lvl": "ERROR",
                    "msg": "check failed url={url} error={error} latency_ms={latency_ms}",
                    "vars": {
                        "url": {"k": "ch", "v": ["https://repo.example.com/health", "https://repo.example.com/repodata/index.json"]},
                        "error": {"k": "ch", "v": ["timeout", "http_503", "http_504", "connection_reset"]},
                        "latency_ms": {"k": "i", "v": [200, 8000]},
                    },
                },
                "page_sent": {
                    "lvl": "CRITICAL",
                    "msg": "paging oncall service={service} incident={incident_id}",
                    "vars": {
                        "service": {"k": "ch", "v": ["hosted-package-repo"]},
                        "incident_id": {"k": "hex", "v": 12},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "edge_lb": {
            "svc": "edge-lb",
            "hosts": ["lb-1", "lb-2"],
            "logs": {
                "access_2xx": {
                    "lvl": "INFO",
                    "msg": "access method={method} uri={uri} status={status} bytes={bytes} rt_ms={rt_ms} backend={backend_host} upstream_ms={upstream_ms}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "uri": {"k": "str", "v": "repo_request_path"},
                        "status": {"k": "ch", "v": ["200", "206", "304"]},
                        "bytes": {"k": "i", "v": [0, 8000000]},
                        "rt_ms": {"k": "i", "v": [5, 12000]},
                        "backend_host": {"k": "ch", "v": ["fe-1", "fe-2", "fe-3"]},
                        "upstream_ms": {"k": "i", "v": [2, 12000]},
                    },
                },
                "access_5xx": {
                    "lvl": "INFO",
                    "msg": "access method={method} uri={uri} status={status} bytes={bytes} rt_ms={rt_ms} backend={backend_host} upstream_ms={upstream_ms}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "uri": {"k": "str", "v": "repo_request_path"},
                        "status": {"k": "ch", "v": ["503", "504"]},
                        "bytes": {"k": "i", "v": [0, 3000]},
                        "rt_ms": {"k": "i", "v": [200, 20000]},
                        "backend_host": {"k": "ch", "v": ["fe-1", "fe-2", "fe-3"]},
                        "upstream_ms": {"k": "i", "v": [200, 20000]},
                    },
                },
                "lb_stats": {
                    "lvl": "INFO",
                    "msg": "stats active_conns={active_conns} backend_q={backend_q} five_xx_1m={five_xx_1m}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active_conns": {"k": "i", "v": [50, 1500]},
                            "backend_q": {"k": "i", "v": [0, 80]},
                            "five_xx_1m": {"k": "i", "v": [0, 5]},
                        },
                        "f": {
                            "active_conns": {"k": "i", "v": [800, 12000]},
                            "backend_q": {"k": "i", "v": [200, 8000]},
                            "five_xx_1m": {"k": "i", "v": [20, 6000]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "lb_stats", "per_min": 1, "scope": "per_host"}]},
                "f": {"emit": [{"id": "lb_stats", "per_min": 1, "scope": "per_host"}]},
            },
        },
        "app_frontend": {
            "svc": "repo-frontend",
            "hosts": ["fe-1", "fe-2", "fe-3"],
            "logs": {
                "app_metrics": {
                    "lvl": "INFO",
                    "msg": "metrics inflight={inflight} busy_pct={busy_pct} dns_avg_ms={dns_avg_ms} dns_timeouts_1m={dns_timeouts_1m} rss_mb={rss_mb}",
                    "vars": {"rss_mb": {"k": "i", "v": [200, 2400]}},
                    "state_vars": {
                        "n": {
                            "inflight": {"k": "i", "v": [0, 300]},
                            "busy_pct": {"k": "i", "v": [5, 75]},
                            "dns_avg_ms": {"k": "i", "v": [1, 25]},
                            "dns_timeouts_1m": {"k": "i", "v": [0, 2]},
                        },
                        "f": {
                            "inflight": {"k": "i", "v": [400, 6000]},
                            "busy_pct": {"k": "i", "v": [70, 100]},
                            "dns_avg_ms": {"k": "i", "v": [50, 4000]},
                            "dns_timeouts_1m": {"k": "i", "v": [20, 200]},
                        },
                    },
                },
                "dns_timeout": {
                    "lvl": "WARN",
                    "msg": "resolver timeout resolver={resolver_ip} timeout_ms={timeout_ms} inflight={inflight}",
                    "vars": {
                        "resolver_ip": {"k": "ip", "v": "10.0.0.53/32"},
                        "timeout_ms": {"k": "i", "v": [300, 3000]},
                    },
                    "state_vars": {
                        "n": {"inflight": {"k": "i", "v": [0, 300]}},
                        "f": {"inflight": {"k": "i", "v": [400, 6000]}},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "app_metrics", "per_min": 6, "scope": "per_host"},
                        {"id": "dns_timeout", "per_min": 0.05, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "app_metrics", "per_min": 6, "scope": "per_host"},
                        {"id": "dns_timeout", "per_min": 30, "scope": "per_host"},
                    ]
                },
            },
        },
        "dns_resolver": {
            "svc": "dc-dns",
            "hosts": ["dns-1"],
            "logs": {
                "dns_stats": {
                    "lvl": "INFO",
                    "msg": "dns stats qps={qps} tcp_conn_errors_1m={tcp_conn_errors_1m} avg_resp_ms={avg_resp_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "qps": {"k": "i", "v": [200, 1500]},
                            "tcp_conn_errors_1m": {"k": "i", "v": [0, 20]},
                            "avg_resp_ms": {"k": "i", "v": [1, 15]},
                        },
                        "f": {
                            "qps": {"k": "i", "v": [2000, 12000]},
                            "tcp_conn_errors_1m": {"k": "i", "v": [50, 2500]},
                            "avg_resp_ms": {"k": "i", "v": [20, 2500]},
                        },
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "dns_stats", "per_min": 1, "scope": "per_host"}]},
                "f": {"emit": [{"id": "dns_stats", "per_min": 1, "scope": "per_host"}]},
            },
        },
        "oncall_ops": {
            "svc": "ops",
            "hosts": ["ops-1"],
            "logs": {
                "lb_weight_change": {
                    "lvl": "INFO",
                    "msg": "changed lb backend weight host={backend_host} weight={weight}",
                    "vars": {"backend_host": {"k": "ch", "v": ["fe-1", "fe-2", "fe-3"]}, "weight": {"k": "i", "v": [50, 500]}},
                },
                "shift_capacity": {
                    "lvl": "INFO",
                    "msg": "shifted capacity from worker tier to frontend add_instances={add_instances}",
                    "vars": {"add_instances": {"k": "i", "v": [1, 4]}},
                },
                "strace_sample": {
                    "lvl": "INFO",
                    "msg": "strace pid={pid} observed connect({resolver_ip}:53) blocked_ms={blocked_ms}",
                    "vars": {
                        "pid": {"k": "i", "v": [1000, 45000]},
                        "resolver_ip": {"k": "ip", "v": "10.0.0.53/32"},
                        "blocked_ms": {"k": "i", "v": [200, 8000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "fetch_package_n",
                    "rpm": 800,
                    "emit": ["edge_lb.access_2xx"],
                    "latency_ms": [[15, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "monitor_check_n",
                    "rpm": 6,
                    "emit": ["external_monitor.check_ok"],
                    "latency_ms": [[30, 300]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "fetch_package_ok_f",
                    "rpm": 2400,
                    "emit": ["edge_lb.access_2xx"],
                    "latency_ms": [[60, 500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "fetch_package_fail_f",
                    "rpm": 2400,
                    "emit": ["edge_lb.access_5xx"],
                    "latency_ms": [[600, 10000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "monitor_check_f",
                    "rpm": 6,
                    "emit": ["external_monitor.check_fail"],
                    "latency_ms": [[500, 8000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "dns_resolver_stall_spike_outage"},
    "time": {"total_minutes": 24, "phases": {"n": {"start_min": 0, "end_min": 12}, "f": {"start_min": 12, "end_min": 24}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 12,
                    "rate_multipliers": {
                        "fetch_package_ok_f": 1.0,
                        "fetch_package_fail_f": 1.0,
                        "monitor_check_f": 1.0,
                        "app_frontend.dns_timeout": 1.0,
                    },
                    "latency_multipliers": {
                        "fetch_package_ok_f": {"p50": 1.2, "p95": 1.4},
                        "fetch_package_fail_f": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [{"ref": "external_monitor.page_sent", "count": 1, "hosts": ["mon-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 15,
                    "rate_multipliers": {"fetch_package_ok_f": 0.5, "fetch_package_fail_f": 1.5, "app_frontend.dns_timeout": 2.0},
                    "latency_multipliers": {
                        "fetch_package_ok_f": {"p50": 1.5, "p95": 2.0},
                        "fetch_package_fail_f": {"p50": 1.2, "p95": 1.3},
                    },
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 18,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "oncall_ops.lb_weight_change", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "oncall_ops.shift_capacity", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "oncall_ops.strace_sample", "count": 1, "hosts": ["ops-1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 21,
                    "rate_multipliers": {
                        "fetch_package_ok_f": 0.2,
                        "fetch_package_fail_f": 1.8,
                        "monitor_check_f": 1.0,
                        "app_frontend.dns_timeout": 2.5,
                    },
                    "latency_multipliers": {
                        "fetch_package_ok_f": {"p50": 2.0, "p95": 3.0},
                        "fetch_package_fail_f": {"p50": 1.5, "p95": 1.8},
                    },
                    "one_shots": [],
                },
            ]
        }
    },
}

SEED = 1337
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def dt_to_iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"


def _h_bytes(key: str) -> bytes:
    return hashlib.md5((str(SEED) + "|" + key).encode("utf-8")).digest()


def h01(key: str) -> float:
    b = _h_bytes(key)
    x = int.from_bytes(b[:8], "big", signed=False)
    return (x % (10**15)) / float(10**15)


def h_int(key: str) -> int:
    b = _h_bytes(key)
    return int.from_bytes(b[:8], "big", signed=False)


def hex_of(key: str, length: int) -> str:
    return hashlib.sha1((str(SEED) + "|" + key).encode("utf-8")).hexdigest()[:length]


def norm_ppf(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def choose_from(key: str, choices: List[str]) -> str:
    idx = int(h01(key) * len(choices))
    if idx >= len(choices):
        idx = len(choices) - 1
    return choices[idx]


def pick_int_in_range(key: str, lo: int, hi: int, bias_high: float = 0.0) -> int:
    u = h01(key)
    bias_high = max(0.0, min(0.999, bias_high))
    p = u * (1.0 - bias_high) + bias_high
    return lo + int(p * (hi - lo + 1))


def sample_lognormal_quantile_between_p50_p95(p50: float, p95: float, u: float) -> float:
    if p50 <= 0 or p95 <= 0:
        return max(p50, p95, 1.0)
    if p95 < p50:
        p95 = p50
    q = 0.5 + 0.45 * min(max(u, 0.0), 1.0)
    mu = math.log(p50)
    sigma = 0.0
    if p95 > p50:
        sigma = math.log(p95 / p50) / 1.6448536269514722
    z = norm_ppf(q)
    v = math.exp(mu + sigma * z)
    soft_cap = 3.0 * p95
    if v > soft_cap:
        v = soft_cap
    return v


def schedule_times_within_minute(minute_start: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    spacing = 60.0 / count
    jitter_max = min(0.05, spacing * 0.25)
    out: List[datetime] = []
    for i in range(count):
        base_s = (i + 0.5) * spacing
        u = h01(f"{key}|i{i}")
        jitter = (u - 0.5) * 2.0 * jitter_max
        s = base_s + jitter
        if s < 0:
            s = 0.0
        if s >= 60.0:
            s = 60.0 - 1e-6
        out.append(minute_start + timedelta(seconds=s))
    return out


class CarryAllocator:
    """
    Low-variance integer allocation for expected-per-minute intensities.
    Note: This allocator is used for integer-ish sources; very-low-rate sources are planned separately.
    """

    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected_per_min: float) -> int:
        c = self.carry.get(key, 0.0)
        v = expected_per_min + c
        n = int(v)
        self.carry[key] = v - n
        return n


def split_ref(ref: str) -> Tuple[str, str]:
    comp, log_id = ref.split(".", 1)
    return comp, log_id


def get_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][component_id]["logs"][log_id]


def emit_row(rows: List[Dict[str, Any]], ts: datetime, component_id: str, log_id: str, message_vars: Dict[str, Any], trace_id: str = "") -> None:
    tmpl = get_template(component_id, log_id)
    msg = tmpl["msg"].format(**message_vars)
    comp = SYSTEM["components"][component_id]
    rows.append(
        {
            "timestamp_dt": ts,
            "timestamp": "",
            "level": tmpl["lvl"],
            "message": msg,
            "trace_id": trace_id,
            "service": comp.get("svc", "") or "",
            "host": message_vars.get("__host", "") or "",
        }
    )


def gen_repo_uri(key: str) -> str:
    u = h01(key)
    if u < 0.10:
        return "/health"
    if u < 0.35:
        return "/repodata/index.json"
    if u < 0.50:
        return "/repodata/repomd.xml"
    pkg_n = (h_int(key + "|pkg") % 5000) + 1
    maj = (h_int(key + "|maj") % 5) + 1
    minr = (h_int(key + "|min") % 40)
    pat = (h_int(key + "|pat") % 30)
    return f"/packages/pkg{pkg_n}-{maj}.{minr}.{pat}.rpm"


def gen_access_vars(state: str, log_id: str, minute: int, idx: int, lb_host: str, latency_ms: int) -> Dict[str, Any]:
    tmpl = get_template("edge_lb", log_id)
    vdef = tmpl["vars"]
    key = f"access|{state}|m{minute}|{log_id}|idx{idx}|lb{lb_host}"
    method = "HEAD" if h01(key + "|method") < 0.12 else "GET"
    uri = gen_repo_uri(key + "|uri")

    if log_id == "access_2xx":
        u = h01(key + "|status")
        if u < 0.82:
            status = "200"
        elif u < 0.92:
            status = "304"
        else:
            status = "206"
    else:
        status = "504" if h01(key + "|status") < 0.7 else "503"

    backend_host = choose_from(key + "|backend", vdef["backend_host"]["v"])

    rt_ms = clamp_int(int(latency_ms), vdef["rt_ms"]["v"][0], vdef["rt_ms"]["v"][1])
    upstream_ms = clamp_int(
        max(2, rt_ms - max(1, int(rt_ms * (0.03 + 0.04 * h01(key + "|up_adj"))))),
        vdef["upstream_ms"]["v"][0],
        vdef["upstream_ms"]["v"][1],
    )

    if method == "HEAD" or status == "304":
        bytes_out = 0
    else:
        if log_id == "access_5xx":
            bytes_out = int(h01(key + "|bytes") * 2000)
        else:
            if uri.startswith("/repodata"):
                bytes_out = clamp_int(500 + int(h01(key + "|bytes") * 400000), 0, vdef["bytes"]["v"][1])
            elif uri == "/health":
                bytes_out = clamp_int(200 + int(h01(key + "|bytes") * 5000), 0, vdef["bytes"]["v"][1])
            else:
                bytes_out = clamp_int(50000 + int(h01(key + "|bytes") * 7900000), 0, vdef["bytes"]["v"][1])

    return {"method": method, "uri": uri, "status": status, "bytes": bytes_out, "rt_ms": rt_ms, "backend_host": backend_host, "upstream_ms": upstream_ms}


def gen_monitor_vars(log_id: str, minute: int, idx: int, latency_ms: int) -> Dict[str, Any]:
    key = f"monitor|m{minute}|{log_id}|idx{idx}"
    if log_id == "check_ok":
        url = "https://repo.example.com/health" if h01(key + "|url") < 0.6 else "https://repo.example.com/repodata/index.json"
        status = "204" if url.endswith("/health") and h01(key + "|st") < 0.6 else "200"
        return {"url": url, "status": status, "latency_ms": int(latency_ms)}
    url = "https://repo.example.com/health" if h01(key + "|url") < 0.5 else "https://repo.example.com/repodata/index.json"
    u = h01(key + "|err")
    if u < 0.72:
        err = "timeout"
    elif u < 0.85:
        err = "http_504"
    elif u < 0.95:
        err = "http_503"
    else:
        err = "connection_reset"
    return {"url": url, "error": err, "latency_ms": int(latency_ms)}


def compute_failure_controls(events: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    events_sorted = sorted(events, key=lambda e: e["at_min"])
    per_min: Dict[int, Dict[str, Any]] = {}
    cur_rate_flow: Dict[str, float] = {}
    cur_rate_bg: Dict[str, float] = {}
    cur_lat_flow: Dict[str, Tuple[float, float]] = {}

    idx = 0
    for m in range(SCENARIO["time"]["phases"]["f"]["start_min"], SCENARIO["time"]["phases"]["f"]["end_min"]):
        while idx < len(events_sorted) and events_sorted[idx]["at_min"] == m:
            ev = events_sorted[idx]
            for k, v in ev.get("rate_multipliers", {}).items():
                if "." in k:
                    cur_rate_bg[k] = float(v)
                else:
                    cur_rate_flow[k] = float(v)
            for k, v in ev.get("latency_multipliers", {}).items():
                cur_lat_flow[k] = (float(v.get("p50", 1.0)), float(v.get("p95", 1.0)))
            idx += 1
        per_min[m] = {"rate_flow": dict(cur_rate_flow), "rate_bg": dict(cur_rate_bg), "lat_flow": dict(cur_lat_flow)}
    return per_min


def plan_normal_rare_dns_timeouts(n_start: int, n_end: int) -> Dict[int, Dict[str, int]]:
    """
    Implements app_frontend.beh.n dns_timeout per_min=0.05 scope=per_host in a low-variance deterministic way
    that does not round down to 0 every minute.

    We plan over the whole normal interval and distribute a small number of WARNs across hosts and minutes.
    """
    hosts = SYSTEM["components"]["app_frontend"]["hosts"]
    D = max(0, n_end - n_start)
    per_min = float(next(e["per_min"] for e in SYSTEM["components"]["app_frontend"]["beh"]["n"]["emit"] if e["id"] == "dns_timeout"))
    # Expected total per host over the whole normal interval:
    # E_host = per_min * D (here 0.05*12 = 0.6)
    E_host = per_min * D
    base = int(math.floor(E_host))
    frac = max(0.0, min(1.0, E_host - base))

    plan: Dict[int, Dict[str, int]] = {m: {h: 0 for h in hosts} for m in range(n_start, n_end)}
    if D <= 0:
        return plan

    # Deterministic "fractional allocation" across hosts: choose which hosts get the +1 extra using a low-discrepancy threshold.
    # Sort hosts by deterministic hash to avoid always picking the first host.
    ranked = sorted(hosts, key=lambda h: h_int(f"rank|n|app_frontend.dns_timeout|{h}"))
    N = len(ranked)

    extras_hosts: List[str] = []
    if frac > 0 and N > 0:
        for r, h in enumerate(ranked):
            u = (r + 0.5) / N  # evenly spread in (0,1)
            if u < frac:
                extras_hosts.append(h)

    # Total count per host over interval: base + (host in extras_hosts)
    for h in hosts:
        total = base + (1 if h in extras_hosts else 0)
        if total <= 0:
            continue
        # Place these total events at deterministic minute(s) within normal window.
        for k in range(total):
            off = h01(f"place|n|app_frontend.dns_timeout|{h}|k{k}")
            pos = (k + off) / total  # in [0,1)
            m = n_start + int(pos * D)
            if m >= n_end:
                m = n_end - 1
            plan[m][h] += 1
    return plan


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    total_minutes = SCENARIO["time"]["total_minutes"]
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]

    failure_events = SCENARIO["phases"]["f"]["events"]
    failure_controls_by_min = compute_failure_controls(failure_events)
    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for ev in failure_events:
        events_by_min.setdefault(ev["at_min"], []).append(ev)

    flows_by_state: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for req in SYSTEM["flows"][st]["req"]:
            flows_by_state[st][req["id"]] = req

    # Normal-phase low-rate background: plan WARNs over the normal interval so they actually occur.
    normal_dns_warn_plan = plan_normal_rare_dns_timeouts(n_start, n_end)

    allocator = CarryAllocator()
    rows: List[Dict[str, Any]] = []
    global_flow_instance = 0

    for minute in range(total_minutes):
        minute_start = BASE_TIME + timedelta(minutes=minute)
        state = "n" if minute < n_end else "f"

        rate_mult_flow: Dict[str, float] = {}
        rate_mult_bg: Dict[str, float] = {}
        lat_mult_flow: Dict[str, Tuple[float, float]] = {}
        if state == "f":
            ctl = failure_controls_by_min.get(minute, {"rate_flow": {}, "rate_bg": {}, "lat_flow": {}})
            rate_mult_flow = ctl["rate_flow"]
            rate_mult_bg = ctl["rate_bg"]
            lat_mult_flow = ctl["lat_flow"]

        # One-shots at failure event times.
        if state == "f" and minute in events_by_min:
            for ev in sorted(events_by_min[minute], key=lambda x: x.get("order", 0)):
                for os in ev.get("one_shots", []):
                    comp_id, log_id = split_ref(os["ref"])
                    count = int(os["count"])
                    hosts_allowed = os.get("hosts") or SYSTEM["components"][comp_id]["hosts"]
                    for i in range(count):
                        jitter_ms = int(50 + h01(f"oneshot|{minute}|{os['ref']}|i{i}") * 1800)
                        ts = minute_start + timedelta(milliseconds=jitter_ms)
                        host = hosts_allowed[i % len(hosts_allowed)] if hosts_allowed else ""
                        tmpl = get_template(comp_id, log_id)
                        bindings: Dict[str, Any] = {}
                        for var, spec in tmpl.get("vars", {}).items():
                            k = spec["k"]
                            if k == "ch":
                                bindings[var] = choose_from(f"oneshot|{minute}|{os['ref']}|{var}|i{i}", spec["v"])
                            elif k == "i":
                                lo, hi = spec["v"]
                                bindings[var] = pick_int_in_range(f"oneshot|{minute}|{os['ref']}|{var}|i{i}", lo, hi, bias_high=0.2)
                            elif k == "hex":
                                bindings[var] = hex_of(f"oneshot|{minute}|{os['ref']}|{var}", int(spec["v"]))
                            elif k == "ip":
                                bindings[var] = spec["v"].split("/")[0]
                            else:
                                bindings[var] = str(spec.get("v", ""))
                        bindings["__host"] = host
                        emit_row(rows, ts, comp_id, log_id, bindings, trace_id="")

        frontend_hosts = SYSTEM["components"]["app_frontend"]["hosts"]
        lb_hosts = SYSTEM["components"]["edge_lb"]["hosts"]

        # Severity knob for biased metric sampling.
        if state == "n":
            sev = 0.08
        else:
            dns_timeout_mult = float(rate_mult_bg.get("app_frontend.dns_timeout", 1.0))
            fail_lat_p95_mult = lat_mult_flow.get("fetch_package_fail_f", (1.0, 1.0))[1]
            sev = min(0.98, 0.3 + 0.15 * dns_timeout_mult + 0.05 * fail_lat_p95_mult)

        # Snapshot (per-host) values reused for background logs in this minute.
        frontend_snapshot: Dict[str, Dict[str, int]] = {}
        for h in frontend_hosts:
            mv = SYSTEM["components"]["app_frontend"]["logs"]["app_metrics"]["state_vars"][state]
            infl = pick_int_in_range(f"fe_snap|{state}|m{minute}|{h}|inflight", mv["inflight"]["v"][0], mv["inflight"]["v"][1], bias_high=sev)
            busy = pick_int_in_range(f"fe_snap|{state}|m{minute}|{h}|busy", mv["busy_pct"]["v"][0], mv["busy_pct"]["v"][1], bias_high=sev)
            dns_avg = pick_int_in_range(f"fe_snap|{state}|m{minute}|{h}|dnsavg", mv["dns_avg_ms"]["v"][0], mv["dns_avg_ms"]["v"][1], bias_high=sev)
            rss = pick_int_in_range(f"fe_snap|{state}|m{minute}|{h}|rss", 200, 2400, bias_high=0.25 + 0.5 * sev)
            frontend_snapshot[h] = {"inflight": infl, "busy_pct": busy, "dns_avg_ms": dns_avg, "rss_mb": rss}

        # app_frontend.dns_timeout background WARNs
        dns_warn_count_by_host: Dict[str, int] = {}
        if state == "n":
            # Use the precomputed low-rate plan for the normal interval.
            per_host_counts = normal_dns_warn_plan.get(minute, {h: 0 for h in frontend_hosts})
            for h in frontend_hosts:
                n_warn = int(per_host_counts.get(h, 0))
                dns_warn_count_by_host[h] = n_warn
                key = f"bg|n|app_frontend.dns_timeout|{h}|m{minute}"
                times = schedule_times_within_minute(minute_start, n_warn, key + "|ts")
                for i, ts in enumerate(times):
                    u = h01(f"{key}|timeout|i{i}")
                    timeout_ms = int(300 + (3000 - 300) * (u * (1.0 - 0.45 * sev) + 0.45 * sev))
                    timeout_ms = clamp_int(timeout_ms, 300, 3000)
                    bindings = {"resolver_ip": "10.0.0.53", "timeout_ms": timeout_ms, "inflight": frontend_snapshot[h]["inflight"], "__host": h}
                    emit_row(rows, ts, "app_frontend", "dns_timeout", bindings, trace_id="")
        else:
            base_rate = float(next(e["per_min"] for e in SYSTEM["components"]["app_frontend"]["beh"]["f"]["emit"] if e["id"] == "dns_timeout"))
            dns_timeout_mult = float(rate_mult_bg.get("app_frontend.dns_timeout", 1.0))
            dns_warn_expected_per_host = base_rate * dns_timeout_mult
            for h in frontend_hosts:
                key = f"bg|f|app_frontend.dns_timeout|{h}|m{minute}"
                # High-rate in failure: allocator is stable and close to expected.
                n_warn = allocator.alloc(key, dns_warn_expected_per_host)
                dns_warn_count_by_host[h] = n_warn
                times = schedule_times_within_minute(minute_start, n_warn, key + "|ts")
                for i, ts in enumerate(times):
                    u = h01(f"{key}|timeout|i{i}")
                    timeout_ms = int(300 + (3000 - 300) * (u * (1.0 - 0.45 * sev) + 0.45 * sev))
                    timeout_ms = clamp_int(timeout_ms, 300, 3000)
                    bindings = {"resolver_ip": "10.0.0.53", "timeout_ms": timeout_ms, "inflight": frontend_snapshot[h]["inflight"], "__host": h}
                    emit_row(rows, ts, "app_frontend", "dns_timeout", bindings, trace_id="")

        # Flow emissions.
        lb_5xx_by_host = {h: 0 for h in lb_hosts}
        lb_total_by_host = {h: 0 for h in lb_hosts}

        for flow_id, flow in flows_by_state[state].items():
            base_rpm = float(flow["rpm"])
            eff_rpm = base_rpm if state == "n" else base_rpm * float(rate_mult_flow.get(flow_id, 1.0))
            n_req = allocator.alloc(f"flow|{state}|{flow_id}|m{minute}", eff_rpm)
            start_times = schedule_times_within_minute(minute_start, n_req, f"flow|{state}|{flow_id}|m{minute}|starts")

            (base_p50, base_p95) = flow["latency_ms"][0]
            (m50, m95) = (1.0, 1.0) if state == "n" else lat_mult_flow.get(flow_id, (1.0, 1.0))
            eff_p50 = float(base_p50) * float(m50)
            eff_p95 = float(base_p95) * float(m95)

            for i, st_dt in enumerate(start_times):
                u = h01(f"lat|{state}|{flow_id}|m{minute}|i{i}")
                latency = sample_lognormal_quantile_between_p50_p95(eff_p50, eff_p95, u)
                latency_ms = int(max(1, round(latency)))
                emit_ref = flow["emit"][0]
                comp_id, log_id = split_ref(emit_ref)

                if comp_id == "edge_lb":
                    lb_host = lb_hosts[(global_flow_instance + i) % len(lb_hosts)]
                    bindings = gen_access_vars(state, log_id, minute, global_flow_instance + i, lb_host, latency_ms)
                    bindings["__host"] = lb_host
                    if log_id == "access_5xx":
                        lb_5xx_by_host[lb_host] += 1
                    lb_total_by_host[lb_host] += 1
                    ts = st_dt + timedelta(milliseconds=latency_ms)
                    emit_row(rows, ts, comp_id, log_id, bindings, trace_id="")
                elif comp_id == "external_monitor":
                    host = SYSTEM["components"]["external_monitor"]["hosts"][0] if SYSTEM["components"]["external_monitor"]["hosts"] else ""
                    ts = st_dt + timedelta(milliseconds=latency_ms)
                    bindings = gen_monitor_vars(log_id, minute, global_flow_instance + i, latency_ms)
                    bindings["__host"] = host
                    emit_row(rows, ts, comp_id, log_id, bindings, trace_id="")

            global_flow_instance += n_req

        # edge_lb.lb_stats background (per_host, per_min=1)
        for lb_h in lb_hosts:
            ts = minute_start + timedelta(seconds=59.2 + 0.6 * h01(f"lb_stats|m{minute}|{lb_h}"))
            sv = SYSTEM["components"]["edge_lb"]["logs"]["lb_stats"]["state_vars"][state]
            five_xx = clamp_int(lb_5xx_by_host[lb_h], sv["five_xx_1m"]["v"][0], sv["five_xx_1m"]["v"][1])

            total = max(1, lb_total_by_host[lb_h])
            bq_lo, bq_hi = sv["backend_q"]["v"]
            ac_lo, ac_hi = sv["active_conns"]["v"]
            if state == "n":
                backend_q = clamp_int(int(0.05 * total + h01(f"lbq|n|m{minute}|{lb_h}") * 10), bq_lo, bq_hi)
                active_conns = clamp_int(int(60 + 0.6 * total + h01(f"ac|n|m{minute}|{lb_h}") * 90), ac_lo, ac_hi)
            else:
                backend_q = clamp_int(int(200 + 0.55 * five_xx + 0.12 * total), bq_lo, bq_hi)
                active_conns = clamp_int(int(900 + 0.9 * total + 0.35 * backend_q + 0.15 * five_xx), ac_lo, ac_hi)

            bindings = {"active_conns": active_conns, "backend_q": backend_q, "five_xx_1m": five_xx, "__host": lb_h}
            emit_row(rows, ts, "edge_lb", "lb_stats", bindings, trace_id="")

        # app_frontend.app_metrics background (per_host, per_min=6)
        for fe_h in frontend_hosts:
            n_metrics = 6
            times = schedule_times_within_minute(minute_start, n_metrics, f"app_metrics|{state}|m{minute}|{fe_h}")
            mv = SYSTEM["components"]["app_frontend"]["logs"]["app_metrics"]["state_vars"][state]
            # Keep metrics "dns_timeouts_1m" coherent with modeled WARN emission per host per minute (rolling 1-minute count).
            dns_timeouts_1m = clamp_int(dns_warn_count_by_host.get(fe_h, 0), mv["dns_timeouts_1m"]["v"][0], mv["dns_timeouts_1m"]["v"][1])
            for ts in times:
                bindings = {
                    "inflight": frontend_snapshot[fe_h]["inflight"],
                    "busy_pct": frontend_snapshot[fe_h]["busy_pct"],
                    "dns_avg_ms": frontend_snapshot[fe_h]["dns_avg_ms"],
                    "dns_timeouts_1m": dns_timeouts_1m,
                    "rss_mb": frontend_snapshot[fe_h]["rss_mb"],
                    "__host": fe_h,
                }
                emit_row(rows, ts, "app_frontend", "app_metrics", bindings, trace_id="")

        # dns_resolver.dns_stats background (per_host, per_min=1)
        dns_h = SYSTEM["components"]["dns_resolver"]["hosts"][0] if SYSTEM["components"]["dns_resolver"]["hosts"] else ""
        ts = minute_start + timedelta(seconds=30.0 + 0.8 * h01(f"dns_stats|m{minute}"))
        sv = SYSTEM["components"]["dns_resolver"]["logs"]["dns_stats"]["state_vars"][state]
        if state == "n":
            qps = pick_int_in_range(f"dnsq|n|m{minute}", sv["qps"]["v"][0], sv["qps"]["v"][1], bias_high=0.15)
            tcp_err = pick_int_in_range(f"dnse|n|m{minute}", sv["tcp_conn_errors_1m"]["v"][0], sv["tcp_conn_errors_1m"]["v"][1], bias_high=0.08)
            avg_resp = pick_int_in_range(f"dnsr|n|m{minute}", sv["avg_resp_ms"]["v"][0], sv["avg_resp_ms"]["v"][1], bias_high=0.10)
        else:
            ok_mult = rate_mult_flow.get("fetch_package_ok_f", 1.0)
            fail_mult = rate_mult_flow.get("fetch_package_fail_f", 1.0)
            base_total = flows_by_state["f"]["fetch_package_ok_f"]["rpm"] * ok_mult + flows_by_state["f"]["fetch_package_fail_f"]["rpm"] * fail_mult
            total_req_min = int(base_total)
            total_warns = sum(dns_warn_count_by_host.values())
            qps = clamp_int(int(2000 + 0.8 * total_req_min + 200 * h01(f"dnsq|f|m{minute}")), sv["qps"]["v"][0], sv["qps"]["v"][1])
            tcp_err = clamp_int(int(50 + 5.0 * total_warns + 50 * h01(f"dnse|f|m{minute}")), sv["tcp_conn_errors_1m"]["v"][0], sv["tcp_conn_errors_1m"]["v"][1])
            avg_resp = clamp_int(
                int(20 + 0.55 * max(fs["dns_avg_ms"] for fs in frontend_snapshot.values()) + 50 * h01(f"dnsr|f|m{minute}")),
                sv["avg_resp_ms"]["v"][0],
                sv["avg_resp_ms"]["v"][1],
            )

        bindings = {"qps": qps, "tcp_conn_errors_1m": tcp_err, "avg_resp_ms": avg_resp, "__host": dns_h}
        emit_row(rows, ts, "dns_resolver", "dns_stats", bindings, trace_id="")

    df = pd.DataFrame(rows)
    df["timestamp"] = df["timestamp_dt"].apply(dt_to_iso_z)
    df = df.drop(columns=["timestamp_dt"])
    # Sort primarily by timestamp; additional keys keep deterministic row order for identical timestamps.
    df = df.sort_values(by=["timestamp", "service", "host", "level", "message"], kind="mergesort").reset_index(drop=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
