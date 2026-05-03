import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


# Ensure verifier-visible deterministic seeding (even though this simulator is hash-deterministic).
random.seed(0)


# -----------------------------
# Embedded normalized model data
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "edge_waf_proxy_outage"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "edge_frontend": {
            "svc": "edge-frontend",
            "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"],
            "logs": {
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "request {method} {host}{uri} status=200 bytes={bytes} rt_ms={rt_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["example.com", "api.example.com"]},
                        "uri": {"k": "str", "v": "url_path_and_query"},
                        "bytes": {"k": "i", "v": [200, 50000]},
                        "rt_ms": {"k": "i", "v": [5, 250]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "access_ok_slow": {
                    "lvl": "INFO",
                    "msg": "request {method} {host}{uri} status=200 bytes={bytes} rt_ms={rt_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["example.com", "api.example.com"]},
                        "uri": {"k": "str", "v": "url_path_and_query"},
                        "bytes": {"k": "i", "v": [200, 50000]},
                        "rt_ms": {"k": "i", "v": [300, 3000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "upstream_timeout": {
                    "lvl": "WARN",
                    "msg": "upstream timeout talking to http_worker timeout_ms={timeout_ms} host={host}{uri} req_id={req_id}",
                    "vars": {
                        "timeout_ms": {"k": "i", "v": [800, 1200]},
                        "host": {"k": "ch", "v": ["example.com", "api.example.com"]},
                        "uri": {"k": "str", "v": "url_path_and_query"},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "access_502": {
                    "lvl": "INFO",
                    "msg": "request {method} {host}{uri} status=502 bytes={bytes} rt_ms={rt_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["example.com", "api.example.com"]},
                        "uri": {"k": "str", "v": "url_path_and_query"},
                        "bytes": {"k": "i", "v": [500, 2500]},
                        "rt_ms": {"k": "i", "v": [800, 2000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "http_worker": {
            "svc": "http-worker",
            "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "begin rid={rid} {method} {host}{uri}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "host": {"k": "ch", "v": ["example.com", "api.example.com"]},
                        "uri": {"k": "str", "v": "url_path_and_query"},
                    },
                },
                "req_done": {
                    "lvl": "INFO",
                    "msg": "complete rid={rid} status=200 total_ms={total_ms} waf_ms={waf_ms}",
                    "vars": {"rid": {"k": "hex", "v": 16}},
                    "state_vars": {
                        "n": {
                            "total_ms": {"k": "i", "v": [15, 200]},
                            "waf_ms": {"k": "i", "v": [1, 40]},
                        },
                        "f": {
                            "total_ms": {"k": "i", "v": [300, 2500]},
                            "waf_ms": {"k": "i", "v": [250, 2200]},
                        },
                    },
                },
                "req_done_nowaf": {
                    "lvl": "INFO",
                    "msg": "complete rid={rid} status=200 total_ms={total_ms} waf_ms={waf_ms} waf_enabled=false",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "total_ms": {"k": "i", "v": [12, 220]},
                        "waf_ms": {"k": "i", "v": [0, 3]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "waf_engine": {
            "svc": "lua-waf",
            "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"],
            "logs": {
                "ruleset_applied": {
                    "lvl": "INFO",
                    "msg": "applied managed_ruleset={ruleset_ver} mode=simulate regex_guard=off",
                    "vars": {"ruleset_ver": {"k": "ch", "v": ["2019-07-02.1"]}},
                },
                "regex_backtrack_warn": {
                    "lvl": "WARN",
                    "msg": "slow regex rule_id={rule_id} eval_ms={eval_ms} input_len={input_len} rid={rid}",
                    "vars": {
                        "rule_id": {"k": "ch", "v": ["xss_simulate_1001"]},
                        "eval_ms": {"k": "i", "v": [250, 2200]},
                        "input_len": {"k": "i", "v": [50, 4096]},
                        "rid": {"k": "hex", "v": 16},
                    },
                },
                "waf_status": {
                    "lvl": "INFO",
                    "msg": "waf status enabled=false ruleset={ruleset_ver}",
                    "vars": {"ruleset_ver": {"k": "ch", "v": ["2019-07-02.1"]}},
                },
                "waf_disabled_marker": {
                    "lvl": "CRITICAL",
                    "msg": "waf disabled globally by global_terminate change_id={change_id}",
                    "vars": {"change_id": {"k": "hex", "v": 12}},
                },
                "waf_stats": {
                    "lvl": "INFO",
                    "msg": "waf stats host={host} evals_per_s={eps} p95_eval_ms={p95_ms}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "eps": {"k": "f", "v": [0.2, 2.0]},
                        "p95_ms": {"k": "i", "v": [2, 25]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "waf_stats", "per_min": 0.2, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "regex_backtrack_warn", "per_min": 2.0, "scope": "per_host"},
                        {"id": "waf_status", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "quicksilver_kv": {
            "svc": "quicksilver",
            "hosts": ["qs-ctl-01"],
            "logs": {
                "publish_ruleset": {
                    "lvl": "INFO",
                    "msg": "publish key=/waf/managed_ruleset version={version} scope=global p99_ms={p99_ms}",
                    "vars": {
                        "version": {"k": "i", "v": [1200, 1400]},
                        "p99_ms": {"k": "i", "v": [1500, 4000]},
                    },
                },
                "publish_waf_disable": {
                    "lvl": "INFO",
                    "msg": "publish key=/waf/enabled value=false version={version} scope=global p99_ms={p99_ms}",
                    "vars": {
                        "version": {"k": "i", "v": [1401, 1600]},
                        "p99_ms": {"k": "i", "v": [1500, 4000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "synth_monitor": {
            "svc": "synth",
            "hosts": ["mon-01"],
            "logs": {
                "check_ok": {
                    "lvl": "INFO",
                    "msg": "check {check_id} ok status=200 latency_ms={latency_ms}",
                    "vars": {"check_id": {"k": "ch", "v": ["waf-basic"]}, "latency_ms": {"k": "i", "v": [50, 300]}},
                },
                "check_fail": {
                    "lvl": "ERROR",
                    "msg": "check {check_id} failed status=502 latency_ms={latency_ms}",
                    "vars": {"check_id": {"k": "ch", "v": ["waf-basic"]}, "latency_ms": {"k": "i", "v": [900, 2000]}},
                },
                "page_sent": {
                    "lvl": "WARN",
                    "msg": "pagerduty page sent incident={incident_key} reason={reason}",
                    "vars": {
                        "incident_key": {"k": "ch", "v": ["pd-waf-1"]},
                        "reason": {"k": "ch", "v": ["synthetic_failures_spike"]},
                    },
                },
                "pagerduty_incident_opened": {
                    "lvl": "CRITICAL",
                    "msg": "incident opened incident={incident_key} severity=P0",
                    "vars": {"incident_key": {"k": "ch", "v": ["pd-waf-1"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "edge_metrics_agent": {
            "svc": "metrics-agent",
            "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"],
            "logs": {
                "cpu_sample_normal": {
                    "lvl": "INFO",
                    "msg": "cpu host={host} cpu_pct={cpu_pct} runq={runq}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "cpu_pct": {"k": "f", "v": [8.0, 45.0]},
                        "runq": {"k": "i", "v": [0, 6]},
                    },
                },
                "cpu_sample_high": {
                    "lvl": "INFO",
                    "msg": "cpu host={host} cpu_pct={cpu_pct} runq={runq}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "cpu_pct": {"k": "f", "v": [88.0, 100.0]},
                        "runq": {"k": "i", "v": [20, 120]},
                    },
                },
                "cpu_sample_recovered": {
                    "lvl": "INFO",
                    "msg": "cpu host={host} cpu_pct={cpu_pct} runq={runq}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "cpu_pct": {"k": "f", "v": [15.0, 55.0]},
                        "runq": {"k": "i", "v": [1, 12]},
                    },
                },
                "traffic_sample_normal": {
                    "lvl": "INFO",
                    "msg": "edge_traffic host={host} rps={rps} err_5xx_pct={err_pct}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "rps": {"k": "f", "v": [1.0, 3.5]},
                        "err_pct": {"k": "f", "v": [0.0, 0.5]},
                    },
                },
                "traffic_sample_impact": {
                    "lvl": "INFO",
                    "msg": "edge_traffic host={host} rps={rps} err_5xx_pct={err_pct}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "rps": {"k": "f", "v": [0.5, 2.5]},
                        "err_pct": {"k": "f", "v": [50.0, 95.0]},
                    },
                },
                "traffic_sample_recovered": {
                    "lvl": "INFO",
                    "msg": "edge_traffic host={host} rps={rps} err_5xx_pct={err_pct}",
                    "vars": {
                        "host": {"k": "ch", "v": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                        "rps": {"k": "f", "v": [0.8, 3.0]},
                        "err_pct": {"k": "f", "v": [0.0, 3.0]},
                    },
                },
                "traffic_drop_alert": {
                    "lvl": "WARN",
                    "msg": "global traffic drop detected drop_pct={drop_pct} window_min={window_min}",
                    "vars": {"drop_pct": {"k": "i", "v": [60, 90]}, "window_min": {"k": "i", "v": [3, 10]}},
                },
                "perf_top_snapshot": {
                    "lvl": "WARN",
                    "msg": "perf snapshot top_func={func} cpu_pct={cpu_pct}",
                    "vars": {"func": {"k": "ch", "v": ["pcre_exec", "lua_pcall"]}, "cpu_pct": {"k": "f", "v": [70.0, 99.0]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "cpu_sample_normal", "per_min": 1.0, "scope": "per_host"},
                        {"id": "traffic_sample_normal", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "cpu_sample_high", "per_min": 1.0, "scope": "per_host"},
                        {"id": "cpu_sample_recovered", "per_min": 1.0, "scope": "per_host"},
                        {"id": "traffic_sample_impact", "per_min": 1.0, "scope": "per_host"},
                        {"id": "traffic_sample_recovered", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        "control_panel": {
            "svc": "control-panel",
            "hosts": ["cp-01"],
            "logs": {
                "auth_failed": {
                    "lvl": "ERROR",
                    "msg": "auth failed user={user} reason={reason}",
                    "vars": {
                        "user": {"k": "ch", "v": ["sre_oncall", "perf_eng"]},
                        "reason": {"k": "ch", "v": ["access_timeout", "edge_unreachable", "expired_mfa"]},
                    },
                },
                "bypass_used": {
                    "lvl": "WARN",
                    "msg": "breakglass used user={user} method={method}",
                    "vars": {"user": {"k": "ch", "v": ["sre_oncall", "perf_eng"]}, "method": {"k": "ch", "v": ["bypass_token"]}},
                },
                "global_terminate": {
                    "lvl": "CRITICAL",
                    "msg": "global terminate executed component=waf action=disable change_id={change_id}",
                    "vars": {"change_id": {"k": "hex", "v": 12}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "user_http_ok",
                    "rpm": 300.0,
                    "emit": ["http_worker.req_start", "http_worker.req_done", "edge_frontend.access_ok"],
                    "latency_ms": [[0, 3], [15, 220], [15, 230]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "synthetic_waf_check_ok",
                    "rpm": 6.0,
                    "emit": ["synth_monitor.check_ok"],
                    "latency_ms": [[80, 400]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "user_http_502",
                    "rpm": 220.0,
                    "emit": ["edge_frontend.upstream_timeout", "edge_frontend.access_502"],
                    "latency_ms": [[900, 1200], [905, 2005]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "user_http_ok_degraded",
                    "rpm": 30.0,
                    "emit": ["http_worker.req_start", "http_worker.req_done", "edge_frontend.access_ok_slow"],
                    "latency_ms": [[0, 8], [400, 2500], [420, 2600]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "user_http_ok_nowaf",
                    "rpm": 180.0,
                    "emit": ["http_worker.req_start", "http_worker.req_done_nowaf", "edge_frontend.access_ok"],
                    "latency_ms": [[0, 4], [12, 200], [15, 220]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "synthetic_waf_check_fail",
                    "rpm": 6.0,
                    "emit": ["synth_monitor.check_fail"],
                    "latency_ms": [[900, 2000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "synthetic_waf_check_ok_f",
                    "rpm": 6.0,
                    "emit": ["synth_monitor.check_ok"],
                    "latency_ms": [[80, 450]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "waf_rule_regex_cpu_exhaustion_global"},
    "time": {
        "total_minutes": 60,
        "phases": {"n": {"start_min": 0, "end_min": 30}, "f": {"start_min": 30, "end_min": 60}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 30,
                    "rate_multipliers": {
                        "user_http_ok_nowaf": 0.0,
                        "synthetic_waf_check_ok_f": 0.0,
                        "waf_engine.waf_status": 0.0,
                        "edge_metrics_agent.cpu_sample_recovered": 0.0,
                        "edge_metrics_agent.traffic_sample_recovered": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "quicksilver_kv.publish_ruleset", "count": 1, "hosts": ["qs-ctl-01"]},
                        {"ref": "waf_engine.ruleset_applied", "count": 3, "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 33,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "synth_monitor.page_sent", "count": 1, "hosts": ["mon-01"]},
                        {"ref": "synth_monitor.pagerduty_incident_opened", "count": 1, "hosts": ["mon-01"]},
                        {"ref": "control_panel.auth_failed", "count": 2, "hosts": ["cp-01"]},
                    ],
                },
                {
                    "order": 3,
                    "at_min": 48,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "edge_metrics_agent.traffic_drop_alert", "count": 1, "hosts": ["edge-lhr-01"]},
                        {"ref": "edge_metrics_agent.perf_top_snapshot", "count": 1, "hosts": ["edge-lhr-02"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 55,
                    "rate_multipliers": {
                        "user_http_502": 0.01,
                        "user_http_ok_degraded": 0.0,
                        "user_http_ok_nowaf": 1.0,
                        "synthetic_waf_check_fail": 0.0,
                        "synthetic_waf_check_ok_f": 1.0,
                        "edge_metrics_agent.cpu_sample_high": 0.0,
                        "edge_metrics_agent.traffic_sample_impact": 0.0,
                        "edge_metrics_agent.cpu_sample_recovered": 1.0,
                        "edge_metrics_agent.traffic_sample_recovered": 1.0,
                        "waf_engine.regex_backtrack_warn": 0.0,
                        "waf_engine.waf_status": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "control_panel.bypass_used", "count": 1, "hosts": ["cp-01"]},
                        {"ref": "control_panel.global_terminate", "count": 1, "hosts": ["cp-01"]},
                        {"ref": "quicksilver_kv.publish_waf_disable", "count": 1, "hosts": ["qs-ctl-01"]},
                        {"ref": "waf_engine.waf_disabled_marker", "count": 3, "hosts": ["edge-lhr-01", "edge-lhr-02", "edge-lhr-03"]},
                    ],
                },
            ]
        }
    },
}


# -----------------------------
# Helpers: deterministic hashing
# -----------------------------
def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def hash_uniform01(key: str) -> float:
    h = _md5_hex(key)
    x = int(h[:16], 16)
    return (x % (10**12)) / float(10**12)


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    u = hash_uniform01("round:" + key)
    return n + (1 if u < frac else 0)


def det_choice(seq: List[Any], key: str) -> Any:
    if not seq:
        return None
    u = hash_uniform01("choice:" + key)
    idx = int(u * len(seq))
    if idx == len(seq):
        idx -= 1
    return seq[idx]


def det_int(a: int, b: int, key: str) -> int:
    if b < a:
        a, b = b, a
    u = hash_uniform01("int:" + key)
    return a + int(u * (b - a + 1))


def det_float(a: float, b: float, key: str) -> float:
    if b < a:
        a, b = b, a
    u = hash_uniform01("float:" + key)
    return a + u * (b - a)


def det_hex(n: int, key: str) -> str:
    return _md5_hex("hex:" + key)[:n]


def clamp_i(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def get_int_range(component_id: str, log_id: str, state: str, field: str) -> Optional[Tuple[int, int]]:
    comp = SYSTEM["components"][component_id]
    tmpl = comp["logs"][log_id]
    if "state_vars" in tmpl and state in tmpl["state_vars"] and field in tmpl["state_vars"][state]:
        dom = tmpl["state_vars"][state][field]
        if dom.get("k") == "i":
            a, b = int(dom["v"][0]), int(dom["v"][1])
            return (a, b)
    if field in tmpl.get("vars", {}):
        dom = tmpl["vars"][field]
        if dom.get("k") == "i":
            a, b = int(dom["v"][0]), int(dom["v"][1])
            return (a, b)
    return None


def gen_uri(key: str) -> str:
    paths = ["/", "/login", "/api/v1/items", "/search", "/products", "/assets/app.js", "/checkout", "/api/v1/users/me"]
    path = det_choice(paths, "uri:path:" + key)
    u = hash_uniform01("uri:q:" + key)
    if u < 0.55:
        return path
    qk = det_choice(["q", "id", "page", "sort"], "uri:qk:" + key)
    if qk == "q":
        qv = det_choice(["test", "abc", "widget", "xss"], "uri:qv:" + key)
    elif qk == "id":
        qv = str(det_int(1, 9999, "uri:id:" + key))
    elif qk == "page":
        qv = str(det_int(1, 20, "uri:page:" + key))
    else:
        qv = det_choice(["asc", "desc"], "uri:sort:" + key)
    return f"{path}?{qk}={qv}"


# Inverse normal CDF approximation (Acklam)
def inv_norm_cdf(p: float) -> float:
    if p <= 0.0:
        return float("-inf")
    if p >= 1.0:
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
    phigh = 1 - plow
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
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (
        ((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1
    )


def sample_lognormal_from_p50_p95(p50: float, p95: float, key: str, cap: Optional[float] = None) -> float:
    if p95 < 0:
        p95 = 0
    if p50 <= 0:
        # For zero-ish medians, use a stable uniform up to p95 as a simple positive sampler.
        u = hash_uniform01("ln0:" + key)
        val = u * max(0.0, p95)
        if cap is not None:
            val = min(val, cap)
        return max(0.0, val)

    if p95 <= 0:
        return 0.0

    mu = math.log(p50)
    ratio = max(1.000001, p95 / p50)
    sigma = math.log(ratio) / 1.6448536269514722  # ~inv_norm_cdf(0.95)
    u = hash_uniform01("lnq:" + key)
    q = 0.45 + 0.20 * u  # narrow band around median for low-variance
    z = inv_norm_cdf(q)
    val = math.exp(mu + sigma * z)

    soft_cap = 3.0 * p95
    val = min(val, soft_cap)
    if cap is not None:
        val = min(val, cap)
    return max(0.0, val)


def format_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def ms_to_dt(base: datetime, ms: int) -> datetime:
    return base + timedelta(milliseconds=ms)


def schedule_times(start_ms: int, end_ms: int, n: int, key: str) -> List[int]:
    if n <= 0:
        return []
    dur = max(1, end_ms - start_ms)
    step = dur / float(n)
    max_j = int(min(200, max(1.0, step * 0.12)))
    out = []
    for i in range(n):
        center = start_ms + int((i + 0.5) * step)
        u = hash_uniform01(f"jit:{key}:{i}")
        jitter = int((u - 0.5) * 2 * max_j)
        t = center + jitter
        if t < start_ms:
            t = start_ms
        if t >= end_ms:
            t = end_ms - 1
        out.append(t)
    return out


def format_float_by_name(x: float, name: str) -> str:
    if name in {"cpu_pct", "err_pct"}:
        return f"{x:.1f}"
    if name in {"eps", "rps"}:
        return f"{x:.2f}"
    return f"{x:.2f}"


def sample_var(domain: Dict[str, Any], key: str, varname: str) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "ch":
        return det_choice(list(v), f"{key}:{varname}")
    if k == "i":
        a, b = int(v[0]), int(v[1])
        return det_int(a, b, f"{key}:{varname}")
    if k == "f":
        a, b = float(v[0]), float(v[1])
        x = det_float(a, b, f"{key}:{varname}")
        return format_float_by_name(x, varname)
    if k == "hex":
        return det_hex(int(v), f"{key}:{varname}")
    if k == "str":
        return gen_uri(f"{key}:{varname}")
    if k == "uuid":
        h = _md5_hex("uuid:" + key + ":" + varname)
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "ip":
        h = _md5_hex("ip:" + key + ":" + varname)
        a = 10 + (int(h[0:2], 16) % 10)
        b = int(h[2:4], 16) % 256
        c = int(h[4:6], 16) % 256
        d = int(h[6:8], 16) % 256
        return f"{a}.{b}.{c}.{d}"
    return ""


def render_log(component_id: str, log_id: str, state: str, host_col: str, overrides: Dict[str, Any], key: str) -> Tuple[str, str, str, str]:
    comp = SYSTEM["components"][component_id]
    tmpl = comp["logs"][log_id]
    msg = tmpl["msg"]
    lvl = tmpl["lvl"]
    vars_def = dict(tmpl.get("vars", {}))
    if "state_vars" in tmpl:
        sv = tmpl["state_vars"].get(state, {})
        for k, dom in sv.items():
            vars_def[k] = dom

    vals: Dict[str, Any] = {}
    for varname, dom in vars_def.items():
        vals[varname] = sample_var(dom, key, varname)

    # If the template has a host var that is an edge-host choice, keep it coherent with the emitting host.
    if "host" in vals and host_col and host_col in comp.get("hosts", []):
        host_dom = vars_def.get("host", {})
        if host_dom.get("k") == "ch" and host_col in list(host_dom.get("v", [])):
            vals["host"] = host_col

    for k, v in overrides.items():
        vals[k] = v

    rendered = msg.format(**vals)
    service = comp.get("svc", "") or ""
    return lvl, rendered, service, host_col or ""


# -----------------------------
# Scenario control derivation
# -----------------------------
@dataclass(frozen=True)
class Interval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, float]


def build_failure_intervals() -> List[Interval]:
    f_phase = SCENARIO["time"]["phases"]["f"]
    start = int(f_phase["start_min"])
    end = int(f_phase["end_min"])
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [start] + sorted({int(e["at_min"]) for e in events if start <= int(e["at_min"]) < end}) + [end]
    boundaries = sorted(set(boundaries))
    current_rate: Dict[str, float] = {}
    current_lat: Dict[str, float] = {}
    out: List[Interval] = []
    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(int(e["at_min"]), []).append(e)

    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        for ev in events_by_min.get(s, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                current_rate[k] = float(v)
            for k, v in ev.get("latency_multipliers", {}).items():
                current_lat[k] = float(v)
        out.append(Interval(start_min=s, end_min=e, rate_mult=dict(current_rate), lat_mult=dict(current_lat)))
    return out


FAIL_INTERVALS = build_failure_intervals()


# -----------------------------
# Simulation: background + flows + one-shots
# -----------------------------
def pick_emitter_host(component_id: str, key: str) -> str:
    hosts = SYSTEM["components"][component_id].get("hosts", [])
    if not hosts:
        return ""
    return det_choice(hosts, "emit_host:" + component_id + ":" + key)


def simulate_background_interval(
    rows: List[Dict[str, Any]],
    base_dt: datetime,
    state: str,
    start_min: int,
    end_min: int,
    rate_mult: Dict[str, float],
) -> None:
    start_ms = start_min * 60_000
    end_ms = end_min * 60_000
    dur_min = (end_min - start_min)

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, {})
        for emit in beh.get("emit", []):
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            mult = 1.0
            if state == "f":
                mult = float(rate_mult.get(f"{comp_id}.{log_id}", 1.0))
            eff = per_min * mult
            if eff <= 0:
                continue

            if scope == "global":
                expected = eff * dur_min
                count = det_round(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:global")
                times = schedule_times(start_ms, end_ms, count, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:global")
                for j, tms in enumerate(times):
                    host_col = pick_emitter_host(comp_id, f"{state}:{start_min}:{log_id}:g:{j}")
                    lvl, msg, svc, host = render_log(
                        comp_id, log_id, state, host_col, overrides={}, key=f"bg:{state}:{comp_id}.{log_id}:{start_min}-{end_min}:{j}"
                    )
                    rows.append(
                        {"timestamp": ms_to_dt(base_dt, tms), "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": host}
                    )
            else:
                for host_col in comp.get("hosts", []):
                    expected = eff * dur_min
                    count = det_round(expected, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:{host_col}")
                    times = schedule_times(start_ms, end_ms, count, f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}:{host_col}")
                    for j, tms in enumerate(times):
                        lvl, msg, svc, host = render_log(
                            comp_id, log_id, state, host_col, overrides={}, key=f"bg:{state}:{comp_id}.{log_id}:{host_col}:{start_min}-{end_min}:{j}"
                        )
                        rows.append(
                            {"timestamp": ms_to_dt(base_dt, tms), "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": host}
                        )


def _enforce_monotonic_offsets(offsets_ms: List[int]) -> None:
    prev = 0
    for i in range(len(offsets_ms)):
        if offsets_ms[i] < prev:
            offsets_ms[i] = prev
        prev = offsets_ms[i]


def enforce_emit_timing_domains_offsets(offsets_ms: List[int], emit_chain: List[str], state: str, inst_key: str) -> None:
    """
    Interpret flow_def['latency_ms'][i] as (sampled) absolute offset-from-start for emit_chain[i],
    then adjust offsets so message-carried observed timing fields can be bound directly to these offsets.
    """
    if not offsets_ms:
        return

    _enforce_monotonic_offsets(offsets_ms)

    # Synthetic single-log flows: clamp offset to template domain for latency_ms.
    if len(emit_chain) == 1:
        comp_id, log_id = emit_chain[0].split(".", 1)
        r = get_int_range(comp_id, log_id, state, "latency_ms")
        if r is not None:
            lo, hi = r
            offsets_ms[0] = clamp_i(int(offsets_ms[0]), lo, hi)
        _enforce_monotonic_offsets(offsets_ms)
        return

    # If upstream_timeout is present, make its offset compatible with timeout_ms domain.
    for i, ref in enumerate(emit_chain):
        if ref == "edge_frontend.upstream_timeout":
            r = get_int_range("edge_frontend", "upstream_timeout", state, "timeout_ms")
            if r is not None:
                lo, hi = r
                offsets_ms[i] = clamp_i(int(offsets_ms[i]), lo, hi)

    # If http_worker.req_done(_nowaf) exists, make total_ms match (done_offset - req_start_offset).
    i_start = emit_chain.index("http_worker.req_start") if "http_worker.req_start" in emit_chain else -1
    i_done = -1
    done_ref = None
    for cand in ("http_worker.req_done", "http_worker.req_done_nowaf"):
        if cand in emit_chain:
            i_done = emit_chain.index(cand)
            done_ref = cand
            break

    if i_start >= 0 and i_done > i_start and done_ref is not None:
        done_comp, done_log = done_ref.split(".", 1)
        r = get_int_range(done_comp, done_log, state, "total_ms")
        if r is not None:
            lo, hi = r
            cur_delta = offsets_ms[i_done] - offsets_ms[i_start]
            tgt_delta = clamp_i(int(cur_delta), lo, hi)
            desired_done = offsets_ms[i_start] + tgt_delta
            # preserve ordering against previous emit
            prev_off = offsets_ms[i_done - 1] if i_done - 1 >= 0 else 0
            offsets_ms[i_done] = max(prev_off, desired_done)

    # If last is an access log, make rt_ms match the access offset.
    last_ref = emit_chain[-1]
    if last_ref in ("edge_frontend.access_ok", "edge_frontend.access_ok_slow", "edge_frontend.access_502"):
        comp_id, log_id = last_ref.split(".", 1)
        r = get_int_range(comp_id, log_id, state, "rt_ms")
        if r is not None:
            lo, hi = r
            offsets_ms[-1] = clamp_i(int(offsets_ms[-1]), lo, hi)
            offsets_ms[-1] = max(offsets_ms[-2], offsets_ms[-1])

    _enforce_monotonic_offsets(offsets_ms)


def simulate_flow_instances(
    rows: List[Dict[str, Any]],
    base_dt: datetime,
    state: str,
    flow_def: Dict[str, Any],
    start_ms_list: List[int],
    latency_mult: float = 1.0,
) -> None:
    flow_id = flow_def["id"]
    emit_chain: List[str] = list(flow_def["emit"])
    lat_pairs: List[List[int]] = list(flow_def["latency_ms"])

    for idx, start_ms in enumerate(start_ms_list):
        inst_key = f"flow:{state}:{flow_id}:{idx}"
        method = det_choice(["GET", "POST"], inst_key + ":method")
        customer_host = det_choice(["example.com", "api.example.com"], inst_key + ":c_host")
        uri = gen_uri(inst_key + ":uri")
        rid = det_hex(16, inst_key + ":rid")
        req_id = rid

        comp_host_cache: Dict[str, str] = {}

        def chost(comp_id: str) -> str:
            if comp_id in comp_host_cache:
                return comp_host_cache[comp_id]
            comp_host_cache[comp_id] = pick_emitter_host(comp_id, f"{inst_key}:{comp_id}")
            return comp_host_cache[comp_id]

        # Sample per-log absolute offsets from flow start, then enforce monotonicity and template timing domains.
        offsets_ms: List[int] = []
        for j, (p50, p95) in enumerate(lat_pairs):
            sp50 = float(p50) * float(latency_mult)
            sp95 = float(p95) * float(latency_mult)
            val = sample_lognormal_from_p50_p95(sp50, sp95, f"{inst_key}:off{j}")
            offsets_ms.append(int(round(val)))

        # Ensure offsets non-decreasing (logs in order), then enforce template domains by adjusting offsets.
        enforce_emit_timing_domains_offsets(offsets_ms, emit_chain, state, inst_key)

        # Build actual timestamps directly from offsets.
        log_times: List[int] = [start_ms + max(0, int(o)) for o in offsets_ms]

        overrides_by_emit: Dict[str, Dict[str, Any]] = {}
        common_ctx = {"method": method, "host": customer_host, "uri": uri, "rid": rid, "req_id": req_id}

        if flow_id in {"user_http_ok", "user_http_ok_degraded", "user_http_ok_nowaf"}:
            # Bind worker timing to offsets.
            i_start = emit_chain.index("http_worker.req_start") if "http_worker.req_start" in emit_chain else -1
            off_start = offsets_ms[i_start] if i_start >= 0 else 0

            for j, ref in enumerate(emit_chain):
                if ref == "http_worker.req_start":
                    overrides_by_emit[ref] = {"rid": rid, "method": method, "host": customer_host, "uri": uri}

                elif ref == "http_worker.req_done":
                    total_ms = int(max(0, offsets_ms[j] - off_start))
                    waf_range = get_int_range("http_worker", "req_done", state, "waf_ms") or (0, max(0, total_ms))
                    waf_lo, waf_hi = waf_range

                    if state == "n":
                        frac = 0.05 + 0.20 * hash_uniform01(inst_key + ":waf_frac")
                    else:
                        frac = 0.70 + 0.25 * hash_uniform01(inst_key + ":waf_frac")
                    waf_ms = int(round(total_ms * frac))
                    waf_ms = clamp_i(waf_ms, waf_lo, waf_hi)
                    waf_ms = min(waf_ms, total_ms)
                    overrides_by_emit[ref] = {"rid": rid, "total_ms": total_ms, "waf_ms": waf_ms}

                elif ref == "http_worker.req_done_nowaf":
                    total_ms = int(max(0, offsets_ms[j] - off_start))
                    waf_ms = det_int(0, 3, inst_key + ":nowaf:waf_ms")
                    waf_ms = min(waf_ms, total_ms)
                    overrides_by_emit[ref] = {"rid": rid, "total_ms": total_ms, "waf_ms": waf_ms}

                elif ref in {"edge_frontend.access_ok", "edge_frontend.access_ok_slow"}:
                    rt_ms = int(max(0, offsets_ms[j]))
                    bytes_dom = SYSTEM["components"]["edge_frontend"]["logs"]["access_ok"]["vars"]["bytes"]
                    if ref == "edge_frontend.access_ok_slow":
                        bytes_dom = SYSTEM["components"]["edge_frontend"]["logs"]["access_ok_slow"]["vars"]["bytes"]
                    b = det_int(int(bytes_dom["v"][0]), int(bytes_dom["v"][1]), inst_key + ":bytes:" + ref)
                    overrides_by_emit[ref] = {
                        "method": method,
                        "host": customer_host,
                        "uri": uri,
                        "req_id": req_id,
                        "rt_ms": rt_ms,
                        "bytes": b,
                    }

        elif flow_id == "user_http_502":
            # timeout_ms corresponds to the upstream_timeout offset; rt_ms corresponds to access_502 offset.
            timeout_off = int(offsets_ms[0]) if offsets_ms else 0
            access_off = int(offsets_ms[1]) if len(offsets_ms) > 1 else timeout_off
            bytes_dom = SYSTEM["components"]["edge_frontend"]["logs"]["access_502"]["vars"]["bytes"]
            b = det_int(int(bytes_dom["v"][0]), int(bytes_dom["v"][1]), inst_key + ":bytes:502")
            overrides_by_emit["edge_frontend.upstream_timeout"] = {
                "timeout_ms": timeout_off,
                "host": customer_host,
                "uri": uri,
                "req_id": req_id,
            }
            overrides_by_emit["edge_frontend.access_502"] = {
                "method": method,
                "host": customer_host,
                "uri": uri,
                "req_id": req_id,
                "rt_ms": access_off,
                "bytes": b,
            }

        elif flow_id in {"synthetic_waf_check_ok", "synthetic_waf_check_fail", "synthetic_waf_check_ok_f"}:
            # latency_ms equals the single-log offset.
            ref = emit_chain[0]
            latency_ms = int(max(0, offsets_ms[0] if offsets_ms else 0))
            overrides_by_emit[ref] = {"latency_ms": latency_ms}

        for j, ref in enumerate(emit_chain):
            comp_id, log_id = ref.split(".", 1)
            host_col = chost(comp_id)
            overrides = dict(common_ctx)
            overrides.update(overrides_by_emit.get(ref, {}))
            lvl, msg, svc, host = render_log(comp_id, log_id, state, host_col, overrides=overrides, key=f"{inst_key}:{ref}")
            rows.append(
                {"timestamp": ms_to_dt(base_dt, log_times[j]), "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": host}
            )


def emit_one_shots(rows: List[Dict[str, Any]], base_dt: datetime) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at_min = int(ev["at_min"])
        base_ms = at_min * 60_000
        event_key = f"event:{ev['order']}@{at_min}"
        shared: Dict[str, Any] = {}
        if at_min == 55:
            shared["change_id"] = det_hex(12, event_key + ":change_id")

        for shot in ev.get("one_shots", []):
            ref = shot["ref"]
            comp_id, log_id = ref.split(".", 1)
            count = int(shot["count"])
            hosts = list(shot.get("hosts", []))
            times = schedule_times(base_ms, base_ms + 800, count, f"oneshot:{event_key}:{ref}")
            for i in range(count):
                tms = times[i] if i < len(times) else (base_ms + i)
                host_col = hosts[i % len(hosts)] if hosts else pick_emitter_host(comp_id, f"{event_key}:{ref}:{i}")
                if host_col and host_col not in SYSTEM["components"][comp_id].get("hosts", []):
                    host_col = pick_emitter_host(comp_id, f"{event_key}:{ref}:{i}:fallback")
                lvl, msg, svc, host = render_log(
                    comp_id, log_id, "f", host_col, overrides=dict(shared), key=f"oneshot:{event_key}:{ref}:{i}"
                )
                rows.append({"timestamp": ms_to_dt(base_dt, tms), "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": host})


def main() -> None:
    base_dt = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows: List[Dict[str, Any]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start_ms = int(n_start * 60_000)
    n_end_ms = int(n_end * 60_000)
    n_dur_min = n_end - n_start

    simulate_background_interval(rows, base_dt, "n", n_start, n_end, rate_mult={})

    for flow in SYSTEM["flows"]["n"]["req"]:
        expected_instances = float(flow["rpm"]) * float(n_dur_min)
        count = det_round(expected_instances, f"flow:n:{flow['id']}:{n_start}-{n_end}")
        starts = schedule_times(n_start_ms, n_end_ms, count, f"flow:n:{flow['id']}:{n_start}-{n_end}")
        simulate_flow_instances(rows, base_dt, "n", flow, starts, latency_mult=1.0)

    f_flows = {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}

    for interval in FAIL_INTERVALS:
        smin, emin = interval.start_min, interval.end_min
        start_ms = smin * 60_000
        end_ms = emin * 60_000
        dur_min = emin - smin

        simulate_background_interval(rows, base_dt, "f", smin, emin, rate_mult=interval.rate_mult)

        for flow_id, flow in f_flows.items():
            mult = float(interval.rate_mult.get(flow_id, 1.0))
            rpm_eff = float(flow["rpm"]) * mult
            if rpm_eff <= 0:
                continue
            expected_instances = rpm_eff * float(dur_min)
            count = det_round(expected_instances, f"flow:f:{flow_id}:{smin}-{emin}:m={mult}")
            starts = schedule_times(start_ms, end_ms, count, f"flow:f:{flow_id}:{smin}-{emin}")
            lat_mult = float(interval.lat_mult.get(flow_id, 1.0))
            simulate_flow_instances(rows, base_dt, "f", flow, starts, latency_mult=lat_mult)

    emit_one_shots(rows, base_dt)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].apply(format_ts)

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert 20000 <= len(df) <= 100000, f"Row count {len(df)} out of bounds"

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
