import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "edge_dns_rrdns"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["dns_edge"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "dns_edge": {
            "svc": "rrdns",
            "hosts": ["dns-edge-01", "dns-edge-02", "dns-edge-03", "dns-edge-04"],
            "logs": {
                "dns_query": {
                    "lvl": "INFO",
                    "msg": "dns query received rid={rid} qname={qname} qtype={qtype} client_ip={client_ip}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {
                            "k": "ch",
                            "v": ["example.com", "www.example.com", "api.example.com", "www.theburritobot.com"],
                        },
                        "qtype": {"k": "ch", "v": ["A", "AAAA"]},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                    },
                },
                "cname_lookup_begin": {
                    "lvl": "INFO",
                    "msg": "cname lookup required rid={rid} qname={qname} cname_target={cname_target}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {"k": "ch", "v": ["www.example.com", "api.example.com", "www.theburritobot.com"]},
                        "cname_target": {"k": "ch", "v": ["origin-server.example-hosting.biz", "myapp.herokuapp.com"]},
                    },
                },
                "upstream_selected": {
                    "lvl": "DEBUG",
                    "msg": "selected internal resolver rid={rid} resolver={resolver} srtt_ms={srtt_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "resolver": {"k": "ch", "v": ["res-dc-a-1", "res-dc-a-2", "res-dc-a-3"]},
                    },
                    "state_vars": {
                        "n": {"srtt_ms": {"k": "i", "v": [1, 15]}},
                        "f": {"srtt_ms": {"k": "i", "v": [0, 30]}},
                    },
                },
                "upstream_retry": {
                    "lvl": "WARN",
                    "msg": "retrying internal resolver rid={rid} resolver={resolver} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "resolver": {"k": "ch", "v": ["res-dc-a-1", "res-dc-a-2", "res-dc-a-3"]},
                        "attempt": {"k": "i", "v": [2, 2]},
                        "backoff_ms": {"k": "i", "v": [5, 200]},
                    },
                },
                "dns_response_ok": {
                    "lvl": "INFO",
                    "msg": "dns response sent rid={rid} qname={qname} rcode=NOERROR answer_type={answer_type} latency_ms={latency_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {
                            "k": "ch",
                            "v": ["example.com", "www.example.com", "api.example.com", "www.theburritobot.com"],
                        },
                        "answer_type": {"k": "ch", "v": ["A", "AAAA", "CNAME+A", "CNAME+AAAA"]},
                        "latency_ms": {"k": "i", "v": [1, 2000]},
                    },
                },
                "dns_response_servfail": {
                    "lvl": "ERROR",
                    "msg": "dns response sent rid={rid} qname={qname} rcode=SERVFAIL reason={reason} latency_ms={latency_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {"k": "ch", "v": ["www.example.com", "api.example.com", "www.theburritobot.com"]},
                        "reason": {"k": "ch", "v": ["rrdns_panic", "upstream_timeout"]},
                        "latency_ms": {"k": "i", "v": [1, 2000]},
                    },
                },
                "rrdns_panic_recovered": {
                    "lvl": "ERROR",
                    "msg": "panic recovered in rrdns rid={rid} panic_msg={panic_msg} stack={stack_hash}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "panic_msg": {"k": "ch", "v": ["invalid argument to Int63n", "panic: invalid argument to Int63n"]},
                        "stack_hash": {"k": "hex", "v": 12},
                    },
                },
                "upstream_stats_reset": {
                    "lvl": "WARN",
                    "msg": "reset internal resolver stats rid={rid} resolver={resolver} reason={reason}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "resolver": {"k": "ch", "v": ["res-dc-a-1", "res-dc-a-2", "res-dc-a-3"]},
                        "reason": {"k": "ch", "v": ["negative_srtt", "negative_rtt_sample"]},
                    },
                },
                "rrdns_metrics": {
                    "lvl": "INFO",
                    "msg": "rrdns metrics window_s=60 min_srtt_ms={min_srtt_ms} max_srtt_ms={max_srtt_ms} panics_1m={panics_1m} servfail_1m={servfail_1m}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "min_srtt_ms": {"k": "i", "v": [1, 6]},
                            "max_srtt_ms": {"k": "i", "v": [4, 25]},
                            "panics_1m": {"k": "i", "v": [0, 0]},
                            "servfail_1m": {"k": "i", "v": [0, 2]},
                        },
                        "f": {
                            "min_srtt_ms": {"k": "i", "v": [0, 6]},
                            "max_srtt_ms": {"k": "i", "v": [4, 60]},
                            "panics_1m": {"k": "i", "v": [0, 25]},
                            "servfail_1m": {"k": "i", "v": [0, 45]},
                        },
                    },
                },
                "rrdns_metrics_neg": {
                    "lvl": "WARN",
                    "msg": "rrdns metrics anomaly window_s=60 min_srtt_ms={min_srtt_ms} max_srtt_ms={max_srtt_ms} panics_1m={panics_1m} servfail_1m={servfail_1m}",
                    "vars": {
                        "min_srtt_ms": {"k": "i", "v": [-800, -1]},
                        "max_srtt_ms": {"k": "i", "v": [4, 80]},
                        "panics_1m": {"k": "i", "v": [1, 25]},
                        "servfail_1m": {"k": "i", "v": [1, 45]},
                    },
                },
                "process_restart": {
                    "lvl": "INFO",
                    "msg": "rrdns restarted reason={reason} version={version}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["hotfix_rollout", "operator_restart"]},
                        "version": {"k": "ch", "v": ["rrdns-2017.01.01-hotfix1"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rrdns_metrics", "per_min": 0.5, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "rrdns_metrics", "per_min": 1.0, "scope": "per_host"},
                        {"id": "rrdns_metrics_neg", "per_min": 0.5, "scope": "per_host"},
                    ]
                },
            },
        },
        "internal_resolver": {
            "svc": "recursor",
            "hosts": ["res-01", "res-02"],
            "logs": {
                "recursive_query": {
                    "lvl": "DEBUG",
                    "msg": "recursive query sent rid={rid} qname={qname} upstream={public_ns} proto=udp timeout_ms={timeout_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {"k": "ch", "v": ["origin-server.example-hosting.biz", "myapp.herokuapp.com"]},
                        "public_ns": {"k": "ch", "v": ["1.1.1.1", "8.8.8.8", "9.9.9.9"]},
                        "timeout_ms": {"k": "i", "v": [200, 800]},
                    },
                },
                "recursive_response_ok": {
                    "lvl": "DEBUG",
                    "msg": "recursive response rid={rid} qname={qname} rcode=NOERROR latency_ms={latency_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {"k": "ch", "v": ["origin-server.example-hosting.biz", "myapp.herokuapp.com"]},
                        "latency_ms": {"k": "i", "v": [1, 120]},
                    },
                },
                "recursive_response_err": {
                    "lvl": "DEBUG",
                    "msg": "recursive response rid={rid} qname={qname} rcode=SERVFAIL latency_ms={latency_ms}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "qname": {"k": "ch", "v": ["origin-server.example-hosting.biz", "myapp.herokuapp.com"]},
                        "latency_ms": {"k": "i", "v": [20, 800]},
                    },
                },
                "resolver_health": {
                    "lvl": "INFO",
                    "msg": "resolver health qps={qps} p95_ms={p95_ms} udp_drops_1m={udp_drops_1m}",
                    "vars": {
                        "qps": {"k": "i", "v": [50, 400]},
                        "p95_ms": {"k": "i", "v": [2, 60]},
                        "udp_drops_1m": {"k": "i", "v": [0, 80]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "resolver_health", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "resolver_health", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        "time_sync": {
            "svc": "timesync",
            "hosts": ["dns-edge-01", "dns-edge-02", "dns-edge-03", "dns-edge-04"],
            "logs": {
                "clock_step": {
                    "lvl": "WARN",
                    "msg": "clock step applied direction={direction} offset_ms={offset_ms} source={source}",
                    "vars": {
                        "direction": {"k": "ch", "v": ["backward"]},
                        "offset_ms": {"k": "i", "v": [-1100, -900]},
                        "source": {"k": "ch", "v": ["leap_second_adjust", "ntp_step"]},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "monitoring": {
            "svc": "monitor",
            "hosts": ["mon-01"],
            "logs": {
                "alert_open": {
                    "lvl": "CRITICAL",
                    "msg": "alert opened name={name} severity={sev} value={value} threshold={threshold}",
                    "vars": {
                        "name": {"k": "ch", "v": ["dns_servfail_rate"]},
                        "sev": {"k": "ch", "v": ["page", "critical"]},
                        "value": {"k": "f", "v": [0.60, 1.50]},
                        "threshold": {"k": "f", "v": [0.10, 0.50]},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "deploy_orchestrator": {
            "svc": "deploy",
            "hosts": ["deploy-01"],
            "logs": {
                "deploy_start": {
                    "lvl": "INFO",
                    "msg": "deploy start service={service} version={version} scope={scope}",
                    "vars": {
                        "service": {"k": "ch", "v": ["rrdns"]},
                        "version": {"k": "ch", "v": ["rrdns-2017.01.01-hotfix1"]},
                        "scope": {"k": "ch", "v": ["canary_node", "canary_dc", "major_dcs"]},
                    },
                },
                "deploy_finish": {
                    "lvl": "INFO",
                    "msg": "deploy finish service={service} version={version} scope={scope} success={success}",
                    "vars": {
                        "service": {"k": "ch", "v": ["rrdns"]},
                        "version": {"k": "ch", "v": ["rrdns-2017.01.01-hotfix1"]},
                        "scope": {"k": "ch", "v": ["canary_node", "canary_dc", "major_dcs"]},
                        "success": {"k": "ch", "v": ["true"]},
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
                    "id": "dns_simple_query_n",
                    "rpm": 280.0,
                    "emit": ["dns_edge.dns_query", "dns_edge.dns_response_ok"],
                    "latency_ms": [[0, 1], [1, 4]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "dns_cname_lookup_n",
                    "rpm": 108.0,
                    "emit": [
                        "dns_edge.dns_query",
                        "dns_edge.cname_lookup_begin",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_ok",
                        "dns_edge.dns_response_ok",
                    ],
                    "latency_ms": [[0, 1], [0, 1], [0, 1], [0, 1], [2, 6], [3, 8]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "dns_cname_lookup_retry_n",
                    "rpm": 12.0,
                    "emit": [
                        "dns_edge.dns_query",
                        "dns_edge.cname_lookup_begin",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_err",
                        "dns_edge.upstream_retry",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_ok",
                        "dns_edge.dns_response_ok",
                    ],
                    "latency_ms": [[0, 1], [0, 1], [0, 2], [0, 2], [20, 200], [0, 2], [0, 2], [0, 2], [2, 10], [5, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "dns_simple_query_f",
                    "rpm": 280.0,
                    "emit": ["dns_edge.dns_query", "dns_edge.dns_response_ok"],
                    "latency_ms": [[0, 1], [1, 4]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "dns_cname_lookup_f",
                    "rpm": 105.6,
                    "emit": [
                        "dns_edge.dns_query",
                        "dns_edge.cname_lookup_begin",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_ok",
                        "dns_edge.dns_response_ok",
                    ],
                    "latency_ms": [[0, 1], [0, 1], [0, 2], [0, 2], [2, 8], [3, 12]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "dns_cname_lookup_retry_f",
                    "rpm": 12.0,
                    "emit": [
                        "dns_edge.dns_query",
                        "dns_edge.cname_lookup_begin",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_err",
                        "dns_edge.upstream_retry",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_ok",
                        "dns_edge.dns_response_ok",
                    ],
                    "latency_ms": [[0, 1], [0, 1], [0, 3], [0, 3], [30, 350], [0, 3], [0, 3], [0, 3], [2, 15], [5, 45]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "dns_cname_panic_fail_f",
                    "rpm": 0.4,
                    "emit": [
                        "dns_edge.dns_query",
                        "dns_edge.cname_lookup_begin",
                        "dns_edge.rrdns_panic_recovered",
                        "dns_edge.dns_response_servfail",
                    ],
                    "latency_ms": [[0, 1], [0, 1], [0, 2], [1, 6]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "dns_cname_fixed_reset_f",
                    "rpm": 2.0,
                    "emit": [
                        "dns_edge.dns_query",
                        "dns_edge.cname_lookup_begin",
                        "dns_edge.upstream_stats_reset",
                        "dns_edge.upstream_selected",
                        "internal_resolver.recursive_query",
                        "internal_resolver.recursive_response_ok",
                        "dns_edge.dns_response_ok",
                    ],
                    "latency_ms": [[0, 1], [0, 1], [0, 3], [0, 3], [0, 3], [2, 10], [3, 14]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "leap_second_negative_srtt_rrdns"},
    "time": {
        "total_minutes": 56,
        "phases": {"n": {"start_min": 0, "end_min": 28}, "f": {"start_min": 28, "end_min": 56}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 28,
                    "rate_multipliers": {"dns_cname_fixed_reset_f": 0.0, "dns_edge.rrdns_metrics_neg": 0.0},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "time_sync.clock_step", "count": 2, "hosts": ["dns-edge-02", "dns-edge-04"]}],
                },
                {
                    "order": 2,
                    "at_min": 32,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "monitoring.alert_open", "count": 1, "hosts": ["mon-01"]}],
                },
                {
                    "order": 3,
                    "at_min": 38,
                    "rate_multipliers": {
                        "dns_cname_panic_fail_f": 4.0,
                        "dns_edge.rrdns_metrics": 1.4,
                        "dns_edge.rrdns_metrics_neg": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [],
                },
                {
                    "order": 4,
                    "at_min": 44,
                    "rate_multipliers": {
                        "dns_cname_panic_fail_f": 1.5,
                        "dns_cname_fixed_reset_f": 1.0,
                        "dns_edge.rrdns_metrics": 1.0,
                        "dns_edge.rrdns_metrics_neg": 0.6,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "deploy_orchestrator.deploy_start", "count": 1, "hosts": ["deploy-01"]},
                        {"ref": "dns_edge.process_restart", "count": 1, "hosts": ["dns-edge-02"]},
                        {"ref": "deploy_orchestrator.deploy_finish", "count": 1, "hosts": ["deploy-01"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 50,
                    "rate_multipliers": {
                        "dns_cname_panic_fail_f": 0.6,
                        "dns_cname_fixed_reset_f": 2.0,
                        "dns_edge.rrdns_metrics": 0.8,
                        "dns_edge.rrdns_metrics_neg": 0.3,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "deploy_orchestrator.deploy_start", "count": 1, "hosts": ["deploy-01"]},
                        {"ref": "dns_edge.process_restart", "count": 1, "hosts": ["dns-edge-03"]},
                        {"ref": "deploy_orchestrator.deploy_finish", "count": 1, "hosts": ["deploy-01"]},
                    ],
                },
            ]
        }
    },
}

SEED = 1337
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _h64(s: str) -> int:
    h = hashlib.sha1((str(SEED) + "|" + s).encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def stable_u(s: str) -> float:
    return ((_h64(s) % 1_000_000_000) / 1_000_000_000.0)


def gen_hex(n: int, key: str) -> str:
    need = n
    out = []
    k = 0
    while need > 0:
        chunk = hashlib.sha1((str(SEED) + "|" + key + f"|{k}").encode("utf-8")).hexdigest()
        take = min(need, len(chunk))
        out.append(chunk[:take])
        need -= take
        k += 1
    return "".join(out)[:n].lower()


def ip_from_cidr(cidr: str, key: str) -> str:
    base, _ = cidr.split("/")
    parts = base.split(".")
    last = 1 + (_h64("ip|" + key) % 254)
    return ".".join(parts[:3] + [str(last)])


def sample_int(lo: int, hi: int, key: str) -> int:
    if lo == hi:
        return int(lo)
    u = stable_u(key)
    return int(lo + math.floor(u * ((hi - lo) + 1)))


def sample_float(lo: float, hi: float, key: str) -> float:
    u = stable_u(key)
    return lo + (hi - lo) * u


def sample_choice(vals: List[Any], key: str) -> Any:
    if len(vals) == 1:
        return vals[0]
    u = stable_u(key)
    idx = int(math.floor(u * len(vals)))
    if idx >= len(vals):
        idx = len(vals) - 1
    return vals[idx]


def inv_norm_cdf(p: float) -> float:
    p = float(p)
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
    phigh = 1.0 - plow

    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den

    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -(num / den)

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def lognormal_from_p50_p95(p50: float, p95: float, u: float) -> float:
    if p50 <= 0 and p95 <= 0:
        return 0.0
    if p50 <= 0:
        return max(0.0, (u**2) * p95)
    if p95 <= p50:
        return max(0.0, p50)

    u = min(max(u, 1e-9), 1.0 - 1e-9)
    sigma = math.log(p95 / p50) / 1.645
    mu = math.log(p50)
    z = inv_norm_cdf(u)
    x = math.exp(mu + sigma * z)

    cap = 2.5 * p95
    if x > cap:
        x = cap + (x - cap) * 0.1
    return x


def sample_delay_ms(p50_p95: List[float], key: str, mult: float = 1.0) -> int:
    p50, p95 = float(p50_p95[0]) * mult, float(p50_p95[1]) * mult
    u = stable_u("delay|" + key)
    val = lognormal_from_p50_p95(p50, p95, u)
    return int(round(max(0.0, val)))


def sample_backoff_ms(key: str, lo: int = 5, hi: int = 200) -> int:
    u = stable_u("backoff|" + key)
    val = lognormal_from_p50_p95(30.0, 150.0, u)
    val = int(round(val))
    if val < lo:
        val = lo
    if val > hi:
        val = hi
    return val


class DeterministicRounding:
    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x))
        self.carry[key] = x - n
        if self.carry[key] > 0.999999:
            n += 1
            self.carry[key] -= 1.0
        return max(0, n)


def dt_at_minute(minute: float) -> datetime:
    return BASE_TIME + timedelta(minutes=float(minute))


def schedule_even(t0: datetime, t1: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (t1 - t0).total_seconds()
    if dur_s <= 0:
        return [t0] * count
    spacing = dur_s / count
    jitter_cap = min(0.08, spacing * 0.2)
    out: List[datetime] = []
    for i in range(count):
        base = (i + 0.5) * spacing
        u = stable_u(f"jitter|{key}|{i}")
        jitter = (u - 0.5) * 2.0 * jitter_cap
        t = t0 + timedelta(seconds=base + jitter)
        if t < t0:
            t = t0
        if t >= t1:
            t = t1 - timedelta(milliseconds=1)
        out.append(t)
    out.sort()
    return out


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, lid = ref.split(".", 1)
    return comp, lid


def get_log_def(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def choose_domain_value(domain: Dict[str, Any], key: str) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "hex":
        return gen_hex(int(v), key)
    if k == "ip":
        return ip_from_cidr(str(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return sample_int(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return round(sample_float(lo, hi, key), 3)
    if k == "ch":
        return sample_choice(list(v), key)
    if k == "uuid":
        h = gen_hex(32, "uuid|" + key)
        return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if k == "str":
        return str(v)
    return str(v)


def render_log_message(comp_id: str, log_id: str, state: str, key_prefix: str, bound: Dict[str, Any]) -> Tuple[str, str]:
    ld = get_log_def(comp_id, log_id)
    vars_def = ld.get("vars", {})
    state_vars = ld.get("state_vars", {}).get(state, {})
    vals: Dict[str, Any] = dict(bound)

    for k, dom in vars_def.items():
        if k not in vals:
            vals[k] = choose_domain_value(dom, f"{key_prefix}|{comp_id}.{log_id}|{k}")
    for k, dom in state_vars.items():
        if k not in vals:
            vals[k] = choose_domain_value(dom, f"{key_prefix}|{comp_id}.{log_id}|{k}|{state}")

    msg = ld["msg"].format(**vals)
    lvl = ld["lvl"]
    return lvl, msg


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.replace(microsecond=ms * 1000).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def build_failure_segments() -> List[Dict[str, Any]]:
    f = SCENARIO["time"]["phases"]["f"]
    start = int(f["start_min"])
    end = int(f["end_min"])
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: e["at_min"])
    bounds = [start] + sorted({int(e["at_min"]) for e in events if start <= int(e["at_min"]) <= end}) + [end]
    bounds = sorted(bounds)

    segments: List[Dict[str, Any]] = []
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, float] = {}

    for e in events:
        if int(e["at_min"]) == start:
            active_rate.update(e.get("rate_multipliers", {}) or {})
            active_lat.update(e.get("latency_multipliers", {}) or {})

    for i in range(len(bounds) - 1):
        a, b = bounds[i], bounds[i + 1]
        for e in events:
            if int(e["at_min"]) == a and a != start:
                active_rate.update(e.get("rate_multipliers", {}) or {})
                active_lat.update(e.get("latency_multipliers", {}) or {})
        segments.append({"start_min": a, "end_min": b, "rate": dict(active_rate), "lat": dict(active_lat)})

    return segments


FAILURE_SEGMENTS = build_failure_segments()


def active_multiplier(segments: List[Dict[str, Any]], minute: float, key: str, kind: str = "rate") -> float:
    for seg in segments:
        if seg["start_min"] <= minute < seg["end_min"]:
            d = seg["rate"] if kind == "rate" else seg["lat"]
            return float(d.get(key, 1.0))
    return 1.0


def pick_dns_edge_host(flow_id: str, start_min: float, inst_idx: int) -> str:
    hosts = SYSTEM["components"]["dns_edge"]["hosts"]
    if flow_id == "dns_cname_panic_fail_f":
        if start_min < 44:
            return ["dns-edge-02", "dns-edge-04"][inst_idx % 2]
        return "dns-edge-04"
    return hosts[inst_idx % len(hosts)]


def pick_component_host(comp_id: str, flow_id: str, start_min: float, inst_idx: int) -> str:
    if comp_id == "dns_edge":
        return pick_dns_edge_host(flow_id, start_min, inst_idx)
    hosts = SYSTEM["components"][comp_id]["hosts"]
    if not hosts:
        return ""
    return hosts[_h64(f"host|{comp_id}|{flow_id}|{start_min:.3f}|{inst_idx}") % len(hosts)]


def _latency_ms_bounds_for_log(comp_id: str, log_id: str) -> Tuple[int, int]:
    ld = get_log_def(comp_id, log_id)
    dom = (ld.get("vars", {}) or {}).get("latency_ms")
    if not dom:
        return (0, 2_000_000_000)
    v = dom.get("v")
    return (int(v[0]), int(v[1]))


def simulate_flow_instance(
    flow: Dict[str, Any],
    state: str,
    start_dt: datetime,
    start_min: float,
    inst_idx: int,
    lat_mult: float,
    out_rows: List[Dict[str, Any]],
    seq_counter: List[int],
) -> None:
    flow_id = flow["id"]
    emit_refs = flow["emit"]
    lat_pairs = flow["latency_ms"]
    trace_on = bool(SYSTEM["tracing"]["on"]) and bool(flow.get("trace", False))
    trace_id = gen_hex(32, f"trace|{flow_id}|{start_min:.6f}|{inst_idx}") if trace_on else ""

    rid = gen_hex(16, f"rid|{flow_id}|{start_min:.6f}|{inst_idx}")
    qtype = sample_choice(["A", "AAAA"], f"qtype|{flow_id}|{start_min:.6f}|{inst_idx}")
    client_ip = ip_from_cidr("198.51.100.0/24", f"cip|{flow_id}|{start_min:.6f}|{inst_idx}")

    cname_qnames = ["www.example.com", "api.example.com", "www.theburritobot.com"]
    all_qnames = ["example.com", "www.example.com", "api.example.com", "www.theburritobot.com"]
    cname_targets = ["origin-server.example-hosting.biz", "myapp.herokuapp.com"]

    if flow_id in ("dns_simple_query_n", "dns_simple_query_f"):
        qname = sample_choice(all_qnames, f"qname|{flow_id}|{start_min:.6f}|{inst_idx}")
        cname_target = ""
        answer_type = qtype
    else:
        qname = sample_choice(cname_qnames, f"qname|{flow_id}|{start_min:.6f}|{inst_idx}")
        cname_target = sample_choice(cname_targets, f"ctgt|{flow_id}|{start_min:.6f}|{inst_idx}")
        answer_type = "CNAME+A" if qtype == "A" else "CNAME+AAAA"

    resolvers = ["res-dc-a-1", "res-dc-a-2", "res-dc-a-3"]
    res1 = sample_choice(resolvers, f"res1|{flow_id}|{start_min:.6f}|{inst_idx}")
    res2 = sample_choice([r for r in resolvers if r != res1], f"res2|{flow_id}|{start_min:.6f}|{inst_idx}")

    comp_host: Dict[str, str] = {}
    for ref in emit_refs:
        comp_id, _ = parse_ref(ref)
        if comp_id not in comp_host:
            comp_host[comp_id] = pick_component_host(comp_id, flow_id, start_min, inst_idx)

    times: List[datetime] = []
    cur = start_dt
    for j, ref in enumerate(emit_refs):
        dms = sample_delay_ms(lat_pairs[j], f"{flow_id}|{start_min:.6f}|{inst_idx}|{j}", mult=lat_mult)
        if ref in ("internal_resolver.recursive_response_ok", "internal_resolver.recursive_response_err"):
            comp_id, log_id = parse_ref(ref)
            lo, hi = _latency_ms_bounds_for_log(comp_id, log_id)
            dms = max(lo, min(hi, dms))
        cur = cur + timedelta(milliseconds=dms)
        times.append(cur)

    backoff_ms_value = None
    for j, ref in enumerate(emit_refs):
        if ref == "dns_edge.upstream_retry" and (j + 1) < len(emit_refs):
            backoff_ms_value = sample_backoff_ms(f"{flow_id}|{start_min:.6f}|{inst_idx}|backoff")
            planned_gap = int(round((times[j + 1] - times[j]).total_seconds() * 1000.0))
            desired_gap = int(backoff_ms_value)
            delta = desired_gap - planned_gap
            if delta != 0:
                for k in range(j + 1, len(times)):
                    times[k] = times[k] + timedelta(milliseconds=delta)

    rr_latency_ms: Dict[int, int] = {}
    last_rq_index = None
    for j, ref in enumerate(emit_refs):
        if ref == "internal_resolver.recursive_query":
            last_rq_index = j
        if ref in ("internal_resolver.recursive_response_ok", "internal_resolver.recursive_response_err") and last_rq_index is not None:
            rr_latency_ms[j] = int(round((times[j] - times[last_rq_index]).total_seconds() * 1000.0))

    query_time = None
    for j, ref in enumerate(emit_refs):
        if ref == "dns_edge.dns_query":
            query_time = times[j]
            break

    for j, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        host = comp_host.get(comp_id, "")
        svc = SYSTEM["components"][comp_id]["svc"] or ""

        bound: Dict[str, Any] = {"rid": rid}

        if log_id == "dns_query":
            bound.update({"qname": qname, "qtype": qtype, "client_ip": client_ip})
        elif log_id == "cname_lookup_begin":
            bound.update({"qname": qname, "cname_target": cname_target})
        elif log_id == "upstream_selected":
            if flow_id in ("dns_cname_lookup_retry_n", "dns_cname_lookup_retry_f") and j >= 6:
                bound.update({"resolver": res2})
            else:
                bound.update({"resolver": res1})
        elif log_id == "upstream_retry":
            # Modeled semantics: this log marks switching to another resolver; align with the next selected resolver.
            retry_target = res2 if flow_id in ("dns_cname_lookup_retry_n", "dns_cname_lookup_retry_f") else res1
            bound.update({"resolver": retry_target, "attempt": 2})
            bound["backoff_ms"] = int(backoff_ms_value) if backoff_ms_value is not None else sample_backoff_ms(
                f"{flow_id}|{start_min:.6f}|{inst_idx}|backoff2"
            )
        elif log_id == "upstream_stats_reset":
            bound.update(
                {
                    "resolver": res1,
                    "reason": sample_choice(
                        ["negative_srtt", "negative_rtt_sample"], f"reset_reason|{flow_id}|{start_min:.6f}|{inst_idx}"
                    ),
                }
            )
        elif log_id == "recursive_query":
            bound.update(
                {
                    "qname": cname_target,
                    "public_ns": sample_choice(
                        ["1.1.1.1", "8.8.8.8", "9.9.9.9"], f"pubns|{flow_id}|{start_min:.6f}|{inst_idx}|{j}"
                    ),
                    "timeout_ms": sample_int(200, 800, f"to|{flow_id}|{start_min:.6f}|{inst_idx}|{j}"),
                }
            )
        elif log_id in ("recursive_response_ok", "recursive_response_err"):
            lo, hi = _latency_ms_bounds_for_log(comp_id, log_id)
            lat = rr_latency_ms.get(j)
            if lat is None:
                lat = sample_int(lo, hi, f"rrlat|{flow_id}|{start_min:.6f}|{inst_idx}|{j}")
            lat = max(lo, min(hi, int(lat)))
            bound.update({"qname": cname_target, "latency_ms": lat})
        elif log_id == "rrdns_panic_recovered":
            bound.update(
                {
                    "panic_msg": sample_choice(
                        ["invalid argument to Int63n", "panic: invalid argument to Int63n"],
                        f"panicmsg|{flow_id}|{start_min:.6f}|{inst_idx}",
                    ),
                    "stack_hash": gen_hex(12, f"stack|{flow_id}|{start_min:.6f}|{inst_idx}"),
                }
            )
        elif log_id == "dns_response_ok":
            total_ms = 1
            if query_time is not None:
                total_ms = int(round((times[j] - query_time).total_seconds() * 1000.0))
                total_ms = max(1, min(2000, total_ms))
            bound.update({"qname": qname, "answer_type": answer_type, "latency_ms": total_ms})
        elif log_id == "dns_response_servfail":
            total_ms = 1
            if query_time is not None:
                total_ms = int(round((times[j] - query_time).total_seconds() * 1000.0))
                total_ms = max(1, min(2000, total_ms))
            reason = "rrdns_panic" if flow_id == "dns_cname_panic_fail_f" else sample_choice(
                ["rrdns_panic", "upstream_timeout"], f"sf_reason|{flow_id}|{start_min:.6f}|{inst_idx}"
            )
            bound.update({"qname": qname, "reason": reason, "latency_ms": total_ms})

        lvl, msg = render_log_message(comp_id, log_id, state, f"flow|{flow_id}|{start_min:.6f}|{inst_idx}|{j}", bound)
        out_rows.append(
            {
                "_dt": times[j],
                "_seq": seq_counter[0],
                "timestamp": "",
                "level": lvl,
                "message": msg,
                "trace_id": trace_id,
                "service": svc,
                "host": host,
            }
        )
        seq_counter[0] += 1


def emit_background(
    comp_id: str,
    log_id: str,
    state: str,
    t0: datetime,
    t1: datetime,
    count: int,
    host: str,
    out_rows: List[Dict[str, Any]],
    seq_counter: List[int],
) -> None:
    times = schedule_even(t0, t1, count, f"bg|{comp_id}.{log_id}|{state}|{host}|{t0.isoformat()}|{t1.isoformat()}")
    svc = SYSTEM["components"][comp_id]["svc"] or ""

    for i, dt in enumerate(times):
        bound: Dict[str, Any] = {}

        if comp_id == "dns_edge" and log_id in ("rrdns_metrics", "rrdns_metrics_neg") and state == "f":
            minute = (dt - BASE_TIME).total_seconds() / 60.0
            panic_mult = active_multiplier(FAILURE_SEGMENTS, minute, "dns_cname_panic_fail_f", kind="rate")
            intensity = (panic_mult - 0.6) / (4.0 - 0.6)
            intensity = max(0.0, min(1.0, intensity))

            if log_id == "rrdns_metrics":
                pan = int(round(intensity * 18 + stable_u(f"m_pan|{host}|{minute:.3f}|{i}") * 6))
                sf = int(round(intensity * 30 + stable_u(f"m_sf|{host}|{minute:.3f}|{i}") * 8))
                min_srtt = sample_int(0, 6, f"m_min|{host}|{minute:.3f}|{i}")
                max_srtt = int(round(10 + intensity * 40 + stable_u(f"m_max|{host}|{minute:.3f}|{i}") * 10))

                bound["panics_1m"] = max(0, min(25, pan))
                bound["servfail_1m"] = max(0, min(45, sf))
                bound["min_srtt_ms"] = max(0, min(6, int(min_srtt)))
                bound["max_srtt_ms"] = max(4, min(60, int(max_srtt)))
            else:
                pan = int(round(1 + intensity * 20 + stable_u(f"mn_pan|{host}|{minute:.3f}|{i}") * 3))
                sf = int(round(1 + intensity * 35 + stable_u(f"mn_sf|{host}|{minute:.3f}|{i}") * 5))
                min_srtt = sample_int(-800, -1, f"mn_min|{host}|{minute:.3f}|{i}")
                max_srtt = int(round(10 + intensity * 60 + stable_u(f"mn_max|{host}|{minute:.3f}|{i}") * 10))

                bound["panics_1m"] = max(1, min(25, pan))
                bound["servfail_1m"] = max(1, min(45, sf))
                bound["min_srtt_ms"] = max(-800, min(-1, int(min_srtt)))
                bound["max_srtt_ms"] = max(4, min(80, int(max_srtt)))

        lvl, msg = render_log_message(
            comp_id, log_id, state, f"bg|{comp_id}.{log_id}|{state}|{host}|{i}|{dt.isoformat()}", bound
        )
        out_rows.append(
            {
                "_dt": dt,
                "_seq": seq_counter[0],
                "timestamp": "",
                "level": lvl,
                "message": msg,
                "trace_id": "",
                "service": svc,
                "host": host,
            }
        )
        seq_counter[0] += 1


def emit_one_shots(out_rows: List[Dict[str, Any]], seq_counter: List[int]) -> None:
    events = SCENARIO["phases"]["f"]["events"]
    for e in sorted(events, key=lambda x: x["at_min"]):
        at_min = int(e["at_min"])
        base = dt_at_minute(at_min)
        one_shots = e.get("one_shots", []) or []
        for si, spec in enumerate(one_shots):
            ref = spec["ref"]
            comp_id, log_id = parse_ref(ref)
            svc = SYSTEM["components"][comp_id]["svc"] or ""
            count = int(spec["count"])
            hosts = list(spec.get("hosts") or [])
            for i in range(count):
                host = (
                    hosts[i % len(hosts)]
                    if hosts
                    else (SYSTEM["components"][comp_id]["hosts"][0] if SYSTEM["components"][comp_id]["hosts"] else "")
                )
                u = stable_u(f"oneshot|{at_min}|{ref}|{si}|{i}")
                dt = base + timedelta(milliseconds=int(round(200 * si + 90 * i + u * 50)))
                bound: Dict[str, Any] = {}
                if comp_id == "dns_edge" and log_id == "process_restart":
                    bound["reason"] = "hotfix_rollout"
                    bound["version"] = "rrdns-2017.01.01-hotfix1"
                lvl, msg = render_log_message(comp_id, log_id, "f", f"oneshot|{at_min}|{ref}|{si}|{i}", bound)
                out_rows.append(
                    {
                        "_dt": dt,
                        "_seq": seq_counter[0],
                        "timestamp": "",
                        "level": lvl,
                        "message": msg,
                        "trace_id": "",
                        "service": svc,
                        "host": host,
                    }
                )
                seq_counter[0] += 1


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    rows: List[Dict[str, Any]] = []
    seq_counter = [0]
    allocator = DeterministicRounding()

    n0 = dt_at_minute(SCENARIO["time"]["phases"]["n"]["start_min"])
    n1 = dt_at_minute(SCENARIO["time"]["phases"]["n"]["end_min"])
    n_dur_min = (n1 - n0).total_seconds() / 60.0

    for comp_id, comp in SYSTEM["components"].items():
        for emit in comp.get("beh", {}).get("n", {}).get("emit", []):
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            if scope == "global":
                expected = per_min * n_dur_min
                count = allocator.alloc(f"bg|n|{comp_id}.{log_id}|global", expected)
                host = comp["hosts"][0] if comp.get("hosts") else ""
                emit_background(comp_id, log_id, "n", n0, n1, count, host, rows, seq_counter)
            else:
                for host in comp.get("hosts", []) or [""]:
                    expected = per_min * n_dur_min
                    count = allocator.alloc(f"bg|n|{comp_id}.{log_id}|{host}", expected)
                    emit_background(comp_id, log_id, "n", n0, n1, count, host, rows, seq_counter)

    for flow in SYSTEM["flows"]["n"]["req"]:
        rpm = float(flow["rpm"])
        expected_instances = rpm * n_dur_min
        count = allocator.alloc(f"flow|n|{flow['id']}", expected_instances)
        starts = schedule_even(n0, n1, count, f"flowstart|n|{flow['id']}")
        for i, st in enumerate(starts):
            simulate_flow_instance(flow, "n", st, (st - BASE_TIME).total_seconds() / 60.0, i, 1.0, rows, seq_counter)

    for seg in FAILURE_SEGMENTS:
        t0 = dt_at_minute(seg["start_min"])
        t1 = dt_at_minute(seg["end_min"])
        dur_min = (t1 - t0).total_seconds() / 60.0

        for comp_id, comp in SYSTEM["components"].items():
            for emit in comp.get("beh", {}).get("f", {}).get("emit", []):
                log_id = emit["id"]
                per_min = float(emit["per_min"])
                scope = emit.get("scope", "per_host")
                mult = float(seg["rate"].get(f"{comp_id}.{log_id}", 1.0))
                eff_per_min = per_min * mult
                if scope == "global":
                    expected = eff_per_min * dur_min
                    count = allocator.alloc(
                        f"bg|f|{comp_id}.{log_id}|global|{seg['start_min']}-{seg['end_min']}", expected
                    )
                    host = comp["hosts"][0] if comp.get("hosts") else ""
                    emit_background(comp_id, log_id, "f", t0, t1, count, host, rows, seq_counter)
                else:
                    for host in comp.get("hosts", []) or [""]:
                        expected = eff_per_min * dur_min
                        count = allocator.alloc(
                            f"bg|f|{comp_id}.{log_id}|{host}|{seg['start_min']}-{seg['end_min']}", expected
                        )
                        emit_background(comp_id, log_id, "f", t0, t1, count, host, rows, seq_counter)

    for seg in FAILURE_SEGMENTS:
        t0 = dt_at_minute(seg["start_min"])
        t1 = dt_at_minute(seg["end_min"])
        dur_min = (t1 - t0).total_seconds() / 60.0
        for flow in SYSTEM["flows"]["f"]["req"]:
            flow_id = flow["id"]
            rpm = float(flow["rpm"])
            mult = float(seg["rate"].get(flow_id, 1.0))
            eff_rpm = rpm * mult
            expected_instances = eff_rpm * dur_min
            count = allocator.alloc(f"flow|f|{flow_id}|{seg['start_min']}-{seg['end_min']}", expected_instances)
            starts = schedule_even(t0, t1, count, f"flowstart|f|{flow_id}|{seg['start_min']}-{seg['end_min']}")
            lat_mult = float(seg["lat"].get(flow_id, 1.0))
            for i, st in enumerate(starts):
                simulate_flow_instance(
                    flow, "f", st, (st - BASE_TIME).total_seconds() / 60.0, i, lat_mult, rows, seq_counter
                )

    emit_one_shots(rows, seq_counter)

    for r in rows:
        r["timestamp"] = fmt_ts(r["_dt"])

    df = pd.DataFrame(rows)
    df.sort_values(by=["_dt", "_seq"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
